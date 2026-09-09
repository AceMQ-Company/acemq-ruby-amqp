# frozen_string_literal: true

# Copyright 2026 AceMQ.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "acemq/amqp"
require "opentelemetry/sdk"

# Asserted against spans that were really emitted, through the SDK's in-memory
# exporter, rather than against doubles. A tracing adapter tested with doubles
# proves that it calls methods; it cannot prove that a span came out with the
# right name, the right kind, or — the one that matters — the right parent.
#
# The tracer provider is built here rather than taken from the global one, so
# these examples do not depend on, and do not disturb, whatever the process is
# configured with.
RSpec.describe AceMQ::AMQP::Telemetry::OpenTelemetry do
  let(:exporter) { OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new }

  let(:provider) do
    OpenTelemetry::SDK::Trace::TracerProvider.new.tap do |built|
      # Simple rather than batched: a batching processor exports on its own
      # schedule, and a test that waits for one is a test that is sometimes
      # flaky and always slower than it needed to be.
      built.add_span_processor(
        OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(exporter)
      )
    end
  end

  let(:propagator) { OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator }

  let(:tracing) do
    described_class.new(tracer_provider: provider, propagator: propagator,
                        transport: "rabbitmq")
  end

  let(:transport) { LoopbackTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@otel") }

  def spans = exporter.finished_spans

  def span_named(name) = spans.find { |span| span.name == name }

  describe "a publish" do
    before do
      tracing.install(mq)
      mq.publish({ "order_id" => "A-1" }, to: "order.placed", exchange: "orders",
                                          type: "order.placed.v2")
    end

    it "is a producer span named after its destination and the verb" do
      span = span_named("orders publish")

      expect(span).not_to be_nil
      expect(span.kind).to eq(:producer)
    end

    it "carries the messaging attributes every AceMQ library writes" do
      attributes = span_named("orders publish").attributes

      expect(attributes["messaging.system"]).to eq("rabbitmq")
      expect(attributes["messaging.destination.name"]).to eq("orders")
      expect(attributes["messaging.operation"]).to eq("publish")
      expect(attributes["messaging.rabbitmq.destination.routing_key"]).to eq("order.placed")
      expect(attributes["messaging.acemq.message_type"]).to eq("order.placed.v2")
      expect(attributes["messaging.message.id"]).not_to be_empty
      expect(attributes["messaging.message.conversation_id"]).not_to be_empty
    end

    it "records that the broker took it" do
      expect(span_named("orders publish").attributes["messaging.acemq.outcome"])
        .to eq("confirmed")
      expect(span_named("orders publish").status.code)
        .to eq(OpenTelemetry::Trace::Status::UNSET)
    end

    # The destination of a publish through the default exchange is the queue,
    # because there is no exchange to name and "" publish reads as a bug.
    it "names the queue when there is no exchange" do
      mq.publish({ "a" => 1 }, to: "orders.new")

      expect(span_named("orders.new publish")).not_to be_nil
    end
  end

  describe "the trace on the wire" do
    before { tracing.install(mq) }

    # The W3C name, deliberately not x-acemq- prefixed: other tooling already
    # knows it, and a private name would make these traces invisible to
    # everything that did not know to look for ours.
    it "writes traceparent, and not a prefixed name of our own" do
      mq.publish({ "a" => 1 }, to: "orders.new")
      headers = transport.published.last.headers

      expect(headers).to have_key("traceparent")
      expect(headers["traceparent"]).to start_with("00-")
      expect(headers.keys.grep(/trace/)).not_to include("x-acemq-traceparent")
    end

    it "writes the trace the publish span belongs to, not some other one" do
      mq.publish({ "a" => 1 }, to: "orders.new")
      span = span_named("orders.new publish")

      expect(transport.published.last.headers["traceparent"])
        .to eq("00-#{span.hex_trace_id}-#{span.hex_span_id}-01")
    end

    it "hands the current context out as headers for anything publishing elsewhere" do
      scope = tracing.request_started(destination: "pricing")
      headers = tracing.propagation_headers
      scope.close

      expect(headers["traceparent"]).to include(span_named("pricing request").hex_trace_id)
    end
  end

  describe "a delivery" do
    let(:consumer) do
      AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new", codec: AceMQ::AMQP::JSONCodec.new,
        retry_policy: AceMQ::AMQP::RetryPolicy.none, interceptors: interceptors,
        handler: handler
      )
    end

    let(:interceptors) { AceMQ::AMQP::Interceptors.new.add_consume(tracing) }
    let(:handler) { ->(_message) { AceMQ::AMQP::Ack.accept } }

    # A delivery carrying a trace that was started, and finished, somewhere
    # else. Built rather than published so that nothing in this process is
    # ambiently inside it — which is the only way to show that the parent came
    # out of the message rather than off this thread.
    def delivery(headers: {}, body: '{"a":1}')
      Struct.new(:body, :headers, :routing_key, :content_type, :redelivered, :reply_to, :acked,
                 keyword_init: true) do
        def redelivered? = !!redelivered
        def ack = self.acked = true
      end.new(body: body, headers: headers, routing_key: "orders.new",
              content_type: "application/json", redelivered: false)
    end

    def elsewhere
      other = provider.tracer("elsewhere").start_span("something that already ended")
      carrier = {}
      propagator.inject(carrier, context: OpenTelemetry::Trace.context_with_span(other))
      other.finish
      carrier
    end

    it "is a consumer span named after the queue and the verb" do
      consumer.handle(delivery)
      span = span_named("orders.new process")

      expect(span.kind).to eq(:consumer)
      expect(span.attributes["messaging.operation"]).to eq("process")
      expect(span.attributes["messaging.destination.name"]).to eq("orders.new")
      expect(span.attributes["messaging.acemq.attempt"]).to eq(1)
    end

    # The whole exercise. The publish happened in another process and possibly
    # minutes ago; nothing on this thread remembers it, and the message is the
    # only thing that does.
    it "is a child of the publish that caused it, read out of the message's own headers" do
      carrier = elsewhere
      consumer.handle(delivery(headers: carrier))

      published = spans.find { |span| span.name == "something that already ended" }
      process = span_named("orders.new process")

      expect(process.hex_trace_id).to eq(published.hex_trace_id)
      expect(process.hex_parent_span_id).to eq(published.hex_span_id)
    end

    # And the message wins over what this thread happens to be doing, which is
    # the difference between tracing a message system and tracing a method call.
    it "prefers the message's trace to the ambient one" do
      carrier = elsewhere
      ambient = provider.tracer("ambient").start_span("a thread doing something else")
      OpenTelemetry::Trace.with_span(ambient) { consumer.handle(delivery(headers: carrier)) }
      ambient.finish

      published = spans.find { |span| span.name == "something that already ended" }
      expect(span_named("orders.new process").hex_trace_id).to eq(published.hex_trace_id)
      expect(span_named("orders.new process").hex_trace_id)
        .not_to eq(ambient.context.hex_trace_id)
    end

    it "starts a trace of its own when the message carries none" do
      consumer.handle(delivery)

      expect(span_named("orders.new process").hex_parent_span_id).to eq("0" * 16)
    end
  end

  describe "outcomes" do
    let(:interceptors) { AceMQ::AMQP::Interceptors.new.add_consume(tracing) }

    # Five attempts and a wait of fifty milliseconds, which is short enough that
    # the consumer keeps it rather than sending the message round a rung — so a
    # message handed this really is retried, and the wait costs the suite
    # nothing worth measuring.
    let(:retrying) { AceMQ::AMQP::RetryPolicy.fixed(5, 0.05) }

    def handled(retry_policy: AceMQ::AMQP::RetryPolicy.none, &handler)
      AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new", codec: AceMQ::AMQP::JSONCodec.new,
        retry_policy: retry_policy, interceptors: interceptors,
        handler: handler
      ).handle(
        Struct.new(:body, :headers, :routing_key, :content_type, :redelivered,
                   :reply_to, keyword_init: true) do
          def redelivered? = false
          def ack = nil
        end.new(body: '{"a":1}', headers: {}, routing_key: "orders.new",
                content_type: "application/json")
      )
      span_named("orders.new process")
    end

    it "calls an accepted message acked, and does not call it an error" do
      span = handled { |_m| AceMQ::AMQP::Ack.accept }

      expect(span.attributes["messaging.acemq.outcome"]).to eq("acked")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::UNSET)
    end

    # A message that will be tried again has not failed yet. Marking it as an
    # error is how a trace view fills with red and stops meaning anything.
    it "calls a retry retried, and does not call it an error either" do
      span = handled(retry_policy: retrying) { |_m| AceMQ::AMQP::Ack.retry("no stock") }

      expect(span.attributes["messaging.acemq.outcome"]).to eq("retried")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::UNSET)
      expect(span.events.map(&:name)).to include("message.retried")
    end

    # The delay is not in the ack. It is chosen by the retry policy while the
    # delivery is being settled, and an event raised before that carries no
    # delay at all — which is a retry event nobody can do anything with.
    it "writes the delay the retry policy really chose onto the retry event" do
      span = handled(retry_policy: retrying) { |_m| AceMQ::AMQP::Ack.retry("no stock") }
      event = span.events.find { |one| one.name == "message.retried" }

      expect(event.attributes["messaging.acemq.retry_delay_ms"]).to eq(50)
      expect(event.attributes["messaging.acemq.attempt"]).to eq(1)
      expect(event.attributes["messaging.destination.name"]).to eq("orders.new")
    end

    # The one this change is for. The handler asked for a retry, the policy had
    # no attempts left, and the consumer dead-lettered the message — while the
    # span said "retried" and no dead-letter event was ever raised, so a query
    # for dead letters in a trace backend found nothing at all.
    it "calls a retry with no attempts left dead_lettered rather than retried" do
      span = handled { |_m| AceMQ::AMQP::Ack.retry("the warehouse said no") }
      event = span.events.find { |one| one.name == "message.dead_lettered" }

      expect(span.attributes["messaging.acemq.outcome"]).to eq("dead_lettered")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
      expect(span.events.map(&:name)).not_to include("message.retried")
      expect(event.attributes["messaging.acemq.reason"])
        .to eq("gave up after 1 attempts: the warehouse said no")
      expect(event.attributes["messaging.acemq.attempt"]).to eq(1)
    end

    it "calls a rejection rejected, which is the system working" do
      span = handled { |_m| AceMQ::AMQP::Ack.reject("not ours") }

      expect(span.attributes["messaging.acemq.outcome"]).to eq("rejected")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::UNSET)
    end

    # A rejected message goes to the dead-letter queue like any other, so the
    # event is raised for it too — with the same sentence that is written onto
    # the message, which is what lets somebody draining the queue find the
    # trace. Only the word on the span keeps the two apart.
    it "raises the dead-letter event for a rejection, with the reason on the message" do
      span = handled { |_m| AceMQ::AMQP::Ack.reject("not ours") }
      event = span.events.find { |one| one.name == "message.dead_lettered" }

      expect(event.attributes["messaging.acemq.reason"])
        .to eq("rejected by the handler: not ours")
    end

    # A retry marked fatal is a dead letter, because that is what the consumer
    # will do with it: honouring the mark rather than the request is the point
    # of having it.
    it "calls a fatal retry dead_lettered, and does call that an error" do
      span = handled { |_m| AceMQ::AMQP::Ack.retry(AceMQ::AMQP::FatalError.new("no schema")) }

      expect(span.attributes["messaging.acemq.outcome"]).to eq("dead_lettered")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
      expect(span.events.map(&:name)).to include("message.dead_lettered")
    end

    it "records the exception a handler raised" do
      span = handled { |_m| raise "the database is down" }

      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
      expect(span.events.map(&:name)).to include("exception")
    end

    # A handler that knows the message cannot be read says so, and the message
    # goes to the parking queue rather than into the dead letters with the ones
    # that merely failed.
    it "calls a parked message parked, and does call that an error" do
      span = handled { |_m| AceMQ::AMQP::Ack.park("the schema version is unknown here") }

      expect(span.attributes["messaging.acemq.outcome"]).to eq("parked")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
    end

    it "calls a publish nobody would take failed, and does call that an error" do
      failing = AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@otel")
      tracing.install(failing)
      failing.intercept_publish { |_context| raise "no" }

      expect { failing.publish({ "a" => 1 }, to: "orders.new") }.to raise_error("no")
      expect(span_named("orders.new publish").attributes["messaging.acemq.outcome"])
        .to eq("failed")
      expect(span_named("orders.new publish").status.code)
        .to eq(OpenTelemetry::Trace::Status::ERROR)
    end
  end

  # The property the counters and the spans are meant to have: for one
  # delivery, the counter that goes up and the word on its span are the same
  # decision, said twice. They are both read off the {Settlement} the consumer
  # produced, which is what makes it true; these examples are what keeps it
  # true.
  #
  # The pairs are written out here rather than taken from the constant the
  # consumer maps with, because a test that reuses the map cannot fail when the
  # map is wrong.
  describe "the counter and the span, for one delivery" do
    let(:interceptors) { AceMQ::AMQP::Interceptors.new.add_consume(tracing) }
    let(:metrics) { AceMQ::AMQP::Telemetry::Registry.new }
    let(:retrying) { AceMQ::AMQP::RetryPolicy.fixed(5, 0.05) }
    let(:giving_up) { AceMQ::AMQP::RetryPolicy.none }

    # Every counter a delivery could land on, so "exactly one went up" is a
    # thing an example can assert rather than a thing it has to trust.
    OUTCOME_COUNTERS = ["acemq.messages.accepted", "acemq.messages.retried",
                        "acemq.messages.rejected", "acemq.messages.dead.lettered",
                        "acemq.messages.parked"].freeze

    def counted
      OUTCOME_COUNTERS.to_h { |name| [name, metrics[name, queue: "orders.new"]] }
                      .reject { |_name, value| value.zero? }
    end

    def deliver(retry_policy, &handler)
      AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new", codec: AceMQ::AMQP::JSONCodec.new,
        retry_policy: retry_policy, interceptors: interceptors, telemetry: metrics,
        handler: handler
      ).handle(
        Struct.new(:body, :headers, :routing_key, :content_type, :redelivered,
                   :reply_to, keyword_init: true) do
          def redelivered? = false
          def ack = nil
        end.new(body: '{"a":1}', headers: {}, routing_key: "orders.new",
                content_type: "application/json")
      )
      span_named("orders.new process").attributes["messaging.acemq.outcome"]
    end

    it "agrees on acked" do
      outcome = deliver(giving_up) { |_m| AceMQ::AMQP::Ack.accept }

      expect(outcome).to eq("acked")
      expect(counted).to eq("acemq.messages.accepted" => 1)
    end

    it "agrees on retried" do
      outcome = deliver(retrying) { |_m| AceMQ::AMQP::Ack.retry("no stock") }

      expect(outcome).to eq("retried")
      expect(counted).to eq("acemq.messages.retried" => 1)
    end

    it "agrees on rejected, and does not also count it as a dead letter" do
      outcome = deliver(giving_up) { |_m| AceMQ::AMQP::Ack.reject("not ours") }

      expect(outcome).to eq("rejected")
      expect(counted).to eq("acemq.messages.rejected" => 1)
    end

    # The one this change is for. The handler asked for a retry and had no
    # attempt left to spend, so the message is dead-lettered — and used to be
    # counted as retried on the way there, which is why a dashboard's retry rate
    # and its dead-letter rate never added up.
    it "agrees on dead_lettered, and does not also count it as a retry" do
      outcome = deliver(giving_up) { |_m| AceMQ::AMQP::Ack.retry("the warehouse said no") }

      expect(outcome).to eq("dead_lettered")
      expect(counted).to eq("acemq.messages.dead.lettered" => 1)
    end

    # Parking has its own counter and its own word, and a handler that asks for
    # it must land on both — not on the dead-letter counter with the messages
    # that were tried and failed.
    it "agrees on parked, and does not also count it as a dead letter" do
      outcome = deliver(retrying) { |_m| AceMQ::AMQP::Ack.park("nothing here reads version 9") }

      expect(outcome).to eq("parked")
      expect(counted).to eq("acemq.messages.parked" => 1)
    end
  end

  # A failure has to say what became of the operation in the vocabulary every
  # other signal uses. The exception and the error status are not enough on
  # their own: a span that said nothing where the counter said failed is a
  # dashboard full of failures and a trace backend, asked for
  # messaging.acemq.outcome = "failed", that finds none of the spans behind
  # them. Java was fixed for exactly this; Python already did it.
  describe "the outcome a failure carries" do
    it "names it failed when nothing else named an outcome" do
      scope = tracing.consume_started(queue: "orders.new", envelope: AceMQ::AMQP::Envelope.new)
      scope.failed(RuntimeError.new("the database is down"))
      scope.close

      span = span_named("orders.new process")
      expect(span.attributes["messaging.acemq.outcome"]).to eq("failed")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
    end

    # A caller who named an outcome knows more than "it threw". timed_out is the
    # one that matters: a request nobody answered in time is the absence of a
    # reply, and calling it failed would lose that.
    it "leaves an outcome that was named explicitly alone" do
      scope = tracing.request_started(destination: "pricing.quote")
      scope.outcome("timed_out")
      scope.failed(RuntimeError.new("no reply within 5 seconds"))
      scope.close

      expect(span_named("pricing.quote request").attributes["messaging.acemq.outcome"])
        .to eq("timed_out")
    end

    it "still records the exception and the error status either way" do
      scope = tracing.request_started(destination: "pricing.quote")
      scope.outcome("timed_out")
      scope.failed(RuntimeError.new("no reply within 5 seconds"))
      scope.close

      span = span_named("pricing.quote request")
      expect(span.events.map(&:name)).to include("exception")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
    end
  end

  describe "a request" do
    # CLIENT rather than PRODUCER: this span waits for an answer, so its
    # duration includes somebody else's work, and a reader who cannot tell the
    # two apart cannot tell a slow broker from a slow responder.
    it "is a client span named after its destination and the verb" do
      tracing.request("pricing.quote") { :a_quote }
      span = span_named("pricing.quote request")

      expect(span.kind).to eq(:client)
      expect(span.attributes["messaging.operation"]).to eq("request")
      expect(span.attributes["messaging.acemq.outcome"]).to eq("answered")
    end

    it "gives back what the block returned" do
      expect(tracing.request("pricing.quote") { :a_quote }).to eq(:a_quote)
    end

    it "records a failure and lets it out" do
      expect { tracing.request("pricing.quote") { raise "no route to the broker" } }
        .to raise_error("no route to the broker")
      span = span_named("pricing.quote request")

      expect(span.attributes["messaging.acemq.outcome"]).to eq("failed")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
    end

    # A request nobody answered in time is the absence of a reply rather than a
    # failure of this process — usually a responder's queue being long. Java and
    # Go both name it and neither marks the span red; a trace view that fills
    # with red for slow responders stops meaning anything.
    it "calls an unanswered request timed_out, and does not call it an error" do
      expect do
        tracing.request("pricing.quote") do
          raise AceMQ::AMQP::Patterns::RequestTimedOut, "no reply within 5 seconds"
        end
      end.to raise_error(AceMQ::AMQP::Patterns::RequestTimedOut)
      span = span_named("pricing.quote request")

      expect(span.attributes["messaging.acemq.outcome"]).to eq("timed_out")
      expect(span.status.code).to eq(OpenTelemetry::Trace::Status::UNSET)
    end

    # The publish inside a request is a child of it, so one trace covers the
    # question and the sending of it rather than two unrelated hops.
    it "makes a publish inside it a child" do
      tracing.install(mq)
      tracing.request("pricing.quote") { mq.publish({ "a" => 1 }, to: "pricing.quote") }

      expect(span_named("pricing.quote publish").hex_parent_span_id)
        .to eq(span_named("pricing.quote request").hex_span_id)
    end
  end

  describe "events rather than spans" do
    # A zero-length span at the end of a trace adds a row and no information.
    it "puts an outbox failure and a finished pipeline run on the span already open" do
      scope = tracing.consume_started(queue: "orders.new", envelope: AceMQ::AMQP::Envelope.new)
      tracing.outbox_publish_failed(exchange: "orders", reason: "the broker refused it")
      tracing.pipeline_run_finished(pipeline: "fulfilment", step: "pick", outcome: "completed",
                                    age: 1.5)
      scope.close

      events = span_named("orders.new process").events
      expect(events.map(&:name))
        .to contain_exactly("outbox.publish_failed", "pipeline.run_finished")
      finished = events.find { |event| event.name == "pipeline.run_finished" }
      expect(finished.attributes["messaging.acemq.run_age_ms"]).to eq(1500)
      expect(finished.attributes["step"]).to eq("pick")
    end

    # An outbox relay on its own thread has no span to hang an event on, and
    # opening one for the event alone would produce exactly the zero-length span
    # this avoids.
    it "says nothing at all when no span is open" do
      tracing.outbox_publish_failed(exchange: "orders", reason: "the broker refused it")

      expect(spans).to be_empty
    end

    # An attribute rather than an event, and the same one the Java, Go and
    # Python adapters write: the lag measures the publish that is happening, not
    # something that happened during it.
    it "puts the outbox lag on the span as an attribute" do
      scope = tracing.publish_started(exchange: "orders", routing_key: "order.placed",
                                      envelope: AceMQ::AMQP::Envelope.new)
      tracing.outbox_published(lag: 2.5)
      scope.close

      span = span_named("orders publish")
      expect(span.attributes["messaging.acemq.outbox_lag_ms"]).to eq(2500)
      # An attribute and not an event: a span with no events at all is what the
      # SDK reports as nil here, and that is the point being made.
      expect(span.events.to_a).to be_empty
    end

    it "records no lag when no span is open" do
      tracing.outbox_published(lag: 2.5)

      expect(spans).to be_empty
    end
  end

  describe "the gem it needs" do
    it "names it, and the Gemfile line, when it is not installed" do
      # The gemspec declares no runtime dependencies on purpose, so this is the
      # first moment anybody finds out. "cannot load such file --
      # opentelemetry-api" does not say which library wanted it.
      allow(described_class).to receive(:require).with("opentelemetry-api")
                                                 .and_raise(LoadError, "no opentelemetry")

      expect { described_class.load_api! }
        .to raise_error(AceMQ::AMQP::DependencyMissing, /gem "opentelemetry-api"/)
    end
  end

  describe "the shape of the thing" do
    it "runs before every other interceptor on the way in, so a span covers them" do
      expect(tracing.order).to be < 0
    end

    it "leaves nothing current after a span is closed" do
      before = OpenTelemetry::Trace.current_span
      tracing.request("pricing.quote") { :ok }

      expect(OpenTelemetry::Trace.current_span).to eq(before)
    end

    it "closes a scope only once" do
      scope = tracing.request_started(destination: "pricing")
      scope.close
      scope.close

      expect(scope).to be_closed
      expect(spans.size).to eq(1)
    end

    it "leaves an attribute out rather than writing an empty one" do
      envelope = AceMQ::AMQP::Envelope.new(type: "")
      scope = tracing.publish_started(exchange: "", routing_key: "orders.new",
                                      envelope: envelope)
      scope.close

      expect(span_named("orders.new publish").attributes)
        .not_to have_key("messaging.acemq.message_type")
    end
  end
end
