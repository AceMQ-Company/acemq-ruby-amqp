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

require "acemq/amqp/patterns"

RSpec.describe AceMQ::AMQP::Patterns::RoutingSlip do
  let(:transport) { LoopbackTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }

  def itinerary
    described_class.new
                   .step("", "validate", name: "validate")
                   .step("", "charge", name: "charge")
                   .step("", "ship", name: "ship")
  end

  # A message carrying a JSON slip, as one arrives at a step.
  def carrying(slip, key)
    AceMQ::AMQP::Message.new(
      payload: {}, routing_key: key, content_type: "application/json",
      redelivered: false, body: "{}",
      envelope: AceMQ::AMQP::Envelope.new(
        headers: { AceMQ::AMQP::Patterns::SLIP_HEADER => slip.to_header }
      )
    )
  end

  describe "the itinerary itself" do
    it "knows where the message is going next" do
      expect(itinerary.next_step.routing_key).to eq("validate")
      expect(itinerary).not_to be_finished
    end

    it "moves a step to done rather than dropping it" do
      # A slip that fails half way has to say how far it got. That is exactly
      # what whoever finds the message in a dead-letter queue is asking.
      advanced = itinerary.advance

      expect(advanced.next_step.routing_key).to eq("charge")
      expect(advanced.done.map(&:to_s)).to eq(["validate"])
      expect(advanced.done.first.completed_at).to match(/\A\d{4}-\d\d-\d\dT/)
    end

    it "leaves the slip it advanced alone" do
      # A message's slip changing under a handler is a message nothing can
      # reason about.
      slip = itinerary
      slip.advance

      expect(slip.next_step.routing_key).to eq("validate")
      expect(slip.done).to be_empty
    end

    it "is finished once every step is done" do
      finished = itinerary.advance.advance.advance

      expect(finished).to be_finished
      expect(finished.next_step).to be_nil
      expect(finished.advance).to be_finished
    end

    it "reads back exactly what it wrote" do
      slip = itinerary.advance
      envelope = AceMQ::AMQP::Envelope.new(
        headers: { AceMQ::AMQP::Patterns::SLIP_HEADER => slip.to_header }
      )
      read = described_class.from(envelope)

      expect(read.steps.map(&:to_s)).to eq(%w[charge ship])
      expect(read.done.map(&:to_s)).to eq(["validate"])
    end

    it "writes the keys the other languages read" do
      # A slip written by a Go service is read by a Ruby one, so the keys inside
      # it are the shared ones rather than anything more Rubyish.
      wire = JSON.parse(itinerary.to_header)

      expect(wire["steps"].first.keys).to contain_exactly("exchange", "routingKey", "name")
      expect(wire["steps"].first["routingKey"]).to eq("validate")
    end

    it "reads as something worth putting in a log" do
      expect(itinerary.advance.to_s)
        .to eq("RoutingSlip[done: validate | next: charge -> ship]")
    end

    it "is nothing at all when the message has no slip" do
      expect(described_class.from(AceMQ::AMQP::Envelope.new)).to be_nil
    end

    it "refuses a slip it cannot read, once and for all" do
      # It will not parse next time either, and a message going round the broker
      # while nothing can tell where it is meant to go is the worst of both.
      envelope = AceMQ::AMQP::Envelope.new(
        headers: { AceMQ::AMQP::Patterns::SLIP_HEADER => "{ not json" }
      )

      expect { described_class.from(envelope) }
        .to raise_error(AceMQ::AMQP::FatalError, /cannot read the routing slip/)
    end
  end

  describe "travelling" do
    it "sends the payload to the first stop, carrying the slip" do
      itinerary.start(mq, { "order_id" => "A-1" }, type: "order.placed.v2")

      sent = transport.published_to("validate").first
      expect(sent.body).to eq('{"order_id":"A-1"}')
      expect(sent.headers[AceMQ::AMQP::Headers::TYPE]).to eq("order.placed.v2")
      carried = JSON.parse(sent.headers[AceMQ::AMQP::Patterns::SLIP_HEADER])
      expect(carried["steps"].size).to eq(3)
    end

    it "refuses to start a slip with nothing on it" do
      expect { described_class.new.start(mq, {}) }
        .to raise_error(ArgumentError, /no steps in it/)
    end

    it "goes the whole route, one service at a time" do
      # Nothing here knows the route. Each step reads the slip off the message,
      # does its part and sends it on, which is the entire point.
      visited = []
      %w[validate charge ship].each do |stop|
        mq.consume(stop, &AceMQ::AMQP::Patterns.follow_slip(mq) do |message|
          visited << stop
          message.payload.merge(stop => true)
        end)
      end

      itinerary.start(mq, { "order_id" => "A-1" })

      expect(visited).to eq(%w[validate charge ship])
      final = transport.published_to("ship").first
      expect(JSON.parse(final.body)).to eq({ "order_id" => "A-1", "validate" => true,
                                             "charge" => true })
    end

    it "carries the correlation the whole way, and says what caused each hop" do
      %w[validate charge].each do |stop|
        mq.consume(stop, &AceMQ::AMQP::Patterns.follow_slip(mq, &:payload))
      end
      sent = itinerary.start(mq, { "order_id" => "A-1" })

      charge = transport.published_to("charge").first
      expect(charge.headers[AceMQ::AMQP::Headers::CORRELATION]).to eq(sent.correlation_id)
      expect(charge.headers[AceMQ::AMQP::Headers::CAUSATION])
        .to eq(transport.published_to("validate").first.headers[AceMQ::AMQP::Headers::ID])
    end

    it "publishes nothing more once the last step is done" do
      handler = AceMQ::AMQP::Patterns.follow_slip(mq, &:payload)
      last = described_class.new.step("", "ship", name: "ship")
      message = AceMQ::AMQP::Message.new(
        payload: {}, routing_key: "ship", content_type: "application/json",
        redelivered: false, body: "{}",
        envelope: AceMQ::AMQP::Envelope.new(
          headers: { AceMQ::AMQP::Patterns::SLIP_HEADER => last.to_header }
        )
      )

      expect(handler.call(message)).to be_accept
      expect(transport.published).to be_empty
    end

    it "dead-letters a message with no slip, because there is nowhere to send it" do
      mq.consume("charge", &AceMQ::AMQP::Patterns.follow_slip(mq, &:payload))
      mq.publish({ "order_id" => "A-1" }, to: "charge")

      dead = transport.published_to("charge.dlq")
      expect(dead.size).to eq(1)
      expect(dead.first.headers[AceMQ::AMQP::Headers::ERROR])
        .to match(/has no routing slip, so there is nowhere to send it next/)
    end

    it "retries the step rather than losing the work when the next hop will not go out" do
      # The step is done but the message did not move, so it runs again — which
      # is why a step that changes anything should be idempotent.
      handler = AceMQ::AMQP::Patterns.follow_slip(mq, &:payload)
      message = AceMQ::AMQP::Message.new(
        payload: {}, routing_key: "validate", content_type: "application/json",
        redelivered: false, body: "{}",
        envelope: AceMQ::AMQP::Envelope.new(
          headers: { AceMQ::AMQP::Patterns::SLIP_HEADER => itinerary.to_header }
        )
      )
      allow(transport).to receive(:publish).and_raise(AceMQ::AMQP::PublishError, "no confirm")

      ack = handler.call(message)
      expect(ack).to be_retry
      expect(ack.error).to match(/validate is done for message .* but the next step did not go/)
    end

    it "lets a step's own failure reach the retry engine" do
      handler = AceMQ::AMQP::Patterns.follow_slip(mq) { raise "the card was declined" }
      message = AceMQ::AMQP::Message.new(
        payload: {}, routing_key: "charge", content_type: "application/json",
        redelivered: false, body: "{}",
        envelope: AceMQ::AMQP::Envelope.new(
          headers: { AceMQ::AMQP::Patterns::SLIP_HEADER => itinerary.to_header }
        )
      )

      expect { handler.call(message) }.to raise_error(/the card was declined/)
    end

    it "ends the run where a step returns nothing, and publishes no more" do
      # A step that decides the message goes no further is making a decision,
      # not failing. The same rule Patterns.then_publish follows, and the one
      # Java's pipeline counts as ended_early.
      handler = AceMQ::AMQP::Patterns.follow_slip(mq) { nil }

      expect(handler.call(carrying(itinerary, "validate"))).to be_accept
      expect(transport.published).to be_empty
    end
  end

  # Java writes the itinerary as x-acemq-route: the ordered step names of a
  # declared Pipeline, a position, and a run identifier. A Ruby step could not
  # read one at all before this, so a Ruby service could not stand in a pipeline
  # a Java service declared.
  describe "a route Java wrote" do
    let(:pipeline) { AceMQ::AMQP::Patterns::Pipeline.new("orders", %w[validate charge ship]) }

    # Exactly what a Java Pipeline puts on the wire, built from headers rather
    # than from this library's own writer — a test that produced the message
    # with the code under test could not fail when the reader is wrong.
    def from_java(route: "validate,charge,ship", position: 1, run: "run-7", key: "charge")
      raw = { AceMQ::AMQP::Headers::ID => "J-1",
              AceMQ::AMQP::Headers::ROUTE => route,
              AceMQ::AMQP::Headers::ROUTE_POSITION => position,
              AceMQ::AMQP::Headers::ROUTE_ID => run }
      AceMQ::AMQP::Message.new(
        payload: { "order_id" => "A-1" }, routing_key: key, content_type: "application/json",
        redelivered: false, body: "{}",
        envelope: AceMQ::AMQP::Envelope.from_headers(raw, key)
      )
    end

    it "reads the steps and how far along the message is" do
      slip = described_class.from(from_java.envelope, pipeline: pipeline)

      expect(slip.form).to eq(described_class::ROUTE)
      expect(slip.all.map(&:name)).to eq(%w[validate charge ship])
      expect(slip.position).to eq(1)
      expect(slip.next_step.name).to eq("charge")
      expect(slip.run_id).to eq("run-7")
    end

    # The naming has to be Java's exactly or a Ruby hop publishes somewhere no
    # Java consumer is listening: the pipeline's own direct exchange, with the
    # step name as the routing key.
    it "sends the message on to where the next Java step is listening" do
      expect(pipeline.follow(mq, &:payload).call(from_java)).to be_accept

      sent = transport.published.last
      expect([sent.exchange, sent.routing_key]).to eq(%w[orders ship])
    end

    it "advances the position and keeps the run identifier" do
      pipeline.follow(mq, &:payload).call(from_java)

      headers = transport.published.last.headers
      expect(headers[AceMQ::AMQP::Headers::ROUTE]).to eq("validate,charge,ship")
      expect(headers[AceMQ::AMQP::Headers::ROUTE_POSITION]).to eq(2)
      # The whole point of the identifier: it is the same at every hop, so one
      # run can be followed across a dead-letter and a replay.
      expect(headers[AceMQ::AMQP::Headers::ROUTE_ID]).to eq("run-7")
    end

    it "hands it on in the shape it arrived in, and writes no JSON slip" do
      # What lets a Ruby step sit in the middle of a Java pipeline: the next
      # Java step reads what this one wrote.
      pipeline.follow(mq, &:payload).call(from_java)

      expect(transport.published.last.headers)
        .not_to have_key(AceMQ::AMQP::Patterns::SLIP_HEADER)
    end

    it "stops at the last step without publishing anything" do
      expect(pipeline.follow(mq, &:payload).call(from_java(position: 2, key: "ship")))
        .to be_accept
      expect(transport.published).to be_empty
    end

    # Java's rule, for Java's reason: sending the message to an arbitrary step
    # is worse than starting the route again.
    it "reads a position it cannot parse as the beginning" do
      slip = described_class.from(from_java(position: "halfway").envelope, pipeline: pipeline)

      expect(slip.position).to eq(0)
      expect(slip.next_step.name).to eq("validate")
    end

    it "mints a run identifier when the message carries none" do
      slip = described_class.from(from_java(run: nil).envelope, pipeline: pipeline)

      expect(slip.run_id).to match(/\A\h{8}-\h{4}-/)
    end

    it "is nil for a message carrying neither form" do
      expect(described_class.from(AceMQ::AMQP::Envelope.new, pipeline: pipeline)).to be_nil
    end

    # A message carrying both is written by nothing in this family, but a
    # gateway between two of them might. The JSON wins because it names its own
    # destinations and so is readable whatever this consumer has declared.
    it "prefers the JSON slip when a message somehow carries both" do
      envelope = AceMQ::AMQP::Envelope.from_headers(
        { AceMQ::AMQP::Headers::ROUTE => "validate,charge,ship",
          AceMQ::AMQP::Patterns::SLIP_HEADER => itinerary.to_header }, "validate"
      )

      expect(described_class.from(envelope, pipeline: pipeline).form)
        .to eq(described_class::SLIP)
    end
  end

  describe "a pipeline declared here" do
    let(:pipeline) { AceMQ::AMQP::Patterns::Pipeline.new("orders", %w[validate charge ship]) }

    it "names its exchange and queues the way Java does" do
      # A Ruby consumer that got any of these wrong would be listening where no
      # Java service publishes.
      expect(pipeline.exchange).to eq("orders")
      expect(pipeline.queue_for("charge")).to eq("orders.charge")
    end

    it "refuses a step it does not have, rather than a queue nothing fills" do
      expect { pipeline.queue_for("refund") }
        .to raise_error(ArgumentError, /has no step called 'refund'.*validate, charge, ship/m)
    end

    it "declares a direct exchange, a queue per step and a binding per step" do
      pipeline.topology.apply(mq)

      expect(transport.declared_exchanges.map { |name, options| [name, options[:kind]] })
        .to eq([%w[orders direct]])
      expect(transport.declared_queues.map(&:first))
        .to eq(%w[orders.validate orders.charge orders.ship])
      expect(transport.bindings)
        .to eq([%w[orders.validate orders validate], %w[orders.charge orders charge],
                %w[orders.ship orders ship]])
    end

    it "sends a message into the first step, in the declared form" do
      pipeline.start(mq, { "order_id" => "A-1" })

      sent = transport.published.last
      expect([sent.exchange, sent.routing_key]).to eq(%w[orders validate])
      expect(sent.headers[AceMQ::AMQP::Headers::ROUTE]).to eq("validate,charge,ship")
      expect(sent.headers[AceMQ::AMQP::Headers::ROUTE_POSITION]).to eq(0)
      expect(sent.headers).not_to have_key(AceMQ::AMQP::Patterns::SLIP_HEADER)
    end

    it "writes a JSON slip as the step-name form when asked to" do
      # The other direction: a route assembled here, handed to a pipeline the
      # rest of which is declared elsewhere.
      handler = AceMQ::AMQP::Patterns.follow_slip(
        mq, pipeline: pipeline, write: described_class::ROUTE, &:payload
      )

      expect(handler.call(carrying(itinerary, "validate"))).to be_accept
      headers = transport.published.last.headers
      expect(headers[AceMQ::AMQP::Headers::ROUTE]).to eq("validate,charge,ship")
      expect(headers).not_to have_key(AceMQ::AMQP::Patterns::SLIP_HEADER)
    end

    it "refuses to write a route with no pipeline to resolve it against" do
      expect { itinerary.as(described_class::ROUTE) }
        .to raise_error(ArgumentError, /needs the pipeline its steps belong to/)
    end
  end

  # The seam that was documented as not worth opening while there was no object
  # owning a pipeline's name. There is one now, so it costs a name and a step
  # rather than an invention.
  describe "what a pipeline run reports" do
    let(:metrics) { AceMQ::AMQP::Telemetry::Registry.new }
    let(:mq) do
      AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here",
                                  telemetry: metrics)
    end
    let(:pipeline) { AceMQ::AMQP::Patterns::Pipeline.new("orders", %w[validate charge ship]) }

    def counted(outcome, step)
      metrics[AceMQ::AMQP::Telemetry::PIPELINE_RUN_TOTAL,
              pipeline: "orders", step: step, outcome: outcome]
    end

    def route(position:, key:)
      AceMQ::AMQP::Message.new(
        payload: { "order_id" => "A-1" }, routing_key: key, content_type: "application/json",
        redelivered: false, body: "{}",
        envelope: AceMQ::AMQP::Envelope.from_headers(
          { AceMQ::AMQP::Headers::ID => "J-1",
            AceMQ::AMQP::Headers::ROUTE => "validate,charge,ship",
            AceMQ::AMQP::Headers::ROUTE_POSITION => position }, key
        )
      )
    end

    it "counts a run that reached the end of its route" do
      pipeline.follow(mq, &:payload).call(route(position: 2, key: "ship"))

      expect(counted("completed", "ship")).to eq(1)
      timing = metrics.timings[AceMQ::AMQP::Telemetry::Registry.key(
        AceMQ::AMQP::Telemetry::PIPELINE_RUN_DURATION, pipeline: "orders"
      )]
      expect(timing.count).to eq(1)
    end

    it "counts a run a step ended early apart from one that finished" do
      pipeline.follow(mq) { nil }.call(route(position: 1, key: "charge"))

      expect(counted("ended_early", "charge")).to eq(1)
      expect(counted("completed", "charge")).to eq(0)
    end

    it "counts nothing for a bare slip, which has no pipeline to name" do
      # An itinerary assembled per message is not a thing with an identity to
      # put on a dashboard, and a metric tagged with an empty name is worse
      # than no metric.
      AceMQ::AMQP::Patterns.follow_slip(mq, &:payload)
                           .call(carrying(described_class.new.step("", "ship", name: "ship"),
                                          "ship"))

      expect(metrics.counts.keys.grep(/pipeline/)).to be_empty
    end
  end
end
