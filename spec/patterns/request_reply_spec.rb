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

RSpec.describe AceMQ::AMQP::Patterns::Requester do
  let(:transport) { LoopbackTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }

  after { mq.close }

  def serving(&handler)
    AceMQ::AMQP::Patterns.serve(mq, "price.requests", &handler)
  end

  it "carries a question to a responder and the answer back" do
    serving { |message| { "price" => message.payload["sku"].length * 100 } }
    requester = described_class.new(mq, to: "price.requests")

    expect(requester.call({ "sku" => "X-12" })).to eq({ "price" => 400 })
    requester.close
  end

  it "pairs a reply with its request by correlation, not by arrival" do
    # Two requests in flight, answered out of order. Only the correlation
    # identifier says which answer belongs to which caller.
    held = []
    serving do |message|
      held << message
      { "for" => message.payload["sku"] }
    end
    requester = described_class.new(mq, to: "price.requests")

    requester.call({ "sku" => "A" })
    requester.call({ "sku" => "B" })

    correlations = held.map { |m| m.envelope.correlation_id }
    expect(correlations.uniq.size).to eq(2)
    requester.close
  end

  it "tells the responder where to reply, in a header a handler can read" do
    # An application header, because it travels through the same envelope
    # machinery as everything else and survives a hop through a service that
    # rebuilds the message.
    seen = nil
    serving do |message|
      seen = message.envelope.headers[AceMQ::AMQP::Patterns::REPLY_TO_HEADER]
      {}
    end
    requester = described_class.new(mq, to: "price.requests", reply_to: "replies")
    requester.call({ "sku" => "A" })

    expect(seen).to eq("replies")
    requester.close
  end

  it "writes the reply address twice: the header and the AMQP property" do
    # Both, and the same name in each. Java and .NET responders read the
    # property; Go, Python and Ruby responders read the header. A requester that
    # wrote only one of them could talk to half the family.
    serving { |_message| { "ok" => true } }
    requester = described_class.new(mq, to: "price.requests", reply_to: "replies")
    requester.call({ "sku" => "A" })

    request = transport.published_to("price.requests").first
    expect(request.headers[AceMQ::AMQP::Patterns::REPLY_TO_HEADER]).to eq("replies")
    expect(request.reply_to).to eq("replies")
    requester.close
  end

  describe "a request from another library" do
    # The two halves of the interop rule, each on its own. Neither request is
    # one this library would produce — a requester here writes both — and each
    # is exactly what one of the other four sends today.

    it "answers one carrying only the AMQP reply-to property" do
      # What a Java or a .NET requester sends: the native property and no
      # acemq-reply-to header anywhere on the message.
      serving { |message| { "price" => message.payload["sku"].length * 100 } }
      mq.publish({ "sku" => "X-12" }, to: "price.requests",
                                      reply_to: "replies", correlation_id: "from-java")

      reply = transport.published_to("replies").first
      expect(reply).not_to be_nil
      expect(JSON.parse(reply.body)).to eq({ "price" => 400 })
      expect(reply.headers[AceMQ::AMQP::Headers::CORRELATION]).to eq("from-java")
    end

    it "answers one carrying only the acemq-reply-to header" do
      # What a Go, a Python or an older Ruby requester sends: the header and no
      # reply-to property.
      serving { |message| { "price" => message.payload["sku"].length * 100 } }
      mq.publish({ "sku" => "X-12" }, to: "price.requests", correlation_id: "from-go",
                                      headers: {
                                        AceMQ::AMQP::Patterns::REPLY_TO_HEADER => "replies"
                                      })

      reply = transport.published_to("replies").first
      expect(reply).not_to be_nil
      expect(JSON.parse(reply.body)).to eq({ "price" => 400 })
      expect(reply.headers[AceMQ::AMQP::Headers::CORRELATION]).to eq("from-go")
    end

    it "prefers the header when a message carries both and they disagree" do
      # They never disagree on a message this library produced. The order is
      # written down anyway, and it is the same order in all five: the header
      # is the one that survives a service that rebuilds the message.
      serving { |_message| { "ok" => true } }
      mq.publish({ "sku" => "A" }, to: "price.requests", reply_to: "the-property",
                                   headers: {
                                     AceMQ::AMQP::Patterns::REPLY_TO_HEADER => "the-header"
                                   })

      expect(transport.published_to("the-header").size).to eq(1)
      expect(transport.published_to("the-property")).to be_empty
    end
  end

  it "marks the reply as caused by the request" do
    serving { |_message| { "ok" => true } }
    requester = described_class.new(mq, to: "price.requests", reply_to: "replies")
    requester.call({ "sku" => "A" })

    reply = transport.published_to("replies").first
    request = transport.published_to("price.requests").first
    expect(reply.headers[AceMQ::AMQP::Headers::CAUSATION])
      .to eq(request.headers[AceMQ::AMQP::Headers::ID])
    expect(reply.headers[AceMQ::AMQP::Headers::CORRELATION])
      .to eq(request.headers[AceMQ::AMQP::Headers::CORRELATION])
    requester.close
  end

  it "sends a responder's failure back rather than leaving the caller to time out" do
    # A caller blocked on a reply should learn that it failed. Waiting out a
    # thirty-second timeout for an answer that was decided immediately is the
    # worst outcome available.
    serving { |_message| raise "the catalogue is down" }
    requester = described_class.new(mq, to: "price.requests")

    expect { requester.call({ "sku" => "A" }) }
      .to raise_error(AceMQ::AMQP::Patterns::ResponderFailed, /the catalogue is down/)
    requester.close
  end

  it "settles a failed request rather than retrying it, having already answered" do
    # Replying and then retrying would answer twice.
    serving { |_message| raise "the catalogue is down" }
    requester = described_class.new(mq, to: "price.requests")
    begin
      requester.call({ "sku" => "A" })
    rescue AceMQ::AMQP::Patterns::ResponderFailed
      nil
    end

    expect(transport.published_to("price.requests").size).to eq(1)
    expect(transport.published_to("price.requests.dlq").size).to eq(1)
    requester.close
  end

  it "gives up on a reply that never comes, and says so" do
    # Nothing is serving the queue. A timeout says an answer did not arrive; it
    # says nothing about whether the work was done, which is why a request that
    # changes anything wants an idempotent responder.
    requester = described_class.new(mq, to: "price.requests", timeout: 0.05)

    expect { requester.call({ "sku" => "A" }) }
      .to raise_error(AceMQ::AMQP::Patterns::RequestTimedOut, /within 0.05 seconds/)
    requester.close
  end

  it "drops a reply nobody is waiting for any more" do
    # A reply to a request that already timed out. Blocking or raising here
    # would stall the reply consumer for everybody else on the same queue.
    requester = described_class.new(mq, to: "price.requests", reply_to: "replies",
                                        timeout: 0.01)
    begin
      requester.call({ "sku" => "A" })
    rescue AceMQ::AMQP::Patterns::RequestTimedOut
      nil
    end

    expect { mq.publish({ "late" => true }, to: "replies", correlation_id: "nobody") }
      .not_to raise_error
    requester.close
  end

  it "gives a request with nowhere to reply to the dead-letter queue" do
    # Retrying cannot make a reply queue appear. Neither the header nor the
    # property is there, which is the only case with no answer available.
    serving { |_message| { "ok" => true } }
    mq.publish({ "sku" => "A" }, to: "price.requests")

    dead = transport.published_to("price.requests.dlq")
    expect(dead.size).to eq(1)
    expect(dead.first.headers[AceMQ::AMQP::Headers::ERROR])
      .to match(/neither the acemq-reply-to header nor a reply-to property/)
  end

  it "generates an exclusive reply queue that goes away with the requester" do
    # A reply queue that outlived its requester would collect answers nobody is
    # waiting for.
    requester = described_class.new(mq, to: "price.requests")

    expect(requester.reply_queue).to start_with("acemq-reply-")
    declared = transport.declared_queues.find { |name, _| name == requester.reply_queue }
    expect(declared.last).to include(exclusive: true, auto_delete: true, durable: false)
    requester.close
  end

  describe "what a responder counts" do
    it "has both numbers before it has been asked anything" do
      responder = serving { |_message| { "ok" => true } }

      expect(responder.answered).to eq(0)
      expect(responder.unanswerable).to eq(0)
    end

    it "counts a request it answered" do
      responder = serving { |message| { "price" => message.payload["sku"].length } }
      requester = described_class.new(mq, to: "price.requests")
      requester.call({ "sku" => "X-12" })

      expect(responder.answered).to eq(1)
      expect(responder.unanswerable).to eq(0)
      requester.close
    end

    it "counts the answer before the reply is published, not after" do
      # The ordering is the whole promise. Counting afterwards leaves a window
      # in which the reply is already in the caller's hands and the responder
      # still says nothing has been answered — a dashboard reporting an idle
      # service that is demonstrably working. This reads the number from inside
      # the publish, which is precisely that window.
      responder = serving { |_message| { "ok" => true } }
      counted_mid_publish = nil
      mq.intercept_publish do |context|
        counted_mid_publish = responder.answered if context.routing_key == "replies"
      end

      requester = described_class.new(mq, to: "price.requests", reply_to: "replies")
      requester.call({ "sku" => "A" })

      expect(counted_mid_publish).to eq(1)
      expect(responder.answered).to eq(1)
      requester.close
    end

    it "hands the increment back when the reply could not be published" do
      # Counting early would otherwise buy a failure of its own: a reply that
      # never went out counted as an answer. It counts replies that were sent.
      responder = serving { |_message| { "ok" => true } }
      mq.intercept_publish do |context|
        next unless context.routing_key == "replies"

        raise AceMQ::AMQP::TransportError, "the broker went away"
      end

      mq.publish({ "sku" => "A" }, to: "price.requests", reply_to: "replies")

      expect(responder.answered).to eq(0)
    end

    it "counts a request that named nowhere to reply" do
      responder = serving { |_message| { "ok" => true } }
      mq.publish({ "sku" => "A" }, to: "price.requests")

      expect(responder.unanswerable).to eq(1)
      expect(responder.answered).to eq(0)
    end

    it "counts a request handed over during start-up like any other" do
      # A queue with a backlog, from in here: the broker hands the first request
      # over from inside the subscribe, before anything has been handed back to
      # the caller. Counters reached through the responder would still be nil
      # there, and the first number of the day would be lost in silence.
      mq.publish({ "sku" => "A" }, to: "price.requests", reply_to: "replies")
      responder = serving { |_message| { "ok" => true } }

      expect(responder.answered).to eq(1)
      expect(transport.published_to("replies").size).to eq(1)
    end

    it "counts a failure it answered with, which is still an answer here" do
      # Where Ruby diverges from Java and says so: a responder that raises sends
      # the failure back rather than letting the caller wait out its deadline,
      # and a reply that was sent is a request that was answered. What happened
      # to the request is a different question, and acemq.consume.total answers
      # it — this one is rejected rather than acked.
      responder = serving { |_message| raise "the catalogue is down" }
      requester = described_class.new(mq, to: "price.requests")
      begin
        requester.call({ "sku" => "A" })
      rescue AceMQ::AMQP::Patterns::ResponderFailed
        nil
      end

      expect(responder.answered).to eq(1)
      requester.close
    end

    it "is still the consumer underneath, for everything it does not count" do
      responder = serving { |_message| { "ok" => true } }

      expect(responder.queue).to eq("price.requests")
      expect(responder.running?).to be(true)
      expect(responder.consumer).to be_a(AceMQ::AMQP::Consumer)
      responder.cancel
      expect(responder.running?).to be(false)
    end
  end

  describe "what a requester counts" do
    let(:metrics) { AceMQ::AMQP::Telemetry::Registry.new }
    let(:mq) do
      AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here",
                                  telemetry: metrics)
    end

    # The labels Java and .NET tag the same two series with, dots and all.
    def tags(outcome, type: "price.request")
      { AceMQ::AMQP::Telemetry::TAG_ROUTING_KEY => "price.requests",
        AceMQ::AMQP::Telemetry::TAG_MESSAGE_TYPE => type, outcome: outcome }
    end

    def timing(outcome, type: "price.request")
      metrics.timings[AceMQ::AMQP::Telemetry::Registry.key(
        AceMQ::AMQP::Telemetry::REQUEST_DURATION, tags(outcome, type: type)
      )]
    end

    it "counts a round trip that was answered, and how long the caller waited" do
      serving { |_message| { "ok" => true } }
      requester = described_class.new(mq, to: "price.requests")
      requester.call({ "sku" => "A" }, type: "price.request")

      expect(metrics[AceMQ::AMQP::Telemetry::REQUEST_TOTAL,
                     **tags(AceMQ::AMQP::Telemetry::Outcome::ANSWERED)]).to eq(1)
      expect(timing(AceMQ::AMQP::Telemetry::Outcome::ANSWERED).count).to eq(1)
      requester.close
    end

    it "counts a round trip that ran out of patience, apart from a failure" do
      # Nothing is serving the queue. `timed_out` is not `failed`: it says a
      # reply did not arrive in time, not that anything went wrong, and the
      # request may still be queued or long since done with the reply lost.
      requester = described_class.new(mq, to: "price.requests", timeout: 0.02)
      begin
        requester.call({ "sku" => "A" }, type: "price.request")
      rescue AceMQ::AMQP::Patterns::RequestTimedOut
        nil
      end

      expect(metrics[AceMQ::AMQP::Telemetry::REQUEST_TOTAL,
                     **tags(AceMQ::AMQP::Telemetry::Outcome::TIMED_OUT)]).to eq(1)
      expect(metrics[AceMQ::AMQP::Telemetry::REQUEST_TOTAL,
                     **tags(AceMQ::AMQP::Telemetry::Outcome::FAILED)]).to eq(0)
      # The deadline rather than the work, which is the point: a timed_out
      # distribution sitting on the timeout is a deadline set too short.
      expect(timing(AceMQ::AMQP::Telemetry::Outcome::TIMED_OUT).count).to eq(1)
      requester.close
    end

    it "counts a responder's failure as a failed round trip" do
      serving { |_message| raise "the catalogue is down" }
      requester = described_class.new(mq, to: "price.requests")
      begin
        requester.call({ "sku" => "A" }, type: "price.request")
      rescue AceMQ::AMQP::Patterns::ResponderFailed
        nil
      end

      expect(metrics[AceMQ::AMQP::Telemetry::REQUEST_TOTAL,
                     **tags(AceMQ::AMQP::Telemetry::Outcome::FAILED)]).to eq(1)
      requester.close
    end

    it "spells the labels the way the other four spell them" do
      serving { |_message| { "ok" => true } }
      requester = described_class.new(mq, to: "price.requests")
      requester.call({ "sku" => "A" }, type: "price.request")

      expect(metrics.counts.keys)
        .to include("acemq.request.total{message.type=price.request,outcome=answered," \
                    "routing.key=price.requests}")
      # Illegal in Prometheus, and turned into something legal on the way out
      # rather than left to make the whole scrape unparseable.
      expect(metrics.to_prometheus)
        .to include('acemq_request_total{message_type="price.request",outcome="answered",' \
                    'routing_key="price.requests"}')
      requester.close
    end
  end

  it "keeps a generated reply queue classic, and lets a named one be quorum" do
    # The one place the library's quorum default would have broken something.
    # RabbitMQ refuses an exclusive or auto-delete quorum queue outright, so a
    # generated reply queue has to stay classic or it stops being declarable at
    # all. A named one follows the default instead, because it is a queue a
    # topology may well have declared too — and the two declarations have to
    # agree or the second is refused with PRECONDITION_FAILED.
    generated = described_class.new(mq, to: "price.requests")
    named = described_class.new(mq, to: "price.requests", reply_to: "price.replies")

    declared = transport.declared_queues.to_h
    expect(declared[generated.reply_queue])
      .to include(queue_type: :classic, exclusive: true, arguments: {})
    expect(declared["price.replies"])
      .to include(queue_type: :quorum, arguments: { "x-queue-type" => "quorum" })

    generated.close
    named.close
  end
end
