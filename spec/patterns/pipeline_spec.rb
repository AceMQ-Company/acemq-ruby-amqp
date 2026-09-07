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

RSpec.describe "chaining handlers" do
  let(:transport) { LoopbackTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }

  def message(id: "msg-1", payload: { "order_id" => "A-1" })
    AceMQ::AMQP::Message.new(
      payload: payload,
      envelope: AceMQ::AMQP::Envelope.new(id: id, type: "order.placed.v2"),
      routing_key: "orders.new", content_type: "application/json",
      redelivered: false, body: JSON.dump(payload)
    )
  end

  describe "chain" do
    it "puts the first middleware named on the outside" do
      # The order somebody reading the list expects, rather than the order the
      # code would fall into: logging named first records what everything
      # inside it decided.
      order = []
      recording = lambda do |name|
        lambda do |inner|
          lambda do |m|
            order << :"#{name}_in"
            ack = inner.call(m)
            order << :"#{name}_out"
            ack
          end
        end
      end

      wrapped = AceMQ::AMQP::Patterns.chain(->(_m) { AceMQ::AMQP::Ack.accept },
                                            recording.call("outer"), recording.call("inner"))
      wrapped.call(message)

      expect(order).to eq(%i[outer_in inner_in inner_out outer_out])
    end

    it "is the handler itself when there is no middleware" do
      handler = ->(_m) { AceMQ::AMQP::Ack.accept }

      expect(AceMQ::AMQP::Patterns.chain(handler)).to be(handler)
    end
  end

  describe "with_timeout" do
    it "leaves a handler that finished in time alone" do
      handler = AceMQ::AMQP::Patterns.chain(->(_m) { AceMQ::AMQP::Ack.accept },
                                            AceMQ::AMQP::Patterns.with_timeout(5))

      expect(handler.call(message)).to be_accept
    end

    it "reports an overrun as a retry, whatever the handler said about itself" do
      # Between two imperfect answers: retrying work that may have succeeded
      # risks doing it twice, and accepting work that may have failed loses it.
      # A duplicate is a problem somebody can solve; a lost message is not.
      slow = lambda do |_m|
        sleep(0.05)
        AceMQ::AMQP::Ack.accept
      end
      handler = AceMQ::AMQP::Patterns.chain(slow, AceMQ::AMQP::Patterns.with_timeout(0.01))
      ack = handler.call(message)

      expect(ack).to be_retry
      expect(ack.error).to match(/took .* seconds, longer than the 0.01 it is allowed/)
    end

    it "does not interrupt the handler, and does not pretend to" do
      # Ruby's Timeout.timeout raises inside whatever line the handler happened
      # to be on, which can leave a transaction half-written. The message is
      # held until the handler returns either way, so there is nothing to gain.
      finished = false
      slow = lambda do |_m|
        sleep(0.03)
        finished = true
        AceMQ::AMQP::Ack.accept
      end
      AceMQ::AMQP::Patterns.chain(slow, AceMQ::AMQP::Patterns.with_timeout(0.001)).call(message)

      expect(finished).to be(true)
    end
  end

  describe "with_logging" do
    it "records the message, the attempt, how long it took and what was decided" do
      lines = []
      handler = AceMQ::AMQP::Patterns.chain(
        ->(_m) { AceMQ::AMQP::Ack.retry("the warehouse is down") },
        AceMQ::AMQP::Patterns.with_logging { |line| lines << line }
      )
      handler.call(message)

      expect(lines.size).to eq(1)
      expect(lines.first).to match(/\Aacemq msg-1 type=order\.placed\.v2 attempt=1 took=\d+ms /)
      expect(lines.first).to end_with("retry: the warehouse is down")
    end

    it "logs an exception and then lets it through" do
      # Swallowing it would turn a failure into an acknowledgement; not logging
      # it would leave the middleware blind to the case it was added for.
      lines = []
      handler = AceMQ::AMQP::Patterns.chain(
        ->(_m) { raise "the database went away" },
        AceMQ::AMQP::Patterns.with_logging { |line| lines << line }
      )

      expect { handler.call(message) }.to raise_error(/the database went away/)
      expect(lines.first).to end_with("raised RuntimeError: the database went away")
    end

    it "needs somewhere to write" do
      expect { AceMQ::AMQP::Patterns.with_logging }
        .to raise_error(ArgumentError, /needs a block to write with/)
    end
  end

  describe "the other patterns as middleware" do
    it "guards duplicates from inside a chain" do
      store = AceMQ::AMQP::Patterns::InMemoryIdempotencyStore.new
      runs = 0
      handler = AceMQ::AMQP::Patterns.chain(
        lambda { |_m|
          runs += 1
          AceMQ::AMQP::Ack.accept
        },
        AceMQ::AMQP::Patterns.with_idempotency(store)
      )

      2.times { handler.call(message) }
      expect(runs).to eq(1)
    end

    it "orders from inside a chain" do
      handler = AceMQ::AMQP::Patterns.chain(
        ->(_m) { AceMQ::AMQP::Ack.accept },
        AceMQ::AMQP::Patterns.with_ordering(AceMQ::AMQP::Patterns.by_correlation)
      )

      expect(handler.call(message)).to be_accept
    end
  end

  describe "then_publish" do
    it "publishes what came out, and accepts only once it is gone" do
      handler = AceMQ::AMQP::Patterns.then_publish(mq, to: "shipment.requested") do |m|
        { "order_id" => m.payload["order_id"], "shipped" => true }
      end

      expect(handler.call(message)).to be_accept
      sent = transport.published_to("shipment.requested").first
      expect(JSON.parse(sent.body)).to eq({ "order_id" => "A-1", "shipped" => true })
    end

    it "carries the correlation forward and records what caused what" do
      handler = AceMQ::AMQP::Patterns.then_publish(mq, to: "shipment.requested", &:payload)
      incoming = message
      handler.call(incoming)

      sent = transport.published_to("shipment.requested").first
      expect(sent.headers[AceMQ::AMQP::Headers::CORRELATION])
        .to eq(incoming.envelope.correlation_id)
      expect(sent.headers[AceMQ::AMQP::Headers::CAUSATION]).to eq("msg-1")
    end

    it "publishes nothing for a step that says this one does not continue" do
      # Rather than inventing an empty message for the next service to work out
      # how to ignore.
      handler = AceMQ::AMQP::Patterns.then_publish(mq, to: "shipment.requested") { nil }

      expect(handler.call(message)).to be_accept
      expect(transport.published).to be_empty
    end

    it "retries the input when the next message will not go out" do
      # The work is done but nothing moved, so it runs again — which is why a
      # step that changes anything should be idempotent.
      allow(transport).to receive(:publish).and_raise(AceMQ::AMQP::PublishError, "no confirm")
      handler = AceMQ::AMQP::Patterns.then_publish(mq, to: "shipment.requested", &:payload)
      ack = handler.call(message)

      expect(ack).to be_retry
      expect(ack.error).to match(/is done but the next message did not go out/)
    end

    it "lets the step's own failure reach the retry engine" do
      handler = AceMQ::AMQP::Patterns.then_publish(mq, to: "x") { raise "the warehouse is down" }

      expect { handler.call(message) }.to raise_error(/the warehouse is down/)
    end
  end
end
