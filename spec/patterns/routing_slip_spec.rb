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
  end
end
