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

# Everything the unit specs cannot prove, and nothing they can.
#
# The retry arithmetic is tested against a fake transport because it is
# arithmetic. What is left needs a broker and nothing else will do: that the
# headers survive a round trip through RabbitMQ's field table with their types
# intact, that a nack really does come back marked redelivered, and that a
# message which runs out of attempts really does end up on a queue somebody can
# go and look at.
#
# Every name here starts with rbit. so that a broker shared with anything else
# is left alone.
RSpec.describe "against a real broker", :integration do
  PREFIX = "rbit."
  BROKER = ENV.fetch("ACEMQ_TEST_BROKER", "amqp://guest:guest@localhost:5672")

  # Named per example group so two of them cannot collide on a queue, which is
  # the kind of failure that only shows up when the suite is run in a different
  # order.
  def queue_named(suffix) = "#{PREFIX}#{suffix}"

  let(:mq) { AceMQ::AMQP::Connection.open(BROKER, origin: "rspec@rbit") }

  after { mq.close }

  # Polling rather than a condition variable: the thing being waited on happens
  # on bunny's threads, and a test that deadlocks tells you nothing at all.
  def wait_for(seconds: 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    loop do
      found = yield
      return found if found
      break if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep(0.05)
    end
    nil
  end

  # Takes one message off a queue without a consumer, which is how a test looks
  # at a dead-letter queue nothing is draining.
  def take_one(queue, seconds: 10)
    channel = mq.transport.session.create_channel
    wait_for(seconds: seconds) do
      _info, properties, body = channel.basic_get(queue, manual_ack: false)
      properties && [properties, body]
    end
  ensure
    channel&.close
  end

  def scrub(*queues)
    queues.each { |name| mq.delete_queue(name) }
  rescue StandardError
    # A queue that is not there is the state this wanted anyway.
    nil
  end

  describe "a round trip" do
    let(:queue) { queue_named("roundtrip") }

    before do
      scrub(queue)
      AceMQ::AMQP::Topology.new.queue(queue).apply(mq)
    end

    after { scrub(queue) }

    it "brings the envelope back the way it went out" do
      received = []
      mq.consume(queue) do |message|
        received << message
        AceMQ::AMQP::Ack.accept
      end

      sent = mq.publish({ "order_id" => "A-1", "total_cents" => 250 },
                        to: queue, type: "order.placed.v2", version: 3,
                        correlation_id: "corr-9", causation_id: "cause-8",
                        headers: { "tenant" => "acme" })

      wait_for { received.any? }
      expect(received.size).to eq(1)

      message = received.first
      expect(message.payload).to eq({ "order_id" => "A-1", "total_cents" => 250 })
      expect(message.content_type).to eq("application/json")
      expect(message.routing_key).to eq(queue)

      envelope = message.envelope
      expect(envelope.id).to eq(sent.id)
      expect(envelope.type).to eq("order.placed.v2")
      expect(envelope.version).to eq(3)
      expect(envelope.correlation_id).to eq("corr-9")
      expect(envelope.causation_id).to eq("cause-8")
      expect(envelope.origin).to eq("rspec@rbit")
      expect(envelope.attempt).to eq(1)
      expect(envelope.headers).to eq({ "tenant" => "acme" })
      # Epoch milliseconds, not a broker timestamp and not seconds: the whole
      # point of pinning the type is that a Java consumer reads the same number.
      expect(envelope.first_seen.to_f).to be_within(1.0).of(sent.first_seen.to_f)
    end
  end

  describe "a handler that keeps failing" do
    let(:queue) { queue_named("retry") }
    let(:dlq) { AceMQ::AMQP::Naming.dead_letter_queue(queue) }

    before do
      scrub(queue, dlq)
      AceMQ::AMQP::Topology.new.queue(queue).queue(dlq).apply(mq)
    end

    after { scrub(queue, dlq) }

    it "really does retry, and then really does dead-letter with the reason" do
      attempts = []
      mq.consume(queue, retry_policy: AceMQ::AMQP::RetryPolicy.fixed(3, 0.05)) do |message|
        attempts << [message.attempt, message.redelivered?]
        AceMQ::AMQP::Ack.retry("the warehouse is down")
      end

      sent = mq.publish({ "order_id" => "A-2" }, to: queue, type: "order.placed.v2")

      wait_for { attempts.size >= 3 }
      # Three deliveries, counted by the broker's redelivery flag rather than
      # by the header, which a requeue leaves untouched.
      expect(attempts).to eq([[1, false], [2, true], [3, true]])

      properties, body = take_one(dlq)
      expect(properties).not_to be_nil
      expect(body).to eq('{"order_id":"A-2"}')

      headers = properties[:headers]
      expect(headers[AceMQ::AMQP::Headers::ID]).to eq(sent.id)
      expect(headers[AceMQ::AMQP::Headers::ATTEMPT]).to eq(3)
      expect(headers[AceMQ::AMQP::Headers::ERROR])
        .to eq("gave up after 3 attempts: the warehouse is down")
      # The source queue is empty: the original was acknowledged once the copy
      # was safely somewhere else.
      expect(wait_for { mq.message_count(queue).zero? }).to be(true)
    end
  end

  describe "a body nothing can read" do
    let(:queue) { queue_named("undecodable") }
    let(:parked) { AceMQ::AMQP::Naming.parked_queue(queue) }

    before do
      scrub(queue, parked)
      AceMQ::AMQP::Topology.new.queue(queue).queue(parked).apply(mq)
    end

    after { scrub(queue, parked) }

    it "parks it with the reason rather than retrying it forever" do
      seen = []
      mq.consume(queue, retry_policy: AceMQ::AMQP::RetryPolicy.fixed(10, 0)) do |message|
        seen << message
        AceMQ::AMQP::Ack.accept
      end

      # Published as bytes so the broker carries something the JSON codec on
      # the consuming side cannot possibly read.
      mq.publish("{ not json at all", to: queue, codec: AceMQ::AMQP::BytesCodec.new)

      properties, body = take_one(parked)
      expect(body).to eq("{ not json at all")
      expect(properties[:headers][AceMQ::AMQP::Headers::ERROR]).to match(/could not be decoded/)
      expect(seen).to be_empty
    end
  end

  describe "a topology" do
    let(:queue) { queue_named("topology") }
    let(:dlq) { AceMQ::AMQP::Naming.dead_letter_queue(queue) }
    let(:exchange) { queue_named("topology.events") }
    let(:dlx) { queue_named("dlx") }

    before { scrub(queue, dlq) }

    after do
      scrub(queue, dlq)
      mq.transport.delete_exchange(exchange)
      mq.transport.delete_exchange(dlx)
    end

    it "declares what it said it would, and the broker accepts the wiring" do
      AceMQ::AMQP::Topology.new(dead_letter_exchange: dlx)
                           .exchange(exchange, :topic)
                           .queue(queue, dead_letter: true)
                           .binding(queue, exchange, "order.#")
                           .apply(mq)

      expect(mq.queue_exists?(queue)).to be(true)
      expect(mq.queue_exists?(dlq)).to be(true)

      # Routed by the topic binding rather than by queue name, which is the bit
      # a plan on paper cannot prove.
      mq.publish({ "order_id" => "A-3" }, to: "order.placed", exchange: exchange)
      expect(wait_for { mq.message_count(queue).positive? }).to be(true)
    end

    it "is refused by the broker when it disagrees with what is already there" do
      # PRECONDITION_FAILED is the only way AMQP reports drift without the
      # management API, and passing it on is the point: it means this service
      # and the broker disagree about what the queue is.
      AceMQ::AMQP::Topology.new(dead_letter_exchange: dlx).queue(queue).apply(mq)

      expect { mq.declare_queue(queue, durable: false) }
        .to raise_error(AceMQ::AMQP::TransportError, /PRECONDITION_FAILED/i)
    end
  end

  describe "publishing" do
    let(:queue) { queue_named("confirms") }

    before do
      scrub(queue)
      AceMQ::AMQP::Topology.new.queue(queue).apply(mq)
    end

    after { scrub(queue) }

    it "waits for the broker to say it has the message" do
      # Without confirms a successful publish means the bytes reached a socket,
      # which is not the same as the broker having them.
      mq.publish({ "order_id" => "A-4" }, to: queue)
      expect(wait_for { mq.message_count(queue) == 1 }).to be(true)
    end
  end
end
