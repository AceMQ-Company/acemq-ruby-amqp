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

require "timeout"

require "acemq/amqp/patterns"

RSpec.describe "ordering by key" do
  def message(order: nil, correlation: "corr-1", payload: { "n" => 1 })
    headers = order.nil? ? {} : { "order-id" => order }
    AceMQ::AMQP::Message.new(
      payload: payload,
      envelope: AceMQ::AMQP::Envelope.new(correlation_id: correlation, headers: headers),
      routing_key: "orders.new", content_type: "application/json",
      redelivered: false, body: "{}"
    )
  end

  it "never has two messages with the same key in the handler at once" do
    # The failure this prevents: an "order cancelled" overtaking the "order
    # placed" it cancels, because the consumer was handed both at once.
    inside = 0
    overlaps = 0
    counter = Mutex.new
    handler = AceMQ::AMQP::Patterns.ordered("order-id") do
      counter.synchronize do
        overlaps += 1 if inside.positive?
        inside += 1
      end
      sleep(0.02)
      counter.synchronize { inside -= 1 }
      AceMQ::AMQP::Ack.accept
    end

    4.times.map { Thread.new { handler.call(message(order: "A-1")) } }.each(&:join)

    expect(overlaps).to eq(0)
  end

  it "lets messages with different keys run at the same time" do
    # Ordering per key, concurrency across keys. If this serialised them the
    # second arrival would never happen and the timeout would fire.
    arrived = Queue.new
    carry_on = Queue.new
    handler = AceMQ::AMQP::Patterns.ordered("order-id") do |message|
      arrived << message.envelope.headers["order-id"]
      carry_on.pop
      AceMQ::AMQP::Ack.accept
    end

    workers = %w[A-1 B-2].map { |order| Thread.new { handler.call(message(order: order)) } }
    both = Timeout.timeout(5) { [arrived.pop, arrived.pop] }
    2.times { carry_on << :go }
    workers.each(&:join)

    expect(both.sort).to eq(%w[A-1 B-2])
  end

  it "handles a message with no key without ordering it against anything" do
    ran = false
    handler = AceMQ::AMQP::Patterns.ordered("order-id") do
      ran = true
      AceMQ::AMQP::Ack.accept
    end

    expect(handler.call(message)).to be_accept
    expect(ran).to be(true)
  end

  it "takes a lambda for a key that lives in the payload" do
    seen = []
    handler = AceMQ::AMQP::Patterns.ordered(->(m) { m.payload["n"] }) do |message|
      seen << message.payload["n"]
      AceMQ::AMQP::Ack.accept
    end
    handler.call(message(payload: { "n" => 7 }))

    expect(seen).to eq([7])
  end

  it "orders by correlation, which keeps one business action in sequence" do
    seen = []
    handler = AceMQ::AMQP::Patterns.ordered(AceMQ::AMQP::Patterns.by_correlation) do |message|
      seen << message.envelope.correlation_id
      AceMQ::AMQP::Ack.accept
    end
    handler.call(message(correlation: "corr-9"))

    expect(seen).to eq(["corr-9"])
  end

  it "passes whatever the handler decided straight back" do
    # A wrapper that swallowed a retry would turn a failure into an
    # acknowledgement, which is the one thing it must never do.
    handler = AceMQ::AMQP::Patterns.ordered("order-id") { AceMQ::AMQP::Ack.retry("not yet") }

    expect(handler.call(message(order: "A-1"))).to be_retry
  end

  it "lets an exception through, so the retry engine still sees it" do
    handler = AceMQ::AMQP::Patterns.ordered("order-id") { raise "the database went away" }

    expect { handler.call(message(order: "A-1")) }.to raise_error(/the database went away/)
  end

  it "forgets a key once nothing is using it" do
    # The natural key here is per order or per customer, so a lock kept for
    # every key ever seen is a lock kept for every order this process handled.
    locks = AceMQ::AMQP::Patterns::KeyedLocks.new
    100.times { |i| locks.holding("order-#{i}") { nil } }

    expect(locks.size).to eq(0)
  end

  it "keeps a key's lock while somebody else is waiting for it" do
    locks = AceMQ::AMQP::Patterns::KeyedLocks.new
    held = Queue.new
    release = Queue.new
    first = Thread.new do
      locks.holding("A") do
        held << :in
        release.pop
      end
    end
    held.pop

    expect(locks.size).to eq(1)
    release << :go
    first.join
    expect(locks.size).to eq(0)
  end

  describe "partitioning" do
    it "gives the same key the same slot, in every process and every language" do
      # Ruby randomises String#hash per process, so two workers using it would
      # disagree about where a key belongs — and so would a Go publisher and a
      # Ruby consumer. These are FNV-1a, which does not move.
      # Asked for more partitions than the hash has values, so the partition is
      # the hash and these are FNV-1a's own published vectors rather than
      # whatever this implementation happens to produce.
      every = 2**32
      expect(AceMQ::AMQP::Patterns.partition("", every)).to eq(0x811c9dc5)
      expect(AceMQ::AMQP::Patterns.partition("a", every)).to eq(0xe40c292c)
      expect(AceMQ::AMQP::Patterns.partition("foobar", every)).to eq(0xbf9cf968)
    end

    it "stays inside the number of partitions asked for" do
      slots = 200.times.map { |i| AceMQ::AMQP::Patterns.partition("order-#{i}", 8) }

      expect(slots.min).to be >= 0
      expect(slots.max).to be <= 7
      # Not a distribution test, just a check that it is not answering 0 to
      # everything, which a broken hash does silently.
      expect(slots.uniq.size).to eq(8)
    end

    it "answers zero when there is only one partition" do
      expect(AceMQ::AMQP::Patterns.partition("anything", 1)).to eq(0)
      expect(AceMQ::AMQP::Patterns.partition("anything", 0)).to eq(0)
    end

    it "builds a routing key for a queue-per-partition arrangement" do
      key = AceMQ::AMQP::Patterns.partitioned_routing_key("orders", "order-42", 8)

      expect(key).to eq("orders.#{AceMQ::AMQP::Patterns.partition("order-42", 8)}")
      expect(key).to match(/\Aorders\.[0-7]\z/)
    end
  end
end
