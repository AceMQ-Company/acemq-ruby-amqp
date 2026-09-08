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

RSpec.describe AceMQ::AMQP::Patterns::ConsumerGroup do
  let(:transport) { LoopbackTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }

  # One example below deliberately breaks a consumer's shutdown, and the point
  # of it is that the rest still stop; the connection then reports the same
  # refusal on its way out, which is not this block's business.
  after do
    mq.close
  rescue StandardError
    nil
  end

  it "starts the number of consumers it was asked for" do
    group = described_class.new(mq, "orders.new", size: 4) { AceMQ::AMQP::Ack.accept }

    expect(group.size).to eq(4)
    expect(group.queue).to eq("orders.new")
    group.close
    expect(group.size).to eq(0)
  end

  it "names each consumer, so the broker can say which one holds a message" do
    # Four identical rows in a management interface tell nobody anything.
    described_class.new(mq, "orders.new", size: 3) { AceMQ::AMQP::Ack.accept }

    # The queue being consumed is somebody else's, and a group of three does
    # not get three votes on what kind of queue it is. What each member does
    # declare is its own dead-letter half, which is idempotent and names no
    # queue but the two it owns.
    expect(transport.declared_queues.map(&:first).uniq)
      .to eq(["orders.new.dlq", "orders.new.parked"])
    tags = transport.instance_variable_get(:@subscribers)["orders.new"].size
    expect(tags).to eq(3)
  end

  it "shares the queue between its consumers rather than giving one all of it" do
    # The broker round-robins between consumers, which is the property a group
    # buys over one consumer with concurrency: four here compete evenly with
    # four in another instance.
    handled = Hash.new(0)
    counter = Mutex.new
    group = described_class.new(mq, "orders.new", size: 3) do |message|
      counter.synchronize { handled[message.payload["worker"]] += 1 }
      AceMQ::AMQP::Ack.accept
    end

    6.times { |i| mq.publish({ "worker" => i % 3 }, to: "orders.new") }
    group.close

    expect(handled.values.sum).to eq(6)
  end

  it "refuses a group of nobody" do
    expect { described_class.new(mq, "orders.new", size: 0) { AceMQ::AMQP::Ack.accept } }
      .to raise_error(ArgumentError, /at least one consumer/)
  end

  it "refuses a group with nothing to handle messages" do
    expect { described_class.new(mq, "orders.new", size: 2) }
      .to raise_error(ArgumentError, /needs a block/)
  end

  it "stops the ones it started when a later one will not start" do
    # A half-started group holds messages nothing is going to handle.
    calls = 0
    subscribing = lambda do |original, *args, **rest, &block|
      calls += 1
      raise AceMQ::AMQP::TransportError, "the channel went away" if calls == 3

      original.call(*args, **rest, &block)
    end
    allow(transport).to receive(:subscribe).and_wrap_original(&subscribing)

    expect { described_class.new(mq, "orders.new", size: 4) { AceMQ::AMQP::Ack.accept } }
      .to raise_error(AceMQ::AMQP::TransportError, /cannot start consumer 3 of 4/)
    expect(transport.instance_variable_get(:@subscribers)["orders.new"]).to be_empty
  end

  it "stops every consumer even when one of them refuses" do
    # Leaving the rest running after a failed shutdown is worse than the
    # failure, so the refusal is raised only once the others are down.
    group = described_class.new(mq, "orders.new", size: 3) { AceMQ::AMQP::Ack.accept }
    allow(group.consumers[0]).to receive(:cancel).and_raise("the connection went away")

    expect { group.close }.to raise_error(/the connection went away/)
    expect(transport.instance_variable_get(:@subscribers)["orders.new"].size).to eq(1)
  end

  it "is safe to close twice" do
    group = described_class.new(mq, "orders.new", size: 2) { AceMQ::AMQP::Ack.accept }
    group.close

    expect { group.close }.not_to raise_error
  end
end
