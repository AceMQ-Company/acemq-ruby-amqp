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

Topology = AceMQ::AMQP::Topology
TopologyError = AceMQ::AMQP::TopologyError

RSpec.describe AceMQ::AMQP::Topology do
  it "wires a dead-letter queue using the names every language uses" do
    # An operator looking for the dead letters of orders.new must find them in
    # orders.new.dlq whichever language declared the queue.
    topology = described_class.new.queue("orders.new", dead_letter: true)

    expect(topology.queues.map(&:name)).to eq(["orders.new", "orders.new.dlq"])
    expect(topology.queues.first.arguments).to eq(
      "x-dead-letter-exchange" => "acemq.dlx",
      "x-dead-letter-routing-key" => "orders.new.dlq"
    )
  end

  it "sets the dead-letter routing key as well as the exchange" do
    # Without the key the broker reuses the message's original routing key,
    # which on a topic exchange is whatever it was published under — so the
    # dead letters of six routing keys would land in six places, none of them
    # named .dlq.
    arguments = described_class.new.queue("orders.new",
                                          dead_letter: true).queues.first.arguments
    expect(arguments["x-dead-letter-routing-key"]).to eq(AceMQ::AMQP::Naming.dead_letter_queue("orders.new"))
  end

  it "declares the shared dead-letter exchange once, however many queues want it" do
    topology = described_class.new
                              .queue("orders.new", dead_letter: true)
                              .queue("orders.shipped", dead_letter: true)

    expect(topology.exchanges.map(&:name)).to eq(["acemq.dlx"])
    expect(topology.bindings.map(&:queue)).to eq(["orders.new.dlq", "orders.shipped.dlq"])
  end

  it "keeps a parked queue apart from a dead-letter queue" do
    # A message that failed five times and a message nothing could read are two
    # different problems; mixing them means somebody sorts them by hand.
    topology = described_class.new.queue("orders.new",
                                         dead_letter: true).parked_queue("orders.new")
    expect(topology.queues.map(&:name)).to eq(["orders.new", "orders.new.dlq",
                                               "orders.new.parked"])
  end

  it "catches a binding to a queue nothing declares" do
    # The broker would accept it if the queue happened to exist already, and
    # the service would then depend on something no deployment creates.
    topology = described_class.new.exchange("events", :topic).binding("absent", "events", "a.#")
    expect(topology.problems)
      .to include(/names queue "absent", which this topology does not declare/)
  end

  it "catches a binding to an exchange nothing declares" do
    topology = described_class.new.queue("q").binding("q", "absent", "a.#")
    expect(topology.problems).to include(/names exchange "absent"/)
  end

  it "refuses a binding to the default exchange, which cannot be bound to" do
    topology = described_class.new.queue("q").binding("q", "")
    expect(topology.problems).to include(/default exchange, which cannot be bound to/)
  end

  it "catches the same queue declared twice" do
    topology = described_class.new.queue("q").queue("q")
    expect(topology.problems).to include('queue "q" is declared 2 times')
  end

  it "catches an exchange with no kind" do
    topology = described_class.new.exchange("events", "")
    expect(topology.problems).to include(/no kind \(direct, topic, fanout or headers\)/)
  end

  it "catches a queue with no name, which the broker would silently accept" do
    # RabbitMQ reads an empty name as "generate one for me", so the declaration
    # succeeds and the service then consumes from nothing, with no error.
    expect(described_class.new.queue("").problems)
      .to include("a queue in this topology has no name")
  end

  it "will not apply a topology it has already found fault with" do
    topology = described_class.new.queue("q").binding("q", "absent")
    expect do
      topology.apply(FakeTransport.new)
    end.to raise_error(TopologyError, /does not declare/)
  end

  it "declares exchanges before queues before bindings" do
    # The order a broker needs: a binding to an exchange that does not exist
    # yet is a channel-level error, and it takes the channel with it.
    transport = FakeTransport.new
    described_class.new
                   .exchange("events", :topic)
                   .queue("orders.new", dead_letter: true)
                   .binding("orders.new", "events", "order.#")
                   .apply(transport)

    expect(transport.declared_exchanges.map(&:first)).to eq(["events", "acemq.dlx"])
    expect(transport.declared_queues.map(&:first)).to eq(["orders.new", "orders.new.dlq"])
    expect(transport.bindings).to eq([["orders.new.dlq", "acemq.dlx", "orders.new.dlq"],
                                      ["orders.new", "events", "order.#"]])
  end

  it "reads as something worth putting in a deployment log" do
    topology = described_class.new.exchange("events", :topic).queue("orders.new")
    expect(topology.to_s).to eq(<<~PLAN.chomp)
      Topology: 1 exchanges, 1 queues, 0 bindings
        declare exchange events (topic)
        declare queue orders.new (durable)
    PLAN
  end

  it "says what is wrong rather than a plan, when something is" do
    expect(described_class.new.queue("q").binding("q", "absent").to_s).to match(/invalid: /)
  end
end
