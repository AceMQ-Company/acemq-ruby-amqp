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
      "x-dead-letter-routing-key" => "orders.new.dlq",
      "x-queue-type" => "quorum"
    )
  end

  it "declares a source queue quorum, which is what the other four declare" do
    # The whole of the interop problem in one argument. A Java service declares
    # orders.new with x-queue-type=quorum; a Ruby service that left the argument
    # out would be declaring a classic queue, and whichever of the two started
    # second would be refused with PRECONDITION_FAILED and consume nothing.
    queue = described_class.new.queue("orders.new").queues.first

    expect(queue.type).to eq(:quorum)
    expect(queue.arguments).to eq("x-queue-type" => "quorum")
  end

  it "leaves the rungs, the dead-letter queue and the parked queue classic" do
    # Java's RetryTopology declares all three CLASSIC. They are queues two
    # libraries declare and one of them publishes into by name, so the argument
    # table has to match — and a rung is a time-to-live expiring into an
    # exchange, which is the plainest thing a classic queue does.
    policy = AceMQ::AMQP::RetryPolicy.fixed(3, 60)
    topology = described_class.new
                              .queue("orders.new", dead_letter: true, retry_policy: policy)
                              .parked_queue("orders.new")

    types = topology.queues.to_h { |queue| [queue.name, queue.type] }
    expect(types).to eq(
      "orders.new" => :quorum,
      "orders.new.dlq" => :classic,
      "orders.new.retry.1m" => :classic,
      "orders.new.parked" => :classic
    )
    # And no x-queue-type anywhere on the classic ones: Java sends none, and an
    # argument table that differs is the refusal this change exists to prevent.
    kinds = topology.queues.drop(1).map { |queue| queue.arguments["x-queue-type"] }
    expect(kinds).to all(be_nil)
  end

  it "still gives a caller the classic queue they asked for" do
    queue = described_class.new.queue("orders.new", queue_type: :classic).queues.first

    expect(queue.type).to eq(:classic)
    expect(queue.arguments).to eq({})
  end

  it "leaves a queue classic when it is exclusive, auto-deleting or transient" do
    # RabbitMQ replicates nothing that goes away on its own, so a quorum default
    # applied to these would declare a queue the broker refuses outright.
    topology = described_class.new
                              .queue("temporary", exclusive: true, durable: false)
                              .queue("scratch", auto_delete: true)
                              .queue("transient", durable: false)

    expect(topology.queues.map(&:type)).to eq(%i[classic classic classic])
    expect(topology.queues.map(&:arguments)).to all(eq({}))
  end

  it "refuses a quorum queue that is also exclusive rather than declaring it" do
    expect do
      described_class.new.queue("replies", queue_type: :quorum, exclusive: true)
    end.to raise_error(AceMQ::AMQP::QueueTypeError, /quorum queue while it is exclusive/)
  end

  it "leaves a stream alone, however it was asked for" do
    # A stream says so in its own arguments, which is how Patterns.declare_stream
    # declares one, and the default must not overrule it.
    topology = described_class.new
                              .queue("events", arguments: { "x-queue-type" => "stream" })
                              .queue("events.two", queue_type: :stream)

    expect(topology.queues.map(&:type)).to eq(%i[stream stream])
    expect(topology.queues.map(&:arguments)).to all(eq("x-queue-type" => "stream"))
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

  it "declares the rungs a retry policy needs, and nothing for the delays it does not" do
    # The rungs are the policy's schedule, which is a finite list known before
    # anything is published — which is exactly why the topology can declare
    # them rather than a consumer discovering them one failure at a time.
    policy = AceMQ::AMQP::RetryPolicy.new(max_attempts: 4, initial_delay: 30, multiplier: 2)
    topology = described_class.new.queue("orders.new", dead_letter: true,
                                                       retry_policy: policy)

    expect(topology.queues.map(&:name)).to eq(["orders.new", "orders.new.dlq",
                                               "orders.new.retry.30s", "orders.new.retry.1m",
                                               "orders.new.retry.2m"])
    expect(topology.queues[2].arguments).to eq(
      "x-message-ttl" => 30_000,
      "x-dead-letter-exchange" => AceMQ::AMQP::Naming::RETRY_EXCHANGE,
      "x-dead-letter-routing-key" => "orders.new"
    )
  end

  it "declares the retry exchange and the one binding that brings a rung home" do
    policy = AceMQ::AMQP::RetryPolicy.fixed(3, 60)
    topology = described_class.new.queue("orders.new", retry_policy: policy)

    # The binding is the part that fails silently when it is missing: the rung
    # holds the message for its time-to-live and the broker then drops it,
    # having nowhere to route it. So it is declared with the rungs rather than
    # arranged by whoever remembers.
    expect(topology.exchanges.map(&:name)).to eq(["acemq.retry"])
    expect(topology.exchanges.first.kind).to eq("direct")
    expect(topology.exchanges.first.durable).to be(true)
    expect(topology.bindings.map(&:to_s)).to eq(["acemq.retry -> orders.new (orders.new)"])
    expect(topology.problems).to be_empty
  end

  it "declares the retry exchange once, however many queues have rungs" do
    policy = AceMQ::AMQP::RetryPolicy.fixed(3, 60)
    topology = described_class.new
                              .queue("orders.new", retry_policy: policy)
                              .queue("orders.shipped", retry_policy: policy)

    expect(topology.exchanges.map(&:name)).to eq(["acemq.retry"])
    expect(topology.bindings.map(&:queue)).to eq(["orders.new", "orders.shipped"])
  end

  it "wires dead letters and parking through the shared exchange, bound by their own names" do
    # The same shape Java declares, and the reason both queues are reachable at
    # all: nothing is bound to them by the source queue's name.
    topology = described_class.new.queue("orders.new",
                                         dead_letter: true).parked_queue("orders.new")

    expect(topology.exchanges.map(&:name)).to eq([AceMQ::AMQP::Naming::DEAD_LETTER_EXCHANGE])
    expect(topology.exchanges.first.kind).to eq("direct")
    expect(topology.exchanges.first.durable).to be(true)
    expect(topology.bindings.map { |b| [b.queue, b.exchange, b.routing_key] }).to eq(
      [["orders.new.dlq", "acemq.dlx", "orders.new.dlq"],
       ["orders.new.parked", "acemq.dlx", "orders.new.parked"]]
    )
  end

  it "declares no rungs for a policy whose delays all fit in the consumer" do
    # The common case. A schedule that runs in seconds costs the broker nothing.
    seconds = AceMQ::AMQP::RetryPolicy.exponential(5, 1, 8)
    topology = described_class.new.queue("orders.new", retry_policy: seconds)

    expect(topology.queues.map(&:name)).to eq(["orders.new"])
    expect(topology.exchanges).to be_empty
  end

  it "takes a threshold, so a rung can be declared for whatever the consumer was told" do
    # The two have to agree: a consumer laddering at half a second against a
    # topology that laddered at thirty publishes into a queue nobody declared.
    topology = described_class.new.queue("orders.new",
                                         retry_policy: AceMQ::AMQP::RetryPolicy.fixed(3, 1),
                                         retry_threshold: 0.5)

    expect(topology.queues.map(&:name)).to eq(["orders.new", "orders.new.retry.1s"])
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
    # The kind is in the plan because the kind is what a reviewer holding this
    # beside the Java, Go, .NET or Python plan is checking.
    topology = described_class.new
                              .exchange("events", :topic)
                              .queue("orders.new", dead_letter: true)
    expect(topology.to_s).to eq(<<~PLAN.chomp)
      Topology: 2 exchanges, 2 queues, 1 bindings
        declare exchange events (topic)
        declare exchange acemq.dlx (direct)
        declare queue orders.new (quorum, durable, x-dead-letter-exchange=acemq.dlx, \
      x-dead-letter-routing-key=orders.new.dlq)
        declare queue orders.new.dlq (classic, durable)
        bind orders.new.dlq to acemq.dlx on "orders.new.dlq"
    PLAN
  end

  it "says what is wrong rather than a plan, when something is" do
    expect(described_class.new.queue("q").binding("q", "absent").to_s).to match(/invalid: /)
  end
end
