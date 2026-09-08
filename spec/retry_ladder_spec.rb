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

RetryPolicy = AceMQ::AMQP::RetryPolicy

RSpec.describe AceMQ::AMQP::RetryLadder do
  describe "which delays get a queue" do
    it "gives one to every delay at or above the threshold, and none below it" do
      # 1s, 10s, 100s, capped at 100s. Only the last two are worth a queue.
      policy = RetryPolicy.new(max_attempts: 5, initial_delay: 1, multiplier: 10,
                               max_delay: 100)
      ladder = described_class.for("orders.new", policy, threshold: 30)

      expect(ladder.queues).to eq(["orders.new.retry.100s"])
      expect(ladder.rung_for(1.0)).to be_nil
      expect(ladder.rung_for(10.0)).to be_nil
      expect(ladder.rung_for(100.0)).to eq("orders.new.retry.100s")
    end

    it "is empty for a schedule that runs entirely in seconds" do
      # The common case, and the one the broker should not pay for: five
      # doubling attempts from a second are over before anybody notices.
      ladder = described_class.for("orders.new", RetryPolicy.exponential(5, 1, 8))

      expect(ladder).to be_empty
      expect(ladder.queues).to be_empty
    end

    it "spends one queue per distinct delay, not one per attempt" do
      # A policy that reaches its ceiling repeats the same delay for every
      # remaining attempt. Ten attempts at 60s is one rung, not seven.
      policy = RetryPolicy.new(max_attempts: 10, initial_delay: 30, multiplier: 2,
                               max_delay: 60)
      ladder = described_class.for("orders.new", policy, threshold: 30)

      expect(ladder.queues).to eq(["orders.new.retry.30s", "orders.new.retry.1m"])
    end

    it "gives no rung to anything when the threshold is zero" do
      # Zero reads as "from zero, so everything" and means the opposite: it
      # switches the broker off. Java, Go, .NET and Python all define it that
      # way and the contract fixture records it, so a delay long enough to
      # deserve a rung still waits in the consumer when the threshold is zero.
      ladder = described_class.for("orders.new", RetryPolicy.fixed(4, 300), threshold: 0)

      expect(ladder).to be_empty
      expect(ladder.rung_for(300)).to be_nil
    end

    it "never gives a rung to a delay of zero, however low the threshold" do
      # A queue whose time-to-live is zero expires on arrival, which is a round
      # trip through the broker to achieve nothing.
      ladder = described_class.for("orders.new", RetryPolicy.fixed(4, 0), threshold: 0)

      expect(ladder).to be_empty
    end
  end

  describe "the arguments a rung is declared with" do
    it "expires to the source queue through the shared retry exchange" do
      ladder = described_class.for("orders.new", RetryPolicy.fixed(3, 60), threshold: 30)

      expect(ladder.rungs.first.arguments).to eq(
        "x-message-ttl" => 60_000,
        "x-dead-letter-exchange" => AceMQ::AMQP::Naming::RETRY_EXCHANGE,
        "x-dead-letter-routing-key" => "orders.new"
      )
    end

    it "declares a rung exactly as Java, Go, .NET and Python declare it" do
      # Two services on one queue declare the same rung by name. If they
      # disagree about its arguments the second one gets PRECONDITION_FAILED
      # and cannot consume at all, so this table is contract rather than
      # preference. The names are read from the constant, so that renaming the
      # exchange cannot leave this passing against a stale copy of the string;
      # the keys and the values are written out, so that changing the table
      # itself has to be deliberate.
      ladder = described_class.for("orders.new", RetryPolicy.fixed(3, 60), threshold: 30)

      expect(ladder.rungs.first.arguments.keys).to eq(
        %w[x-message-ttl x-dead-letter-exchange x-dead-letter-routing-key]
      )
      expect(ladder.rungs.first.arguments["x-message-ttl"]).to be_an(Integer)
      expect(AceMQ::AMQP::Naming::RETRY_EXCHANGE).to eq("acemq.retry")
      expect(AceMQ::AMQP::Naming::DEAD_LETTER_EXCHANGE).to eq("acemq.dlx")
    end

    it "puts the delay on the queue rather than on the message" do
      # Per-message TTL is the trap this design exists to avoid: RabbitMQ
      # expires messages only from the head of a queue, so one long wait at the
      # front holds back every shorter one behind it. The delay belongs to the
      # queue, which is why there is a queue per delay.
      ladder = described_class.for("orders.new", RetryPolicy.fixed(3, 45), threshold: 30)

      expect(ladder.rungs.first.arguments).to include("x-message-ttl" => 45_000)
      expect(ladder.rungs.first.arguments).not_to have_key("expiration")
    end
  end

  describe "picking a rung" do
    let(:ladder) do
      policy = RetryPolicy.new(max_attempts: 4, initial_delay: 30, multiplier: 2)
      described_class.for("orders.new", policy, threshold: 30)
    end

    it "rounds a delay up to the next rung rather than down" do
      # Waiting slightly too long is harmless; coming back early defeats the
      # backoff that was asked for.
      expect(ladder.rung_for(45.0)).to eq("orders.new.retry.1m")
    end

    it "uses the longest rung for a delay longer than any of them" do
      expect(ladder.rung_for(3600.0)).to eq("orders.new.retry.2m")
    end
  end

  describe "declaring" do
    let(:transport) { FakeTransport.new }
    let(:retry_exchange) { AceMQ::AMQP::Naming::RETRY_EXCHANGE }
    let(:dead_letter_exchange) { AceMQ::AMQP::Naming::DEAD_LETTER_EXCHANGE }

    it "declares the exchange, the rungs and the binding that brings them home" do
      ladder = described_class.for("orders.new", RetryPolicy.fixed(3, 60), threshold: 30)
      ladder.declare(transport)

      expect(transport.declared_exchanges)
        .to include([AceMQ::AMQP::Naming::RETRY_EXCHANGE, { kind: :direct, durable: true }])
      expect(transport.declared_queues.map(&:first)).to include("orders.new.retry.1m")
      expect(transport.bindings)
        .to include(["orders.new", AceMQ::AMQP::Naming::RETRY_EXCHANGE, "orders.new"])
    end

    it "never declares a rung without the binding that carries it home" do
      # The failure this exists to prevent is silent. A rung with no binding
      # accepts the message, holds it for the time-to-live and then drops it,
      # because an unroutable dead letter goes nowhere and reports nothing, so
      # the binding is part of declaring rather than something arranged later.
      ladder = described_class.for("orders.new", RetryPolicy.fixed(4, 90), threshold: 30)
      ladder.declare(transport)

      expect(transport.declared_queues.map(&:first)).to include("orders.new.retry.90s")
      home = transport.bindings.select { |_queue, exchange, _key| exchange == retry_exchange }
      expect(home.size).to eq(1)
      queue, exchange, key = home.first
      expect(queue).to eq("orders.new")
      expect(exchange).to eq(ladder.rungs.first.arguments["x-dead-letter-exchange"])
      expect(key).to eq(ladder.rungs.first.arguments["x-dead-letter-routing-key"])
    end

    it "declares no rung and no retry exchange when no delay needs a queue" do
      described_class.for("orders.new", RetryPolicy.fixed(3, 1)).declare(transport)

      declared = transport.declared_queues.map(&:first)

      expect(declared).to all(satisfy { |name| !name.include?(".retry.") })
      expect(transport.declared_exchanges.map(&:first)).not_to include(retry_exchange)
      expect(transport.bindings.map { |binding| binding[1] }).not_to include(retry_exchange)
    end

    # ADR-032. The half of this that a consumer needs whether or not it has a
    # single rung, because giving up does not depend on having somewhere to
    # retry — RetryPolicy.none is the library default and gives up on the first
    # failure.
    describe "the dead-letter half" do
      it "declares the exchange, the two queues and their bindings" do
        described_class.for("orders.new", RetryPolicy.fixed(3, 60), threshold: 30)
                       .declare(transport)

        expect(transport.declared_exchanges)
          .to include([dead_letter_exchange, { kind: :direct, durable: true }])
        expect(transport.declared_queues.map(&:first))
          .to include("orders.new.dlq", "orders.new.parked")
        expect(transport.bindings)
          .to include(["orders.new.dlq", dead_letter_exchange, "orders.new.dlq"],
                      ["orders.new.parked", dead_letter_exchange, "orders.new.parked"])
      end

      it "declares it for a policy with no rungs at all" do
        # The case that used to declare nothing, which is also the commonest
        # consumer in the library: no retry policy, so every failure is a dead
        # letter and there is no rung to hang the declaration off.
        described_class.for("orders.new", RetryPolicy.none).declare(transport)

        expect(transport.declared_queues.map(&:first))
          .to eq(["orders.new.dlq", "orders.new.parked"])
        expect(transport.declared_exchanges.map(&:first)).to eq([dead_letter_exchange])
      end

      it "declares the two queues classic, durable and with no arguments" do
        # The argument table Topology#dead_letter_queue and Java both use. It
        # has to be that table and not merely a working one: a service that
        # applied its topology first and then started a consumer would be
        # refused PRECONDITION_FAILED by anything else.
        described_class.for("orders.new", RetryPolicy.none).declare(transport)

        transport.declared_queues.map(&:last).each do |options|
          expect(options[:queue_type]).to eq(AceMQ::AMQP::QueueType::CLASSIC)
          expect(options[:durable]).to be(true)
          expect(options[:arguments]).to eq({})
        end
      end

      it "binds nothing to the source queue, which a consumer does not declare" do
        # Topology#retry_ladder needs the source queue in the same topology
        # because it binds to it. The dead-letter half must not pick up that
        # requirement: the only two queues it binds are the two it declares
        # itself.
        described_class.for("orders.new", RetryPolicy.none).declare(transport)

        expect(transport.bindings.map(&:first)).to eq(["orders.new.dlq", "orders.new.parked"])
      end
    end
  end
end
