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

    it "never gives a rung to a delay of zero, however low the threshold" do
      # A queue whose time-to-live is zero expires on arrival, which is a round
      # trip through the broker to achieve nothing.
      ladder = described_class.for("orders.new", RetryPolicy.fixed(4, 0), threshold: 0)

      expect(ladder).to be_empty
    end
  end

  describe "the arguments a rung is declared with" do
    it "expires to the source queue through the retry exchange" do
      ladder = described_class.for("orders.new", RetryPolicy.fixed(3, 60), threshold: 30)

      expect(ladder.rungs.first.arguments).to eq(
        "x-message-ttl" => 60_000,
        "x-dead-letter-exchange" => "acemq.retry",
        "x-dead-letter-routing-key" => "orders.new"
      )
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

    it "declares the exchange, every rung, and one binding home" do
      ladder = described_class.for("orders.new", RetryPolicy.fixed(3, 60), threshold: 30)
      ladder.declare(transport)

      expect(transport.declared_exchanges.map(&:first)).to eq(["acemq.retry"])
      expect(transport.declared_queues.map(&:first)).to eq(["orders.new.retry.1m"])
      # One binding, under the source queue's own name, because every rung
      # dead-letters under that name however many rungs there are.
      expect(transport.bindings).to eq([["orders.new", "acemq.retry", "orders.new"]])
    end

    it "declares nothing at all when no delay needs a queue" do
      described_class.for("orders.new", RetryPolicy.fixed(3, 1)).declare(transport)

      expect(transport.declared_queues).to be_empty
      expect(transport.declared_exchanges).to be_empty
      expect(transport.bindings).to be_empty
    end
  end
end
