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
require "acemq/amqp/patterns"

# What a drain is bounded by.
#
# The arithmetic is the whole subject: a deadline spent per consumer is not a
# bound on anything, because the process only has one grace period and there may
# be eight consumers. Every example here is about elapsed time, so each one is
# written with a deadline of a fraction of a second and asserts against a margin
# that a per-consumer wait could not possibly meet.
RSpec.describe "a drain" do
  let(:transport) { FakeTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "checkout@pod-7") }

  # Handlers that have started and will not finish until they are let go, which
  # is what "in flight" means to a shutdown. Closed rather than pushed to, so
  # every waiting handler is released at once however many there are.
  let(:gate) { Thread::Queue.new }

  after do
    gate.close unless gate.closed?
  end

  def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  # A consumer with a handler that blocks, and a delivery already inside it.
  def occupied(queue)
    consumer = mq.consume(queue) do
      gate.pop
      AceMQ::AMQP::Ack.accept
    end
    delivery, = FakeDelivery.build(body: "{}", routing_key: queue)
    Thread.new { consumer.handle(delivery) }
    consumer
  end

  def wait_for_in_flight(consumers)
    deadline = now + 5
    sleep(0.005) while consumers.any? { |c| c.in_flight.zero? } && now < deadline
    raise "handlers never started" if consumers.any? { |c| c.in_flight.zero? }
  end

  describe "AceMQ::AMQP::Connection#close" do
    it "spends one deadline on every consumer together, not one deadline each" do
      # The defect this is here for: four consumers used to mean four full
      # waits, and eight consumers at thirty seconds each is four minutes —
      # well past the thirty Kubernetes allows before SIGKILL, so the drain that
      # exists to settle messages was itself the reason they were lost.
      consumers = Array.new(4) { |i| occupied("orders.#{i}") }
      wait_for_in_flight(consumers)

      started = now
      expect { mq.close(timeout: 0.5) }.to raise_error(AceMQ::AMQP::DrainTimeout)
      elapsed = now - started

      # Four consumers, half a second each, would be two seconds.
      expect(elapsed).to be < 1.2
      # And it did wait: a bound that finishes instantly has not drained either.
      expect(elapsed).to be >= 0.5
    end

    it "stops every consumer and closes the socket when the deadline expires" do
      consumers = Array.new(2) { |i| occupied("orders.#{i}") }
      wait_for_in_flight(consumers)

      expect { mq.close(timeout: 0.1) }.to raise_error(AceMQ::AMQP::DrainTimeout)

      # Cancelling the subscription and letting the broker redeliver is the
      # honest end of a drain that ran out of time. Leaving the consumers
      # subscribed would be neither draining nor stopping.
      expect(consumers.map(&:running?)).to eq([false, false])
      expect(transport.closed?).to be(true)
    end

    it "says how much was left unsettled rather than reporting a clean drain" do
      consumers = Array.new(2) { |i| occupied("orders.#{i}") }
      wait_for_in_flight(consumers)

      error = begin
        mq.close(timeout: 0.1)
      rescue AceMQ::AMQP::DrainTimeout => e
        e
      end

      # Nothing is lost — an unacknowledged delivery comes back — but an
      # operator whose grace period is too short has no other way to find out.
      expect(error.stranded).to eq("orders.0" => 1, "orders.1" => 1)
      expect(error.timeout).to eq(0.1)
      expect(error.message)
        .to match(/2 deliveries were left unsettled and will be redelivered/)
    end

    it "returns as soon as the handlers are done, rather than waiting the deadline out" do
      consumers = Array.new(3) { |i| occupied("orders.#{i}") }
      wait_for_in_flight(consumers)
      Thread.new do
        sleep(0.05)
        gate.close
      end

      started = now
      expect(mq.close(timeout: 10)).to be_nil
      expect(now - started).to be < 5
    end

    it "stops every consumer without waiting when given no time at all" do
      consumers = Array.new(3) { |i| occupied("orders.#{i}") }
      wait_for_in_flight(consumers)

      started = now
      expect { mq.close(timeout: 0) }.to raise_error(AceMQ::AMQP::DrainTimeout)

      expect(now - started).to be < 0.5
      expect(consumers.map(&:running?)).to eq([false, false, false])
    end

    it "closes cleanly and raises nothing when there is nothing in flight" do
      mq.consume("orders.new") { AceMQ::AMQP::Ack.accept }

      expect(mq.close(timeout: 0.1)).to be_nil
      expect(transport.closed?).to be(true)
    end

    it "stops the rest and closes the connection when one consumer refuses" do
      # Kept from before the deadline existed, and still the right rule:
      # stopping at the first failure would leave the others running and the
      # socket open, so a shutdown that went slightly wrong would become a
      # process that will not exit.
      first = mq.consume("orders.new") { AceMQ::AMQP::Ack.accept }
      second = mq.consume("orders.shipped") { AceMQ::AMQP::Ack.accept }
      allow(first).to receive(:cancel).and_raise(AceMQ::AMQP::TransportError, "channel gone")

      expect { mq.close(timeout: 0.1) }
        .to raise_error(AceMQ::AMQP::TransportError, /channel gone/)
      expect(second).not_to be_running
      expect(transport.closed?).to be(true)
    end

    it "defaults to a deadline an orchestrator can live with" do
      # Twenty inside Kubernetes' thirty-second grace period leaves ten for the
      # web server and for the process to exit. The number is asserted because
      # it is a promise to whoever wrote the deployment, not an implementation
      # detail.
      expect(AceMQ::AMQP::Connection::DRAIN_TIMEOUT).to eq(20)
    end
  end

  describe "AceMQ::AMQP::Patterns::ConsumerGroup#close" do
    let(:transport) { LoopbackTransport.new }

    it "spends one deadline on the group, not one per member" do
      # A group is the one place this library starts several consumers at once,
      # so a per-consumer wait is multiplied by exactly the number the caller
      # configured.
      group = AceMQ::AMQP::Patterns::ConsumerGroup.new(mq, "orders.new", size: 4) do
        gate.pop
        AceMQ::AMQP::Ack.accept
      end
      group.consumers.each do |consumer|
        delivery, = FakeDelivery.build(body: "{}", routing_key: "orders.new")
        Thread.new { consumer.handle(delivery) }
      end
      wait_for_in_flight(group.consumers)

      started = now
      expect { group.close(timeout: 0.5) }.to raise_error(AceMQ::AMQP::DrainTimeout)

      # Four members at half a second each would be two seconds.
      expect(now - started).to be < 1.2
    end
  end
end
