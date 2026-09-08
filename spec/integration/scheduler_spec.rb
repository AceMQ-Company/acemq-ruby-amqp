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

# The scheduler is a ladder of time-to-live queues, and a time to live is the
# one thing no fake can honestly stand in for: the loopback specs prove which
# rung a delay chooses and what the headers say, and only a broker proves that
# a message put in a rung comes back out of it, later, by itself.
#
# The argument tables matter as much as the delivery. A rung is declared by
# name, so a Ruby service and a Java service scheduling on one broker declare
# the same queue -- and a queue already there with a different table answers
# PRECONDITION_FAILED, which leaves whichever of them declared second unable to
# schedule anything at all. The parity examples below declare Java's literal
# table against the queues this library created, and then a table that differs
# by one argument, because a check that only ever says yes proves nothing.
RSpec.describe "the scheduler against a real broker", :integration do
  SCHEDULER = AceMQ::AMQP::Patterns::Scheduler

  # Java's table, written out rather than asked of the Ruby code, so that this
  # compares two independent statements of the contract instead of comparing
  # the implementation with itself.
  JAVA_RUNG_ARGUMENTS = {
    "acemq.schedule.1h" => { "x-message-ttl" => 3_600_000,
                             "x-dead-letter-exchange" => "acemq.schedule",
                             "x-dead-letter-routing-key" => "acemq.schedule.due" },
    "acemq.schedule.10m" => { "x-message-ttl" => 600_000,
                              "x-dead-letter-exchange" => "acemq.schedule",
                              "x-dead-letter-routing-key" => "acemq.schedule.due" },
    "acemq.schedule.1m" => { "x-message-ttl" => 60_000,
                             "x-dead-letter-exchange" => "acemq.schedule",
                             "x-dead-letter-routing-key" => "acemq.schedule.due" },
    "acemq.schedule.10s" => { "x-message-ttl" => 10_000,
                              "x-dead-letter-exchange" => "acemq.schedule",
                              "x-dead-letter-routing-key" => "acemq.schedule.due" },
    "acemq.schedule.1s" => { "x-message-ttl" => 1000,
                             "x-dead-letter-exchange" => "acemq.schedule",
                             "x-dead-letter-routing-key" => "acemq.schedule.due" }
  }.freeze

  let(:url) { ENV.fetch("ACEMQ_TEST_BROKER", "amqp://guest:guest@localhost:5672") }
  let(:mq) { AceMQ::AMQP::Connection.open(url, origin: "rspec@rbit") }
  let(:target) { "rbit.schedule" }
  let(:arrivals) { "rbit.schedule.arrivals" }

  before do
    mq.declare_exchange(target, kind: :direct, durable: true)
    mq.declare_queue(arrivals, queue_type: :classic, durable: true, arguments: {})
    mq.bind(queue: arrivals, exchange: target, routing_key: "invoice.due")
    drain(arrivals)
  end

  after do
    mq.delete_queue(arrivals)
    scrub_scheduler
    mq.close
  end

  def wait_for(seconds: 20)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    loop do
      found = yield
      return found if found
      break if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep(0.05)
    end
    nil
  end

  def drain(queue)
    loop do
      delivery = mq.pull(queue)
      break if delivery.nil?

      delivery.ack
    end
  end

  # The scheduler's queues are durable and shared by name, so a run that left
  # them behind would be a run the next one inherits messages from.
  def scrub_scheduler
    SCHEDULER::RUNGS.map { |rung| SCHEDULER.rung_name(rung) }.push(SCHEDULER::CONTROL)
                    .each do |queue|
      mq.delete_queue(queue)
    rescue StandardError
      nil
    end
    mq.transport.delete_exchange(SCHEDULER::EXCHANGE)
  rescue StandardError
    nil
  end

  describe "delivering later" do
    it "holds a message back and then delivers it, bytes and content type intact" do
      scheduler = SCHEDULER.on(mq)
      begin
        scheduler.in(5, { "invoice" => "A-1", "cents" => 12_345 },
                     to: "invoice.due", exchange: target)

        # Not early. The message is hopping through one-second rungs and there
        # is nothing on the target queue yet -- which is the assertion a
        # scheduler exists to satisfy, and the one a per-message expiration on
        # a single queue quietly breaks under mixed load.
        sleep(2)
        expect(mq.message_count(arrivals)).to eq(0)

        delivery = wait_for { mq.pull(arrivals) }
        expect(delivery).not_to be_nil
        expect(delivery.body).to eq('{"invoice":"A-1","cents":12345}')
        expect(delivery.content_type).to eq("application/json")
        # The scheduler's bookkeeping does not reach the consumer that was
        # waiting: what arrives is a message, not a message about scheduling.
        expect(delivery.headers.keys.grep(/\Ax-schedule-/)).to be_empty
        delivery.ack

        expect(scheduler.scheduled).to eq(1)
        expect(scheduler.delivered).to eq(1)
        expect(scheduler.hops).to be >= 4
      ensure
        scheduler.close
      end
    end

    it "does not leak a dead-letter queue for its control queue" do
      scheduler = SCHEDULER.on(mq)
      begin
        expect(mq.queue_exists?("acemq.schedule.due")).to be(true)
        # A consumer declares {queue}.dlq and {queue}.parked at start-up. The
        # control consumer is subscribed on the transport for exactly this
        # reason: two queues per deployment that nothing writes to and nobody
        # reads is a poor trade for a queue whose messages this class put there
        # itself.
        expect(mq.queue_exists?("acemq.schedule.due.dlq")).to be(false)
        expect(mq.queue_exists?("acemq.schedule.due.parked")).to be(false)
      ensure
        scheduler.close
      end
    end
  end

  describe "the topology Java would declare" do
    before { SCHEDULER.declare(mq) }

    it "is accepted by a second connection declaring Java's literal table" do
      java = AceMQ::AMQP::Connection.open(url, origin: "java@rbit")
      begin
        JAVA_RUNG_ARGUMENTS.each do |queue, arguments|
          expect { java.transport.declare_queue(queue, durable: true, arguments: arguments) }
            .not_to raise_error
        end
        expect do
          java.transport.declare_queue("acemq.schedule.due", durable: true, arguments: {})
        end.not_to raise_error
      ensure
        java.close
      end
    end

    it "refuses a table that differs by one argument" do
      java = AceMQ::AMQP::Connection.open(url, origin: "java@rbit")
      begin
        wrong = JAVA_RUNG_ARGUMENTS["acemq.schedule.1m"].merge("x-message-ttl" => 30_000)

        expect do
          java.transport.declare_queue("acemq.schedule.1m", durable: true, arguments: wrong)
        end
          .to raise_error(AceMQ::AMQP::TransportError, /PRECONDITION_FAILED/)
      ensure
        java.close
      end
    end

    it "is six queues and one exchange, and nothing else" do
      expect(SCHEDULER.declare(mq))
        .to eq(["acemq.schedule.1h", "acemq.schedule.10m", "acemq.schedule.1m",
                "acemq.schedule.10s", "acemq.schedule.1s", "acemq.schedule.due"])
      SCHEDULER.declare(mq).each { |queue| expect(mq.queue_exists?(queue)).to be(true) }
    end
  end
end
