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

require "securerandom"

require "acemq/amqp"

# The drain deadline, against bunny rather than against a fake.
#
# The unit specs in spec/shutdown_spec.rb prove the arithmetic and could not see
# this: cancelling the last consumer on a bunny channel shuts that channel's work
# pool down and waits for the busy worker -- up to sixty seconds by default --
# before {AceMQ::AMQP::Connection#close} ever looked at its own deadline. A fake
# subscription returns from +stop+ at once, so every one of those specs passed
# while a real close(timeout: 0.5) on a three-second handler took three seconds.
RSpec.describe "a drain against a real broker", :integration do
  def broker = ENV.fetch("ACEMQ_TEST_BROKER", "amqp://guest:guest@localhost:5672")
  # How long every handler here takes: long enough that a drain which waited
  # for it cannot be mistaken for one that kept a half-second deadline.
  def hold = 3.0

  let(:mq) { AceMQ::AMQP::Connection.open(broker, origin: "rspec@rbit") }
  let(:queues) { Array.new(3) { |i| "rbit.drain.#{i}.#{SecureRandom.hex(4)}" } }
  let(:started) { Queue.new }
  let(:finished) { Queue.new }

  def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  # A connection of its own, so the one under test can be closed and the
  # broker still asked what it is holding.
  def inspector = @inspector ||= AceMQ::AMQP::Transport.open(broker)

  before do
    queues.each { |queue| inspector.declare_queue(queue) }
  end

  after do
    begin
      mq.close(timeout: 0)
    rescue StandardError
      nil
    end
    queues.each do |queue|
      %W[#{queue} #{queue}.dlq #{queue}.parked].each { |name| inspector.delete_queue(name) }
    rescue StandardError
      nil
    end
    inspector.close
  end

  # A consumer whose handler takes +hold+ seconds, with a message already
  # inside it by the time this returns.
  def busy(queue, outcome: AceMQ::AMQP::Ack.accept)
    mq.consume(queue) do
      started << queue
      sleep(hold)
      finished << queue
      outcome
    end
  end

  # Queue#pop(timeout:) is Ruby 3.2; this suite still runs on 3.1.
  def pop_within(queue, seconds)
    deadline = now + seconds
    loop do
      return queue.pop(true)
    rescue ThreadError
      return nil if now > deadline

      sleep(0.01)
    end
  end

  def wait_until_started(count)
    count.times { pop_within(started, 10) || raise("a handler never started") }
  end

  # Times a close, and hands back the error it raised.
  def timed_close(timeout)
    began = now
    error = begin
      mq.close(timeout: timeout)
      nil
    rescue AceMQ::AMQP::DrainTimeout => e
      e
    end
    [now - began, error]
  end

  it "honours the deadline with one busy consumer" do
    busy(queues[0])
    mq.publish({ "n" => 1 }, to: queues[0])
    wait_until_started(1)

    elapsed, error = timed_close(0.5)

    expect(error).to be_a(AceMQ::AMQP::DrainTimeout)
    expect(elapsed).to be >= 0.5
    expect(elapsed).to be < 1.5
  end

  it "spends one deadline across several busy consumers" do
    queues.each do |queue|
      busy(queue)
      mq.publish({ "n" => 1 }, to: queue)
    end
    wait_until_started(3)

    elapsed, error = timed_close(0.5)

    expect(error.stranded).to eq(queues.to_h { |queue| [queue, 1] })
    expect(elapsed).to be < 1.5
  end

  it "does not wait at all when given no time" do
    busy(queues[0])
    mq.publish({ "n" => 1 }, to: queues[0])
    wait_until_started(1)

    elapsed, error = timed_close(0)

    expect(error).to be_a(AceMQ::AMQP::DrainTimeout)
    expect(elapsed).to be < 0.5
  end

  it "reports the deadline it actually kept" do
    busy(queues[0])
    mq.publish({ "n" => 1 }, to: queues[0])
    wait_until_started(1)

    elapsed, error = timed_close(0.5)

    # "within 0.5s" is only true if it came back inside about half a second.
    expect(error.message).to match(/did not finish within 0\.5s: 1 delivery was left unsettled/)
    expect(elapsed).to be < 1.5
  end

  it "bounds Consumer#cancel the same way" do
    consumer = busy(queues[0])
    mq.publish({ "n" => 1 }, to: queues[0])
    wait_until_started(1)

    began = now
    consumer.cancel(timeout: 0.5)

    expect(now - began).to be < 1.5
    expect(consumer.in_flight).to eq(1)
    expect(consumer).not_to be_running
  end

  it "leaves a stranded message for the broker to redeliver" do
    busy(queues[0])
    mq.publish({ "n" => 1 }, to: queues[0])
    wait_until_started(1)

    timed_close(0.5)
    # The handler outlives the drain. Once it has finished, the message must
    # still be on the queue: neither acknowledged late, nor rejected.
    pop_within(finished, hold + 5) || raise("the stranded handler never finished")
    sleep(0.2)

    channel = inspector.session.create_channel
    info, = channel.basic_get(queues[0], manual_ack: false)
    channel.close
    expect(info).not_to be_nil
    expect(info.redelivered).to be(true)
  end

  it "does not start a delivery that arrived before the cancel and was still waiting" do
    busy(queues[0])
    2.times { |n| mq.publish({ "n" => n }, to: queues[0]) }
    wait_until_started(1)

    timed_close(0.2)
    pop_within(finished, hold + 5)
    sleep(0.5)

    # Only the first one was ever handed to the handler; the second went back.
    expect(started.size).to eq(0)
    expect(inspector.message_count(queues[0])).to eq(2)
  end

  it "does not dead-letter a message the drain gave up on" do
    # The connection stays open, so a dead letter could be published; it must
    # not be, because the broker already has the original back.
    consumer = busy(queues[0], outcome: AceMQ::AMQP::Ack.reject("refused"))
    mq.publish({ "n" => 1 }, to: queues[0])
    wait_until_started(1)

    consumer.cancel(timeout: 0.2)
    pop_within(finished, hold + 5) || raise("the stranded handler never finished")
    sleep(0.5)

    expect(inspector.message_count("#{queues[0]}.dlq")).to eq(0)
    expect(inspector.message_count(queues[0])).to eq(1)
  end

  it "stops every consumer before waiting for any of them" do
    # The second consumer is idle when the drain starts. Its message arrives
    # while the first one's handler is being waited for, and must not be
    # started: a consumer still subscribed during somebody else's wait is
    # being handed work the deadline will strand.
    busy(queues[0])
    busy(queues[1])
    mq.publish({ "n" => 1 }, to: queues[0])
    wait_until_started(1)
    late = Thread.new do
      sleep(0.2)
      mq.publish({ "n" => 2 }, to: queues[1])
    end

    timed_close(0.6)
    late.join

    expect(started.size).to eq(0)
  end
end
