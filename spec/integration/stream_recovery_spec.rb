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
require "logger"
require "acemq/amqp"

# A stream reader across a lost connection.
#
# bunny re-subscribes a recovered consumer with the arguments it was first made
# with, so a stream reader asked for its original x-stream-offset again: one that
# began at "first" was handed the whole stream a second time, and one that began at
# "next" never saw what was appended while it was away. Measured before the fix, on
# bunny 2.24 and 3.4 alike: 500 entries read from "first" became 900 deliveries, and
# 50 of 200 read from "next" never arrived.
#
# The connection is dropped by closing bunny's socket under it, which bunny treats as
# any other network failure and which needs no management API.
RSpec.describe "a stream reader whose connection is lost", :integration do
  def broker = ENV.fetch("ACEMQ_TEST_BROKER", "amqp://guest:guest@localhost:5672")

  let(:stream) { "rbit.stream-recovery.#{SecureRandom.hex(4)}" }
  let(:publisher) { AceMQ::AMQP::Transport.open(broker) }
  let(:reader) do
    AceMQ::AMQP::Transport.open(broker, network_recovery_interval: 1,
                                        logger: Logger.new(File::NULL))
  end
  let(:seen) { Hash.new(0) }
  let(:lock) { Mutex.new }

  before { publisher.declare_queue(stream, queue_type: :stream) }

  after do
    [reader, publisher].each do |transport|
      transport.close
    rescue StandardError
      nil
    end
    begin
      AceMQ::AMQP::Transport.open(broker).tap { |t| t.delete_queue(stream) }.close
    rescue StandardError
      nil
    end
  end

  def append(count)
    count.times { |i| publisher.publish(exchange: "", routing_key: stream, body: "e#{i}") }
  end

  def read_from(start)
    arguments = { "x-stream-offset" => start }
    reader.subscribe(stream, prefetch: 10, arguments: arguments) do |delivery|
      lock.synchronize { seen[delivery.headers["x-stream-offset"]] += 1 }
      delivery.ack
    end
  end

  def deliveries = lock.synchronize { seen.values.sum }
  def distinct = lock.synchronize { seen.size }

  def wait_for(count, seconds: 30)
    deadline = Time.now + seconds
    sleep 0.05 until deliveries >= count || Time.now > deadline
  end

  # Drops the reader's connection, appends while it is down, and waits for the
  # recovered subscription to catch up -- then a little longer, so a replay has
  # time to show itself.
  def lose_the_connection_and_append(count, expected)
    reader.session.transport.socket.close
    append(count)
    wait_for(expected)
    sleep 2
  end

  it "does not read the stream again from the start when it began at first" do
    append(20)
    read_from("first")
    wait_for(20)

    lose_the_connection_and_append(10, 30)

    expect([deliveries, distinct]).to eq([30, 30])
  end

  it "does not skip what was appended while it was away when it began at next" do
    read_from("next")
    sleep 0.5
    append(20)
    wait_for(20)

    lose_the_connection_and_append(10, 30)

    expect([deliveries, distinct]).to eq([30, 30])
  end

  # The control: a queue consumer recovers exactly as it always has.
  it "leaves a queue consumer's recovery alone" do
    queue = "#{stream}.queue"
    publisher.declare_queue(queue)
    begin
      reader.subscribe(queue, prefetch: 10) do |delivery|
        lock.synchronize { seen[delivery.body] += 1 }
        delivery.ack
      end
      5.times { |i| publisher.publish(exchange: "", routing_key: queue, body: "before#{i}") }
      wait_for(5)

      reader.session.transport.socket.close
      5.times { |i| publisher.publish(exchange: "", routing_key: queue, body: "during#{i}") }
      wait_for(10)

      expect(distinct).to eq(10)
    ensure
      publisher.delete_queue(queue)
    end
  end
end
