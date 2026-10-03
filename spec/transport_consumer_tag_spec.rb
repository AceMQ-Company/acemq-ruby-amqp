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

# The consumer tag a subscription goes out with, and why it cannot be the broker's.
#
# bunny 3.x records every consumer for topology recovery and keys the record by tag.
# Recovery re-subscribes, which records the consumer again — so with a broker-assigned
# tag the new registration lands under a new key and the registry *grows* rather than
# being replaced. Measured against a three-node cluster with every connection forced
# shut: 1, then 2, then 4 recorded consumers over three recoveries.
#
# Each recorded consumer then gets its own `maybe_reinitialize_consumer_pool!`, and all
# but the last of those pools is abandoned with its threads still parked in
# `ConsumerWorkPool#run_loop`. That is the thread growth a soak found in the Ruby
# standing load: 10 threads to 168 over 240 recoveries, while the Go, Java, Python and
# .NET loads stayed flat under the identical fault.
#
# A plain bunny consumer does not grow this way — its recorded count held at 1 over the
# same faults — which is what showed the tag was ours to fix rather than bunny's. With
# a tag of our own the count holds at 1, one pool stays live, and 20 recoveries leave
# the thread count where it started.
RSpec.describe AceMQ::AMQP::Transport, "the consumer tag a subscription goes out with" do
  # Captures what `subscribe` was asked for, and nothing else.
  let(:queue_double) do
    Class.new do
      attr_reader :options

      def subscribe(**options, &block)
        @options = options
        @block = block
        :a_consumer
      end
    end.new
  end

  let(:channel) do
    queue = queue_double
    Class.new do
      define_method(:initialize) { |q| @queue = q }
      def open? = true
      def close = nil
      def prefetch(_count) = nil
      def queue(_name, **_options) = @queue
    end.new(queue)
  end

  let(:session) do
    channel_object = channel
    Class.new do
      define_method(:initialize) { |ch| @channel = ch }
      def open? = true
      def close = nil
      def create_channel(*) = @channel
    end.new(channel_object)
  end

  subject(:transport) { described_class.new(session) }

  def subscribe(**options)
    transport.subscribe("orders", **options) { |_delivery| nil }
    queue_double.options
  end

  # The bug itself: `consumer_tag: nil` leaves the tag to the broker, and a
  # broker-assigned tag changes on every re-subscribe.
  it "never leaves the tag to the broker" do
    expect(subscribe[:consumer_tag]).to be_a(String)
    expect(subscribe[:consumer_tag]).not_to be_empty
  end

  it "names the library and the queue, so a tag in a management UI says what it is" do
    tag = subscribe[:consumer_tag]

    expect(tag).to start_with("acemq-")
    expect(tag).to include("orders")
  end

  # Two consumers on one queue are an ordinary arrangement — two processes, or two
  # subscriptions in one — and they must not collide on a tag.
  it "gives each subscription its own tag" do
    first = subscribe[:consumer_tag]
    second = subscribe[:consumer_tag]

    expect(first).not_to eq(second)
  end

  it "uses the caller's tag unchanged when there is one" do
    expect(subscribe(tag: "stocktake-1")[:consumer_tag]).to eq("stocktake-1")
  end

  # The rest of the subscription contract, so a change to the tag cannot quietly
  # change how deliveries are settled.
  it "still subscribes with manual acknowledgement and without blocking" do
    options = subscribe

    expect(options[:manual_ack]).to be(true)
    expect(options[:block]).to be(false)
  end
end
