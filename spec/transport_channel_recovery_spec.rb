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

# What a reconnect does to the publishing ceiling.
#
# A publish that is waiting for its confirm when the connection dies keeps its
# permit, and it must: the broker may yet have the message, so nothing here can say
# it did not arrive. The permit can only be freed by something that knows the
# connection itself is gone.
#
# That something used to be the opening of a new publishing channel -- and bunny
# recovers a session by re-opening the *same* channel objects on the new transport, so
# no new channel is ever created and no permit was ever given back. The ceiling shrank
# by every in-flight publish at every reconnect, and a connection that reconnected
# enough times would refuse every publish with "1000 publishes are already waiting for
# a confirm" on a perfectly healthy broker.
#
# Found while investigating a Ruby client that stopped publishing after a fault drill
# restarted a broker node. That failure is *not* explained by this: a single publish
# at a time leaks at most one permit per reconnect, and reproducing the drill's wedge
# in isolation did not reproduce it at all. This is a real defect on its own evidence,
# and the drill's finding remains open.

# A channel that reports open until somebody closes it deliberately, which is what
# bunny's own channels do -- a connection lost at the socket level closes no channel.
class StubChannel
  attr_reader :confirms_enabled

  def initialize
    @open = true
    @confirms_enabled = false
  end

  def open? = @open
  def close = @open = false
  def confirm_select = @confirms_enabled = true
end

# A session that can be recovered, like bunny's.
class RecoverableSession
  def initialize(channel)
    @channel = channel
    @open = true
  end

  def open? = @open
  def create_channel(*) = @channel
  def close = nil

  def after_recovery_completed(&block) = @on_recovered = block

  # The connection drops and comes back, as bunny does it: the same channel, still
  # reporting open, and a callback once it is usable again.
  def recover!
    @open = false
    @open = true
    @on_recovered&.call
  end
end

RSpec.describe AceMQ::AMQP::Transport do
  subject(:transport) { described_class.new(session, max_outstanding_publishes: 4) }

  let(:channel) { StubChannel.new }
  let(:session) { RecoverableSession.new(channel) }

  def permits = transport.send(:instance_variable_get, :@permits)

  # Takes permits the way a publish that never got its confirm does: the message is
  # on the wire and the broker has said nothing, so the permit stays held.
  def strand(count)
    count.times { expect(permits.take).to be(true) }
  end

  it "starts with its whole ceiling available" do
    expect(permits.available).to eq(4)
  end

  it "gives back the permits of publishes the connection died under" do
    strand(3)
    expect(permits.available).to eq(1)

    session.recover!

    expect(permits.available).to eq(4)
  end

  # The failure as it was actually seen: enough reconnects with messages in flight
  # and the ceiling reaches zero, after which nothing can publish again.
  it "does not shrink the ceiling a little with every reconnect" do
    3.times do
      strand(2)
      session.recover!
    end

    expect(permits.available).to eq(4)
    expect(permits.take).to be(true)
  end

  it "leaves the ceiling alone while the connection is up" do
    strand(2)
    expect(permits.available).to eq(2)
  end

  # The guard has to survive a session that does not offer the hook, because one of
  # them is a test double in this very suite.
  it "connects to a session with no recovery callback" do
    plain = Class.new do
      def initialize(channel) = @channel = channel
      def open? = true
      def create_channel(*) = @channel
      def close = nil
    end.new(channel)

    expect { described_class.new(plain) }.not_to raise_error
  end
end
