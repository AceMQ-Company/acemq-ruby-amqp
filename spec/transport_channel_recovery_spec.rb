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
#
# The drill's finding has since been chased down and is not ours: under repeated
# forced recovery, bunny 2.24.0 accumulates threads blocked on its own
# +@channel_mutex+ and eventually stops publishing. Reproduced with no AceMQ code in
# the path at all -- see the note at the bottom of this file.

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

  def before_recovery_attempt_starts(&block) = @on_start = block
  def after_recovery_completed(&block) = @on_recovered = block

  # The connection drops and comes back, as bunny does it: the same channel, still
  # reporting open, an attempt that starts, and a completion once it is usable again.
  def recover!
    @open = false
    @on_start&.call
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

# Publishing *during* a recovery is what destroyed the connection being recovered.
#
# Bunny's recovery has two steps and only the first shows in `open?`: the socket and
# the AMQP handshake come back, and each channel is re-opened after that. A frame
# written in between lands on a channel the broker has never seen, and the broker
# answers CHANNEL_ERROR "expected 'channel.open'" by closing the whole connection --
# so the publisher killed the connection bunny had just rebuilt, over and over,
# without ever converging.
RSpec.describe AceMQ::AMQP::Transport, "while the connection is recovering" do
  subject(:transport) { described_class.new(session) }

  # A session in the middle of a recovery: open, because the handshake is done, and
  # recovering, because its channels are not back yet.
  let(:session) do
    Class.new do
      attr_writer :recovering

      def initialize(channel)
        @channel = channel
        @recovering = false
      end

      def open? = true
      def create_channel(*) = @channel
      def close = nil
      def before_recovery_attempt_starts(&block) = @on_start = block
      def after_recovery_completed(&block) = @on_recovered = block

      # Drives the window the way bunny does: the attempt starts, and completion
      # only arrives once the channels are back.
      def start_recovering = @on_start&.call
      def finish_recovering = @on_recovered&.call
    end.new(StubChannel.new)
  end

  it "refuses to publish, saying the message did not go and a retry will" do
    transport # built first, because it is what registers the recovery callbacks
    session.start_recovering

    expect { transport.send(:publish_channel) { |c| c } }
      .to raise_error(AceMQ::AMQP::TransportError, /not open yet.*was not sent.*retry/m)
  end

  # Not sent and safe to retry is exactly what PublishingPausedError means, so a
  # caller branching on it -- or a load counting refused against failed -- must not
  # have to know that a recovery, rather than a blocked broker, was the reason.
  it "declines with the paused type, which is still a PublishError and a TransportError" do
    transport
    session.start_recovering

    expect { transport.publish(exchange: "x", routing_key: "k", body: "b", message_id: "m-1") }
      .to raise_error(AceMQ::AMQP::PublishingPausedError) { |e|
        expect(e).to be_a(AceMQ::AMQP::PublishError)
        expect(e).to be_a(AceMQ::AMQP::TransportError)
      }
  end

  it "answers every message of a batch as paused rather than raising" do
    transport
    session.start_recovering

    results = transport.publish_all([
                                      { exchange: "x", routing_key: "a", body: "1",
                                        message_id: "m-1" },
                                      { exchange: "x", routing_key: "b", body: "2",
                                        message_id: "m-2" }
                                    ])
    expect(results.size).to eq(2)
    expect(results).to all(be_a(AceMQ::AMQP::PublishingPausedError))
  end

  it "publishes normally once the recovery is over" do
    transport
    session.start_recovering
    expect { transport.send(:publish_channel) { |c| c } }.to raise_error(AceMQ::AMQP::TransportError)

    session.finish_recovering
    expect(transport.send(:publish_channel) { |c| c }).to be_a(StubChannel)
  end

  it "says nothing about recovery to a session that cannot be asked" do
    plain = Class.new do
      def initialize(channel) = @channel = channel
      def open? = true
      def create_channel(*) = @channel
      def close = nil
    end.new(StubChannel.new)

    expect { described_class.new(plain).send(:publish_channel) { |c| c } }.not_to raise_error
  end
end

# The threads a replaced channel holds.
#
# A bunny channel carries a consumer work pool -- one thread by default -- and both the
# publishing channel and the pulling channel are replaced whenever the old one is no
# longer open. Replacing without closing leaks that pool and everything it holds.
#
# Scope, stated plainly because this was first written believing it was more: bunny
# usually recovers a session by re-opening the *same* channel objects, so an ordinary
# reconnect does not reach the replace branch at all, and a standing load measured with
# and without the fix grew threads identically. The leak closed here is on the paths
# that really do replace a channel -- one the broker closed for a channel-level error,
# or a pull channel dropped between passes.
#
# The thread growth a soak found under repeated recovery is bunny 2.24.0's own: a client
# built on bunny with none of this library in the path went from 6 threads to 146 over
# 90 forced recoveries and stopped publishing and consuming entirely, where this library
# over the same 90 reached 49 and kept both directions moving.
#
# The stubs here hand out a *new* channel per call, unlike StubChannel above. That
# matters: a session that returns the same channel object every time cannot show a
# channel being abandoned, which is why the suite had not covered this.
RSpec.describe AceMQ::AMQP::Transport, "the threads a replaced channel holds" do
  # A channel that records what was done to it on the way out, and that can be told
  # its connection died -- the state a socket-level failure leaves a channel in.
  class CountingChannel
    attr_reader :closed, :pool_killed

    def initialize(raise_on_close: false)
      @open = true
      @closed = false
      @pool_killed = false
      @raise_on_close = raise_on_close
    end

    def open? = @open
    def confirm_select = nil
    def prefetch(*) = nil

    def close
      @closed = true
      @open = false
      raise "the connection this channel belonged to is gone" if @raise_on_close
    end

    def maybe_kill_consumer_work_pool! = @pool_killed = true

    # What a lost connection does: the channel stops being usable and nothing closed
    # it deliberately.
    def die! = @open = false
  end

  # A session that opens a fresh channel every time it is asked, as bunny does when a
  # channel is created rather than recovered.
  class ChannelPerCallSession
    attr_reader :channels

    def initialize(raise_on_close: false)
      @channels = []
      @raise_on_close = raise_on_close
    end

    def open? = true
    def close = nil

    def create_channel(*)
      @channels << CountingChannel.new(raise_on_close: @raise_on_close)
      @channels.last
    end
  end

  subject(:transport) { described_class.new(session) }

  let(:session) { ChannelPerCallSession.new }

  def publish_once = transport.send(:publish_channel) { |c| c }
  def pull_once = transport.send(:pull_channel)

  it "stops the work pool of the publishing channel it replaces" do
    publish_once
    first = session.channels.first
    first.die!

    publish_once

    expect(session.channels.size).to eq(2)
    expect(first.pool_killed).to be(true)
  end

  # The failure as the soak measured it: one thread per reconnect, for as long as the
  # process runs.
  it "does not leave a work pool behind on every reconnect" do
    10.times do
      publish_once
      session.channels.last.die!
    end

    # Ten channels were opened and nine of them replaced: the tenth died at the end of
    # the loop with nothing asking for a channel afterwards, so it is still the
    # current one rather than an abandoned one.
    expect(session.channels.size).to eq(10)
    abandoned = session.channels[0..-2]
    expect(abandoned.size).to eq(9)
    expect(abandoned.map(&:pool_killed)).to all(be(true))
  end

  it "stops the work pool of the pulling channel it replaces" do
    pull_once
    first = session.channels.first
    first.die!

    pull_once

    expect(first.pool_killed).to be(true)
  end

  # Closing a channel whose connection has gone raises, and that is the ordinary case
  # here rather than the exceptional one. It must cost a thread at worst, never a
  # publish.
  context "when closing the old channel raises" do
    let(:session) { ChannelPerCallSession.new(raise_on_close: true) }

    it "still replaces it, and still stops its work pool" do
      publish_once
      first = session.channels.first
      first.die!

      expect { publish_once }.not_to raise_error
      expect(first.pool_killed).to be(true)
    end
  end

  # A channel that predates this fix, or any other object standing in for one, has no
  # such method. Losing the cleanup is acceptable; raising is not.
  it "accepts a channel that cannot be asked to stop its pool" do
    bare = Class.new do
      def initialize = @open = true
      def open? = @open
      def close = @open = false
      def confirm_select = nil
      def die! = @open = false
    end

    plain = Class.new(ChannelPerCallSession) do
      def initialize(factory)
        super()
        @factory = factory
      end

      def create_channel(*)
        channels << @factory.new
        channels.last
      end
    end.new(bare)

    transport = described_class.new(plain)
    transport.send(:publish_channel) { |c| c }
    plain.channels.first.die!

    expect { transport.send(:publish_channel) { |c| c } }.not_to raise_error
  end
end
