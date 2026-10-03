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

# Back pressure told apart from a failed publish.
#
# A publish on a connection the broker has blocked is declined before anything is
# written, and the error says so by its type: {AceMQ::AMQP::PublishingPausedError}
# means "not sent, safe to send again", where a plain {AceMQ::AMQP::PublishError}
# may mean a message the broker has and never confirmed. Go, .NET and Java already
# make the same split, and a load or a caller that cannot make it has to count back
# pressure as loss.
RSpec.describe AceMQ::AMQP::PublishingPausedError do
  # A publishing channel that answers every confirm at once, the way it is told to.
  let(:channel_class) do
    Class.new do
      attr_reader :sent
      attr_accessor :acks, :refuses

      def initialize
        @sent = []
        @acks = true
        @refuses = false
        @seq = 1
      end

      def open? = true
      def close = nil
      def confirm_select = nil
      def next_publish_seq_no = @seq
      def nacked_set = Set.new
      def unconfirmed_set = Set.new
      def wait_for_confirms = @acks

      def basic_publish(body, *_rest)
        raise IOError, "the socket went away" if @refuses

        @sent << body
        @seq += 1
      end
    end
  end

  # Enough of a bunny session for the blocked flag and its two callbacks.
  let(:session_class) do
    Class.new do
      def initialize(channel)
        @channel = channel
        @blocked = false
      end

      def create_channel(*) = @channel
      def blocked? = @blocked
      def on_blocked(&block) = @on_blocked = block
      def on_unblocked(&block) = @on_unblocked = block

      def block!(reason)
        @blocked = true
        @on_blocked.call(Struct.new(:reason).new(reason))
      end

      def unblock!
        @blocked = false
        @on_unblocked.call(nil)
      end
    end
  end

  let(:channel) { channel_class.new }
  let(:session) { session_class.new(channel) }
  let!(:transport) { AceMQ::AMQP::Transport.new(session) }
  let(:message) { { exchange: "", routing_key: "orders", body: "x", message_id: "m-1" } }

  it "is a publish error, so every existing rescue still catches it" do
    expect(described_class.ancestors).to include(AceMQ::AMQP::PublishError,
                                                 AceMQ::AMQP::TransportError)
  end

  describe "a single publish" do
    it "is declined on a blocked connection, with the reason, and nothing is written" do
      session.block!("low on memory")

      expect { transport.publish(**message) }.to raise_error(described_class) { |e|
        expect(e).to be_a(AceMQ::AMQP::PublishError)
        expect(e).not_to be_unroutable
        expect(e.message).to include("low on memory", "m-1", "not sent")
      }
      expect(channel.sent).to be_empty
    end

    it "goes out again once the broker unblocks the connection" do
      session.block!("low on disk space")
      expect { transport.publish(**message) }.to raise_error(described_class)

      session.unblock!

      expect(transport.publish(**message)).to eq("m-1")
      expect(channel.sent).to eq(["x"])
    end

    it "is not what a confirm the broker refused raises" do
      channel.acks = false

      expect { transport.publish(**message) }.to raise_error(AceMQ::AMQP::PublishError) { |e|
        expect(e).not_to be_a(described_class)
      }
    end

    it "is not what a channel that would not take the message raises" do
      channel.refuses = true

      expect { transport.publish(**message) }.to raise_error(AceMQ::AMQP::PublishError) { |e|
        expect(e).not_to be_a(described_class)
      }
    end
  end

  describe "a batch" do
    it "answers every message as declined on a blocked connection, and writes none" do
      session.block!("low on memory")

      results = transport.publish_all([message, message.merge(message_id: "m-2")])

      expect(results.size).to eq(2)
      expect(results).to all(be_a(described_class))
      expect(channel.sent).to be_empty
    end

    it "does not answer a refused confirm as declined" do
      channel.acks = false
      allow(channel).to receive(:nacked_set).and_return(Set.new([1]))

      results = transport.publish_all([message])

      expect(results.first).to be_a(AceMQ::AMQP::PublishError)
      expect(results.first).not_to be_a(described_class)
    end
  end

  describe "Connection#publish_all" do
    # A transport that answers each message with whatever the test hands it.
    let(:answering) do
      Class.new do
        def initialize(answers) = @answers = answers
        def publish_all(_messages) = @answers
      end
    end

    let(:paused) { described_class.new("paused") }

    def connection_answering(*answers)
      AceMQ::AMQP::Connection.new(transport: answering.new(answers), origin: "spec")
    end

    it "raises the paused type when every failure in the batch was declined" do
      mq = connection_answering(paused, paused)

      expect { mq.publish_all([{ "n" => 1 }, { "n" => 2 }], to: "orders") }
        .to raise_error(described_class, /2 of 2 messages were not confirmed/)
    end

    it "raises a plain publish error when any failure may have been lost" do
      mq = connection_answering(paused, AceMQ::AMQP::PublishError.new("not confirmed"))

      expect { mq.publish_all([{ "n" => 1 }, { "n" => 2 }], to: "orders") }
        .to raise_error(AceMQ::AMQP::PublishError) { |e| expect(e).not_to be_a(described_class) }
    end
  end
end
