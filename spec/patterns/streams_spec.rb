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

require "acemq/amqp/patterns"

RSpec.describe "streams" do
  let(:transport) { LoopbackTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }

  describe "declaring one" do
    it "asks for a queue that keeps its messages" do
      AceMQ::AMQP::Patterns.declare_stream(mq, "events")

      name, options = transport.declared_queues.first
      expect(name).to eq("events")
      expect(options[:arguments]).to eq({ "x-queue-type" => "stream" })
    end

    it "is durable and neither exclusive nor auto-deleting" do
      # A stream cannot be any of those, and the broker's refusal does not
      # mention streams — it reads like a bug in the caller's own code.
      AceMQ::AMQP::Patterns.declare_stream(mq, "events")

      _name, options = transport.declared_queues.first
      expect(options).to include(durable: true, auto_delete: false, exclusive: false)
    end

    it "sets whatever retention it was given" do
      # Unbounded means "until the disk is full", which is a mistake an ordinary
      # queue cannot make because it forgets what it delivers.
      AceMQ::AMQP::Patterns.declare_stream(mq, "events", max_age: 7 * 24 * 3600,
                                                         max_bytes: 1024, segment_bytes: 512)

      _name, options = transport.declared_queues.first
      expect(options[:arguments]).to eq(
        "x-queue-type" => "stream", "x-max-age" => "7D",
        "x-max-length-bytes" => 1024, "x-stream-max-segment-size-bytes" => 512
      )
    end

    it "writes an age the way RabbitMQ wants it, with a unit on the end" do
      # A bare number is refused, and the refusal does not say what unit it
      # wanted.
      expect(AceMQ::AMQP::Patterns.duration_argument(2 * 86_400)).to eq("2D")
      expect(AceMQ::AMQP::Patterns.duration_argument(3600)).to eq("1h")
      expect(AceMQ::AMQP::Patterns.duration_argument(90 * 60)).to eq("90m")
      expect(AceMQ::AMQP::Patterns.duration_argument(45)).to eq("45s")
    end
  end

  describe "where to start reading" do
    it "names the positions the broker understands" do
      offsets = AceMQ::AMQP::Patterns::StreamOffset
      expect(offsets.first.to_argument).to eq("first")
      expect(offsets.next.to_argument).to eq("next")
      expect(offsets.last.to_argument).to eq("last")
    end

    it "carries an exact offset as a number rather than a word" do
      # Which is why this is a value object and not a string: a caller passing
      # the wrong shape gets an error from the broker that does not mention
      # streams.
      offset = AceMQ::AMQP::Patterns::StreamOffset.at(4096)

      expect(offset.to_argument).to eq(4096)
      expect(offset.to_s).to eq("offset(4096)")
    end

    it "carries a timestamp as a time" do
      at = Time.now
      expect(AceMQ::AMQP::Patterns::StreamOffset.since(at).to_argument).to eq(at)
    end
  end

  describe "reading one" do
    it "tells the broker where to start" do
      AceMQ::AMQP::Patterns.read_stream(mq, "events",
                                        offset: AceMQ::AMQP::Patterns::StreamOffset.first) do
        AceMQ::AMQP::Ack.accept
      end

      _queue, options = transport.subscriptions.first
      expect(options[:arguments]).to eq({ "x-stream-offset" => "first" })
    end

    it "always sends a prefetch, because RabbitMQ refuses a stream consumer without one" do
      # And the error it gives does not explain why.
      AceMQ::AMQP::Patterns.read_stream(mq, "events") { AceMQ::AMQP::Ack.accept }

      _queue, options = transport.subscriptions.first
      expect(options[:prefetch]).to eq(AceMQ::AMQP::Patterns::DEFAULT_STREAM_PREFETCH)
      expect(options[:prefetch]).to be_positive
    end

    it "names the consumer when asked, which is what lets the broker track its offset" do
      AceMQ::AMQP::Patterns.read_stream(mq, "events", name: "projection-1") do
        AceMQ::AMQP::Ack.accept
      end

      _queue, options = transport.subscriptions.first
      expect(options[:tag]).to eq("projection-1")
    end

    it "reads with no retry policy, whatever the connection carries" do
      # A retry republishes, and republishing onto a stream appends a second
      # copy rather than redelivering the first — so a projection reading that
      # stream would see the message twice.
      retrying = AceMQ::AMQP::Connection.new(transport: transport,
                                             retry_policy: AceMQ::AMQP::RetryPolicy.fixed(5, 0))
      consumer = AceMQ::AMQP::Patterns.read_stream(retrying, "events") do
        AceMQ::AMQP::Ack.retry("no")
      end

      expect(consumer.retry_policy.max_attempts).to eq(1)
    end

    it "hands messages to the handler like any other consumer" do
      seen = []
      AceMQ::AMQP::Patterns.read_stream(mq, "events") do |message|
        seen << message.payload
        AceMQ::AMQP::Ack.accept
      end
      mq.publish({ "n" => 1 }, to: "events")

      expect(seen).to eq([{ "n" => 1 }])
    end

    it "needs a block to handle messages" do
      expect { AceMQ::AMQP::Patterns.read_stream(mq, "events") }
        .to raise_error(ArgumentError, /needs a block/)
    end
  end
end
