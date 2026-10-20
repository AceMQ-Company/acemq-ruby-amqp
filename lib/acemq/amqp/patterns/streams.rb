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

require_relative "../retry_policy"

module AceMQ
  module AMQP
    # A queue that keeps what it has delivered.
    #
    # The difference from an ordinary queue is the whole of it: a stream does
    # not remove a message when somebody reads it, so several consumers read the
    # same stream independently and a new one can start from the beginning. What
    # a consumer chooses is a position, not a place in a line.
    module Patterns
      # Where a stream consumer starts reading.
      #
      # A value object rather than a bare string because two of the five are not
      # strings — an exact offset is a number and a timestamp is a time — and a
      # caller passing the wrong one gets an error from the broker that does not
      # mention streams.
      class StreamOffset
        attr_reader :kind, :value

        def initialize(kind, value = nil)
          @kind = kind
          @value = value
          freeze
        end

        # The oldest message the stream **still holds**.
        #
        # For building a projection from scratch, or a new consumer that needs
        # the history. "Still holds" matters: a stream has a retention policy,
        # and its oldest message is not necessarily the first one ever written.
        def self.first = new("first")

        # The next message published, ignoring everything already there. The
        # default, and what most consumers want.
        def self.next = new("next")

        # The last chunk the stream holds, which is roughly "the recent past"
        # rather than an exact number of messages.
        def self.last = new("last")

        # An exact position, which is what a consumer that records its own
        # progress uses to carry on where it left off.
        def self.at(offset) = new("offset", offset)

        # The first message published at or after a time.
        def self.since(time) = new("timestamp", time)

        # What the broker is told.
        #
        # @api private
        def to_argument = @value.nil? ? @kind : @value

        def to_s = @value.nil? ? @kind : "#{@kind}(#{@value})"
      end

      # How much of a stream to keep.
      #
      # Unbounded by default, which for a stream means "until the disk is full".
      # Set at least one of these on anything that will run for long — a queue
      # forgets what it delivers and a stream does not, so the mistake this
      # prevents is one an ordinary queue cannot make.
      #
      # +segment_bytes+ is how large each file on disk gets. Retention happens a
      # segment at a time, so a very large segment means retention is coarse:
      # nothing is discarded until a whole segment can be.
      StreamRetention = Struct.new(:max_age, :max_bytes, :segment_bytes, keyword_init: true) do
        # @api private
        def to_arguments
          arguments = { "x-queue-type" => "stream" }
          arguments["x-max-age"] = Patterns.duration_argument(max_age) if max_age
          arguments["x-max-length-bytes"] = max_bytes if max_bytes
          arguments["x-stream-max-segment-size-bytes"] = segment_bytes if segment_bytes
          arguments
        end
      end

      # Declares a queue that keeps its messages.
      #
      #   Patterns.declare_stream(mq, "events", max_age: 7 * 24 * 3600,
      #                           max_bytes: 10 * 1024**3)
      #
      # A stream is durable and can be neither exclusive nor auto-deleting.
      # Those are set here rather than left to fail at the broker, whose refusal
      # does not mention streams and reads like a bug in the caller's own code.
      #
      # @param connection [Connection, Transport]
      # @param name [String]
      # @param max_age [Numeric, nil] seconds; discard messages older than this
      # @param max_bytes [Integer, nil] discard the oldest once this is exceeded
      # @param segment_bytes [Integer, nil] how large each file on disk gets
      def self.declare_stream(connection, name, max_age: nil, max_bytes: nil,
                              segment_bytes: nil)
        retention = StreamRetention.new(max_age: max_age, max_bytes: max_bytes,
                                        segment_bytes: segment_bytes)
        connection.declare_queue(name, durable: true, auto_delete: false, exclusive: false,
                                       arguments: retention.to_arguments)
      end

      # How many messages a stream consumer holds when nothing else is said.
      #
      # RabbitMQ refuses a stream consumer with no prefetch at all, and the
      # error it gives does not explain why, so there is always one.
      #
      # **The number is this library's choice and not part of the
      # cross-language contract.** Java and .NET default to 100 here, Go, Python
      # and Ruby to 10, and the disagreement is deliberate: prefetch trades
      # memory against throughput, the right answer depends on payload size and
      # handler speed, and both of those belong to the application rather than
      # to the protocol. A stream one library wrote is read by any of them
      # whatever this says — the offset, the retention arguments and the message
      # on the wire are the contract, and prefetch is a consumer-side setting
      # that never leaves the channel. Do not harmonise this with Java's; state
      # the number at the call site if it matters. See +docs/streams.md+.
      DEFAULT_STREAM_PREFETCH = 10

      # Reads a stream from a chosen position.
      #
      #   Patterns.read_stream(mq, "events", offset: Patterns::StreamOffset.first,
      #                        prefetch: 100) do |message|
      #     project(message.payload)
      #     Ack.accept
      #   end
      #
      # == How this differs from consuming a queue
      #
      # Acknowledging does not remove the message: a stream keeps everything
      # until its retention policy discards it. What an acknowledgement does is
      # advance this consumer's position, so restarting from +next+ carries on
      # rather than re-reading.
      #
      # == A handler that fails stops the reader
      #
      # By default (AMQP-STREAM-004) a handler that raises, or answers anything
      # but {Ack.accept}, stops the reader at that message: the subscription is
      # cancelled, nothing after the message is handled, and no copy of it is
      # written anywhere — not +{stream}.dlq+, not +{stream}.parked+, not the
      # stream. The stream is the record, so a reader restarted at the same
      # offset after the fix sees the message again; a projection with a gap
      # in it is invisible once it exists, a stopped reader is not.
      # {Consumer#running?} is then false and {Consumer#stopped_by} says why.
      #
      # +on_failure: :dead_letter+ is the opt-out, and was the default before
      # this release: the retry policy is {RetryPolicy.none} whatever the
      # connection carries, so a failed message is copied to +{stream}.dlq+
      # with +x-acemq-error+ and the reader carries on past it. To skip a
      # failure instead, rescue it in the handler and accept. Never a retry: a
      # republish onto a stream appends a second copy for every reader.
      #
      # @param connection [Connection]
      # @param stream [String]
      # @param offset [StreamOffset] where to start
      # @param prefetch [Integer] required by RabbitMQ for a stream
      # @param name [String, nil] identifies this consumer to the broker, which
      #   is what makes server-side offset tracking possible
      # @param on_failure [Symbol] +:stop+ (the default) or +:dead_letter+
      # @return [Consumer]
      def self.read_stream(connection, stream, offset: StreamOffset.next,
                           prefetch: DEFAULT_STREAM_PREFETCH, name: nil, on_failure: :stop,
                           **options, &handler)
        raise ArgumentError, "read_stream needs a block to handle messages" unless handler
        unless ON_FAILURE.include?(on_failure)
          raise ArgumentError,
                "read_stream on_failure: must be one of #{ON_FAILURE.inspect}, " \
                "not #{on_failure.inspect}"
        end

        connection.consume(
          stream, prefetch: prefetch, tag: name,
                  retry_policy: options.delete(:retry_policy) || RetryPolicy.none,
                  arguments: { "x-stream-offset" => offset.to_argument },
                  stop_on_failure: on_failure == :stop,
                  **options, &refusing_put_back(stream, handler)
        )
      end

      # What {read_stream} does with a message its handler fails on.
      ON_FAILURE = %i[stop dead_letter].freeze

      # A stream cannot put a message back: republishing appends a second copy
      # to the log for every reader. So {Ack.in_progress} -- what
      # {Patterns.idempotent} answers for a claim held elsewhere -- is refused
      # here and dead-lettered with the reason, rather than duplicated.
      #
      # @api private
      def self.refusing_put_back(stream, handler)
        lambda do |message|
          ack = handler.call(message)
          next ack unless ack.is_a?(Ack) && ack.in_progress?

          Ack.reject(FatalError.new(
                       "a handler reading the stream #{stream} answered in_progress for " \
                       "#{message.id}, and a stream cannot put a message back"
                     ))
        end
      end
      private_class_method :refusing_put_back

      # A duration as RabbitMQ wants it: a number with a unit suffix rather than
      # a bare number of anything.
      #
      # @api private
      def self.duration_argument(seconds)
        seconds = seconds.to_i
        return "#{seconds / 86_400}D" if seconds.positive? && (seconds % 86_400).zero?
        return "#{seconds / 3600}h" if seconds.positive? && (seconds % 3600).zero?
        return "#{seconds / 60}m" if seconds.positive? && (seconds % 60).zero?

        "#{seconds}s"
      end
    end
  end
end
