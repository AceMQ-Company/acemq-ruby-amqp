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

require_relative "../envelope"

module AceMQ
  module AMQP
    # Putting messages back after the bug is fixed.
    #
    # The thing somebody actually does at three in the morning: a dead-letter
    # queue has two thousand messages in it, the fix is deployed, and they need
    # to go back through — but not all of them, and not silently.
    module Patterns
      # What a replay did.
      #
      # +reason+ is why it stopped, and it is worth reporting: "moved 500" means
      # something quite different when the limit was 500.
      ReplayResult = Struct.new(:moved, :skipped, :reason, keyword_init: true) do
        def to_s = "moved #{moved}, skipped #{skipped} (#{reason})"
      end

      # A replay that stopped part-way. +result+ is what it had done first,
      # because a tool that says only "it failed" leaves somebody to work out by
      # hand how much of a queue has already moved.
      class ReplayFailed < StandardError
        attr_reader :result

        def initialize(message, result)
          super(message)
          @result = result
        end
      end

      # The queue a message was replayed out of.
      REPLAYED_FROM_HEADER = "acemq-replayed-from"

      # When it was replayed, as RFC 3339.
      REPLAYED_AT_HEADER = "acemq-replayed-at"

      # How many times it has been replayed.
      REPLAY_COUNT_HEADER = "acemq-replay-count"

      # Moves messages from a queue back onto an exchange.
      #
      #   result = Patterns.replay(mq, from: "orders.new.dlq",
      #                            exchange: "orders-events", limit: 500) do |envelope, _body|
      #     envelope.error.include?("timeout")
      #   end
      #
      #   result.to_s   # => "moved 37, skipped 463 (drained)"
      #
      # The block decides which messages go. Returning false leaves one where it
      # is, which is what makes a replay something that can be done in stages
      # rather than all at once. Without a block every message goes.
      #
      # Each message is stamped so a replayed one can be told from an original:
      # {REPLAYED_FROM_HEADER}, {REPLAYED_AT_HEADER} and {REPLAY_COUNT_HEADER}.
      # A consumer that needs to treat them differently can, and one that does
      # not is unaffected.
      #
      # @param connection [Connection]
      # @param from [String] the queue the messages are on now
      # @param exchange [String] where they go back to; empty publishes to a
      #   queue by name
      # @param routing_key [String, nil] overrides the message's own. Nil keeps
      #   it, so a message goes back where it came from rather than everywhere.
      # @param limit [Integer] stop after this many; zero for no limit, which
      #   against a queue somebody is still writing to may mean never stopping
      # @param deadline [Numeric] stop after this many seconds; zero for none
      # @return [ReplayResult]
      # @raise [ReplayFailed] when the broker refused part-way
      def self.replay(connection, from:, exchange: "", routing_key: nil, limit: 0,
                      deadline: 0, &filter)
        raise ArgumentError, "a replay needs a queue to read from" if from.to_s.empty?

        refuse_a_loop(from, exchange, routing_key)
        Replay.new(connection, from: from, exchange: exchange, routing_key: routing_key,
                               limit: limit, deadline: deadline, filter: filter).run
      end

      # Refuses a replay that would put every message back where it found it.
      #
      # The default exchange routes to the queue named by the routing key, and a
      # dead letter's routing key is the dead-letter queue — the consumer put it
      # there by name. So a replay through the default exchange with no routing
      # key of its own reads a message and writes it straight back, for ever,
      # and the only sign of it is a queue that never empties.
      #
      # Refused rather than defended against with a limit, because a limit would
      # turn an infinite loop into a finite one that still did nothing.
      #
      # @api private
      def self.refuse_a_loop(from, exchange, routing_key)
        return unless exchange.to_s.empty?
        return unless routing_key.nil? || routing_key.to_s == from.to_s

        raise ArgumentError,
              "replaying #{from} through the default exchange needs a routing key that is " \
              "not #{from}, or it would publish every message straight back onto the queue " \
              "it was read from; name an exchange or a routing key"
      end

      private_class_method :refuse_a_loop

      # One pass over a queue.
      #
      # @api private
      class Replay
        def initialize(connection, from:, exchange:, routing_key:, limit:, deadline:, filter:)
          # The raw publish, because a replayed message keeps the bytes and the
          # envelope it already had; re-encoding it would be inventing a new
          # message with an old one's identity.
          @transport = connection.respond_to?(:transport) ? connection.transport : connection
          @from = from
          @exchange = exchange
          @routing_key = routing_key
          @limit = limit.to_i
          @deadline = deadline.to_f
          @filter = filter
          @result = ReplayResult.new(moved: 0, skipped: 0, reason: :drained)
          # Messages the filter declines are held unacknowledged, not returned
          # one at a time. Returning one immediately does not work: the broker
          # puts it back where it was, at the head of the queue, so the next
          # read hands over the same message and everything behind it is never
          # looked at. Holding them takes them out of the way for the length of
          # the pass, and the broker still has them, so a crash half way through
          # returns them rather than losing them.
          @declined = []
        end

        def run
          ends_at = @deadline.positive? ? monotonic + @deadline : nil
          loop do
            break @result.reason = :deadline if ends_at && monotonic >= ends_at
            break @result.reason = :limit if @limit.positive? && @result.moved >= @limit

            delivery = @transport.pull(@from)
            break @result.reason = :drained if delivery.nil?

            consider(delivery)
          end
          @result
        ensure
          release
        end

        private

        def consider(delivery)
          envelope = Envelope.from_headers(delivery.headers, delivery.routing_key)
          unless @filter.nil? || @filter.call(envelope, delivery.body)
            @result.skipped += 1
            return @declined << delivery
          end

          move(delivery, envelope)
        end

        def move(delivery, envelope)
          @transport.publish(
            exchange: @exchange, routing_key: @routing_key || delivery.routing_key,
            body: delivery.body, content_type: delivery.content_type,
            message_id: envelope.id, headers: stamped(envelope, delivery.routing_key),
            persistent: true
          )
        rescue StandardError => e
          # Returned rather than dropped, and the replay stops. A replay that
          # loses messages is worse than one that stops early.
          delivery.nack(requeue: true)
          raise ReplayFailed.new("cannot republish a message from #{@from}: #{e.message}",
                                 @result)
        else
          # Acknowledged only once the broker has confirmed the new copy, so a
          # failure between the two leaves the message where it was. The cost is
          # that a crash in the gap replays it twice, which is the right way
          # round for a dead-letter queue: a duplicate can be recognised, and a
          # message deleted from the only place it existed cannot.
          delivery.ack
          @result.moved += 1
        end

        def stamped(envelope, routing_key)
          envelope.to_headers(routing_key).merge(
            REPLAYED_FROM_HEADER => @from,
            REPLAYED_AT_HEADER => Time.now.utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            REPLAY_COUNT_HEADER => replay_count(envelope) + 1
          )
        end

        # A count written by another language's library arrives as whatever that
        # language's field table produced, so it is read for what it is worth
        # rather than trusted to be an Integer.
        def replay_count(envelope)
          Envelope.number(envelope.headers[REPLAY_COUNT_HEADER], 0)
        end

        # Everything the pass declined goes back to the queue it came from.
        # Nothing to report if it cannot: the pass is over, and a message that
        # will not go back is one the broker returns itself when the connection
        # closes.
        def release
          @declined.each do |delivery|
            delivery.nack(requeue: true)
          rescue StandardError
            nil
          end
          @declined = []
        end

        def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
