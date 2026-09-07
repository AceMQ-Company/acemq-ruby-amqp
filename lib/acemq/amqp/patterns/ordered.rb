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

module AceMQ
  module AMQP
    # Keeping some messages in order without putting all of them in order.
    #
    # A queue delivers in order and a consumer with concurrency above one stops
    # honouring that. Usually the right trade; the wrong one where a later
    # message about the same thing must not overtake an earlier one — an "order
    # cancelled" arriving before the "order placed" it cancels.
    module Patterns
      # Wraps a handler so messages sharing a key are never handled at once.
      #
      #   mq.consume("orders.new", concurrency: 16,
      #              &Patterns.ordered("x-order-id") { |message| apply(message.payload) })
      #
      # Ordering per key, concurrency across keys.
      #
      # == What this does not do
      #
      # It orders the handling of messages that have already been delivered. It
      # cannot reorder ones the broker delivered out of order, and with several
      # consumers on one queue it orders only within each process. Ordering
      # across processes needs the messages to reach the same one to begin with,
      # which is a routing decision rather than a handler one — a consistent
      # hash exchange, or a queue per partition, which is what {Patterns.partition}
      # is for.
      #
      # A message whose key comes out empty is handled with no ordering at all,
      # because there is nothing to order it against.
      #
      # @param key [#call, String, Symbol] how to find a message's ordering key.
      #   Anything callable is given the message; a name is read as an
      #   application header, which is the common case and not worth a lambda.
      # @return [Proc] a handler to pass to {Connection#consume}
      def self.ordered(key, &handler)
        raise ArgumentError, "Patterns.ordered needs a block to wrap" unless handler

        pick = key.respond_to?(:call) ? key : by_header(key)
        locks = KeyedLocks.new

        lambda do |message|
          ordering = pick.call(message).to_s
          next handler.call(message) if ordering.empty?

          locks.holding(ordering) { handler.call(message) }
        end
      end

      # Orders by an application header — a tenant, a customer, an aggregate
      # identifier.
      def self.by_header(name)
        name = name.to_s
        ->(message) { message.envelope.headers[name] }
      end

      # Orders by correlation identifier, which keeps one business action's
      # messages in sequence.
      def self.by_correlation
        ->(message) { message.envelope.correlation_id }
      end

      # Maps a key onto one of +count+ slots.
      #
      # For deciding which queue or which worker a message belongs to, when
      # ordering has to hold across processes rather than within one. FNV-1a
      # rather than Ruby's own +hash+, and that is the whole point: Ruby
      # randomises string hashes per process, so two workers would disagree
      # about where a key belongs, and so would a Go publisher and a Ruby
      # consumer. This gives the same answer everywhere, for ever.
      #
      # @param key [String]
      # @param count [Integer] how many partitions
      # @return [Integer] 0 to count - 1
      def self.partition(key, count)
        return 0 if count.to_i <= 1

        hash = 2_166_136_261
        key.to_s.each_byte do |byte|
          hash = ((hash ^ byte) * 16_777_619) & 0xFFFF_FFFF
        end
        hash % count
      end

      # A routing key with its partition on the end, for publishing into a
      # queue-per-partition arrangement.
      #
      #   Patterns.partitioned_routing_key("orders", order_id, 8)   # => "orders.3"
      def self.partitioned_routing_key(base, key, partitions)
        "#{base}.#{partition(key, partitions)}"
      end

      # One lock per key, and no lock for a key nothing is using.
      #
      # Counted rather than left in place, because the natural key here is a
      # per-order or per-customer identifier: keeping a mutex for every key ever
      # seen means keeping one for every order this process ever handled.
      #
      # @api private
      class KeyedLocks
        def initialize
          @lock = Mutex.new
          @held = {}
          @users = Hash.new(0)
        end

        def holding(key, &)
          mutex = claim(key)
          begin
            mutex.synchronize(&)
          ensure
            release(key)
          end
        end

        # How many keys are currently locked, for a test that wants to know the
        # map is not growing.
        def size = @lock.synchronize { @held.size }

        private

        def claim(key)
          @lock.synchronize do
            @users[key] += 1
            @held[key] ||= Mutex.new
          end
        end

        def release(key)
          @lock.synchronize do
            @users[key] -= 1
            next if @users[key].positive?

            @users.delete(key)
            @held.delete(key)
          end
        end
      end
    end
  end
end
