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

require_relative "../ack"

module AceMQ
  module AMQP
    # Doing a message's work once, however many times the message arrives.
    #
    # At-least-once delivery is not a defect to be configured away; it is what
    # a broker can actually promise. A retry, a redelivery after a consumer
    # died, a relay that published a record it had already published — all of
    # them hand the same message over twice, and the only place that can be
    # made harmless is the handler.
    module Patterns
      # Remembers which messages have already been handled.
      #
      # A store is anything answering two methods:
      #
      #   first_time?(key)  # records the key, true when it had not been seen
      #   forget(key)       # removes it, so a message that failed can be redone
      #
      # and, optionally, a third:
      #
      #   confirm(key)      # the work is done; start remembering it properly
      #
      # +first_time?+ has to be atomic. Two consumers handed the same message at
      # the same moment must not both be told they are first, or the guard has
      # done nothing except cost a round trip.
      #
      # +confirm+ is what a store needs when its record outlives the process
      # that made it. {InMemoryIdempotencyStore} has no use for one — a crash
      # wipes it, so a key it holds is a key somebody is working on now. A store
      # in a database has to tell the two apart, because a key left behind by a
      # consumer that died has to expire and a key left behind by work that
      # finished must not. {SQLIdempotencyStore} answers it; a store that does
      # not is simply never asked.
      #
      # A duck type rather than a class to inherit from, because the store
      # somebody actually wants is their own database — ideally the very rows
      # the handler writes, in the same transaction — and asking them to subclass
      # something from a messaging library to get there is asking for the wrong
      # thing.
      module IdempotencyStore
      end

      # An idempotency store that remembers keys in this process.
      #
      # Right behind one worker, and wrong the moment there are two: each has its
      # own memory, so both are told they are first and the duplicate goes
      # through. It is also lost on restart, which turns every message in flight
      # into a duplicate.
      #
      # Use it in tests, and in a single-process service. Anything else wants a
      # store the workers share — the same database the work is written to,
      # in the same transaction, which is the only arrangement that actually
      # holds.
      class InMemoryIdempotencyStore
        # How long a key is remembered when no window is given.
        DEFAULT_WINDOW = 3600.0

        # @param window [Numeric] seconds to remember a key for. There has to be
        #   one: without it the map grows for as long as the process lives. Make
        #   it comfortably longer than the longest a message can take to stop
        #   being retried, because a key forgotten too early is a duplicate that
        #   gets through.
        def initialize(window: DEFAULT_WINDOW)
          @window = window.positive? ? window.to_f : DEFAULT_WINDOW
          @seen = {}
          @lock = Mutex.new
        end

        # Records a key, and says whether it is new.
        #
        # @param key [String]
        # @return [Boolean] true the first time, false for every repeat
        def first_time?(key)
          key = key.to_s
          @lock.synchronize do
            sweep(Process.clock_gettime(Process::CLOCK_MONOTONIC))
            next false if @seen.key?(key)

            @seen[key] = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            true
          end
        end

        # Removes a key, so the message it belongs to can be handled again.
        def forget(key)
          @lock.synchronize { @seen.delete(key.to_s) }
          nil
        end

        # How many keys are remembered, for a test that wants to know the window
        # is doing its job.
        def size = @lock.synchronize { @seen.size }

        private

        # Swept on the way past rather than on a timer, so the store owns no
        # thread and there is nothing to close.
        def sweep(now)
          @seen.delete_if { |_, at| now - at > @window }
        end
      end

      # Wraps a handler so a message that has already been handled is accepted
      # without running it again.
      #
      #   store = Patterns::InMemoryIdempotencyStore.new(window: 6 * 3600)
      #
      #   mq.consume("orders.new", &Patterns.idempotent(store) do |message|
      #     warehouse.reserve(message.payload)
      #     Ack.accept
      #   end)
      #
      # A duplicate is **accepted**, not rejected. The work was done, so the
      # message has been handled; dead-lettering it would raise an alarm about
      # something that went right.
      #
      # When the handler does not accept, the key is forgotten so that the retry
      # can actually run. That ordering is the honest one — remembering a message
      # that then failed would mean its retry silently does nothing — and it is
      # also why this is a guard against duplicates rather than exactly-once.
      # Between the handler finishing and the acknowledgement reaching the
      # broker, a crash still leaves a message that will be delivered again. Only
      # a store written in the same transaction as the work closes that gap, and
      # no library can do that on the application's behalf.
      #
      # @param store [IdempotencyStore] where keys are remembered
      # @param key [#call, nil] a message's key, when it is not the message id.
      #   Use it when the natural key is in the payload: an order identifier that
      #   two different messages both carry, where handling either one twice is
      #   the thing to prevent.
      # @return [Proc] a handler to pass to {Connection#consume}
      def self.idempotent(store, key: nil, &handler)
        raise ArgumentError, "Patterns.idempotent needs a block to wrap" unless handler

        lambda do |message|
          seen_as = (key ? key.call(message) : message.envelope.id).to_s
          if seen_as.empty?
            # Fatal rather than a retry: the key function will produce the same
            # nothing next time, and a guard that cannot key a message is not
            # guarding it.
            next Ack.reject(FatalError.new(
                              "message #{message.id} produced an empty idempotency key"
                            ))
          end

          run_once(store, seen_as, message, handler)
        end
      end

      # @api private
      def self.run_once(store, key, message, handler)
        begin
          first = store.first_time?(key)
        rescue StandardError => e
          # The store is what is broken, not the message. Retrying is right;
          # carrying on and risking a duplicate is what the store was for.
          return Ack.retry(e)
        end
        return Ack.accept unless first

        forgetting_on_failure(store, key) { handler.call(message) }
      end

      # @api private
      def self.forgetting_on_failure(store, key)
        ack = yield
        if ack.is_a?(Ack) && ack.accept?
          # Only a store that keeps its keys past the life of this process has
          # anything to do here, and asking rather than requiring it is what
          # lets a two-method store stay a two-method store.
          store.confirm(key) if store.respond_to?(:confirm)
        else
          store.forget(key)
        end
        ack
      rescue StandardError
        # A handler that raises has failed, and in Ruby that is the ordinary way
        # to fail. Forgetting before the exception goes on to the retry engine is
        # what lets the retry do anything.
        store.forget(key)
        raise
      end

      private_class_method :run_once, :forgetting_on_failure
    end
  end
end
