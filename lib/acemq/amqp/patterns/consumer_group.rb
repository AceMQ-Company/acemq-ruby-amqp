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
    # Several consumers over one queue, started and stopped as one thing.
    module Patterns
      # A group of consumers reading the same queue.
      #
      #   group = Patterns::ConsumerGroup.new(mq, "orders.new", size: 4) do |message|
      #     place(message.payload)
      #     Ack.accept
      #   end
      #
      #   at_exit { group.close }
      #
      # Two things it saves. Starting workers by hand means remembering to stop
      # every one, and a partial shutdown leaves messages held by a consumer
      # nobody is waiting for. And a group can be sized from configuration,
      # which is the number most often changed after a service is running.
      #
      # == Concurrency, or a group?
      #
      # +concurrency:+ on {Connection#consume} runs several handlers on one
      # consumer and one channel. A group runs several consumers, each with its
      # own channel and its own prefetch. Reach for the group when handlers are
      # slow enough that one channel's prefetch becomes the limit, or when a
      # fair share across processes matters: the broker round-robins between
      # consumers, so four here compete evenly with four in another instance
      # where one consumer with concurrency four would not.
      class ConsumerGroup
        attr_reader :queue, :consumers

        # @param connection [Connection]
        # @param queue [String]
        # @param size [Integer] how many consumers to run
        # @param options [Hash] passed to {Connection#consume}
        def initialize(connection, queue, size:, **options, &handler)
          unless handler
            raise ArgumentError, "a consumer group needs a block to handle messages"
          end
          unless size.to_i.positive?
            raise ArgumentError,
                  "a consumer group needs at least one consumer, not #{size}"
          end

          @queue = queue
          @lock = Mutex.new
          @consumers = start(connection, size.to_i, options, handler)
        end

        # How many consumers are running.
        def size = @consumers.size

        # Stops every consumer and waits for the handlers already running.
        #
        # Every one is stopped even when one of them refuses, because leaving
        # the rest running after a failed shutdown is worse than the failure.
        # The first refusal is raised once the others are down.
        def close(timeout: 30)
          running = @lock.synchronize do
            taken = @consumers
            @consumers = []
            taken
          end

          failure = nil
          running.each do |consumer|
            consumer.cancel(timeout: timeout)
          rescue StandardError => e
            failure ||= e
          end
          raise failure if failure

          nil
        end

        private

        # A half-started group holds messages nothing is going to handle, so
        # anything already running is stopped before the failure is passed on.
        def start(connection, size, options, handler)
          started = []
          size.times do |index|
            # Named, so the broker's management interface shows which consumer
            # is holding a message rather than four identical rows.
            tag = "acemq-#{@queue}-#{index + 1}"
            started << connection.consume(@queue, tag: tag, **options, &handler)
          end
          started
        rescue StandardError => e
          started.each { |consumer| consumer.cancel(timeout: 1) }
          raise TransportError,
                "cannot start consumer #{started.size + 1} of #{size} on " \
                "#{@queue.inspect}: #{e.message}"
        end
      end
    end
  end
end
