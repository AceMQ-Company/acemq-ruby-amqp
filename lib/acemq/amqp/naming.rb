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
    # Where a message goes when it cannot be handled.
    #
    # Convention rather than protocol, which is exactly why it has to be
    # identical everywhere: an operator looking for the dead letters of
    # +orders.new+ should find them in +orders.new.dlq+ whether the consumer
    # that gave up was written in Java, Go, .NET, Python or Ruby.
    module Naming
      # Where a message goes when every attempt has been used.
      DEAD_LETTER_SUFFIX = ".dlq"

      # Where a message goes when a person has to look at it.
      PARKED_SUFFIX = ".parked"

      # +orders.new+ becomes +orders.new.dlq+.
      def self.dead_letter_queue(queue)
        queue + DEAD_LETTER_SUFFIX
      end

      # +orders.new+ becomes +orders.new.parked+.
      def self.parked_queue(queue)
        queue + PARKED_SUFFIX
      end

      # +orders.new+ and 30 seconds become +orders.new.retry.30s+.
      #
      # The delay is in the name because a delay queue is per-delay: its
      # +x-message-ttl+ is fixed at declaration, so a policy with four
      # different waits needs four queues, and an operator should be able to
      # tell which is which without reading their arguments.
      #
      # @param queue [String]
      # @param delay [Numeric] seconds
      def self.retry_queue(queue, delay)
        "#{queue}.retry.#{short(delay)}"
      end

      # A duration as the shortest thing that reads as one: 30s, 5m, 2h.
      def self.short(delay)
        seconds = delay.to_i
        return "0s" if seconds <= 0
        return "#{seconds / 3600}h" if (seconds % 3600).zero?
        return "#{seconds / 60}m" if (seconds % 60).zero?

        "#{seconds}s"
      end
    end
  end
end
