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
    # What a handler says about a message: it worked, try it again, or never
    # again.
    #
    # Returned rather than performed, so a handler that forgets to decide is
    # caught immediately rather than leaving a message unacknowledged until the
    # connection drops — after which it comes back, usually to the same
    # handler, with the same outcome.
    class Ack
      ACCEPT = :accept
      RETRY = :retry
      REJECT = :reject

      attr_reader :action, :error

      def initialize(action, error = nil)
        @action = action
        @error = error
        freeze
      end

      # Confirms the message. It will not be delivered again.
      def self.accept
        new(ACCEPT)
      end

      # Returns the message to be tried again.
      #
      # The retry policy decides whether there is another attempt left; when
      # there is not, the message is dead-lettered with +error+ as the reason.
      # A {FatalError} skips the remaining attempts, because they would all
      # fail the same way.
      def self.retry(error = nil)
        new(RETRY, error)
      end

      # Dead-letters the message without trying again.
      #
      # For when the message itself is the problem — a field that cannot be
      # missing is missing — rather than when the world is temporarily
      # unhelpful.
      def self.reject(error = nil)
        new(REJECT, error)
      end

      def accept? = action == ACCEPT
      def retry? = action == RETRY
      def reject? = action == REJECT

      def to_s = action.to_s

      def ==(other)
        other.is_a?(Ack) && action == other.action
      end
    end

    # An error no number of retries will fix.
    #
    # Raising this says "stop now" without the handler having to know how many
    # attempts remain, which is knowledge handlers should not need.
    class FatalError < StandardError; end

    # A gem this library needs for one optional thing is not installed.
    #
    # The gem declares no runtime dependencies, so the transport and the
    # Protobuf, Avro and XML codecs reach for theirs lazily. Its own class so a
    # caller can rescue it and fall back, and its message names the gem rather
    # than leaving somebody to work out which library +cannot load such file --
    # bunny+ was talking about.
    class DependencyMissing < StandardError; end
  end
end
