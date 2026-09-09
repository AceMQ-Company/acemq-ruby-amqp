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
    # What a handler says about a message: it worked, try it again, never again,
    # or put it somewhere a person will look.
    #
    # Returned rather than performed, so a handler that forgets to decide is
    # caught immediately rather than leaving a message unacknowledged until the
    # connection drops — after which it comes back, usually to the same
    # handler, with the same outcome.
    class Ack
      ACCEPT = :accept
      RETRY = :retry
      REJECT = :reject
      PARK = :park

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

      # Sends the message to +{queue}.parked+ without trying again.
      #
      # Parked and not dead-lettered, because they are different problems and
      # the two queues exist to keep them apart: the dead-letter queue holds
      # messages that were tried and failed, and the parking queue holds
      # messages nothing could make sense of. A handler that already knows a
      # message is unreadable — a field that is not a date where a date has to
      # be, a version this service was never taught — used to have to reject it
      # into the dead letters and lose that distinction. Somebody draining the
      # dead-letter queue after an outage should not have to sort out the
      # messages that were never going to work from the ones that failed while
      # the database was down.
      #
      # The engine parks a message it could not decode by itself; this is the
      # same destination, asked for by a handler that got further.
      #
      # @param reason [String, Exception, nil] why it cannot be read
      def self.park(reason = nil)
        new(PARK, reason)
      end

      def accept? = action == ACCEPT
      def retry? = action == RETRY
      def reject? = action == REJECT
      def park? = action == PARK

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
