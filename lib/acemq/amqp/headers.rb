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
    # The names AceMQ puts on the wire.
    #
    # These are the contract between languages, not an implementation detail of
    # this one. A Ruby consumer reads what a Java producer wrote because both
    # agree on the strings here and on the types of their values, so nothing is
    # renamed for Ruby's benefit: +x-acemq-first-seen+ stays hyphenated and
    # stays epoch milliseconds, however un-Rubyish that looks.
    #
    # Pinned by spec/fixtures/envelope-fixtures.json, which the Java
    # implementation produces and Go and .NET are held to as well.
    module Headers
      # The unique message identifier, and the default idempotency key.
      ID = "x-acemq-id"

      # The logical message type, for example +order.placed.v2+.
      TYPE = "x-acemq-type"

      # The schema version of the payload, an integer starting at 1.
      VERSION = "x-acemq-version"

      # The business correlation identifier, propagated across hops.
      CORRELATION = "x-acemq-correlation"

      # The message that caused this one, absent when there was none.
      CAUSATION = "x-acemq-causation"

      # The delivery attempt, an integer starting at 1.
      ATTEMPT = "x-acemq-attempt"

      # When the message was first published, as epoch milliseconds.
      FIRST_SEEN = "x-acemq-first-seen"

      # The publishing process, conventionally +service@host+.
      ORIGIN = "x-acemq-origin"

      # Why a message was dead-lettered, present only when it was.
      ERROR = "x-acemq-error"

      # The claim-check URI, when the payload lives outside the message.
      CLAIM = "x-acemq-claim"

      # The ordered step names of a declared pipeline, comma-separated; which
      # of them is next, counting from zero; and the identifier of one run
      # through the pipeline, stable across every hop.
      #
      # The itinerary as Java writes it, resolved against a
      # {Patterns::Pipeline} rather than carrying its own destinations. Ruby's
      # own +acemq-routing-slip+ is the self-describing form and stays the
      # default; these three are what makes a Ruby step able to stand in a
      # Java-declared pipeline. See {Patterns::RoutingSlip}.
      ROUTE = "x-acemq-route"
      ROUTE_POSITION = "x-acemq-route-position"
      ROUTE_ID = "x-acemq-route-id"

      # The prefix every reserved name shares.
      PREFIX = "x-acemq-"

      # The three the itinerary is written in, in the order they are read.
      ROUTE_HEADERS = [ROUTE, ROUTE_POSITION, ROUTE_ID].freeze

      # Every name this library writes and understands.
      RESERVED = [ID, TYPE, VERSION, CORRELATION, CAUSATION, ATTEMPT,
                  FIRST_SEEN, ORIGIN, ERROR, CLAIM, *ROUTE_HEADERS].freeze

      # Whether a header belongs to AceMQ rather than to the application.
      #
      # Matched on the prefix rather than on the known set: a header from a
      # newer version of another language's library is still not the
      # application's, and handing it back as though it were would have an
      # application unknowingly copy it onto a message it publishes.
      #
      # @param name [String] a header name
      # @return [Boolean] whether AceMQ reserves it
      def self.reserved?(name)
        name.to_s.start_with?(PREFIX)
      end
    end
  end
end
