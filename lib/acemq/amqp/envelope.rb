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

require "securerandom"
require_relative "headers"

module AceMQ
  module AMQP
    # What travels with a message, besides the message.
    #
    # Frozen once built, because an envelope describes a message that has
    # already been published or received: changing one in place would change
    # what a log line said after it was written. {#with} returns a modified
    # copy for the cases where a new message derives from an old one.
    #
    # Defaults are applied when the envelope is built rather than when it is
    # read, so two libraries reading the same message agree without having to
    # agree on a second set of rules: +correlation_id+ falls back to +id+, an
    # attempt starts at 1, a version starts at 1, and +type+ falls back to the
    # routing key, and to {DEFAULT_TYPE} when that is empty too.
    class Envelope
      # The type of a message published with neither a type nor a routing key,
      # as through a fanout exchange. The word the other four libraries write.
      DEFAULT_TYPE = "message"

      attr_reader :id, :type, :version, :correlation_id, :causation_id,
                  :attempt, :first_seen, :origin, :error, :claim, :headers, :route,
                  :extensions

      # @param id [String] the message identifier
      # @param type [String] the logical type
      # @param version [Integer] the schema version
      # @param correlation_id [String] defaults to the id
      # @param causation_id [String] the message that caused this one
      # @param attempt [Integer] the delivery attempt, from 1
      # @param first_seen [Time] when it was first published
      # @param origin [String] +service@host+
      # @param error [String] why it was dead-lettered
      # @param claim [String] where the payload is, when it is stored elsewhere
      # @param headers [Hash] the application's own headers
      # @param route [Hash] the +x-acemq-route+ headers, when the message is
      #   travelling a declared pipeline. Carried opaquely and passed through
      #   every hop: an envelope has to hold them because they are reserved
      #   names and so cannot go in +headers+, and it does not have to know what
      #   they mean. {Patterns::RoutingSlip} is what reads and writes them.
      # @param extensions [Hash] the +x-acemq-+ headers this library has no
      #   field for, as a newer version or another language's library wrote
      #   them. Carried unchanged through every retry, dead letter, park and
      #   replay, so a header added to the family later is not lost the first
      #   time a Ruby consumer republishes the message.
      # @raise [ArgumentError] if a reserved name is in +headers+
      def initialize(id: SecureRandom.uuid, type: "", version: 1,
                     correlation_id: nil, causation_id: "", attempt: 1,
                     first_seen: Time.now, origin: "", error: "", claim: "",
                     headers: {}, route: {}, extensions: {})
        self.class.refuse_reserved_names(headers)

        @id = id
        @type = type
        @version = [version, 1].max
        @correlation_id = correlation_id.nil? || correlation_id.empty? ? id : correlation_id
        @causation_id = causation_id
        @attempt = [attempt, 1].max
        @first_seen = first_seen
        @origin = origin
        @error = error
        @claim = claim
        @headers = headers.freeze
        @route = route.freeze
        @extensions = extensions.freeze
        freeze
      end

      # Reads an envelope off a delivery.
      #
      # Anything missing takes its default, and anything unreadable takes its
      # default too: a message from a producer that wrote +x-acemq-attempt+ as
      # a string is still a message, and refusing to deliver it would hand the
      # application an outage rather than a message.
      #
      # A message without +x-acemq-id+ — one a publisher outside this family
      # sent — takes its AMQP +message_id+ as its id when it has one, the
      # property every AceMQ library sets equal to +x-acemq-id+. Two copies of
      # such a message are then one message, to an idempotent consumer and to
      # whoever reads a dead letter; a fresh id per delivery would make them
      # two. Only with neither is an id made up.
      #
      # @param raw [Hash, nil] the delivery's headers
      # @param routing_key [String] what it arrived on
      # @param message_id [String, nil] the delivery's AMQP +message_id+
      # @return [Envelope]
      def self.from_headers(raw, routing_key = "", message_id: nil)
        raw = (raw || {}).transform_keys(&:to_s)
        application = raw.reject { |name, _| Headers.reserved?(name) }

        id = id_of(raw, message_id)
        new(
          id: id,
          type: presence(text(raw[Headers::TYPE]), presence(routing_key, DEFAULT_TYPE)),
          version: number(raw[Headers::VERSION], 1),
          correlation_id: presence(text(raw[Headers::CORRELATION]), id),
          causation_id: text(raw[Headers::CAUSATION]),
          attempt: number(raw[Headers::ATTEMPT], 1),
          first_seen: from_millis(number(raw[Headers::FIRST_SEEN], millis(Time.now))),
          origin: text(raw[Headers::ORIGIN]),
          error: text(raw[Headers::ERROR]),
          claim: text(raw[Headers::CLAIM]),
          headers: application,
          route: raw.slice(*Headers::ROUTE_HEADERS),
          extensions: raw.select { |name, _| Headers.extension?(name) }
        )
      end

      # The type this envelope goes out with under a routing key: its own,
      # else the key, else {DEFAULT_TYPE}. The one rule the header and the
      # +message.type+ tag on a publish are both read off.
      #
      # @param routing_key [String]
      # @return [String]
      def type_on(routing_key = "")
        [type, routing_key.to_s].find { |t| !t.empty? } || DEFAULT_TYPE
      end

      # The AMQP headers for this envelope.
      #
      # @param routing_key [String] used when no type was given
      # @return [Hash] the reserved headers, plus the application's own
      def to_headers(routing_key = "")
        written = {
          Headers::ID => id,
          Headers::TYPE => type_on(routing_key),
          Headers::VERSION => version,
          Headers::CORRELATION => correlation_id.empty? ? id : correlation_id,
          Headers::ATTEMPT => attempt,
          Headers::FIRST_SEEN => self.class.millis(first_seen)
        }

        # Absent rather than empty. A header carrying "" is a header somebody
        # has to write a special case for at the other end.
        { Headers::CAUSATION => causation_id, Headers::ORIGIN => origin,
          Headers::ERROR => error, Headers::CLAIM => claim }.each do |name, value|
          written[name] = value unless value.nil? || value.empty?
        end

        written.merge(extensions).merge(route).merge(headers)
      end

      # A copy with fields changed.
      #
      # @param changes [Hash] the fields to replace
      # @return [Envelope]
      def with(**changes)
        Envelope.new(
          id: changes.fetch(:id, id),
          type: changes.fetch(:type, type),
          version: changes.fetch(:version, version),
          correlation_id: changes.fetch(:correlation_id, correlation_id),
          causation_id: changes.fetch(:causation_id, causation_id),
          attempt: changes.fetch(:attempt, attempt),
          first_seen: changes.fetch(:first_seen, first_seen),
          origin: changes.fetch(:origin, origin),
          error: changes.fetch(:error, error),
          claim: changes.fetch(:claim, claim),
          headers: changes.fetch(:headers, headers),
          route: changes.fetch(:route, route),
          extensions: changes.fetch(:extensions, extensions)
        )
      end

      # How long since the message was first published, in seconds.
      #
      # The basis for giving up on age rather than on attempts, which is the
      # honest limit when a queue has been paused: a message can be on attempt
      # two and four days old.
      #
      # @return [Float]
      def age
        Time.now - first_seen
      end

      def ==(other)
        other.is_a?(Envelope) && to_headers == other.to_headers
      end

      # Reserved names in an application's own hash are refused rather than
      # dropped: silently discarding a header somebody set is worse than saying
      # no, and these would otherwise be written twice and read back
      # inconsistently.
      #
      # @api private
      def self.refuse_reserved_names(headers)
        offending = headers.keys.map(&:to_s).select { |name| Headers.reserved?(name) }.sort
        return if offending.empty?

        raise ArgumentError,
              "these header names belong to AceMQ and cannot be set by hand: " \
              "#{offending.join(", ")}"
      end

      # @api private
      # Exact, not through a Float: 1767323045678 read back as
      # +Time.at(1767323045.678)+ writes 1767323045677, and a first-seen time
      # that moves a millisecond on every hop is not the time it was first seen.
      def self.millis(time)
        (time.to_r * 1000).floor
      end

      # @api private
      def self.from_millis(value)
        Time.at(Rational(value, 1000))
      end

      # A header as a string. Some clients put strings on the wire as bytes,
      # and +to_s+ on those gives something no other language will recognise.
      #
      # @api private
      def self.text(value)
        return "" if value.nil?

        value.to_s
      end

      # A header as an integer, or the default when it is not one.
      #
      # @api private
      def self.number(value, fallback)
        return fallback if value.nil? || value == true || value == false
        return value if value.is_a?(Integer)

        Integer(value.to_s, 10)
      rescue ArgumentError, TypeError
        fallback
      end

      # +x-acemq-id+, else the AMQP +message_id+, else a fresh one.
      #
      # @api private
      def self.id_of(raw, message_id)
        [text(raw[Headers::ID]), text(message_id)].find { |id| !id.empty? } || SecureRandom.uuid
      end

      # @api private
      def self.presence(value, fallback)
        value.nil? || value.empty? ? fallback : value
      end

      private_class_method :presence, :id_of
    end
  end
end
