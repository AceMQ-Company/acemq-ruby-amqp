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
    # routing key.
    class Envelope
      attr_reader :id, :type, :version, :correlation_id, :causation_id,
                  :attempt, :first_seen, :origin, :error, :claim, :headers

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
      # @raise [ArgumentError] if a reserved name is in +headers+
      def initialize(id: SecureRandom.uuid, type: "", version: 1,
                     correlation_id: nil, causation_id: "", attempt: 1,
                     first_seen: Time.now, origin: "", error: "", claim: "",
                     headers: {})
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
        freeze
      end

      # Reads an envelope off a delivery.
      #
      # Anything missing takes its default, and anything unreadable takes its
      # default too: a message from a producer that wrote +x-acemq-attempt+ as
      # a string is still a message, and refusing to deliver it would hand the
      # application an outage rather than a message.
      #
      # @param raw [Hash, nil] the delivery's headers
      # @param routing_key [String] what it arrived on
      # @return [Envelope]
      def self.from_headers(raw, routing_key = "")
        raw = (raw || {}).transform_keys(&:to_s)
        application = raw.reject { |name, _| Headers.reserved?(name) }

        id = text(raw[Headers::ID])
        id = SecureRandom.uuid if id.empty?

        new(
          id: id,
          type: presence(text(raw[Headers::TYPE]), routing_key),
          version: number(raw[Headers::VERSION], 1),
          correlation_id: presence(text(raw[Headers::CORRELATION]), id),
          causation_id: text(raw[Headers::CAUSATION]),
          attempt: number(raw[Headers::ATTEMPT], 1),
          first_seen: from_millis(number(raw[Headers::FIRST_SEEN], millis(Time.now))),
          origin: text(raw[Headers::ORIGIN]),
          error: text(raw[Headers::ERROR]),
          claim: text(raw[Headers::CLAIM]),
          headers: application
        )
      end

      # The AMQP headers for this envelope.
      #
      # @param routing_key [String] used when no type was given
      # @return [Hash] the reserved headers, plus the application's own
      def to_headers(routing_key = "")
        written = {
          Headers::ID => id,
          Headers::TYPE => type.empty? ? routing_key : type,
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

        written.merge(headers)
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
          headers: changes.fetch(:headers, headers)
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
      def self.millis(time)
        (time.to_f * 1000).to_i
      end

      # @api private
      def self.from_millis(value)
        Time.at(value / 1000.0)
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

      # @api private
      def self.presence(value, fallback)
        value.nil? || value.empty? ? fallback : value
      end

      private_class_method :presence
    end
  end
end
