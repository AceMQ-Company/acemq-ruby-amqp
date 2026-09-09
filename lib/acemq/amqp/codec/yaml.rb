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

require "date"
require "yaml"
require_relative "../ack"

module AceMQ
  module AMQP
    # Reads and writes YAML.
    #
    # Chosen when a message is meant to be read by a person as much as by a
    # program: a configuration change broadcast to a fleet, a deployment
    # instruction, a command replayed by hand from a dead-letter queue. It costs
    # more to parse than JSON and is a poor choice for high volume; it earns its
    # place where somebody will actually look at the message.
    #
    # Psych is a default gem, so this needs nothing installed.
    #
    # == Why this loads safely, and what that costs
    #
    # A message body is input from another machine. +YAML.load+ on it is remote
    # code execution: +!ruby/object:Gem::Requirement+ and its relatives are how
    # a body turns into an instantiated object graph, and a queue is exactly
    # where a body from somewhere unexpected arrives. So this uses
    # +Psych.safe_load+, which builds only the types it has been told about.
    #
    # That is not free, and the cost lands on legitimate messages:
    #
    # * A date or a timestamp is a YAML scalar type, not a Ruby one, and
    #   +safe_load+ refuses it unless the class is permitted. Java and Go both
    #   write timestamps into YAML, so this permits +Date+, +Time+ and
    #   +DateTime+ by default — refusing them would reject messages the other
    #   libraries write every day.
    # * A symbol does not arrive from any other language, so +Symbol+ is not
    #   permitted by default. Pass it in +permitted_classes+ for a Ruby-to-Ruby
    #   queue that wants them, knowing that turning arbitrary remote text into
    #   symbols is a decision worth making on purpose.
    # * Anchors and aliases are refused, because the billion-laughs expansion
    #   needs nothing but aliases to take a consumer down, and neither Jackson
    #   nor gopkg.in/yaml.v3 writes them. Pass +aliases: true+ for a producer
    #   that does.
    #
    # == What it will not write
    #
    # Psych will happily serialise an arbitrary Ruby object as +!ruby/object:+,
    # which is a message no other AceMQ library can read and the exact tag this
    # codec refuses on the way in. Encoding one raises {EncodeError} rather than
    # publishing something only Ruby can load — and only Ruby with the safety
    # off. Publish a Hash, an Array or a scalar.
    #
    # A symbol key is written as +:name+, because that is how YAML spells a Ruby
    # symbol and this library does not quietly rename keys. No other language
    # reads +:name+ as +name+; spell keys as strings.
    class YAMLCodec
      # What this codec writes: the type RFC 9512 registered, and what Java, Go
      # and .NET write.
      CONTENT_TYPE = "application/yaml"

      # The three spellings that predate RFC 9512, which is what most senders
      # still write. Read wider than written, on purpose: a producer in another
      # stack uses whichever spelling its library picked.
      ALSO_READS = ["application/x-yaml", "text/yaml", "text/x-yaml"].freeze

      # What +safe_load+ raises for an alias it was told not to follow, which is
      # not the same class on every Ruby this gem supports: Psych 5 introduced
      # +AliasesNotEnabled+, and Psych 4 -- what Ruby 3.1 ships, and 3.1 is the
      # floor -- raises +BadAlias+.
      #
      # Resolved once here rather than named in the +rescue+, because a +rescue+
      # naming a constant that does not exist raises NameError while it is being
      # matched. That does not merely miss the alias case: it escapes every
      # later clause too, so a body that was not YAML at all came out as a
      # NameError instead of a DecodeError.
      ALIAS_REFUSED = if ::Psych.const_defined?(:AliasesNotEnabled)
                        ::Psych::AliasesNotEnabled
                      else
                        ::Psych::BadAlias
                      end

      # What +safe_load+ is allowed to build without being asked.
      #
      # Java and Go write timestamps into YAML, and a codec that rejected them
      # would refuse messages the other libraries send routinely. Everything
      # else — every Ruby class, +Symbol+ included — has to be named.
      TIME_CLASSES = [Date, DateTime, Time].freeze

      # @param permitted_classes [Array<Class>] classes +safe_load+ may build in
      #   addition to {TIME_CLASSES}. +Symbol+ is the usual one.
      # @param aliases [Boolean] whether to expand anchors and aliases. False,
      #   because a body from a queue is untrusted and aliases are how a small
      #   one becomes a large one.
      def initialize(permitted_classes: [], aliases: false)
        @permitted_classes = (TIME_CLASSES + Array(permitted_classes)).uniq.freeze
        @aliases = aliases
        freeze
      end

      def content_type = CONTENT_TYPE

      # @param payload [Object] a Hash, an Array or a scalar
      # @return [String]
      # @raise [EncodeError] when Psych can only write it as a Ruby-specific tag
      def encode(payload)
        written = ::Psych.dump(payload)
        if written.include?("!ruby/")
          raise EncodeError,
                "this payload only serialises as YAML with a Ruby-specific tag " \
                "(#{written[%r{!ruby/[\w:]+}]}), which no other AceMQ library can read. " \
                "Publish a Hash, an Array or a scalar."
        end

        # No leading "---". It is valid and it is noise, a message body is a
        # single document by definition, and neither Java nor Go writes one.
        written.sub(/\A---(?: |\n)/, "")
      end

      # @param body [String]
      # @return [Object]
      # @raise [DecodeError] when the body is not YAML, or is YAML asking for a
      #   class this codec will not build
      def decode(body)
        ::Psych.safe_load(body.to_s, permitted_classes: @permitted_classes, aliases: @aliases)
      rescue ::Psych::DisallowedClass => e
        raise DecodeError,
              "this message asks for a class the YAML codec will not build: #{e.message}. " \
              "Pass permitted_classes: if the sender is trusted and really needs it."
      rescue ALIAS_REFUSED
        raise DecodeError,
              "this message uses YAML anchors and aliases, which are refused because " \
              "they are how a small body expands into one large enough to take a " \
              "consumer down. Pass aliases: true if the sender is trusted."
      rescue ::Psych::Exception => e
        raise DecodeError, "this message is not YAML: #{e.message}"
      end

      # Accepts +application/yaml+ and the three spellings in {ALSO_READS}, plus
      # any +...+yaml+ media type.
      #
      # Never a message whose sender set no content type. YAML is a superset of
      # JSON, so its parser accepts JSON bytes quite happily and would answer
      # for messages meant for {JSONCodec} — giving the right value while
      # recording that a YAML message arrived, which is the sort of wrong that
      # is found much later.
      def can_decode?(content_type)
        lower = content_type.to_s.downcase
        return false if lower.empty?

        lower.start_with?(CONTENT_TYPE, *ALSO_READS) || lower.include?("+yaml")
      end
    end
  end
end
