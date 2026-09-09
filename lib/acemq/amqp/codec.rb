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

require "json"
require_relative "ack"

# The formats beyond JSON, each in a file of its own because each is a page of
# reasoning rather than a method.
#
# Loading them costs nothing that is not already paid for: YAML is a default
# gem, and the TOML reader and writer are written here. The three that need a
# gem — REXML for XML, google-protobuf, avro — reach for it when a codec of
# that kind is built, not when this file is read, so a process publishing JSON
# installs none of them.
require_relative "codec/yaml"
require_relative "codec/toml"
require_relative "codec/xml"
require_relative "codec/protobuf"
require_relative "codec/avro"

module AceMQ
  module AMQP
    # A body that this codec cannot read.
    #
    # Fatal rather than retryable, because bytes do not improve with age: a
    # message that is not JSON is not JSON on the fourth attempt either, and
    # retrying it only holds a queue slot until it ages out.
    class DecodeError < FatalError; end

    # A payload that this codec cannot write.
    class EncodeError < FatalError; end

    # What turns a payload into bytes and back.
    #
    # Not a base class to inherit from — anything answering these four methods
    # is a codec, which is how a codec from another gem works here without
    # depending on this one. The methods are documented here because a duck
    # type still has to be written down somewhere.
    #
    # Ruby's +decode+ returns a value rather than filling in a destination the
    # caller passed. That is the one place this deliberately departs from the
    # Go shape, which has no choice: Ruby has no type to be told, so asking for
    # one would be ceremony with nothing behind it.
    #
    # @!method content_type
    #   @return [String] what this codec writes onto a message
    # @!method encode(payload)
    #   @param payload [Object]
    #   @return [String] the bytes to publish
    # @!method decode(body)
    #   @param body [String] the bytes as they arrived
    #   @return [Object] the payload
    # @!method can_decode?(content_type)
    #   @param content_type [String] what the sender said, or "" when it said
    #     nothing
    #   @return [Boolean] whether this codec should handle it
    module Codec
      # Whether an object can stand in as a codec.
      #
      # Checked when a connection is built rather than when the first message
      # arrives, so a codec missing a method is a mistake at start-up instead
      # of a mystery at three in the morning.
      #
      # @param candidate [Object]
      # @return [Boolean]
      def self.codec?(candidate)
        %i[content_type encode decode can_decode?].all? { |name| candidate.respond_to?(name) }
      end

      # @raise [ArgumentError] when +candidate+ is not a codec
      # @api private
      def self.check!(candidate)
        return candidate if codec?(candidate)

        raise ArgumentError,
              "#{candidate.inspect} is not a codec: it needs content_type, encode, " \
              "decode and can_decode?"
      end
    end

    # Reads and writes JSON.
    #
    # The default, and the one format every AceMQ library has without an extra
    # dependency, which is why an untyped message is assumed to be this one.
    #
    # Keys go on the wire exactly as the payload spells them, because the wire
    # name is the contract with the other four languages and a codec that
    # quietly renamed +total_cents+ to +totalCents+ would be deciding that
    # contract on the caller's behalf. Spell the keys the way the other
    # services already read them.
    class JSONCodec
      # What this codec writes, and what Java and .NET write.
      CONTENT_TYPE = "application/json"

      # @param symbolize_names [Boolean] whether decoded objects come back with
      #   symbol keys. False by default: a key that came off the wire is data,
      #   and turning arbitrary remote data into symbols is a decision worth
      #   making on purpose.
      def initialize(symbolize_names: false)
        @symbolize_names = symbolize_names
        freeze
      end

      def content_type = CONTENT_TYPE

      # @param payload [Object]
      # @return [String]
      def encode(payload)
        JSON.generate(payload)
      rescue JSON::GeneratorError, NoMethodError => e
        raise EncodeError, "this payload will not serialise as JSON: #{e.message}"
      end

      # @param body [String]
      # @return [Object]
      def decode(body)
        JSON.parse(body.to_s, symbolize_names: @symbolize_names)
      rescue JSON::ParserError => e
        raise DecodeError, "this message is not JSON: #{e.message}"
      end

      # Accepts +application/json+, +text/json+, any +...+json+ media type, and
      # a message whose sender set no content type at all.
      #
      # The last of those is the difference between this codec and the YAML and
      # TOML codecs in the other libraries: JSON is the default format, so an
      # untyped message is far more likely to be JSON than anything else, and
      # something has to be willing to read it.
      def can_decode?(content_type)
        lower = content_type.to_s.downcase
        return true if lower.empty?

        lower.start_with?(CONTENT_TYPE) || lower.start_with?("text/json") ||
          lower.include?("+json")
      end
    end

    # Passes bodies through untouched.
    #
    # For a payload that is already encoded — an image, something another
    # system's serialiser produced — and for reading a message this process has
    # no type for. Replaying a dead-lettered message wants it too: the bytes
    # that were committed are the bytes that should go back, and re-encoding
    # through a class that has since gained a field would produce something
    # else.
    #
    # It answers for every content type, which is exactly why it has to be
    # asked for by name rather than found by one.
    class BytesCodec
      CONTENT_TYPE = "application/octet-stream"

      def content_type = CONTENT_TYPE

      # @param payload [String, nil]
      # @return [String]
      def encode(payload)
        return "" if payload.nil?
        return payload if payload.is_a?(String)

        raise EncodeError, "BytesCodec takes a String, not a #{payload.class}"
      end

      # @param body [String]
      # @return [String] the bytes, unchanged
      def decode(body) = body.to_s

      # True for anything, including a message with no content type.
      def can_decode?(_content_type) = true
    end

    # Reads and writes text.
    #
    # For messages that really are text — a line of a log, a command somebody
    # typed — rather than a structure that happens to be readable. Anything
    # with fields wants {JSONCodec}.
    class StringCodec
      CONTENT_TYPE = "text/plain; charset=utf-8"

      def content_type = CONTENT_TYPE

      # @param payload [Object] anything that answers to_s
      # @return [String]
      def encode(payload) = payload.to_s

      # @param body [String]
      # @return [String]
      def decode(body) = body.to_s

      # Accepts +text/*+ and nothing else.
      #
      # Not a message with no content type, unlike {JSONCodec}: an untyped
      # message is much more likely to be JSON, and a codec that claimed
      # everything untyped would take those away from the codec that can
      # actually read them.
      def can_decode?(content_type)
        content_type.to_s.downcase.start_with?("text/")
      end
    end

    # Picks a codec by the content type the sender set.
    #
    # For a queue carrying more than one format — during a migration, or where
    # several producers were written years apart:
    #
    #   codec = CompositeCodec.new(JSONCodec.new, StringCodec.new)
    #
    # The first codec is what it writes. Reading offers the message to the
    # candidates in order and takes the first that manages it, so order matters
    # where two overlap: put the more specific first, since {BytesCodec}
    # answers for everything and would win from anywhere in the list.
    class CompositeCodec
      # @param codecs [Array<#content_type>] at least one, the first of which
      #   is used for writing
      def initialize(*codecs)
        raise ArgumentError, "a CompositeCodec needs at least one codec" if codecs.empty?

        @codecs = codecs.map { |codec| Codec.check!(codec) }.freeze
        freeze
      end

      # The first codec's, since that is what gets written.
      def content_type = @codecs.first.content_type

      # Writes with the first codec.
      def encode(payload) = @codecs.first.encode(payload)

      # Reads with the first candidate that manages it.
      #
      # A content type narrows the candidates to the codecs that claim it. No
      # content type leaves every codec a candidate, because a sender that said
      # nothing has ruled nothing out, and the alternative — guessing one
      # format and failing on the rest — turns a silent producer into a queue
      # of dead letters.
      #
      # @param body [String]
      # @param content_type [String] what the sender said, or "" when it said
      #   nothing
      # @return [Object]
      # @raise [DecodeError] when no candidate could read it
      def decode(body, content_type = "")
        failures = []
        candidates(content_type).each do |codec|
          return read(codec, body, content_type)
        rescue DecodeError => e
          failures << "#{codec.content_type}: #{e.message}"
        end

        raise DecodeError, describe_failure(content_type, failures)
      end

      # True when any codec in the set will take it.
      def can_decode?(content_type)
        candidates(content_type).any?
      end

      # The codecs this one holds, in the order they are tried.
      #
      # @return [Array]
      attr_reader :codecs

      private

      # A codec that reads by content type is told it; a plain one has no use
      # for it. Asked of the codec rather than assumed, so a codec from another
      # gem works either way round — and so the Avro codec inside a composite
      # gets the same signal it would get on its own, which is what tells it
      # which framing it is being handed.
      def read(codec, body, content_type)
        return codec.decode(body) if codec.method(:decode).arity == 1

        codec.decode(body, content_type)
      end

      # Every codec when the sender named no content type, and only those that
      # claim it when it did.
      def candidates(content_type)
        return @codecs if content_type.to_s.empty?

        @codecs.select { |codec| codec.can_decode?(content_type) }
      end

      def describe_failure(content_type, failures)
        held = @codecs.map(&:content_type).join(", ")
        stated = content_type.to_s.empty? ? "an untyped message" : content_type.inspect
        return "no codec here will read #{stated}; this one holds #{held}" if failures.empty?

        "no codec here could read #{stated}; tried #{failures.join("; ")}"
      end
    end

    # Codecs by name, so configuration can ask for a format without the calling
    # code naming the class.
    #
    # The names are shared with the other libraries — +json+, +bytes+, +string+,
    # +yaml+, +toml+ and +xml+ mean the same six things in Go and Java — so a
    # deployment that sets ACEMQ_CODEC does not have to be rewritten per
    # language.
    #
    # {ProtobufCodec} and {AvroCodec} are deliberately not here. Both are built
    # around a message type or a schema, a name in configuration cannot carry
    # one, and the Java library leaves them out of its own registry for exactly
    # the same reason.
    module Codecs
      @registry = {}
      @lock = Mutex.new

      class << self
        # Makes a codec available under a name.
        #
        # Registering a name twice replaces the first, which is what lets a
        # test override a default rather than having to work around it.
        #
        # @param name [String] what configuration will ask for
        # @yieldreturn [Object] a new codec
        def register(name, &build)
          unless build
            raise ArgumentError,
                  "Codecs.register(#{name.inspect}) needs a block that builds one"
          end

          @lock.synchronize { @registry[name.to_s] = build }
          name.to_s
        end

        # Builds the codec registered under a name.
        #
        # @param name [String]
        # @return [Object] a codec
        # @raise [ArgumentError] when nothing is registered under it
        def build(name)
          build = @lock.synchronize { @registry[name.to_s] }
          return build.call if build

          raise ArgumentError,
                "no codec named #{name.to_s.inspect} is registered; known: #{names.join(", ")}"
        end

        # The registered names, sorted.
        #
        # @return [Array<String>]
        def names
          @lock.synchronize { @registry.keys.sort }
        end
      end

      register("json") { JSONCodec.new }
      register("bytes") { BytesCodec.new }
      register("string") { StringCodec.new }
      register("yaml") { YAMLCodec.new }
      register("toml") { TOMLCodec.new }
      register("xml") { XMLCodec.new }
    end
  end
end
