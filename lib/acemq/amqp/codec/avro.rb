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
require "stringio"
require_relative "../ack"

module AceMQ
  module AMQP
    # Reads and writes Avro.
    #
    # Compact on the wire and, unlike Protocol Buffers, able to resolve a
    # writer's schema against a reader's — which is what lets a producer add a
    # field without every consumer being redeployed the same afternoon.
    #
    # == There is no codec without a schema, and there cannot be one
    #
    # Avro's bytes describe nothing about themselves: a reader must already hold
    # the schema the writer used, or the message is unreadable. So this is
    # reached through {of} or {registered} rather than +new+, and +avro+ is not
    # a name {Codecs} knows — a name in configuration cannot carry a schema, and
    # the Java library leaves it out of its own registry for the same reason.
    #
    # == Two modes, and the choice matters more than it looks
    #
    # {of} fixes one schema for the codec's whole life. Small, fast, and nothing
    # extra to run — and the writer's schema is whatever the reader happens to
    # hold. The moment a producer adds a field, every consumer still holding the
    # old schema reads the new bytes wrongly, and Avro will not always notice.
    # Sound only where producer and consumer are released together.
    #
    #   codec = AvroCodec.of(schema_json)
    #
    # {registered} writes the schema's identifier into the front of every
    # message, so a reader can look up exactly what the writer used and let Avro
    # resolve it against its own. This is what makes adding a field safe, and it
    # is the mode to use unless there is a reason not to. The framing is one
    # zero byte, then four bytes of identifier, big-endian, then the Avro body —
    # the layout Confluent's clients use, so messages written here can be read
    # by them, and by the Java, Go and .NET libraries, and the other way round.
    #
    #   registry = Patterns::InMemorySchemaRegistry.new
    #   codec = AvroCodec.registered(registry, subject: "order.placed", schema: schema_json)
    #
    # == Reading against a schema of your own
    #
    # A registered codec resolves every message onto the schema it holds, and by
    # default that is the same schema it writes with. Where the two want to be
    # different, +reader_schema:+ says so:
    #
    #   codec = AvroCodec.registered(registry, subject: "order.placed",
    #                                schema: v3_json, reader_schema: v1_json)
    #
    # The codec then publishes +v3+ and registers +v3+ under the subject, while
    # every message it reads — whatever version wrote it — is resolved onto
    # +v1+. That is the shape a consumer usually wants, and it is not the same
    # as passing +v1+ as +schema:+: a codec that both reads and publishes would
    # then register +v1+ as a new version of the subject and walk the subject
    # backwards. Every library has this: Java spells it
    # +registered(registry, readerSchema)+, .NET +ReaderSchema+, Go
    # +avro.ReadAs(schema)+ and Python +reader_schema=+, which is the spelling
    # followed here.
    #
    # What resolution buys is Avro's, not this library's: a field the writer
    # added that the reader has never heard of is skipped rather than shifting
    # every field after it, and a field the reader expects that the writer never
    # sent is filled in from the reader's own default. A change Avro cannot
    # resolve — a field whose type changed, a field added without a default —
    # raises {DecodeError} naming both schemas, because no reader schema will
    # make those bytes readable and the sooner that is said the better.
    #
    # == Why each mode claims only its own content type
    #
    # The two framings are not interchangeable and the difference is invisible
    # in the bytes: a registered message begins with five bytes of framing that
    # a fixed-schema codec reads as the beginning of the first field. That does
    # not throw — Avro decodes the shifted bytes into whatever they happen to
    # mean — so a codec that accepted the other framing would hand back a record
    # full of silent nonsense. Each accepts only its own, which is what the Java
    # and .NET libraries do. The Go library accepts both in either mode, and is
    # the outlier.
    #
    # Which is why the content type, and not the shape of the bytes, decides
    # what a fixed-schema codec reads: the sender already said which framing it
    # wrote, and guessing from a leading zero byte refuses real messages whose
    # first field encodes to zero. See {unframed} for the whole rule.
    #
    # == The gem
    #
    # +avro+, required lazily and named when it is missing, so that the gem can
    # go on declaring no runtime dependencies.
    class AvroCodec
      # What a codec with a fixed schema writes. Java, Go and .NET agree.
      FIXED_CONTENT_TYPE = "avro/binary"

      # What a codec framing a schema identifier writes. Java, Go and .NET
      # agree.
      REGISTERED_CONTENT_TYPE = "application/vnd.acemq.avro"

      # Confluent's framing: one zero byte, then four bytes of identifier.
      MAGIC = 0
      FRAME_BYTES = 5

      class << self
        # @param schema [String, Hash] an Avro schema, as JSON text or as the
        #   parsed structure
        # @return [AvroCodec] a codec fixed to that schema, carrying nothing on
        #   the wire
        def of(schema)
          new(schema: schema)
        end

        # @param registry [#register, #by_id] where schema identifiers are
        #   resolved; {Patterns::InMemorySchemaRegistry} is one
        # @param subject [String] groups the versions of one message type,
        #   conventionally the type itself: "order.placed"
        # @param schema [String, Hash] the schema this codec writes with, and
        #   the one it registers under +subject+. Also what it reads against,
        #   unless +reader_schema+ says otherwise.
        # @param reader_schema [String, Hash, nil] the schema this consumer was
        #   written against. Every message is resolved onto it from whatever the
        #   writer used, which is what schema evolution actually needs: a field
        #   the reader does not know is skipped, and one the writer omitted is
        #   filled in from the reader's default. Left out, +schema+ serves as
        #   both, which is what this did before the keyword existed.
        # @return [AvroCodec]
        def registered(registry, subject:, schema:, reader_schema: nil)
          unless registry.respond_to?(:register) && registry.respond_to?(:by_id)
            raise ArgumentError,
                  "#{registry.inspect} is not a schema registry: it needs register and by_id"
          end

          new(schema: schema, registry: registry, subject: subject,
              reader_schema: reader_schema)
        end
      end

      # @api private
      def initialize(schema:, registry: nil, subject: nil, reader_schema: nil)
        load_runtime!
        @schema = parse(schema)
        # Only a registered codec learns the writer's schema per message, so
        # only a registered codec has two schemas to resolve. A fixed one reads
        # what it writes by definition, and a reader schema there would be a
        # setting that quietly did nothing.
        if reader_schema && registry.nil?
          raise ArgumentError,
                "a reader schema only means something with a registry, which is where the " \
                "writer's schema comes from. Use registered(registry, ...) to read against it."
        end

        @reader_schema = reader_schema.nil? ? @schema : parse(reader_schema)
        @registry = registry
        @subject = subject
        @lock = Mutex.new
        @schema_id = nil
        @by_id = {}
      end

      # Whether messages carry a schema identifier.
      def registered? = !@registry.nil?

      # +avro/binary+ with a fixed schema, +application/vnd.acemq.avro+ with a
      # registry, because the two framings are different messages.
      def content_type = registered? ? REGISTERED_CONTENT_TYPE : FIXED_CONTENT_TYPE

      # @param payload [Hash, Object] a datum matching the schema
      # @return [String] the bytes to publish
      # @raise [EncodeError] when the payload does not fit the schema
      def encode(payload)
        out = StringIO.new(+"".b, "wb")
        out.write(frame(schema_id)) if registered?
        Avro::IO::DatumWriter.new(@schema).write(payload, Avro::IO::BinaryEncoder.new(out))
        out.string
      rescue EncodeError
        raise
      rescue StandardError => e
        raise EncodeError,
              "this payload does not fit the Avro schema #{schema_name}: #{e.message}"
      end

      # @param body [String]
      # @param content_type [String, nil] what the sender said these bytes are,
      #   which is the only reliable way to tell the two framings apart
      # @return [Object] the datum, resolved onto this codec's reader schema
      # @raise [DecodeError] when the bytes are not Avro this codec can read, or
      #   when the writer's schema cannot be resolved onto the reader's
      def decode(body, content_type = nil)
        bytes = body.to_s.b
        writer_schema, offset = registered? ? framed(bytes) : unframed(bytes, content_type)
        # Both schemas go to Avro, which is the whole point: it resolves the
        # difference rather than trusting that there is none.
        reader = Avro::IO::DatumReader.new(writer_schema, @reader_schema)
        reader.read(Avro::IO::BinaryDecoder.new(StringIO.new(bytes[offset..].to_s)))
      rescue DecodeError
        raise
      rescue StandardError => e
        raise DecodeError, unresolvable(writer_schema, e) if unresolvable?(writer_schema, e)

        raise DecodeError,
              "this message is not Avro that reads as #{reader_schema_name}: #{e.message}"
      end

      # Accepts the Avro content types, and only the framing this codec writes.
      #
      # +application/avro+ and any +...+avro+ media type are taken in either
      # mode, because both are names for Avro without saying which framing, and
      # all four libraries accept them.
      #
      # Never a message whose sender set no content type: Avro bytes are not
      # recognisable, so volunteering would mean decoding whatever arrived and
      # reporting nonsense as a success.
      def can_decode?(content_type)
        lower = content_type.to_s.downcase
        return false if lower.empty?
        return registered? if lower.start_with?(REGISTERED_CONTENT_TYPE)
        return !registered? if lower.start_with?(FIXED_CONTENT_TYPE, "avro/")

        lower.start_with?("application/avro") || lower.include?("+avro")
      end

      private

      def load_runtime!
        require "avro"
      rescue LoadError => e
        raise DependencyMissing,
              "the AceMQ Avro codec needs the avro gem, which is not installed. Add " \
              "`gem \"avro\", \"~> 1.12\"` to your Gemfile, or run `gem install avro`. " \
              "(#{e.message})"
      end

      def parse(schema)
        return schema if schema.is_a?(Avro::Schema)

        Avro::Schema.parse(schema.is_a?(String) ? schema : JSON.generate(schema))
      rescue StandardError => e
        # Broadly, because the avro gem parses schema text through MultiJson and
        # what comes back is whichever JSON library happens to be loaded in the
        # process. Naming those would be naming somebody else's dependencies.
        raise ArgumentError, "this is not a usable Avro schema: #{e.message}"
      end

      # Registers this codec's schema once and remembers the identifier, so a
      # producer that registers on every start does not add a version per
      # restart.
      def schema_id
        @lock.synchronize do
          @schema_id ||= @registry.register(@subject, "avro", @schema.to_s).id
        end
      end

      def frame(id)
        [MAGIC, id].pack("CN")
      end

      # A record schema has a full name; a schema that is a bare +string+ or an
      # array of them has only a type, and an error message still has to say
      # what was being read.
      def name_of(schema)
        return "an unknown schema" if schema.nil?

        schema.respond_to?(:fullname) ? schema.fullname : schema.type_sym
      end

      def schema_name = name_of(@schema)

      def reader_schema_name = name_of(@reader_schema)

      # Whether a failure inside Avro's reader is the two schemas disagreeing
      # rather than the bytes being wrong.
      #
      # Asked only once something has already gone wrong, so the compatibility
      # check is off the ordinary path entirely and nothing that decodes today
      # stops decoding because a checker was stricter than the reader. Asked at
      # all because the difference is the whole of what the reader has to do
      # next: bad bytes are one message to park, and an incompatible schema is
      # every message from that producer until somebody changes a schema.
      #
      # +SchemaMatchException+ is the clear case. The rest are not: a reader
      # field the writer never sent and that has no default fails as a plain
      # +Avro::AvroError+, indistinguishable by class from a truncated body, so
      # the schemas themselves are asked rather than the message text.
      #
      # The two are compared as text rather than with +==+, which two record
      # schemas of the same full name satisfy however differently their fields
      # are declared — exactly the pair being asked about here.
      def unresolvable?(writer_schema, cause)
        return false if writer_schema.nil? || writer_schema.to_s == @reader_schema.to_s
        return true if cause.is_a?(Avro::IO::SchemaMatchException)

        !Avro::SchemaCompatibility.can_read?(writer_schema, @reader_schema)
      rescue StandardError
        # A compatibility checker that cannot answer is not a reason to lose the
        # error that got us here.
        false
      end

      # What to say when Avro will not resolve one schema onto the other.
      #
      # Both schemas are named, and the writer's is quoted in full, because the
      # two are usually two versions of one type and the full name alone names
      # them identically. The reader's is not quoted: it is in the caller's own
      # code, and it is the writer's — a version registered by some other
      # process, possibly some other language — that nobody has in front of
      # them.
      def unresolvable(writer_schema, cause)
        # Avro's own messages end with a full stop about half the time.
        said = cause.message.to_s.sub(/\s*\.?\z/, "")
        "this message was written against #{name_of(writer_schema)} and this codec reads " \
          "#{reader_schema_name}, and Avro cannot resolve one onto the other: #{said}. " \
          "That is an incompatible change rather than an evolution — a field whose type " \
          "changed, or one added without a default — and no reader schema makes those bytes " \
          "readable. The writer's schema was #{writer_schema}"
      end

      # The writer's schema for a message that carries an identifier.
      def framed(bytes)
        if bytes.bytesize < FRAME_BYTES || bytes.getbyte(0) != MAGIC
          raise DecodeError,
                "this message carries no schema identifier, so it was written by a codec " \
                "with a fixed schema rather than a registered one."
        end

        [schema_for(bytes[1, 4].unpack1("N")), FRAME_BYTES]
      end

      # The writer's schema for a message with no identifier on the front.
      #
      # A fixed-schema codec handed framed bytes would read the five bytes of
      # identifier as the beginning of the first field: no exception, and a
      # record whose every value is wrong. What the content type says decides
      # it, and only when it says nothing does the shape of the bytes get a
      # vote:
      #
      # * +avro/binary+, +application/avro+ or any +...+avro+ type — the sender
      #   has said this is a fixed-schema body, so it is read as one and the
      #   bytes are not second-guessed.
      # * +application/vnd.acemq.avro+ — the registry framing, which this codec
      #   cannot read, so it is refused.
      # * nothing, or something that names no Avro type at all — a guess is all
      #   there is, and a body of five or more bytes beginning with a zero one
      #   is refused as probably framed.
      #
      # The guess is a last resort because it is wrong about real messages: a
      # legitimate Avro body begins with a zero byte whenever its first field
      # encodes to zero — an empty string, a +0+, a +false+, branch 0 of a
      # union. Refusing those to catch a framing the content type already
      # names is the wrong trade, and Java and Python read it the same way.
      def unframed(bytes, content_type)
        named = content_type.to_s.downcase
        if named.start_with?(REGISTERED_CONTENT_TYPE)
          raise DecodeError,
                "this message says it carries a schema identifier and this codec has a " \
                "fixed schema, so reading it would quietly produce the wrong values. Build " \
                "the codec with registered(registry, ...) to read it."
        end
        if !names_avro?(named) && looks_framed?(bytes)
          raise DecodeError,
                "these bytes look like they carry a schema identifier and nothing said what " \
                "they are, so a fixed-schema codec will not guess. Build the codec with " \
                "registered(registry, ...), or set the message's content type to " \
                "#{FIXED_CONTENT_TYPE} if it really has a fixed schema."
        end

        [@schema, 0]
      end

      # Whether the sender named a type that means Avro without a framing, or
      # the fixed-schema one. +application/vnd.acemq.avro+ is excluded by the
      # caller, which has already refused it.
      def names_avro?(named)
        return false if named.empty?

        named.start_with?(FIXED_CONTENT_TYPE, "avro/", "application/avro") ||
          named.include?("+avro")
      end

      def looks_framed?(bytes) = bytes.bytesize >= FRAME_BYTES && bytes.getbyte(0) == MAGIC

      def schema_for(id)
        cached = @lock.synchronize { @by_id[id] }
        return cached if cached

        definition = @registry.by_id(id)
        unless definition.format.to_s == "avro"
          raise DecodeError,
                "schema id #{id} is registered as #{definition.format}, and this codec reads " \
                "avro. The message was written by something else."
        end

        parse(definition.definition).tap { |schema| @lock.synchronize { @by_id[id] = schema } }
      end
    end
  end
end
