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
require "strscan"
require_relative "../ack"

module AceMQ
  module AMQP
    # Reads and writes TOML.
    #
    # The same audience as {YAMLCodec} — a message a person reads and edits —
    # with the ambiguity removed. TOML has one way to write a string, no
    # significant indentation, and no Norway problem: in YAML +country: NO+ is
    # the boolean false, while here an unquoted +NO+ is a parse error rather
    # than a country that quietly became a boolean. Where a human edits the
    # message and a machine acts on it, that matters more than terseness.
    #
    # A poor choice for high volume: it is text, it is larger than JSON, and it
    # parses more slowly than any of the binary formats.
    #
    # == The shape of the data has to suit it
    #
    # TOML is a table format, so a message body is an object at the top level.
    # A bare list or a bare number is not a TOML document, and this says so
    # rather than writing something no parser will read back. Deep nesting reads
    # poorly too; where the payload is a tree rather than a table, JSON is the
    # honest answer.
    #
    # == Why the parser is written here
    #
    # There is no TOML parser in Ruby's standard library and this gem declares
    # no runtime dependencies, so the reader and writer below are the codec. The
    # bar they are held to is the one that matters: the bytes Jackson's
    # +TomlMapper+ writes in the Java library and the bytes BurntSushi's encoder
    # writes in the Go one. Those two do not agree on everything — Jackson
    # quotes strings with +'+ and Go with +"+ — so a reader that handled only
    # one of them would be a codec that could read half the estate.
    class TOMLCodec
      # What this codec writes: the type the TOML specification registered, and
      # what Java, Go and .NET write.
      CONTENT_TYPE = "application/toml"

      # Predates the registration and is still what a lot of tooling writes.
      ALSO_READS = ["text/toml"].freeze

      def content_type = CONTENT_TYPE

      # @param payload [Hash] a table; TOML has no document that is not one
      # @return [String]
      # @raise [EncodeError] when the payload is not a table, or holds a value
      #   TOML cannot spell
      def encode(payload)
        unless payload.is_a?(Hash)
          raise EncodeError,
                "cannot encode a #{payload.class} as TOML: a TOML document is a table, so " \
                "the top level has to be a Hash. A list, a string or a number has no TOML " \
                "representation — wrap it in a hash with a named key, or use JSON."
        end

        Writer.new.write(payload)
      end

      # @param body [String]
      # @return [Hash]
      # @raise [DecodeError] when the body is not TOML
      def decode(body)
        Reader.new(body.to_s).parse
      rescue Reader::Invalid => e
        raise DecodeError, "this message is not TOML: #{e.message}"
      end

      # Accepts +application/toml+, +text/toml+ and any +...+toml+ media type.
      #
      # Never a message whose sender set no content type: the same reasoning as
      # {YAMLCodec}. A sender that said nothing is almost always sending JSON,
      # and answering would be right about the value and wrong about the format.
      def can_decode?(content_type)
        lower = content_type.to_s.downcase
        return false if lower.empty?

        lower.start_with?(CONTENT_TYPE, *ALSO_READS) || lower.include?("+toml")
      end
    end
  end
end

require_relative "toml/reader"
require_relative "toml/writer"
