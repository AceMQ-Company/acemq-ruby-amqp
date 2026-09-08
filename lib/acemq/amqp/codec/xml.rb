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

require_relative "../ack"

module AceMQ
  module AMQP
    # Reads and writes XML.
    #
    # Here because most estates have something that speaks XML and will not be
    # rewritten, and a messaging library that cannot talk to it forces a
    # translation layer nobody wants to own. New services should publish JSON;
    # this exists so the ones that cannot are not a special case.
    #
    # == No document type declaration, ever
    #
    # A body carrying a +<!DOCTYPE>+ is refused before it is parsed. A DTD is
    # how an XML message reads files off the machine handling it, opens
    # connections on its behalf, and expands twelve bytes into a gigabyte of
    # heap — and a queue is exactly the sort of place a message from somewhere
    # unexpected arrives. This is not configurable, because the configuration
    # would only ever be wrong, and it costs nothing: the Java library disables
    # DTD support outright and neither Jackson nor Go's +encoding/xml+ writes
    # one.
    #
    # == What a decoded message looks like
    #
    # XML has no types, so every leaf comes back as a String and the caller
    # converts. The root element's name is dropped, because it names the
    # message rather than being part of it — which is also how Jackson and
    # +encoding/xml+ read one into a class:
    #
    #   <order><id>A-1</id><line>widget</line><line>gasket</line></order>
    #   # => { "id" => "A-1", "line" => ["widget", "gasket"] }
    #
    # Elements repeated under one parent become an Array, which is what makes
    # the Go library's lists readable here; Jackson wraps its lists in an
    # element of their own instead, and that comes back as the nesting it is.
    # Attributes become ordinary keys beside the elements, and an element
    # holding both attributes and text puts the text under the empty key, which
    # is the convention Jackson uses reading into a Map.
    #
    # == REXML
    #
    # Ruby's own XML parser, and required lazily. It ships with Ruby, but it has
    # been a *bundled* gem rather than a default one since Ruby 3.4, which means
    # a Bundler process only has it if a Gemfile says so. The gem declares no
    # runtime dependencies, so the failure is caught here and re-raised saying
    # exactly that rather than leaving somebody with +cannot load such file --
    # rexml/document+.
    class XMLCodec
      # What this codec writes, and what Java and Go write.
      CONTENT_TYPE = "application/xml"

      # Predates the registration and is still what a lot of tooling writes.
      ALSO_READS = ["text/xml"].freeze

      # Where the text goes when an element has attributes as well.
      TEXT_KEY = ""

      # A name XML will accept for an element.
      ELEMENT_NAME = /\A[A-Za-z_][\w.-]*\z/

      # @param root [String] the name of the root element this codec writes.
      #   XML has no anonymous document element, and a Hash does not carry a
      #   name the way a Java class does, so one has to be chosen. It matters to
      #   a Go consumer whose struct declares an +XMLName+, and to nobody else:
      #   Jackson ignores the root name when reading into a class, and so does
      #   +encoding/xml+ for a struct without one.
      def initialize(root: "message")
        unless root.to_s.match?(ELEMENT_NAME)
          raise ArgumentError, "#{root.inspect} is not a name XML will take for an element"
        end

        @root = root.to_s
        load_parser!
        freeze
      end

      def content_type = CONTENT_TYPE

      # @param payload [Hash, Array, Object]
      # @return [String]
      # @raise [EncodeError] when a key is not a name XML will take
      def encode(payload)
        document = REXML::Document.new
        fill(document.add_element(@root), payload)
        out = +""
        REXML::Formatters::Default.new.write(document, out)
        out
      end

      # @param body [String]
      # @return [Hash, String]
      # @raise [DecodeError] when the body is not XML, or carries a DTD
      def decode(body)
        text = body.to_s
        if text.match?(/<!DOCTYPE/i)
          raise DecodeError,
                "this message carries a document type declaration, which this codec refuses " \
                "to parse: a DTD is how an XML body reads files off the machine handling it " \
                "and expands a few bytes into a heap full of them."
        end

        root = REXML::Document.new(text).root
        raise DecodeError, "this message has no XML element in it" unless root

        read(root)
      rescue REXML::ParseException => e
        raise DecodeError, "this message is not XML: #{e.message.lines.first.to_s.strip}"
      end

      # Accepts +application/xml+, +text/xml+ and any +...+xml+ media type.
      #
      # Never a message whose sender set no content type. XML is rarely what
      # arrives unannounced, and a codec that guessed wrong here would turn a
      # readable message into a rejected one.
      def can_decode?(content_type)
        lower = content_type.to_s.downcase
        return false if lower.empty?

        lower.start_with?(CONTENT_TYPE, *ALSO_READS) || lower.include?("+xml")
      end

      private

      def load_parser!
        require "rexml/document"
      rescue LoadError => e
        raise DependencyMissing,
              "the AceMQ XML codec needs REXML, which is not installed. It ships with Ruby " \
              "but has been a bundled gem rather than a default one since Ruby 3.4, so a " \
              "Bundler process needs `gem \"rexml\", \"~> 3.3\"` in its Gemfile. (#{e.message})"
      end

      # Fills an element from a payload, making one child element per key and
      # one repeated element per item of a list.
      def fill(element, payload)
        case payload
        when Hash then payload.each { |name, held| add(element, name, held) }
        when Array then payload.each { |item| add(element, element.name, item) }
        else element.text = payload.to_s
        end
        element
      end

      def add(element, name, held)
        text = name.to_s
        unless text.match?(ELEMENT_NAME)
          raise EncodeError, "#{text.inspect} is not a name XML will take for an element"
        end

        return held.each { |item| fill(element.add_element(text), item) } if held.is_a?(Array)

        fill(element.add_element(text), held)
      end

      # Reads an element: its text when it holds nothing else, otherwise a Hash
      # of its attributes and children, with repeated names gathered into lists.
      def read(element)
        attributes = {}
        element.attributes.each { |name, value| attributes[name] = value }
        children = {}
        element.each_element { |child| gather(children, child.name, read(child)) }
        return attributes.merge(children) unless children.empty?
        return text_of(element) if attributes.empty?

        attributes.merge(TEXT_KEY => text_of(element))
      end

      def gather(held, name, value)
        return held[name] = value unless held.key?(name)

        held[name] = [held[name]] unless held[name].is_a?(Array)
        held[name] << value
      end

      def text_of(element)
        element.texts.map(&:value).join
      end
    end
  end
end
