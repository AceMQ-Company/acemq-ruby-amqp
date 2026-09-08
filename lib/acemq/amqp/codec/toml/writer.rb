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
require "time"
require_relative "../../ack"

module AceMQ
  module AMQP
    class TOMLCodec
      # Turns a Hash into TOML text.
      #
      # Values that belong to the table come first and the sub-tables after
      # them, because TOML reads every key after a +[header]+ as belonging to
      # that header — a scalar written after a sub-table would silently move
      # into it.
      #
      # Strings are written as basic strings, with double quotes, which is what
      # BurntSushi's encoder writes in the Go library. Jackson writes literal
      # strings with single quotes in the Java one. Both are TOML, and both read
      # back identically here; the choice only decides what a person sees.
      #
      # @api private
      class Writer
        ESCAPES = {
          "\\" => "\\\\", '"' => '\\"', "\b" => "\\b", "\t" => "\\t",
          "\n" => "\\n", "\f" => "\\f", "\r" => "\\r"
        }.freeze

        # Backslash, quote, and the control characters, which TOML does not
        # allow to stand in a basic string.
        NEEDS_ESCAPING = /[\\"[:cntrl:]]/

        UNQUOTED_KEY = /\A[A-Za-z0-9_-]+\z/

        # @param table [Hash]
        # @return [String]
        def write(table)
          out = +""
          emit(table, [], out)
          out
        end

        private

        def emit(table, path, out)
          pairs, nested = table.partition { |_, held| !table_shaped?(held) }
          pairs.each { |name, held| out << "#{key(name)} = #{scalar(held)}\n" }
          nested.each { |name, held| emit_nested(held, path + [key(name)], out) }
        end

        def emit_nested(held, path, out)
          return emit_headed(held, path, out) unless held.is_a?(Array)

          held.each do |entry|
            out << "\n[[#{path.join(".")}]]\n"
            emit(entry, path, out)
          end
        end

        def emit_headed(table, path, out)
          out << "\n[#{path.join(".")}]\n"
          emit(table, path, out)
        end

        # A Hash, or a list of nothing but hashes, is written under a header of
        # its own. Everything else goes on one line, inline tables included: a
        # list mixing hashes with anything else has no header form in TOML.
        def table_shaped?(held)
          return true if held.is_a?(Hash)

          held.is_a?(Array) && !held.empty? && held.all?(Hash)
        end

        def key(name)
          text = name.to_s
          text.match?(UNQUOTED_KEY) ? text : quoted(text)
        end

        def scalar(held)
          case held
          when Hash then inline_table(held)
          when Array then inline_array(held)
          when String, Symbol then quoted(held.to_s)
          else atom(held)
          end
        end

        def atom(held)
          case held
          when true, false, Integer then held.to_s
          when Float then float(held)
          when Time then held.xmlschema
          when DateTime then held.rfc3339
          when Date then held.iso8601
          else refuse(held)
          end
        end

        def float(held)
          return "nan" if held.nan?
          return held.negative? ? "-inf" : "inf" if held.infinite?

          held.to_s
        end

        def inline_array(held)
          "[#{held.map { |item| scalar(item) }.join(", ")}]"
        end

        def inline_table(held)
          "{ #{held.map { |name, item| "#{key(name)} = #{scalar(item)}" }.join(", ")} }"
        end

        def quoted(text)
          "\"#{text.gsub(NEEDS_ESCAPING) { |char| escape(char) }}\""
        end

        def escape(char)
          ESCAPES[char] || format("\\u%04X", char.ord)
        end

        def refuse(held)
          if held.nil?
            raise EncodeError,
                  "TOML has no null, so a nil cannot be written. Leave the key out, or " \
                  "publish this message as JSON."
          end

          raise EncodeError, "a #{held.class} has no TOML representation"
        end
      end
    end
  end
end
