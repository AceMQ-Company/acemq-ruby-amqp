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
require "time"

module AceMQ
  module AMQP
    class TOMLCodec
      # Turns TOML text into a Hash.
      #
      # A recursive-descent reader over +StringScanner+, covering TOML 1.0:
      # bare, quoted and dotted keys; basic, literal and multi-line strings;
      # integers in four bases; floats including +inf+ and +nan+; booleans; all
      # four date and time kinds; arrays; inline tables; tables; and arrays of
      # tables.
      #
      # == The four date and time kinds, and what Ruby has for them
      #
      # TOML distinguishes an instant from a wall-clock reading, and Ruby has
      # types for only some of that:
      #
      # * An offset date-time names an instant, and becomes a +Time+ carrying
      #   the offset the sender wrote.
      # * A local date-time deliberately has no offset — it is a reading on
      #   somebody's wall — and Ruby has no unzoned time. It becomes a +Time+ in
      #   the reading process's own zone, which is the closest honest answer and
      #   is worth knowing before two of them are compared across machines.
      # * A local date becomes a +Date+.
      # * A local time — +10:32:00+, with no date at all — has no Ruby type
      #   whatsoever, so it comes back as the String the sender wrote rather
      #   than as a +Time+ on a day nobody mentioned.
      #
      # @api private
      class Reader
        # TOML this reader could not make sense of.
        #
        # Caught by {TOMLCodec#decode} and re-raised as a {DecodeError}, so
        # nothing outside this file has to know the reader exists.
        class Invalid < StandardError; end

        BARE_KEY = /[A-Za-z0-9_-]+/
        BLANKS = /(?:[ \t\r\n]|#[^\n]*)+/
        INLINE_SPACE = /[ \t]*/

        ESCAPES = {
          "b" => "\b", "t" => "\t", "n" => "\n", "f" => "\f", "r" => "\r",
          '"' => '"', "\\" => "\\"
        }.freeze

        INFINITY = ->(text) { text.start_with?("-") ? -Float::INFINITY : Float::INFINITY }
        INSTANT = ->(text) { Time.iso8601(text.sub(/[Tt ]/, "T").sub(/z\z/, "Z")) }

        # Ordered, because a date starts with digits and would otherwise be read
        # as an integer, and +1.5+ would be read as the integer 1.
        ATOMS = [
          [/true(?![\w-])/, ->(_) { true }],
          [/false(?![\w-])/, ->(_) { false }],
          [/\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})/,
           INSTANT],
          [/\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?/, ->(text) { Time.parse(text) }],
          [/\d{4}-\d{2}-\d{2}(?![\d:])/, ->(text) { Date.iso8601(text) }],
          [/\d{2}:\d{2}:\d{2}(?:\.\d+)?/, ->(text) { text }],
          [/[+-]?inf(?![\w-])/, INFINITY],
          [/[+-]?nan(?![\w-])/, ->(_) { Float::NAN }],
          [/[+-]?\d[\d_]*(?:\.\d[\d_]*)?[eE][+-]?\d[\d_]*/, lambda { |text|
            Float(text.delete("_"))
          }],
          [/[+-]?\d[\d_]*\.\d[\d_]*/, ->(text) { Float(text.delete("_")) }],
          [/0x\h[\h_]*/, ->(text) { Integer(text.delete("_"), 16) }],
          [/0o[0-7][0-7_]*/, ->(text) { Integer(text.delete("_").delete_prefix("0o"), 8) }],
          [/0b[01][01_]*/, ->(text) { Integer(text.delete("_").delete_prefix("0b"), 2) }],
          [/[+-]?\d[\d_]*(?![\w.-])/, ->(text) { Integer(text.delete("_"), 10) }]
        ].freeze

        def initialize(source)
          @scanner = StringScanner.new(source)
          @root = {}
          @table = @root
        end

        # @return [Hash]
        # @raise [Invalid]
        def parse
          loop do
            @scanner.skip(BLANKS)
            break if @scanner.eos?

            statement
            end_of_line!
          end
          @root
        end

        private

        def statement
          if @scanner.scan(/\[\[/)
            open_array_of_tables
          elsif @scanner.scan(/\[/)
            open_table
          else
            read_pair(@table)
          end
        end

        def open_table
          path = dotted_key
          invalid!("expected ] after a table name") unless @scanner.scan(/\]/)
          @table = descend(@root, path)
        end

        def open_array_of_tables
          path = dotted_key
          invalid!("expected ]] after a table name") unless @scanner.scan(/\]\]/)
          parent = descend(@root, path[0..-2])
          entries = (parent[path.last] ||= [])
          unless entries.is_a?(Array)
            invalid!("#{path.join(".")} is a value, not an array of tables")
          end
          @table = {}
          entries << @table
        end

        def read_pair(into)
          path = dotted_key
          @scanner.skip(INLINE_SPACE)
          invalid!("expected = after #{path.join(".")}") unless @scanner.scan(/=/)
          @scanner.skip(INLINE_SPACE)
          assign(into, path, value)
        end

        def dotted_key
          parts = []
          loop do
            @scanner.skip(INLINE_SPACE)
            parts << key_part
            @scanner.skip(INLINE_SPACE)
            break unless @scanner.scan(/\./)
          end
          parts
        end

        def key_part
          bare = @scanner.scan(BARE_KEY)
          return bare if bare
          return basic_string if @scanner.scan(/"/)
          return literal_string if @scanner.scan(/'/)

          invalid!("expected a key")
        end

        def value
          case @scanner.peek(1)
          when '"', "'" then string
          when "[" then array
          when "{" then inline_table
          else atom
          end
        end

        def atom
          ATOMS.each do |pattern, convert|
            text = @scanner.scan(pattern)
            next unless text

            begin
              return convert.call(text)
            rescue ArgumentError, TypeError => e
              invalid!("#{text.inspect} is not a value TOML can hold: #{e.message}")
            end
          end
          invalid!("expected a value")
        end

        def string
          return multiline_basic_string if @scanner.scan(/"""/)
          return basic_string if @scanner.scan(/"/)
          return multiline_literal_string if @scanner.scan(/'''/)
          return literal_string if @scanner.scan(/'/)

          invalid!("expected a string")
        end

        def basic_string
          out = +""
          loop do
            chunk = @scanner.scan(/[^"\\\n]+/)
            next out << chunk if chunk
            return out if @scanner.scan(/"/)

            invalid!("unterminated string") unless @scanner.scan(/\\/)
            out << escape
          end
        end

        def multiline_basic_string
          @scanner.skip(/\r?\n/)
          out = +""
          loop do
            chunk = @scanner.scan(/[^"\\]+/)
            next out << chunk if chunk
            return out << closing_quotes('"') if @scanner.check(/"""/)
            next out << @scanner.scan(/"{1,2}/) if @scanner.check(/"/)

            invalid!("unterminated string") unless @scanner.scan(/\\/)
            # A backslash at the end of a line folds away the break and the
            # indentation after it, which is how one long string is written
            # over several lines without gaining whitespace.
            next @scanner.skip(/[ \t\r\n]*/) if @scanner.skip(/\r?\n/)

            out << escape
          end
        end

        def literal_string
          text = @scanner.scan(/[^'\n]*/)
          invalid!("unterminated string") unless @scanner.scan(/'/)
          text
        end

        def multiline_literal_string
          @scanner.skip(/\r?\n/)
          out = +""
          loop do
            chunk = @scanner.scan(/[^']+/)
            next out << chunk if chunk
            return out << closing_quotes("'") if @scanner.check(/'''/)

            out << @scanner.scan(/'{1,2}/)
          end
        end

        # Up to two quotes may sit against the closing delimiter, so +""""+ is a
        # string ending in one quote rather than an empty one followed by junk.
        def closing_quotes(quote)
          quote * (@scanner.scan(/#{quote}{3,5}/).length - 3)
        end

        def escape
          simple = @scanner.scan(/[btnfr"\\]/)
          return ESCAPES[simple] if simple
          return codepoint(4) if @scanner.scan(/u/)
          return codepoint(8) if @scanner.scan(/U/)

          invalid!("unknown escape after a backslash")
        end

        def codepoint(digits)
          text = @scanner.scan(/\h{#{digits}}/)
          invalid!("a unicode escape needs #{digits} hex digits") unless text
          begin
            [text.hex].pack("U")
          rescue RangeError
            invalid!("\\u#{text} is not a character")
          end
        end

        def array
          @scanner.scan(/\[/)
          items = []
          loop do
            @scanner.skip(BLANKS)
            return items if @scanner.scan(/\]/)

            items << value
            @scanner.skip(BLANKS)
            next if @scanner.scan(/,/)
            return items if @scanner.scan(/\]/)

            invalid!("expected , or ] in an array")
          end
        end

        def inline_table
          @scanner.scan(/\{/)
          table = {}
          @scanner.skip(INLINE_SPACE)
          return table if @scanner.scan(/\}/)

          loop do
            read_pair(table)
            @scanner.skip(INLINE_SPACE)
            next if @scanner.scan(/,/)
            return table if @scanner.scan(/\}/)

            invalid!("expected , or } in an inline table")
          end
        end

        def assign(table, path, held)
          into = descend(table, path[0..-2])
          key = path.last
          invalid!("#{path.join(".")} is set twice") if into.key?(key)
          into[key] = held
        end

        # Walks a dotted path, making the tables it names on the way. A path
        # through an array of tables lands in its last entry, which is what
        # makes +[[a]]+ followed by +[a.b]+ mean what it looks like.
        def descend(table, path)
          path.reduce(table) do |node, key|
            child = (node[key] ||= {})
            child = child.last if child.is_a?(Array)
            invalid!("#{key} is a value, so it cannot also be a table") unless child.is_a?(Hash)
            child
          end
        end

        def end_of_line!
          @scanner.skip(INLINE_SPACE)
          @scanner.skip(/#[^\n]*/)
          return if @scanner.eos? || @scanner.skip(/\r?\n/)

          invalid!("expected the end of the line")
        end

        def invalid!(message)
          line = @scanner.string[0, @scanner.pos].count("\n") + 1
          raise Invalid, "#{message}, at line #{line}"
        end
      end
    end
  end
end
