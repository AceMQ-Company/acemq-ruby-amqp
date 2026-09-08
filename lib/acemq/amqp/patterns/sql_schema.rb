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

require_relative "schema"
require_relative "sql"

module AceMQ
  module AMQP
    module Patterns
      # A schema registry in a database table.
      #
      #   registry = Patterns::SQLSchemaRegistry.new(connection: db)
      #   registry.create_schema                     # development and tests only
      #
      #   schema = registry.register("order.placed", "avro", definition)
      #   registry.by_id(schema.id)
      #
      # {InMemorySchemaRegistry} is fine for a test and useless for anything
      # else: identifiers must be stable **for ever**, because a message
      # published today may be read next year by a consumer looking up the shape
      # it was written with. A registry that hands out fresh identifiers on
      # restart makes every message written before the restart unreadable, and
      # does it silently.
      #
      # This is the smallest thing that fixes that. It is not Confluent's
      # registry and does not try to be: no compatibility checking, no
      # versioning interface, no HTTP. What it does is remember which integer
      # stands for which schema, across restarts and across processes.
      #
      # Answers are cached in memory and never invalidated, because neither can
      # change: an identifier stands for one schema for ever, and a schema keeps
      # the identifier it was given.
      #
      # As with {InMemorySchemaRegistry}, **nothing here puts anything on the
      # wire**. Which header carries a schema identifier is a cross-language
      # contract and not one AceMQ has agreed yet.
      class SQLSchemaRegistry
        include SQL::Statements

        DEFAULT_TABLE = "acemq_schema_registry"

        # Two is enough to settle the fingerprint race; the rest are for a
        # database having a day.
        REGISTRATION_ATTEMPTS = 5

        # Positional, because two drivers disagree about hash keys and neither
        # disagrees about order.
        COLUMNS = "id, subject, schema_version, format, definition, fingerprint, registered_at"

        # Written here rather than in a .sql file, because the gem ships
        # lib/**/*.rb and a schema missing from the package is a schema that
        # works in the repository and nowhere after it.
        #
        # +TEXT+ rather than the +CLOB+ the Java schema uses: PostgreSQL has no
        # CLOB, and a definition is text everywhere that does.
        def self.ddl(table)
          [
            <<~SQL.chomp,
              CREATE TABLE IF NOT EXISTS #{table} (
                id            INTEGER                  NOT NULL PRIMARY KEY,
                subject       VARCHAR(255)             NOT NULL,
                schema_version INTEGER                 NOT NULL,
                format        VARCHAR(32)              NOT NULL,
                definition    TEXT                     NOT NULL,
                fingerprint   VARCHAR(64)              NOT NULL,
                registered_at TIMESTAMP WITH TIME ZONE NOT NULL
              )
            SQL
            # The fingerprint is what makes registration idempotent: the same
            # definition offered twice must come back with the same id, from any
            # process, for ever. Unique rather than merely indexed, so two
            # processes racing to register the same schema end with one row and
            # one loser that re-reads instead of a second id for the same bytes.
            "CREATE UNIQUE INDEX IF NOT EXISTS #{table}_fingerprint " \
            "ON #{table} (subject, fingerprint)",
            # One row, holding the last identifier handed out.
            #
            # A sequence would be the obvious tool and is spelled differently on
            # every database this has to run on. Taking the highest id and
            # adding one needs no sequence and is wrong under load: two writers
            # registering two different schemas at the same moment compute the
            # same next id, and one of them loses a race it cannot win by
            # retrying, because the writer it lost to is doing the same
            # arithmetic. Updating this row takes a row lock, so writers queue
            # for an instant and every one of them gets a number.
            <<~SQL.chomp
              CREATE TABLE IF NOT EXISTS #{table}_seq (
                only_row INTEGER NOT NULL PRIMARY KEY,
                last_id  INTEGER NOT NULL
              )
            SQL
          ]
        end

        # @param connection [Object, #call] where the table lives
        # @param table [String] a plain SQL identifier
        def initialize(connection:, table: DEFAULT_TABLE)
          @source = connection
          @table = SQL.table_name!(table)
          @lock = Mutex.new
          @by_id = {}
        end

        # The table this registry reads and writes.
        attr_reader :table

        # Creates the tables if they are not already there, and seeds the
        # counter so the first registration has something to lock rather than
        # something to create.
        def create_schema
          self.class.ddl(@table).each { |statement| connection.run(statement, []) }
          seed_counter
          nil
        end

        # Records a schema and returns it with an identifier.
        #
        # The same definition registered twice returns the same identifier
        # rather than making a second version. Without that, a service that
        # registers its schemas on every start adds a version per restart, and a
        # week later the subject has three hundred identical versions.
        #
        # @param subject [String] groups the versions of one message type
        # @param format [String] "avro", "protobuf", "json-schema" — nothing
        #   here interprets it
        # @param definition [String] the schema
        # @return [SchemaDefinition]
        def register(subject, format, definition)
          if subject.to_s.empty? || definition.to_s.empty?
            raise ArgumentError, "a schema needs a subject and a definition"
          end

          fingerprint = Patterns.fingerprint(definition)
          # The loop is for one race: two writers registering the same schema at
          # the same moment. The unique index lets exactly one of them insert,
          # and the loser comes back round to read the winner's row rather than
          # inventing a second identifier for the same bytes.
          REGISTRATION_ATTEMPTS.times do
            found = by_fingerprint(subject.to_s, fingerprint)
            return remember(found) if found
            next unless insert_new(subject.to_s, format.to_s, definition.to_s, fingerprint)

            return remember(by_fingerprint(subject.to_s, fingerprint))
          end

          raise FatalError,
                "could not register #{subject.inspect} after #{REGISTRATION_ATTEMPTS} " \
                "attempts. Every insert was refused for breaking a constraint and no " \
                "matching fingerprint was there afterwards, so #{@table} has a constraint " \
                "this registry does not know about -- check what a migration added to it."
        end

        # The schema an identifier names.
        #
        # @raise [SchemaNotFound]
        def by_id(id)
          cached = @lock.synchronize { @by_id[id.to_i] }
          return cached if cached

          rows = run("SELECT #{COLUMNS} FROM #{@table} WHERE id = ?", [id.to_i]).rows
          if rows.empty?
            # A message naming a schema this registry has never held is usually
            # one environment reading another's messages, which is worth saying
            # plainly: the alternative guess, a corrupt message, sends people
            # looking in the wrong place.
            raise SchemaNotFound,
                  "no schema with id #{id}. Either a message was written against a schema " \
                  "registered somewhere else, or this is not the registry that wrote it."
          end

          remember(schema_from(rows[0]))
        end

        # The newest version of a subject.
        #
        # @raise [SchemaNotFound]
        def latest(subject)
          rows = run("SELECT #{COLUMNS} FROM #{@table} WHERE subject = ? " \
                     "ORDER BY schema_version DESC LIMIT 1", [subject.to_s]).rows
          raise SchemaNotFound, "no schema for subject #{subject.inspect}" if rows.empty?

          remember(schema_from(rows[0]))
        end

        # Every version of a subject, oldest first.
        #
        # Empty when there are none, because "what versions are there" has a
        # sensible answer for a subject nobody has registered and "which schema
        # is this" does not.
        def versions(subject)
          run("SELECT #{COLUMNS} FROM #{@table} WHERE subject = ? ORDER BY schema_version",
              [subject.to_s]).rows.map { |row| remember(schema_from(row)) }
        end

        # How many schemas this registry holds.
        def size = run("SELECT COUNT(*) FROM #{@table}").rows.dig(0, 0).to_i

        def to_s = "SQLSchemaRegistry(#{@table})"

        private

        def seed_counter
          run_unless_taken("INSERT INTO #{@table}_seq (only_row, last_id) VALUES (1, 0)")
        end

        # The identifier is one past the highest, read and written in one
        # transaction along with the row it belongs to. Either both land or
        # neither does, so a failed insert cannot burn an identifier and leave a
        # gap that looks like a deleted schema.
        def insert_new(subject, format, definition, fingerprint)
          connection.transaction do
            id = next_id
            run("INSERT INTO #{@table} (#{COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [id, subject, version_after(subject), format, definition, fingerprint,
                 SQL.at_utc(Time.now)])
          end
          true
        rescue StandardError => e
          raise unless connection.constraint_violation?(e)

          false
        end

        def next_id
          bumped = run("UPDATE #{@table}_seq SET last_id = last_id + 1 WHERE only_row = 1")
          if bumped.affected.to_i != 1
            raise FatalError,
                  "the counter row for #{@table} is missing. Either create_schema has not " \
                  "run, or a migration created the registry table without #{@table}_seq " \
                  "alongside it."
          end

          run("SELECT last_id FROM #{@table}_seq WHERE only_row = 1").rows.dig(0, 0).to_i
        end

        def version_after(subject)
          run("SELECT COUNT(*) FROM #{@table} WHERE subject = ?", [subject])
            .rows.dig(0, 0).to_i + 1
        end

        def by_fingerprint(subject, fingerprint)
          rows = run("SELECT #{COLUMNS} FROM #{@table} WHERE subject = ? AND fingerprint = ?",
                     [subject, fingerprint]).rows
          rows.empty? ? nil : schema_from(rows[0])
        end

        def schema_from(row)
          SchemaDefinition.new(
            id: row[0].to_i, subject: row[1].to_s, version: row[2].to_i, format: row[3].to_s,
            definition: row[4].to_s, fingerprint: row[5].to_s, registered_at: SQL.time(row[6])
          )
        end

        def remember(schema)
          @lock.synchronize { @by_id[schema.id] = schema }
          schema
        end

        def connection = SQL.connect(@source.respond_to?(:call) ? @source.call : @source)
      end
    end
  end
end
