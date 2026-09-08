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
require "securerandom"

require_relative "outbox"
require_relative "sql"

module AceMQ
  module AMQP
    module Patterns
      # An outbox in a database table, in the same database as the work it
      # accompanies.
      #
      #   store = Patterns::SQLOutboxStore.new(connection: db)
      #   store.create_schema                        # development and tests only
      #
      #   db.transaction do
      #     orders.insert(order)
      #     store.add(Patterns.record(mq, event, to: "order.placed"), connection: db)
      #   end
      #
      # The connection handed to +add+ is the point of the whole class.
      # {InMemoryOutboxStore} is criticised in its own comment for not sharing a
      # transaction with anybody's database, and a store that opened its own
      # connection would have exactly that flaw while looking as though it had
      # been dealt with: the insert would commit on its own, and a business
      # write that rolled back afterwards would leave a message queued for
      # something that never happened.
      #
      # So the insert goes on **the caller's connection**, inside the caller's
      # transaction, and this class neither commits it nor closes it. The
      # message becomes durable exactly when the work does, and not before.
      #
      # == Two sources of connections
      #
      # The two halves of this class run in different worlds. +add+ belongs to
      # the caller's transaction. Everything the relay does — claiming, marking,
      # counting — happens on a background thread with no ambient transaction,
      # so it wants a connection of its own. Under a connection pool +relay:+
      # has to be a different connection from the one a request is using, or the
      # relay's thread and the request's thread are writing down the same
      # socket. With one connection — SQLite, a single-threaded service — the
      # same one for both is correct and is the default.
      #
      # == Claiming is a lease, not a lock
      #
      # A held lock lasts as long as its transaction, so a relay that dies
      # mid-batch either strands its rows until the database notices the
      # connection has gone or holds a transaction open across a network
      # publish. Neither is acceptable. A lease is a timestamp: it expires on
      # its own, however the holder died, and the rows return to circulation
      # with nobody intervening.
      #
      # The claim is taken in two statements, and the second is what makes it
      # safe. Candidates are selected, then each is updated with the lease still
      # conditional on being unclaimed. Two relays that select the same
      # candidates both try to update them; row locking serialises the updates,
      # the loser re-evaluates its condition against the committed row and
      # matches nothing. **The claim is decided by the update's row count, never
      # by the select.**
      class SQLOutboxStore
        include SQL::Statements

        DEFAULT_TABLE = "acemq_outbox"

        # How many failures a record may accumulate before it stops being
        # claimed and stays for somebody to look at.
        DEFAULT_MAX_ATTEMPTS = 10

        # How long a relay holds a record before another may take it over.
        # Comfortably longer than a publish and a confirm, and short enough that
        # a relay that died does not strand a batch for long.
        DEFAULT_LEASE = 60.0

        # More than this in one sweep is a relay that has stopped being a relay.
        MAX_BATCH = 10_000

        # Positional, because two drivers disagree about hash keys and neither
        # disagrees about order.
        COLUMNS = "id, exchange_name, routing_key, body, body_encoding, content_type, " \
                  "headers, created_at, attempts, last_error"

        # The table, as the two statements that make it.
        #
        # Written here rather than read from a .sql file because the gem ships
        # lib/**/*.rb and nothing else, and a schema that is missing from the
        # package is a schema that works in the repository and nowhere after
        # it. Keeping it as separate statements also avoids splitting a file on
        # semicolons, which goes wrong the first time a comment contains one.
        def self.ddl(table)
          [
            <<~SQL.chomp,
              CREATE TABLE IF NOT EXISTS #{table} (
                id             VARCHAR(64)              NOT NULL PRIMARY KEY,
                exchange_name  VARCHAR(255)             NOT NULL,
                routing_key    VARCHAR(255)             NOT NULL,
                body           TEXT                     NOT NULL,
                body_encoding  VARCHAR(16)              NOT NULL,
                content_type   VARCHAR(255)             NOT NULL,
                headers        TEXT                     NOT NULL,
                created_at     TIMESTAMP WITH TIME ZONE NOT NULL,
                published_at   TIMESTAMP WITH TIME ZONE,
                attempts       INTEGER                  NOT NULL DEFAULT 0,
                last_error     VARCHAR(1000),
                locked_by      VARCHAR(64),
                locked_until   TIMESTAMP WITH TIME ZONE
              )
            SQL
            # The relay's only query is "oldest unpublished first", so that is
            # what is indexed.
            "CREATE INDEX IF NOT EXISTS #{table}_pending ON #{table} (published_at, created_at)"
          ]
        end

        # @param connection [Object, #call] the caller's connection, or something
        #   that returns it — a lambda reading a thread local is the usual shape
        #   under a framework that binds one per request. What it must never be
        #   is something that opens a fresh connection: that is the flaw this
        #   class exists to avoid.
        # @param relay [Object, #call, nil] connections for the relay's own
        #   work; the same as +connection+ when there is only one
        # @param table [String] a plain SQL identifier
        # @param max_attempts [Integer] failures before a record is left alone
        # @param lease [Numeric] seconds a relay holds a claimed record
        def initialize(connection:, relay: nil, table: DEFAULT_TABLE,
                       max_attempts: DEFAULT_MAX_ATTEMPTS, lease: DEFAULT_LEASE)
          @source = connection
          @relay_source = relay || connection
          @table = SQL.table_name!(table)
          raise ArgumentError, "max_attempts must be at least 1" if max_attempts.to_i < 1
          raise ArgumentError, "a lease must be positive" unless lease.to_f.positive?

          @max_attempts = max_attempts.to_i
          @lease = lease.to_f
        end

        # The table this store reads and writes.
        attr_reader :table

        # Creates the table if it is not already there.
        #
        # For development and tests. In production the table belongs in the
        # migration tool that owns the rest of the schema, alongside the
        # business tables it commits with — a library that creates tables at
        # start-up has taken a decision about when your database changes that is
        # not its to take.
        def create_schema
          self.class.ddl(@table).each { |statement| connection.run(statement, []) }
          nil
        end

        # Records a message to be published, on the caller's connection.
        #
        # Neither committed nor closed here. The connection belongs to the
        # caller's transaction, and this insert becomes durable exactly when
        # that transaction does — which is the guarantee the whole pattern rests
        # on.
        #
        # Adding the same record twice is not an error, because the caller may
        # be retrying its own transaction, but it must not become two messages.
        #
        # @param record [OutboxRecord]
        # @param connection [Object, nil] the connection the caller's
        #   transaction is on, when it is not the one this store was built with
        def add(record, connection: nil)
          raise ArgumentError, "an outbox record needs an id" if record.id.to_s.empty?

          body, encoding = encoded(record.body)
          sql = "INSERT INTO #{@table} (#{COLUMNS}, published_at, locked_by, locked_until) " \
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)"
          run_unless_taken(sql, [record.id.to_s, record.exchange.to_s, record.routing_key.to_s,
                                 body, encoding, record.content_type.to_s,
                                 JSON.generate(record.headers || {}),
                                 SQL.at_utc(record.created_at || Time.now), 0, nil],
                           on: connection_for(connection))
          nil
        end

        # The records waiting, oldest first, claimed under a lease.
        #
        # Claiming rather than merely reading, because two relays that both read
        # the same batch both publish it, and the duplicate is the one thing a
        # relay can avoid without any help from the consumer. What comes back is
        # what this call won, which may be fewer than were asked for and may be
        # none.
        #
        # @param limit [Integer] how many at most; zero for the maximum batch
        # @return [Array<OutboxRecord>]
        def pending(limit = 0)
          token = SecureRandom.uuid
          now = Time.now.utc
          candidates = candidate_ids(limit_of(limit), now)
          return [] if candidates.empty?
          # Every candidate went to another relay between the select and the
          # update. Not an error, and not worth a second round trip: the next
          # sweep will find more.
          return [] if take_lease(candidates, token, now).zero?

          claimed(token)
        end

        # Removes a record the broker has confirmed.
        #
        # The lease is cleared along with the mark, so the row reads plainly
        # afterwards: a published row still showing a lock confuses whoever is
        # reading the table during an incident.
        def mark_published(id)
          run("UPDATE #{@table} SET published_at = ?, locked_by = NULL, locked_until = NULL " \
              "WHERE id = ?", [SQL.at_utc(Time.now), id.to_s])
          nil
        end

        # Records that publishing a record failed, and gives up its lease.
        #
        # Counting the attempt is what eventually stops a record nothing can
        # publish from being claimed on every sweep for ever, and releasing the
        # lease is what lets the next sweep try it rather than waiting out a
        # minute for nothing.
        def mark_failed(id, reason)
          run("UPDATE #{@table} SET attempts = attempts + 1, last_error = ?, " \
              "locked_by = NULL, locked_until = NULL WHERE id = ?",
              [truncate(reason), id.to_s])
          nil
        rescue StandardError
          # Swallowed on purpose. This is already the failure path, and a relay
          # that died here would leave the record leased rather than free;
          # letting the lease expire is the gentler outcome, and the message is
          # still in the table either way.
          nil
        end

        # How many records are waiting.
        def pending_count
          result = run("SELECT COUNT(*) FROM #{@table} WHERE published_at IS NULL", [])
          result.rows.dig(0, 0).to_i
        end
        alias size pending_count

        # Removes published records older than +older_than+ seconds.
        #
        # Nothing deletes them otherwise, and an outbox table that only grows
        # eventually makes the relay's own query slow. How long to keep them is
        # a judgement about auditing rather than about messaging, so it is the
        # caller's to make.
        #
        # @return [Integer] how many rows were removed
        def purge_published(older_than)
          cutoff = SQL.at_utc(Time.now - older_than.to_f)
          run("DELETE FROM #{@table} WHERE published_at IS NOT NULL AND published_at < ?",
              [cutoff]).affected.to_i
        end

        def to_s = "SQLOutboxStore(#{@table})"

        private

        def candidate_ids(limit, now)
          run("SELECT id FROM #{@table} WHERE published_at IS NULL AND attempts < ? " \
              "AND (locked_until IS NULL OR locked_until < ?) ORDER BY created_at, id " \
              "LIMIT #{limit}",
              [@max_attempts, SQL.at_utc(now)])
            .rows.map { |row| row[0] }
        end

        def take_lease(ids, token, now)
          until_at = SQL.at_utc(now + @lease)
          stamp = SQL.at_utc(now)
          ids.sum do |id|
            run("UPDATE #{@table} SET locked_by = ?, locked_until = ? WHERE id = ? " \
                "AND published_at IS NULL AND (locked_until IS NULL OR locked_until < ?)",
                [token, until_at, id, stamp]).affected.to_i
          end
        end

        def claimed(token)
          run("SELECT #{COLUMNS} FROM #{@table} WHERE locked_by = ? AND published_at IS NULL " \
              "ORDER BY created_at, id", [token])
            .rows.map { |row| record_from(row) }
        end

        def record_from(row)
          OutboxRecord.new(
            id: row[0], exchange: row[1].to_s, routing_key: row[2].to_s,
            body: decoded(row[3], row[4]), content_type: row[5].to_s,
            headers: JSON.parse(row[6].to_s), created_at: SQL.time(row[7])
          )
        end

        # A body is stored as text, because a table somebody may have to read
        # during an incident is worth more than a few bytes. A body that is not
        # text — anything a claim check or a binary codec produced — is stored
        # base64 with the column saying so, because PostgreSQL refuses invalid
        # UTF-8 in a text column and losing the message to a driver error would
        # be a worse trade than a third more disk.
        #
        # pack rather than the base64 library, which stopped being a default gem
        # in Ruby 3.4. This gem declares no runtime dependencies, and adding one
        # to spell a four-line encoder would be a poor trade for that.
        def encoded(body)
          text = body.to_s.dup.force_encoding(Encoding::UTF_8)
          return [text, "utf-8"] if text.valid_encoding?

          [[body.to_s].pack("m0"), "base64"]
        end

        def decoded(body, encoding)
          return body.to_s.unpack1("m0") if encoding.to_s == "base64"

          body.to_s
        end

        def truncate(reason)
          reason = reason.to_s
          reason.length <= 1000 ? reason : "#{reason[0, 997]}..."
        end

        def connection_for(given)
          return SQL.connect(resolve(given)) if given

          SQL.connect(resolve(@source))
        end

        # Everything but +add+ runs here: the relay's own work, which has no
        # ambient transaction to join and wants none.
        def connection = SQL.connect(resolve(@relay_source))

        def resolve(source) = source.respond_to?(:call) ? source.call : source
      end
    end
  end
end
