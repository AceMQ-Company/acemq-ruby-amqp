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

require_relative "idempotency"
require_relative "sql"

module AceMQ
  module AMQP
    module Patterns
      # An idempotency store in a database table, shared by every consumer that
      # points at it.
      #
      #   store = Patterns::SQLIdempotencyStore.new(connection: db)
      #   store.create_schema                        # development and tests only
      #
      #   mq.consume("orders.new", &Patterns.idempotent(store) do |message|
      #     payments.charge(message.payload)
      #     Ack.accept
      #   end)
      #
      # The one {InMemoryIdempotencyStore} cannot replace. An in-process store
      # deduplicates within one process, which is enough when duplicates arrive
      # seconds apart on the same worker and useless the moment there are three
      # workers behind one queue: the redelivery lands on a different one, finds
      # an empty hash, and does the work again. Charging a card twice is not a
      # caching problem.
      #
      # == The failure this has and the in-memory one does not
      #
      # A shared store outlives the process using it, and that cuts both ways.
      # If a consumer takes a key and then dies mid-handler, the row stays in
      # the table. Naively, every redelivery of that message — for ever — is
      # discarded as a duplicate, and a crash that should have cost one retry
      # has silently deleted a message instead. The in-memory store never had
      # this problem only because a crash wiped it.
      #
      # So a key here is held under a **lease**, not a lock. It expires after
      # +claim_timeout+, after which another consumer may take it over. That
      # makes the timeout a real decision: too short and two consumers work on
      # the same message at once while the first is still going; too long and a
      # crashed consumer stalls that message for the duration. It should
      # comfortably exceed the slowest handler, and the default is deliberately
      # generous.
      #
      # Confirmations are kept for +retention+ and then forgotten, because a
      # table remembering every identifier ever seen is a disk-space incident
      # waiting to happen. A duplicate arriving after retention is handled
      # again — retention is the window within which duplicates are actually
      # expected, not a permanent record.
      #
      # == What it does not do
      #
      # This deduplicates the delivery, not the work. If the handler writes to a
      # different database from this table, a crash between the write committing
      # and the confirmation landing leaves the work done and unrecorded, and
      # the redelivery repeats it. Putting this table in the same database as
      # the handler's writes closes that gap, and doing so is the caller's
      # decision, since only the caller knows what the handler touches.
      #
      # Schedule {#purge_expired} — hourly is ample. Nothing on the message path
      # deletes anything, because a store that tidies up on the hot path makes
      # every message pay for it.
      class SQLIdempotencyStore
        include SQL::Statements

        DEFAULT_TABLE = "acemq_idempotency"

        # Long enough for a slow handler, short enough that a crash is not a
        # permanent stall.
        DEFAULT_CLAIM_TIMEOUT = 300.0

        # How long a confirmed identifier is remembered.
        DEFAULT_RETENTION = 24 * 3600.0

        # Taken and being worked on.
        CLAIMED = "CLAIMED"

        # Done, and remembered until retention runs out.
        CONFIRMED = "CONFIRMED"

        # Written here rather than in a .sql file, because the gem ships
        # lib/**/*.rb and a schema missing from the package is a schema that
        # works in the repository and nowhere after it.
        def self.ddl(table)
          [
            <<~SQL.chomp,
              CREATE TABLE IF NOT EXISTS #{table} (
                message_id  VARCHAR(255)             NOT NULL PRIMARY KEY,
                state       VARCHAR(16)              NOT NULL,
                claimed_by  VARCHAR(64),
                recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
                expires_at  TIMESTAMP WITH TIME ZONE NOT NULL
              )
            SQL
            # One column carries both deadlines because they are never both
            # meaningful: while a row is CLAIMED it holds the lease expiry, and
            # once CONFIRMED it holds the retention expiry. Both answer the same
            # question — is this row still binding? — so one index serves the
            # claim path and the purge.
            "CREATE INDEX IF NOT EXISTS #{table}_expiry ON #{table} (expires_at)"
          ]
        end

        # @param connection [Object, #call] where the table lives
        # @param retention [Numeric] seconds a confirmed key is remembered
        # @param claim_timeout [Numeric] seconds one consumer may hold a message
        #   before another may take it over. Must exceed the slowest handler, or
        #   two consumers will work on the same message at once.
        # @param table [String] a plain SQL identifier
        def initialize(connection:, retention: DEFAULT_RETENTION,
                       claim_timeout: DEFAULT_CLAIM_TIMEOUT, table: DEFAULT_TABLE)
          @source = connection
          @table = SQL.table_name!(table)
          raise ArgumentError, "retention must be positive" unless retention.to_f.positive?
          unless claim_timeout.to_f.positive?
            raise ArgumentError, "claim_timeout must be positive"
          end

          @retention = retention.to_f
          @claim_timeout = claim_timeout.to_f
          # Identifies this instance's holds, so {#forget} can only give up its
          # own. Without it, releasing a failed handler's key would also delete
          # a key another worker had legitimately taken over after the lease
          # expired, and both would then run.
          @worker = SecureRandom.uuid
        end

        # The table this store reads and writes.
        attr_reader :table

        # Which worker this store claims as. Two stores on one table have
        # different ones, which is what makes a release specific.
        attr_reader :worker

        # Creates the table if it is not already there.
        #
        # For development and tests. In production the table belongs in the
        # migration tool that owns the rest of the schema.
        def create_schema
          self.class.ddl(@table).each { |statement| connection.run(statement, []) }
          nil
        end

        # Records a key, and says whether this caller is the one to handle it.
        #
        # The insert is the claim. A primary key makes it atomic across every
        # process using this table, which is the entire reason a shared store
        # can be trusted: two consumers racing on the same identifier cannot
        # both succeed, whatever else they are doing.
        #
        # @param key [String]
        # @return [Boolean] true for the caller that may do the work
        def first_time?(key)
          key = key.to_s
          now = Time.now.utc
          return true if take(key, now)

          # Somebody already has a row. Taking it over is allowed only if their
          # hold has run out, and the guard lives in the WHERE clause so that
          # the check and the take-over are one statement — a select followed by
          # an update would let two consumers both pass the check and both
          # conclude they had won.
          taken_over?(key, now)
        end

        # Records that the work is done, and starts the retention clock.
        #
        # Called by {Patterns.idempotent} after a handler accepts. Without it a
        # lease would simply expire and the next duplicate would be handled
        # again, which is the whole failure a shared store is for.
        def confirm(key)
          key = key.to_s
          now = Time.now.utc
          updated = run(
            "UPDATE #{@table} SET state = ?, claimed_by = NULL, recorded_at = ?, " \
            "expires_at = ? WHERE message_id = ?",
            [CONFIRMED, SQL.at_utc(now), SQL.at_utc(now + @retention), key]
          ).affected
          # The row was purged, or the lease expired and somebody else took it.
          # The work still happened, so it is recorded either way: an unrecorded
          # confirmation is how the same charge gets made twice.
          insert_confirmed(key, now) if updated.to_i.zero?
          nil
        end

        # Gives up this store's own hold, so a message that failed can be
        # redone.
        #
        # Only a live hold of this worker's, and never a confirmation. Deleting
        # somebody else's would put two consumers on one message; deleting a
        # confirmation would undo it.
        def forget(key)
          run("DELETE FROM #{@table} WHERE message_id = ? AND state = ? AND claimed_by = ?",
              [key.to_s, CLAIMED, @worker])
          nil
        end

        # Whether a key is recorded as done and still within retention.
        def confirmed?(key)
          rows = run("SELECT expires_at FROM #{@table} WHERE message_id = ? AND state = ?",
                     [key.to_s, CONFIRMED]).rows
          return false if rows.empty?

          # An expired row is answered as false rather than deleted: a read that
          # writes turns every duplicate check into a write on a shared table.
          SQL.time(rows[0][0]) > Time.now
        end

        # Deletes rows nobody is bound by any more: confirmations past their
        # retention, and holds whose lease has run out.
        #
        # @return [Integer] how many rows were removed
        def purge_expired
          run("DELETE FROM #{@table} WHERE expires_at <= ?", [SQL.at_utc(Time.now)])
            .affected.to_i
        end

        # How many identifiers the table holds, expired ones included.
        def size
          run("SELECT COUNT(*) FROM #{@table}").rows.dig(0, 0).to_i
        end

        def to_s = "SQLIdempotencyStore(#{@table})"

        private

        def take(key, now)
          run_unless_taken(
            "INSERT INTO #{@table} (message_id, state, claimed_by, recorded_at, expires_at) " \
            "VALUES (?, ?, ?, ?, ?)",
            [key, CLAIMED, @worker, SQL.at_utc(now), SQL.at_utc(now + @claim_timeout)]
          )
        end

        def taken_over?(key, now)
          run(
            "UPDATE #{@table} SET state = ?, claimed_by = ?, recorded_at = ?, expires_at = ? " \
            "WHERE message_id = ? AND expires_at <= ?",
            [CLAIMED, @worker, SQL.at_utc(now), SQL.at_utc(now + @claim_timeout), key,
             SQL.at_utc(now)]
          ).affected.to_i == 1
        end

        def insert_confirmed(key, now)
          # Another worker may have inserted between the update and this insert.
          # Its row is a hold on work that is already finished; not worth a third
          # round trip to correct, because the worst case is one repeat of an
          # operation that has to be idempotent anyway.
          run_unless_taken(
            "INSERT INTO #{@table} (message_id, state, claimed_by, recorded_at, expires_at) " \
            "VALUES (?, ?, NULL, ?, ?)",
            [key, CONFIRMED, SQL.at_utc(now), SQL.at_utc(now + @retention)]
          )
        end

        def connection = SQL.connect(@source.respond_to?(:call) ? @source.call : @source)
      end
    end
  end
end
