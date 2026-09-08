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

require "time"

require_relative "../ack"

module AceMQ
  module AMQP
    module Patterns
      # The little that the database-backed stores need from a database.
      #
      # Ruby has no DBI. sqlite3, pg, mysql2 and everything built on them spell
      # a query differently, bind parameters differently and report how many
      # rows changed differently, so a store written against one of them is a
      # store that works with one of them. This is the seam that avoids that,
      # and it is deliberately three methods wide: anything more and it becomes
      # an object-relational mapper nobody asked for, anything less and the
      # stores cannot tell whether they won a race.
      #
      # A connection is anything answering:
      #
      #   run(sql, params)              # => Result
      #   placeholder(index)            # "?" or "$1", by driver
      #   constraint_violation?(error)  # whether a raise means "somebody else won"
      #
      # {SQLite3Connection} and {PGConnection} are here because those are the two
      # this library is tested against. Wrapping a pool, a Sequel database or an
      # ActiveRecord connection is a dozen lines of the same shape.
      #
      # None of this is a runtime dependency. Neither driver is required until
      # something hands one over, so a process that only publishes messages does
      # not install a database driver to do it.
      module SQL
        # What a statement did.
        #
        # +rows+ is an array of arrays, always, and never a hash: two drivers
        # disagree about hash keys and none of them disagree about position.
        # +affected+ is how many rows a statement changed, which is the answer a
        # conditional update is asked for — a claim is decided by the row count,
        # never by the select that preceded it.
        Result = Struct.new(:rows, :affected)

        # A table name reaches SQL by concatenation because no database lets one
        # be bound as a parameter. Rejecting anything but a plain identifier is
        # what keeps that safe.
        SAFE_TABLE_NAME = /\A[A-Za-z_][A-Za-z0-9_]{0,62}\z/

        # Fixed-width UTC, so that a string comparison in the database orders
        # instants the way instants are ordered.
        #
        # There is no timestamp type spelled the same way in SQLite and
        # PostgreSQL, and no portable way to bind a Time either. ISO-8601 in UTC
        # is understood by both — PostgreSQL casts it into the column's type,
        # SQLite keeps the text — and because every value is the same width and
        # the same zone, +expires_at <= ?+ means what it says on both.
        TIMESTAMP = "%Y-%m-%dT%H:%M:%S.%6NZ"

        class << self
          # Wraps a driver's connection, or passes one through that is already
          # wrapped.
          #
          # Recognised by the methods the object has rather than by its class,
          # so neither driver has to be loaded for this file to load.
          #
          # @param driver [Object] a +SQLite3::Database+, a +PG::Connection+, or
          #   anything already answering +run+
          # @return [Object] a connection
          def connect(driver)
            return driver if driver.respond_to?(:run) && driver.respond_to?(:placeholder)
            return PGConnection.new(driver) if driver.respond_to?(:exec_params)
            return SQLite3Connection.new(driver) if driver.respond_to?(:execute)

            raise ArgumentError,
                  "#{driver.class} is not a database connection this understands. Hand over " \
                  "a SQLite3::Database, a PG::Connection, or an object answering run(sql, " \
                  "params), placeholder(index) and constraint_violation?(error)."
          end

          # @raise [ArgumentError] when +name+ is not a plain SQL identifier
          def table_name!(name)
            return name.to_s if SAFE_TABLE_NAME.match?(name.to_s)

            raise ArgumentError,
                  "a table must be a plain SQL identifier, was #{name.inspect}. It reaches " \
                  "the statement by concatenation, because no database binds one as a " \
                  "parameter."
          end

          # @param time [Time]
          # @return [String] the instant as fixed-width UTC text
          def at_utc(time) = time.utc.strftime(TIMESTAMP)

          # @return [Time] whatever a driver handed back for a timestamp column
          def time(value)
            return value if value.is_a?(Time)
            return Time.now.utc if value.nil?

            # SQLite hands back the text that was written; PostgreSQL hands back
            # its own rendering of a timestamptz. Time.parse reads both.
            Time.parse(value.to_s)
          rescue ArgumentError
            Time.now.utc
          end

          # Renders a statement's placeholders in the driver's own style.
          #
          # The templates are written with +?+ and belong to this library, so
          # counting them positionally is safe in a way it would not be for
          # arbitrary SQL: none of them contains a literal question mark.
          #
          # @param template [String]
          # @param connection [Object]
          # @return [String]
          def bind(template, connection)
            index = 0
            template.gsub("?") do
              index += 1
              connection.placeholder(index)
            end
          end
        end

        # A connection over the sqlite3 gem.
        #
        # The one the specs run against, and the one to reach for in a test or a
        # single-process service. A SQLite database is a file one process writes
        # at a time, so it is not the shared store the outbox and the idempotency
        # table exist to be — but it is a real database with real transactions,
        # which is what those two need to be tested honestly.
        class SQLite3Connection
          # @param database [SQLite3::Database]
          def initialize(database)
            @database = database
          end

          # The wrapped +SQLite3::Database+.
          attr_reader :database

          def run(sql, params = [])
            rows = @database.execute(sql, params)
            # changes reports the last statement's row count, which is what a
            # conditional update needs and what a select has no use for.
            Result.new(rows, @database.changes)
          end

          # SQLite numbers its parameters for you.
          def placeholder(_index) = "?"

          def constraint_violation?(error)
            defined?(SQLite3::ConstraintException) &&
              error.is_a?(SQLite3::ConstraintException)
          end

          # Runs a block inside a transaction, rolling back if it raises.
          def transaction(&) = @database.transaction(&)
        end

        # A connection over the pg gem.
        #
        # PostgreSQL hands every column back as text, which is why the stores
        # coerce what they read rather than trusting a driver to have guessed.
        class PGConnection
          # @param connection [PG::Connection]
          def initialize(connection)
            @connection = connection
          end

          # The wrapped +PG::Connection+.
          attr_reader :connection

          def run(sql, params = [])
            result = @connection.exec_params(sql, params)
            Result.new(result.values, result.cmd_tuples)
          end

          # PostgreSQL numbers its parameters, and repeats matter: $1 twice is
          # one parameter used twice, which is not what these statements mean,
          # so every one of them is numbered in order.
          def placeholder(index) = "$#{index}"

          def constraint_violation?(error)
            # By SQLSTATE class rather than by class name: 23 is the integrity
            # constraint violations, and the only constraints on these tables
            # are their keys.
            return false unless error.respond_to?(:result) && error.result

            error.result.error_field(PG::PG_DIAG_SQLSTATE).to_s.start_with?("23")
          end

          # pg yields the connection to the block and this seam does not, so the
          # yielded one is dropped rather than passed on: a caller writing
          # +transaction { }+ against this seam gets the same shape whichever
          # driver is underneath.
          def transaction = @connection.transaction { |_| yield }
        end

        # The statement plumbing the three stores share.
        #
        # An includer defines a private +connection+, which is the one every
        # statement uses unless it is handed another — and the one that is
        # handed another is the outbox insert, which is the whole point of it.
        #
        # @api private
        module Statements
          private

          # Runs a statement, rendering its placeholders for this driver.
          def run(sql, params = [], on: connection)
            on.run(SQL.bind(sql, on), params)
          end

          # Runs a statement whose only interesting outcome is whether a key was
          # already there.
          #
          # @return [Boolean] true when the statement ran, false when a
          #   constraint refused it
          def run_unless_taken(sql, params = [], on: connection)
            run(sql, params, on: on)
            true
          rescue StandardError => e
            raise unless on.constraint_violation?(e)

            false
          end

          # A limit is interpolated rather than bound, because no database binds
          # one portably. It is an integer this library bounded itself, so
          # nothing but a number can reach the statement.
          def limit_of(limit, most: self.class::MAX_BATCH)
            limit = limit.to_i
            return most unless limit.positive?

            [limit, most].min
          end
        end
      end
    end
  end
end
