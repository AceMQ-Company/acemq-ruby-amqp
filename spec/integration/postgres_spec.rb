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

require "pg"

require "acemq/amqp/patterns"

# The same stores, against PostgreSQL.
#
# SQLite proves the logic and PostgreSQL proves the dialect, and they are not
# the same proof. The parameter markers differ ($1 rather than ?), every column
# comes back as text rather than as the type it was written with, a timestamptz
# is rendered its own way, and a duplicate key raises a different exception. A
# store that only ever met SQLite would be a store that claims PostgreSQL
# support nobody has run.
#
# Every table here starts with rbit_ so that a database shared with anything
# else is left alone, and all of them are dropped afterwards.
RSpec.describe "the database-backed stores against PostgreSQL", :postgres do
  PG_PREFIX = "rbit_"

  let(:db) { PG.connect(ENV.fetch("ACEMQ_TEST_POSTGRES")) }
  let(:outbox) do
    AceMQ::AMQP::Patterns::SQLOutboxStore.new(connection: db, table: "#{PG_PREFIX}outbox")
  end
  let(:keys) do
    AceMQ::AMQP::Patterns::SQLIdempotencyStore.new(connection: db,
                                                   table: "#{PG_PREFIX}idempotency")
  end
  let(:registry) do
    AceMQ::AMQP::Patterns::SQLSchemaRegistry.new(connection: db, table: "#{PG_PREFIX}registry")
  end
  let(:transport) { FakeTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "checkout@pod-7") }

  before do
    db.exec("CREATE TABLE IF NOT EXISTS #{PG_PREFIX}orders (id TEXT PRIMARY KEY)")
    outbox.create_schema
    keys.create_schema
    registry.create_schema
  end

  after do
    %W[#{PG_PREFIX}outbox #{PG_PREFIX}idempotency #{PG_PREFIX}registry #{PG_PREFIX}registry_seq
       #{PG_PREFIX}orders].each do |table|
      db.exec("DROP TABLE IF EXISTS #{table}")
    end
    db.close
  end

  def recorded(payload = { "order_id" => "A-1" })
    AceMQ::AMQP::Patterns.record(mq, payload, to: "order.placed", exchange: "orders-events")
  end

  describe "the outbox" do
    it "keeps no message when the transaction that decided to send it rolls back" do
      record = recorded

      expect do
        db.transaction do
          db.exec_params("INSERT INTO #{PG_PREFIX}orders (id) VALUES ($1)", ["A-1"])
          outbox.add(record, connection: db)
          raise "the work failed after the message had been recorded"
        end
      end.to raise_error("the work failed after the message had been recorded")

      expect(db.exec("SELECT COUNT(*) FROM #{PG_PREFIX}orders").values.dig(0, 0).to_i).to eq(0)
      expect(outbox.pending_count).to eq(0)
    end

    it "keeps both when the transaction commits" do
      db.transaction do
        db.exec_params("INSERT INTO #{PG_PREFIX}orders (id) VALUES ($1)", ["A-2"])
        outbox.add(recorded, connection: db)
      end

      expect(db.exec("SELECT COUNT(*) FROM #{PG_PREFIX}orders").values.dig(0, 0).to_i).to eq(1)
      expect(outbox.pending_count).to eq(1)
    end

    it "round trips a record through columns PostgreSQL hands back as text" do
      record = recorded({ "order_id" => "A-3" })
      outbox.add(record, connection: db)

      pending = outbox.pending
      expect(pending.size).to eq(1)
      expect(pending.first.id).to eq(record.id)
      expect(pending.first.body).to eq('{"order_id":"A-3"}')
      expect(pending.first.headers).to eq(record.headers)
      expect(pending.first.created_at).to be_a(Time)
    end

    it "claims under a lease, so a second relay does not publish the same record" do
      outbox.add(recorded, connection: db)
      other = AceMQ::AMQP::Patterns::SQLOutboxStore.new(connection: db,
                                                        table: "#{PG_PREFIX}outbox")

      expect(outbox.pending.size).to eq(1)
      expect(other.pending).to be_empty
    end

    it "publishes what a committed transaction left behind" do
      db.transaction { outbox.add(recorded, connection: db) }
      relay = AceMQ::AMQP::Patterns::OutboxRelay.new(mq, outbox)

      expect(relay.sweep).to eq(1)
      expect(outbox.pending_count).to eq(0)
    end
  end

  describe "the idempotency store" do
    it "lets one worker in and turns the other away" do
      other = AceMQ::AMQP::Patterns::SQLIdempotencyStore.new(connection: db,
                                                             table: "#{PG_PREFIX}idempotency")

      expect(keys.first_time?("m-1")).to be(true)
      expect(other.first_time?("m-1")).to be(false)
    end

    it "leaves the connection usable after a duplicate key" do
      # The PostgreSQL-only failure: an error inside a transaction poisons it
      # until a rollback, so a claim that collides has to be an ordinary
      # statement rather than one inside a transaction of the store's own.
      keys.first_time?("m-2")
      keys.first_time?("m-2")

      expect(keys.size).to eq(1)
      expect(keys.first_time?("m-3")).to be(true)
    end

    it "holds a confirmation past the lease and a claim only until it expires" do
      brief = AceMQ::AMQP::Patterns::SQLIdempotencyStore.new(
        connection: db, claim_timeout: 0.05, table: "#{PG_PREFIX}idempotency"
      )
      brief.first_time?("m-4")
      brief.first_time?("m-5")
      brief.confirm("m-5")
      sleep 0.1

      expect(keys.first_time?("m-4")).to be(true)
      expect(keys.first_time?("m-5")).to be(false)
    end
  end

  describe "the schema registry" do
    it "hands out stable identifiers through the counter row" do
      first = registry.register("order.placed", "avro", "{}")
      second = registry.register("order.shipped", "avro", "{}")

      expect([first.id, second.id]).to eq([1, 2])
      expect(registry.register("order.placed", "avro", "{}").id).to eq(1)
    end

    it "reads a schema back after a restart" do
      id = registry.register("order.placed", "avro", '{"a":1}').id
      after_restart = AceMQ::AMQP::Patterns::SQLSchemaRegistry.new(
        connection: db, table: "#{PG_PREFIX}registry"
      )

      expect(after_restart.by_id(id).definition).to eq('{"a":1}')
      expect(after_restart.latest("order.placed").version).to eq(1)
    end
  end
end
