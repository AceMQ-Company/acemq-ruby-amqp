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

require "sqlite3"

require "acemq/amqp/patterns"

# Against SQLite, which is a real database with real transactions.
#
# A stub cannot test any of this. The whole claim of the outbox store is that
# an insert becomes durable exactly when somebody else's transaction does, and
# the only way to show that is to roll one back and look.
RSpec.describe "the database-backed stores" do
  let(:db) do
    database = SQLite3::Database.new(":memory:")
    database.execute("CREATE TABLE orders (id TEXT PRIMARY KEY)")
    database
  end

  after { db.close }

  describe AceMQ::AMQP::Patterns::SQLOutboxStore do
    let(:transport) { FakeTransport.new }
    let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "checkout@pod-7") }
    let(:store) { described_class.new(connection: db) }

    before { store.create_schema }

    def recorded(payload = { "order_id" => "A-1" }, **fields)
      AceMQ::AMQP::Patterns.record(mq, payload, to: "order.placed",
                                                exchange: "orders-events", **fields)
    end

    describe "the guarantee the pattern exists for" do
      it "writes the message inside the caller's transaction" do
        # Visible to the connection that wrote it, before any commit. If it were
        # not, the insert would be on a connection of the store's own and the
        # message would be durable independently of the work.
        db.transaction do
          store.add(recorded, connection: db)
          expect(store.pending_count).to eq(1)
        end

        expect(store.pending_count).to eq(1)
      end

      it "keeps no message when the transaction that decided to send it rolls back" do
        record = recorded

        expect do
          db.transaction do
            db.execute("INSERT INTO orders (id) VALUES (?)", ["A-1"])
            store.add(record, connection: db)
            raise "the work failed after the message had been recorded"
          end
        end.to raise_error("the work failed after the message had been recorded")

        # Neither the order nor the message. That is the entire point: a store
        # opening its own connection would leave the message behind, queued for
        # something that never happened.
        expect(db.execute("SELECT COUNT(*) FROM orders").dig(0, 0)).to eq(0)
        expect(store.pending_count).to eq(0)
        expect(store.pending).to be_empty
      end

      it "keeps both when the transaction commits" do
        db.transaction do
          db.execute("INSERT INTO orders (id) VALUES (?)", ["A-2"])
          store.add(recorded, connection: db)
        end

        expect(db.execute("SELECT COUNT(*) FROM orders").dig(0, 0)).to eq(1)
        expect(store.pending_count).to eq(1)
      end

      it "takes the connection from a callable, for a framework that binds one per request" do
        supplied = described_class.new(connection: -> { db }, table: "acemq_outbox")

        db.transaction do
          supplied.add(recorded, connection: nil)
          raise "rolled back"
        end
      rescue RuntimeError
        expect(supplied.pending_count).to eq(0)
      end
    end

    describe "what comes back out" do
      it "round trips a record, headers and all" do
        record = recorded({ "order_id" => "A-3" }, type: "order.placed.v2", version: 4)
        store.add(record, connection: db)

        pending = store.pending
        expect(pending.size).to eq(1)
        expect(pending.first.id).to eq(record.id)
        expect(pending.first.body).to eq('{"order_id":"A-3"}')
        expect(pending.first.content_type).to eq("application/json")
        expect(pending.first.exchange).to eq("orders-events")
        expect(pending.first.routing_key).to eq("order.placed")
        expect(pending.first.headers).to eq(record.headers)
      end

      it "carries a body that is not text, which the claim check produces" do
        # A framed claim check starts with 0xAC and is not valid UTF-8, and
        # PostgreSQL refuses invalid UTF-8 in a text column. Losing the message
        # to a driver error would be the worse trade.
        record = recorded
        record.body = [0xAC, 0x01, 0x01, 0xFF].pack("C*")
        store.add(record, connection: db)

        expect(store.pending.first.body.bytes).to eq([0xAC, 0x01, 0x01, 0xFF])
      end

      it "hands them back oldest first" do
        # The order they were written in is usually the order the writer meant,
        # and a relay publishing them out of order has invented a reordering
        # nobody asked for.
        3.times do |n|
          record = recorded({ "order_id" => "A-#{n}" })
          record.created_at = Time.now - (10 - n)
          store.add(record, connection: db)
        end

        expect(store.pending.map { |record| JSON.parse(record.body)["order_id"] })
          .to eq(%w[A-0 A-1 A-2])
      end

      it "records the same message twice as one message" do
        record = recorded
        store.add(record, connection: db)
        store.add(record, connection: db)

        expect(store.pending_count).to eq(1)
      end

      it "removes a record the broker has confirmed" do
        record = recorded
        store.add(record, connection: db)
        store.mark_published(record.id)

        expect(store.pending_count).to eq(0)
        expect(store.pending).to be_empty
      end
    end

    describe "claiming under a lease" do
      it "does not hand the same record to a second relay" do
        store.add(recorded, connection: db)
        other = described_class.new(connection: db)

        expect(store.pending.size).to eq(1)
        expect(other.pending).to be_empty
      end

      it "lets a second relay take over once the lease has run out" do
        # A relay that dies mid-batch has to release its records without anybody
        # intervening, which a lock cannot do and a timestamp can.
        brief = described_class.new(connection: db, lease: 0.05)
        brief.add(recorded, connection: db)
        brief.pending
        sleep 0.1

        expect(described_class.new(connection: db).pending.size).to eq(1)
      end

      it "stops claiming a record that has failed too often" do
        record = recorded
        store.add(record, connection: db)
        2.times { store.pending.each { |r| store.mark_failed(r.id, "the broker is down") } }

        expect(described_class.new(connection: db, max_attempts: 2).pending).to be_empty
        # Still in the table, for somebody to look at: giving up claiming it is
        # not the same as throwing it away.
        expect(store.pending_count).to eq(1)
      end

      it "gives up the lease when a publish failed, so the next sweep can try" do
        record = recorded
        store.add(record, connection: db)
        store.pending
        store.mark_failed(record.id, "connection reset")

        expect(described_class.new(connection: db).pending.size).to eq(1)
      end
    end

    describe "with a relay" do
      it "publishes what a committed transaction left behind" do
        db.transaction { store.add(recorded, connection: db) }
        relay = AceMQ::AMQP::Patterns::OutboxRelay.new(mq, store)

        expect(relay.sweep).to eq(1)
        expect(store.pending_count).to eq(0)
        expect(transport.published.size).to eq(1)
        expect(transport.published.first[:body]).to eq('{"order_id":"A-1"}')
      end

      it "counts the attempt and frees the record when the broker refuses it" do
        db.transaction { store.add(recorded, connection: db) }
        allow(transport).to receive(:publish).and_raise(StandardError, "connection reset")
        relay = AceMQ::AMQP::Patterns::OutboxRelay.new(mq, store)

        expect { relay.sweep }.to raise_error(StandardError, "connection reset")
        # Released rather than left leased for a minute, and counted, so a
        # record nothing can publish eventually stops being claimed.
        expect(store.pending.size).to eq(1)
      end
    end

    describe "housekeeping" do
      it "purges records published longer ago than the age given" do
        record = recorded
        store.add(record, connection: db)
        store.mark_published(record.id)

        expect(store.purge_published(3600)).to eq(0)
        expect(store.purge_published(-1)).to eq(1)
      end

      it "refuses a table name that is not a plain identifier" do
        expect { described_class.new(connection: db, table: "outbox; DROP TABLE orders") }
          .to raise_error(ArgumentError, /plain SQL identifier/)
      end
    end
  end

  describe AceMQ::AMQP::Patterns::SQLIdempotencyStore do
    let(:store) { described_class.new(connection: db) }

    before { store.create_schema }

    def message(id:)
      AceMQ::AMQP::Message.new(
        payload: { "order_id" => "A-1" }, envelope: AceMQ::AMQP::Envelope.new(id: id),
        routing_key: "orders.new", content_type: "application/json",
        redelivered: false, body: '{"order_id":"A-1"}'
      )
    end

    it "is true the first time and false for every repeat" do
      expect(store.first_time?("m-1")).to be(true)
      expect(store.first_time?("m-1")).to be(false)
    end

    it "is shared, which is the whole reason it exists" do
      # Two stores are two workers. The in-memory one gets this wrong by
      # construction: each worker has its own hash, so both are told they are
      # first and the duplicate goes through.
      other = described_class.new(connection: db)

      expect(store.first_time?("m-2")).to be(true)
      expect(other.first_time?("m-2")).to be(false)
    end

    it "lets a message be redone when the hold is given up" do
      store.first_time?("m-3")
      store.forget("m-3")

      expect(store.first_time?("m-3")).to be(true)
    end

    it "will not let one worker give up another's hold" do
      # Deleting somebody else's hold would put two consumers on one message.
      store.first_time?("m-4")
      described_class.new(connection: db).forget("m-4")

      expect(store.first_time?("m-4")).to be(false)
    end

    describe "a hold is a lease, not a lock" do
      it "lets another worker take over a hold whose lease has run out" do
        # A consumer that died mid-handler leaves its row behind. Without the
        # lease every redelivery of that message, for ever, is discarded as a
        # duplicate — a crash that should have cost one retry silently deleting
        # a message instead.
        died = described_class.new(connection: db, claim_timeout: 0.05)
        died.first_time?("m-5")
        sleep 0.1

        expect(described_class.new(connection: db).first_time?("m-5")).to be(true)
      end

      it "does not let anybody take over work that finished" do
        # The difference the lease cannot make on its own: a confirmed key is
        # held for retention rather than for the claim timeout.
        done = described_class.new(connection: db, claim_timeout: 0.05)
        done.first_time?("m-6")
        done.confirm("m-6")
        sleep 0.1

        expect(described_class.new(connection: db).first_time?("m-6")).to be(false)
        expect(store.confirmed?("m-6")).to be(true)
      end

      it "will not let a confirmation be given up as though it were a hold" do
        store.first_time?("m-7")
        store.confirm("m-7")
        store.forget("m-7")

        expect(store.first_time?("m-7")).to be(false)
      end

      it "records the work even when the row went while the handler was running" do
        store.first_time?("m-8")
        store.purge_expired
        db.execute("DELETE FROM acemq_idempotency")
        store.confirm("m-8")

        expect(store.confirmed?("m-8")).to be(true)
      end
    end

    describe "retention" do
      it "forgets a confirmation once its retention has run out" do
        brief = described_class.new(connection: db, retention: 0.05)
        brief.first_time?("m-9")
        brief.confirm("m-9")
        sleep 0.1

        expect(brief.confirmed?("m-9")).to be(false)
        expect(brief.first_time?("m-9")).to be(true)
      end

      it "deletes nothing on the message path, and everything expired on demand" do
        brief = described_class.new(connection: db, claim_timeout: 0.05)
        brief.first_time?("m-10")
        sleep 0.1

        expect(brief.size).to eq(1)
        expect(brief.purge_expired).to eq(1)
        expect(brief.size).to eq(0)
      end
    end

    describe "behind the handler wrapper" do
      it "does a message's work once and accepts the duplicate" do
        done = 0
        handler = AceMQ::AMQP::Patterns.idempotent(store) do |_message|
          done += 1
          AceMQ::AMQP::Ack.accept
        end
        message = message(id: "m-11")

        expect(handler.call(message).accept?).to be(true)
        expect(handler.call(message).accept?).to be(true)
        expect(done).to eq(1)
      end

      it "confirms on the way out, so the key outlives the lease" do
        handler = AceMQ::AMQP::Patterns.idempotent(store) { AceMQ::AMQP::Ack.accept }
        handler.call(message(id: "m-12"))

        expect(store.confirmed?("m-12")).to be(true)
      end

      it "gives up the hold when the handler did not accept" do
        handler = AceMQ::AMQP::Patterns.idempotent(store) do
          AceMQ::AMQP::Ack.retry("the warehouse is down")
        end
        handler.call(message(id: "m-13"))

        expect(store.first_time?("m-13")).to be(true)
      end
    end
  end

  describe AceMQ::AMQP::Patterns::SQLSchemaRegistry do
    let(:registry) { described_class.new(connection: db) }
    let(:definition) { '{"type":"record","name":"OrderPlaced"}' }

    before { registry.create_schema }

    it "hands out identifiers from one, because zero is what an unset field reads as" do
      expect(registry.register("order.placed", "avro", definition).id).to eq(1)
      expect(registry.register("order.shipped", "avro", "{}").id).to eq(2)
    end

    it "returns the same identifier for the same definition, however often it is offered" do
      # A service that registers its schemas on every start would otherwise add
      # a version per restart.
      first = registry.register("order.placed", "avro", definition)
      second = registry.register("order.placed", "avro", definition)

      expect(second.id).to eq(first.id)
      expect(registry.versions("order.placed").size).to eq(1)
    end

    it "keeps an identifier across a restart, which the in-memory one cannot" do
      # The failure that makes the in-memory registry useless in production: a
      # message published today is read next year by a consumer looking up the
      # shape it was written with.
      id = registry.register("order.placed", "avro", definition).id
      after_restart = described_class.new(connection: db)

      expect(after_restart.by_id(id).definition).to eq(definition)
      expect(after_restart.register("order.placed", "avro", definition).id).to eq(id)
    end

    it "numbers the versions of a subject" do
      registry.register("order.placed", "avro", definition)
      registry.register("order.placed", "avro", "#{definition} ")

      expect(registry.versions("order.placed").map(&:version)).to eq([1, 2])
      expect(registry.latest("order.placed").version).to eq(2)
    end

    it "keeps the fingerprint it was registered with" do
      schema = registry.register("order.placed", "avro", definition)

      expect(schema.fingerprint).to eq(AceMQ::AMQP::Patterns.fingerprint(definition))
    end

    it "says so rather than guessing, for a schema it has never held" do
      expect { registry.by_id(99) }
        .to raise_error(AceMQ::AMQP::Patterns::SchemaNotFound, /registered somewhere else/)
      expect { registry.latest("nothing.here") }
        .to raise_error(AceMQ::AMQP::Patterns::SchemaNotFound)
      expect(registry.versions("nothing.here")).to be_empty
    end

    it "refuses a schema with no subject or no definition" do
      expect { registry.register("", "avro", definition) }.to raise_error(ArgumentError)
      expect { registry.register("order.placed", "avro", "") }.to raise_error(ArgumentError)
    end
  end

  describe AceMQ::AMQP::Patterns::SQL do
    it "recognises a driver by what it answers to, not by its class" do
      expect(described_class.connect(db)).to be_a(described_class::SQLite3Connection)
    end

    it "passes through something that is already a connection" do
      wrapped = described_class.connect(db)

      expect(described_class.connect(wrapped)).to be(wrapped)
    end

    it "says what it wanted, when handed something that is not a database" do
      expect { described_class.connect(Object.new) }
        .to raise_error(ArgumentError, /not a database connection/)
    end

    it "renders placeholders in the driver's own style" do
      sqlite = described_class.connect(db)
      template = "SELECT ? FROM t WHERE a = ? AND b = ?"

      expect(described_class.bind(template, sqlite)).to eq(template)
      expect(described_class.bind(template, described_class::PGConnection.new(nil)))
        .to eq("SELECT $1 FROM t WHERE a = $2 AND b = $3")
    end

    it "writes fixed-width UTC, so a string comparison orders instants" do
      earlier = described_class.at_utc(Time.at(0))
      later = described_class.at_utc(Time.at(1_000_000_000))

      expect(earlier).to eq("1970-01-01T00:00:00.000000Z")
      expect(earlier.length).to eq(later.length)
      expect(earlier < later).to be(true)
    end

    it "reads back what either driver hands over for a timestamp" do
      expect(described_class.time("2026-09-08T10:11:12.000000Z").utc.year).to eq(2026)
      expect(described_class.time("2026-09-08 10:11:12.000000+00").utc.hour).to eq(10)
    end
  end
end
