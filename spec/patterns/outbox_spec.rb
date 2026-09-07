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

require "acemq/amqp/patterns"

RSpec.describe AceMQ::AMQP::Patterns::OutboxRelay do
  let(:transport) { FakeTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "checkout@pod-7") }
  let(:store) { AceMQ::AMQP::Patterns::InMemoryOutboxStore.new }

  def recorded(payload = { "order_id" => "A-1" }, **fields)
    AceMQ::AMQP::Patterns.record(mq, payload, to: "order.placed",
                                              exchange: "orders-events", **fields)
  end

  describe "recording" do
    it "encodes the payload and renders the envelope, exactly as publishing would" do
      # A message that went through the outbox has to be indistinguishable on
      # the wire from one that did not. The outbox is a delivery mechanism, not
      # a different kind of message.
      record = recorded({ "order_id" => "A-1" }, type: "order.placed.v2", version: 2)

      expect(record.body).to eq('{"order_id":"A-1"}')
      expect(record.content_type).to eq("application/json")
      expect(record.exchange).to eq("orders-events")
      expect(record.routing_key).to eq("order.placed")
      expect(record.headers[AceMQ::AMQP::Headers::TYPE]).to eq("order.placed.v2")
      expect(record.headers[AceMQ::AMQP::Headers::VERSION]).to eq(2)
      expect(record.headers[AceMQ::AMQP::Headers::ORIGIN]).to eq("checkout@pod-7")
      expect(record.headers[AceMQ::AMQP::Headers::ID]).to eq(record.id)
    end

    it "stores bytes rather than the object they came from" do
      # The record outlives the process that wrote it, and the class it was
      # encoded from may not survive the deployment that happens while it waits.
      expect(recorded.body).to be_a(String)
    end

    it "refuses an envelope and the fields to build one at the same time" do
      envelope = AceMQ::AMQP::Envelope.new(type: "order.placed.v2")
      expect { recorded({ "id" => "A" }, envelope: envelope, version: 3) }
        .to raise_error(ArgumentError, /one or the other/)
    end
  end

  describe "the in-memory store" do
    it "keeps the order records were written in" do
      # Usually the order the writer meant. A relay that publishes them out of
      # order has invented a reordering nobody asked for.
      three = Array.new(3) { |i| recorded({ "n" => i }) }
      three.each { |record| store.add(record) }

      expect(store.pending.map(&:id)).to eq(three.map(&:id))
    end

    it "does not turn a record added twice into two messages" do
      # The caller may be retrying its own transaction.
      record = recorded
      store.add(record)
      store.add(record)

      expect(store.size).to eq(1)
    end

    it "refuses a record with no id, which nothing could de-duplicate" do
      expect { store.add(AceMQ::AMQP::Patterns::OutboxRecord.new(id: "")) }
        .to raise_error(ArgumentError, /needs an id/)
    end

    it "returns at most a batch" do
      5.times { |i| store.add(recorded({ "n" => i })) }
      expect(store.pending(2).size).to eq(2)
    end
  end

  describe "sweeping" do
    it "publishes what was recorded, and then forgets it" do
      record = recorded
      store.add(record)

      expect(described_class.new(mq, store).sweep).to eq(1)

      sent = transport.published.first
      expect(sent.exchange).to eq("orders-events")
      expect(sent.routing_key).to eq("order.placed")
      expect(sent.body).to eq('{"order_id":"A-1"}')
      expect(sent.message_id).to eq(record.id)
      expect(store.size).to eq(0)
    end

    it "leaves a record in the outbox when the broker will not take it" do
      # Nothing is lost by the relay failing, only delayed. That is the whole
      # property the pattern is for.
      store.add(recorded)
      allow(transport).to receive(:publish).and_raise(AceMQ::AMQP::PublishError, "no confirm")

      relay = described_class.new(mq, store)
      expect { relay.sweep }.to raise_error(AceMQ::AMQP::PublishError)
      expect(store.size).to eq(1)
    end

    it "marks a record published only after the broker has confirmed it" do
      # A crash in this gap republishes the record, which is the at-least-once
      # this pattern promises. Marking first would lose it instead.
      store.add(recorded)
      order = []
      allow(transport).to receive(:publish).and_wrap_original do |original, **args|
        order << :published
        original.call(**args)
      end
      allow(store).to receive(:mark_published).and_wrap_original do |original, id|
        order << :marked
        original.call(id)
      end

      described_class.new(mq, store).sweep
      expect(order).to eq(%i[published marked])
    end

    it "publishes nothing when the outbox is empty" do
      expect(described_class.new(mq, store).sweep).to eq(0)
      expect(transport.published).to be_empty
    end
  end

  describe "running" do
    it "sweeps on its own until it is closed" do
      store.add(recorded)
      relay = described_class.new(mq, store, interval: 0.01).start

      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
      sleep(0.01) while store.size.positive? &&
                        Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      relay.close

      expect(transport.published.size).to eq(1)
      expect(relay).not_to be_running
    end

    it "keeps sweeping after a failure, and says what went wrong" do
      # A relay whose sweeps are all failing is an outbox filling up, and
      # without somewhere to report that the only symptom is messages nobody
      # ever receives.
      store.add(recorded)
      seen = Queue.new
      allow(transport).to receive(:publish).and_raise(AceMQ::AMQP::PublishError, "no confirm")

      relay = described_class.new(mq, store, interval: 0.01,
                                             on_error: ->(e) { seen << e }).start
      first = seen.pop
      relay.close

      expect(first).to be_a(AceMQ::AMQP::PublishError)
      expect(store.size).to eq(1)
    end

    it "closes quickly even with a long interval" do
      # Waiting on a condition rather than sleeping, so shutting down a relay
      # that sweeps every thirty seconds does not take thirty seconds.
      relay = described_class.new(mq, store, interval: 30).start
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      relay.close

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
    end
  end
end
