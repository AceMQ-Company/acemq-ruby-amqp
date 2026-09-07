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

Patterns = AceMQ::AMQP::Patterns

RSpec.describe AceMQ::AMQP::Patterns do
  def message(id: "msg-1", payload: { "order_id" => "A-1" })
    AceMQ::AMQP::Message.new(
      payload: payload, envelope: AceMQ::AMQP::Envelope.new(id: id),
      routing_key: "orders.new", content_type: "application/json",
      redelivered: false, body: JSON.dump(payload)
    )
  end

  describe "the in-memory store" do
    it "says first once and never again" do
      store = Patterns::InMemoryIdempotencyStore.new

      expect(store.first_time?("k")).to be(true)
      expect(store.first_time?("k")).to be(false)
      expect(store.first_time?("other")).to be(true)
    end

    it "forgets a key on request, so a failed message can be redone" do
      store = Patterns::InMemoryIdempotencyStore.new
      store.first_time?("k")
      store.forget("k")

      expect(store.first_time?("k")).to be(true)
    end

    it "forgets a key once the window has passed" do
      # Without a window the map grows for as long as the process lives, which
      # is a memory leak with a message id on it.
      store = Patterns::InMemoryIdempotencyStore.new(window: 0.05)
      store.first_time?("k")
      sleep(0.08)

      expect(store.first_time?("k")).to be(true)
      expect(store.size).to eq(1)
    end
  end

  describe "idempotent" do
    let(:store) { Patterns::InMemoryIdempotencyStore.new }

    it "runs the handler once and accepts the repeat without running it" do
      # Accepted rather than rejected: the work was done, so the message has
      # been handled, and dead-lettering it would raise an alarm about
      # something that went right.
      runs = 0
      handler = Patterns.idempotent(store) do
        runs += 1
        AceMQ::AMQP::Ack.accept
      end

      expect(handler.call(message)).to be_accept
      expect(handler.call(message)).to be_accept
      expect(runs).to eq(1)
    end

    it "keys on the message id, so a retry of the same message is one message" do
      runs = 0
      handler = Patterns.idempotent(store) do
        runs += 1
        AceMQ::AMQP::Ack.accept
      end

      handler.call(message(id: "msg-1"))
      handler.call(message(id: "msg-2"))

      expect(runs).to eq(2)
    end

    it "takes a key from the payload when that is the one that matters" do
      # Two different messages carrying the same order, where doing the order
      # twice is the thing to prevent.
      runs = 0
      handler = Patterns.idempotent(store, key: ->(m) { m.payload["order_id"] }) do
        runs += 1
        AceMQ::AMQP::Ack.accept
      end

      handler.call(message(id: "msg-1"))
      handler.call(message(id: "msg-2"))

      expect(runs).to eq(1)
    end

    it "forgets the key when the handler does not accept, so the retry can run" do
      # Remembering a message that then failed would mean its retry silently
      # does nothing, which is the worst of both.
      runs = 0
      handler = Patterns.idempotent(store) do
        runs += 1
        runs == 1 ? AceMQ::AMQP::Ack.retry("the warehouse is down") : AceMQ::AMQP::Ack.accept
      end

      expect(handler.call(message)).to be_retry
      expect(handler.call(message)).to be_accept
      expect(runs).to eq(2)
    end

    it "forgets the key when the handler raises, and lets the exception through" do
      # An exception is the ordinary failure channel in Ruby and the retry
      # engine above this turns it into a retry. Swallowing it here would turn
      # a failure into an acknowledgement.
      handler = Patterns.idempotent(store) { raise "the database went away" }

      expect { handler.call(message) }.to raise_error(/the database went away/)
      expect(store.first_time?("msg-1")).to be(true)
    end

    it "retries rather than risking a duplicate when the store is broken" do
      broken = Class.new do
        def first_time?(_key) = raise("the idempotency table is gone")
        def forget(_key) = nil
      end.new

      ack = Patterns.idempotent(broken) { AceMQ::AMQP::Ack.accept }.call(message)

      expect(ack).to be_retry
      expect(ack.error.message).to eq("the idempotency table is gone")
    end

    it "rejects a message it cannot key rather than guarding nothing" do
      # The key function will produce the same nothing next time, so a retry is
      # a promise this cannot keep.
      handler = Patterns.idempotent(store, key: ->(_m) { "" }) { AceMQ::AMQP::Ack.accept }
      ack = handler.call(message)

      expect(ack).to be_reject
      expect(ack.error).to be_a(AceMQ::AMQP::FatalError)
    end

    it "is still an ordinary handler, so the consumer around it is unchanged" do
      # A pattern that took over the consumer would have to reimplement the
      # retry policy and the dead-lettering, and then there would be two of
      # each to keep in step.
      transport = FakeTransport.new
      consumer = AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new",
        handler: Patterns.idempotent(store) { AceMQ::AMQP::Ack.retry("no") },
        codec: AceMQ::AMQP::JSONCodec.new, retry_policy: AceMQ::AMQP::RetryPolicy.fixed(2, 0)
      )

      first, = FakeDelivery.build(body: '{"order_id":"A-1"}',
                                  headers: { "x-acemq-id" => "msg-1", "x-acemq-attempt" => 1 })
      consumer.handle(first)
      expect(transport.published_to("orders.new").size).to eq(1)

      second, = FakeDelivery.build(body: '{"order_id":"A-1"}',
                                   headers: { "x-acemq-id" => "msg-1", "x-acemq-attempt" => 2 })
      consumer.handle(second)
      expect(transport.published_to("orders.new.dlq").size).to eq(1)
    end
  end
end
