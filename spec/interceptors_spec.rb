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

require "acemq/amqp"

RSpec.describe AceMQ::AMQP::Interceptors do
  let(:transport) { FakeTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }

  def delivery_for(body: '{"id":"A-1"}', attempt: 1)
    FakeDelivery.build(
      body: body,
      headers: { AceMQ::AMQP::Headers::ID => "msg-1",
                 AceMQ::AMQP::Headers::TYPE => "order.placed.v2",
                 AceMQ::AMQP::Headers::ATTEMPT => attempt,
                 AceMQ::AMQP::Headers::FIRST_SEEN => AceMQ::AMQP::Envelope.millis(Time.now) }
    )
  end

  describe "on the way out" do
    it "stamps a header onto every message without touching a call site" do
      # The thing the seam exists for: a tenant, a trace, a token — written
      # once here rather than copied into every publisher, where one of them is
      # always the one that forgot.
      mq.intercept_publish { |context| context.set_header("tenant", "acme") }
      mq.publish({ "order_id" => "A-1" }, to: "orders.new")

      expect(transport.published.last.headers["tenant"]).to eq("acme")
    end

    it "can redirect a message as well as decorate it" do
      # An interceptor that could stamp but not redirect would be a seam with a
      # hole in it, and whoever needed to route by tenant would find the hole.
      mq.intercept_publish do |context|
        context.exchange = "orders-events"
        context.routing_key = "order.placed"
        context.payload = context.payload.merge("stamped" => true)
      end
      mq.publish({ "order_id" => "A-1" }, to: "orders.new")

      sent = transport.published.last
      expect(sent.exchange).to eq("orders-events")
      expect(sent.routing_key).to eq("order.placed")
      expect(sent.body).to eq('{"order_id":"A-1","stamped":true}')
    end

    it "returns the envelope the interceptors left, not the one handed in" do
      mq.intercept_publish { |context| context.set_header("tenant", "acme") }
      envelope = mq.publish({ "order_id" => "A-1" }, to: "orders.new")

      expect(envelope.headers).to eq({ "tenant" => "acme" })
    end

    it "stops the publish when an interceptor refuses, and says so" do
      # Refusing is the whole reason for intercepting rather than observing. A
      # message that must not go out is stopped once, here.
      mq.intercept_publish { |_context| raise ArgumentError, "no tenant on this thread" }

      expect { mq.publish({ "order_id" => "A-1" }, to: "orders.new") }
        .to raise_error(ArgumentError, /no tenant/)
      expect(transport.published).to be_empty
    end

    it "refuses to let an interceptor write a reserved header" do
      # An interceptor quietly overwriting the attempt count would break the
      # retry engine from outside it, with nothing in the code to show why.
      mq.intercept_publish { |context| context.set_header(AceMQ::AMQP::Headers::ATTEMPT, 99) }

      expect { mq.publish({ "order_id" => "A-1" }, to: "orders.new") }
        .to raise_error(ArgumentError, /belong to AceMQ and cannot be set by hand/)
    end

    it "tells an interceptor when the broker has the message, and when it did not" do
      confirmed = []
      failed = []
      watcher = Class.new do
        def initialize(confirmed, failed)
          @confirmed = confirmed
          @failed = failed
        end

        def before_publish(_context) = nil
        def after_confirm(context) = @confirmed << context.envelope.id
        def on_error(context, failure) = @failed << [context.routing_key, failure.class]
      end
      mq.intercept_publish(watcher.new(confirmed, failed))

      sent = mq.publish({ "order_id" => "A-1" }, to: "orders.new")
      expect(confirmed).to eq([sent.id])
      expect(failed).to be_empty

      allow(transport).to receive(:publish).and_raise(AceMQ::AMQP::PublishError, "no confirm")
      expect { mq.publish({ "order_id" => "A-2" }, to: "orders.new") }
        .to raise_error(AceMQ::AMQP::PublishError)
      expect(failed).to eq([["orders.new", AceMQ::AMQP::PublishError]])
    end
  end

  describe "on the way in" do
    it "sees the decoded payload and can change what the handler is given" do
      seen = nil
      mq.intercept_consume { |context| context.set_header("tenant", "acme") }
      consumer = AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new", codec: AceMQ::AMQP::JSONCodec.new,
        retry_policy: AceMQ::AMQP::RetryPolicy.none, interceptors: mq.interceptors,
        handler: lambda { |message|
          seen = message
          AceMQ::AMQP::Ack.accept
        }
      )
      delivery, = delivery_for
      consumer.handle(delivery)

      expect(seen.payload).to eq({ "id" => "A-1" })
      expect(seen.envelope.headers).to eq({ "tenant" => "acme" })
    end

    it "carries what it stamped all the way onto the dead letter" do
      # A header that the handler saw and the dead-letter queue did not would be
      # a header that is missing exactly when somebody goes looking for it.
      mq.intercept_consume { |context| context.set_header("tenant", "acme") }
      consumer = AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new", codec: AceMQ::AMQP::JSONCodec.new,
        retry_policy: AceMQ::AMQP::RetryPolicy.none, interceptors: mq.interceptors,
        handler: ->(_message) { AceMQ::AMQP::Ack.retry("the warehouse is down") }
      )
      delivery, = delivery_for
      consumer.handle(delivery)

      dead = transport.published_to("orders.new.dlq").first
      expect(dead.headers["tenant"]).to eq("acme")
    end

    it "treats a refusal on the way in exactly as a failed handler" do
      # And deliberately: the message is retried and then dead-lettered rather
      # than acknowledged as though something had processed it.
      called = false
      mq.intercept_consume { |_context| raise "not our tenant" }
      consumer = AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new", codec: AceMQ::AMQP::JSONCodec.new,
        retry_policy: AceMQ::AMQP::RetryPolicy.none, interceptors: mq.interceptors,
        handler: lambda { |_message|
          called = true
          AceMQ::AMQP::Ack.accept
        }
      )
      delivery, recorder = delivery_for
      consumer.handle(delivery)

      expect(called).to be(false)
      expect(transport.published_to("orders.new.dlq").size).to eq(1)
      expect(recorder.acked?).to be(true)
    end

    it "runs the after hook whether the handler returned or raised" do
      acks = []
      failures = []
      watcher = Class.new do
        def initialize(acks, failures)
          @acks = acks
          @failures = failures
        end

        def before_handle(_context) = nil
        def after_handle(_context, ack) = @acks << ack.class
        def on_error(_context, failure) = @failures << failure.message
      end
      mq.intercept_consume(watcher.new(acks, failures))
      consumer = AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new", codec: AceMQ::AMQP::JSONCodec.new,
        retry_policy: AceMQ::AMQP::RetryPolicy.none, interceptors: mq.interceptors,
        handler: ->(_message) { raise "the warehouse is down" }
      )
      delivery, = delivery_for
      consumer.handle(delivery)

      expect(acks).to eq([AceMQ::AMQP::Ack])
      expect(failures).to eq(["the warehouse is down"])
    end
  end

  describe "order" do
    it "runs lower first, and equal orders in the order they were registered" do
      ran = []
      mq.intercept_publish(order: 10) { |_c| ran << :late }
      mq.intercept_publish(order: -10) { |_c| ran << :early }
      mq.intercept_publish { |_c| ran << :first_of_the_middle }
      mq.intercept_publish { |_c| ran << :second_of_the_middle }
      mq.publish({ "order_id" => "A-1" }, to: "orders.new")

      expect(ran).to eq(%i[early first_of_the_middle second_of_the_middle late])
    end

    it "reads the order off an interceptor that has one of its own" do
      ran = []
      first = Class.new do
        def initialize(ran) = @ran = ran
        def order = -100
        def before_publish(_context) = @ran << :first
      end
      mq.intercept_publish { |_c| ran << :second }
      mq.intercept_publish(first.new(ran))
      mq.publish({ "order_id" => "A-1" }, to: "orders.new")

      expect(ran).to eq(%i[first second])
    end

    it "unwinds a handler in reverse, so a pair nests" do
      # The first to open a scope has to be the last to close it. Running both
      # halves in the same order would close the outer scope while the inner one
      # was still inside it.
      ran = []
      nesting = Class.new do
        def initialize(ran, name)
          @ran = ran
          @name = name
        end

        def before_handle(_context) = @ran << "open #{@name}"
        def after_handle(_context, _ack) = @ran << "close #{@name}"
      end
      mq.intercept_consume(nesting.new(ran, "outer"), order: 1)
      mq.intercept_consume(nesting.new(ran, "inner"), order: 2)
      consumer = AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new", codec: AceMQ::AMQP::JSONCodec.new,
        retry_policy: AceMQ::AMQP::RetryPolicy.none, interceptors: mq.interceptors,
        handler: ->(_message) { AceMQ::AMQP::Ack.accept }
      )
      delivery, = delivery_for
      consumer.handle(delivery)

      expect(ran).to eq(["open outer", "open inner", "close inner", "close outer"])
    end
  end

  describe "an interceptor that fails on the way out" do
    it "is reported and stepped over, because the message has already gone" do
      # Letting it out would report a successful publish as a failed one, and a
      # caller would then send the message twice.
      broken = Class.new do
        def before_publish(_context) = nil
        def after_confirm(_context) = raise("the metrics endpoint is down")
      end
      mq.intercept_publish(broken.new)

      expect { mq.publish({ "order_id" => "A-1" }, to: "orders.new") }
        .to output(/an interceptor raised RuntimeError after a publish/).to_stderr
      expect(transport.published.size).to eq(1)
    end
  end

  describe "what it takes to write one" do
    it "needs nothing private, which is the only way to know the seam is wide enough" do
      # This one is written the way somebody outside the library would have to
      # write it: a public registration method, a context whose every field is
      # public, and no reaching into a consumer or a transport.
      counts = Hash.new(0)
      durations = []
      timing = Class.new do
        def initialize(counts, durations)
          @counts = counts
          @durations = durations
          @started = {}
        end

        def before_publish(context) = @counts[[:published, context.routing_key]] += 1

        def before_handle(context)
          @started[context.envelope.id] = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end

        def after_handle(context, ack)
          started = @started.delete(context.envelope.id)
          @durations << (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started)
          @counts[[:handled, context.queue, ack.accept? ? :accepted : :failed]] += 1
        end
      end
      watcher = timing.new(counts, durations)
      mq.intercept_publish(watcher)
      mq.intercept_consume(watcher)

      mq.publish({ "order_id" => "A-1" }, to: "orders.new")
      consumer = AceMQ::AMQP::Consumer.new(
        transport: transport, queue: "orders.new", codec: AceMQ::AMQP::JSONCodec.new,
        retry_policy: AceMQ::AMQP::RetryPolicy.none, interceptors: mq.interceptors,
        handler: ->(_message) { AceMQ::AMQP::Ack.accept }
      )
      delivery, = delivery_for
      consumer.handle(delivery)

      expect(counts[[:published, "orders.new"]]).to eq(1)
      expect(counts[[:handled, "orders.new", :accepted]]).to eq(1)
      expect(durations.size).to eq(1)
      expect(durations.first).to be >= 0
    end
  end

  describe "registering" do
    it "refuses an interceptor that is neither an object nor a block" do
      expect { mq.intercept_publish }.to raise_error(ArgumentError, /object or a block/)
      expect { mq.intercept_consume }.to raise_error(ArgumentError, /object or a block/)
    end

    it "refuses both at once, because only one of them could be meant" do
      expect { mq.intercept_publish(Object.new) { |_c| nil } }
        .to raise_error(ArgumentError, /not both/)
    end

    it "counts what is registered on each side" do
      mq.intercept_publish { |_c| nil }
      mq.intercept_consume { |_c| nil }
      mq.intercept_consume { |_c| nil }

      expect(mq.interceptors.publishing).to eq(1)
      expect(mq.interceptors.consuming).to eq(2)
      expect(mq.interceptors).not_to be_empty
    end

    it "starts empty, and a connection with none behaves exactly as before" do
      expect(described_class.new).to be_empty
      expect(mq.interceptors).to be_empty

      mq.publish({ "order_id" => "A-1" }, to: "orders.new")
      expect(transport.published.size).to eq(1)
    end
  end
end
