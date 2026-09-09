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

Ack = AceMQ::AMQP::Ack
Envelope = AceMQ::AMQP::Envelope
FatalError = AceMQ::AMQP::FatalError
Headers = AceMQ::AMQP::Headers

RSpec.describe AceMQ::AMQP::Consumer do
  let(:transport) { FakeTransport.new }

  # Nothing here opens a socket. What is being tested is which attempt this is,
  # how long to wait, when to give up and what reason gets written onto the
  # dead letter, and none of that is the broker's arithmetic.
  def consumer(policy: AceMQ::AMQP::RetryPolicy.none, codec: AceMQ::AMQP::JSONCodec.new,
               threshold: AceMQ::AMQP::RetryLadder::DEFAULT_THRESHOLD,
               interceptors: AceMQ::AMQP::Interceptors.new, telemetry: nil, &handler)
    described_class.new(transport: transport, queue: "orders.new", handler: handler,
                        codec: codec, retry_policy: policy, retry_threshold: threshold,
                        interceptors: interceptors, telemetry: telemetry)
  end

  def headers_for(id: "msg-1", attempt: 1, **extra)
    { Headers::ID => id, Headers::TYPE => "order.placed.v2", Headers::VERSION => 1,
      Headers::CORRELATION => "corr-9", Headers::ATTEMPT => attempt,
      Headers::FIRST_SEEN => Envelope.millis(Time.now) }.merge(extra)
  end

  describe "accepting" do
    it "acknowledges, and nothing goes anywhere else" do
      delivery, recorder = FakeDelivery.build(body: '{"id":"A-1"}', headers: headers_for)
      consumer { Ack.accept }.handle(delivery)

      expect(recorder.acked?).to be(true)
      expect(transport.published).to be_empty
    end

    it "hands the handler the payload, the envelope and the bytes" do
      delivery, = FakeDelivery.build(body: '{"id":"A-1"}', headers: headers_for)
      seen = nil
      consumer do |message|
        seen = message
        Ack.accept
      end.handle(delivery)

      expect(seen.payload).to eq({ "id" => "A-1" })
      expect(seen.envelope.correlation_id).to eq("corr-9")
      expect(seen.envelope.type).to eq("order.placed.v2")
      expect(seen.body).to eq('{"id":"A-1"}')
      expect(seen.routing_key).to eq("orders.new")
    end
  end

  describe "counting attempts" do
    it "reads the attempt off the wire, where the contract puts it" do
      seen = []
      subject = consumer(policy: AceMQ::AMQP::RetryPolicy.fixed(5, 0)) do |message|
        seen << message.attempt
        Ack.retry("not yet")
      end

      [1, 2, 3].each do |attempt|
        delivery, = FakeDelivery.build(headers: headers_for(attempt: attempt))
        subject.handle(delivery)
      end

      expect(seen).to eq([1, 2, 3])
    end

    it "advances the attempt on the message it republishes" do
      # The count has to travel with the message. Kept in this process instead,
      # it is wrong the moment a second consumer exists: a message that moves
      # between them is for ever on attempt one, and five attempts becomes
      # unbounded.
      delivery, = FakeDelivery.build(headers: headers_for(attempt: 2))
      consumer(policy: AceMQ::AMQP::RetryPolicy.fixed(5, 0)) { Ack.retry("no") }
        .handle(delivery)

      again = transport.published_to("orders.new")
      expect(again.size).to eq(1)
      expect(again.first.headers[Headers::ATTEMPT]).to eq(3)
    end

    it "keeps the identity of the message it retries" do
      delivery, = FakeDelivery.build(headers: headers_for(attempt: 1))
      consumer(policy: AceMQ::AMQP::RetryPolicy.fixed(5, 0)) { Ack.retry("no") }
        .handle(delivery)

      # Same id, same correlation: a retry is the same message again, not a new
      # one, and anything keyed on the id has to agree.
      republished = transport.published_to("orders.new").first
      expect(republished.headers[Headers::ID]).to eq(headers_for[Headers::ID])
      expect(republished.headers[Headers::CORRELATION])
        .to eq(headers_for[Headers::CORRELATION])
    end
  end

  describe "retrying" do
    it "puts the message back on its own queue while attempts remain" do
      policy = AceMQ::AMQP::RetryPolicy.fixed(3, 0)
      delivery, recorder = FakeDelivery.build(headers: headers_for)
      consumer(policy: policy) { Ack.retry("the warehouse is down") }.handle(delivery)

      # Republished and then acknowledged, rather than requeued: the attempt
      # has to advance, and only a new publish can carry it.
      expect(transport.published_to("orders.new").size).to eq(1)
      expect(recorder.acked?).to be(true)
      expect(recorder.requeued?).to be(false)
    end

    it "waits the policy's delay before returning it" do
      policy = AceMQ::AMQP::RetryPolicy.fixed(3, 0.05)
      delivery, = FakeDelivery.build(headers: headers_for)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      consumer(policy: policy) { Ack.retry("not yet") }.handle(delivery)

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be >= 0.05
    end

    it "dead-letters with the reason once the attempts are used up" do
      policy = AceMQ::AMQP::RetryPolicy.fixed(2, 0)
      subject = consumer(policy: policy) { Ack.retry("the warehouse is down") }

      first, first_recorder = FakeDelivery.build(headers: headers_for(attempt: 1))
      subject.handle(first)
      expect(first_recorder.acked?).to be(true)
      expect(transport.published_to("orders.new").size).to eq(1)

      second, second_recorder = FakeDelivery.build(headers: headers_for(attempt: 2))
      subject.handle(second)

      dead = transport.published_to("orders.new.dlq")
      expect(dead.size).to eq(1)
      expect(dead.first.headers[Headers::ERROR])
        .to eq("gave up after 2 attempts: the warehouse is down")
      expect(dead.first.headers[Headers::ATTEMPT]).to eq(2)
      # Acknowledged, not rejected: the message has already been safely
      # republished, so the original is a copy that has been dealt with.
      expect(second_recorder.acked?).to be(true)
      expect(second_recorder.requeued?).to be(false)
    end

    it "gives up on age as well as on attempts, and says which" do
      # The honest limit when a queue has been paused: a message can be on
      # attempt one and four days old.
      policy = AceMQ::AMQP::RetryPolicy.fixed(10, 0).give_up_after(60)
      old = Envelope.millis(Time.now - 3600)
      delivery, = FakeDelivery.build(headers: headers_for(Headers::FIRST_SEEN => old))
      consumer(policy: policy) { Ack.retry("still failing") }.handle(delivery)

      expect(transport.published_to("orders.new.dlq").first.headers[Headers::ERROR])
        .to eq("gave up on a message older than 60.0 seconds: still failing")
    end

    it "skips the remaining attempts when the reason is marked fatal" do
      # The handler asked for a retry but marked the reason as one that will
      # not change. Honouring the mark rather than the request is the point of
      # having it.
      policy = AceMQ::AMQP::RetryPolicy.fixed(10, 0)
      delivery, recorder = FakeDelivery.build(headers: headers_for)
      consumer(policy: policy) { Ack.retry(FatalError.new("this order has no customer")) }
        .handle(delivery)

      expect(recorder.requeued?).to be(false)
      expect(transport.published_to("orders.new.dlq").first.headers[Headers::ERROR])
        .to eq("retrying cannot help: AceMQ::AMQP::FatalError: this order has no customer")
    end

    it "dead-letters immediately when no policy allows a second attempt" do
      # RetryPolicy.none is one delivery. A retry against it is a dead letter,
      # which is a great deal easier to explain than a message going round the
      # broker as fast as it can be handed back.
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer { Ack.retry("try again") }.handle(delivery)

      expect(transport.published_to("orders.new.dlq").size).to eq(1)
    end
  end

  describe "a delay long enough to be worth the broker's while" do
    # Half a second, so the examples stay fast; thirty is the default and the
    # arithmetic is the same either side of it.
    let(:policy) { AceMQ::AMQP::RetryPolicy.fixed(3, 1) }

    it "puts the message in the rung queue instead of sleeping on it" do
      delivery, recorder = FakeDelivery.build(headers: headers_for)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      consumer(policy: policy, threshold: 0.5) { Ack.retry("the warehouse is down") }
        .handle(delivery)

      # The wait is the queue's time-to-live now, so this call does not wait at
      # all. That is the whole point: an unacknowledged message held for a long
      # backoff is a message the broker redelivers the moment this process
      # restarts, which turns the backoff into nothing.
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 0.5
      expect(transport.published_to("orders.new")).to be_empty
      expect(transport.published_to("orders.new.retry.1s").size).to eq(1)
      expect(recorder.acked?).to be(true)
      expect(recorder.requeued?).to be(false)
    end

    it "advances the attempt on the way into the rung, as an immediate retry does" do
      # A rung is where the message waits, not a different kind of retry: the
      # count on the wire has to move whichever way the wait happened, or the
      # policy never runs out.
      delivery, = FakeDelivery.build(headers: headers_for(attempt: 2))
      consumer(policy: policy, threshold: 0.5) { Ack.retry("no") }.handle(delivery)

      waiting = transport.published_to("orders.new.retry.1s").first
      expect(waiting.headers[Headers::ATTEMPT]).to eq(3)
      expect(waiting.headers[Headers::ID]).to eq("msg-1")
    end

    it "sets no expiration on the message itself" do
      # Per-message TTL expires only from the head of a queue, so one long wait
      # at the front holds back every shorter one behind it. The delay lives on
      # the rung queue and nowhere else.
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer(policy: policy, threshold: 0.5) { Ack.retry("no") }.handle(delivery)

      headers = transport.published_to("orders.new.retry.1s").first.headers
      expect(headers.keys).not_to include("expiration", "x-message-ttl")
    end

    it "still dead-letters rather than laddering when the attempts run out" do
      delivery, = FakeDelivery.build(headers: headers_for(attempt: 3))
      consumer(policy: policy, threshold: 0.5) { Ack.retry("the warehouse is down") }
        .handle(delivery)

      expect(transport.published_to("orders.new.retry.1s")).to be_empty
      expect(transport.published_to("orders.new.dlq").first.headers[Headers::ERROR])
        .to eq("gave up after 3 attempts: the warehouse is down")
    end

    it "picks the rung from the schedule, not from the jittered delay" do
      # Jitter is random by definition and a rung queue is named after a fixed
      # delay, so a jittered number could name a queue nothing declared. Above
      # the threshold the spread comes free anyway: each message's time-to-live
      # starts when it enters the rung.
      jittery = AceMQ::AMQP::RetryPolicy.fixed(5, 60).with_jitter(0.5)
      subject = consumer(policy: jittery, threshold: 30) { Ack.retry("no") }

      20.times do
        delivery, = FakeDelivery.build(headers: headers_for)
        subject.handle(delivery)
      end

      expect(transport.published_to("orders.new.retry.1m").size).to eq(20)
      expect(transport.published.map(&:routing_key).uniq).to eq(["orders.new.retry.1m"])
    end
  end

  describe "rejecting" do
    it "dead-letters without trying again" do
      policy = AceMQ::AMQP::RetryPolicy.fixed(10, 0)
      delivery, recorder = FakeDelivery.build(headers: headers_for)
      consumer(policy: policy) { Ack.reject("no customer on this order") }.handle(delivery)

      expect(recorder.requeued?).to be(false)
      expect(transport.published_to("orders.new.dlq").first.headers[Headers::ERROR])
        .to eq("rejected by the handler: no customer on this order")
    end
  end

  # An ack says what the handler asked for. It cannot say whether there is an
  # attempt left to spend on it, or how long the next wait will be, so anything
  # reporting on this consumer from the outside has to be told rather than left
  # to infer it.
  describe "what the interceptors are told about the settlement" do
    let(:seen) { [] }

    let(:interceptors) do
      recorded = seen
      watcher = Object.new
      watcher.define_singleton_method(:after_handle) do |context, _ack|
        recorded << context.settlement
      end
      AceMQ::AMQP::Interceptors.new.add_consume(watcher)
    end

    it "hands after_handle the delay the retry policy chose" do
      policy = AceMQ::AMQP::RetryPolicy.fixed(5, 0.05)
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer(policy: policy, interceptors: interceptors) { Ack.retry("not yet") }
        .handle(delivery)

      expect(seen.last.outcome).to eq("retried")
      expect(seen.last.delay).to eq(0.05)
    end

    it "says dead_lettered, not retried, once the attempts are used up" do
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer(interceptors: interceptors) { Ack.retry("the warehouse said no") }
        .handle(delivery)

      expect(seen.last.outcome).to eq("dead_lettered")
      expect(seen.last.reason).to eq("gave up after 1 attempts: the warehouse said no")
      expect(seen.last.delay).to be_nil
    end

    it "keeps a rejection apart from a give-up, though both are dead-lettered" do
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer(interceptors: interceptors) { Ack.reject("not ours") }.handle(delivery)

      expect(seen.last.outcome).to eq("rejected")
      expect(seen.last).to be_dead_letters
      expect(seen.last.reason).to eq("rejected by the handler: not ours")
    end

    it "keeps parking apart from both, because it goes to a queue of its own" do
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer(interceptors: interceptors) { Ack.park("unreadable") }.handle(delivery)

      expect(seen.last.outcome).to eq(AceMQ::AMQP::Settlement::PARKED)
      expect(seen.last).to be_parked
      expect(seen.last).not_to be_dead_letters
      expect(seen.last.reason).to eq("parked by the handler: unreadable")
    end

    it "says acked when the handler was happy" do
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer(interceptors: interceptors) { Ack.accept }.handle(delivery)

      expect(seen.last).to be_acked
      expect(seen.last.reason).to be_nil
    end
  end

  describe "a handler that raises" do
    it "treats an ordinary failure as worth another go" do
      policy = AceMQ::AMQP::RetryPolicy.fixed(3, 0)
      delivery, recorder = FakeDelivery.build(headers: headers_for)
      consumer(policy: policy) { raise "the database went away" }.handle(delivery)

      expect(transport.published_to("orders.new").size).to eq(1)
      expect(recorder.acked?).to be(true)
    end

    it "treats a FatalError as a rejection, however many attempts remain" do
      policy = AceMQ::AMQP::RetryPolicy.fixed(10, 0)
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer(policy: policy) { raise FatalError, "this can never work" }.handle(delivery)

      expect(transport.published_to("orders.new.dlq").first.headers[Headers::ERROR])
        .to match(/rejected by the handler: .*this can never work/)
    end

    it "does not let a handler that decides nothing loop forever" do
      # A handler that returns something other than an Ack is a bug, and a bug
      # repeats. Round the queue forever is the one outcome worth ruling out.
      policy = AceMQ::AMQP::RetryPolicy.fixed(10, 0)
      delivery, recorder = FakeDelivery.build(headers: headers_for)
      consumer(policy: policy) { :done }.handle(delivery)

      expect(recorder.requeued?).to be(false)
      expect(transport.published_to("orders.new.dlq").first.headers[Headers::ERROR])
        .to match(/returned a Symbol rather than an Ack/)
    end
  end

  describe "a body nothing can read" do
    it "parks it rather than retrying it" do
      # It will not decode any better next time, and the parking lot keeps it
      # apart from the messages that failed for a reason worth reading.
      policy = AceMQ::AMQP::RetryPolicy.fixed(10, 0)
      delivery, recorder = FakeDelivery.build(body: "{ not json", headers: headers_for)
      consumer(policy: policy) { Ack.accept }.handle(delivery)

      expect(recorder.requeued?).to be(false)
      expect(recorder.acked?).to be(true)
      expect(transport.published_to("orders.new.dlq")).to be_empty

      parked = transport.published_to("orders.new.parked")
      expect(parked.size).to eq(1)
      expect(parked.first.headers[Headers::ERROR]).to match(/could not be decoded: /)
      expect(parked.first.body).to eq("{ not json")
    end

    it "counts it as parked, once" do
      metrics = AceMQ::AMQP::Telemetry::Registry.new
      delivery, = FakeDelivery.build(body: "{ not json", headers: headers_for)
      consumer(telemetry: metrics) { Ack.accept }.handle(delivery)

      expect(metrics[AceMQ::AMQP::Telemetry::CONSUME_TOTAL, queue: "orders.new",
                                                            outcome: "parked"]).to eq(1)
    end
  end

  # A handler that already knows a message is unreadable used to have to reject
  # it into the dead letters, which lost the distinction the parking queue
  # exists to make.
  describe "parking, asked for by the handler" do
    it "sends it to the parking queue and not to the dead letters" do
      delivery, recorder = FakeDelivery.build(body: '{"id":"A-1"}', headers: headers_for)
      consumer { Ack.park("the schema version is one nothing here was taught") }
        .handle(delivery)

      expect(recorder.acked?).to be(true)
      expect(recorder.requeued?).to be(false)
      expect(transport.published_to("orders.new.dlq")).to be_empty

      parked = transport.published_to("orders.new.parked")
      expect(parked.size).to eq(1)
      expect(parked.first.headers[Headers::ERROR])
        .to match(/parked by the handler: the schema version is one nothing here was taught/)
      expect(parked.first.body).to eq('{"id":"A-1"}')
    end

    it "does not try again, however many attempts are left" do
      policy = AceMQ::AMQP::RetryPolicy.fixed(10, 0)
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer(policy: policy) { Ack.park("unreadable") }.handle(delivery)

      expect(transport.published_to("orders.new")).to be_empty
      expect(transport.published_to("orders.new.parked").size).to eq(1)
    end

    it "counts it as parked and as nothing else" do
      metrics = AceMQ::AMQP::Telemetry::Registry.new
      delivery, = FakeDelivery.build(headers: headers_for)
      consumer(telemetry: metrics) { Ack.park("unreadable") }.handle(delivery)

      queue = { queue: "orders.new" }
      total = AceMQ::AMQP::Telemetry::CONSUME_TOTAL
      expect(metrics[total, **queue, outcome: "parked"]).to eq(1)
      expect(metrics[total, **queue, outcome: "rejected"]).to eq(0)
      expect(metrics[total, **queue, outcome: "dead_lettered"]).to eq(0)
      expect(metrics[AceMQ::AMQP::Telemetry::DEAD_LETTERED_TOTAL, **queue]).to eq(0)
    end
  end

  # Republishing to the dead-letter or parking queue can itself fail — a queue
  # that was never declared is the usual reason. Nothing is lost: the delivery
  # is never settled and the broker redelivers it. What that looks like from
  # outside is a handler failing over and over on the same message, and this
  # counter is the only thing that tells the two apart.
  describe "a message that cannot be set aside" do
    it "counts acemq.messages.set.aside.failed and leaves the delivery unsettled" do
      metrics = AceMQ::AMQP::Telemetry::Registry.new
      transport.refuse!("orders.new.dlq")
      delivery, recorder = FakeDelivery.build(headers: headers_for)

      expect { consumer(telemetry: metrics) { Ack.reject("nope") }.handle(delivery) }
        .to raise_error(AceMQ::AMQP::PublishError)

      expect(metrics[AceMQ::AMQP::Telemetry::SET_ASIDE_FAILED,
                     queue: "orders.new", target: "orders.new.dlq"]).to eq(1)
      expect(recorder.acked?).to be(false)
      expect(recorder.requeued?).to be(false)
    end

    it "counts it for the parking queue too, and names which one it was" do
      metrics = AceMQ::AMQP::Telemetry::Registry.new
      transport.refuse!("orders.new.parked")
      delivery, = FakeDelivery.build(body: "{ not json", headers: headers_for)

      expect { consumer(telemetry: metrics) { Ack.accept }.handle(delivery) }
        .to raise_error(AceMQ::AMQP::PublishError)

      expect(metrics[AceMQ::AMQP::Telemetry::SET_ASIDE_FAILED,
                     queue: "orders.new", target: "orders.new.parked"]).to eq(1)
    end
  end

  describe "the message that goes to the dead-letter queue" do
    it "keeps the envelope that arrived, plus the reason" do
      # Whoever drains a dead-letter queue is trying to work out where the
      # message came from, so every field it arrived with has to survive.
      policy = AceMQ::AMQP::RetryPolicy.none
      delivery, = FakeDelivery.build(body: '{"id":"A-1"}',
                                     headers: headers_for.merge("tenant" => "acme"))
      consumer(policy: policy) { Ack.reject("nope") }.handle(delivery)

      dead = transport.published_to("orders.new.dlq").first
      expect(dead.headers[Headers::ID]).to eq("msg-1")
      expect(dead.headers[Headers::CORRELATION]).to eq("corr-9")
      expect(dead.headers[Headers::TYPE]).to eq("order.placed.v2")
      expect(dead.headers["tenant"]).to eq("acme")
      expect(dead.body).to eq('{"id":"A-1"}')
      expect(dead.content_type).to eq("application/json")
    end
  end
end
