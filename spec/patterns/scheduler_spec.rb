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

require "acemq/amqp/patterns"

RSpec.describe "the scheduler" do
  Scheduler = AceMQ::AMQP::Patterns::Scheduler

  let(:transport) { LoopbackTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }
  let(:scheduler) { Scheduler.on(mq) }

  after { scheduler.close }

  # What went out, ignoring the message that put it there.
  def last_published = transport.published.last

  def publish_into_control(headers, body: "{}")
    mq.publish(body, to: Scheduler::CONTROL, exchange: Scheduler::EXCHANGE,
                     codec: AceMQ::AMQP::BytesCodec.new, headers: headers)
  end

  describe "the topology" do
    it "is one direct exchange, five rungs and a control queue" do
      scheduler

      expect(transport.declared_exchanges.map(&:first)).to eq(["acemq.schedule"])
      expect(transport.declared_exchanges.first.last).to include(kind: :direct, durable: true)
      expect(transport.declared_queues.map(&:first))
        .to eq(["acemq.schedule.1h", "acemq.schedule.10m", "acemq.schedule.1m",
                "acemq.schedule.10s", "acemq.schedule.1s", "acemq.schedule.due"])
    end

    it "declares the rungs longest first, so a long delay is the cheap case" do
      expect(Scheduler::RUNGS).to eq([3600, 600, 60, 10, 1])
    end

    it "names a rung by the shortest rendering of its duration" do
      expect(Scheduler::RUNGS.map { |rung| Scheduler.rung_name(rung) })
        .to eq(["acemq.schedule.1h", "acemq.schedule.10m", "acemq.schedule.1m",
                "acemq.schedule.10s", "acemq.schedule.1s"])
      # The rule itself, not just the five it is applied to: hours divide out
      # first, then minutes, and anything else is seconds.
      expect(Scheduler.rung_name(7200)).to eq("acemq.schedule.2h")
      expect(Scheduler.rung_name(300)).to eq("acemq.schedule.5m")
      expect(Scheduler.rung_name(45)).to eq("acemq.schedule.45s")
    end

    it "gives a rung exactly the three arguments Java gives it" do
      scheduler
      rung = transport.declared_queues.find { |name, _| name == "acemq.schedule.1m" }.last

      expect(rung[:arguments]).to eq(
        "x-message-ttl" => 60_000,
        "x-dead-letter-exchange" => "acemq.schedule",
        "x-dead-letter-routing-key" => "acemq.schedule.due"
      )
      # Classic, and classic carries no x-queue-type at all, which is what the
      # other four libraries send. A fourth argument here is a queue Java
      # cannot declare.
      expect(rung[:queue_type]).to eq(:classic)
      expect(rung[:durable]).to be(true)
      expect(rung[:arguments].keys.size).to eq(3)
    end

    it "gives the control queue no arguments" do
      scheduler
      control = transport.declared_queues.find { |name, _| name == "acemq.schedule.due" }.last

      expect(control[:arguments]).to eq({})
      expect(control[:queue_type]).to eq(:classic)
    end

    it "binds every queue to the exchange under its own name" do
      scheduler

      expect(transport.bindings).to eq(
        [["acemq.schedule.1h", "acemq.schedule", "acemq.schedule.1h"],
         ["acemq.schedule.10m", "acemq.schedule", "acemq.schedule.10m"],
         ["acemq.schedule.1m", "acemq.schedule", "acemq.schedule.1m"],
         ["acemq.schedule.10s", "acemq.schedule", "acemq.schedule.10s"],
         ["acemq.schedule.1s", "acemq.schedule", "acemq.schedule.1s"],
         ["acemq.schedule.due", "acemq.schedule", "acemq.schedule.due"]]
      )
    end

    it "can be declared without starting a consumer" do
      expect(Scheduler.declare(mq))
        .to eq(["acemq.schedule.1h", "acemq.schedule.10m", "acemq.schedule.1m",
                "acemq.schedule.10s", "acemq.schedule.1s", "acemq.schedule.due"])
      expect(transport.subscriptions).to be_empty
    end

    # The regression this class was written around. A consumer declares its
    # dead-letter queues at start-up, so consuming the control queue the
    # ordinary way would leave acemq.schedule.due.dlq and .parked on every
    # broker any service ever ran a scheduler against.
    it "declares no dead-letter queues for the control queue" do
      scheduler

      expect(transport.declared_queues.map(&:first))
        .not_to include("acemq.schedule.due.dlq", "acemq.schedule.due.parked")
      expect(transport.declared_exchanges.map(&:first)).not_to include("acemq.dlx",
                                                                       "acemq.retry")
      expect(transport.declared_queues.size).to eq(6)
    end
  end

  describe "scheduling" do
    it "puts a message in the largest rung that does not overshoot" do
      scheduler.in(90, { "id" => "A-1" }, to: "invoice.due", exchange: "billing")

      expect(last_published.exchange).to eq("acemq.schedule")
      expect(last_published.routing_key).to eq("acemq.schedule.1m")
    end

    it "chooses the rung by the delay that is left" do
      {
        86_400 => "acemq.schedule.1h", 3600 => "acemq.schedule.1h",
        3599 => "acemq.schedule.10m", 600 => "acemq.schedule.10m",
        599 => "acemq.schedule.1m", 60 => "acemq.schedule.1m",
        59 => "acemq.schedule.10s", 10 => "acemq.schedule.10s",
        9 => "acemq.schedule.1s", 1 => "acemq.schedule.1s"
      }.each do |delay, rung|
        # A second of slack: the arithmetic is against the clock, and a delay
        # of exactly one rung must not fall to the one below while the test
        # runs.
        scheduler.in(delay + 0.5, { "id" => delay }, to: "later", exchange: "billing")
        expect(last_published.routing_key).to eq(rung)
      end
    end

    it "delivers straight away when the delay is under the smallest rung" do
      scheduler.in(0, { "id" => "A-2" }, to: "invoice.due", exchange: "billing")

      expect(last_published.exchange).to eq("billing")
      expect(last_published.routing_key).to eq("invoice.due")
      expect(scheduler.hops).to eq(0)
      expect(scheduler.delivered).to eq(1)
    end

    it "delivers straight away for a moment in the past" do
      scheduler.at(Time.now - 3600, { "id" => "A-3" }, to: "invoice.due", exchange: "billing")

      expect(last_published.exchange).to eq("billing")
    end

    it "writes the four headers a scheduled message carries" do
      due = Time.now + 90
      scheduler.at(due, { "id" => "A-4" }, to: "invoice.due", exchange: "billing")

      headers = last_published.headers
      expect(headers["x-schedule-exchange"]).to eq("billing")
      expect(headers["x-schedule-routing-key"]).to eq("invoice.due")
      expect(headers["x-schedule-content-type"]).to eq("application/json")
      # Epoch milliseconds, which is what Java's Instant#toEpochMilli writes
      # and the same integer x-acemq-first-seen carries.
      expect(headers["x-schedule-due-at"]).to be_a(Integer)
      expect(headers["x-schedule-due-at"]).to eq((due.to_f * 1000).to_i)
    end

    it "does not use the reserved prefix, which the envelope would refuse" do
      names = [Scheduler::TARGET_EXCHANGE, Scheduler::TARGET_ROUTING_KEY,
               Scheduler::DUE_AT, Scheduler::CONTENT_TYPE]

      expect(names).to eq(%w[x-schedule-exchange x-schedule-routing-key
                             x-schedule-due-at x-schedule-content-type])
      expect(names.any? { |name| AceMQ::AMQP::Headers.reserved?(name) }).to be(false)
      expect { AceMQ::AMQP::Envelope.new(headers: { "x-acemq-schedule" => "1" }) }
        .to raise_error(ArgumentError)
    end

    it "encodes the payload once, and carries bytes from then on" do
      scheduler.in(90, { "id" => "A-5" }, to: "invoice.due", exchange: "billing")

      expect(last_published.body).to eq('{"id":"A-5"}')
      expect(last_published.content_type).to eq("application/octet-stream")
    end

    it "counts what it was given" do
      scheduler.in(90, { "id" => "A-6" }, to: "invoice.due", exchange: "billing")
      scheduler.in(0, { "id" => "A-7" }, to: "invoice.due", exchange: "billing")

      expect(scheduler.scheduled).to eq(2)
      expect(scheduler.hops).to eq(1)
      expect(scheduler.delivered).to eq(1)
      expect(scheduler.to_s).to eq("Scheduler{rungs=1h, 10m, 1m, 10s, 1s, scheduled=2, " \
                                   "delivered=1}")
    end
  end

  describe "the control consumer" do
    let(:due_at) { Scheduler.millis(Time.now + 90) }
    let(:headers) do
      { "x-schedule-exchange" => "billing", "x-schedule-routing-key" => "invoice.due",
        "x-schedule-due-at" => due_at, "x-schedule-content-type" => "application/json" }
    end

    it "puts a message that is not due yet into the next rung down" do
      scheduler
      publish_into_control(headers)

      expect(last_published.routing_key).to eq("acemq.schedule.1m")
      expect(scheduler.hops).to eq(1)
      expect(scheduler.delivered).to eq(0)
    end

    it "carries the headers on unchanged, so the next hop knows the same things" do
      scheduler
      publish_into_control(headers)

      expect(last_published.headers).to include(headers)
    end

    it "delivers to the target when the message is due" do
      scheduler
      publish_into_control(headers.merge("x-schedule-due-at" => Scheduler.millis(Time.now - 1)),
                           body: '{"id":"A-8"}')

      expect(last_published.exchange).to eq("billing")
      expect(last_published.routing_key).to eq("invoice.due")
      expect(last_published.body).to eq('{"id":"A-8"}')
      expect(scheduler.delivered).to eq(1)
    end

    it "delivers under the content type the payload was encoded as" do
      scheduler
      publish_into_control(headers.merge("x-schedule-due-at" => Scheduler.millis(Time.now - 1),
                                         "x-schedule-content-type" => "text/csv"),
                           body: "a,b,c")

      expect(last_published.content_type).to eq("text/csv")
      expect(last_published.body).to eq("a,b,c")
    end

    it "falls back to JSON when nothing said what the payload was" do
      scheduler
      publish_into_control(headers.merge("x-schedule-due-at" => Scheduler.millis(Time.now - 1),
                                         "x-schedule-content-type" => ""))

      expect(last_published.content_type).to eq("application/json")
    end

    it "does not pass its own bookkeeping on to the consumer that was waiting" do
      scheduler
      publish_into_control(headers.merge("x-schedule-due-at" => Scheduler.millis(Time.now - 1)))

      expect(last_published.headers.keys.grep(/\Ax-schedule-/)).to be_empty
    end

    it "reads a due-at somebody wrote as a string" do
      scheduler
      publish_into_control(headers.merge("x-schedule-due-at" => (due_at - 100_000).to_s))

      expect(last_published.exchange).to eq("billing")
    end

    it "drops a message that no scheduler put there, and counts it" do
      scheduler
      before = transport.published.size
      publish_into_control({ "something" => "else" })

      expect(transport.published.size).to eq(before + 1)
      expect(scheduler.malformed).to eq(1)
      expect(scheduler.delivered).to eq(0)
    end

    it "drops one that is missing only the target exchange" do
      scheduler
      publish_into_control(headers.except("x-schedule-exchange"))

      expect(scheduler.malformed).to eq(1)
      expect(scheduler.hops).to eq(0)
    end
  end

  describe "its life" do
    it "is running until it is closed" do
      expect(scheduler).to be_running
      scheduler.close
      expect(scheduler).not_to be_running
    end

    it "closes itself when given a block" do
      inner = nil
      Scheduler.on(mq) do |s|
        inner = s
        expect(s).to be_running
      end

      expect(inner).not_to be_running
    end

    it "closes itself when the block raises" do
      inner = nil
      expect do
        Scheduler.on(mq) do |s|
          inner = s
          raise "the caller gave up"
        end
      end.to raise_error("the caller gave up")

      expect(inner).not_to be_running
    end
  end

  describe "the verbatim codec" do
    let(:codec) { Scheduler::Verbatim.new("text/csv") }

    it "writes bytes out unchanged under the content type they were encoded as" do
      expect(codec.content_type).to eq("text/csv")
      expect(codec.encode("a,b,c")).to eq("a,b,c")
    end

    it "only publishes" do
      expect(codec.can_decode?("text/csv")).to be(false)
      expect { codec.decode("a,b,c") }.to raise_error(AceMQ::AMQP::DecodeError)
      expect { codec.encode({ "a" => 1 }) }.to raise_error(AceMQ::AMQP::EncodeError)
    end
  end
end
