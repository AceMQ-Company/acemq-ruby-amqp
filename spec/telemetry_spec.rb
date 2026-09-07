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

Telemetry = AceMQ::AMQP::Telemetry

RSpec.describe AceMQ::AMQP::Telemetry do
  let(:metrics) { Telemetry::Registry.new }
  let(:transport) { FakeTransport.new }
  let(:mq) do
    AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here", telemetry: metrics)
  end

  def consumer(policy: AceMQ::AMQP::RetryPolicy.none, threshold: 30, &handler)
    AceMQ::AMQP::Consumer.new(
      transport: transport, queue: "orders.new", handler: handler,
      codec: AceMQ::AMQP::JSONCodec.new, retry_policy: policy,
      retry_threshold: threshold, telemetry: metrics
    )
  end

  def delivery_for(body: '{"id":"A-1"}', attempt: 1)
    FakeDelivery.build(
      body: body,
      headers: { AceMQ::AMQP::Headers::ID => "msg-1",
                 AceMQ::AMQP::Headers::ATTEMPT => attempt,
                 AceMQ::AMQP::Headers::FIRST_SEEN => AceMQ::AMQP::Envelope.millis(Time.now) }
    )
  end

  describe "publishing" do
    it "counts what reached the broker, by exchange" do
      mq.publish({ "order_id" => "A-1" }, to: "orders.new")
      mq.publish({ "order_id" => "A-2" }, to: "order.placed", exchange: "orders-events")

      expect(metrics[Telemetry::PUBLISHED, exchange: ""]).to eq(1)
      expect(metrics[Telemetry::PUBLISHED, exchange: "orders-events"]).to eq(1)
    end

    it "counts a publish the broker would not take, and one an interceptor refused" do
      # Both are messages that did not reach the broker, which is what the
      # metric is about; whether it was the broker or a policy that said no is
      # the exception's business, not the counter's.
      allow(transport).to receive(:publish).and_raise(AceMQ::AMQP::PublishError, "no confirm")
      expect { mq.publish({ "order_id" => "A-1" }, to: "orders.new") }
        .to raise_error(AceMQ::AMQP::PublishError)

      mq.intercept_publish { |_c| raise "not allowed" }
      expect { mq.publish({ "order_id" => "A-2" }, to: "orders.new") }
        .to raise_error(RuntimeError)

      expect(metrics[Telemetry::PUBLISH_FAILED, exchange: ""]).to eq(2)
      expect(metrics[Telemetry::PUBLISHED, exchange: ""]).to eq(0)
    end
  end

  describe "consuming" do
    it "counts every delivery and what was decided about it" do
      consumer { AceMQ::AMQP::Ack.accept }.handle(delivery_for.first)
      consumer(policy: AceMQ::AMQP::RetryPolicy.fixed(5, 0)) do
        AceMQ::AMQP::Ack.retry("not yet")
      end.handle(delivery_for.first)
      consumer { AceMQ::AMQP::Ack.reject("no such customer") }.handle(delivery_for.first)

      queue = { queue: "orders.new" }
      expect(metrics[Telemetry::CONSUMED, **queue]).to eq(3)
      expect(metrics[Telemetry::ACCEPTED, **queue]).to eq(1)
      expect(metrics[Telemetry::RETRIED, **queue]).to eq(1)
      expect(metrics[Telemetry::REJECTED, **queue]).to eq(1)
    end

    it "times the handler, and counts the message before it runs" do
      # Counted on the way in, so a handler that never returns is still a
      # message this consumer was given — which is the difference between a
      # queue nothing is reading and a queue one thing is stuck on.
      consumer do
        sleep(0.01)
        AceMQ::AMQP::Ack.accept
      end.handle(delivery_for.first)

      timing = metrics.timings[Telemetry::Registry.key(Telemetry::HANDLER_DURATION,
                                                       queue: "orders.new")]
      expect(timing.count).to eq(1)
      expect(timing.sum).to be >= 0.01
      expect(timing.mean).to eq(timing.sum)
    end

    it "leaves the in-flight gauge back where it started" do
      consumer { AceMQ::AMQP::Ack.accept }.handle(delivery_for.first)

      key = Telemetry::Registry.key(Telemetry::IN_FLIGHT, queue: "orders.new")
      expect(metrics.gauges[key]).to eq(0)
    end

    it "counts a message that ran out of attempts, and one nothing could read" do
      consumer { AceMQ::AMQP::Ack.retry("the warehouse is down") }.handle(delivery_for.first)
      consumer { AceMQ::AMQP::Ack.accept }.handle(delivery_for(body: "{ not json").first)

      queue = { queue: "orders.new" }
      expect(metrics[Telemetry::DEAD_LETTERED, **queue]).to eq(1)
      expect(metrics[Telemetry::PARKED, **queue]).to eq(1)
    end
  end

  describe "a rung that is not on the broker" do
    let(:policy) { AceMQ::AMQP::RetryPolicy.fixed(3, 60) }

    it "counts it, and waits here instead, rather than losing the message" do
      # Nothing breaks and no message is lost: the retry still happens and so
      # does the wait. What is lost is the reason the rung exists — a restart
      # mid-wait turns a minute's backoff into none — and without this counter
      # a topology that was never applied looks exactly like one that was.
      transport.missing!("orders.new.retry.1m")
      subject = consumer(policy: policy) { AceMQ::AMQP::Ack.retry("the warehouse is down") }
      allow(subject).to receive(:sleep)

      subject.handle(delivery_for.first)

      expect(metrics[Telemetry::RUNG_MISSING, queue: "orders.new"]).to eq(1)
      # Back onto the source queue, waited here, rather than into a rung the
      # broker would have dropped it into.
      expect(transport.published_to("orders.new").size).to eq(1)
      expect(transport.published_to("orders.new.retry.1m")).to be_empty
    end

    it "counts nothing when the rung is there" do
      subject = consumer(policy: policy) { AceMQ::AMQP::Ack.retry("the warehouse is down") }
      subject.handle(delivery_for.first)

      expect(metrics[Telemetry::RUNG_MISSING, queue: "orders.new"]).to eq(0)
      expect(transport.published_to("orders.new.retry.1m").size).to eq(1)
    end

    it "asks the broker once per rung and remembers the answer" do
      # One round trip per rung for the life of the consumer. Asking on every
      # retry would put a round trip on the path a failing queue takes most.
      subject = consumer(policy: policy) { AceMQ::AMQP::Ack.retry("the warehouse is down") }
      allow(transport).to receive(:queue_exists?).and_call_original

      3.times { subject.handle(delivery_for.first) }

      expect(transport).to have_received(:queue_exists?).once
    end
  end

  describe "the registry" do
    it "gives the same key to the same labels, however they were written" do
      # Otherwise one counter quietly becomes several and the total is wrong in
      # a way nobody notices.
      metrics.count("acemq.test", 1, queue: "q", exchange: "e")
      metrics.count("acemq.test", 1, exchange: "e", queue: "q")

      expect(metrics.counts).to eq({ "acemq.test{exchange=e,queue=q}" => 2 })
    end

    it "renders the Prometheus text format, dots and all" do
      metrics.count(Telemetry::PUBLISHED, 2, exchange: "orders-events")
      metrics.gauge(Telemetry::IN_FLIGHT, 3, queue: "orders.new")
      metrics.observe(Telemetry::HANDLER_DURATION, 0.5, queue: "orders.new")

      rendered = metrics.to_prometheus
      expect(rendered).to include(<<~METRIC)
        # TYPE acemq_messages_published counter
        acemq_messages_published{exchange="orders-events"} 2
      METRIC
      expect(rendered).to include(<<~METRIC)
        # TYPE acemq_messages_in_flight gauge
        acemq_messages_in_flight{queue="orders.new"} 3
      METRIC
      expect(rendered).to include("acemq_handler_duration_count{queue=\"orders.new\"} 1\n")
      expect(rendered).to include("acemq_handler_duration_sum{queue=\"orders.new\"} 0.5\n")
    end

    it "keeps the fastest and the slowest, and no percentiles" do
      # Percentiles need either every sample kept or a sketch, and a library
      # that quietly did either would be deciding about this process's memory.
      [0.1, 0.9, 0.5].each { |seconds| metrics.observe("acemq.test", seconds) }

      timing = metrics.timings["acemq.test"]
      expect([timing.count, timing.min, timing.max]).to eq([3, 0.1, 0.9])
      expect(timing.mean).to be_within(0.0001).of(0.5)
    end
  end

  describe "an observer that misbehaves" do
    it "cannot stop a message being handled, and says so once" do
      # A metrics backend that is down is not a reason to stop delivering
      # messages, and a library that let it become one would have made
      # observability the thing that caused the outage.
      broken = Class.new do
        def count(_metric, _delta = 1, **_labels) = raise("the metrics endpoint is down")
        def observe(_metric, _seconds, **_labels) = nil
        def gauge(_metric, _value, **_labels) = nil
      end
      mq = AceMQ::AMQP::Connection.new(transport: transport, telemetry: broken.new)

      expect { 3.times { mq.publish({ "order_id" => "A-1" }, to: "orders.new") } }
        .to output(/the telemetry observer raised RuntimeError/).to_stderr
      expect(transport.published.size).to eq(3)
    end
  end

  describe "measuring nothing" do
    it "is what a connection does until somebody asks for numbers" do
      quiet = AceMQ::AMQP::Connection.new(transport: transport)
      expect(quiet.telemetry.observer).to be_a(Telemetry::None)

      quiet.publish({ "order_id" => "A-1" }, to: "orders.new")
      expect(transport.published.size).to eq(1)
    end
  end
end
