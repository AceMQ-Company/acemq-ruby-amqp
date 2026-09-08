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

Health = AceMQ::AMQP::Health

RSpec.describe AceMQ::AMQP::Health do
  let(:transport) { FakeTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }

  describe "a connection" do
    it "is up when the broker answers, and says how long it took" do
      report = mq.health

      expect(report).to be_up
      expect(report.parts["consumers"]).to eq(0)
      expect(report.parts["round_trip_ms"]).to be >= 0
    end

    it "proves the round trip rather than reading a flag" do
      # An open socket answers the same as a healthy broker until something is
      # asked of it, so the check asks: it declares a queue named for this
      # moment, which cannot collide with another instance running the same
      # check, and then deletes it again.
      allow(transport).to receive(:delete_queue).and_call_original
      mq.health

      declared = transport.declared_queues.map(&:first)
      expect(declared.size).to eq(1)
      expect(declared.first).to match(/\Aacemq-health-[0-9a-f-]{36}\z/)
      # Classic, and it could be nothing else: a probe queue is exclusive and
      # auto-deleting, and RabbitMQ refuses to replicate either.
      expect(transport.declared_queues.first.last)
        .to eq({ queue_type: :classic, durable: false, auto_delete: true,
                 exclusive: true, arguments: {} })
      expect(transport).to have_received(:delete_queue).with(declared.first)
    end

    it "is down when the connection has been closed" do
      mq.close
      report = mq.health

      expect(report).to be_down
      expect(report.detail).to eq("the connection has been closed")
    end

    it "is down when the broker will not answer, and says what it said" do
      allow(transport).to receive(:declare_queue)
        .and_raise(AceMQ::AMQP::TransportError, "connection refused")
      report = mq.health

      expect(report).to be_down
      expect(report.detail).to match(/the broker did not answer: connection refused/)
    end

    it "is degraded, not down, when a consumer has stopped under a live connection" do
      # The process can still publish and its other consumers still work, so
      # failing the probe would take out something doing most of its job. But a
      # queue with nothing reading it is a real fault and has to be visible.
      mq.consume("orders.new") { AceMQ::AMQP::Ack.accept }
      mq.consume("orders.shipped") { AceMQ::AMQP::Ack.accept }
      mq.consumers.first.cancel

      report = mq.health
      expect(report).to be_degraded
      expect(report.detail).to eq("1 of 2 consumers has stopped")
      expect(report.parts["consumers_running"]).to eq(1)
      expect(report.parts["queues"]).to eq(["orders.new", "orders.shipped"])
    end

    it "asks the subscription whether it is running rather than remembering" do
      # The two can disagree: a channel closed by the broker stops delivery
      # without anything here being told, and a flag set in cancel would call a
      # consumer that had been deaf for an hour running.
      consumer = mq.consume("orders.new") { AceMQ::AMQP::Ack.accept }
      expect(consumer).to be_running

      consumer.cancel
      expect(consumer).not_to be_running
    end
  end

  describe "several checks at once" do
    Passing = Struct.new(:name) do
      def check
        AceMQ::AMQP::Health::Report.new(status: Health::UP, checked_at: Time.now, parts: {})
      end
    end

    Failing = Struct.new(:name) do
      def check
        AceMQ::AMQP::Health::Report.new(status: Health::DOWN, detail: "no route to host",
                                        checked_at: Time.now, parts: {})
      end
    end

    it "takes the worst of them, because a service that cannot reach one thing is not ready" do
      report = Health.aggregate(Passing.new("database"), Failing.new("catalogue"))

      expect(report).to be_down
      expect(report.detail).to eq("catalogue")
      expect(report.parts["database"]).to be_up
      expect(report.parts["catalogue"].detail).to eq("no route to host")
    end

    it "combines the connection's own check with the application's" do
      report = Health.aggregate(Health::Check.new("broker", mq), Passing.new("database"))

      expect(report).to be_up
      expect(report.parts.keys).to contain_exactly("broker", "database")
    end

    it "turns a check that raises into a part rather than an exception" do
      # A readiness probe that raises tells the orchestrator nothing at all.
      exploding = Struct.new(:name) do
        def check = raise("the check itself is broken")
      end
      report = Health.aggregate(exploding.new("catalogue"))

      expect(report).to be_down
      expect(report.parts["catalogue"].detail).to match(/the check itself failed/)
    end

    it "is up with nothing to check, which is the honest answer" do
      expect(Health.aggregate).to be_up
    end
  end

  describe "what a probe reads" do
    it "renders the shape every AceMQ library serves at /acemq-health" do
      rendered = mq.health.to_h

      expect(rendered["status"]).to eq("up")
      expect(rendered["checked"]).to match(/\A\d{4}-\d\d-\d\dT.*Z\z/)
      expect(rendered["parts"]["consumers"]).to eq(0)
    end

    it "renders a nested report as a hash rather than an object" do
      rendered = Health.aggregate(Health::Check.new("broker", mq)).to_h

      expect(rendered["parts"]["broker"]).to be_a(Hash)
      expect(rendered["parts"]["broker"]["status"]).to eq("up")
    end
  end
end
