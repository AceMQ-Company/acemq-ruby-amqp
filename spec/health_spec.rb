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

  describe "a blocked connection" do
    # RabbitMQ blocks a connection when it is low on memory or disk, and every
    # publish on it stops. The temptation is to fail the probe, and failing it
    # is exactly wrong — so the rule is the one Java's AceMqHealthIndicator and
    # Go's actuator both keep: up, with the reason.
    #
    # The transport is asked, not bunny. A double that has never heard of bunny
    # answers `blocked_reason` and that is the whole seam.
    before { transport.blocked!("low on disk space") }

    it "is up, because restarting into the same blocked broker helps nobody" do
      # An orchestrator told this instance is unready restarts it into the same
      # pressured broker, having thrown away whatever it was holding — and doing
      # that to every replica turns a memory alarm into an outage with a crash
      # loop on top.
      expect(mq.health).to be_up
    end

    it "says why, in fixed words an alert rule can match, with the broker's own" do
      report = mq.health

      expect(report.detail).to eq("the broker has blocked this connection; " \
                                  "publishing is paused: low on disk space")
      expect(report.parts["blocked"]).to be(true)
      expect(report.parts["blocked_reason"]).to eq("low on disk space")
    end

    it "does not hide a stopped consumer behind the block, or the block behind it" do
      mq.consume("orders.new") { AceMQ::AMQP::Ack.accept }
      mq.consume("orders.shipped") { AceMQ::AMQP::Ack.accept }
      mq.consumers.first.cancel(timeout: 0)

      report = mq.health
      expect(report).to be_degraded
      expect(report.detail).to eq("1 of 2 consumers has stopped; the broker has blocked " \
                                  "this connection; publishing is paused: low on disk space")
    end

    it "is down when the connection is shut, because that is the better reason" do
      mq.close
      expect(mq.health).to be_down
    end

    it "does not run the round trip the rest of this check is built on" do
      # A blocked connection is one the broker has stopped reading, so the
      # declare does not fail — it hangs until bunny's continuation timeout
      # gives up, and then this check calls a live broker down. Verified
      # against a real broker with the memory watermark at zero: the declare
      # timed out and the report said "the broker did not answer".
      report = mq.health

      expect(transport.declared_queues).to be_empty
      expect(report.parts).not_to have_key("round_trip_ms")
      expect(report).to be_up
    end

    it "reads a block that arrived while the probe was in flight as the reason for it" do
      # The race the line above cannot close: not blocked when the check
      # started, blocked by the time the declare gave up. The block is the
      # explanation for the silence, not a second fault beside it.
      transport.unblocked!
      allow(transport).to receive(:declare_queue) do
        transport.blocked!("low on memory")
        raise AceMQ::AMQP::TransportError, "Timeout::Error"
      end

      report = mq.health
      expect(report).to be_up
      expect(report.detail).to match(/publishing is paused: low on memory/)
    end

    it "is answered off the connection too, for a publisher that would rather not hang" do
      # A publish on a blocked connection hangs rather than failing: the broker
      # stops reading the socket. This is a flag in memory, so unlike the report
      # it costs no round trip.
      expect(mq).to be_blocked
      expect(mq.blocked_reason).to eq("low on disk space")

      transport.unblocked!
      expect(mq).not_to be_blocked
      expect(mq.blocked_reason).to be_nil
    end
  end

  describe "a transport that cannot say whether it is blocked" do
    let(:transport) { LoopbackTransport.new }

    it "is not asked, and the report says nothing about blocking" do
      # The seam is deliberately double-friendly: a hand-written transport that
      # predates this and answers nothing about blocking is a transport that is
      # simply not blocked, not a health check that raises inside a probe.
      report = mq.health

      expect(report).to be_up
      expect(mq).not_to be_blocked
      expect(report.to_h["parts"]).not_to have_key("blocked")
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

    it "keeps a blocked connection up rather than dragging the aggregate down" do
      # The failure mode worth naming: a check that reported a blocked broker as
      # degraded or down would poison every aggregate it is part of, so the
      # careful answer here would be overruled by whichever check folded a plain
      # one in beside it.
      transport.blocked!("low on memory")
      report = Health.aggregate(Health::Check.new("broker", mq), Passing.new("database"))

      expect(report).to be_up
      expect(report.parts["broker"].parts["blocked"]).to be(true)
    end

    it "does not lose the reason a part gave for being up" do
      # Summarising by status alone would answer "up" with an empty detail and
      # throw away, at exactly the line an operator reads first, the one fact
      # the check went to the trouble of finding.
      transport.blocked!("low on memory")
      report = Health.aggregate(Health::Check.new("broker", mq), Passing.new("database"))

      expect(report.detail).to eq("broker")
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
