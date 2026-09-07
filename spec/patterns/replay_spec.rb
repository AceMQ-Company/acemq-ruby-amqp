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

RSpec.describe "replaying a dead-letter queue" do
  let(:transport) { LoopbackTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }
  let(:dlq) { "orders.new.dlq" }

  # Puts messages on the dead-letter queue with nothing consuming it, which is
  # the state a replay is for.
  def dead_letter(count, error: "the warehouse is down")
    count.times do |i|
      mq.publish({ "order_id" => "A-#{i}" }, to: dlq, type: "order.placed.v2",
                                             error: error, headers: { "n" => i })
    end
  end

  it "moves everything back and says why it stopped" do
    dead_letter(3)
    result = AceMQ::AMQP::Patterns.replay(mq, from: dlq, routing_key: "orders.new")

    expect(result.moved).to eq(3)
    expect(result.skipped).to eq(0)
    expect(result.reason).to eq(:drained)
    expect(transport.message_count("orders.new")).to eq(3)
    expect(transport.message_count(dlq)).to eq(0)
  end

  it "leaves behind what the filter declines, and comes back to it later" do
    # What makes a replay something that can be done in stages: put back the
    # ones that failed for a reason the fix addressed, and look at the rest
    # afterwards.
    dead_letter(2, error: "connection timeout")
    dead_letter(2, error: "no such customer")

    result = AceMQ::AMQP::Patterns.replay(mq, from: dlq,
                                              routing_key: "orders.new") do |envelope, _body|
      envelope.error.include?("timeout")
    end

    expect(result.moved).to eq(2)
    expect(result.skipped).to eq(2)
    expect(transport.message_count(dlq)).to eq(2)
  end

  it "reaches every message even though a declined one goes back to the head" do
    # The bug this design exists to avoid. Returning a declined message
    # immediately puts it back at the head of the queue, so the next read hands
    # over the same message for ever and everything behind it is never seen.
    dead_letter(1, error: "no such customer")
    dead_letter(1, error: "connection timeout")

    result = AceMQ::AMQP::Patterns.replay(mq, from: dlq,
                                              routing_key: "orders.new") do |envelope, _body|
      envelope.error.include?("timeout")
    end

    expect(result.moved).to eq(1)
    expect(result.reason).to eq(:drained)
    expect(transport.message_count(dlq)).to eq(1)
  end

  it "stops at the limit, and says that is why" do
    # "Moved 2" means something quite different when the limit was 2.
    dead_letter(5)
    result = AceMQ::AMQP::Patterns.replay(mq, from: dlq, routing_key: "orders.new", limit: 2)

    expect(result.moved).to eq(2)
    expect(result.reason).to eq(:limit)
    expect(transport.message_count(dlq)).to eq(3)
  end

  it "stops at the deadline, whatever is left" do
    # Somebody draining a queue at three in the morning wants their tool back
    # before the incident is over.
    dead_letter(10)
    result = AceMQ::AMQP::Patterns.replay(mq, from: dlq, routing_key: "orders.new",
                                              deadline: 0.05) do |_envelope, _body|
      sleep(0.02)
      true
    end

    expect(result.reason).to eq(:deadline)
    expect(result.moved).to be_between(1, 9)
    expect(transport.message_count(dlq)).to eq(10 - result.moved)
  end

  it "keeps the message's own routing key when none is given" do
    # So a message goes back where it came from rather than everywhere.
    dead_letter(1)
    AceMQ::AMQP::Patterns.replay(mq, from: dlq, exchange: "orders-events")

    expect(transport.published.last.exchange).to eq("orders-events")
    expect(transport.published.last.routing_key).to eq(dlq)
  end

  it "refuses a replay that would put every message back where it found it" do
    # The default exchange routes to the queue named by the routing key, and a
    # dead letter's routing key is the dead-letter queue. Without this the
    # replay reads a message and writes it straight back, for ever, and the only
    # sign of it is a queue that never empties.
    expect { AceMQ::AMQP::Patterns.replay(mq, from: dlq) }
      .to raise_error(ArgumentError, /straight back onto the queue it was read from/)
    expect { AceMQ::AMQP::Patterns.replay(mq, from: dlq, routing_key: dlq) }
      .to raise_error(ArgumentError, /name an exchange or a routing key/)
  end

  it "stamps a replayed message so it can be told from an original" do
    dead_letter(1)
    AceMQ::AMQP::Patterns.replay(mq, from: dlq, routing_key: "orders.new")

    headers = transport.published_to("orders.new").first.headers
    expect(headers[AceMQ::AMQP::Patterns::REPLAYED_FROM_HEADER]).to eq(dlq)
    expect(headers[AceMQ::AMQP::Patterns::REPLAY_COUNT_HEADER]).to eq(1)
    expect(headers[AceMQ::AMQP::Patterns::REPLAYED_AT_HEADER]).to match(/\A\d{4}-\d\d-\d\dT/)
  end

  it "counts a second replay of the same message" do
    # Somebody replayed it, it failed again, and here they are once more. The
    # count is the only thing that says so.
    dead_letter(1)
    AceMQ::AMQP::Patterns.replay(mq, from: dlq, routing_key: "staging")
    AceMQ::AMQP::Patterns.replay(mq, from: "staging", routing_key: "orders.new")

    headers = transport.published_to("orders.new").first.headers
    expect(headers[AceMQ::AMQP::Patterns::REPLAY_COUNT_HEADER]).to eq(2)
    expect(headers[AceMQ::AMQP::Patterns::REPLAYED_FROM_HEADER]).to eq("staging")
  end

  it "keeps the envelope the message arrived with, reason included" do
    # Whoever is watching a replay go past needs to know what these messages
    # were, and the identity has to survive or nothing keyed on it agrees.
    dead_letter(1)
    original = transport.published_to(dlq).first
    AceMQ::AMQP::Patterns.replay(mq, from: dlq, routing_key: "orders.new")

    replayed = transport.published_to("orders.new").first.headers
    expect(replayed[AceMQ::AMQP::Headers::ID]).to eq(original.headers[AceMQ::AMQP::Headers::ID])
    expect(replayed[AceMQ::AMQP::Headers::TYPE]).to eq("order.placed.v2")
    expect(replayed[AceMQ::AMQP::Headers::ERROR]).to eq("the warehouse is down")
    expect(replayed["n"]).to eq(0)
  end

  it "puts a message back rather than losing it when the broker refuses" do
    # A replay that loses messages is worse than one that stops early.
    dead_letter(2)
    allow(transport).to receive(:publish).and_raise(AceMQ::AMQP::PublishError, "no confirm")

    expect { AceMQ::AMQP::Patterns.replay(mq, from: dlq, routing_key: "orders.new") }
      .to raise_error(AceMQ::AMQP::Patterns::ReplayFailed) { |e| expect(e.result.moved).to eq(0) }
    expect(transport.message_count(dlq)).to eq(2)
  end

  it "refuses a replay with no queue to read from" do
    expect { AceMQ::AMQP::Patterns.replay(mq, from: "") }
      .to raise_error(ArgumentError, /needs a queue to read from/)
  end
end
