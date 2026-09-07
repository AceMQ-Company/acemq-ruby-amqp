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

RSpec.describe AceMQ::AMQP::Patterns::Requester do
  let(:transport) { LoopbackTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: transport, origin: "rspec@here") }

  after { mq.close }

  def serving(&handler)
    AceMQ::AMQP::Patterns.serve(mq, "price.requests", &handler)
  end

  it "carries a question to a responder and the answer back" do
    serving { |message| { "price" => message.payload["sku"].length * 100 } }
    requester = described_class.new(mq, to: "price.requests")

    expect(requester.call({ "sku" => "X-12" })).to eq({ "price" => 400 })
    requester.close
  end

  it "pairs a reply with its request by correlation, not by arrival" do
    # Two requests in flight, answered out of order. Only the correlation
    # identifier says which answer belongs to which caller.
    held = []
    serving do |message|
      held << message
      { "for" => message.payload["sku"] }
    end
    requester = described_class.new(mq, to: "price.requests")

    requester.call({ "sku" => "A" })
    requester.call({ "sku" => "B" })

    correlations = held.map { |m| m.envelope.correlation_id }
    expect(correlations.uniq.size).to eq(2)
    requester.close
  end

  it "tells the responder where to reply, in a header a handler can read" do
    # An application header rather than AMQP's reply-to property, so it travels
    # through the same envelope machinery as everything else and survives a hop
    # through a service that rebuilds the message.
    seen = nil
    serving do |message|
      seen = message.envelope.headers[AceMQ::AMQP::Patterns::REPLY_TO_HEADER]
      {}
    end
    requester = described_class.new(mq, to: "price.requests", reply_to: "replies")
    requester.call({ "sku" => "A" })

    expect(seen).to eq("replies")
    requester.close
  end

  it "marks the reply as caused by the request" do
    serving { |_message| { "ok" => true } }
    requester = described_class.new(mq, to: "price.requests", reply_to: "replies")
    requester.call({ "sku" => "A" })

    reply = transport.published_to("replies").first
    request = transport.published_to("price.requests").first
    expect(reply.headers[AceMQ::AMQP::Headers::CAUSATION])
      .to eq(request.headers[AceMQ::AMQP::Headers::ID])
    expect(reply.headers[AceMQ::AMQP::Headers::CORRELATION])
      .to eq(request.headers[AceMQ::AMQP::Headers::CORRELATION])
    requester.close
  end

  it "sends a responder's failure back rather than leaving the caller to time out" do
    # A caller blocked on a reply should learn that it failed. Waiting out a
    # thirty-second timeout for an answer that was decided immediately is the
    # worst outcome available.
    serving { |_message| raise "the catalogue is down" }
    requester = described_class.new(mq, to: "price.requests")

    expect { requester.call({ "sku" => "A" }) }
      .to raise_error(AceMQ::AMQP::Patterns::ResponderFailed, /the catalogue is down/)
    requester.close
  end

  it "settles a failed request rather than retrying it, having already answered" do
    # Replying and then retrying would answer twice.
    serving { |_message| raise "the catalogue is down" }
    requester = described_class.new(mq, to: "price.requests")
    begin
      requester.call({ "sku" => "A" })
    rescue AceMQ::AMQP::Patterns::ResponderFailed
      nil
    end

    expect(transport.published_to("price.requests").size).to eq(1)
    expect(transport.published_to("price.requests.dlq").size).to eq(1)
    requester.close
  end

  it "gives up on a reply that never comes, and says so" do
    # Nothing is serving the queue. A timeout says an answer did not arrive; it
    # says nothing about whether the work was done, which is why a request that
    # changes anything wants an idempotent responder.
    requester = described_class.new(mq, to: "price.requests", timeout: 0.05)

    expect { requester.call({ "sku" => "A" }) }
      .to raise_error(AceMQ::AMQP::Patterns::RequestTimedOut, /within 0.05 seconds/)
    requester.close
  end

  it "drops a reply nobody is waiting for any more" do
    # A reply to a request that already timed out. Blocking or raising here
    # would stall the reply consumer for everybody else on the same queue.
    requester = described_class.new(mq, to: "price.requests", reply_to: "replies",
                                        timeout: 0.01)
    begin
      requester.call({ "sku" => "A" })
    rescue AceMQ::AMQP::Patterns::RequestTimedOut
      nil
    end

    expect { mq.publish({ "late" => true }, to: "replies", correlation_id: "nobody") }
      .not_to raise_error
    requester.close
  end

  it "gives a request with nowhere to reply to the dead-letter queue" do
    # Retrying cannot make a reply queue appear.
    serving { |_message| { "ok" => true } }
    mq.publish({ "sku" => "A" }, to: "price.requests")

    dead = transport.published_to("price.requests.dlq")
    expect(dead.size).to eq(1)
    expect(dead.first.headers[AceMQ::AMQP::Headers::ERROR])
      .to match(/carries no acemq-reply-to header/)
  end

  it "generates an exclusive reply queue that goes away with the requester" do
    # A reply queue that outlived its requester would collect answers nobody is
    # waiting for.
    requester = described_class.new(mq, to: "price.requests")

    expect(requester.reply_queue).to start_with("acemq-reply-")
    declared = transport.declared_queues.find { |name, _| name == requester.reply_queue }
    expect(declared.last).to include(exclusive: true, auto_delete: true, durable: false)
    requester.close
  end
end
