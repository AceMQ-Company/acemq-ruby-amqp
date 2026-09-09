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

RSpec.describe AceMQ::AMQP::Connection do
  let(:transport) { FakeTransport.new }
  let(:mq) { described_class.new(transport: transport, origin: "checkout@pod-7") }

  it "puts the envelope's headers on the message" do
    mq.publish({ "id" => "A-1" }, to: "orders.new", type: "order.placed.v2")
    sent = transport.published.last

    expect(sent.headers[AceMQ::AMQP::Headers::TYPE]).to eq("order.placed.v2")
    expect(sent.headers[AceMQ::AMQP::Headers::ORIGIN]).to eq("checkout@pod-7")
    expect(sent.headers[AceMQ::AMQP::Headers::ATTEMPT]).to eq(1)
    expect(sent.headers[AceMQ::AMQP::Headers::VERSION]).to eq(1)
    expect(sent.content_type).to eq("application/json")
    expect(sent.body).to eq('{"id":"A-1"}')
  end

  it "defaults the type to the routing key, as every other language does" do
    mq.publish({}, to: "orders.new")
    expect(transport.published.last.headers[AceMQ::AMQP::Headers::TYPE]).to eq("orders.new")
  end

  it "sends the message id as the broker's message id as well as the header" do
    # Two names for the same thing, because the broker's own field is what a
    # management console shows and the header is what another language reads.
    envelope = mq.publish({}, to: "orders.new")
    expect(transport.published.last.message_id).to eq(envelope.id)
    expect(transport.published.last.headers[AceMQ::AMQP::Headers::ID]).to eq(envelope.id)
  end

  it "returns what actually went on the wire" do
    envelope = mq.publish({}, to: "orders.new", correlation_id: "corr-9")
    expect(envelope.correlation_id).to eq("corr-9")
  end

  it "refuses to be told the envelope twice" do
    # One or the other. Silently letting the keywords lose to the envelope
    # would leave somebody looking for a correlation id they know they set.
    expect { mq.publish({}, to: "q", envelope: AceMQ::AMQP::Envelope.new, type: "x") }
      .to raise_error(ArgumentError, /one or the other/)
  end

  it "names the machine when nobody names the service" do
    expect(described_class.new(transport: transport).origin).to start_with("acemq@")
  end

  it "refuses something that is not a codec, at start-up rather than at 3am" do
    expect { described_class.new(transport: transport, codec: Object.new) }
      .to raise_error(ArgumentError, /not a codec/)
  end

  it "refuses to consume without a handler" do
    expect { mq.consume("orders.new") }.to raise_error(ArgumentError, /needs a block/)
  end

  it "declares a queue as a quorum queue, the way declareQueue does in Java" do
    # The direct call, not the topology: the same default has to hold here, or
    # a service that declares its queue in one line still ends up with a classic
    # queue no Java service can share.
    mq.declare_queue("orders.new")

    name, options = transport.declared_queues.last
    expect(name).to eq("orders.new")
    expect(options[:queue_type]).to eq(:quorum)
    expect(options[:arguments]).to eq("x-queue-type" => "quorum")
  end

  it "declares a classic queue when asked for one, with no x-queue-type on it" do
    mq.declare_queue("orders.new", queue_type: :classic)

    expect(transport.declared_queues.last.last)
      .to include(queue_type: :classic, arguments: {})
  end

  it "will not declare a quorum queue the broker would refuse" do
    expect { mq.declare_queue("replies", queue_type: :quorum, auto_delete: true) }
      .to raise_error(AceMQ::AMQP::QueueTypeError, /auto-delete/)
    expect(transport.declared_queues).to be_empty
  end

  it "closes the transport" do
    mq.close
    expect(transport.closed?).to be(true)
  end
end

RSpec.describe AceMQ::AMQP::Transport do
  it "names the gem to install when bunny is not there" do
    # The gemspec declares no runtime dependencies on purpose, so this is the
    # first moment anybody finds out. "cannot load such file -- bunny" does not
    # say which library wanted it or what to do about it.
    allow(described_class).to receive(:require).with("bunny").and_raise(LoadError, "no bunny")

    expect { described_class.load_driver! }
      .to raise_error(AceMQ::AMQP::DependencyMissing, /gem "bunny"/)
  end

  it "keeps the password out of an error that will be logged" do
    # A credential that reaches a log is a credential that has to be rotated.
    expect(described_class.redact("amqps://svc:hunter2@broker:5671/prod"))
      .to eq("amqps://svc:***@broker:5671/prod")
  end
end

# One flag rather than two exception classes, so a caller who only wants to know
# that the message did not arrive rescues one thing. Go and Python split the same
# failure the same way, and the telemetry reads the word off this.
RSpec.describe AceMQ::AMQP::PublishError do
  it "is not unroutable unless it says so" do
    expect(described_class.new("no confirm").unroutable?).to be(false)
    # Raised with the two-argument form `raise Class, "message"` all over this
    # library, which has to keep working.
    error = begin
      raise described_class, "no confirm"
    rescue described_class => e
      e
    end
    expect(error.unroutable?).to be(false)
    expect(error.message).to eq("no confirm")
  end

  it "is a TransportError either way, so one rescue still catches both" do
    unroutable = described_class.new("nowhere to route it", unroutable: true)

    expect(unroutable.unroutable?).to be(true)
    expect(unroutable).to be_a(AceMQ::AMQP::TransportError)
  end
end
