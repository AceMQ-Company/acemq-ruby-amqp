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

# Records what it was told about, in the order it was told.
class BatchRecordingInterceptor
  attr_reader :before, :confirmed, :errors

  def initialize
    @before = []
    @confirmed = []
    @errors = []
  end

  def before_publish(context) = @before << context.envelope.id
  def after_confirm(context) = @confirmed << context.envelope.id
  def on_error(context, failure) = @errors << [context.payload, failure]
end

# The same, and it refuses one message of the batch.
class BatchRefusingInterceptor < BatchRecordingInterceptor
  def initialize(word)
    super()
    @word = word
  end

  def before_publish(context)
    super
    raise "#{@word} is not allowed" if context.payload["word"] == @word
  end
end

# What `publish_all` promises: everything goes out before anything is waited
# for, the envelopes come back in the order the payloads were given, and a batch
# that half succeeded says so with counts.
#
# Most of these run against a broker that answers a message only when the test
# says so, one at a time. {FakeTransport} confirms as it is called, which cannot
# tell a pipelined batch from a loop of single publishes — both pass — so the
# round trip per message this method exists to avoid would be able to come back
# unnoticed. A broker that holds every confirm until the whole batch has arrived
# can only be satisfied by the pipelined one.
RSpec.describe "AceMQ::AMQP::Connection#publish_all" do
  let(:broker) { HeldConfirmsTransport.new }
  let(:mq) { AceMQ::AMQP::Connection.new(transport: broker, origin: "checkout@pod-7") }
  let(:payloads) { %w[one two three four five].map { |word| { "word" => word } } }

  after { @batches&.each(&:kill) }

  # Started on a thread of its own, because the broker answers nothing until
  # this test tells it to — which is the whole point of that broker.
  def batch(**options)
    thread = Thread.new { mq.publish_all(payloads, to: "order.placed", **options) }
    thread.report_on_exception = false
    (@batches ||= []) << thread
    thread
  end

  # The failure a batch came to, as an object rather than as a matcher block:
  # the sentence is the contract here, and there are several things to read in
  # it.
  def failure
    yield
    raise "the batch was expected to fail and did not"
  rescue AceMQ::AMQP::PublishError => e
    e
  end

  it "returns the envelopes in payload order whatever order the confirms arrive in" do
    sending = batch
    broker.wait_until_sent(payloads.size)

    # Backwards, so that a list built in the order the broker answered would
    # come back reversed rather than accidentally right.
    (payloads.size - 1).downto(0) { |index| broker.confirm(index) }
    envelopes = sending.value

    expect(envelopes.size).to eq(payloads.size)
    expect(envelopes.map(&:id)).to eq(broker.sent.map(&:message_id))
    expect(broker.bodies).to eq(payloads.map { |payload| JSON.generate(payload) })
  end

  it "publishes every message before waiting for any confirm" do
    sending = batch

    # Nothing has been answered yet, so a call that waited for each confirm in
    # turn would still be sitting on its first message. This line returning at
    # all is the assertion: five messages are on the wire with nothing
    # confirmed.
    broker.wait_until_sent(payloads.size)
    payloads.each_index { |index| broker.confirm(index) }

    expect(sending.value.size).to eq(payloads.size)
  end

  it "waits for the rest of the batch after a failure, and says how many arrived" do
    sending = batch
    broker.wait_until_sent(payloads.size)

    broker.confirm(0)
    broker.confirm(1)
    broker.refuse(2, "the queue is full")

    # The failure has been seen and the last two are still outstanding. Giving
    # up here is what loses the count, so the batch must still be waiting.
    sleep(0.05)
    expect(sending).to be_alive

    broker.confirm(3)
    broker.confirm(4)
    met = failure { sending.value }

    expect(met.message).to start_with("1 of 5 messages were not confirmed; 4 were.")
    expect(met.message).to include("The first failure was: the queue is full")
  end

  it "counts every failure, and quotes the first one in payload order" do
    sending = batch
    broker.wait_until_sent(payloads.size)

    # Answered out of order and refused out of order: the failure named has to
    # be the first payload that failed, not the first failure the broker got
    # round to.
    broker.refuse(3, "the third refusal")
    broker.confirm(4)
    broker.refuse(1, "the first refusal")
    broker.confirm(0)
    broker.confirm(2)
    met = failure { sending.value }

    expect(met.message).to start_with("2 of 5 messages were not confirmed; 3 were.")
    expect(met.message).to end_with("The first failure was: the first refusal")
  end

  it "reports an unroutable message in a batch the way a single publish does" do
    sending = batch(mandatory: true)
    broker.wait_until_sent(payloads.size)

    broker.confirm(0)
    broker.unroutable(1, "312 NO_ROUTE")
    broker.confirm(2)
    broker.unroutable(3, "312 NO_ROUTE")
    broker.confirm(4)
    met = failure { sending.value }

    expect(met.message).to start_with("2 of 5 messages were not confirmed; 3 were.")
    expect(met.message).to include("312 NO_ROUTE")
    # Every failure was a message the broker had nowhere to put, so the batch is
    # one too, and a caller rescuing `unroutable?` to mean "nothing is bound" is
    # right about this batch.
    expect(met.unroutable?).to be(true)
    expect(broker.sent.map(&:mandatory)).to all(be(true))
  end

  it "is not an unroutable batch when only part of it was unroutable" do
    # A missing binding and a broker that would not take a message are fixed in
    # different places, and a batch that met both is not the first one.
    sending = batch(mandatory: true)
    broker.wait_until_sent(payloads.size)

    broker.unroutable(0, "312 NO_ROUTE")
    broker.refuse(1, "the queue is full")
    (2..4).each { |index| broker.confirm(index) }

    expect(failure { sending.value }.unroutable?).to be(false)
  end

  it "sends nothing at all for an empty batch" do
    expect(mq.publish_all([], to: "order.placed")).to eq([])
    expect(broker.sent).to be_empty
  end

  describe "against a broker that answers as it is asked" do
    let(:transport) { FakeTransport.new }
    let(:metrics) { AceMQ::AMQP::Telemetry::Registry.new }
    let(:mq) do
      AceMQ::AMQP::Connection.new(transport: transport, origin: "checkout@pod-7",
                                  telemetry: metrics)
    end

    it "runs the publish interceptors once for each message" do
      interceptor = BatchRecordingInterceptor.new
      mq.intercept_publish(interceptor)

      envelopes = mq.publish_all(payloads, to: "order.placed", exchange: "orders-events")

      expect(interceptor.before).to eq(envelopes.map(&:id))
      expect(interceptor.confirmed).to eq(envelopes.map(&:id))
      expect(interceptor.before.uniq.size).to eq(payloads.size)
    end

    it "lets an interceptor stamp each message, and sends what it left" do
      counter = 0
      mq.intercept_publish { |context| context.set_header("seq", counter += 1) }

      mq.publish_all(payloads, to: "order.placed")

      expect(transport.published.map { |m| m.headers["seq"] }).to eq([1, 2, 3, 4, 5])
    end

    it "tells the interceptors about a message it refused, and sends the rest" do
      interceptor = BatchRefusingInterceptor.new("three")
      mq.intercept_publish(interceptor)

      met = failure { mq.publish_all(payloads, to: "order.placed") }

      expect(met.message).to start_with("1 of 5 messages were not confirmed; 4 were.")
      # The message an interceptor refused never went and the other four did:
      # one failure out of five, not the end of the batch.
      expect(transport.published.size).to eq(4)
      expect(interceptor.errors.map(&:first)).to eq([{ "word" => "three" }])
      expect(interceptor.confirmed.size).to eq(4)
    end

    it "counts each message of a batch on its own, like a publish of its own" do
      transport.refuse!("order.placed")

      expect { mq.publish_all(payloads, to: "order.placed", exchange: "orders-events") }
        .to raise_error(AceMQ::AMQP::PublishError)
      mq.publish_all(payloads.first(2), to: "orders.new")

      total = AceMQ::AMQP::Telemetry::PUBLISH_TOTAL
      expect(metrics[total, exchange: "orders-events",
                            outcome: AceMQ::AMQP::Telemetry::Outcome::FAILED]).to eq(5)
      expect(metrics[total, exchange: "",
                            outcome: AceMQ::AMQP::Telemetry::Outcome::CONFIRMED]).to eq(2)
    end

    it "builds one envelope per payload, and takes envelopes of its own" do
      envelopes = mq.publish_all(payloads, to: "order.placed", type: "order.placed.v2")
      expect(envelopes.map(&:id).uniq.size).to eq(payloads.size)
      expect(envelopes.map(&:type)).to all(eq("order.placed.v2"))

      mine = payloads.map { AceMQ::AMQP::Envelope.new(correlation_id: "corr-9") }
      sent = mq.publish_all(payloads, to: "order.placed", envelopes: mine)
      expect(sent.map(&:correlation_id)).to all(eq("corr-9"))
    end

    it "refuses a batch it cannot make sense of, before anything is sent" do
      expect { mq.publish_all({ "word" => "one" }, to: "q") }
        .to raise_error(ArgumentError, /rather than an array of payloads/)
      expect { mq.publish_all(payloads, to: "q", envelopes: [AceMQ::AMQP::Envelope.new]) }
        .to raise_error(ArgumentError, /one envelope for each payload/)
      expect { mq.publish_all(payloads, to: "q", envelopes: [], type: "x") }
        .to raise_error(ArgumentError, /one or the other/)
      expect(transport.published).to be_empty
    end
  end
end
