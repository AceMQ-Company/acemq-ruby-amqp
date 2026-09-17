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

# The one spec that needs bunny itself, because the thing under test is how this
# library reads bunny's confirms: the delivery tags in `nacked_set` and
# `unconfirmed_set`, and the `basic.return` a mandatory publish comes back with.
# A double for that has to be a bunny channel rather than a transport.
require "bunny"

# A bunny channel that answers confirms only when this test says so.
#
# Enough of one for {AceMQ::AMQP::Transport} to publish down: it hands out
# delivery tags the way bunny does, records the order it was called in, and
# holds `wait_for_confirms` open until the test answers. That last part is what
# tells a batch that pipelines from a loop that does not — a loop would be
# waiting inside its first message while the other four were still unwritten.
class FakeChannel
  Sent = Struct.new(:body, :exchange, :routing_key, :options, keyword_init: true)
  Returned = Struct.new(:reply_code, :reply_text)

  # What was published and what was waited for, in the order it happened.
  attr_reader :calls, :sent, :nacked_set, :unconfirmed_set

  def initialize
    @calls = []
    @sent = []
    @tags = []
    @nacked_set = Set.new
    @unconfirmed_set = Set.new
    @next_publish_seq_no = 0
    @answers = Thread::Queue.new
    @refused = []
    @exchanges = {}
    @error = nil
    @open = true
    @lock = Mutex.new
  end

  def open? = @lock.synchronize { @open }
  def close = nil
  def register_exchange(exchange) = @exchanges[exchange.name] = exchange
  def next_publish_seq_no = @lock.synchronize { @next_publish_seq_no }

  # The broker took the channel down, the way it does for an unroutable
  # declaration or a connection that dropped. The transport notices on its next
  # publish and opens another — this same object, since a session hands out one.
  def closed! = @lock.synchronize { @open = false }

  def confirm_select(_callback = nil)
    @lock.synchronize do
      @open = true
      @next_publish_seq_no = 1 if @next_publish_seq_no.zero?
    end
    nil
  end

  # bunny's own signature: the options are a positional hash, not keywords.
  def basic_publish(body, exchange, routing_key, options = {})
    # Raised before a sequence number is taken, which is where bunny raises for
    # a channel that has been closed or a routing key that is too long.
    if @refused.include?(options[:message_id])
      raise ArgumentError, "the channel would not take it"
    end

    @lock.synchronize do
      @unconfirmed_set.add(@next_publish_seq_no)
      @tags << @next_publish_seq_no
      @next_publish_seq_no += 1
      @sent << Sent.new(body: body, exchange: exchange, routing_key: routing_key,
                        options: options)
      @calls << :publish
    end
    self
  end

  def wait_for_confirms
    @lock.synchronize { @calls << :wait }
    @answers.pop
    raise @error if @error

    @lock.synchronize { @nacked_set.empty? }
  end

  # Answers everything published so far. +nack+ and +silent+ are positions among
  # the messages that were published, which is what the transport has to map
  # back from delivery tags.
  #
  # A real wait does not come back while anything is still unconfirmed — it
  # raises once bunny's continuation timeout is up — so +silent+ comes with the
  # error that wait really ended with.
  def answer!(nack: [], silent: [], error: nil)
    @lock.synchronize do
      nack.each { |position| @nacked_set.add(@tags.fetch(position)) }
      @unconfirmed_set.replace(silent.to_set { |position| @tags.fetch(position) })
      @error = error
    end
    @answers.push(true)
  end

  # Hands a message back the way a broker with nowhere to route it does, which
  # happens before the confirm rather than after it.
  def return!(message_id, exchange:, code: 312, text: "NO_ROUTE")
    registered = @exchanges.fetch(exchange)
    registered.handle_return(Returned.new(code, text), { message_id: message_id }, nil)
  end

  # Refuses to take this message at all, without consuming a delivery tag.
  def refuse!(*message_ids) = @refused.concat(message_ids)

  def wait_until_sent(count, timeout: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    while @lock.synchronize { @sent.size } < count
      if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        raise "only #{@sent.size} of #{count} messages were published within #{timeout}s"
      end

      sleep(0.005)
    end
    nil
  end
end

# One channel, handed out however often it is asked for.
class FakeSession
  def initialize(channel) = @channel = channel
  def create_channel(*) = @channel
  def open? = true
  def close = nil
end

RSpec.describe AceMQ::AMQP::Transport do
  subject(:transport) { described_class.new(FakeSession.new(channel)) }

  let(:channel) { FakeChannel.new }

  after { @batches&.each(&:kill) }

  def messages(count, **extra)
    (1..count).map do |n|
      { exchange: "orders-events", routing_key: "order.placed", body: %({"n":#{n}}),
        content_type: "application/json", message_id: "m-#{n}", headers: {},
        persistent: true, **extra }
    end
  end

  def batch(outgoing, using: transport)
    thread = Thread.new { using.publish_all(outgoing) }
    thread.report_on_exception = false
    (@batches ||= []) << thread
    thread
  end

  # A transport whose ceiling is small enough to reach in a test. Everything
  # about it is the ordinary one; only the number differs.
  def bounded(limit)
    described_class.new(FakeSession.new(channel), max_outstanding_publishes: limit)
  end

  it "hands every message to the broker before it waits for any confirm" do
    sending = batch(messages(5))

    # Nothing has been answered, so a publish that waited for its own confirm
    # would still be inside the first message and this would time out. That it
    # returns is the assertion.
    channel.wait_until_sent(5)
    # Five publishes with no wait among them. The batch is free to have reached
    # its own single wait by now, which is what the count below is about.
    expect(channel.calls.first(5)).to eq([:publish] * 5)

    channel.answer!
    expect(sending.value).to eq(%w[m-1 m-2 m-3 m-4 m-5])
    # One wait for the batch, at the end, rather than one per message.
    expect(channel.calls.count(:wait)).to eq(1)
    expect(channel.calls.last).to eq(:wait)
  end

  it "names the messages the broker refused, in payload order" do
    sending = batch(messages(5))
    channel.wait_until_sent(5)
    channel.answer!(nack: [3, 1])
    results = sending.value

    expect(results.map { |result| result.is_a?(AceMQ::AMQP::PublishError) })
      .to eq([false, true, false, true, false])
    expect(results[1].message)
      .to eq("the broker would not confirm message m-2 on exchange " \
             '"orders-events" with key "order.placed"')
    expect(results[1].unroutable?).to be(false)
    expect(results.values_at(0, 2, 4)).to eq(%w[m-1 m-3 m-5])
  end

  it "counts a confirm that never came as a message that did not arrive" do
    sending = batch(messages(3))
    channel.wait_until_sent(3)
    channel.answer!(silent: [2], error: Timeout::Error.new("the confirm never came"))
    results = sending.value

    expect(results.first(2)).to eq(%w[m-1 m-2])
    expect(results[2]).to be_a(AceMQ::AMQP::PublishError)
    expect(results[2].message).to include("would not confirm message m-3")
    # Which absence it was: the broker said no, or nothing was ever said.
    expect(results[2].message).to end_with("the confirm never came")
  end

  it "does not blame a batch for a nack from a publish that finished before it" do
    # bunny never empties nacked_set, so the tags of everything ever refused on
    # this channel are still in it. Only this batch's tags are looked up there.
    single = Thread.new { transport.publish(**messages(1).first) }
    single.report_on_exception = false
    channel.wait_until_sent(1)
    channel.answer!(nack: [0])
    expect { single.value }.to raise_error(AceMQ::AMQP::PublishError)

    sending = batch(messages(2))
    channel.wait_until_sent(3)
    channel.answer!

    expect(sending.value).to eq(%w[m-1 m-2])
  end

  it "reports an unroutable message in a batch the way a single publish does" do
    sending = batch(messages(3, mandatory: true))
    channel.wait_until_sent(3)
    # The broker hands a message back before it confirms it, and bunny dispatches
    # the frames in that order.
    channel.return!("m-2", exchange: "orders-events")
    channel.answer!
    results = sending.value

    expect(results.values_at(0, 2)).to eq(%w[m-1 m-3])
    expect(results[1]).to be_a(AceMQ::AMQP::PublishError)
    expect(results[1].unroutable?).to be(true)
    expect(results[1].message)
      .to eq("the broker had nowhere to route message m-2 published to exchange " \
             '"orders-events" with key "order.placed": 312 NO_ROUTE')
  end

  it "keeps the tags lined up when a message is refused before it is sent" do
    # A publish that never reached the broker consumed no delivery tag, so the
    # tag the message after it was given belongs to that one. Read the tags
    # before the publishes rather than after and this is the test that says so:
    # the failure would be reported against m-3.
    channel.refuse!("m-2")
    sending = batch(messages(3))
    channel.wait_until_sent(2)
    channel.answer!
    results = sending.value

    expect(results.values_at(0, 2)).to eq(%w[m-1 m-3])
    expect(results[1]).to be_a(AceMQ::AMQP::PublishError)
    expect(results[1].message).to start_with("cannot publish message m-2 to exchange")
  end

  it "publishes what it was given, with the properties a single publish uses" do
    sending = batch(messages(2, reply_to: "replies", headers: { tenant: "acme" }))
    channel.wait_until_sent(2)
    channel.answer!
    sending.value

    sent = channel.sent.first
    expect(sent.body).to eq('{"n":1}')
    expect(sent.exchange).to eq("orders-events")
    expect(sent.routing_key).to eq("order.placed")
    expect(sent.options).to include(content_type: "application/json", message_id: "m-1",
                                    reply_to: "replies", persistent: true, mandatory: false)
    # Header names go to the broker as strings, as everywhere else here.
    expect(sent.options[:headers]).to eq("tenant" => "acme")
  end

  it "sends nothing for an empty batch" do
    expect(transport.publish_all([])).to eq([])
    expect(channel.calls).to be_empty
  end

  describe "how many publishes may be unconfirmed at once" do
    it "bounds a connection at a thousand, the number Java and .NET bound at" do
      expect(transport.max_outstanding_publishes).to eq(1_000)
    end

    it "refuses a ceiling nothing could ever publish under" do
      expect { bounded(0) }
        .to raise_error(AceMQ::AMQP::ConfigurationError, /must be at least 1, was 0/)
    end

    it "writes a batch in waves of the ceiling rather than all of it at once" do
      sending = batch(messages(5), using: bounded(2))

      # Two messages, then a wait — not five and one wait. Without the ceiling
      # the whole batch is on the wire before anything is confirmed, which is
      # exactly the unbounded buffer this is here to prevent.
      channel.wait_until_sent(2)
      channel.answer!
      channel.wait_until_sent(4)
      channel.answer!
      channel.wait_until_sent(5)
      channel.answer!

      expect(sending.value).to eq(%w[m-1 m-2 m-3 m-4 m-5])
      expect(channel.calls)
        .to eq(%i[publish publish wait publish publish wait publish wait])
    end

    it "keeps payload order and return attribution across the waves" do
      sending = batch(messages(4, mandatory: true), using: bounded(2))
      channel.wait_until_sent(2)
      channel.answer!
      channel.wait_until_sent(4)
      # A message of the *second* wave comes back. Reading the returns once for
      # the whole batch would charge this to the first mandatory message with
      # nothing against it yet, which is m-1.
      channel.return!("m-4", exchange: "orders-events")
      channel.answer!
      results = sending.value

      expect(results.values_at(0, 1, 2)).to eq(%w[m-1 m-2 m-3])
      expect(results[3]).to be_a(AceMQ::AMQP::PublishError)
      expect(results[3].unroutable?).to be(true)
      expect(results[3].message).to include("nowhere to route message m-4")
    end

    it "tells the caller the broker is not keeping up when a wave frees nothing" do
      sending = batch(messages(4), using: bounded(2))
      channel.wait_until_sent(2)
      # Neither message was ever answered for, so both are still outstanding as
      # far as the broker is concerned and neither gives its room back.
      channel.answer!(silent: [0, 1], error: Timeout::Error.new("the confirm never came"))
      results = sending.value

      # Only one wave was ever written: the rest was refused rather than
      # buffered behind a broker that had stopped answering.
      expect(channel.calls.count(:publish)).to eq(2)
      expect(results).to all(be_a(AceMQ::AMQP::PublishError))
      expect(results.first.message).to include("would not confirm message m-1")
      expect(results.last.message)
        .to eq("cannot publish message m-4 to exchange \"orders-events\" with key " \
               "\"order.placed\": 2 publishes are already waiting for a confirm and " \
               "none of them completed. The broker is not keeping up; publish more " \
               "slowly rather than buffering more.")
    end

    it "bounds a single publish against the same ceiling" do
      mq = bounded(1)
      # The confirm for this one never comes, so its room is never given back.
      first = Thread.new { mq.publish(**messages(1).first) }
      first.report_on_exception = false
      channel.wait_until_sent(1)
      channel.answer!(silent: [0], error: Timeout::Error.new("the confirm never came"))
      expect { first.value }.to raise_error(AceMQ::AMQP::PublishError)

      expect { mq.publish(**messages(1).first) }
        .to raise_error(AceMQ::AMQP::PublishError, /The broker is not keeping up/)
      # Nothing more went on the wire, which is the point of saying so.
      expect(channel.calls.count(:publish)).to eq(1)
    end

    it "gives the room back when the publishing channel is reopened" do
      mq = bounded(1)
      first = Thread.new { mq.publish(**messages(1).first) }
      first.report_on_exception = false
      channel.wait_until_sent(1)
      channel.answer!(silent: [0], error: Timeout::Error.new("the confirm never came"))
      expect { first.value }.to raise_error(AceMQ::AMQP::PublishError)

      # The unconfirmed message went down with the old channel and can never be
      # confirmed on the new one. Holding its room would shrink the ceiling by
      # one at every reconnect until publishing stopped altogether.
      channel.closed!
      sending = Thread.new { mq.publish(**messages(1).first) }
      channel.wait_until_sent(2)
      channel.answer!
      expect(sending.value).to eq("m-1")
    end
  end
end
