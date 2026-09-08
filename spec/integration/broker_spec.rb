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

require "fileutils"
require "tmpdir"

require "acemq/amqp"
require "acemq/amqp/patterns"

# Everything the unit specs cannot prove, and nothing they can.
#
# The retry arithmetic is tested against a fake transport because it is
# arithmetic. What is left needs a broker and nothing else will do: that the
# headers survive a round trip through RabbitMQ's field table with their types
# intact, that a nack really does come back marked redelivered, and that a
# message which runs out of attempts really does end up on a queue somebody can
# go and look at.
#
# Every name here starts with rbit. so that a broker shared with anything else
# is left alone.
RSpec.describe "against a real broker", :integration do
  PREFIX = "rbit."
  BROKER = ENV.fetch("ACEMQ_TEST_BROKER", "amqp://guest:guest@localhost:5672")

  # Named per example group so two of them cannot collide on a queue, which is
  # the kind of failure that only shows up when the suite is run in a different
  # order.
  def queue_named(suffix) = "#{PREFIX}#{suffix}"

  let(:mq) { AceMQ::AMQP::Connection.open(BROKER, origin: "rspec@rbit") }

  after { mq.close }

  # Polling rather than a condition variable: the thing being waited on happens
  # on bunny's threads, and a test that deadlocks tells you nothing at all.
  def wait_for(seconds: 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    loop do
      found = yield
      return found if found
      break if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep(0.05)
    end
    nil
  end

  # Takes one message off a queue without a consumer, which is how a test looks
  # at a dead-letter queue nothing is draining.
  def take_one(queue, seconds: 10)
    channel = mq.transport.session.create_channel
    wait_for(seconds: seconds) do
      _info, properties, body = channel.basic_get(queue, manual_ack: false)
      properties && [properties, body]
    end
  ensure
    channel&.close
  end

  def scrub(*queues)
    queues.each { |name| mq.delete_queue(name) }
  rescue StandardError
    # A queue that is not there is the state this wanted anyway.
    nil
  end

  # One argument to a line, sorted by name, which is what makes a printed table
  # something somebody can hold beside the Java, Go, .NET or Python one.
  def argument_lines(queue)
    return "      (no arguments)" if queue.arguments.empty?

    queue.arguments.sort_by { |name, _| name.to_s }
         .map { |name, value| "      #{name.to_s.ljust(26)} #{value.inspect}" }.join("\n")
  end

  describe "a round trip" do
    let(:queue) { queue_named("roundtrip") }

    before do
      scrub(queue)
      AceMQ::AMQP::Topology.new.queue(queue).apply(mq)
    end

    after { scrub(queue) }

    it "brings the envelope back the way it went out" do
      received = []
      mq.consume(queue) do |message|
        received << message
        AceMQ::AMQP::Ack.accept
      end

      sent = mq.publish({ "order_id" => "A-1", "total_cents" => 250 },
                        to: queue, type: "order.placed.v2", version: 3,
                        correlation_id: "corr-9", causation_id: "cause-8",
                        headers: { "tenant" => "acme" })

      wait_for { received.any? }
      expect(received.size).to eq(1)

      message = received.first
      expect(message.payload).to eq({ "order_id" => "A-1", "total_cents" => 250 })
      expect(message.content_type).to eq("application/json")
      expect(message.routing_key).to eq(queue)

      envelope = message.envelope
      expect(envelope.id).to eq(sent.id)
      expect(envelope.type).to eq("order.placed.v2")
      expect(envelope.version).to eq(3)
      expect(envelope.correlation_id).to eq("corr-9")
      expect(envelope.causation_id).to eq("cause-8")
      expect(envelope.origin).to eq("rspec@rbit")
      expect(envelope.attempt).to eq(1)
      expect(envelope.headers).to eq({ "tenant" => "acme" })
      # Epoch milliseconds, not a broker timestamp and not seconds: the whole
      # point of pinning the type is that a Java consumer reads the same number.
      expect(envelope.first_seen.to_f).to be_within(1.0).of(sent.first_seen.to_f)
    end
  end

  describe "a handler that keeps failing" do
    let(:queue) { queue_named("retry") }
    let(:dlq) { AceMQ::AMQP::Naming.dead_letter_queue(queue) }

    before do
      scrub(queue, dlq)
      # dead_letter_queue rather than queue(dlq): a dead-letter queue is classic
      # in every one of the five libraries, and declaring it here as an ordinary
      # queue would now make it quorum and disagree with all of them.
      AceMQ::AMQP::Topology.new.queue(queue).dead_letter_queue(queue).apply(mq)
    end

    after { scrub(queue, dlq) }

    it "really does retry, and then really does dead-letter with the reason" do
      attempts = []
      mq.consume(queue, retry_policy: AceMQ::AMQP::RetryPolicy.fixed(3, 0.05)) do |message|
        attempts << message.attempt
        AceMQ::AMQP::Ack.retry("the warehouse is down")
      end

      sent = mq.publish({ "order_id" => "A-2" }, to: queue, type: "order.placed.v2")

      wait_for { attempts.size >= 3 }
      # Three deliveries, and the count came off the wire: each retry was
      # republished with the attempt advanced, so the number survives leaving
      # this process. The broker's redelivery flag is false every time for the
      # same reason — a republished retry is a new delivery, not a requeue.
      expect(attempts).to eq([1, 2, 3])

      properties, body = take_one(dlq)
      expect(properties).not_to be_nil
      expect(body).to eq('{"order_id":"A-2"}')

      headers = properties[:headers]
      expect(headers[AceMQ::AMQP::Headers::ID]).to eq(sent.id)
      expect(headers[AceMQ::AMQP::Headers::ATTEMPT]).to eq(3)
      expect(headers[AceMQ::AMQP::Headers::ERROR])
        .to eq("gave up after 3 attempts: the warehouse is down")
      # The source queue is empty: the original was acknowledged once the copy
      # was safely somewhere else.
      expect(wait_for { mq.message_count(queue).zero? }).to be(true)
    end
  end

  describe "a retry too long to wait for" do
    # Two seconds, and a threshold of one, so the example runs in about the time
    # it takes to read it. Thirty is the default and the arithmetic either side
    # of it is the same.
    let(:queue) { queue_named("rung") }
    let(:dlq) { AceMQ::AMQP::Naming.dead_letter_queue(queue) }
    let(:rung) { AceMQ::AMQP::Naming.retry_queue(queue, 2) }
    let(:policy) { AceMQ::AMQP::RetryPolicy.fixed(3, 2) }
    let(:ladder) { AceMQ::AMQP::RetryLadder.for(queue, policy, threshold: 1) }

    before do
      scrub(queue, dlq, rung)
      AceMQ::AMQP::Topology.new
                           .queue(queue, retry_policy: policy, retry_threshold: 1)
                           .dead_letter_queue(queue)
                           .apply(mq)
    end

    # The exchange is shared and conventional — every AceMQ library declares
    # `acemq.retry` by that name — so it is left where it is. The binding goes
    # with the queue: RabbitMQ removes a queue's bindings when the queue is
    # deleted, so scrubbing the queues leaves nothing of this behind.
    after { scrub(queue, dlq, rung) }

    it "declares the rung the way every other library declares it" do
      # Printed rather than only asserted, because the point of this table is
      # that somebody can hold it beside the Go, .NET, Java and Python ones and
      # see that they are the same.
      arguments = ladder.rungs.first.arguments
      puts <<~RUNG
        --- rung declaration, as this library builds it ---
          exchange  #{AceMQ::AMQP::Naming::RETRY_EXCHANGE} (direct, durable)
          queue     #{ladder.rungs.first.queue} (classic, durable)
                      x-message-ttl              #{arguments["x-message-ttl"].inspect}
                      x-dead-letter-exchange     #{arguments["x-dead-letter-exchange"].inspect}
                      x-dead-letter-routing-key  #{arguments["x-dead-letter-routing-key"].inspect}
          binding   #{queue} -> #{AceMQ::AMQP::Naming::RETRY_EXCHANGE} -> #{queue}
          dead letters
                    #{dlq} -> #{AceMQ::AMQP::Naming::DEAD_LETTER_EXCHANGE} -> #{dlq}
                    #{AceMQ::AMQP::Naming.parked_queue(queue)} -> \
        #{AceMQ::AMQP::Naming::DEAD_LETTER_EXCHANGE} -> #{AceMQ::AMQP::Naming.parked_queue(queue)}
        ---------------------------------------------------
      RUNG

      expect(arguments).to eq(
        "x-message-ttl" => 2000,
        "x-dead-letter-exchange" => "acemq.retry",
        "x-dead-letter-routing-key" => queue
      )

      # The broker agrees, which a plan on paper cannot show: redeclaring the
      # rung with this exact table is accepted, and redeclaring it with the old
      # one — the default exchange — is refused. That refusal is the whole
      # reason the table has to be identical in five languages.
      #
      # queue_type: :classic is said out loud because declare_queue defaults to
      # quorum, and a rung is one of the queues that stays classic. Left off, it
      # is this refusal all over again with x-queue-type as the argument that
      # differs — which is exactly what a hand-written redeclaration of a rung
      # would hit after upgrading.
      mq.declare_queue(rung, queue_type: :classic, durable: true, arguments: arguments)
      expect do
        mq.declare_queue(rung, queue_type: :classic, durable: true,
                               arguments: arguments.merge("x-dead-letter-exchange" => ""))
      end.to raise_error(AceMQ::AMQP::TransportError, /PRECONDITION_FAILED/i)
      expect { mq.declare_queue(rung, durable: true, arguments: arguments) }
        .to raise_error(AceMQ::AMQP::TransportError, /x-queue-type/i)
    end

    it "routes a message home through the retry exchange and nothing else" do
      # The binding on its own, with no time-to-live and no consumer involved:
      # publishing to acemq.retry under the source queue's name reaches the
      # source queue. A rung with this binding missing would look perfectly
      # healthy right up to the moment the broker silently dropped the message.
      mq.transport.publish(exchange: AceMQ::AMQP::Naming::RETRY_EXCHANGE,
                           routing_key: queue, body: '{"order_id":"A-7"}',
                           content_type: "application/json")

      expect(wait_for { mq.message_count(queue) == 1 }).to be(true)
    end

    it "waits in the broker, and the broker brings it back with the attempt advanced" do
      attempts = []
      consumer = mq.consume(queue, retry_policy: policy, retry_threshold: 1) do |message|
        attempts << message.attempt
        AceMQ::AMQP::Ack.retry("the warehouse is down")
      end

      published = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      mq.publish({ "order_id" => "A-5" }, to: queue, type: "order.placed.v2")

      expect(wait_for { mq.message_count(rung) == 1 }).to be(true)
      expect(attempts).to eq([1])

      # The consumer is closed before anything is counted, so a zero on the
      # source queue is the broker's own count and not a message this process
      # happens to be holding. This is the difference the whole design turns on:
      # were the consumer sleeping on an unacknowledged message instead,
      # cancelling it here would return the message at once and the two-second
      # backoff would be nothing.
      consumer.cancel
      expect(mq.message_count(rung)).to eq(1)
      expect(mq.message_count(queue)).to eq(0)

      # Nobody woke it up and nothing is attached. The rung's time-to-live
      # expired, and its dead-letter exchange and routing key carried the
      # message back through acemq.retry to the queue it came from.
      expect(wait_for(seconds: 15) { mq.message_count(queue) == 1 }).to be(true)
      expect(mq.message_count(rung)).to eq(0)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - published).to be >= 2

      properties, body = take_one(queue)
      expect(body).to eq('{"order_id":"A-5"}')
      expect(properties[:headers][AceMQ::AMQP::Headers::ATTEMPT]).to eq(2)
      # RabbitMQ writes its own account of the journey: it came out of the rung,
      # and it came out because its time ran out rather than because anything
      # rejected it.
      death = properties[:headers]["x-death"]&.first
      expect(death["queue"]).to eq(rung)
      expect(death["reason"]).to eq("expired")
    end
  end

  describe "an interceptor" do
    let(:queue) { queue_named("intercepted") }
    let(:dlq) { AceMQ::AMQP::Naming.dead_letter_queue(queue) }

    before do
      scrub(queue, dlq)
      AceMQ::AMQP::Topology.new.queue(queue).dead_letter_queue(queue).apply(mq)
    end

    after { scrub(queue, dlq) }

    it "stamps on the way out and on the way in, and both survive the wire" do
      # The unit specs prove the seam runs. What only a broker can show is that
      # what an interceptor put on an envelope goes through RabbitMQ's field
      # table and comes back out the other side — and that a header stamped on
      # the way in is still there on the dead letter, which is where somebody
      # actually goes looking for it.
      mq.intercept_publish { |context| context.set_header("tenant", "acme") }
      mq.intercept_consume { |context| context.set_header("handled-by", "rspec") }

      mq.consume(queue, retry_policy: AceMQ::AMQP::RetryPolicy.none) do |message|
        expect(message.envelope.headers["tenant"]).to eq("acme")
        AceMQ::AMQP::Ack.retry("the warehouse is down")
      end

      mq.publish({ "order_id" => "A-8" }, to: queue, type: "order.placed.v2")

      properties, = take_one(dlq)
      expect(properties).not_to be_nil
      expect(properties[:headers]["tenant"]).to eq("acme")
      expect(properties[:headers]["handled-by"]).to eq("rspec")
      expect(properties[:headers][AceMQ::AMQP::Headers::ERROR])
        .to match(/the warehouse is down/)
    end
  end

  describe "a body nothing can read" do
    let(:queue) { queue_named("undecodable") }
    let(:parked) { AceMQ::AMQP::Naming.parked_queue(queue) }

    before do
      scrub(queue, parked)
      AceMQ::AMQP::Topology.new.queue(queue).parked_queue(queue).apply(mq)
    end

    after { scrub(queue, parked) }

    it "parks it with the reason rather than retrying it forever" do
      seen = []
      mq.consume(queue, retry_policy: AceMQ::AMQP::RetryPolicy.fixed(10, 0)) do |message|
        seen << message
        AceMQ::AMQP::Ack.accept
      end

      # Published as bytes so the broker carries something the JSON codec on
      # the consuming side cannot possibly read.
      mq.publish("{ not json at all", to: queue, codec: AceMQ::AMQP::BytesCodec.new)

      properties, body = take_one(parked)
      expect(body).to eq("{ not json at all")
      expect(properties[:headers][AceMQ::AMQP::Headers::ERROR]).to match(/could not be decoded/)
      expect(seen).to be_empty
    end
  end

  describe "a topology" do
    let(:queue) { queue_named("topology") }
    let(:dlq) { AceMQ::AMQP::Naming.dead_letter_queue(queue) }
    let(:exchange) { queue_named("topology.events") }
    let(:dlx) { queue_named("dlx") }

    before { scrub(queue, dlq) }

    after do
      scrub(queue, dlq)
      mq.transport.delete_exchange(exchange)
      mq.transport.delete_exchange(dlx)
    end

    it "declares what it said it would, and the broker accepts the wiring" do
      AceMQ::AMQP::Topology.new(dead_letter_exchange: dlx)
                           .exchange(exchange, :topic)
                           .queue(queue, dead_letter: true)
                           .binding(queue, exchange, "order.#")
                           .apply(mq)

      expect(mq.queue_exists?(queue)).to be(true)
      expect(mq.queue_exists?(dlq)).to be(true)

      # Routed by the topic binding rather than by queue name, which is the bit
      # a plan on paper cannot prove.
      mq.publish({ "order_id" => "A-3" }, to: "order.placed", exchange: exchange)
      expect(wait_for { mq.message_count(queue).positive? }).to be(true)
    end

    it "is refused by the broker when it disagrees with what is already there" do
      # PRECONDITION_FAILED is the only way AMQP reports drift without the
      # management API, and passing it on is the point: it means this service
      # and the broker disagree about what the queue is.
      AceMQ::AMQP::Topology.new(dead_letter_exchange: dlx).queue(queue).apply(mq)

      expect { mq.declare_queue(queue, durable: false) }
        .to raise_error(AceMQ::AMQP::TransportError, /PRECONDITION_FAILED/i)
    end
  end

  # The one thing that stops a Java service and a Ruby service sharing a queue.
  #
  # RabbitMQ treats x-queue-type as part of a queue's identity, so this is not a
  # preference each library gets to hold on its own: the second service to
  # declare a queue whose type it disagrees about is answered
  # PRECONDITION_FAILED and consumes nothing at all. Java declares a source
  # queue quorum, so all five do.
  describe "the queue type five libraries have to agree on" do
    let(:queue) { queue_named("quorum") }
    let(:dlq) { AceMQ::AMQP::Naming.dead_letter_queue(queue) }
    let(:parked) { AceMQ::AMQP::Naming.parked_queue(queue) }
    let(:rung) { AceMQ::AMQP::Naming.retry_queue(queue, 2) }
    let(:policy) { AceMQ::AMQP::RetryPolicy.fixed(3, 2) }

    # What a Java service writes for the same queue: Topology.Builder.queue is a
    # durable quorum queue, and queueWithDeadLetter adds the two dead-letter
    # arguments. Held here as a literal rather than built from this library's
    # own code, because a table generated by the thing under test proves nothing
    # about the other four.
    let(:java_arguments) do
      {
        "x-queue-type" => "quorum",
        "x-dead-letter-exchange" => "acemq.dlx",
        "x-dead-letter-routing-key" => dlq
      }
    end

    let(:topology) do
      AceMQ::AMQP::Topology.new
                           .queue(queue, dead_letter: true, retry_policy: policy,
                                         retry_threshold: 1)
                           .parked_queue(queue)
    end

    before do
      scrub(queue, dlq, parked, rung)
      topology.apply(mq)
    end

    after { scrub(queue, dlq, parked, rung) }

    # A second connection is what a second service is. The transport rather than
    # the connection, so nothing this library defaults gets added on the way
    # past: these are the arguments as another language would send them.
    def declared_by_another_service(name, arguments)
      other = AceMQ::AMQP::Connection.open(BROKER, origin: "java@rbit")
      other.transport.declare_queue(name, durable: true, arguments: arguments)
    ensure
      other&.close
    end

    it "accepts the arguments a Java service sends, and refuses the same queue as classic" do
      # The whole claim, in two declarations. The first is what a Java service
      # starting second would send for a queue this one declared; the broker
      # taking it means the two can share the queue. The second is that same
      # declaration without x-queue-type — a classic queue, which is what this
      # library sent before this change — and the broker refusing it is why the
      # change had to happen.
      expect { declared_by_another_service(queue, java_arguments) }.not_to raise_error

      classic = java_arguments.except("x-queue-type")
      expect { declared_by_another_service(queue, classic) }
        .to raise_error(AceMQ::AMQP::TransportError, /PRECONDITION_FAILED/i)
    end

    it "keeps the dead-letter queue classic, which is the half that must not change" do
      # The mirror image, and just as load-bearing. Java declares {queue}.dlq
      # CLASSIC, so a dlq this library quietly made quorum would break the same
      # two services in the other direction.
      expect { declared_by_another_service(dlq, {}) }.not_to raise_error
      expect { declared_by_another_service(dlq, { "x-queue-type" => "quorum" }) }
        .to raise_error(AceMQ::AMQP::TransportError, /PRECONDITION_FAILED/i)
    end

    it "prints the whole topology, so it can be held beside the other four" do
      puts <<~TOPOLOGY
        --- topology, as this library declares it ---
        #{topology.plan.map { |line| "  #{line}" }.join("\n")}

          queues, with the argument table each is declared with
        #{topology.queues.map { |q| "    #{q.name} (#{q.type})\n#{argument_lines(q)}" }.join("\n")}
          exchanges
        #{topology.exchanges.map { |e| "    #{e.name} (#{e.kind}, durable: #{e.durable})" }.join("\n")}
          bindings
        #{topology.bindings.map { |b| "    #{b}" }.join("\n")}
        ---------------------------------------------
      TOPOLOGY

      types = topology.queues.to_h { |q| [q.name, q.type] }
      expect(types).to eq(queue => :quorum, dlq => :classic, rung => :classic,
                          parked => :classic)
      expect(topology.queues.first.arguments).to eq(java_arguments)
    end

    it "runs a whole retry cycle on the quorum queue, through a classic rung" do
      # Quorum queues do not dead-letter the way classic ones do — the strategy
      # is at-most-once by default and the accounting is different — so the
      # round trip is proved rather than assumed: out of the quorum queue, into
      # the classic rung, and home again on attempt 2 with nothing attached.
      attempts = []
      consumer = mq.consume(queue, retry_policy: policy, retry_threshold: 1) do |message|
        attempts << message.attempt
        AceMQ::AMQP::Ack.retry("the warehouse is down")
      end

      mq.publish({ "order_id" => "A-11" }, to: queue, type: "order.placed.v2")
      expect(wait_for { mq.message_count(rung) == 1 }).to be(true)
      expect(attempts).to eq([1])

      # Cancelled before anything is counted, so the zero below is the broker's
      # own count of the quorum queue and not a message this process is holding.
      consumer.cancel
      expect(mq.message_count(queue)).to eq(0)
      expect(mq.message_count(rung)).to eq(1)

      # Nothing woke it up. The rung's time-to-live expired and carried it back
      # through acemq.retry into the quorum queue it came from.
      expect(wait_for(seconds: 15) { mq.message_count(queue) == 1 }).to be(true)
      expect(mq.message_count(rung)).to eq(0)

      properties, body = take_one(queue)
      expect(body).to eq('{"order_id":"A-11"}')
      expect(properties[:headers][AceMQ::AMQP::Headers::ATTEMPT]).to eq(2)
      death = properties[:headers]["x-death"]&.first
      expect(death["queue"]).to eq(rung)
      expect(death["reason"]).to eq("expired")
    end
  end

  describe "an outbox" do
    let(:queue) { queue_named("outbox") }
    let(:store) { AceMQ::AMQP::Patterns::InMemoryOutboxStore.new }

    before do
      scrub(queue)
      AceMQ::AMQP::Topology.new.queue(queue).apply(mq)
    end

    after { scrub(queue) }

    it "sends a recorded message the broker cannot tell from a published one" do
      # The point of the integration test rather than the unit one: the record
      # holds bytes and a rendered header table, and what has to survive is
      # RabbitMQ's field table putting them back with their types intact.
      store.add(AceMQ::AMQP::Patterns.record(
                  mq, { "order_id" => "A-6" },
                  to: queue, type: "order.placed.v2", version: 4
                ))
      relay = AceMQ::AMQP::Patterns::OutboxRelay.new(mq, store)

      expect(relay.sweep).to eq(1)
      expect(store.size).to eq(0)

      properties, body = take_one(queue)
      expect(body).to eq('{"order_id":"A-6"}')
      headers = properties[:headers]
      expect(headers[AceMQ::AMQP::Headers::TYPE]).to eq("order.placed.v2")
      expect(headers[AceMQ::AMQP::Headers::VERSION]).to eq(4)
      expect(headers[AceMQ::AMQP::Headers::ORIGIN]).to eq("rspec@rbit")
      expect(headers[AceMQ::AMQP::Headers::ATTEMPT]).to eq(1)
    end
  end

  describe "a claim check" do
    let(:queue) { queue_named("claims") }
    let(:directory) { Dir.mktmpdir("acemq-claims") }
    let(:store) { AceMQ::AMQP::Patterns::FilesystemClaimCheckStore.new(directory) }
    let(:codec) do
      AceMQ::AMQP::Patterns::ClaimCheckCodec.wrapping(AceMQ::AMQP::JSONCodec.new, store)
    end
    # A separate connection, so what the consumer gets is what the broker
    # carried rather than an object the publisher still had a reference to.
    let(:consumer) { AceMQ::AMQP::Connection.open(BROKER, origin: "rspec@rbit", codec: codec) }
    let(:report) { { "scan" => "x" * (256 * 1024) } }

    before do
      scrub(queue)
      AceMQ::AMQP::Topology.new.queue(queue).apply(mq)
    end

    after do
      consumer.close
      scrub(queue)
      FileUtils.remove_entry(directory)
    end

    it "puts the key on the broker and the payload in the store" do
      # The claim being made: a quarter of a megabyte of scan does not go
      # through RabbitMQ. Reading the body with basic_get rather than through
      # the codec is the only way to see what the broker actually carried.
      publisher = AceMQ::AMQP::Connection.new(transport: mq.transport, codec: codec,
                                              origin: "rspec@rbit")
      publisher.publish(report, to: queue, type: "document.stored")

      _properties, body = take_one(queue)
      expect(body.bytesize).to be < 100
      expect(body.bytes.first(3)).to eq([0xAC, 0x01, 0x01])

      key = AceMQ::AMQP::Patterns::ClaimCheckCodec.key_of(body)
      expect(store.get(key).bytesize).to be > (256 * 1024)
    end

    it "hands a consumer the whole payload back" do
      received = []
      consumer.consume(queue) do |message|
        received << message.payload
        AceMQ::AMQP::Ack.accept
      end

      AceMQ::AMQP::Connection.new(transport: mq.transport, codec: codec, origin: "rspec@rbit")
                             .publish(report, to: queue, type: "document.stored")

      wait_for { received.any? }
      expect(received.first).to eq(report)
    end

    it "leaves a small message inline, so the store is not on the common path" do
      publisher = AceMQ::AMQP::Connection.new(transport: mq.transport, codec: codec,
                                              origin: "rspec@rbit")
      publisher.publish({ "order_id" => "A-9" }, to: queue)

      _properties, body = take_one(queue)
      expect(body.bytes.first(3)).to eq([0xAC, 0x01, 0x00])
      expect(AceMQ::AMQP::Patterns::ClaimCheckCodec.key_of(body)).to be_nil
      expect(Dir.children(directory)).to be_empty
    end
  end

  describe "a request and its reply" do
    let(:queue) { queue_named("requests") }
    let(:replies) { queue_named("replies") }

    before do
      scrub(queue, replies)
      AceMQ::AMQP::Topology.new.queue(queue).queue(replies).apply(mq)
    end

    after { scrub(queue, replies) }

    it "answers a caller waiting on another thread" do
      # The unit specs deliver synchronously, which is what makes them fast and
      # also what they cannot prove: here the reply genuinely arrives on the
      # consumer's thread while this one is blocked on a condition variable.
      AceMQ::AMQP::Patterns.serve(mq, queue) do |message|
        { "price" => message.payload["sku"].length * 100 }
      end
      requester = AceMQ::AMQP::Patterns::Requester.new(mq, to: queue, reply_to: replies,
                                                           timeout: 10)

      expect(requester.call({ "sku" => "X-12" })).to eq({ "price" => 400 })
      requester.close
    end

    it "brings a responder's failure back rather than making the caller wait it out" do
      AceMQ::AMQP::Patterns.serve(mq, queue) { |_message| raise "the catalogue is down" }
      requester = AceMQ::AMQP::Patterns::Requester.new(mq, to: queue, reply_to: replies,
                                                           timeout: 10)

      expect { requester.call({ "sku" => "X" }) }
        .to raise_error(AceMQ::AMQP::Patterns::ResponderFailed, /the catalogue is down/)
      requester.close
    end
  end

  describe "replaying a dead-letter queue" do
    let(:queue) { queue_named("replay") }
    let(:dlq) { AceMQ::AMQP::Naming.dead_letter_queue(queue) }

    before do
      scrub(queue, dlq)
      AceMQ::AMQP::Topology.new.queue(queue).dead_letter_queue(queue).apply(mq)
    end

    after { scrub(queue, dlq) }

    it "reaches the whole queue even though a declined message goes back to its head" do
      # The one thing only a broker can show. RabbitMQ returns a rejected
      # message to the head of the queue, so a replay that put a declined
      # message straight back would be handed the same one for ever and never
      # see what was behind it. Holding declined messages unacknowledged for the
      # length of the pass is what gets past that, and it is the reason this
      # test exists rather than a unit one.
      mq.publish({ "order_id" => "keep" }, to: dlq, error: "no such customer", attempt: 5)
      mq.publish({ "order_id" => "move" }, to: dlq, error: "connection timeout", attempt: 5)
      expect(wait_for { mq.message_count(dlq) == 2 }).to be(true)

      result = AceMQ::AMQP::Patterns.replay(mq, from: dlq, routing_key: queue) do |envelope, _|
        envelope.error.include?("timeout")
      end

      expect(result.moved).to eq(1)
      expect(result.skipped).to eq(1)
      expect(result.reason).to eq(:drained)

      properties, body = take_one(queue)
      expect(body).to eq('{"order_id":"move"}')
      expect(properties[:headers][AceMQ::AMQP::Patterns::REPLAYED_FROM_HEADER]).to eq(dlq)
      # Back on attempt one, and through RabbitMQ's field table rather than a
      # Hash in this process: a message that came back on attempt five would be
      # dead-lettered again before any handler saw it.
      expect(properties[:headers][AceMQ::AMQP::Headers::ATTEMPT]).to eq(1)
      expect(properties[:headers]).not_to have_key(AceMQ::AMQP::Headers::ERROR)
      # The declined one went back rather than being lost.
      expect(wait_for { mq.message_count(dlq) == 1 }).to be(true)
    end
  end

  describe "a stream" do
    let(:stream) { queue_named("stream") }

    before do
      scrub(stream)
      AceMQ::AMQP::Patterns.declare_stream(mq, stream, max_age: 3600,
                                                       max_bytes: 10 * 1024 * 1024)
    end

    after { scrub(stream) }

    it "keeps what it has delivered, so a second reader sees it all again" do
      # The whole difference from a queue, and the only place it can be shown:
      # an acknowledgement advances a reader's position rather than removing
      # anything, so a reader starting from the beginning after everything has
      # been read still sees everything.
      3.times { |i| mq.publish({ "n" => i }, to: stream) }

      first = []
      AceMQ::AMQP::Patterns.read_stream(mq, stream,
                                        offset: AceMQ::AMQP::Patterns::StreamOffset.first,
                                        name: "rbit-reader-1") do |message|
        first << message.payload["n"]
        AceMQ::AMQP::Ack.accept
      end
      expect(wait_for { first.size == 3 }).to be(true)

      second = []
      AceMQ::AMQP::Patterns.read_stream(mq, stream,
                                        offset: AceMQ::AMQP::Patterns::StreamOffset.first,
                                        name: "rbit-reader-2") do |message|
        second << message.payload["n"]
        AceMQ::AMQP::Ack.accept
      end

      expect(wait_for { second.size == 3 }).to be(true)
      expect(first).to eq([0, 1, 2])
      expect(second).to eq([0, 1, 2])
    end

    it "starts at the position it was asked for" do
      3.times { |i| mq.publish({ "n" => i }, to: stream) }

      seen = []
      AceMQ::AMQP::Patterns.read_stream(mq, stream,
                                        offset: AceMQ::AMQP::Patterns::StreamOffset.at(2),
                                        name: "rbit-reader-3") do |message|
        seen << message.payload["n"]
        AceMQ::AMQP::Ack.accept
      end

      expect(wait_for { seen.size == 1 }).to be(true)
      expect(seen).to eq([2])
    end
  end

  describe "health" do
    let(:queue) { queue_named("health") }

    before do
      scrub(queue)
      AceMQ::AMQP::Topology.new.queue(queue).apply(mq)
    end

    after { scrub(queue) }

    it "proves the round trip and leaves nothing on the broker" do
      # The half only a broker can show. The probe queue is exclusive and
      # auto-deleting, and the channel it was declared on is closed straight
      # away, so a readiness probe running every five seconds for a week does
      # not leave ten thousand queues behind it. The name is pinned here only so
      # that this can go and look for it afterwards.
      allow(SecureRandom).to receive(:uuid).and_return("rbit-probe")
      report = mq.health

      expect(report).to be_up
      expect(report.parts["round_trip_ms"]).to be >= 0
      expect(mq.queue_exists?("acemq-health-rbit-probe")).to be(false)
    end

    it "is degraded while a consumer is stopped, and up again without one" do
      consumer = mq.consume(queue) { AceMQ::AMQP::Ack.accept }
      expect(mq.health).to be_up

      consumer.cancel
      degraded = mq.health
      expect(degraded).to be_degraded
      expect(degraded.detail).to eq("1 of 1 consumers has stopped")
    end

    it "is down once the connection is closed" do
      # Its own connection, because closing the one this example group shares
      # would leave the tidying up with nothing to tidy up with.
      other = AceMQ::AMQP::Connection.open(BROKER, origin: "rspec@rbit")
      other.close

      expect(other.health).to be_down
    end
  end

  describe "telemetry" do
    let(:queue) { queue_named("telemetry") }
    let(:dlq) { AceMQ::AMQP::Naming.dead_letter_queue(queue) }
    let(:metrics) { AceMQ::AMQP::Telemetry::Registry.new }
    let(:mq) do
      AceMQ::AMQP::Connection.open(BROKER, origin: "rspec@rbit", telemetry: metrics)
    end

    before do
      scrub(queue, dlq)
      AceMQ::AMQP::Topology.new.queue(queue).dead_letter_queue(queue).apply(mq)
    end

    after { scrub(queue, dlq) }

    it "counts a real round trip through a real broker" do
      mq.consume(queue, retry_policy: AceMQ::AMQP::RetryPolicy.fixed(2, 0)) do |_message|
        AceMQ::AMQP::Ack.retry("the warehouse is down")
      end
      mq.publish({ "order_id" => "A-10" }, to: queue)

      expect(wait_for { metrics[AceMQ::AMQP::Telemetry::DEAD_LETTERED, queue: queue] == 1 })
        .to be(true)
      expect(metrics[AceMQ::AMQP::Telemetry::PUBLISHED, exchange: ""]).to be >= 1
      expect(metrics[AceMQ::AMQP::Telemetry::CONSUMED, queue: queue]).to eq(2)
      expect(metrics[AceMQ::AMQP::Telemetry::RETRIED, queue: queue]).to eq(2)
      expect(metrics.to_prometheus).to include("acemq_messages_dead_lettered")
    end
  end

  describe "publishing" do
    let(:queue) { queue_named("confirms") }

    before do
      scrub(queue)
      AceMQ::AMQP::Topology.new.queue(queue).apply(mq)
    end

    after { scrub(queue) }

    it "waits for the broker to say it has the message" do
      # Without confirms a successful publish means the bytes reached a socket,
      # which is not the same as the broker having them.
      mq.publish({ "order_id" => "A-4" }, to: queue)
      expect(wait_for { mq.message_count(queue) == 1 }).to be(true)
    end
  end
end
