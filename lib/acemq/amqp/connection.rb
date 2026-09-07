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

require "socket"

require_relative "ack"
require_relative "codec"
require_relative "envelope"
require_relative "naming"
require_relative "retry_ladder"
require_relative "retry_policy"
require_relative "transport"

module AceMQ
  module AMQP
    # A delivery that has been read.
    #
    # +envelope.attempt+ is the count for this delivery, worked out by the
    # consumer; everything else in the envelope came off the wire. +body+ is
    # there for a handler that wants the bytes the codec was given, which is
    # what anybody debugging a decoding disagreement between two languages
    # actually needs.
    Message = Struct.new(
      :payload, :envelope, :routing_key, :content_type, :redelivered, :body,
      keyword_init: true
    ) do
      def redelivered? = !!redelivered
      def id = envelope.id
      def attempt = envelope.attempt
    end

    # A connection to a broker.
    #
    # Everything a service does with AceMQ goes through one of these: it holds
    # the codec, the origin stamped on what it publishes, and the retry policy
    # its consumers use unless they say otherwise. One per process is the
    # usual shape — a connection is a socket and a heartbeat, and opening one
    # per publish is how a service ends up with a thousand of them.
    #
    #   mq = Connection.open("amqp://guest:guest@localhost:5672",
    #                        origin: "checkout@#{Socket.gethostname}",
    #                        retry_policy: RetryPolicy.exponential(5, 1, 60))
    #
    #   mq.publish({ "order_id" => "A-1" }, to: "orders.new")
    #
    #   mq.consume("orders.new") do |message|
    #     place(message.payload) ? Ack.accept : Ack.retry("the warehouse said no")
    #   end
    class Connection
      # How many unacknowledged messages a consumer holds by default.
      DEFAULT_PREFETCH = 20

      attr_reader :transport, :codec, :origin, :retry_policy, :prefetch,
                  :retry_threshold

      # Opens a connection to a broker.
      #
      # @param url [String] amqp:// or amqps://
      # @param codec [#encode] the default codec, JSON unless said otherwise
      # @param origin [String] what to stamp on published messages,
      #   conventionally +service@host+
      # @param retry_policy [RetryPolicy] what consumers use by default
      # @param prefetch [Integer] unacknowledged messages per consumer
      # @param retry_threshold [Numeric] seconds; a retry delayed this long or
      #   longer waits in the broker rather than in the consumer
      #   through
      # @param transport_options [Hash] passed to {Transport.open}
      # @return [Connection]
      def self.open(url, codec: JSONCodec.new, origin: nil, retry_policy: RetryPolicy.none,
                    prefetch: DEFAULT_PREFETCH,
                    retry_threshold: RetryLadder::DEFAULT_THRESHOLD, **transport_options)
        new(transport: Transport.open(url, **transport_options), codec: codec, origin: origin,
            retry_policy: retry_policy, prefetch: prefetch, retry_threshold: retry_threshold)
      end

      # Wraps a transport that is already open.
      #
      # {open} covers the ordinary case. This is for a transport that is not
      # reached by a URL — a fake in a test, most of all, which is what makes
      # the retry arithmetic below testable without a broker in the room.
      def initialize(transport:, codec: JSONCodec.new, origin: nil,
                     retry_policy: RetryPolicy.none, prefetch: DEFAULT_PREFETCH,
                     retry_threshold: RetryLadder::DEFAULT_THRESHOLD)
        @transport = transport
        @codec = Codec.check!(codec)
        @origin = origin.nil? || origin.to_s.empty? ? self.class.default_origin : origin.to_s
        @retry_policy = retry_policy
        @prefetch = prefetch
        @retry_threshold = retry_threshold
        @consumers = []
        @lock = Mutex.new
      end

      # Names the machine but not the service, which is the most an
      # unconfigured library can honestly say. Set +origin:+ to say more: it is
      # the field somebody reads first when a message arrives from a fleet and
      # the question is which of forty pods sent it.
      def self.default_origin
        "acemq@#{Socket.gethostname}"
      rescue StandardError
        "acemq@unknown"
      end

      # Publishes one message.
      #
      # The envelope is built here unless one is passed: an identifier, a type
      # falling back to the routing key, a correlation falling back to the
      # identifier, this connection's origin, and the current time. Envelope
      # fields given as keywords override those.
      #
      #   mq.publish(order, to: "orders.new", type: "order.placed.v2",
      #              correlation_id: request_id)
      #
      # @param payload [Object] anything the codec will encode
      # @param to [String] the routing key, or the queue name when publishing
      #   through the default exchange
      # @param exchange [String] empty for the default exchange, which routes
      #   to the queue whose name matches the routing key
      # @param envelope [Envelope, nil] one built elsewhere, for when a
      #   message's metadata derives from another message's
      # @param fields [Hash] envelope fields, when no envelope is given
      # @return [Envelope] what was actually put on the wire
      def publish(payload, to:, exchange: "", envelope: nil, codec: nil, persistent: true,
                  **fields)
        if envelope && !fields.empty?
          raise ArgumentError,
                "publish was given both an envelope and the fields to build one " \
                "(#{fields.keys.join(", ")}); pass one or the other"
        end

        codec = codec.nil? ? @codec : Codec.check!(codec)
        envelope ||= Envelope.new(origin: @origin, **fields)
        @transport.publish(exchange: exchange, routing_key: to, body: codec.encode(payload),
                           content_type: codec.content_type, message_id: envelope.id,
                           headers: envelope.to_headers(to), persistent: persistent)
        envelope
      end

      # Reads messages from a queue until the returned consumer is cancelled.
      #
      #   consumer = mq.consume("orders.new") do |message|
      #     place(message.payload)
      #     Ack.accept
      #   end
      #
      # The handler is called on the transport's threads, not this one, and
      # must return an {Ack}. A handler that raises is treated as +Ack.retry+,
      # except for {FatalError}, which is treated as +Ack.reject+ — raising
      # that is how a handler says "stop now" without having to know how many
      # attempts remain.
      #
      # @param queue [String]
      # @param concurrency [Integer] messages worked on at once. One by
      #   default, which keeps a queue's messages in the order the broker
      #   offers them; raising it trades that order for throughput.
      # @param retry_threshold [Numeric, nil] seconds, or nil for the
      #   connection's. A retry delayed this long or longer waits in a rung
      #   queue instead of in this process.
      # @return [Consumer]
      def consume(queue, codec: nil, retry_policy: nil, prefetch: nil, concurrency: 1,
                  tag: nil, arguments: {}, retry_threshold: nil,
                  &handler)
        unless handler
          raise ArgumentError,
                "consume(#{queue.inspect}) needs a block to handle messages"
        end

        consumer = Consumer.new(
          transport: @transport, queue: queue, handler: handler,
          codec: codec.nil? ? @codec : Codec.check!(codec),
          retry_policy: retry_policy || @retry_policy,
          retry_threshold: retry_threshold || @retry_threshold
        )
        consumer.start(prefetch: prefetch || @prefetch, concurrency: concurrency, tag: tag,
                       arguments: arguments)
        @lock.synchronize { @consumers << consumer }
        consumer
      end

      # Declares everything in a topology.
      #
      # @param topology [Topology]
      # @return [Topology]
      def apply(topology) = topology.apply(self)

      def declare_exchange(name, **options) = @transport.declare_exchange(name, **options)
      def declare_queue(name, **options) = @transport.declare_queue(name, **options)
      def bind(**options) = @transport.bind(**options)
      def pull(queue) = @transport.pull(queue)
      def message_count(queue) = @transport.message_count(queue)
      def queue_exists?(name) = @transport.queue_exists?(name)
      def delete_queue(name) = @transport.delete_queue(name)

      # Stops every consumer and closes the connection.
      #
      # Consumers are stopped first and their handlers allowed to finish, so a
      # message being worked on when this is called is acknowledged rather than
      # returned to the queue for somebody else to redo.
      def close
        consumers = @lock.synchronize do
          taken = @consumers
          @consumers = []
          taken
        end

        # Every consumer is stopped even when one of them refuses, and the
        # connection is closed either way. Stopping at the first failure would
        # leave the rest of them running and the socket open, so a shutdown that
        # went slightly wrong would become a process that will not exit — which
        # is a worse problem than whatever the first consumer objected to.
        failure = nil
        consumers.each do |consumer|
          consumer.cancel
        rescue StandardError => e
          failure ||= e
        end
        @transport.close
        raise failure if failure

        nil
      end
    end

    # A running subscription. Cancel it to stop.
    #
    # Where the retry policy is actually enforced: the attempt is advanced here,
    # the decision about where the delay is waited is made here, and so is the
    # decision to give up and dead-letter — that last one because the broker
    # cannot write +x-acemq-error+ onto a message explaining why it gave up.
    #
    # A short delay is waited here, holding one prefetch slot. A long one is
    # waited by the broker, in a rung queue; see {RetryLadder} for why the two
    # are not the same choice.
    class Consumer
      attr_reader :queue, :retry_policy, :codec, :ladder

      def initialize(transport:, queue:, handler:, codec:, retry_policy:,
                     retry_threshold: RetryLadder::DEFAULT_THRESHOLD)
        @transport = transport
        @queue = queue
        @handler = handler
        @codec = codec
        @retry_policy = retry_policy
        @ladder = RetryLadder.for(queue, retry_policy, threshold: retry_threshold)
        @dead_letter_queue = Naming.dead_letter_queue(queue)
        @parked_queue = Naming.parked_queue(queue)
        @lock = Mutex.new
        @in_flight = 0
      end

      # @api private
      def start(prefetch:, concurrency:, tag:, arguments:)
        # Declared before anything is subscribed, and not on the failure path.
        # A rung that does not exist loses the message rather than reporting
        # anything — the default exchange drops what it cannot route — so the
        # moment to find out is the one where nothing has failed yet.
        @ladder.declare(@transport)
        @subscription = @transport.subscribe(
          @queue, prefetch: prefetch, concurrency: concurrency, tag: tag, arguments: arguments
        ) { |delivery| handle(delivery) }
        self
      end

      # Runs one delivery through the codec, the handler and the retry policy.
      #
      # Public because it is the whole of this class worth testing, and a test
      # that has to open a broker to reach it is a test nobody runs.
      def handle(delivery)
        enter
        envelope = envelope_for(delivery)
        payload = decode(delivery)
        settle(delivery, envelope, invoke(payload, envelope, delivery))
      rescue DecodeError => e
        # A body that will not decode decodes no better next time, so it is
        # parked rather than retried. Parked and not dead-lettered: a message
        # that failed five times and a message nothing could read are two
        # different problems, and whoever drains the dead letters should not
        # have to sort them by hand.
        park(delivery, envelope, "could not be decoded: #{e.message}")
      ensure
        leave
      end

      # Stops delivery and waits for handlers already running.
      #
      # @param timeout [Numeric] seconds to wait for in-flight handlers
      def cancel(timeout: 30)
        @subscription&.stop
        wait_for_handlers(timeout)
        @subscription&.close
        nil
      end

      # How many messages this consumer is working on right now.
      def in_flight = @lock.synchronize { @in_flight }

      private

      # The envelope for this delivery, attempt included.
      #
      # Read off the wire, because +x-acemq-attempt+ is defined as the count the
      # retry engine increments, and a retry here republishes with it advanced.
      #
      # Counting in memory instead — from the broker's redelivery flag, keyed by
      # message id — is wrong the moment there is more than one consumer: a
      # requeued message can come back to a different one, which has never seen
      # it and calls it attempt one. A policy of five attempts then retries for
      # ever, and the count is lost across a restart as well.
      def envelope_for(delivery)
        Envelope.from_headers(delivery.headers, delivery.routing_key)
      end

      # A codec that chooses by content type needs to be told it; a plain one
      # has no use for it. Asked of the codec rather than assumed, so a codec
      # from another gem works either way round.
      def decode(delivery)
        if @codec.method(:decode).arity == 1
          @codec.decode(delivery.body)
        else
          @codec.decode(delivery.body, delivery.content_type)
        end
      end

      # Runs the handler, turning anything it raises into a decision.
      #
      # A handler that raises has still said something: an ordinary failure is
      # worth another go, a {FatalError} is not. A handler that returns
      # something other than an {Ack} has said nothing at all, and that is a
      # bug which will repeat, so the message goes to the dead-letter queue
      # with the class name in the reason rather than round the queue forever.
      def invoke(payload, envelope, delivery)
        result = @handler.call(Message.new(
                                 payload: payload, envelope: envelope,
                                 routing_key: delivery.routing_key,
                                 content_type: delivery.content_type,
                                 redelivered: delivery.redelivered?, body: delivery.body
                               ))
        return result if result.is_a?(Ack)

        Ack.reject(FatalError.new(
                     "the handler for #{@queue} returned a #{result.class} rather than an Ack"
                   ))
      rescue FatalError => e
        Ack.reject(e)
      rescue StandardError => e
        Ack.retry(e)
      end

      def settle(delivery, envelope, ack)
        if ack.accept?
          delivery.ack
        elsif ack.reject?
          dead_letter(delivery, envelope, "rejected by the handler: #{describe(ack.error)}")
        else
          retry_or_give_up(delivery, envelope, ack)
        end
      end

      def retry_or_give_up(delivery, envelope, ack)
        if ack.error.is_a?(FatalError)
          # The handler asked for a retry but marked the reason as one that
          # will not change. Honouring the mark rather than the request is the
          # entire point of having it.
          return dead_letter(delivery, envelope, "retrying cannot help: #{describe(ack.error)}")
        end

        # Without jitter, because this number decides where the wait happens and
        # a jittered one names no rung. Jitter is added below, and only to the
        # waits this process performs itself.
        delay = @retry_policy.next_delay(envelope.attempt, envelope.age, jitter: false)
        if delay.nil?
          return dead_letter(delivery, envelope, "#{gave_up(envelope)}: #{describe(ack.error)}")
        end

        # Republished rather than requeued, with the attempt advanced, whichever
        # way the wait happens. A requeue hands back the bytes the broker was
        # given, so the count would have to live in this process — and then a
        # fleet of consumers each counts its own, a message that moves between
        # them is for ever on attempt one, and a restart forgets everything. The
        # trade is that the message goes to the back of its queue rather than
        # the front.
        next_attempt = envelope.with(attempt: envelope.attempt + 1)
        rung = @ladder.rung_for(delay)
        return wait_in_broker(rung, delivery, next_attempt) if rung

        wait_here(delay, delivery, next_attempt)
      end

      # A short delay, waited by this process.
      #
      # Holding the delivery holds one of this consumer's prefetch slots, and
      # losing the process loses the wait — the broker redelivers at once. Both
      # are the honest cost of not spending a queue on a delay measured in
      # seconds.
      def wait_here(delay, delivery, envelope)
        sleep(@retry_policy.jittered(delay)) if delay.positive?
        republish(@queue, delivery, envelope)
        delivery.ack
      end

      # A long delay, waited by the broker.
      #
      # The message goes into a rung queue whose +x-message-ttl+ is the delay
      # and whose dead-letter target is this queue, so it comes home on its own
      # with nothing running. Nothing is set on the message itself: a
      # per-message expiration would look like the flexible answer and is a
      # trap, because RabbitMQ expires messages only from the head of a queue,
      # so one long wait at the front holds back every shorter one behind it.
      #
      # No jitter either, and none is wanted: each message's time-to-live starts
      # when it enters the rung, so a fleet that failed over ten seconds is
      # released over ten seconds without anybody arranging it.
      def wait_in_broker(rung, delivery, envelope)
        republish(rung, delivery, envelope)
        delivery.ack
      end

      def gave_up(envelope)
        if envelope.attempt >= @retry_policy.max_attempts
          "gave up after #{@retry_policy.max_attempts} attempts"
        else
          "gave up on a message older than #{@retry_policy.max_message_age} seconds"
        end
      end

      # Sends a message to its dead-letter queue with the reason attached, then
      # acknowledges the original.
      #
      # Acknowledging a failure looks wrong and is what makes this reliable:
      # the message has already been safely republished elsewhere, so the
      # original is a copy that has been dealt with. Rejecting it instead would
      # either requeue it into a hot loop or hand it to whatever dead-lettering
      # the queue happens to be declared with — and neither of those can write
      # +x-acemq-error+ onto it, which is the one thing whoever finds it in the
      # dead-letter queue actually needs.
      def dead_letter(delivery, envelope, reason)
        republish(@dead_letter_queue, delivery, envelope.with(error: reason))
        delivery.ack
      end

      def park(delivery, envelope, reason)
        envelope ||= Envelope.from_headers(delivery.headers, delivery.routing_key)
        republish(@parked_queue, delivery, envelope.with(error: reason))
        delivery.ack
      end

      def republish(queue, delivery, envelope)
        @transport.publish(exchange: "", routing_key: queue, body: delivery.body,
                           content_type: delivery.content_type, message_id: envelope.id,
                           headers: envelope.to_headers(delivery.routing_key), persistent: true)
      end

      def describe(error)
        return "no reason given" if error.nil?
        return error.to_s if error.is_a?(String)

        message = error.respond_to?(:message) ? error.message : error.to_s
        "#{error.class}: #{message}"
      end

      def enter = @lock.synchronize { @in_flight += 1 }
      def leave = @lock.synchronize { @in_flight -= 1 }

      # Polled rather than signalled. A condition variable would be tidier and
      # would also mean holding a lock across a handler that may be waiting on
      # a database, which is how a shutdown turns into a deadlock.
      def wait_for_handlers(timeout)
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
        sleep(0.01) while in_flight.positive? &&
                          Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      end
    end
  end
end
