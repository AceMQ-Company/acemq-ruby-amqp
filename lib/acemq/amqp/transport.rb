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

require_relative "ack"
require_relative "credentials"
require_relative "queue_type"
require_relative "security"

module AceMQ
  module AMQP
    # The broker, or the network, rather than the message.
    #
    # Worth telling apart from a bad message because it says nothing about the
    # message: the same one may well go through once the broker is back.
    class TransportError < StandardError; end

    # A message the broker would not take, or took and could not route.
    #
    # The two are told apart by {#unroutable?} rather than by two classes,
    # because a caller who only wants to know that the message did not arrive
    # should not have to rescue both. Go and Python split the same failure the
    # same way — a flag on the one error — and the word the telemetry writes is
    # read off it, so the counter and the span cannot disagree about which of
    # the two happened.
    class PublishError < TransportError
      # Whether the broker accepted the message and had nowhere to put it.
      #
      # False for everything else: a refused publish, a connection that dropped
      # mid-flight, a confirm that never came. Only a mandatory publish can ever
      # set it, because only a mandatory publish is told.
      def unroutable? = @unroutable

      # @param message [String, nil]
      # @param unroutable [Boolean]
      def initialize(message = nil, unroutable: false)
        super(message)
        @unroutable = unroutable
      end
    end

    # The sentences a publish that did not arrive comes back with.
    #
    # In one place because a batch has to raise exactly what a single publish
    # raises: whoever greps a log for one of these should not have to know
    # which of the two paths sent the message, and two copies of a sentence are
    # two sentences waiting to drift apart.
    #
    # @api private
    module PublishFailure
      module_function

      def not_confirmed(message_id, exchange, routing_key)
        "the broker would not confirm message #{message_id} on exchange " \
          "#{exchange.inspect} with key #{routing_key.inspect}"
      end

      def not_routed(message_id, exchange, routing_key, reason)
        "the broker had nowhere to route message #{message_id} published to exchange " \
          "#{exchange.inspect} with key #{routing_key.inspect}: #{reason}"
      end

      def not_published(message_id, exchange, routing_key, reason)
        "cannot publish message #{message_id} to exchange #{exchange.inspect} " \
          "with key #{routing_key.inspect}: #{reason}"
      end
    end

    # The two rules about what a property looks like on the wire, shared by
    # everything here that writes one.
    #
    # @api private
    module Wire
      module_function

      # Header and argument names go to the broker as strings, whatever they
      # were written as here. A table keyed by :"x-message-ttl" is not the same
      # table as one keyed by "x-message-ttl", and the broker only recognises
      # the second — a symbol would silently become a queue argument nothing
      # acts on.
      def stringify(table)
        return {} if table.nil? || table.empty?

        table.to_h { |name, value| [name.to_s, value] }
      end

      # An empty property is absent rather than blank. A +reply-to+ carrying ""
      # is a reply-to somebody has to write a special case for at the other end,
      # which is the same rule {Envelope#to_headers} follows for its headers.
      def presence(value)
        text = value.to_s
        text.empty? ? nil : text
      end
    end

    # One message, as it arrived, before any codec has looked at it.
    #
    # +redelivered+ is the broker saying it has handed these bytes over before —
    # a nack that requeued, or a consumer that died holding the message. It is
    # not how a retry is counted: a requeue hands the broker back the bytes it
    # was given, so the flag says a delivery happened twice and nothing about
    # which attempt this is. That lives in +x-acemq-attempt+, which the retry
    # engine advances by republishing rather than requeueing.
    #
    # Settling travels with the delivery as +on_ack+ and +on_nack+ rather than
    # as a delivery tag the consumer would have to hand back to the right
    # channel. That keeps the consumer from knowing which channel a message
    # came down, which is the whole reason a fake transport can stand in for a
    # broker in a test.
    #
    # +reply_to+ is AMQP's own property and not a header, which is why it is a
    # field here rather than something read out of +headers+. The Java and .NET
    # libraries write requests with it and nothing else, so a Ruby responder
    # that could not see it could not answer them.
    Delivery = Struct.new(
      :body, :content_type, :routing_key, :message_id, :headers, :redelivered,
      :reply_to, :on_ack, :on_nack, keyword_init: true
    ) do
      def redelivered? = !!redelivered

      # Confirms the message. It will not be sent again.
      def ack = on_ack&.call

      # Returns the message, requeued or not.
      def nack(requeue: false) = on_nack&.call(requeue)
    end

    # The messages the broker handed back, on their way to whoever sent them.
    #
    # A class of its own because it is the one place in this file where two
    # threads meet: bunny dispatches +basic.return+ on its reader thread while
    # the thread that published is still inside {Transport#publish} waiting for
    # a confirm. It is also the only place that has to know bunny's rule for
    # delivering a return, which is a rule about exchange objects rather than
    # about messages.
    #
    # @api private
    class ReturnedMessages
      def initialize
        @returns = Thread::Queue.new
        @watched = []
      end

      # Gets ready to hear about this exchange, and forgets anything left over.
      #
      # bunny hands +basic.return+ to the +Bunny::Exchange+ object registered on
      # the channel under that name and drops it with a warning when there is
      # none, so there has to be one — and it has to be one that does not
      # declare, because this transport publishes to exchanges the application
      # declared and re-declaring one with a type guessed here is how a publish
      # turns into PRECONDITION_FAILED. +no_declare+ is bunny's word for that;
      # the default exchange skips the declaration on its own name anyway.
      #
      # Registered once per exchange. The object is kept by the channel, which
      # is what makes throwing this one away safe.
      #
      # Anything still queued belongs to a publish that raised before it could
      # read its own return, and attributing it to this message would report the
      # wrong one as unroutable.
      def arm(channel, exchange)
        watch(channel, exchange)
        @returns.clear
      end

      # The same, for a batch: every exchange it publishes to is watched before
      # any of it goes out, and whatever was left over is dropped once rather
      # than between two publishes. A return for the message just sent arrives
      # while the next one is still being written, and clearing per message
      # would throw it away.
      def arm_all(channel, exchanges)
        exchanges.each { |exchange| watch(channel, exchange) }
        @returns.clear
      end

      # Forgets which exchanges are watched, for a channel that has been
      # reopened: the registrations lived on the old one and went with it.
      def reopened
        @watched = []
        @returns.clear
      end

      # Why the message just published was handed back, or nil when it was not.
      #
      # Non-blocking, because a return that was coming has already arrived: the
      # broker sends it ahead of the confirm and bunny dispatches frames in
      # order, so a wait here would put the cost of +mandatory+ on every message
      # rather than on the ones it is about.
      def take
        @returns.pop(true)&.last
      rescue ThreadError
        nil
      end

      # Everything queued, as pairs of message id and reason.
      #
      # A batch has to tell *which* of its messages came back rather than only
      # that one did, and the only thing that can say so is the returned
      # message's own +message-id+ — the returns arrive in whatever order the
      # broker sends them, and nothing about position survives the trip.
      def take_all
        taken = []
        loop { taken << @returns.pop(true) }
      rescue ThreadError
        taken
      end

      private

      # Registers one exchange, once. See {#arm} for why it must not declare.
      def watch(channel, exchange)
        name = exchange.to_s
        return if @watched.include?(name)

        ::Bunny::Exchange.new(channel, :direct, name, no_declare: true)
                         .on_return { |info, properties, _content| record(info, properties) }
        @watched << name
      end

      # Called on bunny's reader thread, so it does nothing but note the reason.
      #
      # The reply text is what the broker said and is worth keeping: NO_ROUTE is
      # not the only reason a message comes back, and the others are the
      # interesting ones. The id is kept with it because a batch is told which
      # message came back by nothing else.
      def record(info, properties)
        code = info.reply_code
        text = info.reply_text.to_s
        reason = text.empty? ? "returned #{code}" : "#{code} #{text}"
        @returns.push([properties[:message_id].to_s, reason])
      end
    end

    # One batch of messages, from the moment they are handed to a channel to
    # the moment every one of them has an answer.
    #
    # A class of its own because it is bookkeeping rather than transport: which
    # delivery tag belongs to which payload, which +basic.return+ belongs to
    # which message, and what each of the two sets bunny leaves behind means.
    # Every method on {Transport} is about one message and a channel; this is
    # the only thing in the file that has to hold a whole batch in its head.
    #
    # @api private
    class BatchPublish
      # @param messages [Array<Hash>] the keywords {Transport#publish} takes,
      #   one hash per message
      # @param returns [ReturnedMessages] the channel's returns, the same object
      #   a single mandatory publish reads
      def initialize(messages, returns)
        @messages = messages
        @returns = returns
        @results = Array.new(messages.size)
      end

      # Publishes every message, waits once, and answers for each of them.
      #
      # @return [Array<String, PublishError>] one entry per message, in the
      #   order they were given
      def run(channel)
        arm_returns(channel)
        confirm_each(channel, publish_each(channel))
        @results
      end

      private

      # Gets ready to hear about the exchanges this batch publishes to
      # mandatory. Nothing to do when none of them does.
      def arm_returns(channel)
        exchanges = @messages.filter_map { |message| message[:exchange] if message[:mandatory] }
        @returns.arm_all(channel, exchanges.uniq) unless exchanges.empty?
      end

      # Hands every message to the channel, and remembers which delivery tag
      # each one was given.
      #
      # The tag is read *after* the publish rather than before. bunny takes the
      # next sequence number as part of +basic_publish+, so a publish refused
      # before it got that far has consumed none, and a tag read in advance
      # would belong to the message after it — which is how a batch ends up
      # reporting the wrong message as the failed one.
      def publish_each(channel)
        tags = {}
        @messages.each_with_index do |message, index|
          channel.basic_publish(message[:body].to_s, message[:exchange],
                                message[:routing_key], publish_options(message))
          tags[channel.next_publish_seq_no - 1] = index
        rescue StandardError => e
          # One failure out of the batch rather than a reason to abandon the
          # messages already on the wire. They are going to be confirmed anyway,
          # and how many of them arrived is what the caller has to act on.
          @results[index] = PublishError.new(
            PublishFailure.not_published(message[:message_id], message[:exchange],
                                         message[:routing_key], e.message)
          )
        end
        tags
      end

      # Waits once for the whole batch, then reads each message's answer off
      # the channel.
      #
      # bunny does not report a failed publish per message. It answers the wait
      # with "were they all acked", leaves the delivery tags of the ones that
      # were not in +nacked_set+, and leaves the ones nothing ever answered for
      # in +unconfirmed_set+. Both are sets of tags rather than of messages,
      # which is what +tags+ is for: without it a batch could say how many
      # failed and not which, and its results would no longer line up with the
      # payloads that produced them.
      #
      # +nacked_set+ is never emptied by bunny, so it can hold tags from
      # publishes that finished long ago. Only this batch's tags are looked up
      # in it, which is what keeps an older failure from being reported twice.
      def confirm_each(channel, tags)
        broke = wait_for_batch(channel)
        nacked = channel.nacked_set.dup
        silent = channel.unconfirmed_set.dup
        returned = returns_for
        tags.each do |tag, index|
          unconfirmed = nacked.include?(tag) || silent.include?(tag)
          @results[index] = answer_for(@messages[index], returned, unconfirmed, broke)
        end
      end

      # What one message of a batch came to.
      def answer_for(message, returned, unconfirmed, broke)
        id = message[:message_id]
        if unconfirmed
          reason = PublishFailure.not_confirmed(id, message[:exchange], message[:routing_key])
          # The wait's own failure is worth carrying: "the broker said no" and
          # "the confirm never came" are the same missing acknowledgement and
          # two different things to go and look at.
          return PublishError.new(broke.nil? ? reason : "#{reason}: #{broke.message}")
        end
        return id unless (why = returned[id.to_s])

        PublishError.new(
          PublishFailure.not_routed(id, message[:exchange], message[:routing_key], why),
          unroutable: true
        )
      end

      # One wait for the batch, and the reason it ended early when it did.
      #
      # A wait that raises — a channel the broker took down, a confirm that
      # never came inside bunny's continuation timeout — is not allowed out on
      # its own. The messages it was waiting for still have to be answered one
      # by one, and an exception here would lose every one of those answers,
      # including the ones the broker had already acknowledged.
      def wait_for_batch(channel)
        channel.wait_for_confirms
        nil
      rescue StandardError => e
        e
      end

      # Which of these messages the broker handed back, keyed by message id.
      #
      # A +basic.return+ carries the returned message's own properties, so a
      # batch can ask which of its messages came back rather than only that one
      # did.
      def returns_for
        ids = @messages.filter_map { |m| m[:message_id].to_s if m[:mandatory] }
        return {} if ids.empty?

        @returns.take_all.each_with_object({}) do |(id, reason), by_id|
          named = charged_to(id, ids, by_id)
          by_id[named] = reason if named
        end
      end

      # A return whose id is not one of this batch's is charged to the first
      # mandatory message not already accounted for. The message cannot be
      # named — it went out without an id, or it belongs to somebody else's
      # publish — but it reached no queue, and the count has to say so.
      def charged_to(id, ids, already)
        return id if ids.include?(id)

        ids.find { |candidate| !already.key?(candidate) }
      end

      # The properties bunny publishes with, built from one batch message.
      def publish_options(message)
        { content_type: message[:content_type], message_id: message[:message_id],
          reply_to: Wire.presence(message[:reply_to]),
          mandatory: message[:mandatory] ? true : false,
          headers: Wire.stringify(message[:headers] || {}),
          persistent: message.fetch(:persistent, true) }
      end
    end

    # The broker, over AMQP 0-9-1.
    #
    # Everything above this class is about envelopes, codecs and retries and
    # knows nothing about channels; everything below it is bunny. The seam is
    # here so the consumer's retry arithmetic can be tested without a broker,
    # by handing {Connection} something else that answers these methods.
    class Transport
      # How this transport reaches RabbitMQ.
      #
      # Required lazily, and only here. The gem declares no runtime
      # dependencies because reading an AceMQ envelope should not oblige a
      # process to install a broker client it will never open a socket with —
      # an audit tool that parses headers out of a log has no business
      # compiling in an AMQP stack. The cost of that choice is that the failure
      # moves from install time to the first connection, so it is caught here
      # and re-raised saying what to install.
      def self.load_driver!
        require "bunny"
      rescue LoadError => e
        raise DependencyMissing,
              "the AceMQ transport needs the bunny gem, which is not installed. " \
              "Add `gem \"bunny\", \"~> 2.23\"` to your Gemfile, or run " \
              "`gem install bunny`. (#{e.message})"
      end

      # Opens a connection.
      #
      # The URL decides how the connection is protected unless a {Security} says
      # otherwise: +amqps://+ is encrypted and the broker is verified against
      # the machine's trust store, +amqp://+ is plaintext. Pass +security:+ for
      # a private certificate authority, a client certificate, or the deliberate
      # absence of verification; pass +credentials:+ to keep the password out of
      # the URL, and so out of the error message two lines below this one.
      #
      # The security options are merged over anything in +options+ rather than
      # under it. A caller who reaches past {Security} to bunny's own TLS keys
      # is describing the same thing twice, and of the two answers the one that
      # went through the checks in this library is the one to honour.
      #
      # @param url [String] amqp:// or amqps://
      # @param heartbeat [Integer, Symbol] seconds, or :server to take the
      #   broker's suggestion
      # @param connection_timeout [Numeric] seconds to wait for the handshake
      # @param security [Security, nil] how to protect the connection
      # @param credentials [Credentials, #call, nil] the broker login
      # @param options [Hash] anything else bunny understands
      # @return [Transport]
      # @raise [DependencyMissing] when bunny is not installed
      # @raise [ConfigurationError] when the security settings cannot be honoured
      # @raise [TransportError] when the broker cannot be reached
      def self.open(url, heartbeat: :server, connection_timeout: 10, security: nil,
                    credentials: nil, **options)
        load_driver!
        security = Security.for_connection(url, security: security, credentials: credentials)
        session = Bunny.new(url, heartbeat: heartbeat, connection_timeout: connection_timeout,
                                 **options, **security.to_transport_options)
        security.configure(session)
        session.start
        new(session)
      rescue DependencyMissing, ConfigurationError
        raise
      rescue StandardError => e
        raise TransportError, "cannot reach the broker at #{redact(url)}: #{e.message}"
      end

      # A URL with its password taken out, for an error message that will be
      # logged. A credential that reaches a log is a credential that has to be
      # rotated.
      #
      # @api private
      def self.redact(url)
        url.to_s.sub(%r{(://[^:/@]+):[^@]*@}, '\1:***@')
      end

      # @param session [Bunny::Session] an already started connection
      def initialize(session)
        @session = session
        @lock = Mutex.new
        # A lock of its own, because a pull holds messages unacknowledged across
        # a whole pass and settling one has to reach the channel it came down.
        # Sharing the publish lock would mean a replay's own republish waiting
        # on the channel it is about to acknowledge from.
        @pull_lock = Mutex.new
        @subscriptions = []
        @returns = ReturnedMessages.new
      end

      # The bunny session, for the things this class deliberately does not wrap.
      attr_reader :session

      # Creates an exchange unless it is already there.
      #
      # @param name [String]
      # @param kind [Symbol, String] direct, topic, fanout or headers
      def declare_exchange(name, kind:, durable: true, auto_delete: false, arguments: {})
        with_admin_channel do |channel|
          channel.exchange_declare(name, kind.to_s, durable: durable, auto_delete: auto_delete,
                                                    arguments: stringify(arguments))
        end
      end

      # Creates a queue unless it is already there.
      #
      # Deliberately without the quorum default {Connection#declare_queue} has:
      # this is the layer that does what it is told, and a caller who reached
      # past the connection to it is declaring exactly what they wrote down.
      # +queue_type+ is honoured when it is given, because {Topology#apply} can
      # be handed a transport and its plan says which kind each queue is.
      #
      # @param queue_type [Symbol, nil] +:classic+, +:quorum+ or +:stream+;
      #   nothing is added to the arguments when it is not given
      def declare_queue(name, queue_type: nil, durable: true, auto_delete: false,
                        exclusive: false, arguments: {})
        arguments = QueueType.table(queue_type, arguments) if queue_type
        with_admin_channel do |channel|
          channel.queue_declare(name, durable: durable, auto_delete: auto_delete,
                                      exclusive: exclusive, arguments: stringify(arguments))
        end
      end

      # Routes messages matching a key from an exchange to a queue.
      def bind(queue:, exchange:, routing_key: "")
        with_admin_channel do |channel|
          channel.queue_bind(queue, exchange, routing_key: routing_key)
        end
      end

      # Sends one message and waits for the broker to take responsibility.
      #
      # The wait is the point. Without publisher confirms a successful publish
      # means the bytes reached a socket, which is not the same as the broker
      # having them, and the difference only ever shows up as messages that
      # were never anywhere.
      #
      # +mandatory+ buys the other half of that. A confirm says the broker has
      # the message; it does not say the message reached a queue, and a publish
      # to an exchange with no matching binding is confirmed and dropped in the
      # same breath. Asking for +mandatory+ makes the broker hand such a message
      # back as +basic.return+ before it confirms it, which is the only way
      # AMQP will tell you. It costs a round trip only when the message really
      # is unroutable.
      #
      # @param reply_to [String, nil] AMQP's own +reply-to+ property, left off
      #   the message entirely when it is nil
      # @param mandatory [Boolean] whether reaching no queue is an error rather
      #   than a silence
      # @return [String] the message id it went out with
      # @raise [PublishError] when the broker did not confirm it, or when it was
      #   mandatory and reached no queue — {PublishError#unroutable?} tells the
      #   two apart
      def publish(exchange:, routing_key:, body:, content_type: nil, message_id: nil,
                  headers: {}, persistent: true, reply_to: nil, mandatory: false)
        returned = nil
        confirmed = publish_channel do |channel|
          @returns.arm(channel, exchange) if mandatory
          channel.basic_publish(body.to_s, exchange, routing_key,
                                content_type: content_type, message_id: message_id,
                                reply_to: presence(reply_to), mandatory: mandatory,
                                headers: stringify(headers), persistent: persistent)
          # The return, when there is one, arrives ahead of the confirm and is
          # dispatched on the reader thread while this one waits — so by the
          # time the wait is over it is already waiting to be taken.
          ok = channel.wait_for_confirms
          returned = @returns.take if mandatory
          ok
        end
        unless confirmed
          raise PublishError, PublishFailure.not_confirmed(message_id, exchange, routing_key)
        end
        return message_id if returned.nil?

        raise PublishError.new(
          PublishFailure.not_routed(message_id, exchange, routing_key, returned),
          unroutable: true
        )
      rescue PublishError
        raise
      rescue StandardError => e
        raise PublishError,
              PublishFailure.not_published(message_id, exchange, routing_key, e.message)
      end

      # Sends a batch, and waits for every confirm at the end.
      #
      # The difference from a loop around {#publish} is the whole point of the
      # method: there, every message costs a round trip to the broker and back
      # before the next one is written, because every message waits for its own
      # confirm. Here every message is handed to the channel first and the wait
      # happens once, for all of them — which is what publisher confirms were
      # designed for and what makes a thousand-message publish take about as
      # long as one.
      #
      # **It is not atomic**, and nothing in AMQP could make it so. A batch that
      # half arrived is the ordinary outcome of a broker problem partway
      # through, so each message gets its own answer rather than the batch
      # getting one between them.
      #
      # @param messages [Array<Hash>] the keywords {#publish} takes, one hash
      #   per message
      # @return [Array<String, PublishError>] one entry per message, in the
      #   order they were given: the id it went out with, or the failure it met
      def publish_all(messages)
        return [] if messages.empty?

        batch = BatchPublish.new(messages, @returns)
        publish_channel { |channel| batch.run(channel) }
      end

      # Delivers messages until the returned subscription is cancelled.
      #
      # The block runs on bunny's consumer pool rather than on the caller's
      # thread, so +concurrency+ is how many messages this subscription works
      # on at once. One by default, which keeps a queue's messages in the order
      # the broker offers them.
      #
      # @yieldparam delivery [Delivery]
      # @return [Subscription]
      def subscribe(queue, prefetch: 20, concurrency: 1, tag: nil, arguments: {}, &handler)
        channel = @session.create_channel(nil, concurrency)
        channel.prefetch(prefetch) if prefetch.positive?
        consumer = channel.queue(queue, passive: true).subscribe(
          manual_ack: true, block: false, consumer_tag: tag, arguments: stringify(arguments)
        ) do |info, properties, body|
          handler.call(delivery_from(channel, info, properties, body))
        end
        track(Subscription.new(channel, consumer))
      rescue StandardError => e
        channel.close if channel&.open?
        raise TransportError, "cannot consume from #{queue.inspect}: #{e.message}"
      end

      # Takes one message off a queue, without subscribing to it.
      #
      # A subscription is a standing arrangement; this is a single read, which
      # is what a pass over a queue with a beginning and an end needs. Draining
      # a dead-letter queue with a consumer means writing the code that decides
      # when to stop, and getting it wrong means a tool that never exits.
      #
      # The delivery is unacknowledged, and settling it is the caller's job.
      # That is deliberate: a message left unacknowledged is held by the broker
      # rather than lost, so a tool that dies half way through a pass returns
      # everything it was holding.
      #
      # @param queue [String]
      # @return [Delivery, nil] nil when the queue has nothing waiting
      def pull(queue)
        @pull_lock.synchronize do
          channel = pull_channel
          info, properties, body = channel.basic_get(queue, manual_ack: true)
          next nil if info.nil?

          delivery_from(channel, info, properties, body, lock: @pull_lock)
        end
      rescue StandardError => e
        raise TransportError, "cannot read a message from #{queue.inspect}: #{e.message}"
      end

      # How many messages are waiting on a queue.
      #
      # A number for a dashboard or a test, not a decision to make in a
      # handler: it is a snapshot of a queue that is still moving, and it is
      # already wrong by the time it is read anywhere else.
      def message_count(queue)
        with_admin_channel do |channel|
          channel.queue_declare(queue, passive: true).message_count
        end
      end

      # Whether a queue is on the broker, creating nothing.
      #
      # Its own question because a declaration cannot answer it: declaring a
      # queue that is missing creates it, so anything built on declarations
      # would create the very queues it was asked only to look for.
      def queue_exists?(name)
        @session.queue_exists?(name)
      end

      # Removes a queue and every message still on it.
      #
      # For tests and for tools. A service that deletes queues has usually
      # confused a queue with a session, and the messages go with it —
      # including the ones somebody was about to be paid for.
      def delete_queue(name)
        with_admin_channel { |channel| channel.queue_delete(name) }
      end

      # Removes an exchange.
      def delete_exchange(name)
        with_admin_channel { |channel| channel.exchange_delete(name) }
      end

      # Whether the connection is up.
      def open? = @session.open?

      # Cancels every subscription and closes the connection.
      def close
        # Taken out under the lock and cancelled outside it. A handler still
        # running is about to want this same lock to publish a dead letter, and
        # holding it across a cancel that waits on the broker is how a shutdown
        # becomes a deadlock nobody can reproduce.
        subscriptions = @lock.synchronize do
          taken = @subscriptions
          @subscriptions = []
          taken
        end
        subscriptions.each(&:cancel)

        @lock.synchronize do
          @publish_channel.close if @publish_channel&.open?
          @publish_channel = nil
        end
        # Anything a pass was still holding goes back to its queue when this
        # closes, which is the whole reason a declined message is held rather
        # than returned one at a time.
        @pull_lock.synchronize do
          @pull_channel.close if @pull_channel&.open?
          @pull_channel = nil
        end
        @session.close
        nil
      rescue StandardError => e
        raise TransportError, "cannot close the connection cleanly: #{e.message}"
      end

      # A running subscription.
      #
      # Stopping and closing are two steps rather than one because a handler
      # that is still running needs its channel: closing it out from under an
      # in-flight acknowledgement loses the message the handler just finished.
      # {Consumer#cancel} stops delivery, waits, and only then closes.
      class Subscription
        def initialize(channel, consumer)
          @channel = channel
          @consumer = consumer
        end

        # Whether the broker can still deliver down this subscription.
        #
        # The channel's own answer rather than a flag kept here. The two can
        # disagree: a channel closed by the broker, or taken down by an error on
        # it, stops delivery without anything in this process being told, and a
        # health check reading a local flag would report a consumer that had
        # been deaf for an hour as running.
        def open? = @channel.open?

        # Tells the broker to send no more. Deliveries already handed over are
        # unaffected.
        #
        # Both steps check the channel first, and both are safe to call twice.
        # A connection is closed by whatever gets there first — the consumer it
        # owns, or the connection closing every consumer it tracks — and a
        # basic.cancel on a channel that has already gone is a CHANNEL_ERROR,
        # which RabbitMQ answers by dropping the whole connection. The symptom
        # is a shutdown that takes ten seconds while bunny reconnects to a
        # process that is exiting.
        def stop
          @consumer.cancel if @channel.open?
          nil
        rescue StandardError
          # Cancelling on a connection that has already gone is what shutdown
          # looks like when the broker went first, and raising here would turn
          # a tidy exit into a stack trace about a socket nobody is waiting on.
          nil
        end

        # Releases the channel. Nothing may be acknowledged after this.
        def close
          @channel.close if @channel.open?
          nil
        rescue StandardError
          nil
        end

        # Both, for a caller with no handlers to wait for.
        def cancel
          stop
          close
        end
      end

      private

      # The channel every publish goes down, opened once and guarded.
      #
      # One channel rather than one per publish because a channel is a
      # round trip to open and confirms have to be enabled on it; guarded
      # because a bunny channel is not safe to use from two threads at once,
      # and a consumer thread dead-lettering a message publishes on this same
      # channel while an application thread may be publishing its own.
      def publish_channel
        @lock.synchronize do
          if @publish_channel.nil? || !@publish_channel.open?
            @publish_channel = @session.create_channel
            @publish_channel.confirm_select
            @returns.reopened
          end
          yield @publish_channel
        end
      end

      # A channel of its own for each declaration, because a refused
      # declaration kills the channel it was made on.
      #
      # A queue that already exists with different arguments answers
      # PRECONDITION_FAILED, which is the only way AMQP reports drift without
      # the management API. Sharing a channel would mean one such refusal took
      # every later declaration down with it, and the error nobody would then
      # be able to explain is the second one.
      def with_admin_channel
        channel = @session.create_channel
        yield channel
      rescue StandardError => e
        raise TransportError, e.message
      ensure
        channel.close if channel&.open?
      end

      def track(subscription)
        @lock.synchronize { @subscriptions << subscription }
        subscription
      end

      # +lock+ is given for a pulled delivery, which is settled by whoever is
      # running the pass rather than on the thread it arrived on, and may be
      # settled long after. A subscribed delivery needs none: it is settled on
      # the consumer thread that owns its channel.
      def delivery_from(channel, info, properties, body, lock: nil)
        tag = info.delivery_tag
        settle = lambda do |&action|
          lock ? lock.synchronize { action.call } : action.call
        end
        Delivery.new(
          body: body,
          content_type: properties[:content_type].to_s,
          routing_key: info.routing_key.to_s,
          message_id: properties[:message_id].to_s,
          headers: properties[:headers] || {},
          redelivered: info.redelivered,
          reply_to: properties[:reply_to].to_s,
          on_ack: -> { settle.call { channel.ack(tag, false) } },
          on_nack: ->(requeue) { settle.call { channel.nack(tag, false, requeue) } }
        )
      end

      # The channel every pull goes down, opened once.
      #
      # One channel, because the messages a pass declines are held
      # unacknowledged on it until the pass ends, and a channel per pull would
      # release them the moment it closed.
      def pull_channel
        @pull_channel = @session.create_channel if @pull_channel.nil? || !@pull_channel.open?
        @pull_channel
      end

      # See {Wire}, which {BatchPublish} writes its properties with too.
      def stringify(table) = Wire.stringify(table)
      def presence(value) = Wire.presence(value)
    end
  end
end
