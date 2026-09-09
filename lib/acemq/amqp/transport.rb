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

    # A message the broker would not take.
    class PublishError < TransportError; end

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
      # @param reply_to [String, nil] AMQP's own +reply-to+ property, left off
      #   the message entirely when it is nil
      # @return [String] the message id it went out with
      # @raise [PublishError] when the broker did not confirm it
      def publish(exchange:, routing_key:, body:, content_type: nil, message_id: nil,
                  headers: {}, persistent: true, reply_to: nil)
        confirmed = publish_channel do |channel|
          channel.basic_publish(body.to_s, exchange, routing_key,
                                content_type: content_type, message_id: message_id,
                                reply_to: presence(reply_to),
                                headers: stringify(headers), persistent: persistent)
          channel.wait_for_confirms
        end
        return message_id if confirmed

        raise PublishError,
              "the broker would not confirm message #{message_id} on exchange " \
              "#{exchange.inspect} with key #{routing_key.inspect}"
      rescue PublishError
        raise
      rescue StandardError => e
        raise PublishError,
              "cannot publish message #{message_id} to exchange #{exchange.inspect} " \
              "with key #{routing_key.inspect}: #{e.message}"
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
  end
end
