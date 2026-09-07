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

module AceMQ
  module AMQP
    # The broker, or the network, rather than the message.
    #
    # Worth telling apart from a bad message because it says nothing about the
    # message: the same one may well go through once the broker is back.
    class TransportError < StandardError; end

    # A message the broker would not take.
    class PublishError < TransportError; end

    # The gem this transport needs is not installed.
    #
    # Its own class so a caller can rescue it and fall back, and its message
    # names the gem rather than leaving somebody to work out which library
    # +cannot load such file -- bunny+ was talking about.
    class DependencyMissing < StandardError; end

    # One message, as it arrived, before any codec has looked at it.
    #
    # +redelivered+ is the broker saying it has handed these bytes over before,
    # and it is the only signal that a delivery is a retry. The attempt header
    # cannot serve: a broker requeues the bytes it was given, so the header
    # still reads whatever the publisher wrote however many times the message
    # has come round.
    #
    # Settling travels with the delivery as +on_ack+ and +on_nack+ rather than
    # as a delivery tag the consumer would have to hand back to the right
    # channel. That keeps the consumer from knowing which channel a message
    # came down, which is the whole reason a fake transport can stand in for a
    # broker in a test.
    Delivery = Struct.new(
      :body, :content_type, :routing_key, :message_id, :headers, :redelivered,
      :on_ack, :on_nack, keyword_init: true
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
      # @param url [String] amqp:// or amqps://
      # @param heartbeat [Integer, Symbol] seconds, or :server to take the
      #   broker's suggestion
      # @param connection_timeout [Numeric] seconds to wait for the handshake
      # @param options [Hash] anything else bunny understands
      # @return [Transport]
      # @raise [DependencyMissing] when bunny is not installed
      # @raise [TransportError] when the broker cannot be reached
      def self.open(url, heartbeat: :server, connection_timeout: 10, **options)
        load_driver!
        session = Bunny.new(url, heartbeat: heartbeat, connection_timeout: connection_timeout,
                                 **options)
        session.start
        new(session)
      rescue DependencyMissing
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
      def declare_queue(name, durable: true, auto_delete: false, exclusive: false,
                        arguments: {})
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
      # @return [String] the message id it went out with
      # @raise [PublishError] when the broker did not confirm it
      def publish(exchange:, routing_key:, body:, content_type: nil, message_id: nil,
                  headers: {}, persistent: true)
        confirmed = publish_channel do |channel|
          channel.basic_publish(body.to_s, exchange, routing_key,
                                content_type: content_type, message_id: message_id,
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

      def delivery_from(channel, info, properties, body)
        tag = info.delivery_tag
        Delivery.new(
          body: body,
          content_type: properties[:content_type].to_s,
          routing_key: info.routing_key.to_s,
          message_id: properties[:message_id].to_s,
          headers: properties[:headers] || {},
          redelivered: info.redelivered,
          on_ack: -> { channel.ack(tag, false) },
          on_nack: ->(requeue) { channel.nack(tag, false, requeue) }
        )
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
    end
  end
end
