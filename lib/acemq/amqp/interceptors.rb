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

require_relative "envelope"

module AceMQ
  module AMQP
    # A message on its way out, as an interceptor sees it.
    #
    # Everything on it can be changed, and changing it changes what is sent:
    # the exchange and the routing key redirect the message, the envelope
    # carries the headers, and the payload is the value that has not been
    # encoded yet — so an interceptor can rewrite it while it is still a Ruby
    # object rather than trying to patch bytes.
    #
    # The envelope is replaced rather than mutated, because an {Envelope} is
    # frozen on purpose: what a log line said about a message should not change
    # after it was written. {#set_header} does the replacing, which is what most
    # interceptors want.
    class PublishContext
      attr_accessor :exchange, :routing_key, :envelope, :payload

      def initialize(exchange:, routing_key:, envelope:, payload:)
        @exchange = exchange
        @routing_key = routing_key
        @envelope = envelope
        @payload = payload
      end

      # Adds an application header to the message about to be sent.
      #
      # Reserved +x-acemq-+ names are refused, here as everywhere: an
      # interceptor that quietly overwrote the attempt count would break the
      # retry engine from outside it, with nothing in the code to show why.
      #
      # @raise [ArgumentError] when the name is reserved
      def set_header(name, value)
        @envelope = @envelope.with(headers: @envelope.headers.merge(name.to_s => value))
      end

      def to_s = "publish #{@envelope.id} to #{@exchange.inspect}/#{@routing_key}"
    end

    # A message that has arrived, as an interceptor sees it before the handler.
    #
    # The payload is already decoded, which is the useful moment: an interceptor
    # that had to decode the body itself would be doing the codec's work twice
    # and could disagree with it.
    #
    # The envelope can be replaced, and what replaces it is what the handler
    # receives and what any dead letter is written with — so an interceptor can
    # stamp something onto a message on the way in and have it survive all the
    # way to the queue somebody looks at.
    class ConsumeContext
      attr_accessor :envelope
      attr_reader :queue, :payload, :body, :content_type, :redelivered

      def initialize(queue:, envelope:, payload:, body:, content_type:, redelivered:)
        @queue = queue
        @envelope = envelope
        @payload = payload
        @body = body
        @content_type = content_type
        @redelivered = redelivered
      end

      def redelivered? = !!@redelivered

      # @raise [ArgumentError] when the name is reserved
      def set_header(name, value)
        @envelope = @envelope.with(headers: @envelope.headers.merge(name.to_s => value))
      end

      def to_s = "handle #{@envelope.id} from #{@queue}"
    end

    # The interceptors registered on a connection, and the rules for running
    # them.
    #
    # The extension point for the things every message in an organisation needs
    # and no library can guess: a tenant identifier, a trace context, a log
    # scope, a size limit, a metric. Without a seam they end up copied into
    # every call site, where one of them is always missing.
    #
    #   mq.intercept_publish { |context| context.set_header("tenant", Current.tenant) }
    #
    #   mq.intercept_consume do |context|
    #     raise AceMQ::AMQP::FatalError, "not our tenant" unless ours?(context)
    #   end
    #
    # An interceptor is anything answering the hooks it cares about, so the
    # common case is a block and the full case is an object:
    #
    #   class Timing
    #     def order = -100
    #     def before_handle(context) = @started[context.envelope.id] = now
    #     def after_handle(context, ack) = record(context, ack, now - @started.delete(...))
    #   end
    #
    #   mq.intercept_consume(Timing.new)
    #
    # Nothing here needs anything private. An interceptor is handed a context
    # whose every field is public and is registered through a public method, so
    # everything in this library that could have been an interceptor could have
    # been written by somebody outside it — which is the same rule the patterns
    # follow, and the only way to know a seam is wide enough.
    #
    # == What a failure means
    #
    # Raising from +before_publish+ *stops the publish*, and the caller sees the
    # exception. That is the point of intercepting rather than observing: a
    # message that must not go out is stopped once, here, rather than in every
    # publisher.
    #
    # Raising from +before_handle+ means the handler never runs and the delivery
    # is treated exactly as a failed handler would be — retried, then
    # dead-lettered. An interceptor that refuses a message has to be willing for
    # that message to end up in the dead-letter queue, which is the honest
    # outcome; the alternative is acknowledging something nothing processed.
    # {FatalError} still means what it means, so an interceptor that refuses for
    # a reason retrying cannot fix can say so.
    #
    # Raising from +after_confirm+, +after_handle+ or +on_error+ is reported on
    # stderr and otherwise ignored. By then the message has been sent or the
    # delivery settled, and an exception cannot un-send or un-settle it: letting
    # it out would report a successful publish as a failed one, or skip the
    # remaining teardowns.
    #
    # == Order
    #
    # Lower +order+ runs first, and interceptors with the same order run in the
    # order they were registered. On the way out of a handler the order is
    # reversed, so a pair that opens something on the way in and closes it on
    # the way out nests properly: the first to open is the last to close.
    #
    # An interceptor is called from whatever thread is publishing or handling,
    # so one that keeps state has to be safe to call from several at once.
    class Interceptors
      # One registration: what to call, where it sits, and when it arrived.
      #
      # The sequence is kept because Ruby's sort is not stable, and "register
      # these two in this sequence" has to keep meaning something once a third
      # with the same order joins them.
      Registered = Struct.new(:before, :after, :error, :order, :sequence, keyword_init: true)

      def initialize
        @lock = Mutex.new
        @publishing = [].freeze
        @consuming = [].freeze
        @registered = 0
      end

      # Adds an interceptor to every publish on this connection.
      #
      # @param interceptor [#before_publish, nil] or nil when a block is given
      # @param order [Integer, nil] lower runs first; nil asks the interceptor
      # @yieldparam context [PublishContext] for the common case of one hook
      # @return [Interceptors] self
      def add_publish(interceptor = nil, order: nil, &block)
        add(:@publishing, interceptor, block, order,
            before: :before_publish, after: :after_confirm)
      end

      # Adds an interceptor to every consumer on this connection.
      #
      # @param interceptor [#before_handle, nil] or nil when a block is given
      # @param order [Integer, nil] lower runs first; nil asks the interceptor
      # @yieldparam context [ConsumeContext] for the common case of one hook
      # @return [Interceptors] self
      def add_consume(interceptor = nil, order: nil, &block)
        add(:@consuming, interceptor, block, order,
            before: :before_handle, after: :after_handle)
      end

      # Whether anything at all is registered, which is what a caller checks
      # before doing work only an interceptor would want done.
      def empty? = @publishing.empty? && @consuming.empty?

      # How many are registered on each side.
      def publishing = @publishing.size
      def consuming = @consuming.size

      # Runs every +before_publish+, in order. An exception is let straight out:
      # refusing a publish is the whole reason a policy interceptor exists, and
      # swallowing it here would turn "this message is not allowed" into "this
      # message was sent".
      #
      # @param context [PublishContext] changed in place by the interceptors
      # @return [PublishContext] the same context
      def before_publish(context)
        @publishing.each { |entry| entry.before&.call(context) }
        context
      end

      # Runs every +after_confirm+, once the broker has the message.
      def after_confirm(context)
        @publishing.each do |entry|
          safely(entry.after, "after a publish") { entry.after.call(context) }
        end
        context
      end

      # Runs every +on_error+ after a publish failed, including when an
      # interceptor refused it. Never allowed to replace the original failure:
      # the caller needs to see what went wrong, not what an observer did about
      # it.
      def on_publish_error(context, failure)
        report(@publishing, context, failure, "a failed publish")
      end

      # Runs every +before_handle+, in order. An exception is let out and the
      # consumer treats it as a handler failure, which is the honest outcome for
      # a refused message.
      def before_handle(context)
        @consuming.each { |entry| entry.before&.call(context) }
        context
      end

      # Runs every +after_handle+, in reverse order, after the handler has
      # returned or raised and before the delivery is settled.
      def after_handle(context, ack)
        @consuming.reverse_each do |entry|
          safely(entry.after, "after a handler") { entry.after.call(context, ack) }
        end
        context
      end

      # Runs every +on_error+ after a handler raised.
      def on_consume_error(context, failure)
        report(@consuming, context, failure, "a failed handler")
      end

      private

      def add(slot, interceptor, block, order, before:, after:)
        if interceptor.nil? && block.nil?
          raise ArgumentError, "an interceptor needs an object or a block"
        end
        if interceptor && block
          raise ArgumentError, "an interceptor is an object or a block, not both"
        end

        entry = entry_for(interceptor, block, order, before: before, after: after)
        @lock.synchronize do
          entry.sequence = (@registered += 1)
          # Replaced whole rather than sorted in place. A consumer on another
          # thread reads this list while it is being changed, and a list that is
          # briefly empty is a message that briefly has no interceptors — which
          # is a message that should have been stamped or refused and was not.
          current = instance_variable_get(slot) + [entry]
          instance_variable_set(slot, current.sort_by { |e| [e.order, e.sequence] }.freeze)
        end
        self
      end

      # The hooks an interceptor actually has, worked out once at registration.
      #
      # Asking +respond_to?+ per message would be the same answer computed a
      # million times, and a block is turned into the same shape as an object so
      # that running them is one code path rather than two.
      def entry_for(interceptor, block, order, before:, after:)
        return Registered.new(before: block, order: order || 0) if block

        Registered.new(
          before: hook(interceptor, before),
          after: hook(interceptor, after),
          error: hook(interceptor, :on_error),
          order: order || (interceptor.respond_to?(:order) ? interceptor.order : 0)
        )
      end

      def hook(interceptor, name)
        interceptor.respond_to?(name) ? interceptor.method(name) : nil
      end

      def report(entries, context, failure, what)
        entries.each do |entry|
          safely(entry.error, "while handling #{what}") { entry.error.call(context, failure) }
        end
        context
      end

      # An interceptor that fails on the way out is reported and stepped over.
      #
      # Written to stderr rather than to a logger this library does not have.
      # Silence would be worse: an interceptor that has been raising on every
      # message since a deploy is something somebody needs to be told about, and
      # the one place it could be noticed is here.
      def safely(hook, moment)
        return if hook.nil?

        yield
      rescue StandardError => e
        warn("acemq: an interceptor raised #{e.class} #{moment}; " \
             "the message was handled anyway: #{e.message}")
      end
    end
  end
end
