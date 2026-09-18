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

require "securerandom"

require_relative "../ack"
require_relative "../queue_type"
require_relative "../retry_policy"

module AceMQ
  module AMQP
    # Asking a question and waiting for the answer.
    #
    # Messaging is asynchronous and request-reply is a synchronous shape drawn
    # on top of it, which is a real cost rather than a free convenience: a
    # caller blocked on a reply is holding a thread, a connection and a
    # deadline, and a queue that backs up turns into a service that stops
    # responding. Reach for it where a caller genuinely cannot go on without
    # the answer, and publish an event otherwise.
    module Patterns
      # No reply arrived before the deadline.
      #
      # It says nothing about whether the request was handled. A timeout is the
      # absence of an answer, not evidence that nothing happened — which is why
      # a request that changes anything wants {Patterns.idempotent} on the other
      # end.
      class RequestTimedOut < StandardError; end

      # The responder answered, and the answer was a failure.
      class ResponderFailed < StandardError; end

      # Where a responder should send its answer.
      #
      # An application header as well as AMQP's own +reply-to+ property, and the
      # two always say the same thing. The header travels through the same
      # envelope machinery as everything else and survives a hop through a
      # service that rebuilds the message; the property is what a broker, a
      # management console and the Java and .NET libraries understand. Writing
      # only one of them is what kept a Java requester and a Ruby responder from
      # talking to each other.
      #
      # The rule is the same in all five libraries: a requester writes both, and
      # a responder reads the header first and falls back to the property.
      # Header first because it is the one that survives a rebuild — a service
      # that reads a message and publishes a new one keeps the headers and
      # usually drops the properties.
      #
      # Deliberately without the +x-acemq-+ prefix: that namespace belongs to
      # the engine and is kept away from application headers, so a responder
      # could never read this one if it were in there.
      REPLY_TO_HEADER = "acemq-reply-to"

      # A responder's failure, carried back to whoever is waiting.
      ERROR_HEADER = "acemq-error"

      # Sends a request and waits for the reply.
      #
      #   prices = Patterns::Requester.new(mq, to: "price.requests")
      #   quote = prices.call({ "sku" => "X-1" })
      #   prices.close
      #
      # One requester is meant to be kept and reused. It holds a queue and a
      # consumer, and building one per request means a queue per request.
      class Requester
        # How long to wait for an answer when nothing else is said.
        DEFAULT_TIMEOUT = 30.0

        attr_reader :reply_queue, :timeout

        # @param connection [Connection]
        # @param to [String] the routing key requests go out on
        # @param exchange [String] empty for the default exchange
        # @param reply_to [String, nil] the queue replies come back on. One is
        #   generated when this is not given: exclusive, transient and
        #   auto-deleting, belonging to this process and going away with it,
        #   because a reply queue that outlived its requester would collect
        #   answers nobody is waiting for. Name one only when replies have to
        #   survive a restart.
        # @param timeout [Numeric] seconds to wait for a reply
        def initialize(connection, to:, exchange: "", reply_to: nil, timeout: DEFAULT_TIMEOUT)
          @connection = connection
          @to = to
          @exchange = exchange
          @timeout = timeout.to_f
          @generated = reply_to.nil? || reply_to.to_s.empty?
          @reply_queue = @generated ? "acemq-reply-#{SecureRandom.uuid}" : reply_to.to_s
          @waiting = {}
          @lock = Mutex.new
          declare_reply_queue
          # No retry policy on the reply consumer, whatever the connection's is.
          # This handler cannot fail — it hands a message to a waiter and
          # accepts — so a policy could only add rung queues to an ephemeral
          # reply queue that is about to be deleted.
          @consumer = connection.consume(@reply_queue, retry_policy: RetryPolicy.none) do |m|
            deliver(m)
            Ack.accept
          end
        end

        # Sends a request and waits for its reply.
        #
        # @param request [Object] anything the codec will encode
        # @param fields [Hash] envelope fields for the request
        # @return [Object] the reply's payload
        # @raise [RequestTimedOut] when nothing came back in time
        # @raise [ResponderFailed] when the answer was a failure
        def call(request, **fields)
          # The correlation identifier is what pairs a reply with its request,
          # so it is generated here rather than taken from the caller: a caller
          # reusing one across two requests would have them answer each other.
          correlation = SecureRandom.uuid
          waiter = Waiter.new
          @lock.synchronize { @waiting[correlation] = waiter }
          # A monotonic clock, because this is a duration: the wall clock can
          # step backwards over an NTP correction and hand a round trip a
          # negative one.
          started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          # The pessimistic word until something better is known. Every way out
          # of the block below sets it, including the raise, and a failure that
          # is none of the three named ones is still a round trip that did not
          # come back with an answer.
          outcome = Telemetry::Outcome::FAILED
          type = nil

          begin
            type = publish(request, correlation, fields).type
            reply = answer(waiter.await(@timeout), correlation)
            outcome = Telemetry::Outcome::ANSWERED
            reply
          rescue RequestTimedOut
            outcome = Telemetry::Outcome::TIMED_OUT
            raise
          ensure
            @lock.synchronize { @waiting.delete(correlation) }
            record(type, outcome, Process.clock_gettime(Process::CLOCK_MONOTONIC) - started)
          end
        end

        # Stops consuming replies. The generated queue goes with it.
        def close
          @consumer&.cancel
          @consumer = nil
          nil
        end

        private

        # The generated queue is classic and could be nothing else. RabbitMQ
        # refuses an exclusive or auto-delete quorum queue outright, so a reply
        # queue that goes away with the requester that made it is not a queue
        # the broker will replicate — which is the one place the library's
        # quorum default would have declared something undeclarable, and the
        # reason it is written down here rather than left to the flags.
        #
        # A named one is left to that default, so it comes out quorum. That is
        # what makes it safe to name a queue a topology also declares: both
        # declarations then say the same thing, and the second one is accepted
        # rather than refused with PRECONDITION_FAILED.
        def declare_reply_queue
          if @generated
            @connection.declare_queue(@reply_queue, queue_type: QueueType::CLASSIC,
                                                    durable: false, auto_delete: true,
                                                    exclusive: true)
          else
            @connection.declare_queue(@reply_queue)
          end
        end

        # Both places, the same name. The header is what a responder reads
        # first; the property is what a responder written against another
        # library reads, and what a broker's own tooling shows.
        def publish(request, correlation, fields)
          envelope = fields.merge(
            correlation_id: correlation,
            headers: fields.fetch(:headers, {}).merge(REPLY_TO_HEADER => @reply_queue)
          )
          @connection.publish(request, to: @to, exchange: @exchange,
                                       reply_to: @reply_queue, **envelope)
        end

        # The round trip as the caller experienced it.
        #
        # One counter and one distribution, named and tagged as Java and .NET
        # name and tag theirs, because a request-reply dashboard is read across
        # the fleet rather than per language. The publish and the reply are
        # already counted by +acemq.publish.total+ and +acemq.consume.total+ —
        # what neither of them can say is how long the *caller* waited, because
        # the caller's wait spans two queues, two processes and a responder,
        # and no single message's metrics see all of it.
        #
        # The type is the envelope's own, read off what was really published
        # rather than off the fields handed in: an interceptor is allowed to
        # change it, and a tag that disagreed with the message would be worse
        # than no tag. It is nil only when the publish itself failed, and an
        # empty string is the honest value there.
        def record(type, outcome, seconds)
          tags = { Telemetry::TAG_ROUTING_KEY => @to, Telemetry::TAG_MESSAGE_TYPE => type.to_s,
                   outcome: outcome }
          telemetry = @connection.telemetry
          telemetry.count(Telemetry::REQUEST_TOTAL, 1, **tags)
          telemetry.observe(Telemetry::REQUEST_DURATION, seconds, **tags)
        end

        def answer(message, correlation)
          if message.nil?
            raise RequestTimedOut,
                  "no reply to #{correlation} arrived within #{@timeout} seconds"
          end

          failure = message.envelope.headers[ERROR_HEADER]
          raise ResponderFailed, "the responder failed: #{failure}" if failure

          message.payload
        end

        # Hands a reply to whoever is waiting for it.
        #
        # A reply nobody is waiting for is dropped, which is what a reply to a
        # request that already timed out is. Blocking here would stall the reply
        # consumer for everybody else waiting on the same queue.
        def deliver(message)
          waiter = @lock.synchronize { @waiting.delete(message.envelope.correlation_id) }
          waiter&.deliver(message)
        end

        # One caller's wait for one reply.
        #
        # A condition variable rather than a Queue with a timeout, because
        # +Queue#pop(timeout:)+ arrived in Ruby 3.2 and this library runs on 3.1.
        #
        # @api private
        class Waiter
          def initialize
            @lock = Mutex.new
            @ready = ConditionVariable.new
            @message = nil
            @delivered = false
          end

          def deliver(message)
            @lock.synchronize do
              @message = message
              @delivered = true
              @ready.signal
            end
          end

          # @return [Message, nil] nil when the deadline passed first
          def await(timeout)
            deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
            @lock.synchronize do
              until @delivered
                left = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
                return nil if left <= 0

                # Waited in a loop because a condition variable may wake for no
                # reason, and a spurious wake would otherwise look like a reply.
                @ready.wait(@lock, left)
              end
              @message
            end
          end
        end
      end

      # Answers requests on a queue.
      #
      #   responder = Patterns.serve(mq, "price.requests") do |message|
      #     { "price" => catalogue.price(message.payload["sku"]) }
      #   end
      #
      # The block returns the response rather than an {Ack}: a responder's job
      # is to answer, and what to do with the request afterwards follows from
      # whether it could. Raising sends the failure back to the caller, because
      # a caller blocked on a reply should learn that it failed rather than wait
      # out its timeout.
      #
      # @param connection [Connection]
      # @param queue [String] where requests arrive
      # @param options [Hash] passed to {Connection#consume}
      # @return [Responder]
      def self.serve(connection, queue, **options, &handler)
        raise ArgumentError, "Patterns.serve needs a block to answer with" unless handler

        # Made before the subscribe rather than after it, and closed over by the
        # handler rather than reached through the responder this returns. A
        # broker may hand the first request over from inside the subscribe —
        # which is what a queue with a backlog looks like from in here — and a
        # handler reaching for a responder the subscribe has not returned yet
        # would answer that request and count nothing. Silent, and only ever at
        # start-up, which is the worst place to lose the first number of the
        # day. .NET had to lift its counters out of the responder for exactly
        # this, and Java gets it from field initialisers running before the
        # constructor body.
        counters = Responder::Counters.new
        consumer = connection.consume(queue, **options) do |message|
          reply_to = reply_address(message)
          if reply_to.empty?
            # Counted before the delivery is settled, and settled rather than
            # retried: retrying cannot make a reply queue appear, so this is
            # dead-lettered. Anything above zero here means a caller is
            # publishing where it meant to request.
            counters.unanswerable!
            next Ack.reject(FatalError.new(
                              "request #{message.id} carries neither the #{REPLY_TO_HEADER} " \
                              "header nor a reply-to property, so there is nowhere to reply"
                            ))
          end

          answer(connection, reply_to, message, handler, counters)
        end
        Responder.new(consumer, counters)
      end

      # A running responder, and the two numbers it keeps.
      #
      #   responder = Patterns.serve(mq, "price.requests") { |m| price(m.payload) }
      #   responder.answered       # requests answered, counted before each reply left
      #   responder.unanswerable   # requests that named nowhere to reply
      #
      # The same two numbers Java's +Responder.answered()+ and
      # +unanswerable()+ report, with the same promise about when they can be
      # read: **never a wait**. Both are in place before the first delivery, and
      # {#answered} is incremented *before* the reply is published. Code that
      # sleeps before reading one is working around a defect that is not here.
      #
      # Everything else about it is the consumer underneath, which is what
      # {Patterns.serve} used to return: {#cancel}, {#running?} and {#queue}
      # are that consumer's, and {#consumer} is the consumer itself for
      # anything this does not forward.
      class Responder
        # @api private
        def initialize(consumer, counters)
          @consumer = consumer
          @counters = counters
        end

        # The consumer underneath, for whatever this does not forward.
        attr_reader :consumer

        # How many requests were answered, counted before each reply left.
        #
        # A caller holding a reply can rely on this having counted it: the
        # increment happens before the publish, so there is no interleaving in
        # which the answer is visible and the number is not. The other order
        # reads more naturally and is wrong — it leaves a window where a reply
        # is in the caller's hands and the responder still says nothing has been
        # answered, which is a dashboard reporting an idle service that is
        # demonstrably working.
        #
        # A publish that fails hands its increment back, so this counts replies
        # that were sent rather than replies that were attempted.
        #
        # A responder that raised is counted here too, because a Ruby responder
        # *answers* that request: the failure goes back to the caller in
        # +acemq-error+ and the caller raises {ResponderFailed} rather than
        # waiting out its deadline. Java's responder does not reply at all in
        # that case and so does not count it — the divergence is in what the two
        # do with a failure, not in what the counter means. Split the two apart
        # with +acemq.consume.total+, where the same delivery is +acked+ or
        # +rejected+.
        def answered = @counters.answered

        # How many requests arrived naming nowhere to reply, counted before the
        # delivery is settled.
        #
        # Anything above zero means a caller is publishing where it means to
        # request. Nothing can answer such a request and nothing about
        # redelivering it would make a reply address appear, so it is
        # dead-lettered once rather than looped.
        def unanswerable = @counters.unanswerable

        # Whether the broker is still sending this responder requests.
        def running? = @consumer.running?

        # The queue requests arrive on.
        def queue = @consumer.queue

        # Stops serving, draining the requests already in hand.
        #
        # A request being answered right now has a caller blocked on the other
        # side, and cutting it off turns their call into a timeout.
        def cancel(timeout: Connection::DRAIN_TIMEOUT) = @consumer.cancel(timeout: timeout)

        # The two numbers, held apart from the responder that reports them.
        #
        # A class of its own so the handler can close over it: see
        # {Patterns.serve} for why reaching them through the responder is a
        # number lost at start-up.
        #
        # @api private
        class Counters
          def initialize
            @lock = Mutex.new
            @answered = 0
            @unanswerable = 0
          end

          def answered = @lock.synchronize { @answered }
          def unanswerable = @lock.synchronize { @unanswerable }
          def answered! = @lock.synchronize { @answered += 1 }
          def unanswerable! = @lock.synchronize { @unanswerable += 1 }

          # Takes back an increment whose publish then failed.
          def unanswered! = @lock.synchronize { @answered -= 1 }
        end
      end

      # Where to send the answer: the header first, the AMQP property second.
      #
      # Both, because the five libraries do not all write both yet and a
      # responder that read only one of them could not answer half the fleet. A
      # requester here writes both and they agree, so which one is read makes no
      # difference; the order matters only for a request from somewhere else.
      #
      # @param message [Message]
      # @return [String] empty when the request asked for no answer
      def self.reply_address(message)
        header = message.envelope.headers[REPLY_TO_HEADER].to_s
        header.empty? ? message.reply_to.to_s : header
      end

      # @api private
      def self.answer(connection, reply_to, message, handler, counters)
        response = handler.call(message)
        begin
          reply(connection, reply_to, message, response, counters: counters)
        rescue StandardError => e
          # The work is done but the answer did not get out. A retry repeats the
          # work, which is why a responder that changes anything should be
          # idempotent.
          return Ack.retry(e)
        end
        Ack.accept
      rescue StandardError => e
        # The failure goes back to the caller, and then the request is settled
        # rather than retried: replying and then retrying would answer twice.
        reply(connection, reply_to, message, nil,
              error: describe_failure(e), counters: counters)
        Ack.reject(e)
      end

      # @api private
      def self.reply(connection, reply_to, request, response, error: nil, counters: nil)
        headers = error.nil? ? {} : { ERROR_HEADER => error }
        # Counted before the reply goes out, and that order is the contract —
        # see {Responder#answered}. Both replies go through here, the answer and
        # the failure, so both are counted in the one place rather than in two
        # that would drift.
        counters&.answered!
        begin
          connection.publish(response, to: reply_to,
                                       correlation_id: request.envelope.correlation_id,
                                       causation_id: request.envelope.id, headers: headers)
        rescue StandardError
          # A send that never happened must not be counted as an answer, which
          # is the one failure incrementing early would otherwise introduce.
          counters&.unanswered!
          raise
        end
      end

      # @api private
      def self.describe_failure(error)
        message = error.respond_to?(:message) ? error.message : error.to_s
        "#{error.class}: #{message}"
      end

      private_class_method :answer, :reply, :describe_failure
    end
  end
end
