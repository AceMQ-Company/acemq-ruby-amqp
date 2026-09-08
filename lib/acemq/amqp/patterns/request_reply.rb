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
      # An application header rather than AMQP's own +reply-to+ property, so it
      # travels through the same envelope machinery as everything else and
      # survives a hop through a service that rebuilds the message.
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

          begin
            publish(request, correlation, fields)
            answer(waiter.await(@timeout), correlation)
          ensure
            @lock.synchronize { @waiting.delete(correlation) }
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

        def publish(request, correlation, fields)
          envelope = fields.merge(
            correlation_id: correlation,
            headers: fields.fetch(:headers, {}).merge(REPLY_TO_HEADER => @reply_queue)
          )
          @connection.publish(request, to: @to, exchange: @exchange, **envelope)
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
      # @return [Consumer]
      def self.serve(connection, queue, **options, &handler)
        raise ArgumentError, "Patterns.serve needs a block to answer with" unless handler

        connection.consume(queue, **options) do |message|
          reply_to = message.envelope.headers[REPLY_TO_HEADER].to_s
          if reply_to.empty?
            # Retrying cannot make a reply queue appear, so this is
            # dead-lettered rather than looped.
            next Ack.reject(FatalError.new(
                              "request #{message.id} carries no #{REPLY_TO_HEADER} header, " \
                              "so there is nowhere to reply"
                            ))
          end

          answer(connection, reply_to, message, handler)
        end
      end

      # @api private
      def self.answer(connection, reply_to, message, handler)
        response = handler.call(message)
        begin
          reply(connection, reply_to, message, response)
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
        reply(connection, reply_to, message, nil, error: describe_failure(e))
        Ack.reject(e)
      end

      # @api private
      def self.reply(connection, reply_to, request, response, error: nil)
        headers = error.nil? ? {} : { ERROR_HEADER => error }
        connection.publish(response, to: reply_to,
                                     correlation_id: request.envelope.correlation_id,
                                     causation_id: request.envelope.id, headers: headers)
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
