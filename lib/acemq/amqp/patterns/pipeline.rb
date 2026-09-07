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

require_relative "../ack"

module AceMQ
  module AMQP
    # Handlers wrapped in handlers, and services chained into a pipeline.
    #
    # Everything here is a handler that takes a handler, so it composes with
    # {Patterns.idempotent} and {Patterns.ordered} and with anything an
    # application writes itself. Nothing takes over the consumer.
    module Patterns
      # Wraps a handler in middleware.
      #
      #   mq.consume("orders.new", &Patterns.chain(
      #     ->(message) { place(message.payload) },
      #     Patterns.with_logging { |line| logger.info(line) },
      #     Patterns.with_timeout(10),
      #     Patterns.with_idempotency(store)
      #   ))
      #
      # The order reads outside-in: the first one named is the outermost, so
      # logging above records what the timeout and the idempotency guard
      # decided. They are applied in reverse to make that so, which is the
      # order somebody reading the list expects rather than the order the code
      # would fall into.
      #
      # @param handler [#call] the innermost handler
      # @param middleware [Array<#call>] each given a handler, each returning one
      # @return [Proc] a handler to pass to {Connection#consume}
      def self.chain(handler, *middleware)
        middleware.reverse.reduce(handler) { |inner, wrap| wrap.call(inner) }
      end

      # Reports a handler that ran longer than it was allowed.
      #
      # An overrun is reported as a **retry**, whatever the handler said about
      # itself. That is a choice between two imperfect answers: retrying work
      # that may have succeeded risks doing it twice, and accepting work that
      # may have failed loses it. Duplicates are a problem somebody can solve —
      # see {Patterns.idempotent} — and a lost message is not.
      #
      # It reports, and does not interrupt. Ruby's +Timeout.timeout+ would
      # interrupt, by raising inside whatever line the handler happened to be
      # on, which can leave a transaction half-written or a lock held by nobody;
      # and it would not help anyway, because the message is held until the
      # handler returns either way. What this buys is that a handler which
      # quietly takes three minutes stops being invisible.
      #
      # @param seconds [Numeric]
      def self.with_timeout(seconds)
        lambda do |inner|
          lambda do |message|
            started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            ack = inner.call(message)
            took = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
            next ack if took <= seconds

            Ack.retry("handling message #{message.id} took #{took.round(3)} seconds, " \
                      "longer than the #{seconds} it is allowed")
          end
        end
      end

      # Records what happened to each message.
      #
      #   Patterns.with_logging { |line| logger.info(line) }
      #
      # A block that is given a line, so it fits a Logger, a structured logger,
      # +puts+ or a test — without this library choosing a logging gem on
      # anybody's behalf.
      #
      # An exception is logged and then re-raised. Swallowing it would turn a
      # failure into an acknowledgement, and not logging it would leave the
      # middleware blind to the case somebody added it for.
      def self.with_logging(&write)
        raise ArgumentError, "Patterns.with_logging needs a block to write with" unless write

        lambda do |inner|
          lambda do |message|
            started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            begin
              ack = inner.call(message)
            rescue StandardError => e
              write.call(log_line(message, started, "raised #{e.class}: #{e.message}"))
              raise
            end
            write.call(log_line(message, started, describe_ack(ack)))
            ack
          end
        end
      end

      # {Patterns.idempotent} as middleware, so it can sit in a {chain}.
      def self.with_idempotency(store, key: nil)
        ->(inner) { idempotent(store, key: key, &inner) }
      end

      # {Patterns.ordered} as middleware.
      def self.with_ordering(key)
        ->(inner) { ordered(key, &inner) }
      end

      # Publishes the result of handling a message onwards.
      #
      #   mq.consume("orders.new", &Patterns.then_publish(mq, to: "shipment.requested",
      #                                                   exchange: "shipping-events") do |m|
      #     m.payload["digital"] ? nil : { "order_id" => m.payload["order_id"] }
      #   end)
      #
      # The step that makes a pipeline out of a chain of services: this one
      # consumes, does its work, and publishes what comes out, carrying the
      # correlation forward and recording what caused what.
      #
      # Returning nil publishes nothing and accepts the message, which is how a
      # step says "this one does not continue" without inventing an empty
      # message for the next service to work out how to ignore.
      #
      # The message is accepted only once the next one is out. A publish that
      # fails retries the input, so the work runs again — which is why a step
      # that changes anything should be idempotent.
      #
      # @param connection [Connection]
      # @param to [String] the routing key for what comes out
      # @param exchange [String] empty for the default exchange
      # @return [Proc] a handler to pass to {Connection#consume}
      def self.then_publish(connection, to:, exchange: "", &step)
        raise ArgumentError, "Patterns.then_publish needs a block to do the work" unless step

        lambda do |message|
          outgoing = step.call(message)
          next Ack.accept if outgoing.nil?

          begin
            connection.publish(outgoing, to: to, exchange: exchange,
                                         correlation_id: message.envelope.correlation_id,
                                         causation_id: message.envelope.id)
          rescue StandardError => e
            next Ack.retry("the work for message #{message.id} is done but the next " \
                           "message did not go out: #{e.message}")
          end
          Ack.accept
        end
      end

      # @api private
      def self.log_line(message, started, outcome)
        took = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        "acemq #{message.id} type=#{message.envelope.type} " \
          "attempt=#{message.attempt} took=#{(took * 1000).round}ms #{outcome}"
      end

      # @api private
      def self.describe_ack(ack)
        return "returned #{ack.class} rather than an Ack" unless ack.is_a?(Ack)
        return ack.to_s if ack.error.nil?

        "#{ack}: #{ack.error}"
      end

      private_class_method :log_line, :describe_ack
    end
  end
end
