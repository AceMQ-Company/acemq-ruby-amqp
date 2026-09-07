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

require "json"

require_relative "../ack"

module AceMQ
  module AMQP
    # An itinerary the message carries, instead of an orchestrator that knows
    # the route.
    #
    # Each service does its part and sends the message to the next stop on the
    # slip, so the route is decided once — by whoever started the work — and
    # travels with the message rather than living in a component every service
    # has to talk to.
    module Patterns
      # The itinerary, as JSON, on the message.
      #
      # An application header, so it survives every hop and is readable by
      # anything that can read JSON. The shape is shared with the other AceMQ
      # libraries, which is why the keys inside it are +routingKey+ and
      # +completedAt+ rather than anything more Rubyish: a slip written by a Go
      # service is read by a Ruby one.
      SLIP_HEADER = "acemq-routing-slip"

      # One stop on a routing slip.
      Step = Struct.new(:exchange, :routing_key, :name, :completed_at, keyword_init: true) do
        def to_s = name.to_s.empty? ? "#{exchange}/#{routing_key}" : name

        # @api private
        def to_wire
          wire = { "exchange" => exchange.to_s, "routingKey" => routing_key.to_s }
          wire["name"] = name unless name.to_s.empty?
          wire["completedAt"] = completed_at unless completed_at.to_s.empty?
          wire
        end

        # @api private
        def self.from_wire(raw)
          raw = raw.to_h { |key, value| [key.to_s, value] }
          new(exchange: raw["exchange"].to_s, routing_key: raw["routingKey"].to_s,
              name: raw["name"], completed_at: raw["completedAt"])
        end
      end

      # Where a message is going, and where it has been.
      #
      #   slip = Patterns::RoutingSlip.new
      #                               .step("orders-events", "order.validate", name: "validate")
      #                               .step("orders-events", "order.charge", name: "charge")
      #                               .step("orders-events", "order.ship", name: "ship")
      #
      #   slip.start(mq, order)
      #
      # What it costs: no single place says what the whole route is at runtime,
      # so a route that is wrong is discovered one hop at a time. Worth it when
      # the steps vary per message, and not worth it when every message goes the
      # same way — a fixed chain of consumers is simpler and easier to follow.
      class RoutingSlip
        attr_reader :steps, :done

        def initialize(steps: [], done: [])
          @steps = steps
          @done = done
        end

        # Adds a stop, and returns self.
        #
        # Building mutates, in the one place this library allows it, for the
        # same reason {Topology} does: a slip is assembled and then sent, and
        # threading a new copy through four chained calls would buy an
        # immutability nobody is using. {#advance} is the other half and does
        # return a copy, because by then the slip is on a message and a message
        # that changed under a handler is a message nothing can reason about.
        def step(exchange, routing_key, name: nil)
          @steps << Step.new(exchange: exchange.to_s, routing_key: routing_key.to_s, name: name)
          self
        end

        # The stop this message is going to, or nil at the end of the route.
        def next_step = @steps.first

        # Whether every step has been done.
        def finished? = @steps.empty?

        # A copy with the first step moved to +done+, stamped with the time.
        #
        # +done+ is carried rather than dropped so a slip that fails half way
        # says how far it got. Whoever finds the message in a dead-letter queue
        # is asking exactly that question.
        def advance
          return self if finished?

          completed = @steps.first.dup
          completed.completed_at = Time.now.utc.strftime("%Y-%m-%dT%H:%M:%SZ")
          RoutingSlip.new(steps: @steps.drop(1), done: @done + [completed])
        end

        # Sends a payload to the first stop.
        #
        # @param connection [Connection]
        # @param payload [Object] anything the codec will encode
        # @param fields [Hash] envelope fields
        # @return [Envelope] what went on the wire
        def start(connection, payload, **fields)
          raise ArgumentError, "this routing slip has no steps in it" if finished?

          Patterns.send_to(connection, next_step, self, payload, fields)
        end

        # The slip as it goes on the wire.
        def to_header = JSON.generate(to_wire)

        # @api private
        def to_wire
          wire = { "steps" => @steps.map(&:to_wire) }
          wire["done"] = @done.map(&:to_wire) unless @done.empty?
          wire
        end

        def to_s
          "RoutingSlip[done: #{@done.join(" -> ")} | next: #{@steps.join(" -> ")}]"
        end

        # Reads the itinerary off a message, or nil when it has none.
        #
        # @param envelope [Envelope]
        # @return [RoutingSlip, nil]
        # @raise [FatalError] when there is a slip and it cannot be read. Fatal
        #   rather than retryable: a slip that will not parse will not parse
        #   next time either, and a message going round the broker while
        #   nothing can tell where it is meant to go is the worst of both.
        def self.from(envelope)
          raw = envelope.headers[SLIP_HEADER]
          return nil if raw.nil?

          parsed = JSON.parse(raw.to_s)
          new(steps: Array(parsed["steps"]).map { |s| Step.from_wire(s) },
              done: Array(parsed["done"]).map { |s| Step.from_wire(s) })
        rescue JSON::ParserError, TypeError, NoMethodError => e
          raise FatalError,
                "cannot read the routing slip on message #{envelope.id}: #{e.message}"
        end
      end

      # Wraps a handler so the message carries on to its next stop.
      #
      #   mq.consume("charge-queue", &Patterns.follow_slip(mq) do |message|
      #     charge(message.payload)      # the payload to send onwards
      #   end)
      #
      # The block returns the payload for the next stop, which may be the one it
      # received or a changed copy. When the slip has no steps left the work is
      # finished and nothing more is published.
      #
      # The message is accepted only once the next one is out, so a failure to
      # publish retries this step — which is why a step that changes anything
      # should be idempotent.
      #
      # @param connection [Connection]
      # @return [Proc] a handler to pass to {Connection#consume}
      def self.follow_slip(connection, &step)
        raise ArgumentError, "Patterns.follow_slip needs a block to do the work" unless step

        lambda do |message|
          slip = RoutingSlip.from(message.envelope)
          if slip.nil?
            next Ack.reject(FatalError.new(
                              "message #{message.id} has no routing slip, so there is " \
                              "nowhere to send it next"
                            ))
          end

          carry_on(connection, slip, message, step)
        rescue FatalError => e
          Ack.reject(e)
        end
      end

      # @api private
      def self.carry_on(connection, slip, message, step)
        payload = step.call(message)
        advanced = slip.advance
        # The end of the itinerary. Nothing to publish, and the work is done.
        return Ack.accept if advanced.finished?

        begin
          send_to(connection, advanced.next_step, advanced, payload,
                  { correlation_id: message.envelope.correlation_id,
                    causation_id: message.envelope.id })
        rescue StandardError => e
          return Ack.retry(
            "#{slip.next_step} is done for message #{message.id} but the next step " \
            "did not go out: #{e.message}"
          )
        end
        Ack.accept
      end

      # @api private
      def self.send_to(connection, step, slip, payload, fields = {})
        headers = fields.fetch(:headers, {}).merge(SLIP_HEADER => slip.to_header)
        connection.publish(payload, to: step.routing_key, exchange: step.exchange,
                                    **fields.merge(headers: headers))
      end

      private_class_method :carry_on
    end
  end
end
