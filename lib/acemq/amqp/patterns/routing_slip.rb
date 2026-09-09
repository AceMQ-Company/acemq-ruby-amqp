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
require "securerandom"

require_relative "../ack"
require_relative "../headers"
require_relative "../telemetry"
require_relative "../topology"

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
        # The two shapes an itinerary travels in.
        #
        # {SLIP} is the JSON in +acemq-routing-slip+, which names its own
        # destinations and needs nothing else to be read. {ROUTE} is Java's:
        # +x-acemq-route+ carrying the ordered step names of a {Pipeline} and
        # nothing more, resolved by the consumer against a pipeline it has
        # declared.
        #
        # Ruby writes {SLIP} unless told otherwise — three of the five
        # libraries write it and it is the self-describing one — and reads
        # either. What {ROUTE} buys is that a Ruby step can stand in a pipeline
        # a Java service declared, and hand the message on in a shape the next
        # Java step can read.
        SLIP = :slip
        ROUTE = :route

        attr_reader :steps, :done, :form, :pipeline, :run_id

        # @param steps [Array<Step>] the stops still to come
        # @param done [Array<Step>] the stops already made
        # @param form [Symbol] {SLIP} or {ROUTE}; how this one goes on the wire
        # @param pipeline [Pipeline, nil] which pipeline a {ROUTE} slip belongs
        #   to, since the header does not carry its own name
        # @param run_id [String, nil] identifies one run, across every hop
        def initialize(steps: [], done: [], form: SLIP, pipeline: nil, run_id: nil)
          @steps = steps
          @done = done
          @form = form
          @pipeline = pipeline
          # Minted the moment a slip becomes a route, and carried by every copy
          # after that, so the identifier is the same at every hop. Minting it
          # on the way out instead would give each hop a different one, which is
          # the opposite of what the header is for: it survives a dead-letter
          # and a replay so that one run can be followed across both.
          @run_id = form == ROUTE ? (run_id || SecureRandom.uuid) : run_id
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
          copy(steps: @steps.drop(1), done: @done + [completed])
        end

        # Which step is next, counting from zero — Java's +route-position+.
        def position = @done.size

        # The whole route, done and undone, in order.
        def all = @done + @steps

        # A copy that will go on the wire in the other form.
        #
        # A slip changes shape only when somebody asks it to. Left alone it
        # keeps the shape it arrived in, which is what lets a Ruby step sit in
        # the middle of a Java-declared pipeline without the run turning into
        # something the next Java step cannot read.
        #
        # Becoming a {ROUTE} re-resolves every step against the pipeline rather
        # than keeping the destinations the slip was carrying. It has to: the
        # header will carry only the names, so the exchange the next hop is
        # published to has to be the one the pipeline declared, or the message
        # goes somewhere nothing is listening and the run stops silently.
        #
        # @param form [Symbol] {SLIP} or {ROUTE}
        # @param pipeline [Pipeline, nil] required to become a {ROUTE}, because
        #   the header carries step names and no exchange
        # @raise [ArgumentError] with no pipeline, or when the pipeline has no
        #   step by one of the slip's names
        def as(form, pipeline: @pipeline)
          return copy(form: form) unless form == ROUTE

          if pipeline.nil?
            raise ArgumentError,
                  "a routing slip written as x-acemq-route needs the pipeline its steps " \
                  "belong to; the header carries their names and nothing else"
          end
          copy(form: form, pipeline: pipeline,
               steps: @steps.map { |step| pipeline.step_named(step.name) },
               done: @done.map { |step| pipeline.step_named(step.name) })
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

        # The application header a {SLIP} travels in, or nothing for a {ROUTE}.
        #
        # @return [Hash]
        def to_headers
          @form == ROUTE ? {} : { SLIP_HEADER => to_header }
        end

        # The reserved headers a {ROUTE} travels in, or nothing for a {SLIP}.
        #
        # Kept apart from {#to_headers} because the two halves go to different
        # places: an application header is passed to +publish+ as +headers:+,
        # and these are +x-acemq-+ names, which the envelope owns and an
        # application is refused.
        #
        # @return [Hash]
        def to_route
          return {} unless @form == ROUTE

          { Headers::ROUTE => all.map(&:name).join(","),
            Headers::ROUTE_POSITION => position,
            Headers::ROUTE_ID => @run_id }
        end

        def to_s
          shape = @form == ROUTE ? "#{@pipeline&.name} " : ""
          "RoutingSlip[#{shape}done: #{@done.join(" -> ")} | next: #{@steps.join(" -> ")}]"
        end

        # Reads the itinerary off a message, in whichever form it is written.
        #
        # The JSON slip is looked for first. On a message carrying both — which
        # nothing in this family writes, but a gateway between two of them
        # might — it wins, because it names its own destinations and is
        # therefore readable whether or not this consumer happens to have
        # declared the right pipeline.
        #
        # @param envelope [Envelope]
        # @param pipeline [Pipeline, nil] the pipeline an +x-acemq-route+ names
        #   the steps of. Without one, the steps and the position are still
        #   read, and there is nowhere to send the message onwards to.
        # @return [RoutingSlip, nil] nil when the message carries neither
        # @raise [FatalError] when there is a slip and it cannot be read. Fatal
        #   rather than retryable: a slip that will not parse will not parse
        #   next time either, and a message going round the broker while
        #   nothing can tell where it is meant to go is the worst of both.
        def self.from(envelope, pipeline: nil)
          raw = envelope.headers[SLIP_HEADER]
          return from_route(envelope, pipeline) if raw.nil?

          parsed = JSON.parse(raw.to_s)
          new(steps: Array(parsed["steps"]).map { |s| Step.from_wire(s) },
              done: Array(parsed["done"]).map { |s| Step.from_wire(s) })
        rescue JSON::ParserError, TypeError, NoMethodError => e
          raise FatalError,
                "cannot read the routing slip on message #{envelope.id}: #{e.message}"
        end

        # Java's form: comma-separated step names, a position, and a run.
        #
        # A position that will not parse reads as zero rather than as an error,
        # which is what Java does and for the same reason: sending the message
        # to an arbitrary step is worse than starting the route again.
        #
        # @api private
        def self.from_route(envelope, pipeline)
          raw = envelope.route[Headers::ROUTE]
          return nil if raw.nil?

          names = raw.to_s.split(",").map(&:strip).reject(&:empty?)
          return nil if names.empty?

          at = Integer(envelope.route[Headers::ROUTE_POSITION].to_s, exception: false) || 0
          at = at.clamp(0, names.size)
          steps = names.map { |name| step_in(pipeline, name) }
          new(steps: steps.drop(at), done: steps.take(at), form: ROUTE, pipeline: pipeline,
              run_id: presence(envelope.route[Headers::ROUTE_ID]))
        end

        # A step name resolved against the pipeline that declared it.
        #
        # Java publishes to the pipeline's own direct exchange with the step
        # name as the routing key, and this has to agree exactly or a Ruby hop
        # sends the message somewhere no Java consumer is listening. Without a
        # pipeline the name is all there is, and the step has no destination —
        # readable, and not followable.
        #
        # @api private
        def self.step_in(pipeline, name)
          Step.new(exchange: pipeline.nil? ? "" : pipeline.exchange,
                   routing_key: pipeline.nil? ? "" : name, name: name)
        end

        # @api private
        def self.presence(value)
          value.nil? || value.to_s.empty? ? nil : value.to_s
        end

        private_class_method :from_route, :step_in, :presence

        private

        def copy(**changes)
          RoutingSlip.new(
            steps: changes.fetch(:steps, @steps), done: changes.fetch(:done, @done),
            form: changes.fetch(:form, @form), pipeline: changes.fetch(:pipeline, @pipeline),
            run_id: changes.fetch(:run_id, @run_id)
          )
        end
      end

      # A route declared once, that messages travel by name.
      #
      #   orders = Patterns::Pipeline.new("orders", %w[validate charge ship])
      #   mq.apply(orders.topology)
      #
      #   mq.consume(orders.queue_for("charge"), &orders.follow(mq) do |message|
      #     charge(message.payload)
      #   end)
      #
      #   orders.start(mq, { "order_id" => "A-1" })
      #
      # The other half of {RoutingSlip}, and the half Java has. A slip carries
      # its destinations; a pipeline knows them, and the message carries only
      # the step names and how far along it is. That is three short headers
      # instead of a JSON document, and a route that reads in a management
      # console without decoding anything — at the price of a route that is
      # the same for every message and has to be declared on both ends.
      #
      # **The naming is Java's, exactly.** The exchange is the pipeline's name
      # and is direct, the routing key is the step name, and the queue behind a
      # step is +pipeline.step+. A Ruby consumer that got any of those wrong
      # would be listening where no Java service publishes, so they are not
      # arranged for Ruby's convenience.
      class Pipeline
        attr_reader :name, :steps

        # @param name [String] the pipeline, which is also its exchange
        # @param steps [Array<String>] the step names, in order
        def initialize(name, steps)
          @name = name.to_s
          @steps = Array(steps).map(&:to_s)
          raise ArgumentError, "a pipeline needs a name" if @name.empty?
          raise ArgumentError, "pipeline #{@name} needs at least one step" if @steps.empty?

          freeze
        end

        # The exchange every step of this pipeline is published to.
        def exchange = @name

        # The queue behind a step.
        #
        # @raise [ArgumentError] when the pipeline has no such step, which is
        #   worth catching here: a consumer subscribed to a queue nothing
        #   publishes to simply never receives anything.
        def queue_for(step)
          unless @steps.include?(step.to_s)
            raise ArgumentError,
                  "pipeline #{@name} has no step called '#{step}'. Its steps are " \
                  "#{@steps.join(", ")}."
          end
          "#{@name}.#{step}"
        end

        # The exchange, the queues and the bindings this pipeline needs.
        #
        # A {Topology} to apply rather than declarations made here, so it
        # composes with whatever else a service declares and is applied once at
        # start-up like everything else.
        #
        # @return [Topology]
        def topology
          @steps.reduce(Topology.new.exchange(@name, "direct")) do |built, step|
            built.queue(queue_for(step)).binding(queue_for(step), @name, step)
          end
        end

        # A fresh slip at the start of this route, in the declared form.
        def slip
          RoutingSlip.new(steps: @steps.map { |step| step_for(step) }, form: RoutingSlip::ROUTE,
                          pipeline: self)
        end

        # Sends a payload into the first step.
        #
        # @return [Envelope] what went on the wire
        def start(connection, payload, **fields) = slip.start(connection, payload, **fields)

        # {Patterns.follow_slip} bound to this pipeline.
        def follow(connection, &) = Patterns.follow_slip(connection, pipeline: self, &)

        # One of this pipeline's steps, as a {Step} that knows where to publish.
        #
        # @raise [ArgumentError] when the pipeline has no step by that name
        def step_named(step)
          queue_for(step)
          step_for(step.to_s)
        end

        def to_s = "Pipeline[#{@name}: #{@steps.join(" -> ")}]"

        private

        def step_for(step) = Step.new(exchange: @name, routing_key: step, name: step)
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
      # Returning nil from the block ends the run there: nothing is published
      # and the message is accepted. A step that decides a message goes no
      # further is making a decision, not failing, and it is counted apart from
      # both — the same rule {Patterns.then_publish} follows.
      #
      # @param connection [Connection]
      # @param pipeline [Pipeline, nil] the pipeline this consumer is a step of,
      #   needed only for a message travelling in the +x-acemq-route+ form
      # @param write [Symbol, nil] {RoutingSlip::SLIP} or {RoutingSlip::ROUTE},
      #   to send the message onwards in a shape other than the one it arrived
      #   in. Left alone, a slip keeps its form, which is what lets a Ruby step
      #   sit in the middle of a Java-declared pipeline.
      # @return [Proc] a handler to pass to {Connection#consume}
      def self.follow_slip(connection, pipeline: nil, write: nil, &step)
        raise ArgumentError, "Patterns.follow_slip needs a block to do the work" unless step

        lambda do |message|
          slip = RoutingSlip.from(message.envelope, pipeline: pipeline)
          if slip.nil?
            next Ack.reject(FatalError.new(
                              "message #{message.id} has no routing slip, so there is " \
                              "nowhere to send it next"
                            ))
          end

          onwards = write.nil? ? slip : slip.as(write, pipeline: slip.pipeline || pipeline)
          carry_on(connection, onwards, message, step)
        rescue FatalError => e
          Ack.reject(e)
        end
      end

      # @api private
      def self.carry_on(connection, slip, message, step)
        at = slip.next_step
        payload = step.call(message)
        # Stopped before the end of the route: a decision, not a failure, and
        # counted apart from a run that finished so that "how many were filtered
        # out" needs no log reading.
        if payload.nil?
          return finish(connection, slip, message, Telemetry::Outcome::ENDED_EARLY)
        end

        advanced = slip.advance
        # The end of the itinerary. Nothing to publish, and the work is done.
        if advanced.finished?
          return finish(connection, slip, message, Telemetry::Outcome::COMPLETED)
        end

        begin
          send_to(connection, advanced.next_step, advanced, payload,
                  { correlation_id: message.envelope.correlation_id,
                    causation_id: message.envelope.id })
        rescue StandardError => e
          return Ack.retry("#{at} is done for message #{message.id} but the next step " \
                           "did not go out: #{e.message}")
        end
        Ack.accept
      end

      # Records that a run left the pipeline, and accepts the message.
      #
      # Only for a declared {Pipeline}, because +acemq.pipeline.run.total+ is
      # tagged with a pipeline name and a bare JSON slip has none: it is an
      # itinerary somebody assembled per message, not a thing with an identity
      # to put on a dashboard. The duration is the age of the envelope rather
      # than the time in this step, so it is the whole run: the envelope was
      # created when the message entered and carried through every hop.
      #
      # @api private
      def self.finish(connection, slip, message, outcome)
        pipeline = slip.pipeline
        return Ack.accept if pipeline.nil?

        telemetry = connection.telemetry
        tags = { pipeline: pipeline.name, step: slip.next_step&.name.to_s, outcome: outcome }
        telemetry.count(Telemetry::PIPELINE_RUN_TOTAL, 1, **tags)
        telemetry.observe(Telemetry::PIPELINE_RUN_DURATION, message.envelope.age,
                          pipeline: pipeline.name)
        Ack.accept
      end

      # @api private
      def self.send_to(connection, step, slip, payload, fields = {})
        headers = fields.fetch(:headers, {}).merge(slip.to_headers)
        connection.publish(payload, to: step.routing_key, exchange: step.exchange,
                                    **fields.merge(headers: headers, route: slip.to_route))
      end

      private_class_method :carry_on, :finish
    end
  end
end
