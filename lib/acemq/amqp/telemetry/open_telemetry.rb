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
require_relative "../interceptors"
require_relative "../version"

module AceMQ
  module AMQP
    module Telemetry
      # Emits OpenTelemetry spans for publishes and deliveries.
      #
      #   tracing = AceMQ::AMQP::Telemetry::OpenTelemetry.new
      #   tracing.install(mq)
      #
      # The point of tracing a message system is the join: the span covering a
      # handler must be a child of the span that published the message, even
      # though the two ran in different processes minutes apart. That is what
      # the +traceparent+ header carries, and why it is written on the way out
      # and read on the way in.
      #
      # == It is an interceptor, not a telemetry observer
      #
      # {Telemetry} here counts things — three methods, no dependency, and a
      # {Registry} for when the numbers are all that is wanted. Tracing is a
      # different shape: a span wraps a publish or a handler, so it has to know
      # when one starts and when it ends, and it has to be able to change the
      # message on the way out. {Interceptors} is exactly that seam, and it is
      # public, so everything below could have been written outside this gem.
      #
      # Register on both sides, which {install} does:
      #
      #   mq.intercept_publish(tracing)
      #   mq.intercept_consume(tracing)
      #
      # == The names on the wire
      #
      # +traceparent+ and +tracestate+, which are the W3C names and are
      # deliberately *not* +x-acemq-+ prefixed: other tooling already knows
      # them, and renaming them would make this library's traces invisible to
      # everything that did not know to look. The Java library writes the same
      # two, so a Ruby consumer joins a Java producer's trace without either
      # side being configured for the other.
      #
      # == Span names, kinds and attributes
      #
      # Shared with the Java adapter, which follows the OpenTelemetry messaging
      # conventions:
      #
      #   <destination> publish   PRODUCER
      #   <queue> process         CONSUMER
      #   <destination> request   CLIENT
      #
      # CLIENT for a request rather than PRODUCER because that span waits for an
      # answer, and its duration means something different as a result — a
      # reader who cannot tell the two apart cannot tell a slow broker from a
      # slow responder.
      #
      # == Events rather than spans
      #
      # A retry, a dead letter, an outbox failure and a finished pipeline run
      # are events on the span that is already open. A zero-length span at the
      # end of a trace adds a row and no information.
      #
      # == The gem
      #
      # +opentelemetry-api+ is required when one of these is built, not when
      # this file is read, and the gem declares no runtime dependencies at all.
      # A process that publishes messages and traces nothing installs nothing.
      class OpenTelemetry
        # What the tracer is registered under, in every AceMQ library.
        INSTRUMENTATION_NAME = "org.acemq.amqp"

        # The W3C names, which are what makes a trace legible to anything else.
        TRACEPARENT = "traceparent"
        TRACESTATE = "tracestate"

        # The attributes, shared with the Java adapter.
        SYSTEM = "messaging.system"
        DESTINATION = "messaging.destination.name"
        OPERATION = "messaging.operation"
        MESSAGE_ID = "messaging.message.id"
        CONVERSATION_ID = "messaging.message.conversation_id"
        ROUTING_KEY = "messaging.rabbitmq.destination.routing_key"
        MESSAGE_TYPE = "messaging.acemq.message_type"
        ATTEMPT = "messaging.acemq.attempt"
        OUTCOME = "messaging.acemq.outcome"
        REASON = "messaging.acemq.reason"
        OUTBOX_LAG = "messaging.acemq.outbox_lag_ms"

        # What a failure is called when nothing more specific was said. The same
        # word the counters use, which is the whole point of having one.
        FAILED = "failed"

        # The outcomes that make a span an error, and the ones that do not.
        #
        # +retried+ and +rejected+ are deliberately absent. A message that will
        # be tried again has not failed yet, and a message the handler refused
        # on purpose is the system working; marking either as an error is how a
        # trace view fills with red and stops meaning anything.
        #
        # +parked+ is deliberately absent, for the same reason +rejected+ is: a
        # handler that parks a message meant to, and a decision somebody made is
        # not a failure. Go and Python leave it out too, so a parked span reads
        # the same in all three. The parking queue still has to be looked at --
        # that is what +acemq.consume.total+ tagged +outcome=parked+ is for.
        FAILING_OUTCOMES = %w[unroutable failed dead_lettered].freeze

        # Runs before every other interceptor on the way in and after every
        # other one on the way out, so a span covers whatever they do.
        ORDER = -1000

        # @param tracer_provider [#tracer, nil] defaults to the process's
        # @param propagator [#inject, nil] defaults to the process's, which is
        #   +tracecontext+ once the SDK has been configured
        # @param transport [String] the +messaging.system+ attribute
        # @raise [DependencyMissing] when opentelemetry-api is not installed
        def initialize(tracer_provider: nil, propagator: nil, transport: "rabbitmq")
          self.class.load_api!
          @tracer = (tracer_provider || ::OpenTelemetry.tracer_provider)
                    .tracer(INSTRUMENTATION_NAME, AceMQ::AMQP::VERSION)
          @propagator = propagator || ::OpenTelemetry.propagation
          @transport = transport
          # Per instance and per fiber, so two adapters on one connection do not
          # pop each other's spans and a handler running on another thread has
          # its own.
          @publishing = :"acemq_otel_publish_#{object_id}"
          @consuming = :"acemq_otel_consume_#{object_id}"
        end

        # The tracer this emits through, for a caller that wants to open a span
        # of its own inside one of these.
        attr_reader :tracer

        # Registers on both sides of a connection.
        #
        # @param connection [Connection]
        # @return [OpenTelemetry] self
        def install(connection)
          connection.intercept_publish(self)
          connection.intercept_consume(self)
          self
        end

        def order = ORDER

        # ---------- the spans ----------

        # Opens the span covering a publish, and makes it current.
        #
        # @param exchange [String] empty for the default exchange
        # @param routing_key [String]
        # @param envelope [Envelope]
        # @return [Scope]
        def publish_started(exchange:, routing_key:, envelope:)
          # The exchange is the destination when there is one. Publishing
          # through the default exchange has no exchange to name, and the
          # routing key is the queue, which is what a reader wants to see.
          destination = exchange.to_s.empty? ? routing_key.to_s : exchange.to_s
          start("#{destination} publish", :producer,
                { SYSTEM => @transport, DESTINATION => destination, OPERATION => "publish",
                  ROUTING_KEY => routing_key.to_s, **envelope_attributes(envelope) })
        end

        # Opens the span covering a handler, joined to the publish that caused
        # it.
        #
        # The parent comes out of the message's own headers rather than out of
        # whatever this thread happened to be doing, which is the whole point:
        # the publish happened in another process, possibly minutes ago, and
        # nothing in this one remembers it. Ambient context is the fallback and
        # not the source, the same way round as the Java adapter.
        #
        # @param queue [String]
        # @param envelope [Envelope]
        # @return [Scope]
        def consume_started(queue:, envelope:)
          parent = @propagator.extract(carrier_from(envelope.headers),
                                       context: ::OpenTelemetry::Context.current)
          start("#{queue} process", :consumer,
                { SYSTEM => @transport, DESTINATION => queue.to_s, OPERATION => "process",
                  ATTEMPT => envelope.attempt.to_i, **envelope_attributes(envelope) },
                parent: parent)
        end

        # Opens the span covering a request that waits for an answer.
        #
        #   tracing.request("pricing.quote") { requester.call(order) }
        #
        # CLIENT rather than PRODUCER, because a reader has to know that this
        # span's duration includes somebody else's work.
        #
        # @param destination [String]
        # @param envelope [Envelope, nil]
        # @return [Scope]
        def request_started(destination:, envelope: nil)
          start("#{destination} request", :client,
                { SYSTEM => @transport, DESTINATION => destination.to_s,
                  OPERATION => "request", **envelope_attributes(envelope) })
        end

        # {request_started} with the closing done for you.
        #
        # @param destination [String]
        # @param envelope [Envelope, nil]
        # @yieldreturn [Object] whatever the block returns
        def request(destination, envelope: nil)
          scope = request_started(destination: destination, envelope: envelope)
          answer = yield
          scope.outcome("answered")
          answer
        rescue StandardError => e
          # +timed_out+ rather than +failed+ for a request nobody answered in
          # time, and deliberately not an error: that is the absence of a reply
          # rather than a failure of this process — usually a responder's queue
          # being long — and the Java and Go adapters both write it without
          # marking the span red, because a trace view that fills with red for
          # slow responders stops meaning anything. The exception still reaches
          # the caller, who is free to decide it was one.
          if self.class.timed_out?(e)
            scope&.outcome("timed_out")
          else
            scope&.outcome("failed")
            scope&.failed(e)
          end
          raise
        ensure
          scope&.close
        end

        # The current trace context, as headers.
        #
        # For anything publishing outside this library — an outbox relay writing
        # rows, a scheduled job — that still wants its messages to join the
        # trace it is running in.
        #
        # @return [Hash{String=>String}]
        def propagation_headers
          carrier = {}
          @propagator.inject(carrier, context: ::OpenTelemetry::Context.current)
          carrier
        end

        # ---------- the events ----------

        # @param exchange [String]
        # @param reason [String]
        def outbox_publish_failed(exchange:, reason:)
          record("outbox.publish_failed",
                 { DESTINATION => exchange.to_s, REASON => reason.to_s })
        end

        # Records how far behind the outbox relay is running.
        #
        # An attribute on the current span rather than an event, and the Java,
        # Go and Python adapters write the same one: it measures the publish
        # that is happening, not something that happened during it. Nothing when
        # no span is open, for the same reason
        # {#outbox_publish_failed} records nothing then.
        #
        # @param lag [Numeric] seconds the record sat before it was published
        def outbox_published(lag:)
          span = ::OpenTelemetry::Trace.current_span
          return unless span.recording?

          span.set_attribute(OUTBOX_LAG, (lag * 1000).to_i)
          nil
        end

        # @param pipeline [String]
        # @param step [String]
        # @param outcome [String]
        # @param age [Numeric, nil] seconds since the run started
        def pipeline_run_finished(pipeline:, step:, outcome:, age: nil)
          attributes = { "pipeline" => pipeline.to_s, "step" => step.to_s,
                         "outcome" => outcome.to_s }
          attributes["messaging.acemq.run_age_ms"] = (age * 1000).to_i if age
          record("pipeline.run_finished", attributes)
        end

        # @param queue [String]
        # @param envelope [Envelope]
        # @param delay [Numeric, nil] seconds this message will wait
        def message_retried(queue:, envelope:, delay: nil)
          attributes = { DESTINATION => queue.to_s, ATTEMPT => envelope.attempt.to_i }
          attributes["messaging.acemq.retry_delay_ms"] = (delay * 1000).to_i if delay
          record("message.retried", attributes)
        end

        # @param queue [String]
        # @param envelope [Envelope]
        # @param reason [String] unbounded text, which a span tolerates and a
        #   metric does not
        def message_dead_lettered(queue:, envelope:, reason:)
          record("message.dead_lettered",
                 { DESTINATION => queue.to_s, ATTEMPT => envelope.attempt.to_i,
                   REASON => reason.to_s })
        end

        # ---------- the interceptor ----------

        # Opens the publish span and writes the trace into the message.
        #
        # The headers are set through the context rather than on the envelope
        # directly, so what goes on the wire is what every other interceptor
        # sees too — and so an interceptor registered after this one can read
        # the trace it is part of.
        def before_publish(context)
          scope = publish_started(exchange: context.exchange, routing_key: context.routing_key,
                                  envelope: context.envelope)
          push(@publishing, scope)
          propagation_headers.each { |name, value| context.set_header(name, value) }
          context
        end

        # The broker has the message.
        def after_confirm(_context)
          finish(pop(@publishing), "confirmed")
        end

        # Opens the handler span, joined to whoever published the message.
        def before_handle(context)
          push(@consuming, consume_started(queue: context.queue, envelope: context.envelope))
          context
        end

        # The handler has returned or raised, and the delivery has not been
        # settled yet — which is the last moment the outcome and the span are
        # both in hand.
        #
        # The outcome comes off the context's {Settlement} when there is one,
        # because that is what the consumer is about to do; the ack is only what
        # the handler asked for. A retry on the last attempt is a dead letter,
        # and reading it off the ack is how a dead-lettered message ends up with
        # +outcome=retried+ on its span and nothing in the trace to say it was
        # dropped. The ack is still the fallback, for anything driving this
        # interceptor without a consumer behind it.
        def after_handle(context, ack)
          scope = pop(@consuming)
          return context if scope.nil?

          settlement = settlement_of(context)
          outcome = settlement&.outcome || self.class.outcome_of(ack)
          note(context, ack, settlement, outcome)
          scope.failed(ack.error) if ack.error.is_a?(Exception)
          finish(scope, outcome)
          context
        end

        # A publish that never reached the broker, or a handler that raised.
        #
        # On the publish side this is the only hook that runs, so the span is
        # closed here. On the consume side +after_handle+ still runs afterwards
        # and closes it, so this records the exception and leaves it open.
        def on_error(context, failure)
          if context.is_a?(PublishContext)
            scope = pop(@publishing)
            scope&.failed(failure)
            finish(scope, "failed")
          else
            peek(@consuming)&.failed(failure)
          end
          context
        end

        # Whether a failure is a request that went unanswered.
        #
        # Asked with +defined?+ rather than named in a +rescue+ clause, because
        # {Patterns::RequestTimedOut} lives in the patterns file and a process
        # that traces without ever asking anybody a question need not have
        # loaded it — and a +rescue+ naming a constant that is not there raises
        # a +NameError+ in place of the exception the caller threw.
        #
        # @param failure [Exception]
        # @return [Boolean]
        def self.timed_out?(failure)
          return false unless defined?(Patterns::RequestTimedOut)

          failure.is_a?(Patterns::RequestTimedOut)
        end

        # What an ack alone is called on a span.
        #
        # The same words the Java adapter writes, plus +parked+ for the action
        # Java's vocabulary does not have. +dead_lettered+ rather than +retried+
        # for a retry marked fatal, because that is what the consumer will
        # actually do with it — honouring the mark rather than the request is
        # the entire point of having it.
        #
        # This is the fallback. What an ack cannot know is whether a retry has
        # any attempts left, so a {Settlement} on the context wins over it; see
        # {#after_handle}.
        #
        # @param ack [Ack]
        # @return [String]
        def self.outcome_of(ack)
          return "acked" if ack.accept?
          return "rejected" if ack.reject?
          return "parked" if ack.park?
          return "dead_lettered" if ack.error.is_a?(FatalError)

          "retried"
        end

        # Attributes OpenTelemetry will take: nothing nil, nothing empty.
        #
        # An empty string is an attribute that says nothing and costs a column
        # in every backend that stores one. The Java adapter writes them because
        # its API will not take a null; Ruby's will not take a nil either, and
        # leaving the key out entirely is the better answer.
        #
        # @api private
        def self.clean(attributes)
          attributes.reject { |_, value| value.nil? || value == "" }
        end

        # Reaches for the gem, and says which one when it is not there.
        #
        # The gemspec declares no runtime dependencies on purpose, so this is
        # the first moment anybody finds out. "cannot load such file --
        # opentelemetry-api" does not say which library wanted it or what to do
        # about it.
        #
        # @raise [DependencyMissing]
        # @api private
        def self.load_api!
          require "opentelemetry-api"
        rescue LoadError => e
          raise DependencyMissing,
                "the AceMQ OpenTelemetry adapter needs the opentelemetry-api gem, which is " \
                "not installed. Add `gem \"opentelemetry-api\", \"~> 1.8\"` to your Gemfile " \
                "— this gem declares no runtime dependencies, so a process that traces " \
                "nothing installs nothing. (#{e.message})"
        end

        # One span, and the context it was made current in.
        #
        # Closing detaches the context before ending the span, in that order:
        # the other way round leaves a fiber pointing at a span that has already
        # finished, and anything opened afterwards is parented to a corpse.
        class Scope
          # @return [OpenTelemetry::Trace::Span] for a caller that wants to add
          #   an attribute this library does not know about
          attr_reader :span

          def initialize(span, token)
            @span = span
            @token = token
            @closed = false
            @named = false
          end

          # Records what happened, and marks the span an error when it was one.
          #
          # @param outcome [String]
          # @return [Scope] self
          def outcome(outcome)
            @named = true
            @span.set_attribute(OUTCOME, outcome.to_s)
            if FAILING_OUTCOMES.include?(outcome.to_s)
              @span.status = ::OpenTelemetry::Trace::Status.error(outcome.to_s)
            end
            self
          end

          # Records a failure, and says so in the vocabulary every other signal
          # uses.
          #
          # The exception and the error status are not enough on their own: they
          # say something went wrong without saying what became of the
          # operation, and a span that said nothing where the counter said
          # +failed+ is the same disagreement between a metric and a trace that
          # this library treats as a defect everywhere else — a dashboard shows
          # the failures and the trace backend, asked for
          # +messaging.acemq.outcome = "failed"+, finds none of the spans behind
          # them. The Java adapter was fixed for exactly this and Python already
          # did it.
          #
          # An outcome already named wins, because a caller who named one knows
          # more than "it threw": a request that ran out of time is +timed_out+,
          # which is the absence of an answer rather than a failure here.
          #
          # @param failure [Exception, String, nil]
          # @return [Scope] self
          def failed(failure)
            @span.record_exception(failure) if failure.is_a?(Exception)
            @span.set_attribute(OUTCOME, FAILED) unless @named
            @span.status = ::OpenTelemetry::Trace::Status.error(describe(failure))
            self
          end

          # Ends the span. Calling it twice does nothing the second time.
          def close
            return if @closed

            @closed = true
            ::OpenTelemetry::Context.detach(@token) if @token
            @span.finish
            nil
          end

          def closed? = @closed

          private

          def describe(failure)
            return "failed" if failure.nil?
            return failure.to_s unless failure.is_a?(Exception)

            failure.message.to_s.empty? ? failure.class.name : failure.message
          end
        end

        private

        def start(name, kind, attributes, parent: nil)
          span = @tracer.start_span(name, attributes: self.class.clean(attributes), kind: kind,
                                          with_parent: parent)
          token = ::OpenTelemetry::Context.attach(
            ::OpenTelemetry::Trace.context_with_span(span)
          )
          Scope.new(span, token)
        end

        # Adds an event to whatever span is open, and does nothing when none is.
        #
        # Nothing is a legitimate answer: an outbox relay running on its own
        # thread with no delivery in flight has no span to hang an event on, and
        # opening one for the event alone would produce exactly the zero-length
        # span this adapter avoids.
        def record(name, attributes)
          span = ::OpenTelemetry::Trace.current_span
          return nil unless span.recording?

          span.add_event(name, attributes: self.class.clean(attributes))
          nil
        end

        def envelope_attributes(envelope)
          return {} if envelope.nil?

          { MESSAGE_ID => envelope.id.to_s,
            CONVERSATION_ID => envelope.correlation_id.to_s,
            MESSAGE_TYPE => envelope.type.to_s }
        end

        # The message's headers, as the propagator wants them: string keys,
        # string values. A header that arrived off the wire as bytes or as an
        # integer is still a header, and +traceparent+ from another client can
        # be either.
        def carrier_from(headers)
          (headers || {}).each_with_object({}) do |(name, value), carrier|
            carrier[name.to_s] = value.to_s
          end
        end

        # The settlement the consumer attached, or nil for anything else.
        #
        # Asked of the context rather than assumed, because this interceptor is
        # public and can be handed a context built by something that is not this
        # library's consumer.
        def settlement_of(context)
          context.respond_to?(:settlement) ? context.settlement : nil
        end

        # The two events the consume path knows enough to raise on its own.
        #
        # Both are written with what the settlement decided: the delay is the
        # one the retry policy really chose, and the reason is the sentence that
        # goes onto the dead letter itself — which is what somebody reading the
        # trace and somebody draining the queue need to be able to match up. A
        # rejection is dead-lettered too, so it raises the event as well; only
        # the word on the span keeps them apart.
        def note(context, ack, settlement, outcome)
          case outcome
          when "retried"
            message_retried(queue: context.queue, envelope: context.envelope,
                            delay: settlement&.delay)
          when "dead_lettered", "rejected"
            message_dead_lettered(queue: context.queue, envelope: context.envelope,
                                  reason: settlement&.reason || ack.error.to_s)
          end
        end

        def finish(scope, outcome)
          return nil if scope.nil?

          scope.outcome(outcome)
          scope.close
          nil
        end

        def push(key, scope)
          (Thread.current[key] ||= []).push(scope)
        end

        def pop(key) = Thread.current[key]&.pop

        def peek(key) = Thread.current[key]&.last
      end
    end
  end
end
