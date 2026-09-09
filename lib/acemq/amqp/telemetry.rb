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

# The OpenTelemetry adapter, in a file of its own because it is a page of
# reasoning rather than a method. Loading it costs nothing that is not already
# paid for: it reaches for the opentelemetry-api gem when one is built, not when
# this file is read, so a process counting messages and tracing none of them
# installs nothing.
require_relative "telemetry/open_telemetry"

module AceMQ
  module AMQP
    # What the library counts, and where it sends the numbers.
    #
    # The names are shared with the Java, Go, .NET and Python libraries, so a
    # dashboard or an alert written against one service reads the same against
    # the next. Java publishes them through Micrometer and .NET through
    # System.Diagnostics.Metrics; Ruby has no standard metrics interface at all,
    # so this counts them and hands them to whatever you already run.
    #
    # The vocabulary is Java's +MetricNames+, which is the family's. It says
    # what a message did in the tag rather than in the metric name — one
    # +acemq.consume.total+ tagged +outcome+ rather than five counters — so a
    # dashboard sums the whole and breaks it down without knowing the list in
    # advance, and a new outcome does not need a new panel.
    #
    # == The interface
    #
    # An observer is anything answering three methods:
    #
    #   def count(metric, delta = 1, **labels)   # a counter goes up
    #   def observe(metric, value, **labels)     # a distribution takes a sample
    #   def gauge(metric, value, **labels)       # a current value is set
    #
    # +observe+ is a distribution rather than a timer. Durations go through it
    # in seconds, and {CONSUME_ATTEMPTS} puts an attempt number through the same
    # method, because the summary worth having is the same one either way.
    #
    # == The tags
    #
    # +exchange+, +queue+, +outcome+, +target+, +pipeline+ and +step+ are what
    # this library attaches. The wider family vocabulary also has +routing.key+,
    # +message.type+ and +transport+, and the first two of those are **not legal
    # Prometheus label names**: a dot is not allowed in one, and a scrape
    # containing +routing.key="order.placed"+ is rejected whole rather than in
    # part. {Registry#to_prometheus} renders them as +routing_key+ and
    # +message_type+; an observer of your own that talks to Prometheus has to do
    # the same.
    #
    # Three methods and no dependency, deliberately. Depending on a metrics gem
    # would put every service using this library on the same one, and the choice
    # between Prometheus, statsd and a log line belongs to the application.
    # {Registry} is here for when the numbers themselves are all that is
    # wanted, and {OpenTelemetry} for spans, which are a different shape
    # again and live in an interceptor rather than in an observer.
    #
    # Every method is called on the path a message takes, from whatever thread
    # is publishing or handling, so an observer has to be safe to call from
    # several at once and must not block. Anything an observer raises is
    # swallowed: a metrics backend that is down is not a reason to stop
    # delivering messages, and a library that let it become one would have made
    # observability a source of outages.
    module Telemetry
      # Publishes, tagged with +exchange+ and +outcome+.
      #
      # One counter rather than two: +outcome=confirmed+ is a message the broker
      # took, +outcome=failed+ one it would not, and the sum of the two is how
      # many times this process tried — a number that used to need adding up.
      PUBLISH_TOTAL = "acemq.publish.total"

      # Deliveries handled, tagged with +queue+ and +outcome+.
      #
      # What the consumer decided — not what the handler asked for. Exactly one
      # +acemq.consume.total+ series goes up per delivery, and the +outcome+ tag
      # is the word naming what really happened: +acked+, +retried+,
      # +rejected+, +dead_lettered+ or +parked+. A handler asking for a retry on
      # its last attempt is tagged +dead_lettered+ and not +retried+, because
      # that is what the consumer will really do with the message, and it is the
      # word its span carries too.
      #
      # +rejected+ is kept apart from +dead_lettered+ even though both end in
      # the dead-letter queue: a message the handler refused on purpose is the
      # system working and one that ran out of attempts is not. +parked+ is a
      # third thing again — nothing could read it, which no number of further
      # attempts will change.
      CONSUME_TOTAL = "acemq.consume.total"

      # How long handlers take, in seconds, tagged with +queue+ and +outcome+.
      #
      # Tagged with the outcome because the interesting question is almost
      # always about one of them: a p99 that includes the failures is a
      # different number from the p99 of the work that succeeded, and a queue
      # whose retries are slow and whose acks are fast looks healthy without
      # this tag.
      CONSUME_DURATION = "acemq.consume.duration"

      # Which attempt each delivery was, tagged with +queue+.
      #
      # A distribution rather than a counter, and it is two numbers in one. Its
      # sample count is how many deliveries this consumer was given, counted on
      # the way **in** — the difference between a queue nothing is reading and a
      # queue one thing is stuck on. Its maximum and mean say how many goes
      # those deliveries are taking, so a rising distribution is a dependency
      # starting to struggle before any of it has reached the dead-letter queue.
      CONSUME_ATTEMPTS = "acemq.consume.attempts"

      # How many messages are being handled right now, tagged with +queue+.
      CONSUME_IN_FLIGHT = "acemq.consume.in.flight"

      # Messages sent to a retry queue, tagged with +queue+.
      #
      # The same deliveries as +acemq.consume.total+ tagged +outcome=retried+,
      # counted again under a name of their own. Kept because a retry rate is
      # the number most often wanted on its own and this is the way to have it
      # without a tag filter — and because Java, Go, .NET and Python all keep
      # it, so an alert written once reads the same against all five.
      RETRIED_TOTAL = "acemq.messages.retried.total"

      # Messages sent to a dead-letter queue, tagged with +queue+.
      #
      # The counterpart of {RETRIED_TOTAL}, and the same deliveries as
      # +acemq.consume.total+ tagged +outcome=dead_lettered+. A message the
      # handler rejected on purpose is not counted here: both end in the same
      # queue, and only the word keeps them apart.
      DEAD_LETTERED_TOTAL = "acemq.messages.dead.lettered.total"

      # Pipeline runs that finished, tagged with +pipeline+, +step+ and
      # +outcome+ — +completed+ when the route ran out, +ended_early+ when a
      # step decided the message goes no further.
      PIPELINE_RUN_TOTAL = "acemq.pipeline.run.total"

      # How long a message had existed when it left a pipeline, in seconds,
      # tagged with +pipeline+.
      #
      # The age of the envelope rather than the time in the last step, so this
      # is the whole run: the envelope was created when the message entered and
      # carried through every hop.
      PIPELINE_RUN_DURATION = "acemq.pipeline.run.duration"

      # Messages that could not be moved to their dead-letter or parking queue,
      # because the republish itself failed.
      #
      # Worth an alert, and usually a queue that was never declared. Nothing is
      # lost: the delivery is never settled, so the broker redelivers it. What
      # it looks like from outside is a handler failing over and over on the
      # same message, which is a different problem with a different fix — and
      # this counter is the only thing that tells the two apart.
      #
      # The Go and Python libraries raise the same counter where they reject the
      # message to the broker instead, so an alert written once reads the same
      # against all three.
      SET_ASIDE_FAILED = "acemq.messages.set.aside.failed"

      # Long retries that had to wait in the consumer because the rung queue
      # they were meant to wait on is not on the broker.
      #
      # Worth an alert. Nothing breaks and no message is lost: the retry still
      # happens and the wait still happens. What is lost is the reason the rung
      # exists — a consumer restarted mid-wait now turns a five-minute backoff
      # into no wait at all — and without this there would be no sign of it,
      # because a topology that was never applied looks exactly like one that
      # was until something has to wait.
      RUNG_MISSING = "acemq.retry.rung.missing"

      # The words the +outcome+ tag is allowed to take.
      #
      # A closed list, and the same list in all five languages, because an
      # +outcome+ tag is only worth having if a dashboard can enumerate it. The
      # delivery outcomes are the {Settlement} words letter for
      # letter — the counter that goes up and the +messaging.acemq.outcome+
      # attribute on the span for the same delivery are read off the one
      # decision and cannot disagree.
      module Outcome
        # A publish the broker took.
        CONFIRMED = "confirmed"
        # A publish the broker accepted and could route nowhere.
        UNROUTABLE = "unroutable"
        # A publish that did not happen.
        FAILED = "failed"

        # The handler was happy.
        ACKED = "acked"
        # Going round again.
        RETRIED = "retried"
        # Out of attempts, or marked as something retrying cannot fix.
        DEAD_LETTERED = "dead_lettered"
        # Refused by the handler.
        REJECTED = "rejected"
        # Nothing could read it.
        PARKED = "parked"

        # A request that got its answer, and one that ran out of patience.
        ANSWERED = "answered"
        TIMED_OUT = "timed_out"

        # An outbox record that made it out.
        PUBLISHED = "published"

        # A pipeline run that reached the end of its route, and one a step
        # stopped before the end.
        COMPLETED = "completed"
        ENDED_EARLY = "ended_early"

        # Every word above, for a test that wants to check a tag is one of them.
        ALL = [CONFIRMED, UNROUTABLE, FAILED, ACKED, RETRIED, DEAD_LETTERED,
               REJECTED, PARKED, ANSWERED, TIMED_OUT, PUBLISHED, COMPLETED,
               ENDED_EARLY].freeze
      end

      # Ignores everything, and is what a connection uses until it is given
      # something else. Measuring nothing is the right default for a library:
      # the cost of metrics belongs to whoever asked for them.
      class None
        def count(_metric, _delta = 1, **_labels) = nil
        def observe(_metric, _value, **_labels) = nil
        def gauge(_metric, _value, **_labels) = nil
      end

      # Keeps the numbers in memory.
      #
      # Enough to serve from a health endpoint, assert on in a test, or print on
      # a signal. It is not a substitute for a real metrics system: there are no
      # percentiles and nothing is exported anywhere on its own.
      #
      #   metrics = Telemetry::Registry.new
      #   mq = Connection.open(url, telemetry: metrics)
      #   ...
      #   puts metrics.to_prometheus
      class Registry
        # What a registry knows about a timing.
        #
        # Deliberately not percentiles. Computing those needs either every
        # sample kept or a sketch, and a library that quietly did either would
        # be making a decision about this process's memory that belongs to
        # whoever runs it.
        class Timing
          attr_reader :count, :sum, :min, :max

          def initialize(count, sum, min, max)
            @count = count
            @sum = sum
            @min = min
            @max = max
            freeze
          end

          # Adds one sample, giving back a new timing rather than changing this
          # one, so a reader holding a timing is holding a number that was true
          # when it read it.
          def plus(value)
            Timing.new(@count + 1, @sum + value, [@min, value].min, [@max, value].max)
          end

          def mean = @count.zero? ? 0.0 : @sum / @count
          def to_a = [@count, @sum, @min, @max]
          def ==(other) = other.is_a?(Timing) && to_a == other.to_a
          alias eql? ==
          def hash = to_a.hash
          def to_s = "#{@count} in #{format("%.3f", @sum)}s (mean #{format("%.4f", mean)}s)"
        end

        def initialize
          @lock = Mutex.new
          @counts = Hash.new(0)
          @gauges = {}
          @timings = {}
        end

        def count(metric, delta = 1, **labels)
          key = self.class.key(metric, labels)
          @lock.synchronize { @counts[key] += delta }
        end

        def observe(metric, value, **labels)
          key = self.class.key(metric, labels)
          @lock.synchronize do
            timing = @timings[key]
            @timings[key] =
              timing.nil? ? Timing.new(1, value, value, value) : timing.plus(value)
          end
        end

        def gauge(metric, value, **labels)
          key = self.class.key(metric, labels)
          @lock.synchronize { @gauges[key] = value }
        end

        # Every counter, keyed by metric and labels.
        def counts = @lock.synchronize { @counts.dup }

        # Every gauge.
        def gauges = @lock.synchronize { @gauges.dup }

        # Every timing, as {Timing}.
        def timings = @lock.synchronize { @timings.dup }

        # One counter, for a test or an endpoint that wants a single number.
        def [](metric, **labels) = @lock.synchronize { @counts[self.class.key(metric, labels)] }

        # The Prometheus text exposition format, as a string.
        #
        # A string rather than a Rack application: this library has no web
        # framework and should not choose one. Serve it from whatever already
        # answers HTTP in the process, on a port the ingress does not publish —
        # what a service publishes and how long its handlers take is more than
        # an anonymous caller should be able to learn.
        #
        # The path every AceMQ library serves this at is +/acemq-metrics+, and
        # health at +/acemq-health+, so a scrape configuration written for one
        # works against another.
        def to_prometheus
          (counts.sort.map { |key, value| render(key, value, "counter") } +
            gauges.sort.map { |key, value| render(key, value, "gauge") } +
            timings.sort.flat_map { |key, timing| render_timing(key, timing) }).join
        end

        # A metric and its labels, flattened into one key.
        #
        # Sorted, because the same labels given in a different order have to
        # produce the same key — otherwise one counter quietly becomes several
        # and the total is wrong in a way nobody notices.
        #
        # @api private
        def self.key(metric, labels)
          return metric.to_s if labels.nil? || labels.empty?

          rendered = labels.map { |name, value| "#{name}=#{value}" }.sort.join(",")
          "#{metric}{#{rendered}}"
        end

        # A metric or label name Prometheus will accept.
        #
        # A colon is legal in a metric name and reserved for recording rules,
        # and is not legal in a label name at all, so it goes the way of the
        # dots: nothing AceMQ writes contains one, and the one rule that is
        # right for both beats two that differ where it never matters.
        #
        # @api private
        def self.legal(name)
          name.to_s.gsub(/[^a-zA-Z0-9_]/, "_")
        end

        private

        def render(key, value, type)
          name, labels = split(key)
          "# TYPE #{name} #{type}\n#{name}#{labels} #{value}\n"
        end

        def render_timing(key, timing)
          name, labels = split(key)
          ["# TYPE #{name} summary\n",
           "#{name}_count#{labels} #{timing.count}\n",
           "#{name}_sum#{labels} #{timing.sum}\n",
           "# TYPE #{name}_min gauge\n#{name}_min#{labels} #{timing.min}\n",
           "# TYPE #{name}_max gauge\n#{name}_max#{labels} #{timing.max}\n"]
        end

        # +acemq.consume.total{queue=orders.new}+ becomes the name and labels
        # Prometheus wants, dots and dashes turned into underscores.
        #
        # The label **names** are put through the same rule as the metric name,
        # and that is not tidiness. Prometheus allows +[a-zA-Z_][a-zA-Z0-9_]*+
        # in a label name and nothing else, so the family's +routing.key+ and
        # +message.type+ are illegal ones — and a single bad line does not lose
        # one series, it makes the scrape unparseable and loses every metric
        # this process publishes. They go out as +routing_key+ and
        # +message_type+, which is what Go settled on and what a dashboard
        # written against any of the five will be asking for.
        def split(key)
          metric, labels = key.split("{", 2)
          name = self.class.legal(metric)
          return [name, ""] if labels.nil?

          pairs = labels.delete_suffix("}").split(",").map do |pair|
            field, value = pair.split("=", 2)
            "#{self.class.legal(field)}=#{value.to_s.inspect}"
          end
          [name, "{#{pairs.join(",")}}"]
        end
      end

      # Reports to an observer without letting the observer break anything.
      #
      # Everything in the library goes through one of these rather than calling
      # an observer directly. A metrics backend that is full, slow or misconfigured
      # is a problem for whoever runs the metrics; turning it into undelivered
      # messages would make observability the thing that caused the outage.
      #
      # @api private
      class Reporter
        # Wraps an observer, or hands back one that is already wrapped.
        #
        # A connection wraps once and passes the wrapper to its consumers, and
        # this is what keeps a consumer from wrapping the wrapper — which would
        # work, and would swallow an observer's failure twice while reporting it
        # under the wrong name.
        def self.for(observer) = observer.is_a?(Reporter) ? observer : new(observer)

        def initialize(observer)
          @observer = observer || None.new
        end

        # The observer underneath, for a caller that wants to read its numbers.
        attr_reader :observer

        def count(metric, delta = 1, **labels)
          @observer.count(metric, delta, **labels)
        rescue StandardError => e
          complain(metric, e)
        end

        def observe(metric, value, **labels)
          @observer.observe(metric, value, **labels)
        rescue StandardError => e
          complain(metric, e)
        end

        def gauge(metric, value, **labels)
          @observer.gauge(metric, value, **labels)
        rescue StandardError => e
          complain(metric, e)
        end

        private

        # Said once per metric rather than once per message. An observer that
        # has been raising since a deploy is worth knowing about; the same line
        # ten thousand times a second is not.
        def complain(metric, error)
          @complained ||= {}
          return if @complained[metric]

          @complained[metric] = true
          warn("acemq: the telemetry observer raised #{error.class} recording " \
               "#{metric}; the message was handled anyway: #{error.message}")
        end
      end
    end
  end
end
