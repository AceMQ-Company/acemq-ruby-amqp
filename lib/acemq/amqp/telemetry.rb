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
    # What the library counts, and where it sends the numbers.
    #
    # The names are shared with the Java, Go, .NET and Python libraries, so a
    # dashboard or an alert written against one service reads the same against
    # the next. Java publishes them through Micrometer and .NET through
    # System.Diagnostics.Metrics; Ruby has no standard metrics interface at all,
    # so this counts them and hands them to whatever you already run.
    #
    # == The interface
    #
    # An observer is anything answering three methods:
    #
    #   def count(metric, delta = 1, **labels)   # a counter goes up
    #   def observe(metric, seconds, **labels)   # a duration is recorded
    #   def gauge(metric, value, **labels)       # a current value is set
    #
    # Three methods and no dependency, deliberately. Depending on a metrics gem
    # would put every service using this library on the same one, and the choice
    # between Prometheus, OpenTelemetry, statsd and a log line belongs to the
    # application. {Registry} is here for when the numbers themselves are all
    # that is wanted.
    #
    # Every method is called on the path a message takes, from whatever thread
    # is publishing or handling, so an observer has to be safe to call from
    # several at once and must not block. Anything an observer raises is
    # swallowed: a metrics backend that is down is not a reason to stop
    # delivering messages, and a library that let it become one would have made
    # observability a source of outages.
    module Telemetry
      # Messages handed to the broker and confirmed.
      PUBLISHED = "acemq.messages.published"

      # Publishes the broker would not take.
      PUBLISH_FAILED = "acemq.messages.publish.failed"

      # Messages delivered to a handler.
      CONSUMED = "acemq.messages.consumed"

      # What handlers decided.
      ACCEPTED = "acemq.messages.accepted"
      RETRIED = "acemq.messages.retried"
      REJECTED = "acemq.messages.rejected"

      # Messages that ran out of attempts, or were refused for a reason
      # retrying cannot fix.
      DEAD_LETTERED = "acemq.messages.dead.lettered"

      # Messages nothing could decode, which go somewhere a person looks.
      PARKED = "acemq.messages.parked"

      # How long handlers take, in seconds.
      HANDLER_DURATION = "acemq.handler.duration"

      # How many messages are being handled right now.
      IN_FLIGHT = "acemq.messages.in.flight"

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

      # Ignores everything, and is what a connection uses until it is given
      # something else. Measuring nothing is the right default for a library:
      # the cost of metrics belongs to whoever asked for them.
      class None
        def count(_metric, _delta = 1, **_labels) = nil
        def observe(_metric, _seconds, **_labels) = nil
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
          def plus(seconds)
            Timing.new(@count + 1, @sum + seconds, [@min, seconds].min, [@max, seconds].max)
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

        def observe(metric, seconds, **labels)
          key = self.class.key(metric, labels)
          @lock.synchronize do
            timing = @timings[key]
            @timings[key] =
              timing.nil? ? Timing.new(1, seconds, seconds, seconds) : timing.plus(seconds)
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

        # +acemq.messages.published{queue=orders.new}+ becomes the name and
        # labels Prometheus wants, dots and dashes turned into underscores.
        def split(key)
          metric, labels = key.split("{", 2)
          name = metric.tr(".-", "__")
          return [name, ""] if labels.nil?

          pairs = labels.delete_suffix("}").split(",").map do |pair|
            field, value = pair.split("=", 2)
            "#{field}=#{value.to_s.inspect}"
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

        def observe(metric, seconds, **labels)
          @observer.observe(metric, seconds, **labels)
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
