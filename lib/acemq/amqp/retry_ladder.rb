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

require_relative "naming"

module AceMQ
  module AMQP
    # The queues a long retry waits in, so that the wait is the broker's and not
    # this process's.
    #
    # A consumer that sleeps for a five-minute backoff is holding an
    # unacknowledged message. Restart it — a deploy, a crash, an autoscaler —
    # and the broker redelivers at once, so a five-minute policy becomes
    # instant. That is a correctness bug rather than a throughput one, and it is
    # why delays past a threshold are handed to the broker instead: the message
    # is published into a rung queue whose +x-message-ttl+ is the delay and
    # whose dead-letter target is the queue it came from, and the broker returns
    # it when the time is up. Nothing is consuming a rung; the time-to-live is
    # the only thing that ever takes a message out of one.
    #
    # For +orders.new+ with delays of 1s, 30s and 60s and the default threshold:
    #
    #   orders.new.retry.30s   ttl 30s  -> orders.new
    #   orders.new.retry.1m    ttl 60s  -> orders.new
    #
    # The one-second delay gets no queue. Below the threshold the wait happens
    # in the consumer, where a second lost to a restart is a second, and the
    # broker is spared a queue per rung of a schedule that mostly runs in the
    # time it takes to notice.
    #
    # The rungs are exactly {RetryPolicy#schedule}, which is a finite list known
    # before anything is published — which is what makes them declarable up
    # front, by {Topology}, rather than discovered one failure at a time.
    class RetryLadder
      # The exchange a rung dead-letters through on its way back to the source
      # queue. Shared with the Java, Go, .NET and Python libraries: an operator
      # should see one retry exchange on a broker rather than one per language
      # that happened to publish to it.
      RETRY_EXCHANGE = "acemq.retry"

      # Delays at or above this go to the broker; anything shorter waits here.
      #
      # Thirty seconds because that is roughly where the two failure modes
      # cross. Below it a prefetch slot held for the wait is cheap and a wait
      # lost to a restart costs seconds. Above it the slot is expensive and the
      # lost wait is the whole delay, which is the bug worth spending a queue
      # on.
      DEFAULT_THRESHOLD = 30.0

      # One step of the ladder: a delay, the queue that expresses it, and the
      # arguments that queue has to be declared with for it to mean anything.
      Rung = Struct.new(:delay, :queue, :arguments, keyword_init: true) do
        def to_s = "#{queue} (ttl #{delay}s)"
      end

      attr_reader :source, :threshold, :exchange, :rungs

      # Works out the ladder a policy needs, touching no broker.
      #
      # @param source [String] the queue being consumed
      # @param policy [RetryPolicy] whose schedule the rungs are
      # @param threshold [Numeric] seconds; delays at or above it get a rung
      # @param exchange [String] the exchange rungs dead-letter through
      # @return [RetryLadder]
      def self.for(source, policy, threshold: DEFAULT_THRESHOLD, exchange: RETRY_EXCHANGE)
        source = source.to_s
        rungs = {}
        policy.schedule.each do |delay|
          next unless delay.positive? && delay >= threshold

          # Keyed by name rather than by delay. Two delays inside the same
          # second render to the same name, and a second queue by the same name
          # with a different time-to-live is not a second rung — it is a
          # PRECONDITION_FAILED at declaration time.
          name = Naming.retry_queue(source, delay)
          rungs[name] ||= Rung.new(delay: delay, queue: name,
                                   arguments: arguments_for(source, delay, exchange))
        end
        new(source: source, threshold: threshold.to_f, exchange: exchange,
            rungs: rungs.values)
      end

      # The arguments a rung queue has to carry.
      #
      # +x-message-ttl+ and not a per-message expiration, ever. RabbitMQ expires
      # messages only from the head of a queue, so a message with a long
      # per-message TTL at the front holds back every shorter one behind it, and
      # the delays that come out bear no relation to the ones that went in. A
      # queue per delay is more queues and is the only arrangement that actually
      # delivers the schedule it was given.
      #
      # @api private
      def self.arguments_for(source, delay, exchange)
        {
          "x-message-ttl" => (delay * 1000).round,
          "x-dead-letter-exchange" => exchange,
          "x-dead-letter-routing-key" => source
        }
      end

      def initialize(source:, threshold:, exchange:, rungs:)
        @source = source
        @threshold = threshold
        @exchange = exchange
        @rungs = rungs.freeze
        freeze
      end

      # Whether this policy needs no rungs at all, which is the common case: a
      # schedule that runs in seconds waits in the consumer and costs the broker
      # nothing.
      def empty? = @rungs.empty?

      # The rung a delay belongs in, or nil when the consumer should wait.
      #
      # Nil is the answer for anything below the threshold, and it is the answer
      # a caller acts on rather than a failure — waiting here is the other half
      # of the design, not a fallback.
      #
      # A delay that is not exactly a rung is rounded up to the next one, which
      # cannot happen for a delay this ladder's own policy produced but can for
      # one a caller worked out some other way. Up rather than down because
      # waiting slightly too long is harmless and retrying early defeats the
      # backoff.
      #
      # @param delay [Numeric] seconds
      # @return [String, nil] the queue to publish into
      def rung_for(delay)
        return nil if @rungs.empty? || delay < @threshold

        longer = @rungs.select { |rung| rung.delay >= delay }
        (longer.min_by(&:delay) || @rungs.max_by(&:delay)).queue
      end

      # The rung queue names, in schedule order.
      def queues = @rungs.map(&:queue)

      # Declares the exchange, the rungs and the binding that brings an expired
      # message home.
      #
      # {Topology} declares the same thing up front, which is where it belongs:
      # a queue that appears in a plan somebody reviewed. This exists because
      # the cost of the rung being absent is silent — the default exchange drops
      # what it cannot route, so a retry published into a queue nobody declared
      # is a message that simply stops existing. Declaring is idempotent, and a
      # duplicate declaration is a great deal cheaper than a lost message.
      #
      # @param connection [Connection, Transport] anything answering
      #   declare_exchange, declare_queue and bind
      # @return [RetryLadder] self
      def declare(connection)
        return self if empty?

        connection.declare_exchange(@exchange, kind: "direct", durable: true)
        @rungs.each do |rung|
          connection.declare_queue(rung.queue, durable: true, arguments: rung.arguments)
        end
        # One binding brings every rung's expired messages back, because every
        # rung dead-letters under the source queue's own name.
        connection.bind(queue: @source, exchange: @exchange, routing_key: @source)
        self
      end

      def to_s
        return "no retry rungs for #{@source}" if empty?

        "retry rungs for #{@source}: #{@rungs.join(", ")}"
      end
    end
  end
end
