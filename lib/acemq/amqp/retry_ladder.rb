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
require_relative "queue_type"

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
    # whose dead-letter target is the queue it came from — through
    # {Naming::RETRY_EXCHANGE}, which every source queue is bound to by its own
    # name — and the broker returns it when the time is up. Nothing is consuming
    # a rung; the time-to-live is the only thing that ever takes a message out
    # of one.
    #
    # For +orders.new+ with delays of 1s, 30s and 60s and the default threshold:
    #
    #   acemq.retry            direct, durable
    #   orders.new.retry.30s   ttl 30s  -> acemq.retry -> orders.new
    #   orders.new.retry.1m    ttl 60s  -> acemq.retry -> orders.new
    #
    # The one-second delay gets no queue. Below the threshold the wait happens
    # in the consumer, where a second lost to a restart is a second, and the
    # broker is spared a queue per rung of a schedule that mostly runs in the
    # time it takes to notice.
    #
    # The rungs are exactly {RetryPolicy#schedule}, which is a finite list known
    # before anything is published — which is what makes them declarable up
    # front, by {Topology}, rather than discovered one failure at a time.
    #
    # {#declare} reaches one step past the ladder itself, to +{source}.dlq+ and
    # +{source}.parked+ and the exchange they hang off. A ladder is where a
    # message waits between attempts and those two are where it stops waiting,
    # so a consumer that declares the one without the other has somewhere to
    # retry and nowhere to give up — see {#declare_dead_letters}.
    class RetryLadder
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

      attr_reader :source, :threshold, :rungs

      # Whether a delay of this length belongs on a rung queue.
      #
      # Three conditions, and the first of them is the one that is easy to get
      # wrong. A threshold of zero reads as "from zero, so everything" and means
      # the opposite: it switches the broker off, and no rung queue is declared
      # at all. That is what Java, Go, .NET and Python all say, it is what the
      # contract fixture records, and it is the sense that leaves a caller a way
      # to ask for the behaviour — "never use the broker" has no other spelling,
      # whereas "always use the broker" is any threshold below the shortest
      # delay in the schedule.
      #
      # The other two are the obvious ones: a delay shorter than the threshold
      # waits here, and a delay of zero or less has nothing to wait for, so a
      # rung would be a round trip through the broker to achieve nothing.
      #
      # @param delay [Numeric] seconds, unjittered as {RetryPolicy#schedule}
      #   reports them
      # @param threshold [Numeric] seconds
      def self.waits_in_broker?(delay, threshold)
        threshold.positive? && delay.positive? && delay >= threshold
      end

      # Works out the ladder a policy needs, touching no broker.
      #
      # @param source [String] the queue being consumed
      # @param policy [RetryPolicy] whose schedule the rungs are
      # @param threshold [Numeric] seconds; delays at or above it get a rung,
      #   and zero means no delay ever does
      # @return [RetryLadder]
      def self.for(source, policy, threshold: DEFAULT_THRESHOLD)
        source = source.to_s
        rungs = {}
        policy.schedule.each do |delay|
          next unless waits_in_broker?(delay, threshold)

          # Keyed by name rather than by delay. Two delays inside the same
          # second render to the same name, and a second queue by the same name
          # with a different time-to-live is not a second rung — it is a
          # PRECONDITION_FAILED at declaration time.
          name = Naming.retry_queue(source, delay)
          rungs[name] ||= Rung.new(delay: delay, queue: name,
                                   arguments: arguments_for(source, delay))
        end
        new(source: source, threshold: threshold.to_f, rungs: rungs.values)
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
      # A rung expires through {Naming::RETRY_EXCHANGE}, a durable direct
      # exchange, under the source queue's own name as the routing key.
      #
      # The alternative is the default exchange, which routes by queue name and
      # so needs no exchange and no binding at all. That is a real saving and it
      # is not the one taken. Five libraries have to agree here, because two
      # services consuming one queue declare the same rung by name and a rung
      # declared with different arguments answers the second one
      # PRECONDITION_FAILED, leaving it unable to consume at all; Java is the
      # oldest of the five and the one most of the released code follows, Go has
      # been brought into line with it, and this is the shape they share.
      #
      # It is also the better of the two on its own merits. The default exchange
      # cannot be bound, listed or given a policy, so the path a retry takes
      # home exists only in a queue argument nobody can see from the broker; a
      # named exchange puts that path in the topology, where +acemq.retry+ and
      # its bindings are things an operator can look at and a deployment can
      # review. Permissions follow the same line — RabbitMQ grants write per
      # exchange, so a service can be given +acemq.retry+ rather than the
      # default exchange, which is write access to every queue in the vhost.
      #
      # The cost is the binding, and it is a real one: an expired message with
      # nothing bound to carry it is dropped, silently, because an unroutable
      # dead letter goes nowhere and reports nothing. That is answered by never
      # letting the two apart — {#declare} declares the exchange, the rungs and
      # the binding in one call, in that order, and does not make the binding
      # conditional on anything.
      #
      # These three arguments are the cross-language contract for a rung.
      # Anything that changes here changes in Java, Go, .NET and Python too.
      #
      # @api private
      def self.arguments_for(source, delay)
        {
          "x-message-ttl" => (delay * 1000).round,
          "x-dead-letter-exchange" => Naming::RETRY_EXCHANGE,
          "x-dead-letter-routing-key" => source
        }
      end

      def initialize(source:, threshold:, rungs:)
        @source = source
        @threshold = threshold
        @rungs = rungs.freeze
        freeze
      end

      # Whether this policy needs no rungs at all, which is the common case: a
      # schedule that runs in seconds waits in the consumer and costs the broker
      # nothing.
      def empty? = @rungs.empty?

      # The rung a delay belongs in, or nil when the consumer should wait.
      #
      # Nil is the answer for anything {waits_in_broker?} says stays here, and
      # it is the answer a caller acts on rather than a failure — waiting here
      # is the other half of the design, not a fallback.
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
        return nil if @rungs.empty?
        return nil unless RetryLadder.waits_in_broker?(delay, @threshold)

        longer = @rungs.select { |rung| rung.delay >= delay }
        (longer.min_by(&:delay) || @rungs.max_by(&:delay)).queue
      end

      # The rung queue names, in schedule order.
      def queues = @rungs.map(&:queue)

      # Declares everywhere a message this consumer cannot handle is going to
      # end up: the rungs and the way home from them, and the two queues a
      # message lands in when there is no way home left.
      #
      # {Topology} declares the same thing up front, which is where it belongs:
      # a queue that appears in a plan somebody reviewed. This exists because
      # the cost of any of it being absent is silent — a publish into a queue
      # nobody declared is dropped, and so is an expired message with nothing
      # bound to route it — so a retry or a dead letter that goes missing leaves
      # no trace at all. Declaring is idempotent, and a duplicate declaration is
      # a great deal cheaper than a lost message.
      #
      # The retry half is three things, in the order the broker needs them: the
      # exchange, then the rungs that dead-letter to it, then the one binding
      # that carries every expired message back to the queue it came from. The
      # binding is not optional and is not deferred until something first
      # expires. A rung without it looks entirely healthy — the queue is there,
      # the message goes in, the time-to-live runs out — and then the message is
      # dropped, because an unroutable dead letter goes nowhere and says
      # nothing.
      #
      # The dead-letter half is {#declare_dead_letters}, and unlike the retry
      # half it is declared whether or not this ladder has a single rung.
      #
      # The source queue itself is not declared here. It belongs to the caller,
      # it usually has arguments of its own, and creating it as a side effect of
      # setting up its retries would be this library guessing at somebody else's
      # queue. Nothing declared here is bound to it either, except the one
      # binding that brings an expired message home — which is the same
      # binding a topology adds, and a binding the broker accepts against a
      # queue this call did not create.
      #
      # @param connection [Connection, Transport] anything answering
      #   declare_exchange, declare_queue and bind
      # @return [RetryLadder] self
      def declare(connection)
        declare_rungs(connection) unless empty?
        declare_dead_letters(connection)
        self
      end

      # Declares +acemq.dlx+, +{source}.dlq+, +{source}.parked+ and the two
      # bindings that reach them.
      #
      # Separate from the rungs and not conditional on them, which is the whole
      # point of it. A consumer with no retry policy at all still dead-letters
      # — {RetryPolicy.none} is the default, and it gives up on the first
      # failure — so the dead-letter half is needed by every consumer this
      # library starts, while the retry exchange is needed only by one that has
      # somewhere to retry. Making the two conditional on the same thing would
      # leave the commonest consumer in the library the one with nowhere to put
      # a message it could not handle.
      #
      # A consumer that gives up republishes to +{source}.dlq+ through the
      # default exchange, and the default exchange drops what it cannot route
      # without a word: no return, no confirm failure, no log. If no topology
      # was ever applied, that message is gone. Java's RetryTopology.declare has
      # always closed this and the other four libraries now do too — the union
      # of the two halves is identical on a broker somebody set up properly, and
      # the difference only shows on one where a step was missed, which is
      # exactly when losing the evidence costs most.
      #
      # Classic and durable with an empty argument table, which is what
      # {Topology#dead_letter_queue} and {Topology#parked_queue} declare and
      # what Java declares. Same arguments both ways round: a service that
      # applied its topology first and then started a consumer gets a duplicate
      # declaration rather than a PRECONDITION_FAILED, and so does one that did
      # it the other way round.
      #
      # +acemq.dlx+ by name, even for a {Topology} that renamed its dead-letter
      # exchange. The rename is a per-topology courtesy for a shared vhost; a
      # consumer has not been shown the topology and cannot know about it, and
      # the shared name is the one every library binds these two queues under.
      #
      # @param connection [Connection, Transport] anything answering
      #   declare_exchange, declare_queue and bind
      # @return [RetryLadder] self
      def declare_dead_letters(connection)
        connection.declare_exchange(Naming::DEAD_LETTER_EXCHANGE, kind: :direct, durable: true)
        [Naming.dead_letter_queue(@source), Naming.parked_queue(@source)].each do |queue|
          connection.declare_queue(queue, queue_type: QueueType::CLASSIC, durable: true,
                                          arguments: {})
          connection.bind(queue: queue, exchange: Naming::DEAD_LETTER_EXCHANGE,
                          routing_key: queue)
        end
        self
      end

      def to_s
        return "no retry rungs for #{@source}" if empty?

        "retry rungs for #{@source}: #{@rungs.join(", ")}"
      end

      private

      def declare_rungs(connection)
        connection.declare_exchange(Naming::RETRY_EXCHANGE, kind: :direct, durable: true)
        # Classic, said out loud rather than left to the default, which is
        # quorum. A rung is a queue the Java, Go, .NET and Python libraries
        # declare too, all four of them classic, and the queue type is part of
        # the argument table the broker compares: a rung declared quorum here
        # would be refused to every one of them.
        @rungs.each do |rung|
          connection.declare_queue(rung.queue, queue_type: QueueType::CLASSIC,
                                               durable: true, arguments: rung.arguments)
        end
        connection.bind(queue: @source, exchange: Naming::RETRY_EXCHANGE, routing_key: @source)
      end
    end
  end
end
