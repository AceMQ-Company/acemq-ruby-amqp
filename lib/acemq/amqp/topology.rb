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
require_relative "retry_ladder"

module AceMQ
  module AMQP
    # A topology that describes something a broker cannot be asked to build.
    class TopologyError < StandardError; end

    # The exchanges, queues and bindings a service expects to find.
    #
    # Declaring them one call at a time works, and stops working the moment
    # somebody needs to know what a service will do to a broker before it does
    # it. A topology can be printed and read in a deployment review, checked
    # for the mistakes that are cheaper to catch here than on the broker, and
    # then applied — which is the difference between a change somebody approved
    # and one they found out about.
    #
    #   topology = Topology.new
    #     .exchange("orders-events", :topic)
    #     .queue("shipping-orders", dead_letter: true)
    #     .binding("shipping-orders", "orders-events", "order.placed")
    #     .binding("shipping-orders", "orders-events", "order.cancelled")
    #
    #   puts topology            # what it would do
    #   topology.apply(connection)
    #
    # Every builder method returns self and mutates in place, which is the one
    # place in this library that is not a frozen value object: a topology is
    # assembled and then applied, and threading a new copy through six chained
    # calls would buy immutability nobody is using.
    class Topology
      # The exchange dead letters reach their queue through.
      #
      # The name itself lives in {Naming}, with the retry exchange it is a pair
      # with, so that the two strings the broker's shape depends on are written
      # down in one place and read everywhere else.
      DEAD_LETTER_EXCHANGE = Naming::DEAD_LETTER_EXCHANGE

      # The exchange a retry rung expires through.
      #
      # Unlike {DEAD_LETTER_EXCHANGE} this one cannot be renamed per topology,
      # and that is deliberate rather than an omission. The dead-letter exchange
      # is only ever named by the queue arguments this topology writes itself,
      # so a team on a shared vhost can point it somewhere inside their own
      # prefix without anybody else noticing. The retry exchange is written into
      # the rung's argument table, which is the table five libraries have to
      # agree on: a topology that renamed it would declare rungs that answer
      # PRECONDITION_FAILED to the very consumer meant to publish into them.
      RETRY_EXCHANGE = Naming::RETRY_EXCHANGE

      Exchange = Struct.new(:name, :kind, :durable, :auto_delete, :arguments,
                            keyword_init: true)
      Queue = Struct.new(:name, :type, :durable, :auto_delete, :exclusive, :arguments,
                         keyword_init: true)
      Binding = Struct.new(:queue, :exchange, :routing_key, keyword_init: true) do
        def to_s = "#{exchange} -> #{queue} (#{routing_key})"
      end

      attr_reader :exchanges, :queues, :bindings, :dead_letter_exchange

      # @param dead_letter_exchange [String] where dead letters are routed
      #   through. The shared name by default, which is what makes a broker
      #   legible; nameable because a shared vhost with one team per prefix has
      #   no business declaring an exchange outside its own.
      def initialize(dead_letter_exchange: DEAD_LETTER_EXCHANGE)
        @dead_letter_exchange = dead_letter_exchange
        @exchanges = []
        @queues = []
        @bindings = []
      end

      # Adds a durable exchange.
      #
      # @param name [String]
      # @param kind [Symbol, String] direct, topic, fanout or headers
      # @return [Topology] self
      def exchange(name, kind, durable: true, auto_delete: false, arguments: {})
        @exchanges << Exchange.new(name: name.to_s, kind: kind.to_s, durable: durable,
                                   auto_delete: auto_delete, arguments: arguments)
        self
      end

      # Adds a durable quorum queue, which is the default everywhere in this
      # library.
      #
      # The kind matters as much as the name. RabbitMQ treats +x-queue-type+ as
      # part of a queue's identity, so a queue that exists as a quorum queue
      # answers PRECONDITION_FAILED to anybody declaring it classic — and that
      # anybody is the second service to start, in whichever language it
      # happens to be written. Java declares a source queue quorum, Java is the
      # one with deployments, so the other four libraries declare it quorum too
      # and a Ruby service can share +orders+ with a Java one.
      #
      # +queue_type: :classic+ still gets a classic queue for a caller who
      # wants one, and a +x-queue-type+ in +arguments+ is honoured as it always
      # was, which is how {Patterns.declare_stream} declares a stream. A queue
      # that is exclusive, auto-deleting or transient is classic whatever the
      # default says, because RabbitMQ refuses to replicate a queue that goes
      # away on its own; asking for quorum *and* one of those flags is a
      # {QueueTypeError} rather than a silent downgrade.
      #
      # +dead_letter+ wires the queue to its own dead-letter queue, using the
      # names from {Naming} so that the dead letters of +orders.new+ are in
      # +orders.new.dlq+ whichever language declared them. It adds the queue,
      # the shared exchange and the binding between them, so a caller does not
      # have to remember three declarations to get one behaviour right.
      #
      # +retry_policy+ adds the rungs that policy needs: the queues a retry
      # waits in when its delay is long enough that waiting in the consumer
      # would lose it to a restart. A policy is the right thing to hand a
      # topology because the rungs *are* {RetryPolicy#schedule} — a finite list
      # of delays, known before anything is published — so they can be declared
      # once, here, and appear in a plan somebody reviews, rather than being
      # discovered one failure at a time. Passing the policy rather than a list
      # of delays is what keeps the two from drifting: a queue whose consumer
      # runs a policy this was never told about has rungs that do not match its
      # waits, and the symptom of that is a retry which quietly never comes
      # back.
      #
      # @param name [String]
      # @param queue_type [Symbol, nil] +:quorum+ by default, +:classic+ or
      #   +:stream+ for a caller who needs one
      # @param dead_letter [Boolean] whether to wire up +{name}.dlq+
      # @param retry_policy [RetryPolicy, nil] whose rungs to declare
      # @param retry_threshold [Numeric] seconds; delays at or above it get a
      #   rung, and must match what the consumer of this queue is configured
      #   with
      # @return [Topology] self
      # @raise [QueueTypeError] when the kind asked for and the flags asked for
      #   cannot both be had
      def queue(name, durable: true, auto_delete: false, exclusive: false, arguments: {},
                queue_type: nil, dead_letter: false, retry_policy: nil,
                retry_threshold: RetryLadder::DEFAULT_THRESHOLD)
        name = name.to_s
        arguments = dead_letter_arguments(name, arguments) if dead_letter
        add_queue(name, queue_type, durable: durable, auto_delete: auto_delete,
                                    exclusive: exclusive, arguments: arguments)
        dead_letter_queue(name) if dead_letter
        retry_ladder(name, retry_policy, threshold: retry_threshold) if retry_policy
        self
      end

      # Adds the retry rungs a policy needs for a queue, and the exchange that
      # brings their expired messages home.
      #
      # Called for you by +queue(..., retry_policy: policy)+. Public for the
      # same reason {#dead_letter_queue} is: a deployment that only creates the
      # broker's shape, and never consumes, still has to create these.
      #
      # Nothing binds a consumer to a rung, and nothing should: a rung is only
      # ever published into, and a consumer on one would take the message
      # before its time-to-live had expired, which is the entire wait.
      #
      # The binding that does get added goes the other way: +source+ is bound to
      # {RETRY_EXCHANGE} under its own name, which is what carries an expired
      # message home. It is added here rather than left to whoever declares the
      # source queue because a rung without it fails silently — the message
      # enters the rung, the time-to-live runs out, and the broker drops it,
      # having nowhere to route it and nobody to tell. Which is also why
      # +source+ has to be a queue this topology declares: {#problems} says so,
      # and the alternative is a plan that binds something no deployment
      # creates.
      #
      # @param source [String] the queue whose retries these are
      # @param policy [RetryPolicy]
      # @param threshold [Numeric] seconds
      # @return [Topology] self
      def retry_ladder(source, policy, threshold: RetryLadder::DEFAULT_THRESHOLD)
        source = source.to_s
        ladder = RetryLadder.for(source, policy, threshold: threshold)
        return self if ladder.empty?

        exchange(RETRY_EXCHANGE, :direct) unless declared_exchange?(RETRY_EXCHANGE)
        # Classic, and not because nobody got round to changing it. Java's
        # RetryTopology declares every rung CLASSIC, and a rung is a queue two
        # libraries publish into by name, so the argument table — the queue type
        # included — has to be the same one in both. It is also the queue this
        # design leans on hardest: a rung exists to hold a message until a
        # time-to-live expires it into an exchange, which is the plainest thing
        # a classic queue does.
        ladder.rungs.each do |rung|
          add_queue(rung.queue, QueueType::CLASSIC, durable: true, auto_delete: false,
                                                    exclusive: false, arguments: rung.arguments)
        end
        binding(source, RETRY_EXCHANGE, source)
      end

      # Routes messages matching a key from an exchange to a queue.
      #
      # @return [Topology] self
      def binding(queue, exchange, routing_key = "")
        @bindings << Binding.new(queue: queue.to_s, exchange: exchange.to_s,
                                 routing_key: routing_key.to_s)
        self
      end

      # Adds the dead-letter queue for a source queue, and the exchange that
      # reaches it.
      #
      # Called for you by +queue(..., dead_letter: true)+. It is public because
      # a service that only consumes dead letters needs the queue declared
      # without declaring the source queue it drains.
      #
      # Classic, like the rungs and for the same reason: Java declares
      # +{source}.dlq+ CLASSIC, and a dead-letter queue is a queue two services
      # in two languages both declare before either of them drains it.
      #
      # @param source [String] the queue whose dead letters these are
      # @return [Topology] self
      def dead_letter_queue(source)
        source = source.to_s
        dlq = Naming.dead_letter_queue(source)
        unless declared_exchange?(@dead_letter_exchange)
          exchange(@dead_letter_exchange,
                   :direct)
        end
        add_queue(dlq, QueueType::CLASSIC, durable: true, auto_delete: false,
                                           exclusive: false, arguments: {})
        binding(dlq, @dead_letter_exchange, dlq)
      end

      # Adds the parking queue for a source queue, where messages that cannot
      # even be decoded go.
      #
      # Separate from the dead-letter queue on purpose: a message that failed
      # five times and a message nothing could read are two different problems,
      # and mixing them means whoever drains the dead letters has to sort them
      # by hand.
      #
      # Classic, for the reason {#dead_letter_queue} is.
      #
      # @param source [String]
      # @return [Topology] self
      def parked_queue(source)
        parked = Naming.parked_queue(source.to_s)
        unless declared_exchange?(@dead_letter_exchange)
          exchange(@dead_letter_exchange,
                   :direct)
        end
        add_queue(parked, QueueType::CLASSIC, durable: true, auto_delete: false,
                                              exclusive: false, arguments: {})
        binding(parked, @dead_letter_exchange, parked)
      end

      # What is wrong with the description itself, before any of it reaches a
      # broker.
      #
      # A binding naming a queue nothing declares is the mistake worth catching
      # here. The broker would accept it if the queue happened to exist
      # already, and the service would then quietly depend on something no
      # deployment creates — which works until the day it is deployed
      # somewhere new.
      #
      # @return [Array<String>] every problem found, empty when there are none
      def problems
        blank_names + duplicates + missing_kinds + unresolved_bindings
      end

      # @raise [TopologyError] when {#problems} finds anything
      # @return [Topology] self
      def validate!
        found = problems
        return self if found.empty?

        raise TopologyError, "this topology cannot be applied:\n  #{found.join("\n  ")}"
      end

      # What applying it would do, in the order a broker needs it.
      #
      # Deliberately not a diff against the live broker: AMQP offers no way to
      # enumerate what is there without the management API, and a plan that
      # quietly guessed would be worse than one honest about being a statement
      # of intent.
      #
      # @return [Array<String>]
      def plan
        validate!
        @exchanges.map { |e| "declare exchange #{e.name} (#{describe_exchange(e)})" } +
          @queues.map { |q| "declare queue #{q.name} (#{describe_queue(q)})" } +
          @bindings.map { |b| "bind #{b.queue} to #{b.exchange} on #{b.routing_key.inspect}" }
      end

      # Declares everything, in the order a broker needs: exchanges, then
      # queues, then the bindings between them.
      #
      # It stops at the first failure. A queue that already exists with
      # different settings is refused with PRECONDITION_FAILED, and that
      # refusal is passed on rather than swallowed: it means this service and
      # the broker disagree about what the queue is, and carrying on would
      # leave the disagreement in place with nobody told.
      #
      # @param connection [Connection, Transport] anything answering
      #   declare_exchange, declare_queue and bind
      # @return [Topology] self
      def apply(connection)
        validate!
        @exchanges.each do |e|
          connection.declare_exchange(
            e.name, kind: e.kind, durable: e.durable,
                    auto_delete: e.auto_delete, arguments: e.arguments
          )
        end
        # The kind goes out as well as the arguments, even though the arguments
        # already carry it. A connection defaults an unqualified declaration to
        # quorum, and a plan that said classic and then let the default answer
        # for it would declare something nobody reviewed.
        @queues.each do |q|
          connection.declare_queue(q.name, queue_type: q.type, durable: q.durable,
                                           auto_delete: q.auto_delete,
                                           exclusive: q.exclusive, arguments: q.arguments)
        end
        @bindings.each do |b|
          connection.bind(queue: b.queue, exchange: b.exchange, routing_key: b.routing_key)
        end
        self
      end

      # The plan, as something worth putting in a deployment log.
      def to_s
        header = "Topology: #{@exchanges.size} exchanges, #{@queues.size} queues, " \
                 "#{@bindings.size} bindings"
        found = problems
        return "#{header}\n  invalid: #{found.join("\n  invalid: ")}" unless found.empty?

        ([header] + plan.map { |line| "  #{line}" }).join("\n")
      end

      private

      # Adds one queue, with its kind settled before anything is stored.
      #
      # Settled here rather than at apply time so that the plan somebody reads
      # is the declaration the broker gets: the kind is in the printed line and
      # +x-queue-type+ is in the argument table, and neither is worked out later
      # by something the reviewer never saw.
      def add_queue(name, requested, durable:, auto_delete:, exclusive:, arguments:)
        type, arguments = QueueType.resolve(name: name, requested: requested, durable: durable,
                                            exclusive: exclusive, auto_delete: auto_delete,
                                            arguments: arguments)
        @queues << Queue.new(name: name, type: type, durable: durable, auto_delete: auto_delete,
                             exclusive: exclusive, arguments: arguments)
      end

      # The arguments that send a queue's rejected messages to its own dead
      # letter queue.
      #
      # The routing key is set as well as the exchange. Without it the broker
      # reuses the message's original routing key, which on a topic exchange is
      # whatever the message was published under — so the dead letters of six
      # different routing keys would go to six places, none of them named
      # +.dlq+.
      def dead_letter_arguments(name, arguments)
        {
          "x-dead-letter-exchange" => @dead_letter_exchange,
          "x-dead-letter-routing-key" => Naming.dead_letter_queue(name)
        }.merge(arguments.to_h { |key, value| [key.to_s, value] })
      end

      def declared_exchange?(name)
        @exchanges.any? { |e| e.name == name }
      end

      # A queue declared with no name is not a mistake the broker will report
      # usefully: RabbitMQ takes an empty name as "give me a generated one",
      # so the declaration succeeds and the service then binds and consumes
      # nothing, with no error anywhere.
      def blank_names
        [[@queues, "a queue"], [@exchanges, "an exchange"]].flat_map do |set, what|
          set.select { |item| item.name.empty? }.map { "#{what} in this topology has no name" }
        end
      end

      def duplicates
        [[@queues, "queue"], [@exchanges, "exchange"]].flat_map do |set, what|
          set.map(&:name).tally.select { |_, count| count > 1 }
             .map { |name, count| "#{what} #{name.inspect} is declared #{count} times" }
        end
      end

      def missing_kinds
        @exchanges.select { |e| e.kind.empty? }.map do |e|
          "exchange #{e.name.inspect} has no kind (direct, topic, fanout or headers)"
        end
      end

      def unresolved_bindings
        queues = @queues.map(&:name)
        exchanges = @exchanges.map(&:name)
        @bindings.flat_map do |b|
          [binding_queue_problem(b, queues), binding_exchange_problem(b, exchanges)].compact
        end
      end

      def binding_queue_problem(binding, queues)
        return nil if queues.include?(binding.queue)

        "binding #{binding} names queue #{binding.queue.inspect}, " \
          "which this topology does not declare"
      end

      def binding_exchange_problem(binding, exchanges)
        if binding.exchange.empty?
          return "binding #{binding} names the default exchange, which cannot be bound to"
        end
        return nil if exchanges.include?(binding.exchange)

        "binding #{binding} names exchange #{binding.exchange.inspect}, " \
          "which this topology does not declare"
      end

      def describe_exchange(exchange)
        parts = [exchange.kind]
        parts << "transient" unless exchange.durable
        parts << "auto-delete" if exchange.auto_delete
        parts.join(", ")
      end

      # The kind leads the line because it is the thing a reviewer holding this
      # plan beside another library's is checking. It is not repeated from the
      # argument table below it: +x-queue-type+ *is* the kind, and printing it
      # twice would leave somebody wondering which one to believe.
      def describe_queue(queue)
        parts = [queue.type.to_s, queue.durable ? "durable" : "transient"]
        parts << "auto-delete" if queue.auto_delete
        parts << "exclusive" if queue.exclusive
        arguments = queue.arguments.reject { |name, _| name.to_s == QueueType::ARGUMENT }
                         .sort_by { |name, _| name.to_s }
        (parts + arguments.map { |name, value| "#{name}=#{value}" }).join(", ")
      end
    end
  end
end
