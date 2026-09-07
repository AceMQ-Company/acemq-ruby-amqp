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
      # A name shared with the Java, Go, .NET and Python libraries, because an
      # operator looking at a broker should see one dead-letter exchange rather
      # than one per language that happened to publish to it.
      DEAD_LETTER_EXCHANGE = "acemq.dlx"

      Exchange = Struct.new(:name, :kind, :durable, :auto_delete, :arguments,
                            keyword_init: true)
      Queue = Struct.new(:name, :durable, :auto_delete, :exclusive, :arguments,
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

      # Adds a durable queue.
      #
      # +dead_letter+ wires the queue to its own dead-letter queue, using the
      # names from {Naming} so that the dead letters of +orders.new+ are in
      # +orders.new.dlq+ whichever language declared them. It adds the queue,
      # the shared exchange and the binding between them, so a caller does not
      # have to remember three declarations to get one behaviour right.
      #
      # @param name [String]
      # @param dead_letter [Boolean] whether to wire up +{name}.dlq+
      # @return [Topology] self
      def queue(name, durable: true, auto_delete: false, exclusive: false, arguments: {},
                dead_letter: false)
        name = name.to_s
        arguments = dead_letter_arguments(name, arguments) if dead_letter
        @queues << Queue.new(name: name, durable: durable, auto_delete: auto_delete,
                             exclusive: exclusive, arguments: arguments)
        dead_letter_queue(name) if dead_letter
        self
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
      # @param source [String] the queue whose dead letters these are
      # @return [Topology] self
      def dead_letter_queue(source)
        source = source.to_s
        dlq = Naming.dead_letter_queue(source)
        unless declared_exchange?(@dead_letter_exchange)
          exchange(@dead_letter_exchange,
                   :direct)
        end
        @queues << Queue.new(name: dlq, durable: true, auto_delete: false, exclusive: false,
                             arguments: {})
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
      # @param source [String]
      # @return [Topology] self
      def parked_queue(source)
        parked = Naming.parked_queue(source.to_s)
        unless declared_exchange?(@dead_letter_exchange)
          exchange(@dead_letter_exchange,
                   :direct)
        end
        @queues << Queue.new(name: parked, durable: true, auto_delete: false, exclusive: false,
                             arguments: {})
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
        @queues.each do |q|
          connection.declare_queue(q.name, durable: q.durable, auto_delete: q.auto_delete,
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

      def describe_queue(queue)
        parts = [queue.durable ? "durable" : "transient"]
        parts << "auto-delete" if queue.auto_delete
        parts << "exclusive" if queue.exclusive
        arguments = queue.arguments.sort_by { |name, _| name.to_s }
        (parts + arguments.map { |name, value| "#{name}=#{value}" }).join(", ")
      end
    end
  end
end
