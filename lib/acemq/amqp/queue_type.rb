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
    # A queue the broker would refuse as asked for.
    class QueueTypeError < StandardError; end

    # Which kind of queue to declare, and what that puts in the argument table.
    #
    # RabbitMQ decides a queue's kind from one argument, +x-queue-type+, and
    # then treats that argument as part of the queue's identity: a queue that
    # exists as +quorum+ answers PRECONDITION_FAILED to anyone declaring it as
    # anything else. So the kind is not a local preference. It is part of the
    # contract five libraries have to agree on, in the same way the retry
    # rung's arguments are, and it is written down here rather than at each of
    # the half-dozen call sites that declare something.
    #
    # **A durable, named, shared queue is a quorum queue.** That is the default
    # in the Java library, which is the one with deployments, and the other four
    # follow it: a replicated queue survives losing the node its leader was on,
    # which is the failure a classic queue turns into lost messages.
    #
    # **The queues this library owns around it stay classic**, deliberately:
    # the retry rungs, +{queue}.dlq+ and +{queue}.parked+. They are declared
    # +CLASSIC+ in Java's RetryTopology and have to be declared the same way
    # here, and a rung's whole behaviour is a time-to-live expiring into a
    # dead-letter exchange, which is what a classic queue does most simply.
    #
    # **Anything exclusive or auto-deleting is classic because it can be
    # nothing else.** RabbitMQ rejects an exclusive or auto-delete quorum queue
    # outright, and a quorum queue is always durable. A reply queue belonging to
    # one requester, or a health probe's queue, is therefore left classic rather
    # than being declared into a refusal.
    module QueueType
      # The argument RabbitMQ reads the kind from.
      ARGUMENT = "x-queue-type"

      # A single-node queue. Fast, and lost with its node.
      CLASSIC = :classic

      # A replicated queue with a consensus protocol behind it.
      QUORUM = :quorum

      # An append-only, replayable log with consumer-held offsets.
      STREAM = :stream

      KNOWN = [CLASSIC, QUORUM, STREAM].freeze

      # What a durable, named queue is when nobody says otherwise.
      DEFAULT = QUORUM

      # The kind a declaration is asking for, and the arguments that go with it.
      #
      #   type, arguments = QueueType.resolve(name: "orders")
      #   # => [:quorum, { "x-queue-type" => "quorum" }]
      #
      # The kind can be said three ways and they are read in this order: the
      # +requested+ keyword, an +x-queue-type+ the caller put in the argument
      # table themselves (which is how a stream is declared), and finally the
      # default. Two of them disagreeing is refused rather than resolved, since
      # either answer would be a guess at which one was meant.
      #
      # A classic queue carries no +x-queue-type+ at all, which is what Java
      # sends and therefore what the broker has to be told: an argument table
      # that differs from another library's is a PRECONDITION_FAILED for
      # whichever service declares second.
      #
      # @param name [String] only ever used to say which queue a refusal is about
      # @param requested [Symbol, String, nil] +:classic+, +:quorum+, +:stream+
      # @param durable [Boolean]
      # @param exclusive [Boolean]
      # @param auto_delete [Boolean]
      # @param arguments [Hash] the caller's argument table
      # @return [Array(Symbol, Hash)] the kind, and the arguments to declare with
      # @raise [QueueTypeError] when the kind asked for is not one the broker
      #   would accept for a queue declared this way
      def self.resolve(name: "", requested: nil, durable: true, exclusive: false,
                       auto_delete: false, arguments: {})
        requested = known!(name, requested)
        declared = known!(name, arguments[ARGUMENT] || arguments[ARGUMENT.to_sym])
        impossible = classic_only?(durable: durable, exclusive: exclusive,
                                   auto_delete: auto_delete)
        type = agreed(name, requested, declared) || (impossible ? CLASSIC : DEFAULT)
        refuse_impossible(name, type, durable: durable, exclusive: exclusive,
                                      auto_delete: auto_delete)
        [type, table(type, arguments)]
      end

      # Whether the broker would only ever accept this queue as a classic one.
      #
      # Quorum queues and streams are replicated on disk, which is why RabbitMQ
      # allows neither to be exclusive, auto-deleting or transient: all three
      # mean "this queue goes away on its own", and there is nothing to replicate
      # about a queue that does.
      def self.classic_only?(durable: true, exclusive: false, auto_delete: false)
        !durable || exclusive || auto_delete
      end

      # The argument table a queue of this kind is declared with.
      #
      # Classic queues get the caller's table untouched, which matters more than
      # it looks: adding +x-queue-type+ => +classic+ where Java sends nothing
      # would be a difference the broker judges us on.
      def self.table(type, arguments)
        return arguments if type == CLASSIC

        arguments.merge(ARGUMENT => type.to_s)
      end

      def self.known!(name, value)
        return nil if value.nil? || value.to_s.empty?

        type = value.to_s.to_sym
        return type if KNOWN.include?(type)

        raise QueueTypeError,
              "queue #{name.inspect} asks for an unknown queue type #{value.inspect}; " \
              "it must be one of #{KNOWN.map(&:inspect).join(", ")}"
      end
      private_class_method :known!

      # The kind when it was said twice, or nil when it was not said at all.
      def self.agreed(name, requested, declared)
        return requested || declared if requested.nil? || declared.nil? || requested == declared

        raise QueueTypeError,
              "queue #{name.inspect} is declared #{requested.inspect} and also carries " \
              "#{ARGUMENT}=#{declared} in its arguments; pick one"
      end
      private_class_method :agreed

      def self.refuse_impossible(name, type, durable:, exclusive:, auto_delete:)
        return if type == CLASSIC
        return unless classic_only?(durable: durable, exclusive: exclusive,
                                    auto_delete: auto_delete)

        why = []
        why << "transient" unless durable
        why << "exclusive" if exclusive
        why << "auto-delete" if auto_delete
        raise QueueTypeError,
              "queue #{name.inspect} cannot be a #{type} queue while it is " \
              "#{why.join(" and ")}: RabbitMQ only replicates a queue that outlives the " \
              "connection that declared it. Leave it classic, or drop the flag."
      end
      private_class_method :refuse_impossible
    end
  end
end
