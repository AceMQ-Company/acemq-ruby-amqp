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
    module Patterns
      # What a saga did.
      #
      # Returned rather than raised, because a failed saga is not an
      # exceptional condition to a caller that has to decide what happens next
      # — and because the interesting part is not the exception but
      # {#unresolved}, the list of things that could not be undone.
      #
      # Frozen, like {Envelope}, for the same reason: it describes something
      # that has already happened, and a report that could be edited after the
      # fact is not a report.
      class SagaResult
        # What the saga is called.
        attr_reader :saga

        # The step whose action raised, or nil when every step ran.
        attr_reader :failed_at

        # What it raised, or nil.
        attr_reader :failure

        # The steps that ran, in order, before the failure.
        attr_reader :completed

        # The steps whose compensation raised.
        attr_reader :unresolved

        # @api private
        def self.completed_run(saga, completed)
          new(saga: saga, completed: completed)
        end

        # @api private
        def self.compensated_run(saga, failed_at, failure, completed, unresolved)
          new(saga: saga, failed_at: failed_at, failure: failure,
              completed: completed, unresolved: unresolved)
        end

        def initialize(saga:, completed:, failed_at: nil, failure: nil, unresolved: [])
          @saga = saga
          @failed_at = failed_at
          @failure = failure
          @completed = completed.dup.freeze
          @unresolved = unresolved.dup.freeze
          freeze
        end

        # Whether every step ran.
        def complete? = @failed_at.nil?

        # Whether a step failed and the earlier ones were undone.
        def compensated? = !@failed_at.nil?

        # Whether anything was left in a state nobody intended.
        #
        # **This is the flag to alert on.** Everything else a saga reports is
        # recoverable by construction; these are real-world effects that
        # happened, were meant to be undone, and were not. Nothing else in the
        # system knows about them, no retry will resolve them, and a person has
        # to. Java spells it +hasUnresolved()+; the +has_+ is dropped here
        # because Ruby says the same thing with the question mark.
        def unresolved? = !@unresolved.empty?

        def to_s
          return "SagaResult{#{@saga} completed: #{@completed.join(" -> ")}}" if complete?

          trailer = @unresolved.empty? ? "" : ", UNRESOLVED #{@unresolved.join(", ")}"
          "SagaResult{#{@saga} failed at #{@failed_at}, compensated " \
            "#{@completed.join(", ")}#{trailer}}"
        end
      end

      # A sequence of steps where each one knows how to undo itself.
      #
      #   booking = Patterns::Saga.named("place-order") do |saga|
      #     saga.step("take-payment") { |order| payments.charge(order) }
      #         .compensate_with { |order| payments.refund(order) }
      #     saga.step("reserve-stock") { |order| inventory.reserve(order) }
      #         .compensate_with { |order| inventory.release(order) }
      #     saga.step("book-courier") { |order| couriers.book(order) }
      #   end
      #
      #   result = booking.run(order)
      #
      # If +book-courier+ raises, the stock is released and the payment
      # refunded, in that order, and {SagaResult#compensated?} says so.
      #
      # == What this is not
      #
      # **Not a distributed transaction.** Nothing is isolated: after
      # +take-payment+ the customer's money really has moved, and anybody
      # looking sees that it has. If +book-courier+ then fails, the refund is a
      # _new_ fact rather than an erasure of the old one, and for a few seconds
      # the world contained a charge that should not have happened. That is not
      # a defect in this class; it is what compensating a real-world action
      # means, and a saga is honest about it where a two-phase commit pretends
      # otherwise.
      #
      # So the steps must be things that can be undone by doing something else.
      # Sending an email cannot be compensated — the apology is a second email,
      # not an unsend — and a saga step that sends one should be the last step,
      # after everything that can still fail.
      #
      # **Not durable.** This runs in one process and its state is on the
      # stack. A crash midway leaves the saga half-applied with nothing to
      # resume it, which is the honest limitation of the in-process form and is
      # why the compensations run on the way out of +run+ rather than after a
      # restart. Where a saga must survive the process, the steps have to be
      # messages and the state has to be in a database — a much larger thing,
      # and it is not this.
      #
      # For most systems the in-process form is the right one: it turns
      # "remember to undo the three things you already did" from a comment into
      # something the code can see.
      #
      # == When compensation itself fails
      #
      # It is tried, it is collected, and the remaining compensations still
      # run. The alternative — stopping — leaves more undone than continuing
      # does. What comes back is a {SagaResult} listing what could not be
      # undone, and that list is the thing to alert on: it is the set of facts a
      # human now has to reconcile by hand.
      #
      # == Nothing here touches a broker
      #
      # No message is published, no header is set, and a saga runs perfectly
      # well in a process that has never opened a connection. It is here rather
      # than anywhere else because the work a saga sequences is usually the work
      # a message asked for, and because the other AceMQ libraries put it here.
      class Saga
        # What this saga is called, in results.
        attr_reader :name

        # Starts building one.
        #
        # With a block, the builder is yielded and the saga is built and
        # returned, which is the ordinary way to write one. Without a block the
        # builder itself comes back and {Builder#build} finishes it, for a saga
        # assembled across several methods.
        #
        # @param name [String] what this saga is called, in results
        # @return [Saga, Builder]
        def self.named(name)
          raise ArgumentError, "a saga needs a name" if name.to_s.empty?

          builder = Builder.new(name.to_s)
          return builder unless block_given?

          yield builder
          builder.build
        end

        # @api private
        def initialize(name:, steps:)
          @name = name
          @steps = steps.freeze
          freeze
        end

        # Runs the steps, compensating in reverse if one fails.
        #
        # Never raises for a step failure, because a caller needs the
        # compensation report more than it needs a backtrace. Anything the
        # steps raise that is not a +StandardError+ — an +Interrupt+, a
        # +SignalException+ — is left alone and nothing is compensated: the
        # process is going away, and a compensation running on the way out of a
        # SIGTERM would be a compensation nobody can be sure finished.
        #
        # @param subject [Object] what to operate on
        # @return [SagaResult]
        def run(subject)
          completed = []

          @steps.each do |step|
            step.action.call(subject)
            completed << step
          rescue StandardError => e
            unresolved = compensate(subject, completed)
            return SagaResult.compensated_run(@name, step.name, e,
                                              completed.map(&:name), unresolved)
          end

          SagaResult.completed_run(@name, completed.map(&:name))
        end

        # The step names, in order.
        def step_names = @steps.map(&:name)

        def to_s = "Saga{#{@name}: #{step_names.join(" -> ")}}"

        # One step and the thing that undoes it.
        #
        # +compensation+ is nil when the step needs no undoing, which is
        # legitimate and is checked before use.
        Step = Struct.new(:name, :action, :compensation, keyword_init: true)

        # Collects the steps of a {Saga}.
        #
        # Building mutates, in the one place this library allows it and for the
        # same reason {Topology} and {RoutingSlip} do: a saga is assembled and
        # then run, and threading a new copy through four chained calls would
        # buy an immutability nobody is using. The {Saga} that comes out of
        # {#build} is frozen, which is the copy that matters.
        class Builder
          def initialize(name)
            @name = name
            @steps = []
          end

          # Adds a step, whose block is the work.
          #
          # A step added this way has no compensation, which is legitimate for
          # a step that changed nothing and a mistake for one that did. There
          # is no warning for the second case, because a library cannot tell
          # them apart — which is the argument for writing the compensation
          # first and the action second.
          #
          # @param step_name [String] what the step is called
          # @yieldparam subject [Object] what the saga is operating on
          # @return [Builder] self, so {#compensate_with} can follow
          def step(step_name, &action)
            raise ArgumentError, "a saga step needs a name" if step_name.to_s.empty?
            unless action
              raise ArgumentError, "saga step #{step_name} needs a block to do the work"
            end

            step_name = step_name.to_s
            if @steps.any? { |existing| existing.name == step_name }
              raise ArgumentError,
                    "saga #{@name} already has a step called #{step_name.inspect}. Names " \
                    "identify a step in the compensation report, so two of them would make " \
                    "that report ambiguous."
            end

            @steps << Step.new(name: step_name, action: action, compensation: nil)
            self
          end

          # Gives the most recently added step something that undoes it.
          #
          # @yieldparam subject [Object] what the saga is operating on
          # @return [Builder] self
          def compensate_with(&compensation)
            unless compensation
              raise ArgumentError, "compensate_with needs a block that undoes the last step"
            end
            if @steps.empty?
              raise ArgumentError, "there is no step to compensate yet: call step(...) first"
            end

            @steps[-1] = Step.new(name: @steps.last.name, action: @steps.last.action,
                                  compensation: compensation)
            self
          end

          # @return [Saga]
          # @raise [ArgumentError] when it has no steps
          def build
            raise ArgumentError, "saga #{@name} has no steps" if @steps.empty?

            Saga.new(name: @name, steps: @steps.map(&:freeze))
          end
        end

        private

        # Undoes what was done, most recent first.
        #
        # Reverse order because that is the order the world was changed in, and
        # a compensation often depends on the state a later step has not yet
        # altered.
        #
        # The completed steps are carried as steps rather than looked up again
        # by name, which is the one place this departs from Java: a lookup can
        # fail, and a lookup that cannot fail is better than one that raises an
        # exception no caller could act on.
        #
        # @return [Array<String>] the steps whose compensation failed, which is
        #   what a human has to reconcile
        def compensate(subject, completed)
          unresolved = []

          completed.reverse_each do |step|
            # Nothing to undo, which is legitimate: a step that only read
            # something, or one whose effect is harmless, needs no
            # compensation. Skipped rather than refused.
            next if step.compensation.nil?

            step.compensation.call(subject)
          rescue StandardError
            # Collected and carried on. Stopping here would leave more undone
            # than continuing, and the caller is told exactly which ones did
            # not come back.
            unresolved << step.name
          end

          unresolved
        end
      end
    end
  end
end
