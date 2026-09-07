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
    # When to try again, and when to stop.
    #
    # The arithmetic here is part of the cross-language contract rather than a
    # local choice: the same policy must produce the same delays in Java, Go,
    # .NET, Python and Ruby, because the same message can be retried by a
    # consumer written in any of them. Jitter is the exception — it is random
    # by definition — so {#schedule} exposes the delays *without* it, which is
    # what to read when deciding whether a policy is the one you meant.
    #
    # Durations are seconds, as floats, because that is what Ruby's own
    # +sleep+ and +Time+ arithmetic speak.
    class RetryPolicy
      attr_reader :max_attempts, :initial_delay, :multiplier, :max_delay,
                  :jitter_factor, :max_message_age

      # @param max_attempts [Integer] total deliveries including the first
      # @param initial_delay [Float] seconds before the second attempt
      # @param multiplier [Float] what the delay is multiplied by each time
      # @param max_delay [Float] the ceiling in seconds, or 0 for none
      # @param jitter_factor [Float] how far a delay may move either side, 0 to 1
      # @param max_message_age [Float] give up on anything older, or 0 for never
      def initialize(max_attempts: 1, initial_delay: 0.0, multiplier: 2.0,
                     max_delay: 0.0, jitter_factor: 0.0, max_message_age: 0.0)
        @max_attempts = max_attempts
        @initial_delay = initial_delay.to_f
        @multiplier = multiplier.to_f
        @max_delay = max_delay.to_f
        @jitter_factor = jitter_factor.to_f
        @max_message_age = max_message_age.to_f
        freeze
      end

      # One delivery, no second chance.
      def self.none
        new(max_attempts: 1)
      end

      # Doubling delays with 20% jitter, which is the sane default.
      #
      # @param max_attempts [Integer] total deliveries including the first
      # @param initial_delay [Float] seconds before the second attempt
      # @param max_delay [Float] the ceiling, or 0 for none
      def self.exponential(max_attempts, initial_delay, max_delay = 0.0)
        new(max_attempts: max_attempts, initial_delay: initial_delay,
            multiplier: 2.0, max_delay: max_delay, jitter_factor: 0.2)
      end

      # The same wait every time, with no jitter.
      def self.fixed(max_attempts, delay)
        new(max_attempts: max_attempts, initial_delay: delay, multiplier: 1.0)
      end

      # A copy that abandons a message older than +age+ seconds.
      #
      # The honest limit when a queue has been paused: attempts say nothing
      # about how long a message has been waiting.
      def give_up_after(age)
        copy(max_message_age: age)
      end

      # A copy using a different jitter factor, between 0 and 1.
      def with_jitter(factor)
        copy(jitter_factor: factor)
      end

      # How long to wait before the next attempt, or nil to give up.
      #
      # +jitter: false+ gives the delay the schedule says, which is what to ask
      # for when the number has to line up with something: a retry that waits in
      # the broker waits in a queue named after its delay, and a jittered number
      # names no queue. The retry engine asks for it that way, decides where the
      # wait happens, and only jitters the waits it performs itself.
      #
      # @param attempt [Integer] the attempt that has just failed, from 1
      # @param message_age [Float] how old the message is, in seconds
      # @param jitter [Boolean] whether to spread the delay
      # @return [Float, nil] the delay in seconds, or nil for no further attempt
      def next_delay(attempt, message_age = 0.0, jitter: true)
        return nil if attempt >= max_attempts
        return nil if max_message_age.positive? && message_age >= max_message_age

        delay = backoff(attempt)
        [jitter ? jittered(delay) : delay, 0.0].max
      end

      # The delays this policy would use, without jitter.
      #
      # What to look at when deciding whether a policy is the one you meant:
      # four numbers are easier to argue with than three parameters.
      #
      # @return [Array<Float>]
      def schedule
        delays = []
        delay = initial_delay
        (1...max_attempts).each do
          capped = max_delay.positive? && delay > max_delay ? max_delay : delay
          delays << capped
          delay *= multiplier
        end
        delays
      end

      # The same delay, moved either side by the jitter factor.
      #
      # Both directions, so a fleet of consumers that failed together does not
      # come back together. One-sided jitter only ever delays, which turns a
      # thundering herd into a slower thundering herd.
      #
      # Public because the retry engine applies it separately from working the
      # delay out: a wait that happens in the broker needs no jitter at all,
      # since each message's time-to-live starts when it enters the rung, so a
      # fleet that failed over ten seconds is released over ten seconds.
      #
      # @param delay [Float] seconds
      # @return [Float]
      def jittered(delay)
        return delay unless jitter_factor.positive? && delay.positive?

        delay * (1 + (((Kernel.rand * 2) - 1) * jitter_factor))
      end

      private

      # The delay this attempt has backed off to, capped.
      #
      # Capped inside the loop as well as after it: without the first, a policy
      # with a large multiplier and many attempts overflows towards infinity
      # before the ceiling is ever applied.
      def backoff(attempt)
        delay = initial_delay
        (1...attempt).each do
          delay *= multiplier
          break delay = max_delay if max_delay.positive? && delay > max_delay
        end
        max_delay.positive? && delay > max_delay ? max_delay : delay
      end

      def copy(**changes)
        RetryPolicy.new(
          max_attempts: changes.fetch(:max_attempts, max_attempts),
          initial_delay: changes.fetch(:initial_delay, initial_delay),
          multiplier: changes.fetch(:multiplier, multiplier),
          max_delay: changes.fetch(:max_delay, max_delay),
          jitter_factor: changes.fetch(:jitter_factor, jitter_factor),
          max_message_age: changes.fetch(:max_message_age, max_message_age)
        )
      end
    end
  end
end
