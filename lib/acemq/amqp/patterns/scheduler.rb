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

require_relative "../codec"
require_relative "../queue_type"

module AceMQ
  module AMQP
    module Patterns
      # Delivering a message later.
      #
      #   Patterns::Scheduler.on(mq) do |scheduler|
      #     scheduler.in(4 * 3600, invoice, to: "invoice.due", exchange: "billing")
      #     scheduler.at(renewal_date, policy, to: "policy.renew", exchange: "policies")
      #   end
      #
      # == Why not a per-message time to live
      #
      # The obvious implementation is to set +expiration+ on the message, drop
      # it in a queue nobody consumes, and let it dead-letter to its
      # destination. It is what most articles suggest and it is wrong for
      # anything but a single fixed delay, because **a classic queue expires
      # messages only at its head**.
      #
      # Put a four-hour message in, then a one-minute message behind it, and
      # the one-minute message is delivered in four hours. Nothing reports
      # this: the queue looks healthy, the message is not lost, it is simply
      # late by a factor nobody predicted. It is the single most common way a
      # home-made scheduler fails, and it fails in production under mixed load
      # rather than in testing under uniform load.
      #
      # == What this does instead
      #
      # A small ladder of queues, each with a _uniform_ time to live, and a
      # message hops through them until it is due:
      #
      #   acemq.schedule.1s  acemq.schedule.10s  acemq.schedule.1m
      #   acemq.schedule.10m  acemq.schedule.1h
      #
      # Every message in a given rung has the same delay, so head-of-line
      # expiry is not a problem — the head is always the message due soonest.
      # Each expiry returns the message to this scheduler, which either
      # delivers it or puts it in the largest rung that does not overshoot. A
      # four-hour delay is four one-hour hops; a ninety-second delay is one
      # minute, then three tens.
      #
      # The cost is honest and worth stating: a long delay is several broker
      # round trips rather than one, and delivery is accurate to about the
      # smallest rung rather than to the second. A scheduler that must fire at
      # 09:00:00.000 exactly is a scheduler, not a message broker.
      #
      # The alternative is RabbitMQ's delayed-message-exchange plugin, which
      # does this properly and is a plugin — so it is not available everywhere,
      # and a library that silently required it would be a library that works
      # on your laptop.
      #
      # == The names are the contract
      #
      # Every name, argument and header here is shared with the Java, Go, .NET
      # and Python libraries. A Ruby service and a Java service scheduling on
      # one broker declare the same five rungs and the same control queue, so a
      # rung declared with a different time to live is a PRECONDITION_FAILED
      # for whichever of them declares second, and a header spelled differently
      # is a message that reaches the control queue and cannot be routed.
      # Nothing in this class is a local decision.
      class Scheduler
        # Where a message waits, and where it comes back to be re-examined.
        EXCHANGE = "acemq.schedule"

        # The queue every expired message returns to.
        CONTROL = "acemq.schedule.due"

        # Deliberately not the +x-acemq-+ prefix. That one is reserved:
        # {Headers.reserved?} matches it, {Envelope} refuses it in an
        # application's own header hash outright, and what does arrive under it
        # is materialised as an envelope field rather than handed back as a
        # header. A scheduler header using it would be refused on publish, and
        # the version of this that got past that would be gone on consume.
        TARGET_EXCHANGE = "x-schedule-exchange"

        # The routing key the message should eventually carry.
        TARGET_ROUTING_KEY = "x-schedule-routing-key"

        # When it is due, as epoch milliseconds — the same integer
        # +x-acemq-first-seen+ is written as, and what Java's
        # +Instant#toEpochMilli+ produces. A formatted timestamp would be
        # friendlier to read in the management console and would put a parser,
        # a time zone and a resolution into the contract; an integer has none
        # of those to disagree about.
        DUE_AT = "x-schedule-due-at"

        # What the payload was encoded as when it was scheduled.
        #
        # Carried because the scheduler republishes bytes rather than objects,
        # and a consumer picks its codec from the content type. Publishing
        # pre-encoded bytes under +application/octet-stream+ produces a message
        # the intended consumer cannot decode — it arrives, it is the right
        # bytes, and nothing can read it.
        CONTENT_TYPE = "x-schedule-content-type"

        # The rungs, longest first, in seconds.
        #
        # Five of them, spanning a second to an hour. More rungs mean finer
        # accuracy and more queues; fewer mean more hops for a long delay. This
        # spread delivers a one-day message in twenty-four hops and a
        # one-minute message in one, which is the right way round — short
        # delays are common and want to be cheap.
        RUNGS = [3600, 600, 60, 10, 1].freeze

        # What a scheduled message's type is, in the envelope.
        MESSAGE_TYPE = "ScheduledMessage"

        # How many expiries the control consumer holds at once.
        PREFETCH = 50

        # Writes already-encoded bytes out unchanged, under the content type
        # they were encoded as.
        #
        # Publishing them through an ordinary codec would encode them a second
        # time, and what arrives is JSON containing JSON. Publishing them with
        # {BytesCodec} loses the content type, and what arrives cannot be
        # decoded by the consumer that was waiting for it.
        class Verbatim
          def initialize(content_type)
            @content_type = content_type.to_s
            freeze
          end

          attr_reader :content_type

          def encode(payload)
            return payload if payload.is_a?(String)

            raise EncodeError, "the scheduler moves bytes, not a #{payload.class}"
          end

          def decode(_body)
            raise DecodeError, "the scheduler only publishes"
          end

          def can_decode?(_content_type) = false
        end

        # Opens a scheduler on a connection, with its queues declared.
        #
        # With a block the scheduler is yielded and closed afterwards, which is
        # what Java's try-with-resources says; without one it runs until
        # {#close}.
        #
        # @param connection [Connection]
        # @return [Scheduler]
        def self.on(connection)
          scheduler = new(connection)
          return scheduler unless block_given?

          begin
            yield scheduler
          ensure
            scheduler.close
          end
        end

        # Declares the exchange, the five rungs and the control queue, and
        # binds them.
        #
        # Called by {on}, and public because a deployment that applies its
        # topology up front should be able to include the scheduler's without
        # starting a consumer. Declaring is idempotent, so doing both is free.
        #
        # @param connection [Connection, Transport] anything answering
        #   declare_exchange, declare_queue and bind
        # @return [Array<String>] the queues, longest rung first, control last
        def self.declare(connection)
          connection.declare_exchange(EXCHANGE, kind: :direct, durable: true)
          RUNGS.each do |rung|
            declare_queue(connection, rung_name(rung), arguments_for(rung))
          end
          declare_queue(connection, CONTROL, {})
          RUNGS.map { |rung| rung_name(rung) } + [CONTROL]
        end

        # The queue a delay of this many seconds waits in.
        #
        # A duration divisible by an hour renders as +{n}h+, one divisible by a
        # minute as +{n}m+, and anything else as +{n}s+ — Java's +describe+,
        # kept to the letter, because the names it produces are the queues on
        # the broker.
        #
        # @param seconds [Numeric]
        # @return [String]
        def self.rung_name(seconds) = "#{EXCHANGE}.#{describe(seconds)}"

        # The arguments a rung queue has to carry, and the whole of them.
        #
        # These three, classic, durable, and no others. A fourth argument here
        # is a queue the Java library cannot declare.
        #
        # @param seconds [Numeric]
        # @return [Hash]
        def self.arguments_for(seconds)
          {
            "x-message-ttl" => (seconds * 1000).round,
            "x-dead-letter-exchange" => EXCHANGE,
            "x-dead-letter-routing-key" => CONTROL
          }
        end

        # @api private
        def self.describe(seconds)
          millis = (seconds * 1000).round
          return "#{millis / 3_600_000}h" if (millis % 3_600_000).zero?
          return "#{millis / 60_000}m" if (millis % 60_000).zero?

          "#{millis / 1000}s"
        end

        # @api private
        def self.declare_queue(connection, name, arguments)
          connection.declare_queue(name, queue_type: QueueType::CLASSIC, durable: true,
                                         arguments: arguments)
          connection.bind(queue: name, exchange: EXCHANGE, routing_key: name)
        end

        private_class_method :declare_queue

        # @param connection [Connection]
        def initialize(connection)
          @connection = connection
          @bytes = BytesCodec.new
          @lock = Mutex.new
          @counters = { scheduled: 0, delivered: 0, hops: 0, malformed: 0 }
          self.class.declare(connection)
          @subscription = subscribe(connection)
        end

        # Delivers a message after a delay.
        #
        # +in+ is a Ruby keyword, so this method is only ever reachable through
        # its receiver — +scheduler.in(...)+ — which is how it reads anyway.
        #
        # @param delay [Numeric] seconds; zero or negative delivers immediately
        # @param payload [Object] anything the connection's codec will encode
        # @param to [String] the routing key it should eventually carry
        # @param exchange [String] where it should eventually go
        # @return [void]
        def in(delay, payload, to:, exchange: "")
          at(Time.now + delay, payload, to: to, exchange: exchange)
        end

        # Delivers a message at a moment.
        #
        # The payload is encoded once, here, and carried as bytes from then on.
        # Its content type goes with it, because that is how the eventual
        # consumer chooses a codec.
        #
        # @param moment [Time] anything in the past delivers immediately
        # @param payload [Object] anything the connection's codec will encode
        # @param to [String] the routing key it should eventually carry
        # @param exchange [String] where it should eventually go
        # @return [void]
        def at(moment, payload, to:, exchange: "")
          codec = @connection.codec
          headers = {
            TARGET_EXCHANGE => exchange.to_s,
            TARGET_ROUTING_KEY => to.to_s,
            DUE_AT => self.class.millis(moment),
            CONTENT_TYPE => codec.content_type
          }
          count(:scheduled)
          route(codec.encode(payload), headers)
          nil
        end

        # Messages handed to this scheduler.
        def scheduled = read(:scheduled)

        # Messages that reached their destination.
        def delivered = read(:delivered)

        # How many times a message moved between rungs.
        #
        # Divided by {#delivered} this is the average number of hops, which is
        # the number to look at if the scheduler is busier than expected: long
        # delays cost hops.
        def hops = read(:hops)

        # Messages that reached the control queue without the headers a
        # scheduled message carries, and were therefore dropped.
        #
        # Not zero means something other than a scheduler is publishing into
        # +acemq.schedule+, which it must not: those queues are an
        # implementation detail of this class.
        def malformed = read(:malformed)

        # Whether the control consumer is still being delivered to.
        def running? = !!@subscription&.open?

        # Stops the control consumer. The queues stay, and so do the messages
        # waiting in them.
        def close
          @subscription&.cancel
          @subscription = nil
          nil
        end

        def to_s
          "Scheduler{rungs=#{RUNGS.map { |r| self.class.describe(r) }.join(", ")}, " \
            "scheduled=#{scheduled}, delivered=#{delivered}}"
        end

        # A moment as epoch milliseconds, which is what {DUE_AT} carries and
        # what Java's +Instant#toEpochMilli+ writes.
        #
        # @api private
        def self.millis(moment) = (moment.to_f * 1000).to_i

        # A header read back as epoch milliseconds, or nil when it is not a
        # number.
        #
        # Lenient, like {Envelope.from_headers}: a producer that wrote the
        # integer as a string is still a producer whose message is due, and
        # only a header that means nothing at all makes this nil.
        #
        # @api private
        def self.epoch(value)
          return nil if value.nil? || value == true || value == false

          Integer(value.to_s, 10)
        rescue ArgumentError, TypeError
          nil
        end

        private

        # Subscribed on the transport rather than through {Connection#consume},
        # which is the one deliberate departure from every other pattern here.
        #
        # A consumer declares its dead-letter queues at start-up — see
        # {RetryLadder#declare_dead_letters} — so consuming the control queue
        # the ordinary way would create +acemq.schedule.due.dlq+ and
        # +acemq.schedule.due.parked+ on every broker any service ever ran a
        # scheduler against. Two queues per deployment that nothing writes to
        # and nobody reads, for a queue whose messages this class put there
        # itself.
        #
        # The rest of the consumer is not wanted either. There is no codec to
        # choose: the payload is bytes this class must not look inside, because
        # a scheduler that decodes acquires opinions about message formats it
        # has no business having. There is no envelope to build, no retry
        # policy that means anything — the delay is the message's whole
        # purpose, and retrying a failed hop immediately is exactly right — and
        # no handler an application could supply.
        def subscribe(connection)
          connection.transport.subscribe(CONTROL, prefetch: PREFETCH) do |delivery|
            receive(delivery)
          end
        end

        # Called for every message that has come out of a rung.
        def receive(delivery)
          headers = schedule_headers(delivery.headers)
          return drop(delivery) if headers.nil?

          route(delivery.body, headers)
          delivery.ack
        rescue StandardError
          # The broker would not take the next hop. Requeued rather than
          # dropped: the message is somebody's invoice due in four hours, and
          # the alternative to another attempt is that it is never delivered
          # and nothing anywhere says so.
          delivery.nack(requeue: true)
        end

        # The four headers a scheduled message carries, and nothing else, or
        # nil when the message is not one.
        #
        # Nothing else deliberately. What comes out of a rung also carries the
        # reserved +x-acemq-+ names the last publish wrote, and {Envelope}
        # refuses to be handed those as an application's own headers — quite
        # rightly, since the next hop is a new message and gets its own.
        def schedule_headers(raw)
          raw = (raw || {}).to_h { |name, value| [name.to_s, value] }
          due_at = self.class.epoch(raw[DUE_AT])
          exchange = raw[TARGET_EXCHANGE]
          routing_key = raw[TARGET_ROUTING_KEY]
          return nil if due_at.nil? || exchange.nil? || routing_key.nil?

          { TARGET_EXCHANGE => exchange.to_s, TARGET_ROUTING_KEY => routing_key.to_s,
            DUE_AT => due_at, CONTENT_TYPE => raw[CONTENT_TYPE].to_s }
        end

        # Delivers if it is due, and otherwise puts it in the largest rung that
        # does not overshoot.
        def route(body, headers)
          remaining = (headers[DUE_AT] / 1000.0) - Time.now.to_f
          # Due, or so nearly due that another hop would cost more than the
          # accuracy it buys.
          return deliver(body, headers) if remaining < RUNGS.last

          rung = RUNGS.find { |candidate| candidate <= remaining } || RUNGS.last
          count(:hops)
          @connection.publish(body, to: self.class.rung_name(rung), exchange: EXCHANGE,
                                    codec: @bytes, type: MESSAGE_TYPE, headers: headers)
        end

        # The scheduler's own headers are not passed on: they are bookkeeping,
        # and a consumer that started depending on them would be depending on
        # how a message got to it.
        def deliver(body, headers)
          content_type = headers[CONTENT_TYPE].to_s
          content_type = JSONCodec::CONTENT_TYPE if content_type.empty?
          count(:delivered)
          @connection.publish(body, to: headers[TARGET_ROUTING_KEY].to_s,
                                    exchange: headers[TARGET_EXCHANGE].to_s,
                                    codec: Verbatim.new(content_type), type: MESSAGE_TYPE)
        end

        # A message in the control queue that no scheduler put there.
        #
        # Dropped rather than requeued, and counted. Requeueing it would loop
        # for ever, since nothing about it will be different next time, and
        # this queue has no dead-letter target to send it to on purpose: it is
        # an implementation detail, and giving it one would be two more queues
        # in every deployment.
        def drop(delivery)
          count(:malformed)
          delivery.nack(requeue: false)
        end

        def count(name) = @lock.synchronize { @counters[name] += 1 }
        def read(name) = @lock.synchronize { @counters[name] }
      end
    end
  end
end
