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

require "securerandom"

require_relative "ack"
require_relative "credentials"
require_relative "queue_type"
require_relative "security"

module AceMQ
  module AMQP
    # The broker, or the network, rather than the message.
    #
    # Worth telling apart from a bad message because it says nothing about the
    # message: the same one may well go through once the broker is back.
    class TransportError < StandardError; end

    # A message the broker would not take, or took and could not route.
    #
    # The two are told apart by {#unroutable?} rather than by two classes,
    # because a caller who only wants to know that the message did not arrive
    # should not have to rescue both. Go and Python split the same failure the
    # same way — a flag on the one error — and the word the telemetry writes is
    # read off it, so the counter and the span cannot disagree about which of
    # the two happened.
    class PublishError < TransportError
      # Whether the broker accepted the message and had nowhere to put it.
      #
      # False for everything else: a refused publish, a connection that dropped
      # mid-flight, a confirm that never came. Only a mandatory publish can ever
      # set it, because only a mandatory publish is told.
      def unroutable? = @unroutable

      # @param message [String, nil]
      # @param unroutable [Boolean]
      def initialize(message = nil, unroutable: false)
        super(message)
        @unroutable = unroutable
      end
    end

    # A publish the library declined because the broker has blocked the
    # connection. Nothing was written, so nothing can be half-published, and
    # sending the same message again once the block clears is the right answer.
    #
    # Its own class because the alternative is the one thing a caller cannot
    # act on: a plain {PublishError} may be a message the broker has and never
    # confirmed, and a load that counts the two together counts back pressure
    # as loss. Go's +isPaused+, .NET's +ConnectionBlockedException+ and Java's
    # +PublishingPausedException+ make the same split.
    #
    # A subclass, so every +rescue PublishError+ written before it existed
    # still catches it. It is never unroutable: the message did not get far
    # enough to be routed.
    class PublishingPausedError < PublishError; end

    # The sentences a publish that did not arrive comes back with.
    #
    # In one place because a batch has to raise exactly what a single publish
    # raises: whoever greps a log for one of these should not have to know
    # which of the two paths sent the message, and two copies of a sentence are
    # two sentences waiting to drift apart.
    #
    # @api private
    module PublishFailure
      module_function

      def not_confirmed(message_id, exchange, routing_key)
        "the broker would not confirm message #{message_id} on exchange " \
          "#{exchange.inspect} with key #{routing_key.inspect}"
      end

      def not_routed(message_id, exchange, routing_key, reason)
        "the broker had nowhere to route message #{message_id} published to exchange " \
          "#{exchange.inspect} with key #{routing_key.inspect}: #{reason}"
      end

      def not_published(message_id, exchange, routing_key, reason)
        "cannot publish message #{message_id} to exchange #{exchange.inspect} " \
          "with key #{routing_key.inspect}: #{reason}"
      end

      def paused(message_id, exchange, routing_key, reason)
        not_published(message_id, exchange, routing_key,
                      "the broker has blocked this connection (#{reason}), so the message " \
                      "was not sent. This is broker capacity, not this message: send it " \
                      "again once the block clears.")
      end

      # bunny's refusals that are raised before a frame reaches the socket:
      # +ConnectionClosedError+ from +send_frame+/+send_frameset+, which check
      # +open?+ and raise instead of writing, and +ChannelAlreadyClosed+ from
      # the first line of +basic_publish+. The same in bunny 2.24 and 3.x. By
      # name, because bunny is loaded lazily and this file must load without it.
      # Only ever asked of +basic_publish+: the same classes out of
      # +wait_for_confirms+ come after the write.
      UNSENT = %w[Bunny::ConnectionClosedError Bunny::ChannelAlreadyClosed].freeze

      def unsent?(error) = UNSENT.include?(error.class.name)

      def unsent(message_id, exchange, routing_key, error)
        not_published(message_id, exchange, routing_key,
                      "bunny would not write it because the connection or channel is " \
                      "closed (#{error.class.name}), so the message was not sent. Send it " \
                      "again once the connection is recovered.")
      end
    end

    # How many messages may be on the wire with no confirm back yet.
    #
    # The ceiling is the only backpressure a publisher that does not wait per
    # message has. Without it a caller who publishes faster than the broker
    # confirms accumulates unconfirmed messages until the process dies, which
    # looks like throughput right up to the moment it does not — and the bigger
    # the array handed to {Transport#publish_all}, the closer that moment.
    #
    # A permit is taken *before* the message is written and given back when the
    # broker has answered for it, whichever way it answered: an ack, a nack and
    # a +basic.return+ are all answers. A message nothing ever answered for
    # keeps its permit, because as far as the broker is concerned that message
    # is still outstanding — which is what makes the ceiling bite when a broker
    # stops keeping up rather than only when somebody hands in a huge array.
    # Java holds the permit in exactly the same place, in a +Semaphore+ released
    # from its confirm listener, and .NET in a +SemaphoreSlim+.
    #
    # Nothing waits here. Java can afford to block for its confirm timeout
    # because its publishes are asynchronous and another thread's confirm can
    # release a permit while this one waits; Ruby's transport publishes under
    # the publishing channel's mutex, so the thread that would wait is the only
    # thread that could ever release one, and a wait would be a deadlock dressed
    # up as patience. Exhaustion is therefore reported straight away, in the
    # sentence Java uses for it, rather than stalling silently.
    #
    # @api private
    class PublishPermits
      # What Java's +ConnectionConfig.maxOutstandingPublishes+ and .NET's
      # +MaxOutstandingPublishes+ default to. Large enough that an ordinary
      # batch never notices it, small enough that a runaway publisher is stopped
      # while the process still has memory to report it with.
      DEFAULT = 1_000

      # @return [Integer] the ceiling this was built with
      attr_reader :limit

      # @param limit [Integer] how many publishes may await a confirm at once
      # @raise [ConfigurationError] when the ceiling is not a positive number
      def initialize(limit = DEFAULT)
        @limit = Integer(limit)
        if @limit < 1
          raise ConfigurationError,
                "max_outstanding_publishes must be at least 1, was #{limit.inspect}"
        end

        @available = @limit
        @lock = Mutex.new
      end

      # How many more messages may go out before one has to be answered for.
      def available = @lock.synchronize { @available }

      # Takes one permit, or answers false when there are none left.
      def take
        @lock.synchronize do
          next false if @available.zero?

          @available -= 1
          true
        end
      end

      # Gives back the permits of messages the broker has answered for.
      def release(count)
        return if count.zero?

        @lock.synchronize { @available = [@available + count, @limit].min }
        nil
      end

      # Forgets everything outstanding, for a publishing channel that has been
      # reopened. The messages those permits were held for went down with the
      # old channel and can no longer be confirmed on the new one, which is the
      # same conclusion Java's shutdown listener reaches when it fails every
      # pending publish.
      def reopened
        @lock.synchronize { @available = @limit }
        nil
      end

      # What to say when there is no room left. The second sentence is Java's,
      # word for word, so one runbook covers both.
      def exhausted
        "#{@limit} publishes are already waiting for a confirm and none of them " \
          "completed. The broker is not keeping up; publish more slowly rather " \
          "than buffering more."
      end
    end

    # bunny's +wait_for_confirms+, made to let go when the connection is lost.
    #
    # A message written just as the connection dies is never confirmed, and bunny
    # never tells the thread waiting for it so. Its recovery does try --
    # +release_all_continuations+ wakes the waiting thread -- but the wait loops on
    # an empty continuation queue and goes straight back to sleep, and the same
    # recovery then replaces that queue with a new one, so no confirm can ever
    # reach it. The thread sits out bunny's whole continuation timeout, fifteen
    # seconds by default. Measured on bunny 2.24 and 3.4 alike, with bunny alone
    # and through this library: a forced close at 86.45s, the connection back at
    # 96.46s, the wait ended by +Timeout::Error+ at 101.46s.
    #
    # It costs more than the one message, because the wait holds the publish lock:
    # every other publisher on the connection stalled behind it for those fifteen
    # seconds, five of them after the connection was healthy again.
    #
    # So {Transport} calls {#abandon} when a recovery attempt starts, and the
    # waiting thread is told the connection was lost. The message stays a
    # possible loss -- it was written and nothing answered for it -- so the
    # publish fails as unconfirmed, not as declined.
    #
    # The interruption is +Thread#raise+, which is only safe because it is fenced
    # in: it can land only while the thread is inside +wait_for_confirms+, parked
    # on a condition variable, and one that arrives as the wait is finishing is
    # taken here and dropped rather than escaping later from somewhere else. The
    # earliest bunny offers is +before_recovery_attempt_starts+, which comes one
    # +network_recovery_interval+ (5s) after the loss.
    #
    # @api private
    class ConfirmWait
      # Raised inside the waiting thread. Never escapes {#wait} except as itself,
      # and is a StandardError so a publish answers it the way it answers any
      # other failed wait.
      class Abandoned < StandardError; end

      def initialize
        @lock = Mutex.new
        @waiter = nil
      end

      # bunny's answer to "were they all acked", unless {#abandon} came first.
      #
      # @raise [Abandoned] when the connection was lost during the wait
      def wait(channel)
        Thread.handle_interrupt(Abandoned => :never) do
          @lock.synchronize { @waiter = Thread.current }
          begin
            Thread.handle_interrupt(Abandoned => :immediate) { channel.wait_for_confirms }
          ensure
            @lock.synchronize { @waiter = nil }
            discard_late_abandon
          end
        end
      end

      # Ends the wait in progress, if there is one.
      def abandon
        @lock.synchronize do
          @waiter&.raise(Abandoned,
                         "the connection was lost while waiting for the broker to confirm " \
                         "this message, so whether it arrived is unknown")
        end
        nil
      end

      private

      # An abandon that arrived after the wait had already returned is pending
      # rather than raised. The wait's own answer stands, so it is taken here.
      def discard_late_abandon
        return unless Thread.pending_interrupt?(Abandoned)

        Thread.handle_interrupt(Abandoned => :immediate) { Thread.pass }
      rescue Abandoned
        nil
      end
    end

    # Keeps bunny's topology recovery off channels this library has closed.
    #
    # bunny 3.x records every declaration for replay when a connection comes back, and
    # records it *against the channel it was made on*. This library declares queues,
    # exchanges and bindings on a short-lived channel of their own and closes it —
    # deliberately, because a refused declaration kills the channel it was made on, and
    # sharing one would take every later declaration down with the first PRECONDITION
    # failure. Recovery then tried to replay those declarations on a channel that was
    # closed minutes earlier, and bunny logged one of these per entity, per recovery:
    #
    #   Caught an exception while recovering exchange acemq.dlx:
    #     #<Bunny::ChannelAlreadyClosed: cannot use a closed channel! Channel id: 1>
    #
    # It was not only noise. Every failed replay left a consumer work pool behind, so a
    # client that reconnected often grew threads until it was unhealthy: a standing load
    # reached ~54 threads by its 12th forced recovery, against 9 for the same shape built
    # on bunny alone.
    #
    # So entities whose channel has gone are dropped, and everything on a live channel —
    # the subscription's channel, and the consumers on it — is recovered as bunny
    # intends.
    #
    # **What this gives up.** A queue that only existed because of the connection, an
    # +exclusive+ or +auto_delete+ one declared through {Transport#declare_queue}, is not
    # re-declared on recovery. Durable queues, which is everything this library creates
    # by default, live on the broker and need no replay. Nothing is lost against bunny
    # 2.x either, which has no topology recorder at all and replays nothing.
    #
    # Duck-typed rather than a +Bunny::TopologyRecoveryFilter+ subclass, because bunny is
    # loaded lazily: naming the constant here would require it at load time and drag a
    # broker client into a process that only reads envelopes.
    #
    # @api private
    class RecoverLiveChannelsOnly
      def filter_exchanges(exchanges) = live(exchanges)
      def filter_queues(queues) = live(queues)
      def filter_exchange_bindings(bindings) = live(bindings)
      def filter_queue_bindings(bindings) = live(bindings)

      # Consumers are never filtered, and filtering them was a far worse bug than the
      # one this class fixes. A consumer's channel can read as closed at the moment the
      # filter runs, so asking the same question of it dropped the subscription from
      # recovery: the first version of this took the thread count from 52 to 4 and
      # stopped the client consuming, which is the failure this whole library exists to
      # prevent. Deliveries stopped at 715 while publishing climbed past 17,000.
      #
      # It is also unnecessary. The leak came from replaying *declarations* recorded on
      # the short-lived channels this library closes; a consumer is recorded on the
      # subscription's own channel, which stays open for the life of the subscription.
      def filter_consumers(consumers) = consumers

      private

      # Arrays out of Sets is allowed: bunny's filter contract takes either back.
      def live(entities) = entities.select { |entity| live?(entity) }

      # Anything that cannot be asked is kept. A filter is not the place to decide that
      # an entity bunny recorded is unrecoverable for a reason this does not understand.
      def live?(entity)
        return true unless entity.respond_to?(:channel)

        channel = entity.channel
        return true if channel.nil? || !channel.respond_to?(:open?)

        channel.open?
      end
    end

    # The two rules about what a property looks like on the wire, shared by
    # everything here that writes one.
    #
    # @api private
    module Wire
      module_function

      # Header and argument names go to the broker as strings, whatever they
      # were written as here. A table keyed by :"x-message-ttl" is not the same
      # table as one keyed by "x-message-ttl", and the broker only recognises
      # the second — a symbol would silently become a queue argument nothing
      # acts on.
      def stringify(table)
        return {} if table.nil? || table.empty?

        table.to_h { |name, value| [name.to_s, value] }
      end

      # An empty property is absent rather than blank. A +reply-to+ carrying ""
      # is a reply-to somebody has to write a special case for at the other end,
      # which is the same rule {Envelope#to_headers} follows for its headers.
      def presence(value)
        text = value.to_s
        text.empty? ? nil : text
      end
    end

    # One message, as it arrived, before any codec has looked at it.
    #
    # +redelivered+ is the broker saying it has handed these bytes over before —
    # a nack that requeued, or a consumer that died holding the message. It is
    # not how a retry is counted: a requeue hands the broker back the bytes it
    # was given, so the flag says a delivery happened twice and nothing about
    # which attempt this is. That lives in +x-acemq-attempt+, which the retry
    # engine advances by republishing rather than requeueing.
    #
    # Settling travels with the delivery as +on_ack+ and +on_nack+ rather than
    # as a delivery tag the consumer would have to hand back to the right
    # channel. That keeps the consumer from knowing which channel a message
    # came down, which is the whole reason a fake transport can stand in for a
    # broker in a test.
    #
    # +reply_to+ is AMQP's own property and not a header, which is why it is a
    # field here rather than something read out of +headers+. The Java and .NET
    # libraries write requests with it and nothing else, so a Ruby responder
    # that could not see it could not answer them.
    Delivery = Struct.new(
      :body, :content_type, :routing_key, :message_id, :headers, :redelivered,
      :reply_to, :on_ack, :on_nack, keyword_init: true
    ) do
      def redelivered? = !!redelivered

      # Confirms the message. It will not be sent again.
      def ack = on_ack&.call

      # Returns the message, requeued or not.
      def nack(requeue: false) = on_nack&.call(requeue)
    end

    # The messages the broker handed back, on their way to whoever sent them.
    #
    # A class of its own because it is the one place in this file where two
    # threads meet: bunny dispatches +basic.return+ on its reader thread while
    # the thread that published is still inside {Transport#publish} waiting for
    # a confirm. It is also the only place that has to know bunny's rule for
    # delivering a return, which is a rule about exchange objects rather than
    # about messages.
    #
    # @api private
    class ReturnedMessages
      def initialize
        @returns = Thread::Queue.new
        @watched = []
      end

      # Gets ready to hear about this exchange, and forgets anything left over.
      #
      # bunny hands +basic.return+ to the +Bunny::Exchange+ object registered on
      # the channel under that name and drops it with a warning when there is
      # none, so there has to be one — and it has to be one that does not
      # declare, because this transport publishes to exchanges the application
      # declared and re-declaring one with a type guessed here is how a publish
      # turns into PRECONDITION_FAILED. +no_declare+ is bunny's word for that;
      # the default exchange skips the declaration on its own name anyway.
      #
      # Registered once per exchange. The object is kept by the channel, which
      # is what makes throwing this one away safe.
      #
      # Anything still queued belongs to a publish that raised before it could
      # read its own return, and attributing it to this message would report the
      # wrong one as unroutable.
      def arm(channel, exchange)
        watch(channel, exchange)
        @returns.clear
      end

      # The same, for a batch: every exchange it publishes to is watched before
      # any of it goes out, and whatever was left over is dropped once rather
      # than between two publishes. A return for the message just sent arrives
      # while the next one is still being written, and clearing per message
      # would throw it away.
      def arm_all(channel, exchanges)
        exchanges.each { |exchange| watch(channel, exchange) }
        @returns.clear
      end

      # Forgets which exchanges are watched, for a channel that has been
      # reopened: the registrations lived on the old one and went with it.
      def reopened
        @watched = []
        @returns.clear
      end

      # Why the message just published was handed back, or nil when it was not.
      #
      # Non-blocking, because a return that was coming has already arrived: the
      # broker sends it ahead of the confirm and bunny dispatches frames in
      # order, so a wait here would put the cost of +mandatory+ on every message
      # rather than on the ones it is about.
      def take
        @returns.pop(true)&.last
      rescue ThreadError
        nil
      end

      # Everything queued, as pairs of message id and reason.
      #
      # A batch has to tell *which* of its messages came back rather than only
      # that one did, and the only thing that can say so is the returned
      # message's own +message-id+ — the returns arrive in whatever order the
      # broker sends them, and nothing about position survives the trip.
      def take_all
        taken = []
        loop { taken << @returns.pop(true) }
      rescue ThreadError
        taken
      end

      private

      # Registers one exchange, once. See {#arm} for why it must not declare.
      def watch(channel, exchange)
        name = exchange.to_s
        return if @watched.include?(name)

        # `passive: true` as well as `no_declare: true`, and it is not redundant on
        # bunny 3.x. `no_declare` is what stops this object declaring anything -- all
        # it is for is receiving +basic.return+ -- but bunny 3.x also *records* every
        # exchange it constructs for topology recovery unless it is passive, and a
        # recorded one is replayed as a declaration when a connection comes back. This
        # object invents a type it never checked: it says `:direct` because
        # +basic.return+ does not care, so recording it would have topology recovery
        # declare a caller's topic or fanout exchange as direct and be answered
        # PRECONDITION_FAILED on a connection that had just been rebuilt.
        #
        # Harmless on bunny 2.x, which has no topology recorder and ignores the key.
        ::Bunny::Exchange.new(channel, :direct, name, no_declare: true, passive: true)
                         .on_return { |info, properties, _content| record(info, properties) }
        forget_recorded(channel, name)
        @watched << name
      end

      # Takes this exchange back out of bunny's topology recorder.
      #
      # `passive: true` above is enough on bunny 3.3 and newer, where a passive
      # declaration is not recorded. It is *not* enough on bunny 3.0 to 3.2, which have
      # the recorder but not that exemption -- and 3.2 is exactly what Ruby 3.1 resolves,
      # because bunny 3.3 needs amq-protocol 2.9 and that needs Ruby 3.2. CI caught it
      # on the 3.1 leg after this was written believing `passive` covered every 3.x.
      #
      # Removing it by name is safe even when the application declared the same exchange
      # itself: that declaration is recorded against the short-lived admin channel it
      # was made on, which {RecoverLiveChannelsOnly} already drops from recovery because
      # the channel is closed by then. So nothing that would have been replayed is lost,
      # and the fabricated `:direct` definition this object carries cannot be replayed
      # over a caller's topic or fanout exchange.
      def forget_recorded(channel, name)
        session = channel.respond_to?(:connection) ? channel.connection : nil
        return unless session.respond_to?(:delete_recorded_exchange_named)

        session.delete_recorded_exchange_named(name)
      rescue StandardError
        # A recorder that will not forget costs a redeclaration attempt on recovery,
        # which bunny logs and carries on from. It must not cost a publish.
        nil
      end

      # Called on bunny's reader thread, so it does nothing but note the reason.
      #
      # The reply text is what the broker said and is worth keeping: NO_ROUTE is
      # not the only reason a message comes back, and the others are the
      # interesting ones. The id is kept with it because a batch is told which
      # message came back by nothing else.
      def record(info, properties)
        code = info.reply_code
        text = info.reply_text.to_s
        reason = text.empty? ? "returned #{code}" : "#{code} #{text}"
        @returns.push([properties[:message_id].to_s, reason])
      end
    end

    # One batch of messages, from the moment they are handed to a channel to
    # the moment every one of them has an answer.
    #
    # A class of its own because it is bookkeeping rather than transport: which
    # delivery tag belongs to which payload, which +basic.return+ belongs to
    # which message, and what each of the two sets bunny leaves behind means.
    # Every method on {Transport} is about one message and a channel; this is
    # the only thing in the file that has to hold a whole batch in its head.
    #
    # @api private
    class BatchPublish
      # @param messages [Array<Hash>] the keywords {Transport#publish} takes,
      #   one hash per message
      # @param returns [ReturnedMessages] the channel's returns, the same object
      #   a single mandatory publish reads
      # @param confirms [ConfirmWait] how to wait, so a lost connection ends the
      #   wait as it does for a single publish
      def initialize(messages, returns, confirms = ConfirmWait.new)
        @messages = messages
        @returns = returns
        @confirms = confirms
        @results = Array.new(messages.size)
        # Delivery tag to payload position, for the messages that are on the
        # wire right now with no answer back yet. Emptied by every wave.
        @wave = {}
        @waited = false
      end

      # Publishes every message in waves no wider than the permits allow, and
      # answers for each of them.
      #
      # A batch that fits inside the ceiling is one wave and behaves exactly as
      # it always did: every message is written before anything is waited for,
      # which is the point of the method. What changed is a batch that does not
      # fit. That used to be written in full, however long the array was, with
      # this process and the broker both holding all of it; it is now written in
      # waves, each one confirmed — and its permits handed back — before the
      # next is written. The results still come back in payload order, each
      # message still gets its own answer, and a +basic.return+ is still charged
      # to the message whose id it carries.
      #
      # @param permits [PublishPermits] the connection's ceiling, shared with
      #   every other publish on it
      # @return [Array<String, PublishError>] one entry per message, in the
      #   order they were given
      def run(channel, permits)
        arm_returns(channel)
        @messages.each_index do |index|
          unless room_for_one(channel, permits)
            no_room_for(index, permits)
            return @results
          end

          publish_one(channel, index, permits)
        end
        settle(channel, permits) unless @wave.empty? && @waited
        @results
      end

      private

      # Gets ready to hear about the exchanges this batch publishes to
      # mandatory. Nothing to do when none of them does.
      def arm_returns(channel)
        exchanges = @messages.filter_map { |message| message[:exchange] if message[:mandatory] }
        @returns.arm_all(channel, exchanges.uniq) unless exchanges.empty?
      end

      # Whether there is room on the wire for one more message.
      #
      # The permit is taken before the message is written, never after: a
      # ceiling checked once the bytes are already gone bounds nothing. When
      # there is none left, the wave already out is confirmed first — which
      # hands its permits back and is the ordinary way a large batch proceeds.
      # Only when that returns no room at all is the answer no, and it is the
      # caller's job to say so rather than to keep buffering.
      def room_for_one(channel, permits)
        return true if permits.take

        settle(channel, permits) unless @wave.empty?
        permits.take
      end

      # Hands one message to the channel, and remembers which delivery tag it
      # was given.
      #
      # The tag is read *after* the publish rather than before. bunny takes the
      # next sequence number as part of +basic_publish+, so a publish refused
      # before it got that far has consumed none, and a tag read in advance
      # would belong to the message after it — which is how a batch ends up
      # reporting the wrong message as the failed one.
      def publish_one(channel, index, permits)
        message = @messages[index]
        channel.basic_publish(message[:body].to_s, message[:exchange],
                              message[:routing_key], publish_options(message))
        @wave[channel.next_publish_seq_no - 1] = index
      rescue StandardError => e
        # One failure out of the batch rather than a reason to abandon the
        # messages already on the wire. They are going to be confirmed anyway,
        # and how many of them arrived is what the caller has to act on.
        #
        # Nothing reached the wire, so nothing is outstanding and the permit
        # goes straight back — the same place Java releases it when
        # basicPublish throws.
        permits.release(1)
        where = message.values_at(:message_id, :exchange, :routing_key)
        @results[index] =
          if PublishFailure.unsent?(e)
            PublishingPausedError.new(PublishFailure.unsent(*where, e))
          else
            PublishError.new(PublishFailure.not_published(*where, e.message))
          end
      end

      # Every message from +from+ onwards, answered with the reason there was no
      # room to write it.
      #
      # Answered rather than raised, because that is what {#run} promises for
      # every other failure: a caller told only "the batch failed" republishes
      # the ones that already arrived. The sentence names the ceiling and what
      # to do about it, so a log line is enough to act on.
      def no_room_for(from, permits)
        (from...@messages.size).each do |index|
          message = @messages[index]
          @results[index] = PublishError.new(
            PublishFailure.not_published(message[:message_id], message[:exchange],
                                         message[:routing_key], permits.exhausted)
          )
        end
      end

      # Waits once for the wave that is on the wire, reads each of its messages'
      # answers off the channel, and hands back the permits of the ones the
      # broker answered for.
      #
      # bunny does not report a failed publish per message. It answers the wait
      # with "were they all acked", leaves the delivery tags of the ones that
      # were not in +nacked_set+, and leaves the ones nothing ever answered for
      # in +unconfirmed_set+. Both are sets of tags rather than of messages,
      # which is what +@wave+ is for: without it a batch could say how many
      # failed and not which, and its results would no longer line up with the
      # payloads that produced them.
      #
      # +nacked_set+ is never emptied by bunny, so it can hold tags from
      # publishes that finished long ago. Only this wave's tags are looked up
      # in it, which is what keeps an older failure from being reported twice.
      #
      # A tag still in +unconfirmed_set+ keeps its permit. Nothing answered for
      # that message, so it is still outstanding as far as the broker is
      # concerned, and a ceiling that forgave it would be a ceiling that never
      # bites on the one broker it exists to protect this process from.
      def settle(channel, permits)
        tags = @wave
        @wave = {}
        @waited = true
        broke = wait_for_batch(channel)
        nacked = channel.nacked_set.dup
        silent = channel.unconfirmed_set.dup
        returned = returns_for(tags.values)
        answered = 0
        tags.each do |tag, index|
          unanswered = silent.include?(tag)
          answered += 1 unless unanswered
          unconfirmed = unanswered || nacked.include?(tag)
          @results[index] = answer_for(@messages[index], returned, unconfirmed, broke)
        end
        permits.release(answered)
      end

      # What one message of a batch came to.
      def answer_for(message, returned, unconfirmed, broke)
        id = message[:message_id]
        if unconfirmed
          reason = PublishFailure.not_confirmed(id, message[:exchange], message[:routing_key])
          # The wait's own failure is worth carrying: "the broker said no" and
          # "the confirm never came" are the same missing acknowledgement and
          # two different things to go and look at.
          return PublishError.new(broke.nil? ? reason : "#{reason}: #{broke.message}")
        end
        return id unless (why = returned[id.to_s])

        PublishError.new(
          PublishFailure.not_routed(id, message[:exchange], message[:routing_key], why),
          unroutable: true
        )
      end

      # One wait for the batch, and the reason it ended early when it did.
      #
      # A wait that raises — a channel the broker took down, a confirm that
      # never came inside bunny's continuation timeout — is not allowed out on
      # its own. The messages it was waiting for still have to be answered one
      # by one, and an exception here would lose every one of those answers,
      # including the ones the broker had already acknowledged.
      def wait_for_batch(channel)
        @confirms.wait(channel)
        nil
      rescue StandardError => e
        e
      end

      # Which of this wave's messages the broker handed back, keyed by message
      # id.
      #
      # A +basic.return+ carries the returned message's own properties, so a
      # batch can ask which of its messages came back rather than only that one
      # did. Only the wave that has just been waited for is considered: a return
      # always precedes the confirm for the same message, so nothing belonging
      # to a later wave can have arrived yet, and everything belonging to an
      # earlier one was taken when that wave settled.
      def returns_for(indexes)
        ids = indexes.map { |index| @messages[index] }
                     .filter_map { |m| m[:message_id].to_s if m[:mandatory] }
        return {} if ids.empty?

        @returns.take_all.each_with_object({}) do |(id, reason), by_id|
          named = charged_to(id, ids, by_id)
          by_id[named] = reason if named
        end
      end

      # A return whose id is not one of this batch's is charged to the first
      # mandatory message not already accounted for. The message cannot be
      # named — it went out without an id, or it belongs to somebody else's
      # publish — but it reached no queue, and the count has to say so.
      def charged_to(id, ids, already)
        return id if ids.include?(id)

        ids.find { |candidate| !already.key?(candidate) }
      end

      # The properties bunny publishes with, built from one batch message.
      def publish_options(message)
        { content_type: message[:content_type], message_id: message[:message_id],
          reply_to: Wire.presence(message[:reply_to]),
          mandatory: message[:mandatory] ? true : false,
          headers: Wire.stringify(message[:headers] || {}),
          persistent: message.fetch(:persistent, true) }
      end
    end

    # The broker, over AMQP 0-9-1.
    #
    # Everything above this class is about envelopes, codecs and retries and
    # knows nothing about channels; everything below it is bunny. The seam is
    # here so the consumer's retry arithmetic can be tested without a broker,
    # by handing {Connection} something else that answers these methods.
    class Transport
      # What {#blocked_reason} says when the broker has blocked the connection
      # and this process cannot say why.
      #
      # Two ways that happens: a session started elsewhere and handed to
      # {#initialize} after RabbitMQ had already blocked it, so the frame
      # carrying the reason came and went before anything here was listening;
      # or an application that registered its own +on_blocked+ on the session
      # afterwards, which replaces the callback below — bunny keeps one.
      # Neither is worth a silence: that the connection is blocked at all is the
      # half an operator acts on.
      UNEXPLAINED_BLOCK = "the broker did not say why"

      # How this transport reaches RabbitMQ.
      #
      # Required lazily, and only here. The gem declares no runtime
      # dependencies because reading an AceMQ envelope should not oblige a
      # process to install a broker client it will never open a socket with —
      # an audit tool that parses headers out of a log has no business
      # compiling in an AMQP stack. The cost of that choice is that the failure
      # moves from install time to the first connection, so it is caught here
      # and re-raised saying what to install.
      def self.load_driver!
        require "bunny"
      rescue LoadError => e
        raise DependencyMissing,
              "the AceMQ transport needs the bunny gem, which is not installed. " \
              "Add `gem \"bunny\", \"~> 2.23\"` to your Gemfile, or run " \
              "`gem install bunny`. (#{e.message})"
      end

      # Opens a connection.
      #
      # The URL decides how the connection is protected unless a {Security} says
      # otherwise: +amqps://+ is encrypted and the broker is verified against
      # the machine's trust store, +amqp://+ is plaintext. Pass +security:+ for
      # a private certificate authority, a client certificate, or the deliberate
      # absence of verification; pass +credentials:+ to keep the password out of
      # the URL, and so out of the error message two lines below this one.
      #
      # The security options are merged over anything in +options+ rather than
      # under it. A caller who reaches past {Security} to bunny's own TLS keys
      # is describing the same thing twice, and of the two answers the one that
      # went through the checks in this library is the one to honour.
      #
      # @param url [String] amqp:// or amqps://
      # @param heartbeat [Integer, Symbol] seconds, or :server to take the
      #   broker's suggestion
      # @param connection_timeout [Numeric] seconds to wait for the handshake
      # @param security [Security, nil] how to protect the connection
      # @param credentials [Credentials, #call, nil] the broker login
      # @param max_outstanding_publishes [Integer] how many messages may be
      #   waiting for a confirm on this connection at once; see
      #   {PublishPermits}. The same ceiling as Java's
      #   +maxOutstandingPublishes+ and .NET's +MaxOutstandingPublishes+, and
      #   the same default
      # @param options [Hash] anything else bunny understands
      # @return [Transport]
      # @raise [DependencyMissing] when bunny is not installed
      # @raise [ConfigurationError] when the security settings cannot be honoured
      # @raise [TransportError] when the broker cannot be reached
      def self.open(url, heartbeat: :server, connection_timeout: 10, security: nil,
                    credentials: nil, max_outstanding_publishes: PublishPermits::DEFAULT,
                    **options)
        load_driver!
        security = Security.for_connection(url, security: security, credentials: credentials)
        # Built before the socket is opened: a ceiling of nought is a
        # configuration mistake, and finding it out after a connection has been
        # established means an error that mentions the broker for a reason that
        # has nothing to do with it.
        permits = PublishPermits.new(max_outstanding_publishes)
        # Ours goes first so a caller's own `topology_recovery_filter` in +options+
        # replaces it rather than being silently overridden.
        session = Bunny.new(url, topology_recovery_filter: RecoverLiveChannelsOnly.new,
                                 heartbeat: heartbeat, connection_timeout: connection_timeout,
                                 **options, **security.to_transport_options)
        security.configure(session)
        session.start
        new(session, max_outstanding_publishes: permits.limit,
                     single_host: single_host?(options))
      rescue DependencyMissing, ConfigurationError
        raise
      rescue StandardError => e
        raise TransportError, "cannot reach the broker at #{redact(url)}: #{e.message}"
      end

      # Whether bunny will have one address to connect to, read the way bunny's
      # own +addresses_from+ reads it: the first of these it finds, or the URL.
      #
      # @api private
      def self.single_host?(options)
        hosts = options[:host] || options[:hostname] || options[:addresses] || options[:hosts]
        Array(hosts).size <= 1
      end

      # A URL with its password taken out, for an error message that will be
      # logged. A credential that reaches a log is a credential that has to be
      # rotated.
      #
      # @api private
      def self.redact(url)
        url.to_s.sub(%r{(://[^:/@]+):[^@]*@}, '\1:***@')
      end

      # @param session [Bunny::Session] an already started connection
      # @param max_outstanding_publishes [Integer] see {PublishPermits}
      # @param single_host [Boolean] whether the session has one broker address,
      #   so a recovery can go straight back to it; see {#watch_recovery}.
      #   {.open} works it out; a caller handing in a session of its own says so
      def initialize(session, max_outstanding_publishes: PublishPermits::DEFAULT,
                     single_host: false)
        @session = session
        @single_host = single_host
        @permits = PublishPermits.new(max_outstanding_publishes)
        @lock = Mutex.new
        # A lock of its own, because a pull holds messages unacknowledged across
        # a whole pass and settling one has to reach the channel it came down.
        # Sharing the publish lock would mean a replay's own republish waiting
        # on the channel it is about to acknowledge from.
        @pull_lock = Mutex.new
        @subscriptions = []
        @returns = ReturnedMessages.new
        @confirms = ConfirmWait.new
        # A lock of its own again, and the smallest one in the class: the
        # broker's blocked callback runs on bunny's reader thread, and a health
        # probe reads it from a web server's. Sharing the publish lock would
        # mean a readiness probe queuing behind a publish that is itself waiting
        # for the broker that has just blocked it.
        @blocked_lock = Mutex.new
        @blocked_reason = nil
        # Its own lock again, and for the same reason: this one is written from
        # bunny's recovery thread and read by every thread that publishes.
        @recovery_lock = Mutex.new
        @recovering = false
        watch_blocked
        watch_recovery
      end

      # The bunny session, for the things this class deliberately does not wrap.
      attr_reader :session

      # How many publishes may be waiting for a confirm on this connection at
      # once. See {PublishPermits}.
      def max_outstanding_publishes = @permits.limit

      # Creates an exchange unless it is already there.
      #
      # @param name [String]
      # @param kind [Symbol, String] direct, topic, fanout or headers
      def declare_exchange(name, kind:, durable: true, auto_delete: false, arguments: {})
        with_admin_channel do |channel|
          channel.exchange_declare(name, kind.to_s, durable: durable, auto_delete: auto_delete,
                                                    arguments: stringify(arguments))
        end
      end

      # Creates a queue unless it is already there.
      #
      # Deliberately without the quorum default {Connection#declare_queue} has:
      # this is the layer that does what it is told, and a caller who reached
      # past the connection to it is declaring exactly what they wrote down.
      # +queue_type+ is honoured when it is given, because {Topology#apply} can
      # be handed a transport and its plan says which kind each queue is.
      #
      # @param queue_type [Symbol, nil] +:classic+, +:quorum+ or +:stream+;
      #   nothing is added to the arguments when it is not given
      def declare_queue(name, queue_type: nil, durable: true, auto_delete: false,
                        exclusive: false, arguments: {})
        arguments = QueueType.table(queue_type, arguments) if queue_type
        with_admin_channel do |channel|
          channel.queue_declare(name, durable: durable, auto_delete: auto_delete,
                                      exclusive: exclusive, arguments: stringify(arguments))
        end
      end

      # Routes messages matching a key from an exchange to a queue.
      def bind(queue:, exchange:, routing_key: "")
        with_admin_channel do |channel|
          channel.queue_bind(queue, exchange, routing_key: routing_key)
        end
      end

      # Sends one message and waits for the broker to take responsibility.
      #
      # The wait is the point. Without publisher confirms a successful publish
      # means the bytes reached a socket, which is not the same as the broker
      # having them, and the difference only ever shows up as messages that
      # were never anywhere.
      #
      # +mandatory+ buys the other half of that. A confirm says the broker has
      # the message; it does not say the message reached a queue, and a publish
      # to an exchange with no matching binding is confirmed and dropped in the
      # same breath. Asking for +mandatory+ makes the broker hand such a message
      # back as +basic.return+ before it confirms it, which is the only way
      # AMQP will tell you. It costs a round trip only when the message really
      # is unroutable.
      #
      # @param reply_to [String, nil] AMQP's own +reply-to+ property, left off
      #   the message entirely when it is nil
      # @param mandatory [Boolean] whether reaching no queue is an error rather
      #   than a silence
      # @return [String] the message id it went out with
      # @raise [PublishingPausedError] when the broker has blocked the
      #   connection: nothing was sent, and sending again once it clears is safe
      # @raise [PublishError] when the broker did not confirm it, or when it was
      #   mandatory and reached no queue — {PublishError#unroutable?} tells the
      #   two apart
      def publish(exchange:, routing_key:, body:, content_type: nil, message_id: nil,
                  headers: {}, persistent: true, reply_to: nil, mandatory: false)
        refuse_while_blocked!(message_id, exchange, routing_key)
        returned = nil
        confirmed = publish_channel([message_id, exchange, routing_key]) do |channel|
          @returns.arm(channel, exchange) if mandatory
          ok = write_and_wait(channel, body.to_s, exchange, routing_key,
                              content_type: content_type, message_id: message_id,
                              reply_to: presence(reply_to), mandatory: mandatory,
                              headers: stringify(headers), persistent: persistent)
          # The return, when there is one, arrives ahead of the confirm and is
          # dispatched on the reader thread while this one waits — so by the
          # time the wait is over it is already waiting to be taken.
          returned = @returns.take if mandatory
          ok
        end
        unless confirmed
          raise PublishError, PublishFailure.not_confirmed(message_id, exchange, routing_key)
        end
        return message_id if returned.nil?

        raise PublishError.new(
          PublishFailure.not_routed(message_id, exchange, routing_key, returned),
          unroutable: true
        )
      rescue PublishError
        raise
      rescue StandardError => e
        raise PublishError,
              PublishFailure.not_published(message_id, exchange, routing_key, e.message)
      end

      # Sends a batch, and waits for every confirm at the end.
      #
      # The difference from a loop around {#publish} is the whole point of the
      # method: there, every message costs a round trip to the broker and back
      # before the next one is written, because every message waits for its own
      # confirm. Here every message is handed to the channel first and the wait
      # happens once, for all of them — which is what publisher confirms were
      # designed for and what makes a thousand-message publish take about as
      # long as one.
      #
      # **It is not atomic**, and nothing in AMQP could make it so. A batch that
      # half arrived is the ordinary outcome of a broker problem partway
      # through, so each message gets its own answer rather than the batch
      # getting one between them.
      #
      # @param messages [Array<Hash>] the keywords {#publish} takes, one hash
      #   per message
      # @return [Array<String, PublishError>] one entry per message, in the
      #   order they were given: the id it went out with, or the failure it met
      def publish_all(messages)
        return [] if messages.empty?

        # Answered per message rather than raised, because that is what this
        # method promises for every other failure.
        if (reason = blocked_reason)
          return messages.map do |m|
            PublishingPausedError.new(
              PublishFailure.paused(m[:message_id], m[:exchange], m[:routing_key], reason)
            )
          end
        end

        batch = BatchPublish.new(messages, @returns, @confirms)
        publish_channel { |channel| batch.run(channel, @permits) }
      rescue PublishingPausedError => e
        # Refused before anything was written -- a recovery in progress -- so
        # every message gets the same answer, as it would for a blocked broker.
        messages.map do |m|
          PublishingPausedError.new(
            PublishFailure.not_published(m[:message_id], m[:exchange], m[:routing_key],
                                         e.message)
          )
        end
      end

      # Delivers messages until the returned subscription is cancelled.
      #
      # The block runs on bunny's consumer pool rather than on the caller's
      # thread, so +concurrency+ is how many messages this subscription works
      # on at once. One by default, which keeps a queue's messages in the order
      # the broker offers them.
      #
      # @yieldparam delivery [Delivery]
      # @return [Subscription]
      def subscribe(queue, prefetch: 20, concurrency: 1, tag: nil, arguments: {}, &handler)
        # No shutdown timeout on the channel's work pool, and that +nil+ is what
        # keeps a drain inside its deadline.
        #
        # Cancelling the last consumer on a bunny channel shuts its work pool down
        # and, given a timeout, waits that long for a busy worker -- sixty seconds
        # by default -- inside +basic_cancel+, before {Connection#close} has looked
        # at its own deadline at all. Measured on 0.7.5: close(timeout: 0.5) on a
        # 1.5s handler took 1.5s and raised nothing; on a 63s handler it took 60.5s
        # and then said "within 0.5s". With +nil+ the pool is told to stop and the
        # cancel returns, so the only wait left is the library's own, against one
        # deadline. The fourth positional argument has meant this since bunny 2.x
        # and still does in 3.4.
        channel = @session.create_channel(nil, concurrency, false, nil)
        channel.prefetch(prefetch) if prefetch.positive?
        # A consumer tag of our own when the caller gives none, rather than leaving it
        # to the broker.
        #
        # bunny 3.x records every consumer for topology recovery and keys the record by
        # tag, and recovery re-subscribes -- which records the consumer again. With a
        # broker-assigned tag the new registration lands under a *new* key, so the
        # registry grows instead of being replaced: measured at 1, 2, 4 recorded
        # consumers over three recoveries. Each recorded consumer then gets its own
        # `maybe_reinitialize_consumer_pool!`, and all but the last of those pools is
        # orphaned with its threads still parked -- which is the thread growth a soak
        # found in the Ruby standing load. A plain bunny consumer does not grow this
        # way, which is what showed the tag was ours to fix.
        #
        # Generated once here, so it survives every recovery of this subscription and
        # the record is replaced rather than added to.
        tag ||= "acemq-#{queue}-#{SecureRandom.hex(8)}"
        consumer = channel.queue(queue, passive: true).subscribe(
          manual_ack: true, block: false, consumer_tag: tag, arguments: stringify(arguments)
        ) do |info, properties, body|
          handler.call(delivery_from(channel, info, properties, body))
        end
        track(Subscription.new(channel, consumer))
      rescue StandardError => e
        channel.close if channel&.open?
        raise TransportError, "cannot consume from #{queue.inspect}: #{e.message}"
      end

      # Takes one message off a queue, without subscribing to it.
      #
      # A subscription is a standing arrangement; this is a single read, which
      # is what a pass over a queue with a beginning and an end needs. Draining
      # a dead-letter queue with a consumer means writing the code that decides
      # when to stop, and getting it wrong means a tool that never exits.
      #
      # The delivery is unacknowledged, and settling it is the caller's job.
      # That is deliberate: a message left unacknowledged is held by the broker
      # rather than lost, so a tool that dies half way through a pass returns
      # everything it was holding.
      #
      # @param queue [String]
      # @return [Delivery, nil] nil when the queue has nothing waiting
      def pull(queue)
        @pull_lock.synchronize do
          channel = pull_channel
          info, properties, body = channel.basic_get(queue, manual_ack: true)
          next nil if info.nil?

          delivery_from(channel, info, properties, body, lock: @pull_lock)
        end
      rescue StandardError => e
        raise TransportError, "cannot read a message from #{queue.inspect}: #{e.message}"
      end

      # How many messages are waiting on a queue.
      #
      # A number for a dashboard or a test, not a decision to make in a
      # handler: it is a snapshot of a queue that is still moving, and it is
      # already wrong by the time it is read anywhere else.
      def message_count(queue)
        with_admin_channel do |channel|
          channel.queue_declare(queue, passive: true).message_count
        end
      end

      # Whether a queue is on the broker, creating nothing.
      #
      # Its own question because a declaration cannot answer it: declaring a
      # queue that is missing creates it, so anything built on declarations
      # would create the very queues it was asked only to look for.
      def queue_exists?(name)
        @session.queue_exists?(name)
      end

      # Removes a queue and every message still on it.
      #
      # For tests and for tools. A service that deletes queues has usually
      # confused a queue with a session, and the messages go with it —
      # including the ones somebody was about to be paid for.
      def delete_queue(name)
        with_admin_channel { |channel| channel.queue_delete(name) }
      end

      # Removes an exchange.
      def delete_exchange(name)
        with_admin_channel { |channel| channel.exchange_delete(name) }
      end

      # Whether the connection is up.
      def open? = @session.open?

      # Why the broker has asked this connection to stop publishing, or nil.
      #
      # RabbitMQ sends +connection.blocked+ when it is low on memory or disk and
      # +connection.unblocked+ when the alarm clears. Between the two the broker
      # stops reading, so {#publish} declines with {PublishingPausedError}
      # rather than writing into a socket nobody reads — and this is the
      # difference between an operator seeing "the broker is out of disk" and
      # seeing a service whose publishes are being declined.
      #
      # The reason is the broker's own words, +"low on disk space"+ or
      # +"low on memory"+, recorded from the frame that carried it. Bunny keeps
      # the flag but not the reason, so both are asked for: bunny's +blocked?+
      # decides *whether*, which means a reason recorded before an automatic
      # recovery cannot outlive the connection it belonged to, and the callback
      # below supplies *why*.
      #
      # @return [String, nil]
      def blocked_reason
        return nil unless @session.respond_to?(:blocked?) && @session.blocked?

        @blocked_lock.synchronize { @blocked_reason } || UNEXPLAINED_BLOCK
      end

      # Whether the broker has asked this connection to stop publishing.
      def blocked? = !blocked_reason.nil?

      # Cancels every subscription and closes the connection.
      def close
        # Taken out under the lock and cancelled outside it. A handler still
        # running is about to want this same lock to publish a dead letter, and
        # holding it across a cancel that waits on the broker is how a shutdown
        # becomes a deadlock nobody can reproduce.
        subscriptions = @lock.synchronize do
          taken = @subscriptions
          @subscriptions = []
          taken
        end
        subscriptions.each(&:cancel)

        @lock.synchronize do
          @publish_channel.close if @publish_channel&.open?
          @publish_channel = nil
        end
        # Anything a pass was still holding goes back to its queue when this
        # closes, which is the whole reason a declined message is held rather
        # than returned one at a time.
        @pull_lock.synchronize do
          @pull_channel.close if @pull_channel&.open?
          @pull_channel = nil
        end
        @session.close
        nil
      rescue StandardError => e
        raise TransportError, "cannot close the connection cleanly: #{e.message}"
      end

      # A running subscription.
      #
      # Stopping and closing are two steps rather than one because a handler
      # that is still running needs its channel: closing it out from under an
      # in-flight acknowledgement loses the message the handler just finished.
      # {Consumer#cancel} stops delivery, waits, and only then closes.
      class Subscription
        def initialize(channel, consumer)
          @channel = channel
          @consumer = consumer
        end

        # Whether the broker can still deliver down this subscription.
        #
        # The channel's own answer rather than a flag kept here. The two can
        # disagree: a channel closed by the broker, or taken down by an error on
        # it, stops delivery without anything in this process being told, and a
        # health check reading a local flag would report a consumer that had
        # been deaf for an hour as running.
        def open? = @channel.open?

        # Tells the broker to send no more. Deliveries already handed over are
        # unaffected.
        #
        # Both steps check the channel first, and both are safe to call twice.
        # A connection is closed by whatever gets there first — the consumer it
        # owns, or the connection closing every consumer it tracks — and a
        # basic.cancel on a channel that has already gone is a CHANNEL_ERROR,
        # which RabbitMQ answers by dropping the whole connection. The symptom
        # is a shutdown that takes ten seconds while bunny reconnects to a
        # process that is exiting.
        def stop
          @consumer.cancel if @channel.open?
          nil
        rescue StandardError
          # Cancelling on a connection that has already gone is what shutdown
          # looks like when the broker went first, and raising here would turn
          # a tidy exit into a stack trace about a socket nobody is waiting on.
          nil
        end

        # Releases the channel. Nothing may be acknowledged after this.
        def close
          @channel.close if @channel.open?
          nil
        rescue StandardError
          nil
        end

        # Both, for a caller with no handlers to wait for.
        def cancel
          stop
          close
        end
      end

      private

      # Records the broker asking this connection to stop publishing.
      #
      # Bunny keeps a flag and throws the reason away, so the reason is caught
      # here as it arrives. Registered rather than polled because there is
      # nothing to poll: the frame is the only time the broker says why.
      #
      # Guarded by +respond_to?+ so a session double in a test — which the
      # security and batch specs both use — does not have to grow two callbacks
      # it has no use for.
      def watch_blocked
        return unless @session.respond_to?(:on_blocked) && @session.respond_to?(:on_unblocked)

        @session.on_blocked do |blocked|
          reason = blocked.respond_to?(:reason) ? blocked.reason.to_s : ""
          @blocked_lock.synchronize { @blocked_reason = reason.empty? ? nil : reason }
        end
        @session.on_unblocked { @blocked_lock.synchronize { @blocked_reason = nil } }
      end

      # A recovered connection has nothing outstanding on it.
      #
      # This is the other half of {PublishPermits#reopened}, and without it the first
      # half almost never ran. A publish waiting for its confirm when the connection
      # dies keeps its permit -- correctly, because the broker may yet have the
      # message and nothing here can say it did not arrive -- so the permit can only
      # be freed by something that knows the connection itself is gone.
      #
      # That was the opening of a new publishing channel. But bunny recovers a session
      # by re-opening the *same* channel objects on the new transport, so no new
      # channel is ever created, and the permits of everything in flight were never
      # given back. The ceiling shrank by that many at every reconnect until it
      # reached zero, after which every publish failed with "1000 publishes are
      # already waiting for a confirm" -- for ever, on a healthy broker, over a
      # connection bunny had successfully recovered.
      #
      # A fault drill found it: one broker node restarted under a standing load, and
      # afterwards this library never published successfully again while four others
      # in the same drill recovered within seconds.
      #
      # Separate from {#watch_blocked} because the two are unrelated, and because
      # sharing its guard is how this was missed once already: a session offering
      # recovery but not the blocked callbacks would have lost the hook entirely.
      def watch_recovery
        if @session.respond_to?(:before_recovery_attempt_starts)
          @session.before_recovery_attempt_starts do
            @recovery_lock.synchronize { @recovering = true }
            rewind_single_host
            # A confirm still owed now is owed by a connection that is gone; see
            # {ConfirmWait} for why bunny would otherwise hold the wait, and the
            # publish lock with it, for its whole continuation timeout.
            @confirms.abandon
          end
        end

        return unless @session.respond_to?(:after_recovery_completed)

        @session.after_recovery_completed do
          @recovery_lock.synchronize { @recovering = false }
          # Nothing published on the old connection can be confirmed on the new one,
          # which is the conclusion Java's shutdown listener reaches when it fails
          # every pending publish.
          @permits.reopened
          @returns.reopened
        end
      end

      # Sends a single-address session's attempt straight back to its one broker.
      #
      # bunny advances its address index on every connect and rewinds it only when
      # an attempt finds it past the end -- and that rewind costs a whole retry,
      # +network_recovery_interval+ again. With one address the first connect leaves
      # the index past the end, so every recovery slept twice: 5s, a depleted list,
      # 5s more, then the connect -- about 10s for a broker that was back at once,
      # measured on bunny 2.24 and 3.4 alike. This runs after the first sleep and
      # before bunny picks the address, so the first attempt connects. With several
      # addresses it does nothing: rewinding there would send every attempt back to
      # the first host, the one that may just have died, and bunny's rotation is
      # the point of having several.
      def rewind_single_host
        return unless @single_host && @session.respond_to?(:reset_address_index)

        @session.reset_address_index
      end

      # Refuses to write anything between a connection being lost and its channels
      # being back.
      #
      # A recovery has two steps and only the first is visible in the session's state.
      # Bunny rebuilds the socket and completes the AMQP handshake -- at which point
      # the session reports open -- and re-opens each channel after that. A frame
      # written in between arrives on a channel the broker has never seen, and the
      # broker's answer is not to refuse the frame: it is CHANNEL_ERROR "expected
      # 'channel.open'", which closes the whole connection.
      #
      # A publisher that kept publishing through a recovery therefore destroyed the
      # connection bunny had just rebuilt; bunny started another recovery, and the
      # next publish destroyed that one too. It never converged. The broker's log
      # shows one of these per attempt, for ever:
      #
      #   Recovering from connection.close (CHANNEL_ERROR - expected 'channel.open')
      #
      # Bunny's own `recovering_from_network_failure?` cannot be used for this, and
      # that is the trap: it is cleared as soon as the handshake succeeds and *before*
      # `recover_channels` runs (bunny 2.24, session.rb), so throughout the window
      # that actually matters the session reports open and not recovering. A guard
      # built on it changes nothing, which is exactly what measuring it showed.
      #
      # So the window is tracked here instead, from bunny's two recovery callbacks:
      # `before_recovery_attempt_starts` opens it and `after_recovery_completed`
      # closes it, and the second of those fires after the channels are back.
      #
      # A fault drill found this. One broker node was restarted under a standing load
      # publishing and consuming on one connection, and afterwards the library never
      # published successfully again -- 45,000 consecutive failures, while four other
      # client libraries in the same drill recovered within seconds. A publisher alone
      # survives, because it seldom writes inside the window; a consumer on the same
      # connection adds channels that also have to be re-opened, which widens the
      # window enough to be hit every time at a few hundred publishes a second.
      #
      # Refused rather than waited out: the caller is told the message did not go and
      # that a retry is the right answer, as every other transient failure here does.
      # Waiting inside the publish lock would hold up nothing useful and would turn a
      # broker restart into a stalled application.
      #
      # Raised as PublishingPausedError because that is what it is: declined
      # before anything was written, and safe to retry. It was a bare
      # TransportError, which a load counted as a failed publish.
      #
      # The callback is not the start of the window, though. bunny marks the
      # session lost the moment it notices, then sleeps +network_recovery_interval+
      # before it calls +before_recovery_attempt_starts+. A publish in those
      # seconds found the channel still reading open, and bunny refused the write
      # itself -- unsent, but a plain PublishError, about 85 of them per recovery.
      # So a session that is not open, and was not closed on purpose, is refused
      # here too. A closed one is left to fail as it always has: it is not coming
      # back, and "retry once it recovers" would be a lie.
      def refuse_while_recovering!(context = nil)
        reason =
          if @recovery_lock.synchronize { @recovering }
            "the connection is being recovered and its channels are not open yet. " \
              "Publishing now would be refused by the broker and would close the " \
              "connection again; this message was not sent, and a retry once the " \
              "recovery completes will go down the recovered connection."
          elsif connection_lost?
            "the connection has been lost and is waiting to be recovered; this message " \
              "was not sent, and a retry once the recovery completes will go down the " \
              "recovered connection."
          end
        return if reason.nil?

        reason = PublishFailure.not_published(*context, reason) if context
        raise PublishingPausedError, reason
      end

      # Lost and not given up on: bunny's +:disconnected+ before its recovery
      # attempt, or a socket that died before bunny noticed. Asked only of a
      # session that can also say it was closed, so a double that cannot is
      # never refused on a guess.
      def connection_lost?
        @session.respond_to?(:closed?) && !@session.open? && !@session.closed?
      end

      # Declines a publish on a connection the broker has blocked, before
      # anything is written.
      #
      # Until this existed the publish was written anyway and sat in
      # +wait_for_confirms+ until the broker read the socket again or bunny's
      # timeout gave up, and the second of those came back as an unconfirmed
      # message: a {PublishError} indistinguishable from one the broker may have
      # lost. Declining up front is what .NET does, and it is the only point at
      # which "not sent" can be said truthfully. A block that arrives after the
      # check finds the message already written, and that publish waits and
      # answers as it always has.
      #
      # @raise [PublishingPausedError] when the connection is blocked
      def refuse_while_blocked!(message_id, exchange, routing_key)
        reason = blocked_reason
        return if reason.nil?

        raise PublishingPausedError,
              PublishFailure.paused(message_id, exchange, routing_key, reason)
      end

      # The channel every publish goes down, opened once and guarded.
      #
      # One channel rather than one per publish because a channel is a
      # round trip to open and confirms have to be enabled on it; guarded
      # because a bunny channel is not safe to use from two threads at once,
      # and a consumer thread dead-lettering a message publishes on this same
      # channel while an application thread may be publishing its own.
      def publish_channel(context = nil)
        @lock.synchronize do
          refuse_while_recovering!(context)
          if @publish_channel.nil? || !@publish_channel.open?
            discard_channel(@publish_channel)
            @publish_channel = @session.create_channel
            @publish_channel.confirm_select
            @returns.reopened
            # Whatever was still unconfirmed went down with the old channel and
            # can never be confirmed on this one, so holding its permits would
            # shrink the ceiling a little with every reconnect until publishing
            # stopped for a reason nothing in the logs explained. Java frees the
            # same permits from its shutdown listener.
            @permits.reopened
          end
          yield @publish_channel
        end
      end

      # One message written and waited for, under a permit taken before any of
      # it reaches the wire.
      #
      # A single publish waits for its own confirm, so it can only ever be one
      # message outstanding and the ceiling never refuses it on its own account.
      # The permits are the connection's, though, and a batch that left messages
      # nothing answered for is still holding some of them — so a publish that
      # finds none left is publishing into a broker that has stopped keeping up,
      # and is told so in those words rather than joining the queue.
      #
      # @return [Boolean] bunny's answer to "were they all acked"
      # @raise [PublishError] when there is no room left on the connection
      def write_and_wait(channel, body, exchange, routing_key, **properties)
        # Taken outside the ensure below on purpose: a permit that was never
        # granted must not be given back, and an ensure that cannot tell the two
        # apart hands the connection a permit it never had.
        raise PublishError, @permits.exhausted unless @permits.take

        outstanding = false
        begin
          begin
            channel.basic_publish(body, exchange, routing_key, **properties)
          rescue StandardError => e
            raise unless PublishFailure.unsent?(e)

            # Refused by bunny before the write: declined, not lost.
            raise PublishingPausedError,
                  PublishFailure.unsent(properties[:message_id], exchange, routing_key, e)
          end
          outstanding = true
          confirmed = @confirms.wait(channel)
          outstanding = false
          confirmed
        ensure
          # An ack and a nack are both answers and give the permit back; so does
          # a publish the channel would not even take, since nothing reached the
          # wire. The one case that keeps it is a message the broker has and has
          # said nothing about, which is exactly the message still outstanding.
          @permits.release(1) unless outstanding
        end
      end

      # A channel of its own for each declaration, because a refused
      # declaration kills the channel it was made on.
      #
      # A queue that already exists with different arguments answers
      # PRECONDITION_FAILED, which is the only way AMQP reports drift without
      # the management API. Sharing a channel would mean one such refusal took
      # every later declaration down with it, and the error nobody would then
      # be able to explain is the second one.
      def with_admin_channel
        channel = @session.create_channel
        yield channel
      rescue StandardError => e
        raise TransportError, e.message
      ensure
        channel.close if channel&.open?
      end

      def track(subscription)
        @lock.synchronize { @subscriptions << subscription }
        subscription
      end

      # +lock+ is given for a pulled delivery, which is settled by whoever is
      # running the pass rather than on the thread it arrived on, and may be
      # settled long after. A subscribed delivery needs none: it is settled on
      # the consumer thread that owns its channel.
      def delivery_from(channel, info, properties, body, lock: nil)
        tag = info.delivery_tag
        settle = lambda do |&action|
          lock ? lock.synchronize { action.call } : action.call
        end
        Delivery.new(
          body: body,
          content_type: properties[:content_type].to_s,
          routing_key: info.routing_key.to_s,
          message_id: properties[:message_id].to_s,
          headers: properties[:headers] || {},
          redelivered: info.redelivered,
          reply_to: properties[:reply_to].to_s,
          on_ack: -> { settle.call { channel.ack(tag, false) } },
          on_nack: ->(requeue) { settle.call { channel.nack(tag, false, requeue) } }
        )
      end

      # The channel every pull goes down, opened once.
      #
      # One channel, because the messages a pass declines are held
      # unacknowledged on it until the pass ends, and a channel per pull would
      # release them the moment it closed.
      def pull_channel
        if @pull_channel.nil? || !@pull_channel.open?
          discard_channel(@pull_channel)
          @pull_channel = @session.create_channel
        end
        @pull_channel
      end

      # Hands a replaced channel's threads back.
      #
      # Every bunny channel carries a consumer work pool -- its own threads, one by
      # default and +concurrency+ for a subscription -- so replacing a channel without
      # closing it leaks them. {#publish_channel} and {#pull_channel} both replace one
      # whenever the old channel is no longer open, which is what a lost connection can
      # leave behind, and neither used to close the channel it dropped.
      #
      # **This is hygiene, not the fix for the leak the soak found.** Honest about its
      # own worth: bunny usually recovers a session by re-opening the *same* channel
      # objects, so the branch this runs from is not reached on an ordinary reconnect,
      # and measuring a Ruby standing load with and without this change showed the same
      # thread growth either way. What it removes is a real leak on the paths that *do*
      # replace a channel -- a channel closed by the broker for a channel-level error,
      # or a pull channel dropped between passes -- which is worth closing on its own
      # terms rather than because it was mistaken for something bigger.
      #
      # The soak's finding is bunny 2.24.0's: a client built on bunny alone, with none
      # of this library in the path, went from 6 threads to 146 over 90 forced
      # recoveries and stopped publishing and consuming altogether, where this library
      # over the same 90 reached 49 and kept both directions moving. The threads pile
      # up in +Bunny::Session#handle_network_failure+ and +#recover_channels+, both
      # blocked on bunny's own +@channel_mutex+.
      #
      # Both steps here are needed and neither may raise. `close` is the orderly one,
      # and it reaches the broker only while the connection is alive -- which it often
      # is not, because a dead connection is why the channel is being replaced. That
      # leaves the pool parked on its queue, and `maybe_kill_consumer_work_pool!` is
      # bunny's own way to stop it. Guarded by +respond_to?+ because it is not part of
      # bunny's documented surface, and a future version that drops it must cost a
      # thread rather than a publish.
      def discard_channel(channel)
        return if channel.nil?

        begin
          channel.close if channel.open?
        rescue StandardError
          nil
        end

        return unless channel.respond_to?(:maybe_kill_consumer_work_pool!)

        begin
          channel.maybe_kill_consumer_work_pool!
        rescue StandardError
          nil
        end
      end

      # See {Wire}, which {BatchPublish} writes its properties with too.
      def stringify(table) = Wire.stringify(table)
      def presence(value) = Wire.presence(value)
    end
  end
end
