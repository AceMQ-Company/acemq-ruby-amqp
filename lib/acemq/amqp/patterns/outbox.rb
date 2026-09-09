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
require_relative "../envelope"

module AceMQ
  module AMQP
    # Deciding to send a message and sending it, without a gap between them.
    #
    # A service that writes to a database and then publishes has two things
    # that can fail independently. Crash between them and the work is committed
    # with nobody told; publish first and fail to commit, and the world has been
    # told about something that did not happen. Neither is recoverable
    # afterwards, because by then there is nothing left that remembers what was
    # supposed to happen.
    #
    # The outbox closes the gap by writing the message into the same transaction
    # as the work. Both commit or neither does, and a relay publishes what was
    # committed.
    module Patterns
      # A message that has been decided but not yet published.
      #
      # +body+ is bytes rather than an object, and that is the important part: a
      # record outlives the process that wrote it, and the class it was encoded
      # from may not survive the deployment that happens while it is waiting.
      OutboxRecord = Struct.new(
        :id, :exchange, :routing_key, :body, :content_type, :headers, :created_at,
        keyword_init: true
      ) do
        def to_s = "#{id} -> #{exchange.empty? ? "(default)" : exchange}/#{routing_key}"
      end

      # Holds messages that have been decided but not yet published.
      #
      # A store is anything answering three methods:
      #
      #   add(record)          # record a message to be published
      #   pending(limit)       # the records waiting, oldest first
      #   mark_published(id)   # remove one the broker has confirmed
      #
      # An implementation is only worth having if +add+ can join the caller's
      # transaction. A store that opens its own connection has the gap back, and
      # has it in a place that looks like it has been dealt with — which is
      # worse than not having the pattern at all.
      module OutboxStore
      end

      # An outbox in this process.
      #
      # It has none of the property the pattern exists for: nothing here shares
      # a transaction with anybody's database, so a crash between the work
      # committing and the record being written loses the message exactly as
      # publishing directly would. It is for tests, and for seeing the shape of
      # the thing before wiring the real one.
      class InMemoryOutboxStore
        def initialize
          @records = {}
          @lock = Mutex.new
        end

        # Records a message to be published.
        #
        # Adding the same record twice is not an error — the caller may be
        # retrying its own transaction — but it must not become two messages.
        def add(record)
          raise ArgumentError, "an outbox record needs an id" if record.id.to_s.empty?

          @lock.synchronize { @records[record.id] ||= record }
          nil
        end

        # The records waiting, oldest first.
        #
        # Oldest first because the order they were written in is usually the
        # order the writer meant, and a relay that publishes them out of order
        # has invented a reordering nobody asked for.
        def pending(limit = 0)
          waiting = @lock.synchronize { @records.values }
                         .sort_by { |record| [record.created_at, record.id] }
          limit.positive? ? waiting.first(limit) : waiting
        end

        # Removes a record the broker has confirmed.
        def mark_published(id)
          @lock.synchronize { @records.delete(id) }
          nil
        end

        # How many records are waiting.
        def size = @lock.synchronize { @records.size }
      end

      # Encodes a payload into an outbox record, ready for +store.add+.
      #
      # Call it inside the transaction that does the work:
      #
      #   db.transaction do
      #     orders.insert(order)
      #     store.add(Patterns.record(mq, event, to: "order.placed",
      #                               exchange: "orders-events",
      #                               type: "order.placed.v2"))
      #   end
      #
      # The envelope is built here, by the same rules {Connection#publish} uses
      # and with this connection's origin, so a message that went through the
      # outbox is indistinguishable on the wire from one that did not. That is
      # the point: the outbox is a delivery mechanism, not a different kind of
      # message.
      #
      # @param connection [Connection] whose codec and origin to use
      # @param payload [Object] anything the codec will encode
      # @param to [String] the routing key
      # @param exchange [String] empty for the default exchange
      # @param envelope [Envelope, nil] one built elsewhere
      # @param fields [Hash] envelope fields, when no envelope is given
      # @return [OutboxRecord]
      def self.record(connection, payload, to:, exchange: "", envelope: nil, codec: nil,
                      **fields)
        if envelope && !fields.empty?
          raise ArgumentError,
                "record was given both an envelope and the fields to build one " \
                "(#{fields.keys.join(", ")}); pass one or the other"
        end

        codec = codec.nil? ? connection.codec : Codec.check!(codec)
        envelope ||= Envelope.new(origin: connection.origin, **fields)
        OutboxRecord.new(
          id: envelope.id, exchange: exchange, routing_key: to,
          body: codec.encode(payload), content_type: codec.content_type,
          headers: envelope.to_headers(to), created_at: Time.now
        )
      end

      # Publishes what the outbox holds.
      #
      #   relay = Patterns::OutboxRelay.new(mq, store, interval: 1)
      #   relay.start
      #   at_exit { relay.close }
      #
      # Deliberately at-least-once. A record is removed only once the broker has
      # confirmed the message, so a crash in between publishes it again — which
      # is why consumers of anything sent this way have to be idempotent, and why
      # {Patterns.idempotent} is in the same library. The alternative, removing
      # first, loses messages instead, and a lost message is the worse of the
      # two: a duplicate can be recognised, an absence cannot.
      class OutboxRelay
        # How often the outbox is swept, and how many records go per sweep.
        DEFAULT_INTERVAL = 1.0
        DEFAULT_BATCH = 100

        attr_reader :interval, :batch

        # @param connection [Connection, Transport] where to publish
        # @param store [OutboxStore] what to publish
        # @param interval [Numeric] seconds between sweeps
        # @param batch [Integer] records per sweep
        # @param on_error [#call, nil] handed anything a sweep raised. Worth
        #   passing: a relay whose sweeps are all failing is an outbox filling
        #   up, and without this the only symptom is messages that never arrive.
        #
        #   A one-argument callable is given the exception, as it always was. One
        #   that also declares +exchange:+ and +routing_key:+ is given where the
        #   record was going as well:
        #
        #     on_error: lambda { |error, exchange:, routing_key:|
        #       tracing.outbox_publish_failed(exchange: exchange, reason: error.message)
        #       logger.warn("outbox stuck on #{exchange}/#{routing_key}: #{error.message}")
        #     }
        #
        #   The destination is what the telemetry event wants and what an alert
        #   is worth routing by — "the outbox cannot reach +orders-events+" is
        #   actionable in a way that "a sweep failed" is not. Both are empty
        #   strings when the store itself raised, since no record was in hand.
        def initialize(connection, store, interval: DEFAULT_INTERVAL, batch: DEFAULT_BATCH,
                       on_error: nil)
          # A record already holds encoded bytes and rendered headers, so what
          # this needs is the raw publish rather than the one that builds an
          # envelope. Taking either a connection or a transport also lets a test
          # drive a relay with no socket anywhere.
          @transport = connection.respond_to?(:transport) ? connection.transport : connection
          @store = store
          @interval = interval.to_f
          @batch = batch
          @on_error = on_error
          @on_error_wants_destination = destination_wanted?(on_error)
          @lock = Mutex.new
          @wake = ConditionVariable.new
          @stopped = false
        end

        # Sweeps the outbox until {#close}.
        def start
          @thread ||= Thread.new { run }
          self
        end

        # Publishes one batch, and returns how many went out.
        #
        # Public so a test can drive a relay without waiting for a tick, and so
        # an application can flush its outbox on demand — at the end of a
        # request, say, rather than up to an interval later.
        #
        # @return [Integer] records published
        # @raise [StandardError] whatever the broker or the store raised
        def sweep
          drain { |error, _record| raise error }
        end

        # Whether the sweeping thread is running.
        def running? = !@thread.nil?

        # Stops sweeping and waits for the sweep in progress.
        def close
          @lock.synchronize do
            @stopped = true
            @wake.broadcast
          end
          @thread&.join
          @thread = nil
          nil
        end

        private

        # Publishes one batch, handing whatever went wrong — and the record it
        # went wrong on — to the block rather than raising.
        #
        # One code path for both callers. {#sweep} re-raises from the block and
        # so still raises exactly what the broker or the store raised; the
        # sweeping thread reports instead. Keeping the record in a local is what
        # lets the report name a destination: the callback used to be handed a
        # bare exception, which cannot say which exchange an outbox is stuck on.
        #
        # A failing record stops the batch rather than being skipped. The
        # records were written in an order somebody meant, and stepping over one
        # to publish the next invents a reordering; the next sweep tries again
        # from the same place.
        #
        # @return [Integer] records published before it stopped
        def drain
          published = 0
          record = nil
          begin
            @store.pending(@batch).each do |pending|
              record = pending
              publish(pending)
              # Marked after the confirm, never before. A crash in this gap
              # republishes the record, which is the at-least-once this pattern
              # promises; marking first would lose it instead.
              @store.mark_published(pending.id)
              published += 1
            end
          rescue StandardError => e
            yield e, record
          end
          published
        end

        # Tells whoever asked, in as much detail as they asked for.
        def report(error, record)
          return if @on_error.nil?
          return @on_error.call(error) unless @on_error_wants_destination

          @on_error.call(error, exchange: record ? record.exchange.to_s : "",
                                routing_key: record ? record.routing_key.to_s : "")
        end

        # Whether a callback wants the destination as well as the exception.
        #
        # Asked of the callable rather than versioned into two constructors, so
        # a relay built the way every existing one is built keeps working and a
        # callback that declares the keywords starts receiving them.
        def destination_wanted?(callback)
          return false if callback.nil?

          callable = callback if callback.is_a?(Proc) || callback.is_a?(Method)
          callable ||= callback.method(:call)
          callable.parameters.any? { |kind, _name| %i[key keyreq keyrest].include?(kind) }
        end

        # Publishes one record, telling the store when that failed.
        #
        # A store that claims records under a lease wants to hear about a
        # failure: counting the attempt is what eventually stops a record
        # nothing can publish from being tried on every sweep for ever, and
        # giving up the lease is what lets the next sweep have a go rather than
        # waiting the lease out for nothing. A store with no such notion — the
        # in-memory one — is simply not asked, and the failure travels on
        # exactly as it did before.
        def publish(record)
          @transport.publish(exchange: record.exchange, routing_key: record.routing_key,
                             body: record.body, content_type: record.content_type,
                             message_id: record.id, headers: record.headers,
                             persistent: true)
        rescue StandardError => e
          @store.mark_failed(record.id, e.message) if @store.respond_to?(:mark_failed)
          raise
        end

        def run
          until stopped?
            # A failed sweep is not fatal, and that is the whole point of an
            # outbox: the records are still there, and the next tick tries
            # again. Nothing is lost by the relay being down, only delayed.
            drain { |error, record| report(error, record) }
            pause
          end
        end

        def stopped? = @lock.synchronize { @stopped }

        # Waits on a condition rather than sleeping, so closing a relay with a
        # thirty-second interval does not take thirty seconds.
        def pause
          @lock.synchronize do
            @wake.wait(@lock, @interval) unless @stopped
          end
        end
      end
    end
  end
end
