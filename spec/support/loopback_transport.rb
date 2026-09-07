# frozen_string_literal: true

# A broker that routes, in this process.
#
# FakeTransport records what was published and delivers nothing, which is all the
# retry engine's arithmetic needs. The patterns need more: request and reply is
# not a shape you can see without a reply coming back, and a pipeline is not one
# you can see without the next stage receiving anything. This routes — default
# exchange to the queue of that name, named exchanges by their bindings — so
# those tests run in milliseconds with no Docker anywhere.
#
# Delivery is synchronous, on the publishing thread. Real brokers are not, and
# the difference is deliberate: a test that has to wait for another thread to
# notice something is a test that is sometimes flaky and always slow. What that
# cannot show is concurrency, which is what the integration specs are for.
class LoopbackTransport
  attr_reader :published, :declared_queues, :declared_exchanges, :bindings, :subscriptions

  def initialize
    @published = []
    @declared_queues = []
    @declared_exchanges = []
    @bindings = []
    @subscriptions = []
    @waiting = Hash.new { |queues, name| queues[name] = [] }
    @subscribers = Hash.new { |queues, name| queues[name] = [] }
    @draining = {}
    @kinds = {}
    @lock = Mutex.new
    @closed = false
  end

  def publish(exchange:, routing_key:, body:, content_type: nil, message_id: nil, headers: {},
              persistent: true) # rubocop:disable Lint/UnusedMethodArgument
    @published << FakeTransport::Published.new(
      exchange: exchange, routing_key: routing_key, body: body, content_type: content_type,
      message_id: message_id, headers: headers
    )
    routed(exchange, routing_key).each do |queue|
      offer(queue, body: body, content_type: content_type, routing_key: routing_key,
                   message_id: message_id, headers: headers)
    end
    message_id
  end

  def declare_queue(name, **options)
    @declared_queues << [name, options]
    @lock.synchronize { @waiting[name] } # so message_count answers 0 rather than nothing
    nil
  end

  def declare_exchange(name, kind: "direct", **options)
    @declared_exchanges << [name, options.merge(kind: kind)]
    @kinds[name] = kind.to_s
    nil
  end

  def bind(queue:, exchange:, routing_key: "")
    @bindings << [queue, exchange, routing_key]
    nil
  end

  # Subscribes, and hands over anything that arrived before there was anybody to
  # hand it to.
  def subscribe(queue, **options, &handler)
    @subscriptions << [queue, options]
    @lock.synchronize { @subscribers[queue] << handler }
    drain(queue)
    Subscription.new(self, queue, handler)
  end

  # @api private
  def unsubscribe(queue, handler)
    @lock.synchronize { @subscribers[queue].delete(handler) }
  end

  # Takes one message off without subscribing, unacknowledged, the way a replay
  # reads a dead-letter queue. A message returned with requeue goes back to the
  # head, which is exactly the behaviour a replay has to work around.
  def pull(queue)
    message = @lock.synchronize { @waiting[queue].shift }
    return nil if message.nil?

    delivery_for(queue, message)
  end

  def message_count(queue) = @lock.synchronize { @waiting[queue].size }
  def queue_exists?(name) = @lock.synchronize { @waiting.key?(name) }
  def delete_queue(name) = @lock.synchronize { @waiting.delete(name) }
  def closed? = @closed
  def open? = !@closed
  def close = @closed = true

  def published_to(queue)
    @published.select { |m| m.exchange == "" && m.routing_key == queue }
  end

  # What is still sitting on a queue, for a test that wants to look at a
  # dead-letter queue nothing is draining.
  def contents(queue) = @lock.synchronize { @waiting[queue].dup }

  class Subscription
    def initialize(transport, queue, handler)
      @transport = transport
      @queue = queue
      @handler = handler
      @open = true
    end

    def open? = @open

    def stop
      @open = false
      @transport.unsubscribe(@queue, @handler)
    end

    def close = @open = false
    def cancel = stop
  end

  private

  # The queues a publish reaches. The default exchange routes to the queue whose
  # name matches the key, which is the rule the retry and dead-letter paths rely
  # on; a named exchange routes by its bindings.
  def routed(exchange, routing_key)
    return [routing_key] if exchange.to_s.empty?

    kind = @kinds.fetch(exchange, "direct")
    @bindings.select { |_, name, key| name == exchange && matches?(kind, key, routing_key) }
             .map(&:first)
  end

  def matches?(kind, bound, routing_key)
    case kind
    when "fanout" then true
    when "topic" then topic_matches?(bound, routing_key)
    else bound == routing_key
    end
  end

  # AMQP topic wildcards: * is one word, # is any number of them.
  def topic_matches?(pattern, routing_key)
    expression = pattern.split(".").map do |word|
      case word
      when "*" then "[^.]+"
      when "#" then ".*"
      else Regexp.escape(word)
      end
    end.join('\.')
    Regexp.new("\\A#{expression}\\z").match?(routing_key)
  end

  def offer(queue, **message)
    handler = @lock.synchronize do
      @waiting[queue] << message
      @subscribers[queue].first
    end
    drain(queue) if handler
  end

  # Handed over one at a time, and never re-entered for the same queue: a
  # handler that republishes a retry onto its own queue would otherwise be
  # called from inside itself, one stack frame deeper per attempt. The message
  # it published is picked up by the loop already running instead.
  def drain(queue)
    return unless @lock.synchronize { @draining[queue] ? false : (@draining[queue] = true) }

    begin
      loop do
        message, handler = @lock.synchronize do
          break [nil, nil] if @waiting[queue].empty? || @subscribers[queue].empty?

          # Round-robin, so a queue with two subscribers shares the work the way
          # a broker would. Consumer-group tests turn on it.
          @subscribers[queue].rotate!
          [@waiting[queue].shift, @subscribers[queue].last]
        end
        break if message.nil?

        handler.call(delivery_for(queue, message))
      end
    ensure
      @lock.synchronize { @draining.delete(queue) }
    end
  end

  def delivery_for(queue, message)
    requeued = false
    AceMQ::AMQP::Delivery.new(
      body: message[:body], content_type: message[:content_type],
      routing_key: message[:routing_key].to_s, message_id: message[:message_id].to_s,
      headers: message[:headers] || {}, redelivered: false,
      on_ack: -> {},
      on_nack: lambda { |requeue|
        next if requeued || !requeue

        requeued = true
        @lock.synchronize { @waiting[queue].unshift(message) }
      }
    )
  end
end
