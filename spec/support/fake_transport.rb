# frozen_string_literal: true

# A broker that is only a Hash.
#
# The consumer's retry arithmetic — which attempt this is, how long to wait,
# when to give up, what reason to write onto the dead letter — is the part of
# this library most worth testing and the part least worth a broker. Everything
# it needs from a transport is here, so those tests run on a laptop with no
# Docker, in milliseconds, and the integration specs are left to prove the
# things only a real broker can: that a message survives the wire.
class FakeTransport
  Published = Struct.new(:exchange, :routing_key, :body, :content_type, :message_id, :headers,
                         keyword_init: true)

  attr_reader :published, :declared_queues, :declared_exchanges, :bindings

  def initialize
    @published = []
    @declared_queues = []
    @declared_exchanges = []
    @bindings = []
    @closed = false
  end

  # persistent is accepted and ignored: nothing here survives the process, so
  # there is nothing for it to mean. It is in the signature because a fake that
  # takes fewer arguments than the real thing stops catching the mistake it
  # exists to catch.
  def publish(exchange:, routing_key:, body:, content_type: nil, message_id: nil, headers: {},
              persistent: true) # rubocop:disable Lint/UnusedMethodArgument
    @published << Published.new(exchange: exchange, routing_key: routing_key, body: body,
                                content_type: content_type, message_id: message_id,
                                headers: headers)
    message_id
  end

  def declare_queue(name, **options)
    @declared_queues << [name, options]
  end

  def declare_exchange(name, **options)
    @declared_exchanges << [name, options]
  end

  def bind(queue:, exchange:, routing_key: "")
    @bindings << [queue, exchange, routing_key]
  end

  def subscribe(_queue, **_options, &)
    Subscription.new
  end

  def message_count(_queue) = 0
  def queue_exists?(_name) = true
  def delete_queue(_name) = nil
  def closed? = @closed
  def close = @closed = true

  # What was published to a queue through the default exchange, which is how
  # both dead-lettering and parking get there.
  def published_to(queue)
    @published.select { |m| m.exchange == "" && m.routing_key == queue }
  end

  class Subscription
    def stop = nil
    def close = nil
    def cancel = nil
  end
end

# One delivery, with somewhere to record how it was settled.
#
# Settling is a pair of callables on the delivery itself rather than a tag the
# consumer hands back to a channel, so standing in for a broker here is a
# matter of recording two calls.
class FakeDelivery
  attr_reader :acked, :nacked

  def self.build(body: "{}", headers: {}, routing_key: "orders.new",
                 content_type: "application/json", redelivered: false)
    recorder = new
    delivery = AceMQ::AMQP::Delivery.new(
      body: body, content_type: content_type, routing_key: routing_key,
      message_id: headers["x-acemq-id"].to_s, headers: headers, redelivered: redelivered,
      on_ack: -> { recorder.record_ack },
      on_nack: ->(requeue) { recorder.record_nack(requeue) }
    )
    [delivery, recorder]
  end

  def initialize
    @acked = 0
    @nacked = []
  end

  def record_ack = @acked += 1
  def record_nack(requeue) = @nacked << requeue
  def acked? = @acked.positive?
  def requeued? = @nacked.include?(true)
end
