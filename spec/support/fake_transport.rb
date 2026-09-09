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
                         :reply_to, :mandatory, keyword_init: true)

  attr_reader :published, :declared_queues, :declared_exchanges, :bindings

  def initialize
    @published = []
    @declared_queues = []
    @declared_exchanges = []
    @bindings = []
    @missing = []
    @refused = []
    @unroutable = []
    @closed = false
  end

  # persistent is accepted and ignored: nothing here survives the process, so
  # there is nothing for it to mean. It is in the signature because a fake that
  # takes fewer arguments than the real thing stops catching the mistake it
  # exists to catch.
  def publish(exchange:, routing_key:, body:, content_type: nil, message_id: nil,
              headers: {}, reply_to: nil, mandatory: false,
              persistent: true) # rubocop:disable Lint/UnusedMethodArgument
    if @refused.include?(routing_key)
      raise AceMQ::AMQP::PublishError,
            "the broker would not confirm message #{message_id} with key #{routing_key.inspect}"
    end

    if mandatory && @unroutable.include?(routing_key)
      raise AceMQ::AMQP::PublishError.new(
        "the broker had nowhere to route message #{message_id} " \
        "with key #{routing_key.inspect}: 312 NO_ROUTE", unroutable: true
      )
    end

    @published << Published.new(exchange: exchange, routing_key: routing_key, body: body,
                                content_type: content_type, message_id: message_id,
                                headers: headers, reply_to: reply_to, mandatory: mandatory)
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
  def queue_exists?(name) = !@missing.include?(name)
  def delete_queue(_name) = nil
  def closed? = @closed
  def open? = !@closed
  def close = @closed = true

  # Pretends a queue was never declared, which is how the consumer's
  # missing-rung path is reached without taking a broker away from it.
  def missing!(*names) = @missing.concat(names)

  # Refuses to confirm anything published with these keys, the way a broker
  # refuses a publish to a queue that is not there and is not being created.
  # How the set-aside failure path is reached without a broker.
  def refuse!(*names) = @refused.concat(names)

  # Confirms a publish to these keys and hands it straight back, the way a
  # broker returns a mandatory message it has nowhere to route. Only a mandatory
  # publish notices, which is the point: without it the message is confirmed and
  # dropped, exactly as it is on a real broker.
  def unroutable!(*names) = @unroutable.concat(names)

  # What was published to a queue through the default exchange, which is how
  # both dead-lettering and parking get there.
  def published_to(queue)
    @published.select { |m| m.exchange == "" && m.routing_key == queue }
  end

  class Subscription
    def initialize = @open = true
    def open? = @open
    def stop = @open = false
    def close = @open = false
    def cancel = @open = false
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
                 content_type: "application/json", redelivered: false, reply_to: nil)
    recorder = new
    delivery = AceMQ::AMQP::Delivery.new(
      body: body, content_type: content_type, routing_key: routing_key,
      message_id: headers["x-acemq-id"].to_s, headers: headers, redelivered: redelivered,
      reply_to: reply_to,
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

  # Rejected without a requeue, which is what settles a message the broker is
  # not going to hand back.
  def rejected? = @nacked.include?(false)
  def settled? = acked? || !@nacked.empty?
end
