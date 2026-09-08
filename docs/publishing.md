# Publishing

```ruby
mq.publish({ "order_id" => "A-1" }, to: "order.placed",
           exchange: "orders-events", type: "order.placed.v2")
```

One method. The payload is a Ruby object, `to:` is the routing key, `exchange:`
is where it goes, and everything after that describes the envelope.

## Where a message goes

`exchange:` defaults to `""`, the default exchange, which routes by queue name:
`to:` is then the name of a queue. That is convenient for a first test and a
poor arrangement between services, because the publisher has to know the name of
the consumer's queue. A named exchange with a binding lets the consumer decide
what it wants, and lets a second consumer arrive without the publisher changing.

```ruby
mq.publish(event, to: "orders.new")                                 # default exchange
mq.publish(event, to: "order.placed", exchange: "orders-events")    # topic exchange
```

The default exchange also cannot be bound, listed or given a policy, which is
the same reason [retry rungs come home through `acemq.retry`](reliability.md)
rather than through it.

## The envelope

Every message carries one. Left alone, `publish` builds it: a fresh UUID for
`id`, `correlation_id` equal to that id, `attempt` 1, `first_seen` now, and the
connection's `origin`.

Keyword arguments after `exchange:` set its fields:

```ruby
mq.publish(event, to: "order.placed", exchange: "orders-events",
           type: "order.placed.v2",
           version: 2,
           correlation_id: incoming.envelope.correlation_id,
           causation_id: incoming.envelope.id,
           headers: { "tenant" => "acme" })
```

`headers:` is your own; the reserved `x-acemq-` names are refused rather than
silently dropped. [The envelope](envelope.md) has the full list and what each
field is for.

For a message whose metadata derives from another's, build the envelope and pass
it:

```ruby
outgoing = incoming.envelope.with(type: "shipment.requested",
                                  causation_id: incoming.envelope.id)
mq.publish(payload, to: "shipment.requested", exchange: "shipping-events",
           envelope: outgoing)
```

`Envelope` is frozen, so `with` returns a new one. Passing **both** an envelope
and the fields to build one raises `ArgumentError` — one or the other, because
the two answers differ and neither is obviously the one that was meant.

## What publish returns

The envelope that actually went on the wire — what the interceptors left, not
what you handed in. An interceptor that stamps a tenant or rewrites the routing
key has already run by then, so this is the id to log if you are going to log
one:

```ruby
sent = mq.publish(event, to: "order.placed", exchange: "orders-events")
logger.info("published #{sent.id} correlation=#{sent.correlation_id}")
```

## Durability

`persistent: true` is the default: the broker is asked to write the message to
disk, so a broker restart does not lose it. Pass `persistent: false` for
something whose value expires faster than a restart takes — a cache
invalidation, a heartbeat — and remember that durability is a property of the
message *and* the queue. A persistent message on a transient queue is still lost
with the queue.

## Codecs

The connection's codec turns the payload into bytes and sets the content type.
JSON unless you said otherwise, and per-publish when one message is different:

```ruby
mq = Connection.open(url, codec: StringCodec.new)

mq.publish(png_bytes, to: "thumbnails", codec: BytesCodec.new)
```

See [codecs](serialization.md) for the four that ship, how a composite one
decodes several content types, and what it takes to write your own.

## Failures

`publish` raises. A broker that cannot be reached, a payload no codec can
encode, an interceptor that refused the message — all of them come out as an
exception, and none of them is quietly swallowed, because a publisher that
returns normally having sent nothing is the failure mode you find out about from
the consumer's absence.

```ruby
begin
  mq.publish(event, to: "order.placed", exchange: "orders-events")
rescue AceMQ::AMQP::PublishError => e
  # the broker did not confirm it
rescue AceMQ::AMQP::EncodeError => e
  # the codec could not turn this payload into bytes
end
```

Publishing is confirmed: the transport waits for the broker to acknowledge the
message and raises `PublishError` if it does not. A publish that returns has
been accepted by the broker — not necessarily routed to a queue, which is a
separate thing an unbound exchange will do silently.

Whatever the failure, `acemq.messages.publish.failed` is counted before the
interceptors are told, so a publish an interceptor refused is counted too. It
did not reach the broker, which is what the metric is about. See
[metrics and health](observability.md).

## Publishing and a database in the same breath

A service that writes a row and then publishes has two things that can fail
independently, and there is no arrangement of `begin`/`ensure` that closes the
gap. That is what the [outbox](patterns.md#outbox) is for: the message is
written in the same transaction as the work, and a relay publishes what was
committed.

## Threading

`publish` is safe to call from several threads. The transport keeps one
publishing channel, opened once with confirms enabled, and holds a mutex across
the publish and its confirm — a bunny channel is not safe to use from two
threads at once, and a consumer thread dead-lettering a message publishes on
that same channel while an application thread may be publishing its own. The
cost is that publishes on one connection serialise; the alternative is two
threads interleaving frames into a protocol error.

Interceptors are called on whichever thread is publishing, so one that keeps
state has to be safe to call from several at once.

## Next

- [Consuming](consuming.md) — the other half
- [The envelope](envelope.md) — every field, and what reads it
- [Interceptors](interceptors.md) — stamping every message without editing every
  call site
- [Exchanges, queues and bindings](topology.md) — where a message can actually go
