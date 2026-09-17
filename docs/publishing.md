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
been accepted by the broker — not necessarily *routed to a queue*, which is a
separate thing an unbound exchange will do silently, and which the next section
is about.

Whatever the failure, `acemq.publish.total{outcome="failed"}` is counted before the
interceptors are told, so a publish an interceptor refused is counted too. It
did not reach the broker, which is what the metric is about. See
[metrics and health](observability.md).

## When reaching no queue should be an error

A confirm says the broker has the message. It does not say the message reached a
queue, and the two come apart more often than they look like they should: an
exchange with no matching binding, a typo in a routing key, a consumer's queue
that was never declared. The publish is confirmed and the message is dropped in
the same breath. It is the quietest failure AMQP has — the publisher succeeded,
the consumer is still waiting, and nothing anywhere says why.

`mandatory: true` asks the broker to hand the message back instead:

```ruby
begin
  mq.publish(event, to: "order.placed", exchange: "orders-events", mandatory: true)
rescue AceMQ::AMQP::PublishError => e
  raise unless e.unroutable?

  logger.error("nothing is bound to order.placed on orders-events: #{e.message}")
end
```

`PublishError#unroutable?` is what separates the two: `false` is a message the
broker would not take, `true` is one it took and could not route. One exception
class with a flag rather than two classes, so a caller who only wants to know
that the message did not arrive still rescues one thing. Go and Python split the
same failure the same way; Java has it as `PublishOptions.allowUnroutable()`,
which is the same choice made the other way up.

**It is off by default**, and per publish. Turning it on for everybody would turn
a message nobody happens to be listening for *yet* into an exception in code that
has never seen one, which is a fair description of most first deployments. It
also costs a round trip only when a message really is unroutable: the return
frame arrives ahead of the confirm the publish was already waiting for, so a
message that routes normally pays nothing.

An unroutable publish counts `acemq.publish.total{outcome="unroutable"}` rather
than `outcome="failed"`, and marks the publish span `unroutable`, which **is** an
error outcome — with the broker's own reply text as `messaging.acemq.reason`. The
word is kept apart from `failed` because they are fixed in different places:
`failed` is a broker or a network, `unroutable` is a binding nobody made.

The consumer uses this on its own account. A message being set aside into
`{queue}.dlq` or `{queue}.parked` is republished mandatory, so a dead-letter queue
that is not on the broker is heard rather than silently swallowed by the default
exchange — see
[when a message cannot be set aside](reliability.md#when-a-message-cannot-be-set-aside-at-all).

## Publishing a batch

`publish` waits for the broker to confirm each message before it returns, which
is the right trade for one message and the wrong one for a thousand: a thousand
messages cost a thousand round trips, and the publisher spends nearly all of that
time waiting for a network. `publish_all` hands every message over first and
waits for all of the confirms together:

```ruby
envelopes = mq.publish_all(orders, to: "order.placed", exchange: "orders-events",
                           type: "order.placed.v2")
```

The envelopes come back in the order the payloads were given, whatever order the
broker confirmed them in. Everything else works as it does for a single publish:
each message gets an envelope of its own (or pass `envelopes:`, one per payload),
each one goes through the [publish interceptors](interceptors.md) exactly once,
each one is counted in `acemq.publish.total` on its own, and `mandatory: true`
means for each message what it means for one.

**It is not atomic**, and nothing in AMQP could make it so. There is no way to
publish a hundred messages such that all or none arrive, and a library that
offered one would be lying. What this does instead is tell you how much did
arrive:

```ruby
begin
  mq.publish_all(orders, to: "order.placed", exchange: "orders-events")
rescue AceMQ::AMQP::PublishError => e
  # "3 of 500 messages were not confirmed; 497 were. The first failure was: ..."
  logger.error(e.message)
end
```

A batch that half succeeded is the ordinary outcome of a broker problem partway
through, so **every message is still waited for after the first failure** and the
counts in that sentence are real. A caller told only "it failed" republishes
hundreds of messages that are already on a queue. The failure quoted is the first
one *in payload order* rather than the first the broker answered, so the same
half-failed batch reports the same message twice running. Java's `sendAll` and
.NET's `SendAllAsync` raise that sentence word for word, so one runbook covers
all three.

`unroutable?` on that error is true only when *every* failure was a message the
broker had nowhere to route — that being the only case where concluding "nothing
is bound" is right. A batch that met a missing binding and a refused message is
not an unroutable batch.

### How many messages may be unconfirmed at once

A thousand, by default, for the whole connection:

```ruby
mq = AceMQ::AMQP::Connection.open("amqp://localhost",
                                  max_outstanding_publishes: 1000) # the default
```

That ceiling is the difference between backpressure and a memory leak that looks
like throughput. A publisher that hands over messages faster than the broker
confirms them is accumulating unconfirmed messages in this process and in the
broker, and nothing about the accumulation is visible until the process dies of
it. Java's `maxOutstandingPublishes` and .NET's `MaxOutstandingPublishes` are the
same number in the same place.

`publish_all` respects it by writing in **waves**: up to the ceiling goes out,
those confirms come back, and only then is the next wave written. A batch that
fits inside the ceiling — which is nearly all of them — is one wave and is
exactly as fast as it always was. A batch of a hundred thousand is a hundred
waves rather than a hundred thousand messages nobody bounded, and you no longer
have to split it by hand. The results still come back in payload order and each
message still gets its own answer.

Room is given back when the broker answers for a message, and an ack, a nack and
a `basic.return` are all answers. A message the broker has taken and said
*nothing* about keeps its room, because that message really is still outstanding.
So when a broker stops confirming altogether, the ceiling fills and the messages
that cannot be written are told why rather than buffered:

```
cannot publish message m-4 to exchange "orders-events" with key "order.placed":
1000 publishes are already waiting for a confirm and none of them completed.
The broker is not keeping up; publish more slowly rather than buffering more.
```

Raise the ceiling if you have measured that you need to and have the memory for
it. Reaching it is a signal about the broker, and buffering more is the one
response to that signal which cannot help.

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

`publish_all` holds that same mutex for the whole batch — every wave and every
wait — because the delivery tags it hands back have to belong to this batch and
to nothing else. A very large batch therefore keeps other threads on this
connection waiting for as long as it takes. The
[ceiling](#how-many-messages-may-be-unconfirmed-at-once) bounds the memory a
batch costs, not the time it holds the mutex for; if other threads on this
connection must keep publishing while a long batch runs, give the batch a
connection of its own.

Interceptors are called on whichever thread is publishing, so one that keeps
state has to be safe to call from several at once.

## Next

- [Consuming](consuming.md) — the other half
- [The envelope](envelope.md) — every field, and what reads it
- [Interceptors](interceptors.md) — stamping every message without editing every
  call site
- [Exchanges, queues and bindings](topology.md) — where a message can actually go
