# Patterns

The things everybody writes on top of a message queue, written once. Required
separately, because none of it is protocol and the core stays the contract and
the transport:

```ruby
require "acemq/amqp/patterns"
```

Most of them **wrap a handler and hand back a handler**. That goes to `consume`
unchanged, so the retry policy, the dead-lettering and the envelope are all
still whatever you configured — a pattern that took over the consumer would have
to reimplement them, and then there would be two retry engines to keep in step.

| | |
|---|---|
| [Idempotency](#idempotency) | do a message's work once, however often it arrives |
| [Outbox](#outbox) | decide to send and send, with no gap in between |
| [Request and reply](#request-and-reply) | ask a question and wait for the answer |
| [Replay](#replay) | put dead letters back, once the fix is out |
| [Ordering](#ordering) | keep some messages in order without serialising all of them |
| [Consumer groups](#consumer-groups) | start a set of workers together, and stop them together |
| [Routing slips](#routing-slips) | let the message carry its own itinerary |
| [Pipelines](#pipelines-and-middleware) | wrap a handler; chain one service to the next |
| [Schemas](#schemas) | remember what a message used to look like |
| [Streams](#streams) | a queue that keeps what it has delivered |

Nothing here needs anything private. Every one of them is written against the
public API, which is the only honest test of whether that API is wide enough.

## Idempotency

```ruby
store = Patterns::InMemoryIdempotencyStore.new(window: 6 * 3600)

mq.consume("orders.new", &Patterns.idempotent(store) do |message|
  warehouse.reserve(message.payload)
  Ack.accept
end)
```

A duplicate is **accepted**, not rejected. The work was done, so the message has
been handled, and dead-lettering it would raise an alarm about something that
went right.

A handler that does not accept has its key **forgotten**, so its retry can
actually run. Without that, the first failure would poison the key and every
retry would be waved through as a duplicate.

`key:` takes the key from the payload instead of the message id, for when two
different messages carry the same order and doing the order twice is the thing
to prevent:

```ruby
Patterns.idempotent(store, key: ->(message) { message.payload["order_id"] }) { … }
```

A store is anything answering `first_time?(key)` and `forget(key)`, and
**`first_time?` has to be atomic** — it both asks and claims.

`InMemoryIdempotencyStore` is right behind one worker and wrong the moment there
are two: each has its own memory, so both are told they are first. Its `window:`
is how long a key is remembered, and it sweeps as it goes rather than growing
without limit.

The store worth having is your own database, written in the same transaction as
the work:

```ruby
class OrdersIdempotency
  def first_time?(key)
    DB[:handled].insert(key: key, at: Time.now)
    true
  rescue Sequel::UniqueConstraintViolation
    false
  end

  def forget(key) = DB[:handled].where(key: key).delete
end
```

That is also the only arrangement that closes the gap between the handler
finishing and the acknowledgement reaching the broker, which is why this is a
guard against duplicates rather than exactly-once.

## Outbox

```ruby
store = Patterns::InMemoryOutboxStore.new

db.transaction do
  orders.insert(order)
  store.add(Patterns.record(mq, event, to: "order.placed",
                            exchange: "orders-events", type: "order.placed.v2"))
end

relay = Patterns::OutboxRelay.new(mq, store, interval: 1).start
at_exit { relay.close }
```

A service that writes to a database and then publishes has two things that can
fail independently. Crash between them and the work is committed with nobody
told; publish first and fail to commit, and the world has been told about
something that did not happen. Writing the message into the same transaction as
the work removes the gap — both commit or neither does — and the relay publishes
what was committed.

A record holds **encoded bytes** rather than an object, because it outlives the
process that wrote it and the class may not survive the deployment that happens
while it waits. Its envelope is built by the same rules `publish` uses, so a
message that went through the outbox is indistinguishable on the wire from one
that did not.

The relay is deliberately **at-least-once**: a record is removed only after the
broker has confirmed it, so a crash in between sends it again. Consumers of
anything sent this way need to be [idempotent](#idempotency), which is why that
pattern is in the same library. Removing first would lose messages instead, and
an absence cannot be recognised the way a duplicate can.

`sweep` is public, so an application can flush its outbox at the end of a
request rather than up to an interval later, and a test can drive a relay
without waiting for a tick:

```ruby
relay = Patterns::OutboxRelay.new(mq, store)   # not started
relay.sweep                                     # publish what is pending, now
```

A store is anything answering `add`, `pending(limit = 0)` and
`mark_published(id)`, and it is only worth having **if `add` can join the
caller's transaction**. A store that opens its own connection has the gap back,
in a place that looks like it has been dealt with.

## Request and reply

```ruby
Patterns.serve(mq, "price.requests") do |message|
  { "price" => catalogue.price(message.payload["sku"]) }   # the answer, not an Ack
end

prices = Patterns::Requester.new(mq, to: "price.requests", timeout: 5)
prices.call({ "sku" => "X-1" })   # => { "price" => 1299 }
prices.close
```

Messaging is asynchronous and this is a synchronous shape drawn on top of it,
which is a real cost rather than a free convenience: a caller blocked on a reply
holds a thread, a connection and a deadline, and a queue that backs up turns
into a service that stops responding. Reach for it where a caller genuinely
cannot go on without the answer, and publish an event otherwise.

A requester is meant to be **kept and reused** — it holds a queue and a consumer,
so one per request means a queue per request. Without `reply_to:` it generates an
exclusive, transient, auto-deleting queue that goes away with the process; a
reply queue that outlived its requester would collect answers nobody is waiting
for.

The responder's block returns **the answer rather than an `Ack`**, and raising
sends the failure back to the caller: somebody blocked on a reply should learn
that it failed rather than wait out the timeout. Having answered, the request is
settled rather than retried, because replying and then retrying would answer
twice.

`call` raises `RequestTimedOut` when nothing arrives in time, and
`ResponderFailed` when the far end raised. **A timeout says an answer did not
arrive. It says nothing about whether the work was done**, which is why a
request that changes anything wants an [idempotent](#idempotency) responder.

A request carrying no reply queue is dead-lettered rather than looped: retrying
cannot make a reply queue appear.

Two headers carry this, `acemq-reply-to` and `acemq-error`. They are
**application** headers on purpose — the `x-acemq-` namespace belongs to the
engine and is kept away from what a handler sees, so a responder could never
read them if they lived there.

## Replay

```ruby
result = Patterns.replay(mq, from: "orders.new.dlq", exchange: "orders-events",
                         limit: 500) do |envelope, _body|
  envelope.error.include?("timeout")
end

result.to_s   # => "moved 37, skipped 463 (drained)"
```

The thing somebody actually does at three in the morning: a dead-letter queue
has two thousand messages in it, the fix is deployed, and they need to go back
through — but not all of them, and not silently. The block decides which go, so
a replay can be done in stages; `reason` is `:drained`, `:limit` or `:deadline`,
because "moved 500" means something quite different when the limit was 500.

Messages the block declines are **held unacknowledged** for the length of the
pass rather than returned one at a time. Returning one immediately does not
work: the broker puts it back at the head of the queue, so the next read hands
over the same message and everything behind it is never seen. The broker still
has the held ones, so a tool that dies half way through returns them rather than
losing them.

**A replayed message goes back on attempt one**, with `x-acemq-error` cleared.
Anything else does nothing that can be seen from outside: a message
dead-lettered on the last attempt of a five-attempt policy arrives back on
attempt five, the consumer gives up on it before the handler is called, and two
thousand messages move from the dead-letter queue to the dead-letter queue.

```ruby
Patterns.replay(mq, from: "orders.new.dlq", exchange: "orders-events",
                restart: false)   # put back exactly what was there
```

`restart: false` is for an audit, or for a queue read by something that counts
attempts itself. The identity is untouched either way: same id, same
correlation, same `x-acemq-first-seen`, so
[giving up on age](reliability.md#policies) still applies — which is right, the
fix was for the bug, not for the clock.

Each replayed message is stamped with `acemq-replayed-from`, `acemq-replayed-at`
and `acemq-replay-count`, so a consumer that needs to treat them differently can
and one that does not is unaffected. A message is acknowledged only after the
broker has confirmed the new copy: a crash in that gap replays it twice, which
is the right way round for a dead-letter queue.

`limit:` and `deadline:` both default to zero, meaning no limit — which, against
a queue somebody is still writing to, may mean never stopping. Set one.

Replaying a queue through the default exchange with no routing key is **refused**
rather than allowed to loop. A dead letter's routing key is the dead-letter
queue, so it would publish every message straight back onto the queue it was
read from, for ever, and the only sign would be a queue that never empties.

## Ordering

```ruby
mq.consume("orders.new", concurrency: 16,
           &Patterns.ordered("x-order-id") { |message| apply(message.payload) })
```

A queue delivers in order and a consumer with concurrency above one stops
honouring that. Usually the right trade; the wrong one where a later message
about the same thing must not overtake an earlier one — an "order cancelled"
arriving before the "order placed" it cancels. This buys ordering **per key**
while keeping concurrency across keys.

The key is a header name, or anything callable for a key that lives in the
payload:

```ruby
Patterns.ordered(->(message) { message.payload["order_id"] }) { … }
Patterns.ordered(Patterns.by_correlation) { … }   # one business action, in sequence
Patterns.ordered(Patterns.by_header("x-tenant")) { … }
```

A message whose key comes out empty is handled with no ordering, because there
is nothing to order it against.

**What it does not do**: it orders the handling of messages already delivered.
It cannot reorder ones the broker delivered out of order, and with several
consumers on one queue it orders only within each process. Ordering across
processes is a routing decision — a consistent hash exchange, or a queue per
partition:

```ruby
Patterns.partitioned_routing_key("orders", order_id, 8)   # => "orders.3"
Patterns.partition(order_id, 8)                           # => 3
```

`partition` is FNV-1a rather than Ruby's own `hash`, and that is the point: Ruby
randomises string hashes per process, so two workers would disagree about where
a key belongs, and so would a Go publisher and a Ruby consumer.

## Consumer groups

```ruby
group = Patterns::ConsumerGroup.new(mq, "orders.new", size: 4) do |message|
  place(message.payload)
  Ack.accept
end

at_exit { group.close }
```

Starting workers by hand means remembering to stop every one, and a partial
shutdown leaves messages held by a consumer nobody is waiting for. A group is
also sized from configuration, which is the number most often changed after a
service is running.

**Concurrency, or a group?** `concurrency:` runs several handlers on one consumer
and one channel. A group runs several consumers, each with its own channel and
prefetch. Reach for the group when handlers are slow enough that one channel's
prefetch becomes the limit, or when a fair share across processes matters: the
broker round-robins between consumers, so four here compete evenly with four in
another instance where one consumer with concurrency four would not.

Anything else you pass goes on to each `consume` — `prefetch:`, `retry_policy:`,
`codec:`.

If a later consumer will not start, the ones already running are **stopped
before the failure is passed on** — a half-started group holds messages nothing
is going to handle. `close` stops every consumer even when one refuses, and
raises the refusal afterwards.

## Routing slips

```ruby
slip = Patterns::RoutingSlip.new
                            .step("orders-events", "order.validate", name: "validate")
                            .step("orders-events", "order.charge",   name: "charge")
                            .step("orders-events", "order.ship",     name: "ship")

slip.start(mq, order)

# in the charging service
mq.consume("charge-queue", &Patterns.follow_slip(mq) do |message|
  charge(message.payload)     # the payload to send onwards
end)
```

The alternative to a central orchestrator: the route is decided once, by whoever
started the work, and travels with the message as an `acemq-routing-slip`
header. Each service does its part and sends the message to the next stop.

What it costs: no single place says what the whole route is at runtime, so a
route that is wrong is discovered one hop at a time. Worth it when the steps
vary per message, and not worth it when every message goes the same way — a
fixed chain of consumers is simpler and easier to follow.

Completed steps are **carried rather than dropped**, so a slip that fails half
way says how far it got, which is exactly what whoever finds it in a dead-letter
queue is asking:

```ruby
slip = Patterns::RoutingSlip.from(message.envelope)
slip.done.map(&:to_s)   # => ["validate", "charge"]
slip.next_step          # => the step that was about to run
slip.finished?
slip.to_s               # => "RoutingSlip[done: validate -> charge | next: ship]"
```

Each completed step is stamped with the time it finished. Building a slip
mutates and returns self, so the chained `step` calls above read the way they
look; `advance` returns a **copy**, because by then the slip is on a message and
a message that changed under a handler is one nothing can reason about.

The message is accepted only once the next one is out, so a failure to publish
retries the step — which is why a step that changes anything should be
[idempotent](#idempotency). A slip that will not parse is fatal rather than
retried: it will not parse next time either.

## Pipelines and middleware

```ruby
mq.consume("orders.new", &Patterns.chain(
  ->(message) { place(message.payload) },
  Patterns.with_logging { |line| logger.info(line) },
  Patterns.with_timeout(10),
  Patterns.with_idempotency(store)
))
```

The order reads **outside-in**: the first middleware named is the outermost, so
logging here records what the timeout and the idempotency guard decided.

`with_timeout` **reports** an overrun rather than interrupting one, and reports
it as a retry whatever the handler said about itself. Retrying work that may
have succeeded risks doing it twice, and accepting work that may have failed
loses it, and only the first of those is a problem you can solve. Ruby's
`Timeout.timeout` would interrupt, by raising inside whatever line the handler
happened to be on, which can leave a transaction half-written; and the message
is held until the handler returns either way, so there is nothing to gain.

`with_ordering(key)` is [ordering](#ordering) as a middleware, for when it
belongs in a chain rather than around a lone handler.

There is **no `with_recovery`**. An exception is the ordinary failure channel in
Ruby and the consumer already turns one into a retry; a wrapper that rejected on
every exception would send a database outage straight to the dead-letter queue.

Chaining services into a pipeline:

```ruby
mq.consume("orders.new", &Patterns.then_publish(mq, to: "shipment.requested",
                                                exchange: "shipping-events") do |message|
  message.payload["digital"] ? nil : { "order_id" => message.payload["order_id"] }
end)
```

Returning `nil` publishes nothing and accepts the message, which is how a step
says "this one does not continue" without inventing an empty message for the
next service to work out how to ignore. The correlation goes forward and the
causation records what produced what.

## Schemas

```ruby
registry = Patterns::InMemorySchemaRegistry.new
schema = registry.register("order.placed", "avro", definition)

registry.latest("order.placed")
registry.by_id(schema.id)
registry.versions("order.placed")
```

A producer and a consumer have to agree about what a message means, and they are
deployed on different afternoons. A registry lets the message carry a small
identifier instead of its whole shape.

Registering the **same definition twice returns the same identifier** rather than
making a second version — otherwise a service that registers on every start adds
a version per restart. The fingerprint is SHA-256 of the exact bytes, so two
definitions differing only in whitespace count as different: normalising would
need a parser per format, and a registry that quietly treated two definitions as
one because it mis-parsed them would be worse than a strict one.

A lookup that finds nothing raises `SchemaNotFound` rather than returning an
empty definition.

`InMemorySchemaRegistry` is for tests and for seeing the shape of the thing —
nothing is shared between processes, which is the entire point of a registry.
**Nothing here puts anything on the wire yet**: which header carries a schema
identifier is a cross-language contract, and one invented here would be one the
other AceMQ libraries could not read.

## Streams

```ruby
Patterns.declare_stream(mq, "events", max_age: 7 * 24 * 3600, max_bytes: 10 * 1024**3)

Patterns.read_stream(mq, "events", offset: Patterns::StreamOffset.first,
                     prefetch: 100, name: "projection-1") do |message|
  project(message.payload)
  Ack.accept
end
```

A stream does not remove a message when somebody reads it, so several consumers
read the same stream independently and a new one can start from the beginning.

Positions:

```ruby
Patterns::StreamOffset.first          # everything the stream still has
Patterns::StreamOffset.next           # the default: only what arrives from now
Patterns::StreamOffset.last           # the last chunk
Patterns::StreamOffset.at(4_211)      # an exact offset
Patterns::StreamOffset.since(Time.now - 3600)
```

**Acknowledging does not remove the message** — it advances this consumer's
position, so restarting from `next` carries on rather than re-reading. Rejecting
does not dead-letter it either, because there is nothing to remove it from: a
message that cannot be handled has to be dealt with by the handler, and the
stream moves on regardless. Nothing is lost, and nothing is retried for you.

That is why `read_stream` uses `RetryPolicy.none` whatever the connection
carries. A retry republishes, and republishing onto a stream **appends a second
copy** rather than redelivering the first, so a projection reading it would see
the message twice.

`name:` identifies this consumer to the broker, which is what makes server-side
offset tracking possible. `prefetch:` is always set — RabbitMQ refuses a stream
consumer with no prefetch at all, and the error it gives does not explain why.

Retention is **unbounded by default**, which for a stream means until the disk is
full — a mistake an ordinary queue cannot make. Set at least one of `max_age:`
or `max_bytes:` on anything that will run for long.

## Next

- [Consuming](consuming.md) — every pattern here hands back something `consume`
  takes
- [Reliability](reliability.md) — the retry engine the patterns deliberately do
  not replace
- [Interceptors](interceptors.md) — the cross-cutting version of a pipeline
- [Testing without a broker](testing.md) — all ten are testable in-process
