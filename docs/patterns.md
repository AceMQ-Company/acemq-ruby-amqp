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
| [Claim check](#the-claim-check) | keep a large payload off the broker |
| [Request and reply](#request-and-reply) | ask a question and wait for the answer |
| [Replay](#replay) | put dead letters back, once the fix is out |
| [Ordering](#ordering) | keep some messages in order without serialising all of them |
| [Consumer groups](#consumer-groups) | start a set of workers together, and stop them together |
| [Routing slips](#routing-slips) | let the message carry its own itinerary |
| [Sagas](#sagas) | undo the steps that worked when a later one does not |
| [Scheduling](#scheduling) | deliver a message later, without a scheduler |
| [Pipelines](#pipelines-and-middleware) | wrap a handler; chain one service to the next |
| [Schemas](#schemas) | remember what a message used to look like |
| [Streams](#streams) | a queue that keeps what it has delivered |
| [SQL-backed stores](#sql-backed-stores) | the three of those, in a database that outlives the process |

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
**`first_time?` has to be atomic** — it both asks and claims. A store that also
answers `confirm(key)` has it called after a handler accepts, which is what a
store whose rows outlive the process needs; see
[the idempotency store](#the-idempotency-store-a-hold-is-a-lease).

`InMemoryIdempotencyStore` is right behind one worker and wrong the moment there
are two: each has its own memory, so both are told they are first. Its `window:`
is how long a key is remembered, and it sweeps as it goes rather than growing
without limit. [`SQLIdempotencyStore`](#the-idempotency-store-a-hold-is-a-lease)
is the shared one.

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
in a place that looks like it has been dealt with. `InMemoryOutboxStore` does
not close that gap and says so;
[`SQLOutboxStore`](#the-outbox-add-takes-your-connection) is the one that does.

## The claim check

```ruby
store = Patterns::FilesystemClaimCheckStore.new("/mnt/claims")
checked = Patterns::ClaimCheckCodec.wrapping(JSONCodec.new, store)

mq = Connection.open(url, codec: checked)
mq.publish(document, to: "document.stored")   # 40 MB does not go near the broker
```

A scanned medical report is tens of megabytes. Putting it on a queue is possible
and is a mistake: it fills the broker's memory, it is copied to every bound
queue, it makes a dead-letter queue impossible to inspect, and it turns a broker
into a filesystem with worse tools. What travels instead is a **claim check** —
the payload goes to a store, and the message carries the key.

**Only when it is worth it.** Below the threshold the payload travels inline,
exactly as it would without this codec. Offloading a two-hundred-byte message
turns one broker round trip into a store round trip *and* a broker round trip,
so an unconditional claim check makes the common case slower to fix the rare
one. The default threshold is `ClaimCheckCodec::DEFAULT_THRESHOLD`, 64 KiB —
comfortably above an ordinary event and comfortably below the size at which a
broker starts to care.

```ruby
Patterns::ClaimCheckCodec.wrapping(JSONCodec.new, store, threshold: 0)   # offload everything
```

### What is on the wire

```
0xAC  0x01  0x00  payload   inline, and identical to what the delegate wrote
0xAC  0x01  0x01  key       a claim check
```

Three bytes, and **the third is how a consumer decides** whether it is holding a
payload or a reference to one. The framing rather than a header carries that,
because a header can be stripped by a shovel or a federation link and the body
cannot. What follows the marker on a claim check is the store's key as bare
UTF-8 — not a URI, no scheme, no length prefix — which is what a consumer hands
straight back to its own store.

These are the same three bytes and the same 64 KiB that the Java library writes,
so a Ruby consumer pointed at the same store reads a document a Java publisher
checked in. Get either wrong and the two cannot exchange a large message even
though both "have claim check".

A body with no framing is read as the delegate would read it. That is what makes
it safe to put this codec in front of a queue that already has messages in it,
and to change the threshold afterwards without a flag day.

The **content type is the delegate's**, unchanged. Unlike encryption, where the
bytes really are something else, a claim-checked message is still a document —
it is a document that is somewhere else.

### Reading a key without fetching it

```ruby
Patterns::ClaimCheckCodec.key_of(body)   # the key, or nil for an inline message
```

For the operator looking at a dead-letter queue: which object does this need,
and is it still in the store? Answering that from the message alone is the
difference between a five-minute check and restoring a backup.

The `x-acemq-claim` header on the [envelope](envelope.md) is the application's
own note about where a payload lives; the framing above is what the codec reads
and writes. They are separate, and a codec only ever sees bytes.

### Stores

A store is anything answering `put(content)`, `get(key)` and `delete(key)`.
`get` returns `nil` for a key it no longer holds — that is retention having
expired, not a failure — and the codec turns it into a `DecodeError` that says
so.

| | |
|---|---|
| `Patterns::InMemoryClaimCheckStore` | tests, and nothing else |
| `Patterns::FilesystemClaimCheckStore` | a shared, durable mount |

`InMemoryClaimCheckStore` holds the payloads in the publisher's own memory,
which is where they were going to be anyway — it takes them off the broker and
does nothing else. A consumer in another process gets "the claim check is not in
the store", and a restart loses every payload a queue still refers to.

`FilesystemClaimCheckStore` writes to a temporary file and renames it into
place, so a consumer fast enough to read the key before the writer finished sees
the whole payload or no payload. It is worth having where the filesystem really
is shared and durable — an NFS mount, a persistent volume. On a container's
local disk it is the in-memory store with extra steps. Keys arriving from a
message are **checked rather than trusted** before they become a path segment:
`../../etc/passwd` is a key too, and one that is not a key this store issued
raises a `FatalError` rather than being retried.

Object storage is the usual right answer, and a store in front of S3 is three
short methods.

### Retention is the part that goes wrong

The store and the queue have different lifetimes, and nothing enforces a
relationship between them. A message replayed a month later carries a key, and
if the store expired that key the replay produces a message nobody can read —
**worse than a lost message, because it looks like a message**. The store's
retention has to exceed every retention that could bring a message back: queue
TTLs, dead-letter queues, and however long somebody might sit on a message
before replaying it by hand.

Nothing calls `delete` for you. Deleting on read would break the second consumer
of the same message and deleting on acknowledgement would break a replay, so
when a payload may be removed is a retention decision, and retention decisions
belong to whoever owns the data.

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

## Sagas

```ruby
booking = Patterns::Saga.named("place-order") do |saga|
  saga.step("take-payment") { |order| payments.charge(order) }
      .compensate_with      { |order| payments.refund(order) }
  saga.step("reserve-stock") { |order| inventory.reserve(order) }
      .compensate_with       { |order| inventory.release(order) }
  saga.step("book-courier") { |order| couriers.book(order) }
end

result = booking.run(order)
```

A sequence of steps where each one knows how to undo itself. If `book-courier`
raises, the stock is released and the payment refunded, **in that order**, and
the result says so. Reverse order because that is the order the world was
changed in, and a compensation often depends on state a later step has not yet
altered.

Nothing here touches a broker: no message is published and no header is set. It
is in `patterns` because the work a saga sequences is usually the work a message
asked for.

**A step with no compensation is skipped, not an error.** `book-courier` above
has none — a step that only read something, or one whose effect is harmless,
needs no undoing. There is no warning for the step that should have had one,
because a library cannot tell the two apart, which is the argument for writing
the compensation first and the action second.

**It returns a result rather than raising.** A failed saga is not an exceptional
condition to a caller that has to decide what happens next:

```ruby
result.complete?     # every step ran
result.compensated?  # a step failed and the earlier ones were undone
result.failed_at     # "book-courier"
result.failure       # what it raised
result.completed     # ["take-payment", "reserve-stock"], in order
result.unresolved    # the steps that could not be undone
result.unresolved?   # the flag to alert on
```

**When a compensation itself fails, the remaining ones still run.** It is
collected into `unresolved` and the next compensation is attempted anyway,
because stopping leaves more undone than continuing does. `unresolved?` is the
thing to alert on: everything else a saga reports is recoverable by
construction, and these are real-world effects that happened, were meant to be
undone, and were not. No retry will resolve them — a person has to. Java spells
it `hasUnresolved()`; the `has_` is dropped here because Ruby says the same
thing with the question mark.

The result is frozen, because it describes something that has already happened.

### What a saga is not

**Not a distributed transaction.** Nothing is isolated: after `take-payment` the
customer's money really has moved, and anybody looking sees that it has. If
`book-courier` then fails, the refund is a *new* fact rather than an erasure of
the old one, and for a few seconds the world contained a charge that should not
have happened. That is what compensating a real-world action means, and a saga
is honest about it where a two-phase commit pretends otherwise.

So the steps must be things that can be undone by doing something else. Sending
an email cannot be compensated — the apology is a second email, not an unsend —
and a step that sends one should be the last step, after everything that can
still fail.

**Not durable.** This runs in one process and its state is on the stack. A crash
midway leaves the saga half-applied with nothing to resume it. Where a saga must
survive the process, the steps have to be messages and the state has to be in a
database, which is a much larger thing and is not this. For most systems the
in-process form is the right one: it turns "remember to undo the three things
you already did" from a comment into something the code can see.

Anything that is not a `StandardError` — an `Interrupt`, a `SignalException` —
goes straight through and nothing is compensated. The process is going away, and
a compensation running on the way out of a SIGTERM is one nobody can be sure
finished.

## Scheduling

```ruby
Patterns::Scheduler.on(mq) do |scheduler|
  scheduler.in(4 * 3600, invoice, to: "invoice.due", exchange: "billing")
  scheduler.at(renewal_date, policy, to: "policy.renew", exchange: "policies")
end
```

Delivering a message later, with no scheduler process and no plugin. `in` takes
seconds and `at` takes a `Time`; anything already due is delivered at once.

### Why not a per-message time to live

The obvious implementation is to set `expiration` on the message, drop it in a
queue nobody consumes, and let it dead-letter to its destination. It is what
most articles suggest and it is wrong for anything but a single fixed delay,
because **a classic queue expires messages only at its head**.

Put a four-hour message in, then a one-minute message behind it, and the
one-minute message is delivered in four hours. Nothing reports this: the queue
looks healthy, the message is not lost, it is simply late by a factor nobody
predicted. It fails in production under mixed load rather than in testing under
uniform load.

### What it does instead

A ladder of queues, each with a *uniform* time to live, and a message hops
through them until it is due:

```
acemq.schedule.1h   ttl 1h  -> acemq.schedule -> acemq.schedule.due
acemq.schedule.10m  ttl 10m -> acemq.schedule -> acemq.schedule.due
acemq.schedule.1m   ttl 1m  -> acemq.schedule -> acemq.schedule.due
acemq.schedule.10s  ttl 10s -> acemq.schedule -> acemq.schedule.due
acemq.schedule.1s   ttl 1s  -> acemq.schedule -> acemq.schedule.due
```

Every message in a rung has the same delay, so head-of-line expiry is harmless:
the head is always the message due soonest. Each expiry returns the message to
`acemq.schedule.due`, where the scheduler either delivers it or puts it in the
largest rung that does not overshoot. A four-hour delay is four one-hour hops; a
ninety-second delay is one minute and then three tens. A one-day message costs
twenty-four hops and a one-minute message costs one, which is the right way
round — short delays are common and want to be cheap.

The cost is honest and worth stating: a long delay is several broker round trips
rather than one, and delivery is accurate to about the smallest rung rather than
to the second. A scheduler that must fire at 09:00:00.000 exactly is a
scheduler, not a message broker. The alternative is RabbitMQ's
delayed-message-exchange plugin, which does this properly and is a plugin — so
it is not available everywhere, and a library that silently required it would be
a library that works on your laptop.

### The names are the contract

Every name, argument and header is shared with the Java, Go, .NET and Python
libraries, because a Ruby service and a Java service scheduling on one broker
declare the same queues. A rung already there with a different table answers
`PRECONDITION_FAILED` to whichever declares second, and a header spelled
differently is a message that reaches the control queue and cannot be routed.

| | |
|---|---|
| `acemq.schedule` | direct, durable |
| `acemq.schedule.{1h,10m,1m,10s,1s}` | classic, durable, `x-message-ttl` = the rung, dead-lettering to `acemq.schedule` / `acemq.schedule.due` |
| `acemq.schedule.due` | classic, durable, no arguments |

Every queue is bound to `acemq.schedule` under its own name. The four headers a
scheduled message carries are `x-schedule-exchange`, `x-schedule-routing-key`,
`x-schedule-due-at` (epoch milliseconds, the integer `x-acemq-first-seen` is
also written as) and `x-schedule-content-type`. They deliberately do **not** use
the `x-acemq-` prefix: that one is reserved, and the envelope refuses an
application header carrying it outright.

`Patterns::Scheduler.declare(mq)` declares all of it without starting a
consumer, for a deployment that applies its topology up front.

### What it will not do

**It does not decode your payload.** The payload is encoded once, when it is
scheduled, and carried as bytes from then on; the content type travels with it
in `x-schedule-content-type` and is put back on the message that is finally
delivered. A scheduler that decoded would acquire opinions about message formats
it has no business having, and one that republished under
`application/octet-stream` would deliver the right bytes to a consumer that
cannot read them.

**It does not pass its own headers on.** What arrives at the target is a
message, not a message about scheduling.

**Its control queue has no dead-letter queue.** Every consumer this library
starts declares `{queue}.dlq` and `{queue}.parked`; the scheduler's control
consumer is subscribed on the transport instead, precisely so that it does not.
Two queues in every deployment that nothing writes to and nobody reads is a poor
trade for a queue whose messages the scheduler put there itself. A message that
reaches `acemq.schedule.due` without the headers a scheduled message carries is
dropped and counted in `malformed`, which is not zero only when something other
than a scheduler is publishing into `acemq.schedule`.

```ruby
scheduler.scheduled   # messages handed to it
scheduler.delivered   # messages that reached their destination
scheduler.hops        # times a message moved between rungs; / delivered is the average
scheduler.malformed   # messages in the control queue that no scheduler put there
```

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
[`SQLSchemaRegistry`](#the-schema-registry-identifiers-that-survive-a-restart)
is the one whose identifiers survive a restart.
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

## SQL-backed stores

The idempotency store, the outbox and the schema registry each shipped with an
in-memory implementation and a comment saying why it is not the one to use. This
is the other one.

```ruby
store = Patterns::SQLOutboxStore.new(connection: db)
store.create_schema            # development and tests only

db.transaction do
  orders.insert(order)
  store.add(Patterns.record(mq, event, to: "order.placed"), connection: db)
end
```

| | |
|---|---|
| `Patterns::SQLOutboxStore` | `add`, `pending`, `mark_published`, `mark_failed`, `pending_count`, `purge_published` |
| `Patterns::SQLIdempotencyStore` | `first_time?`, `confirm`, `forget`, `confirmed?`, `purge_expired` |
| `Patterns::SQLSchemaRegistry` | `register`, `by_id`, `latest`, `versions` |

### The connection is yours

**This gem still declares no runtime dependencies.** There is no driver here;
there is a seam three methods wide, and `SQL.connect` recognises what it is
handed by the methods that object answers rather than by its class. A
`SQLite3::Database`, a `PG::Connection`, or anything answering
`run(sql, params)`, `placeholder(index)` and `constraint_violation?(error)` all
work, and wrapping a pool, a Sequel database or an ActiveRecord connection is a
dozen lines of the same shape.

Statements are written with `?` and rendered per driver, so nothing here builds
SQL out of anything a caller supplied except a table name — which is validated
as a plain identifier, because no database binds one as a parameter.

Times are stored as fixed-width ISO-8601 UTC, so that a string comparison in the
database orders instants the way instants are ordered. There is no timestamp
type spelled the same way in SQLite and PostgreSQL and no portable way to bind a
`Time`; this is understood by both.

**What has actually been run.** SQLite, by the ordinary specs — no environment
needed, the sqlite3 gem is a development dependency. PostgreSQL, by
`spec/integration/postgres_spec.rb`, which runs when `ACEMQ_TEST_POSTGRES` names
one:

```sh
export ACEMQ_TEST_POSTGRES="postgres://user:pass@localhost:5432/acemq_test"
bundle exec rspec
```

Nothing else has been exercised. MySQL, SQL Server and everything else are
"should work, has not been run", and that is a different claim.

### The outbox: `add` takes your connection

This is the whole reason the class exists. `InMemoryOutboxStore` is criticised in
its own comment for not sharing a transaction with anybody's database, and **a
store that opened its own connection would have exactly that flaw while looking
as though it had been dealt with**: the insert would commit on its own, and a
business write that rolled back afterwards would leave a message queued for
something that never happened.

So the insert goes on the connection you hand in, inside the transaction you
opened, and the store neither commits it nor closes it. The message becomes
durable exactly when the work does:

```ruby
db.transaction do
  orders.insert(order)
  store.add(record, connection: db)
end
# roll this back and the message is not in the outbox either
```

`connection:` may also be given once, at construction, as a callable — the shape
a framework that binds a connection per request wants:

```ruby
Patterns::SQLOutboxStore.new(connection: -> { Thread.current[:db] }, relay: relay_pool)
```

What it must never be is something that opens a fresh connection.

`relay:` is where the relay's own work goes — claiming, marking, counting — and
it runs on a background thread with no ambient transaction. Under a connection
pool it has to be a different connection from the one a request is using. With
one connection, the same one for both is correct and is the default.

**Claiming is a lease, not a lock.** A held lock lasts as long as its
transaction, so a relay that dies mid-batch either strands its rows or holds a
transaction open across a network publish. A lease is a timestamp: it expires on
its own, however the holder died. The claim is taken in two statements and
decided by the update's row count, never by the select — two relays that select
the same candidates both try to update them, and the loser matches nothing.

`pending` therefore *claims* rather than merely reading, and what comes back is
what this call won. `mark_failed` counts the attempt and frees the lease, so a
record nothing can publish eventually stops being claimed and stays for somebody
to look at rather than being tried for ever. `OutboxRelay` calls it for you when
the store answers to it.

A body is stored as text. One that is not text — anything the
[claim check](#the-claim-check) or a binary codec produced — is stored base64
with a column saying so, because PostgreSQL refuses invalid UTF-8 in a text
column and losing the message to a driver error would be the worse trade.

### The idempotency store: a hold is a lease

`SQLIdempotencyStore` is the one `InMemoryIdempotencyStore` cannot replace. An
in-process store deduplicates within one worker, and the moment there are three
behind one queue the redelivery lands on a different one, finds an empty hash,
and does the work again.

A shared store has a failure the in-memory one does not: a consumer that takes a
key and then dies mid-handler leaves its row behind, and naively every
redelivery of that message, for ever, is discarded as a duplicate — a crash that
should have cost one retry silently deleting a message instead. So a key is held
under a **lease** that expires after `claim_timeout:`. Too short and two
consumers work on the same message at once; too long and a crashed consumer
stalls that message for the duration. It should comfortably exceed the slowest
handler.

That is why the store answers a third method. `Patterns.idempotent` calls
`confirm` after a handler accepts, on any store that has one:

```
first_time?(key)  # take it, under a lease
confirm(key)      # the work is done: hold it for retention, not for the lease
forget(key)       # the work failed: give the hold up so a retry can run
```

A store with no `confirm` — the in-memory one — is simply never asked, because a
crash wipes it and a key it holds is a key somebody is working on now.
Confirmations are kept for `retention:` and then forgotten; a duplicate arriving
later than that is handled again. Schedule `purge_expired` — hourly is ample.
Nothing on the message path deletes anything, because a store that tidies up on
the hot path makes every message pay for it.

`forget` gives up only this store's own live hold, never another worker's and
never a confirmation: the first would put two consumers on one message and the
second would undo one.

### The schema registry: identifiers that survive a restart

`InMemorySchemaRegistry` hands out fresh identifiers on restart, which makes
every message written before the restart unreadable and does it silently.
`SQLSchemaRegistry` fixes exactly that and nothing more — no compatibility
checking, no versioning interface, no HTTP.

Identifiers come from a counter row rather than from `MAX(id) + 1`: two writers
registering different schemas at the same moment compute the same next
identifier, and the loser cannot win by retrying because the winner is doing the
same arithmetic. Updating one row takes a row lock, so writers queue for an
instant and every one gets a number. Registration is rare enough that
serialising it costs nothing worth measuring.

The same definition registered twice returns the same identifier, from any
process, for ever — a unique index on (subject, fingerprint) is what makes that
true rather than a check-then-insert.

As with the in-memory registry, **nothing here puts anything on the wire**.
Which header carries a schema identifier is a cross-language contract and not
one AceMQ has agreed yet.

### `create_schema` is for development

Each store can make its own tables, and in production they belong in whatever
migration tool already owns the schema — alongside the business tables the
outbox commits with. A library that creates tables at start-up has taken a
decision about when your database changes that is not its to take.

## Next

- [Consuming](consuming.md) — every pattern here hands back something `consume`
  takes
- [Reliability](reliability.md) — the retry engine the patterns deliberately do
  not replace
- [Interceptors](interceptors.md) — the cross-cutting version of a pipeline
- [Testing without a broker](testing.md) — every one of them is testable
  in-process
