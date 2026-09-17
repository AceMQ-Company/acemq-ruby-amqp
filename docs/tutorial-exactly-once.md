# Tutorial 3 — Never processing twice

[Tutorial 2](tutorial-surviving-failure.md) built a consumer that retries. Every
retry is a chance to do the work twice, and this tutorial is about the two halves
of that problem: a message handled more than once, and a message published for
work that never happened. About twenty-five minutes.

## The thing to understand first

**There is no exactly-once delivery over a message broker, and anything that says
otherwise is selling you at-least-once with a good story.** The acknowledgement
travels over the same network the message did, so between your handler finishing
and the broker hearing about it there is a window, and a process that dies in
that window leaves a message that will be delivered again. No library closes
that. Java, .NET, Go, Python and Ruby all have this page and all of them say the
same thing.

What you can have is **exactly-once *effect***: the work happens once even when
the message arrives twice. That is a property of your handler and your database,
not of the broker, and everything below is about making it easy to get right.

## Step 1 — See the duplicate

```ruby
require "acemq/amqp"

include AceMQ::AMQP

mq = Connection.open("amqp://guest:guest@localhost:5672")
Topology.new.queue("payments").apply(mq)

charged = []

mq.consume("payments") do |message|
  charged << message.payload["order_id"]
  puts "charged #{message.payload["order_id"]} (#{charged.size} so far)"
  Ack.accept
end

envelope = Envelope.new(type: "payment.requested.v1")
2.times { mq.publish({ "order_id" => "A-1", "amount" => 4200 }, to: "payments", envelope: envelope) }
```

```
charged A-1 (1 so far)
charged A-1 (2 so far)
```

The same envelope published twice is the same message id twice, which is what a
redelivery looks like from inside a handler. You have charged somebody
twice.

Building the `Envelope` yourself is how this tutorial forces the duplicate. In
production you do not have to arrange it: a retry, a replay off the dead-letter
queue, a consumer that crashed after the work and before the acknowledgement, and
an outbox relay that published before it could mark the row — all four produce
exactly this.

## Step 2 — Remember what you have handled

```ruby
require "acemq/amqp/patterns"

store = Patterns::InMemoryIdempotencyStore.new(window: 6 * 3600)

mq.consume("payments", &Patterns.idempotent(store) do |message|
  charged << message.payload["order_id"]
  puts "charged #{message.payload["order_id"]} (#{charged.size} so far)"
  Ack.accept
end)
```

```
charged A-1 (1 so far)
```

`Patterns.idempotent` wraps a handler and hands back a handler, which is why it
goes in with `&`. Every pattern in this library that wraps a handler has that
shape, so they compose without any of them knowing about the others.

Two things about it are decisions rather than details.

**A duplicate is accepted, not rejected.** The work was done, so the message has
been handled; dead-lettering it would raise an alarm about something that went
right.

**A handler that does not accept forgets the key**, so its retry can actually
run. Remembering a message that then failed would mean the retry silently does
nothing, which is the worst of both — the work did not happen and the guard says
it did. A handler that raises is treated the same way: the key is forgotten
before the exception reaches the retry engine.

### Keying on something other than the message id

The default key is `envelope.id`. When the thing that must not happen twice is in
the payload, say so:

```ruby
Patterns.idempotent(store, key: ->(m) { "charge:#{m.payload["order_id"]}" }) do |message|
  …
end
```

That is the version you want when two *different* messages both refer to one
order and charging for either one twice is the problem. A key function that
returns nothing is rejected as fatal rather than retried — it will produce the
same nothing next time, and a guard that cannot key a message is not guarding it.

## Step 3 — Make it survive more than one process

`InMemoryIdempotencyStore` is a Hash with a time window. It deduplicates within
one worker, and the moment there are three workers behind one queue the
redelivery lands on a different one, finds an empty hash, and charges again. It
also forgets everything when the process restarts, which is precisely when
redeliveries happen.

```ruby
require "sqlite3"

db = SQLite3::Database.new("payments.db")
store = Patterns::SQLIdempotencyStore.new(connection: db)
store.create_schema      # development and tests; a migration owns this in production
```

Everything else stays as it was — `Patterns.idempotent(store)` does not care
which store it has.

**This gem still declares no runtime dependencies.** There is no driver in here;
there is a seam three methods wide, and `SQL.connect` recognises what it is handed
by the methods the object answers rather than by its class. A `SQLite3::Database`
and a `PG::Connection` both work as they are, and wrapping a Sequel database or an
ActiveRecord connection is a dozen lines of the same shape. SQLite and PostgreSQL
are what has actually been run; everything else is "should work", which is a
different claim.

**The single handle above is a tutorial's shortcut, not a pattern.** Your handler
runs on the transport's threads, so a store built on one `SQLite3::Database` is
that connection being used from a consumer thread and from yours at the same time,
which no database driver promises to survive. In production this is a pool, and
`connection:` takes a callable for exactly that shape:

```ruby
Patterns::SQLIdempotencyStore.new(connection: -> { Thread.current[:db] })
```

What it must never be is something that opens a fresh connection each time — see
[the connection is yours](patterns.md#the-connection-is-yours).

### Three states, not two

The shared store has a failure the Hash does not, and the extra state is what
handles it. A consumer that takes the key and then dies has left a key claimed by
nobody, and a store with only "seen" and "not seen" either blocks that message for
ever or lets a second consumer do the work concurrently.

So `first_time?` takes a **lease** rather than setting a flag, and `confirm`
turns the lease into a permanent record once the handler has accepted. A lease
that expires is a claim whose holder died, and the message can be picked up
again. `confirmed?` is the question "was this really handled", separate from "is
somebody working on it".

`Patterns.idempotent` calls `confirm` when the store answers to it, and does not
when it does not — which is what lets a two-method store stay a two-method store.

## Step 4 — The other duplicate, and the harder one

Deduplicating the *consumer* leaves the *publisher*:

```ruby
db.transaction do
  orders.insert(order)
end

mq.publish(event, to: "order.placed", exchange: "orders-events")   # and if this fails?
```

Two things that can fail independently, and no arrangement of `begin`/`ensure`
closes the gap. Publish first and the message describes work that may not have
happened; commit first and the work may never be announced. Wrapping the publish
inside the transaction is worse than both, because a broker round trip inside an
open transaction holds a database lock across a network.

## Step 5 — The outbox

Write the message in the same transaction as the work, and let something else
publish it:

```ruby
store = Patterns::SQLOutboxStore.new(connection: db)
store.create_schema

db.transaction do
  orders.insert(order)
  store.add(Patterns.record(mq, event, to: "order.placed", exchange: "orders-events",
                            type: "order.placed.v2"),
            connection: db)
end
# roll this back and the message is not in the outbox either

relay = Patterns::OutboxRelay.new(mq, store, interval: 1)
relay.start
at_exit { relay.close }
```

`Patterns.record` builds the envelope by the same rules `publish` uses and with
this connection's origin, so a message that went through the outbox is
indistinguishable on the wire from one that did not. The outbox is a delivery
mechanism, not a different kind of message.

`connection: db` on `add` is the whole reason `SQLOutboxStore` exists. A store
that opened its own connection would have exactly the flaw it was built to fix,
while looking as though it had been dealt with: the insert would commit on its
own, and a business write that rolled back afterwards would leave a message queued
for something that never happened. The store neither commits your transaction nor
closes it.

`relay.sweep` publishes one batch and returns how many went out, which is what a
test calls instead of waiting for a tick — and what an application calls to flush
its outbox at the end of a request rather than up to an interval later.

### What the outbox gives you, and what it does not

**It is deliberately at-least-once.** A record is removed only once the broker has
confirmed the message, so a crash in between publishes it again. The alternative —
removing first — loses messages instead, and a lost message is the worse of the
two: a duplicate can be recognised, an absence cannot.

Which is why `Patterns.idempotent` is in the same library. The outbox guarantees
*at least one* publish for every committed write; the idempotency store makes more
than one harmless. **Neither is useful without the other**, and together they are
as close to exactly once as a broker gets.

Give the relay an `on_error:`. A relay whose sweeps are all failing is an outbox
filling up, and without one the only symptom is messages that never arrive:

```ruby
Patterns::OutboxRelay.new(mq, store, on_error: lambda { |error, exchange:, routing_key:|
  logger.warn("outbox stuck on #{exchange}/#{routing_key}: #{error.message}")
})
```

The destination is in there because "the outbox cannot reach `orders-events`" is
actionable in a way that "a sweep failed" is not.

## Step 6 — Order of operations

Put together, one handler:

```ruby
mq.consume("payments", &Patterns.idempotent(store) do |message|
  db.transaction do
    payments.insert(message.payload)                       # the work
    outbox.add(Patterns.record(mq, receipt, to: "payment.taken",
                               exchange: "billing-events"),
               connection: db)                             # and what to say about it
  end
  Ack.accept
end)
```

The order matters and this is the order:

1. The idempotency guard runs **outside** the transaction. A duplicate is turned
   away before a transaction is opened, which is the cheap path and the common one.
2. The work and the outgoing message commit **together**. Either both happened or
   neither did.
3. The key is confirmed only after the handler has accepted, so a failure
   anywhere inside leaves the key forgotten and the retry able to run.
4. The relay publishes afterwards, on its own thread, from committed rows.

## Why this is still not exactly once

Look at what is left. Between the transaction committing and `Ack.accept`
reaching the broker, the process can die. The message comes back, the idempotency
guard finds the key **confirmed**, and accepts it without running the handler —
so the work does not happen twice. Good.

But the guard's own record is a row in a database, and it was written by
`confirm` *after* the transaction, not inside it. A crash between the two leaves
work that happened and a key that was never confirmed, and the redelivery runs
the handler again.

Closing that last gap means writing the idempotency key in the same transaction
as the work — which no library can do on your behalf, because it does not know
your transaction. `first_time?`, `confirm` and `forget` are public for exactly
this reason: a handler that wants the guarantee calls them itself, inside its own
`db.transaction`, and gets exactly-once effect at the price of writing four more
lines.

Everything above that point is the library making the common case correct. This
last step is the one you have to want.

## What to watch in production

| | |
|---|---|
| the outbox's `pending_count` | rising means the relay is not keeping up, or not running |
| `acemq.consume.total{queue,outcome="acked"}` | against how many rows your handler actually wrote — the gap is duplicates being turned away |
| the relay's `on_error:` | anything at all here is messages not being published |
| `store.size` / `purge_expired` | a store nobody prunes is a table that grows for ever |

## Next

[Tutorial 4 — Seeing what happens](tutorial-observability.md), which is how you
find out any of this is going wrong.

Or, from the guide: [patterns](patterns.md#idempotency) for the idempotency store
and the outbox in full, and [the SQL-backed
stores](patterns.md#sql-backed-stores) for the seam they sit on.
