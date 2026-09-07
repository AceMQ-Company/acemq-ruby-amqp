# AceMQ for Ruby

[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-3.1%2B-CC342D)](#requirements)
[![brokers](https://img.shields.io/badge/broker-RabbitMQ-lightgrey)](#requirements)

A Ruby client for AceMQ messaging over AMQP, speaking the same wire contract as
the [Java](https://github.com/AceMQ-Company/acemq-java-amqp),
[Go](https://github.com/AceMQ-Company/acemq-go-amqp),
[.NET](https://github.com/AceMQ-Company/acemq-dotnet-amqp) and
[Python](https://github.com/AceMQ-Company/acemq-python-amqp) libraries: the same
reserved headers, the same defaults, the same retry arithmetic. A Ruby consumer
reads what a Java producer writes, and the fixtures the Java implementation
produces pin that rather than leaving it to be discovered in production.

> **Status: in build.** The contract layer and the AMQP transport — connect,
> publish, consume, retry, dead-letter — are implemented and tested, against
> the shared fixtures and against a real broker. Nothing is published to
> RubyGems yet.

## What is here today

```ruby
require "acemq/amqp"

include AceMQ::AMQP

mq = Connection.open("amqp://guest:guest@localhost:5672",
                     origin: "checkout@pod-7",
                     retry_policy: RetryPolicy.exponential(5, 1, 60))

Topology.new
        .exchange("orders-events", :topic)
        .queue("orders.new", dead_letter: true)
        .binding("orders.new", "orders-events", "order.#")
        .apply(mq)

mq.publish({ "order_id" => "A-1" }, to: "order.placed",
           exchange: "orders-events", type: "order.placed.v2")

mq.consume("orders.new") do |message|
  message.payload          # => { "order_id" => "A-1" }
  message.envelope.attempt # => which delivery this is
  warehouse.reserve(message.payload) ? Ack.accept : Ack.retry("the warehouse said no")
end
```

The contract on its own, with no broker anywhere:

```ruby
# What travels with a message, and what another language will read back.
envelope = Envelope.new(type: "order.placed.v2", origin: "checkout@pod-7")
envelope.to_headers("order.placed")

# The same schedule Java, Go, .NET and Python produce for the same policy.
policy = RetryPolicy.exponential(5, 1, 60)
policy.schedule                      # => [1.0, 2.0, 4.0, 8.0]

Naming.dead_letter_queue("orders.new")   # => "orders.new.dlq"
```

The gem has **no runtime dependencies**. Reading an AceMQ envelope should not
require installing a broker client, so `bunny` is required lazily by the
transport, at the moment a connection is opened, and named in the error if it
is not there. Add it to your own Gemfile to use the transport:

```ruby
gem "acemq-amqp"
gem "bunny", "~> 2.23"
```

## What is identical, and what is not

**Identical**, because a message crosses languages: the reserved header names
and their types, the defaults applied when they are absent, the retry schedule
arithmetic, the `{queue}.dlq` / `{queue}.parked` / `{queue}.retry.{delay}`
naming, and the rules for giving up.

**Not identical**, deliberately: the API shape. Go gets `ctx`, .NET gets
`IAsyncEnumerable`, Python gets dataclasses, and Ruby gets keyword arguments,
frozen value objects and `?` predicates. Forcing a Java shape onto Ruby
produces a library nobody enjoys using. The contract is portable; the
ergonomics are native.

### The envelope

| Header | |
|---|---|
| `x-acemq-id` | The message identifier, and the default idempotency key |
| `x-acemq-type` | The logical type, falling back to the routing key |
| `x-acemq-version` | Schema version, from 1 |
| `x-acemq-correlation` | Defaults to the id, so a chain has something to copy |
| `x-acemq-causation` | The message that caused this one |
| `x-acemq-attempt` | Delivery attempt, from 1 |
| `x-acemq-first-seen` | Epoch **milliseconds** of the first publish |
| `x-acemq-origin` | `service@host` |
| `x-acemq-error` | Why it was dead-lettered |
| `x-acemq-claim` | Where the payload is, when it is stored outside the message |

Application headers are kept apart from these. A reserved name in your own
headers raises rather than being dropped — silently discarding a header
somebody set is worse than saying no — and unknown `x-acemq-` names from a
newer version of another language's library are not handed back as yours.

### Retry

```ruby
policy = RetryPolicy.exponential(5, 1, 60).give_up_after(6 * 3600)
```

`schedule` shows the delays without jitter, which is what to read when deciding
whether a policy is the one you meant. Jitter moves a delay **both ways**:
one-sided jitter only ever delays, which turns a thundering herd into a slower
thundering herd.

Giving up on **age** as well as attempts is the honest limit when a queue has
been paused — a message can be on attempt one and four days old.

### What the consumer actually does

`Ack.retry` returns the message to the broker with `x-acemq-attempt` advanced.
The count travels **on the message**, because a requeue hands the broker back
the bytes it was given: counting in the consumer instead is wrong the moment a
second one exists, since a message that moves between them is for ever on
attempt one, and a restart forgets everything anyway. The trade is that a
retried message goes to the back of its queue rather than the front.

**Where the delay is waited depends on how long it is.** Under 30 seconds by
default, the consumer waits, holding one prefetch slot. At or above that, the
message is published into a rung queue — `{queue}.retry.{delay}` — whose
`x-message-ttl` is the delay and whose dead-letter target is the queue it came
from, so the broker returns it when the time is up with nothing running:

```ruby
policy = RetryPolicy.exponential(6, 10, 300)   # 10s, 20s, 40s, 80s, 160s

Topology.new
        .queue("orders.new", dead_letter: true, retry_policy: policy)
        .apply(mq)                             # declares .retry.40s, .retry.80s, .retry.160s

mq.consume("orders.new", retry_policy: policy) { |message| ... }
```

The rungs *are* `policy.schedule`, a finite list known before anything is
published, which is why the topology can declare them up front rather than a
consumer discovering them one failure at a time. Hand `queue` the policy and it
works them out; `retry_threshold:` moves the line, on both the topology and the
consumer, and the two have to agree.

A consumer that sleeps through a five-minute backoff loses the whole wait when
it restarts — the broker redelivers at once — which is a correctness bug rather
than a throughput one. Below the threshold, a lost wait costs seconds and a
queue per rung is not worth it. Jitter applies only below the threshold; above
it the spread comes free, because each message's time-to-live starts when it
enters the rung, so a fleet that failed over ten seconds is released over ten
seconds.

Per-message TTL is never used, and it is worth saying why, because it looks like
the flexible answer: RabbitMQ expires messages only from the head of a queue, so
one long wait sitting at the front holds back every shorter one behind it, and
the delays that come out bear no relation to the ones that went in.

When the policy has no attempt left — or the message is older than the policy
allows, or the handler marked the reason `FatalError` — the message is
republished to `{queue}.dlq` with the reason in `x-acemq-error`, and the
original is then acknowledged. Acknowledging a failure looks wrong and is what
makes it reliable: the message is already safely somewhere else, so the original
is a copy that has been dealt with. Rejecting it instead would either requeue it
into a hot loop or hand it to whatever dead-lettering the queue happens to carry
— and neither of those can write down *why*, which is the one thing whoever
finds it needs.

A body no codec can read goes to `{queue}.parked` rather than `{queue}.dlq`. A
message that failed five times and a message nothing could read are different
problems, and mixing them means somebody sorts them by hand.

Without a policy, `Connection` uses `RetryPolicy.none` — one delivery — so a
retry against an unconfigured connection dead-letters immediately. That is a
great deal easier to explain than a message going round the broker as fast as it
can be handed back.

## Patterns

The things everybody writes on top of a message queue, written once. Required
separately, because none of it is protocol and the core stays the contract and
the transport:

```ruby
require "acemq/amqp/patterns"
```

A pattern here wraps a handler and hands back a handler. It goes to `consume`
unchanged, so the retry policy, the dead-lettering and the envelope are all
still whatever you configured — a pattern that took over the consumer would have
to reimplement them, and then there would be two retry engines to keep in step.

### Idempotency

```ruby
store = Patterns::InMemoryIdempotencyStore.new(window: 6 * 3600)

mq.consume("orders.new", &Patterns.idempotent(store) do |message|
  warehouse.reserve(message.payload)
  Ack.accept
end)
```

A duplicate is **accepted**, not rejected: the work was done, so the message has
been handled, and dead-lettering it would raise an alarm about something that
went right. A handler that does not accept has its key forgotten, so its retry
can actually run.

`key:` takes the key from the payload instead of the message id, for when two
different messages carry the same order and doing the order twice is the thing
to prevent.

A store is anything answering `first_time?(key)` and `forget(key)`, and
`first_time?` has to be atomic. `InMemoryIdempotencyStore` is right behind one
worker and wrong the moment there are two — each has its own memory, so both are
told they are first. The store worth having is your own database, written in the
same transaction as the work; that is also the only arrangement that closes the
gap between the handler finishing and the acknowledgement reaching the broker,
which is why this is a guard against duplicates rather than exactly-once.

### Outbox

```ruby
store = Patterns::InMemoryOutboxStore.new

db.transaction do
  orders.insert(order)
  store.add(Patterns.record(mq, event, to: "order.placed",
                            exchange: "orders-events", type: "order.placed.v2"))
end

relay = Patterns::OutboxRelay.new(mq, store, interval: 1).start
```

A service that writes to a database and then publishes has two things that can
fail independently. Crash between them and the work is committed with nobody
told; publish first and fail to commit, and the world has been told about
something that did not happen. Writing the message into the same transaction as
the work removes the gap — both commit or neither does — and the relay publishes
what was committed.

A record holds encoded bytes rather than an object, because it outlives the
process that wrote it and the class may not survive the deployment that happens
while it waits. Its envelope is built by the same rules `publish` uses, so a
message that went through the outbox is indistinguishable on the wire from one
that did not.

The relay is deliberately at-least-once: a record is removed only after the
broker has confirmed it, so a crash in between sends it again. Consumers of
anything sent this way need to be idempotent, which is why the pattern above is
in the same library. Removing first would lose messages instead, and an absence
cannot be recognised the way a duplicate can.

`sweep` is public, so an application can flush its outbox at the end of a
request rather than up to an interval later, and a test can drive a relay
without waiting for a tick. A store is anything answering `add`, `pending` and
`mark_published`, and it is only worth having if `add` can join the caller's
transaction — a store that opens its own connection has the gap back, in a
place that looks like it has been dealt with.

### Request and reply

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

A requester is meant to be kept and reused — it holds a queue and a consumer,
so one per request means a queue per request. Without `reply_to:` it generates
an exclusive, transient, auto-deleting queue that goes away with the process; a
reply queue that outlived its requester would collect answers nobody is waiting
for.

The responder's block returns the answer rather than an `Ack`, and raising sends
the failure back to the caller: somebody blocked on a reply should learn that it
failed rather than wait out the timeout. Having answered, the request is settled
rather than retried, because replying and then retrying would answer twice.

A timeout says an answer did not arrive. It says nothing about whether the work
was done, which is why a request that changes anything wants an idempotent
responder.

Two headers carry this, `acemq-reply-to` and `acemq-error`. They are application
headers on purpose: the `x-acemq-` namespace belongs to the engine and is kept
away from what a handler sees, so a responder could never read them if they
lived there.

### Replay

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

Each replayed message is stamped with `acemq-replayed-from`, `acemq-replayed-at`
and `acemq-replay-count`, so a consumer that needs to treat them differently
can and one that does not is unaffected. A message is acknowledged only after
the broker has confirmed the new copy: a crash in that gap replays it twice,
which is the right way round for a dead-letter queue.

Replaying a queue through the default exchange with no routing key is refused
rather than allowed to loop. A dead letter's routing key is the dead-letter
queue, so it would publish every message straight back onto the queue it was
read from, for ever, and the only sign would be a queue that never empties.

### Ordering

```ruby
mq.consume("orders.new", concurrency: 16,
           &Patterns.ordered("x-order-id") { |message| apply(message.payload) })
```

A queue delivers in order and a consumer with concurrency above one stops
honouring that. Usually the right trade; the wrong one where a later message
about the same thing must not overtake an earlier one — an "order cancelled"
arriving before the "order placed" it cancels. This buys ordering per key while
keeping concurrency across keys.

The key is a header name, or anything callable for a key that lives in the
payload; `Patterns.by_correlation` keeps one business action's messages in
sequence. A message whose key comes out empty is handled with no ordering,
because there is nothing to order it against.

**What it does not do**: it orders the handling of messages already delivered.
It cannot reorder ones the broker delivered out of order, and with several
consumers on one queue it orders only within each process. Ordering across
processes is a routing decision — a consistent hash exchange, or a queue per
partition:

```ruby
Patterns.partitioned_routing_key("orders", order_id, 8)   # => "orders.3"
```

`Patterns.partition` is FNV-1a rather than Ruby's own `hash`, and that is the
point: Ruby randomises string hashes per process, so two workers would disagree
about where a key belongs, and so would a Go publisher and a Ruby consumer.

## Requirements

Ruby 3.1 or newer. RabbitMQ, and the `bunny` gem, for the transport.

## Development

```bash
bundle install
bundle exec rspec        # the contract and the retry engine, no broker needed
bundle exec rubocop

# Anything that needs a broker is tagged :integration and skipped unless this
# is set, so a laptop with no Docker still runs everything else.
ACEMQ_TEST_BROKER=amqp://guest:guest@localhost:5672 bundle exec rspec
```

The fixtures under `spec/fixtures/` are produced by the Java implementation and
shared with Go, .NET and Python. They are the definition of "the same wire
contract", and they are checked here rather than assumed.

## Licence

Apache-2.0. RabbitMQ is a trademark of Broadcom Inc.; this project is not
affiliated with it.
