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
