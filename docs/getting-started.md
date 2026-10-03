# Getting started

The [overview](index.md) shows a message going out and coming back in fifteen
lines. This page takes those lines apart, in the order you would write them.

## Before you start

Ruby 3.1 or newer, and a RabbitMQ you can reach. The quickest one:

```bash
docker run --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Then, in your `Gemfile`:

```ruby
gem "acemq-amqp"
gem "bunny", ">= 2.23", "< 4"
```

The gem itself has **no runtime dependencies**. `bunny` is required lazily by
the transport, at the moment a connection is opened, and named in the error if
it is missing — so a process that only builds envelopes or checks a retry
schedule never loads a broker client. That is also why `bunny` goes in your
Gemfile rather than being pulled in for you.

**Which bunny.** The range above resolves to bunny 3.x on Ruby 3.2 and newer and to
2.x on Ruby 3.1. Both are supported and the suite passes on both, and from 0.7.4
**bunny 3.2 or newer is the one to want** if your Ruby allows it.

bunny 3.2 fixed a defect of its own: before it, overlapping recovery attempts could
leave a client permanently disconnected, which is exactly what a node closing every
connection before a restart provokes. Forcing every connection shut every 15 seconds,
ninety times, against a client built on **bunny alone**:

| bunny | threads | published | consumed | state at the end |
|---|---|---|---|---|
| 2.24.0 | 6 → 146 | 18,274 | 13,535 | stopped consuming at the 40th recovery |
| 3.4.0 | 6 → 6 | 174,111 | 174,108 | unaffected |

So bunny 3.x is plainly the better transport. Taking that needed a fix on this side
first, and 0.7.4 carries it.

Before 0.7.4 a subscription left its consumer tag to the broker, which assigns a new
one on every re-subscribe. bunny 3.x records each consumer for topology recovery keyed
by tag, so every recovery recorded the consumer under a new key and the registry grew
— 1, then 2, then 4 — each entry recreating the channel's consumer work pool and
abandoning the one before it. The same standing load reached about 54 threads by its
12th forced recovery. With a tag of our own, 60 forced recoveries end with the thread
count where they started and memory grows 0.13MB per recovery, which is bunny's own
floor measured at 0.12.

On Ruby 3.1 the choice is made for you: only bunny 2.x installs there, so a long-lived
client that reconnects many times will still meet bunny's own recovery defect. Raising
Ruby is the only fix for that, and this library runs on 3.1 so the choice stays yours.

## Opening a connection

```ruby
require "acemq/amqp"

include AceMQ::AMQP

mq = Connection.open("amqp://guest:guest@localhost:5672",
                     origin: "checkout@pod-7",
                     retry_policy: RetryPolicy.exponential(5, 1, 60))
```

`include AceMQ::AMQP` is how the examples here are written. It brings
`Connection`, `Topology`, `Ack`, `RetryPolicy`, `Envelope` and the rest into
scope. Nothing depends on it; write `AceMQ::AMQP::Connection.open` if you would
rather qualify.

Three of the keywords are worth setting from the first line you write:

`origin:` is what every message published on this connection is stamped with,
conventionally `service@host`. Left unset it is worked out from the program
name and the hostname, which is a reasonable guess and not as good as the truth.
When somebody finds a message in a dead-letter queue at three in the morning,
this is the field that says who sent it.

`retry_policy:` is what consumers on this connection use unless they are given
their own. **Without one the default is `RetryPolicy.none`** — one delivery — so
a handler that returns `Ack.retry` against an unconfigured connection
dead-letters the message immediately. That is deliberate: it is a great deal
easier to explain than a message going round the broker as fast as it can be
handed back.

`prefetch:` is how many unacknowledged messages a consumer holds, twenty by
default.

Anything else you pass goes on to the transport, which is where `security:` and
`credentials:` live. See [security](security.md).

## Declaring the shape

```ruby
Topology.new
        .exchange("orders-events", :topic)
        .queue("orders.new", dead_letter: true)
        .binding("orders.new", "orders-events", "order.#")
        .apply(mq)
```

A `Topology` is a description, and `apply` is the only part that talks to a
broker. Building one costs nothing and can be done at load time; applying it is
what a deployment step or a start-up hook does.

`dead_letter: true` is doing more than it looks. It adds `orders.new.dlq`, the
shared `acemq.dlx` exchange and the binding between them, and puts the
dead-letter arguments on `orders.new` itself — three declarations to get one
behaviour right, which is exactly the sort of thing that gets forgotten when it
has to be typed out.

`plan` renders the whole thing as text, without connecting to anything, which is
worth putting in a deployment log:

```ruby
puts Topology.new.exchange("orders-events", :topic)
                 .queue("orders.new", dead_letter: true)
                 .plan
```

[Exchanges, queues and bindings](topology.md) has the rest, including what
happens when two services declare the same queue differently.

## Publishing

```ruby
mq.publish({ "order_id" => "A-1" }, to: "order.placed",
           exchange: "orders-events", type: "order.placed.v2")
```

`to:` is the routing key and `exchange:` is where it goes. Leave the exchange
out and the message goes through the default exchange, which routes by queue
name — handy for a first test, and not what you want between services.

The payload is a Ruby object; the connection's codec turns it into bytes. JSON
unless you said otherwise. See [publishing](publishing.md) and
[codecs](serialization.md).

The keyword arguments after `exchange:` are envelope fields — `type:`,
`correlation_id:`, `causation_id:`, `headers:` and the rest. `publish` returns
the envelope that actually went on the wire, which is what the interceptors left
rather than what you handed in.

## Consuming

```ruby
consumer = mq.consume("orders.new") do |message|
  warehouse.reserve(message.payload)
  Ack.accept
end
```

The block is called on the transport's threads, not the one that called
`consume`, so `consume` returns immediately and the process has to stay alive on
its own account.

A handler must return an `Ack`:

| | |
|---|---|
| `Ack.accept` | done; the broker may forget it |
| `Ack.retry(reason)` | try again, if the policy allows another attempt |
| `Ack.reject(reason)` | do not try again; dead-letter it now |

A handler that raises is treated as `Ack.retry`, except for `FatalError`, which
is treated as `Ack.reject` — raising that is how a handler says "stop now"
without having to know how many attempts are left. An exception is the ordinary
failure channel in Ruby, and a library that made you rescue everything yourself
would get a bare `rescue` in every handler.

`message` carries `payload`, `envelope`, `body`, `content_type` and
`redelivered?`, plus `id` and `attempt` forwarded from the envelope for the two
things a handler asks about most.

[Consuming](consuming.md) has concurrency, prefetch, cancellation and what
happens to a message nothing could decode.

## Stopping

```ruby
at_exit do
  consumer.cancel      # stop taking new messages, wait for the ones in hand
  mq.close
end
```

`cancel` stops the subscription and then waits, up to `timeout:` seconds, for
handlers that are still running. Closing the connection without that is what
turns a deploy into a batch of redeliveries.

## What to read next

- [Publishing](publishing.md) — envelopes, exchanges, confirms
- [Consuming](consuming.md) — concurrency, prefetch, decode failures
- [Retries, dead letters and shutdown](reliability.md) — where a delay is
  waited, and what happens when the attempts run out
- [Testing without a broker](testing.md) — most of this library can be exercised
  with no Docker anywhere
