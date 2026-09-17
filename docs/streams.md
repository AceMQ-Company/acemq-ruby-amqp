# Streams

A stream is a queue that keeps what it has delivered. Reading one does not empty
it: every consumer holds its own position, the same message can be read again
tomorrow by somebody else, and a new consumer can start from the beginning.

```ruby
require "acemq/amqp/patterns"

Patterns.declare_stream(mq, "orders.log", max_age: 7 * 24 * 3600,
                        max_bytes: 10 * 1024**3)

Patterns.read_stream(mq, "orders.log", offset: Patterns::StreamOffset.first,
                     prefetch: 100, name: "projection-1") do |message|
  projection.apply(message.payload)
  Ack.accept
end
```

That reads like a queue with better retention. It is not, and the differences are
silent rather than loud — nothing raises, the code looks right, and the
projection is wrong.

## Declaring one

`declare_stream` sets three things that are not yours to choose:

```ruby
connection.declare_queue(name, durable: true, auto_delete: false, exclusive: false,
                               arguments: { "x-queue-type" => "stream", … })
```

A stream is durable and can be neither exclusive nor auto-deleting. Those are
fixed here rather than left to fail at the broker. Declare one by hand with the
wrong flags and you get the library's own refusal, which at least says what is
wrong:

```
AceMQ::AMQP::QueueTypeError: queue "orders.log" cannot be a stream queue while
it is transient and auto-delete: RabbitMQ only replicates …
```

It takes a `Connection` or a `Transport`, so a topology tool with no connection
object in hand can still declare one.

### Retention

```ruby
Patterns.declare_stream(mq, "orders.log",
                        max_age: 7 * 24 * 3600,        # x-max-age                       => "7D"
                        max_bytes: 10 * 1024**3,       # x-max-length-bytes              => 10737418240
                        segment_bytes: 100 * 1024**2)  # x-stream-max-segment-size-bytes => 104857600
```

All three are `nil` by default, which is legal and almost always a mistake. **A
stream with no limit grows until the disk is full**, and a full disk is a
broker-wide alarm that blocks every publisher on the node. Set at least one on
anything that will run for long. This is a mistake an ordinary queue cannot make,
which is exactly why it is easy to make here.

`max_age:` is seconds, and is rendered as the unit-suffixed duration RabbitMQ
wants rather than a bare number — `90` becomes `"90s"`, `36 * 3600` becomes
`"36h"`, `7 * 24 * 3600` becomes `"7D"`. The broker rejects a bare integer, and
the largest whole unit is chosen so the argument on the queue reads the way the
person who set it was thinking.

`segment_bytes:` is how large each file on disk gets, and it matters beyond
tidiness: **retention happens a whole segment at a time**. Nothing is discarded
until an entire segment can be, so the segment size is the granularity of every
other retention setting. A stream told to keep an hour, in segments large enough
to hold a day, keeps a day.

**It is absent unless you ask.** The broker has a default of its own, tuned for
its storage rather than for any particular stream, and nothing is sent unless a
size is named. A declaration carrying an argument that the *first* declaration of
the same stream did not is a `PRECONDITION_FAILED` for whoever declares second,
so a default invented here would break every stream first declared by a Java, Go,
.NET or Python service. All five leave it out, and the argument is spelled
`x-stream-max-segment-size-bytes` in all five.

## Where to start reading

```ruby
Patterns::StreamOffset.first                      # the oldest message still held
Patterns::StreamOffset.next                       # only what arrives from now on
Patterns::StreamOffset.last                       # the last chunk, roughly "recently"
Patterns::StreamOffset.at(4_211)                  # an exact position
Patterns::StreamOffset.since(Time.now - 3600)     # the first message at or after a time
```

A value object rather than a bare string, because two of the five are not
strings — an exact offset is an integer and a timestamp is a `Time` — and a
caller passing the wrong one gets an error from the broker that does not mention
streams. It is frozen on construction, so one can be built at start-up and shared.

`first` means the oldest message the stream **still holds**. A stream has a
retention policy, and its oldest message is not necessarily the first one ever
written — a projection rebuilt from `first` against a stream that has been
discarding for a month is a projection missing a month, and nothing anywhere says
so.

A reader that says nothing reads from `next`. That is right for a new consumer
joining a live system and wrong for a projection: one built without an explicit
`first` silently skips its own history and looks healthy while being wrong. State
the position rather than inheriting it.

## What acknowledging means here

**Acknowledging does not remove the message.** A stream keeps everything until
retention discards it. What an acknowledgement does is advance *this consumer's*
position, so a consumer restarted from `next` carries on rather than re-reading.

**Rejecting does not dead-letter it**, because there is nothing to remove it
from. A message that cannot be handled has to be dealt with by the handler —
logged, copied to another queue, counted — and the stream moves on regardless.
That is the trade a stream makes: nothing is lost, and nothing is retried for you.

Which is why `read_stream` starts its consumer with `RetryPolicy.none` whatever
the connection carries:

```ruby
retry_policy: options.delete(:retry_policy) || RetryPolicy.none
```

A retry in this library republishes the message. Republishing onto a stream
**appends a second copy** rather than redelivering the first, so a projection
reading that stream would see the message twice and a retry ladder would fill the
stream with its own attempts. The `retry_policy:` keyword is still accepted and
still honoured if you pass one, because refusing it would be the library deciding
it knows your stream better than you do — but passing one is almost always the
bug described in this paragraph.

Everything else you can pass to [`consume`](consuming.md) goes through, including
`concurrency:`, which on a stream costs you the ordering that is usually the
reason for having one.

## Resuming where you left off

The broker does not remember your position for you, and this is the part where
Ruby has less than Java does. There is no `last_handled_offset` on the consumer
and no checkpoint store in the library. What there is, is the offset the broker
stamps on every delivery:

```ruby
checkpoint = store.load("projection-1")

Patterns.read_stream(mq, "orders.log", name: "projection-1",
                     offset: checkpoint ? Patterns::StreamOffset.at(checkpoint + 1)
                                        : Patterns::StreamOffset.first) do |message|
  projection.apply(message.payload)
  store.save("projection-1", message.envelope.headers["x-stream-offset"])
  Ack.accept
end
```

`x-stream-offset` is set by the broker and is not in the reserved `x-acemq-`
namespace, so it arrives as an ordinary application header and reaches the
handler untouched. It is an `Integer`, and it counts from **0** for the first
message in the stream — so resume from one *more* than the offset you saved.

Saving the checkpoint in the same database transaction as the projection's own
writes is what makes the pair exactly once. Anywhere else — including the shape
above, where the save and the `Ack.accept` are two separate things — is
at-least-once, which is fine when the handler is
[idempotent](patterns.md#idempotency) and quietly wrong when it is not.

`name:` is passed to the broker as the consumer tag, which is what makes
server-side offset tracking possible at all. Give every stream consumer a stable
one; a generated tag is a consumer the broker cannot recognise after a restart.

## Prefetch

A stream consumer always has one, because RabbitMQ refuses one without:

```
AceMQ::AMQP::TransportError: cannot consume from "orders.log":
PRECONDITION_FAILED - consumer prefetch count is not set for stream queue 'orders.log' in vhost '/'
```

That is what `prefetch: 0` gets you — clear enough once you have seen it, and not
at all obvious the first time, which is why `read_stream` sets one whatever
happens. Prefetch is the only backpressure a stream has: there is no queue depth
to push back with, because nothing is being removed.

```ruby
Patterns.read_stream(mq, "orders.log", prefetch: 500) { |message| … }
```

The default is **10** — `Patterns::DEFAULT_STREAM_PREFETCH` — which is lower than
the 20 an ordinary consumer gets here.

### The number is this library's choice, not the contract

**The default is not part of the cross-language contract, and the five libraries
deliberately disagree about it:**

| Library | Default stream prefetch |
|---|---|
| Java | 100 |
| .NET | 100 |
| Go | 10 |
| Python | 10 |
| Ruby | 10 |

Nothing is wrong with either number. Prefetch is a trade of **memory against
throughput** — how many undelivered messages a consumer is willing to hold in
order to avoid waiting on a round trip for each one — and the right answer
depends on payload size and handler speed, both of which are properties of your
application rather than of the protocol. A library picking a different default
does not make a stream written by one unreadable by another: the offset, the
retention arguments and the message on the wire are the contract, and prefetch is
a consumer-side setting that never leaves the channel.

So do not read across from another AceMQ service and expect the same number, and
do not treat a difference between two languages here as a bug to be filed. **If
the value matters to you, state it** — the same advice as the reading position:

```ruby
Patterns.read_stream(mq, "orders.log",
                     offset: Patterns::StreamOffset.first,
                     prefetch: 50,        # say it rather than inherit it
                     name: "projection-1") { |message| … }
```

Large payloads want a smaller number, because prefetch multiplies them in memory.
A fast handler on small messages wants a larger one, because the round trip
starts to dominate — a projection catching up on a year of history wants
hundreds. The number to raise it to is the one at which this process still has
memory for everything it is holding unacknowledged.

## What a stream cannot do

A stream never removes a message, and nearly every failure-handling tool in this
library is built on moving one:

| On a queue | On a stream |
|---|---|
| The [retry ladder](reliability.md#where-the-delay-is-waited) | **Does not apply.** Nothing can be moved to a rung and back |
| `{queue}.dlq` | **None.** A failed message stays where it is |
| `{queue}.parked` | **None**, for the same reason |
| Requeue | **Nothing to put back.** Acknowledging only advances a position |
| Selective reject | **No.** Positions move forward; they do not skip holes |
| Destructive read | **No.** Every other consumer still sees what you consumed |

So a failing handler has exactly two honest outcomes, and the handler has to pick
one. There is no `skip_failures` flag here as there is in Java — what you do
instead is decide in your own code:

```ruby
Patterns.read_stream(mq, "orders.log", name: "projection-1") do |message|
  projection.apply(message.payload)
  Ack.accept
rescue StandardError => e
  # Skip: record it yourself, because nothing else will.
  metrics.count("projection.skipped", 1)
  logger.error("projection-1 skipped offset #{message.envelope.headers["x-stream-offset"]}: #{e.message}")
  Ack.accept
end
```

Accepting is skipping. The other outcome is to leave a copy somewhere: raising,
or returning `Ack.retry`, runs into `RetryPolicy.none`, which has no attempts
left, so the message is given up on and republished to `orders.log.dlq` with
`x-acemq-error` saying why — see [giving up](reliability.md#giving-up). The
consumer does **not** stop; it advances past the message and carries on. If
nothing declared `orders.log.dlq`, the mandatory republish is returned by the
broker and the delivery is rejected instead, which on a stream means the copy is
simply not made; see
[when a message cannot be set aside](reliability.md#when-a-message-cannot-be-set-aside-at-all).

There is no "stop on the first failure" mode here, and it cannot be had by
cancelling the consumer from inside its own handler — `Consumer#cancel` waits for
in-flight handlers, and the handler doing the cancelling is one of them. Stopping
means recording the offset, setting a flag, and cancelling from the thread that
started the consumer. Which is worth doing for a projection: the message is still
in the stream, and a consumer restarted at the same offset sees it again after
the fix.

**However you skip, nothing else records the gap.** No dead-letter queue holds a
copy unless you arranged one, and no counter moves on its own. The two lines in
that `rescue` are the only evidence there will ever be. Alert on them or the gap
is invisible.

## What you need

Streams need RabbitMQ 3.9 or later, over an ordinary `amqp://` connection — this
library speaks AMQP 0-9-1 to a stream, not the dedicated stream protocol, so
there is nothing extra to install and nothing extra to open. The trade is that
the offset-tracking and batching the stream protocol offers are not available
here; what is available is everything on this page.

**The in-process transports the specs use do not implement streams.** The
loopback transport routes and delivers, but it does not keep messages after
delivery, does not honour `x-stream-offset`, and does not stamp an offset header
— so it can prove that `declare_stream` sends the right arguments and that
`read_stream` subscribes with the right ones, and nothing about stream behaviour.
Stream tests need a real broker; see
[when you do need a broker](testing.md#when-you-do-need-a-broker).

## When to use one

Use a stream when more than one consumer needs the same messages, when history
has to be re-readable, or when a projection has to be rebuildable from scratch:
event sourcing, audit logs, analytics fan-out.

Stay with a queue for work that is done once and then finished. A stream has no
dead-letter queue, no retry ladder and no competing-consumer semantics, and
rebuilding those three on top of one is how a straightforward job turns into a
distributed systems project.

## Related

- [Consuming](consuming.md) — everything `read_stream` passes through
- [Exchanges, queues and bindings](topology.md#queue-types) — why an ordinary
  queue is quorum, and a stream is neither
- [Retries, dead letters and shutdown](reliability.md) — the machinery a stream
  does not have
- [Patterns](patterns.md#streams) — the short version
- [Testing without a broker](testing.md) — and where that stops being possible
