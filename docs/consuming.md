# Consuming

```ruby
consumer = mq.consume("orders.new") do |message|
  warehouse.reserve(message.payload)
  Ack.accept
end
```

`consume` subscribes and returns straight away. The block runs on the
transport's threads, not on the one that called it, so a script that only calls
`consume` and then falls off the end of the file exits without reading anything.

The queue must already exist. The subscription declares it passively, so a typo
in the name is a `TransportError` when the consumer starts rather than a silent
queue nothing ever writes to. Declare it with a [topology](topology.md) first.

## The message

| | |
|---|---|
| `payload` | the decoded object |
| `envelope` | identity, type, correlation, attempt, origin — see [the envelope](envelope.md) |
| `body` | the raw bytes the codec was given |
| `content_type` | what the producer said the bytes were |
| `routing_key` | what it arrived on |
| `redelivered?` | the broker's flag: it has offered this to somebody before |
| `id`, `attempt` | forwarded from the envelope, because they are what a handler asks about |

`body` is there for the case that actually happens: two languages disagreeing
about a payload, and somebody needing the bytes rather than the object the codec
made of them.

`redelivered?` is the broker's opinion and `envelope.attempt` is this library's.
They are not the same thing — a redelivery after a consumer crash has the same
attempt number, because the count travels on the message and a crashed consumer
never republished it. Use `attempt` for "how many times has this been tried",
and `redelivered?` for "might I have half-done this already".

## What a handler returns

An `Ack`, and it has to be one of four:

```ruby
Ack.accept                         # done
Ack.retry("the warehouse said no") # try again, if the policy allows
Ack.reject("no such SKU")          # never going to work; dead-letter it
Ack.park("schema version 9")       # nothing here can read it; park it
```

`Ack.park` puts the message in `{queue}.parked` rather than `{queue}.dlq`. Use it
where the handler already knows the message is unreadable — a version this
service was never taught, a field that is not a date where a date has to be. The
dead-letter queue holds messages that were tried and failed; the parking queue
holds messages that were never going to work, and mixing the two means somebody
sorts them out by hand after an outage. It is counted as `acemq.messages.parked`
and its span carries `messaging.acemq.outcome = "parked"`.

A handler that raises is treated as `Ack.retry`. Raising `FatalError` is treated
as `Ack.reject`:

```ruby
mq.consume("orders.new") do |message|
  raise FatalError, "no such SKU" unless catalogue.key?(message.payload["sku"])
  warehouse.reserve(message.payload)   # a database outage raises, and retries
  Ack.accept
end
```

That is the split worth internalising. An exception is the ordinary failure
channel in Ruby, and almost all of them are transient — a connection reset, a
timeout, a deadlock — so retrying is the right default. `FatalError` is how a
handler says "no number of attempts fixes this" without having to know how many
are left.

Returning something that is not an `Ack` is a bug the consumer will not paper
over.

## Concurrency and prefetch

```ruby
mq.consume("orders.new", concurrency: 8, prefetch: 32) { |message| ... }
```

`concurrency:` is how many messages this consumer works on at once, and it is
**one by default**. That default is not timidity: a queue delivers in order, and
a consumer above one stops honouring that. Raising it trades order for
throughput, which is usually the right trade and occasionally a correctness bug
— an "order cancelled" overtaking the "order placed" it cancels. Where that
matters, keep the concurrency and add [ordering](patterns.md#ordering), which
buys order per key while keeping concurrency across keys.

`prefetch:` is how many unacknowledged messages the broker will hand over,
twenty by default from the connection. It wants to be at least the concurrency —
a prefetch of one with a concurrency of eight is seven idle threads — and not so
high that one instance hoards a queue while another sits empty.

**Concurrency, or several consumers?** `concurrency:` runs several handlers on
one consumer and one channel. A [consumer group](patterns.md#consumer-groups)
runs several consumers, each with its own channel and prefetch. Reach for the
group when handlers are slow enough that one channel's prefetch is the limit, or
when a fair share across processes matters: the broker round-robins between
consumers, so four here compete evenly with four in another instance where one
consumer with concurrency four would not.

## Decoding

The connection's codec decodes the body, and `codec:` overrides it for one
consumer:

```ruby
mq.consume("thumbnails", codec: BytesCodec.new) { |message| ... }
```

**A body no codec can read goes to `{queue}.parked`**, not to `{queue}.dlq`. A
message that failed five times and a message nothing could read are different
problems, and mixing them means somebody sorts them by hand later. It is counted
as `acemq.messages.parked`, the same counter a handler's own `Ack.park` raises.
See [codecs](serialization.md) for how a composite
codec reads several content types, and [reliability](reliability.md) for what
else lands where.

## Retries

Each consumer uses the connection's retry policy unless given its own:

```ruby
policy = RetryPolicy.exponential(6, 10, 300)

mq.consume("orders.new", retry_policy: policy) { |message| ... }
```

The policy has to match the one the [topology](topology.md) declared the queue
with, because the rungs a long delay waits in are declared from it. See
[retries, dead letters and shutdown](reliability.md), which is the whole of that
story.

## Stopping

```ruby
consumer.cancel                # stop delivery, wait up to 30s for handlers
consumer.cancel(timeout: 5)
consumer.running?              # is the subscription still open
consumer.in_flight             # messages being worked on right now
```

`cancel` stops the subscription first and *then* waits for handlers that are
still running, so nothing new arrives while it drains. Closing the connection
does the same for every consumer on it:

```ruby
at_exit { mq.close }
```

`close` stops every consumer even when one of them refuses, closes the socket
either way, and raises the first refusal afterwards. Stopping at the first
failure would leave the rest running and the socket open, so a shutdown that
went slightly wrong would become a process that will not exit.

A message being handled when a process is killed without any of this is not
lost — it was never acknowledged, so the broker offers it again. It is just
handled twice, which is why anything that changes something wants to be
[idempotent](patterns.md#idempotency).

## Reading one message

Not everything is a subscription. A tool that makes one pass over a queue needs
a beginning and an end:

```ruby
while (delivery = mq.pull("orders.new.dlq"))
  puts Envelope.from_headers(delivery.headers, delivery.routing_key).error
  delivery.ack
end
```

`pull` returns `nil` when the queue is empty, and the delivery comes back
**unacknowledged** — settling it is yours to do. That is deliberate: a message
left unacknowledged is held by the broker rather than lost, so a tool that dies
half way through a pass returns everything it was holding. It is what
[replay](patterns.md#replay) is built on.

## Next

- [Retries, dead letters and shutdown](reliability.md) — where a delay is
  waited, and what happens when the attempts run out
- [Patterns](patterns.md) — idempotency, ordering, consumer groups, pipelines
- [Interceptors](interceptors.md) — running something around every handler
- [Metrics, tracing and health](observability.md) — what a consumer counts
