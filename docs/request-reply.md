# Request and reply

```ruby
require "acemq/amqp/patterns"

Topology.new.queue("price.requests", dead_letter: true).apply(mq)

responder = Patterns.serve(mq, "price.requests") do |message|
  { "price" => catalogue.price(message.payload["sku"]) }   # the answer, not an Ack
end

prices = Patterns::Requester.new(mq, to: "price.requests", timeout: 5)
prices.call({ "sku" => "X-1" })   # => { "price" => 1299 }
prices.close
```

Two objects. `Patterns.serve` answers requests on a queue and hands back a
`Patterns::Responder`; `Patterns::Requester` asks and waits. Everything else on
this page is about what waiting costs and what the waiting is allowed to
conclude.

That first line is not optional. **`Patterns.serve` does not declare the request
queue** — it is `consume` underneath, and a subscription
[declares its queue passively](consuming.md), so a queue that is not there is a
`TransportError` reading `NOT_FOUND - no queue 'price.requests' in vhost '/'`
rather than a queue quietly created with whatever arguments this process felt
like. The requester declares its own *reply* queue, because that one is nobody
else's business; the request queue belongs to a [topology](topology.md).

## Read this first

Messaging is asynchronous, and request-reply is a synchronous shape drawn on top
of it. That is a real cost rather than a free convenience. A caller blocked on a
reply is holding a Ruby thread, a connection and a deadline, and a responder's
queue that backs up turns into a caller that stops responding — the queue depth
is somewhere else, so the service that is failing is not the service anybody is
looking at.

If the two sides can speak HTTP or gRPC to each other, they should. Those tools
have timeouts, retries, load balancing, circuit breakers and health checks that
a messaging library will not match, and every operator already knows how to read
them.

What this is genuinely for:

- the far end is reachable **only** through the broker — no HTTP endpoint,
  behind a firewall, on a network you do not control;
- one worker among many, where the broker is already doing the load balancing
  and you would otherwise be building a load balancer;
- a system that is already message-driven, where an HTTP hop would add a second
  failure domain to something that currently has one.

Those cases are real, and doing them by hand means reply queues, correlation
ids, and a timeout somebody always forgets.

## It blocks, and there is no future

`Requester#call` blocks the calling thread until the reply arrives or the
deadline passes. There is no `call_async`, no promise and no callback, and that
is a deliberate difference from Java's `requestAsync`: Ruby has no future in the
standard library that the library could hand back without picking a concurrency
gem for you, and a pattern helper is the wrong place to make that choice for a
whole application.

What Ruby has instead is threads, and they compose the way you would expect:

```ruby
answers = skus.map { |sku| Thread.new { prices.call({ "sku" => sku }) } }.map(&:value)
```

One `Requester` is safe to call from several threads at once. It keeps a hash of
who is waiting for what, guarded by a mutex, and each caller waits on a condition
variable of its own — so a slow answer to one request does not hold up anybody
else's.

That condition variable is there rather than a `Thread::Queue` because
`Queue#pop(timeout:)` arrived in Ruby 3.2 and this library's floor is 3.1. It is
waited in a loop: a condition variable may wake for no reason at all, and a
spurious wake read as a reply would be a `nil` payload handed to a caller.

## How it works on the wire

A request names its reply queue **twice** — AMQP's own `reply-to` property and an
`acemq-reply-to` application header, always the same value — and carries a
`correlation_id`. The responder publishes the answer to that queue with the same
correlation id, and the requester matches it to the caller waiting for it.

A responder reads **the header first and the property second**. That order is the
contract in all five libraries. The header is preferred because it is the half
that survives a hop that rebuilds the message — a retry rung, a dead-letter, a
shovel — where the property does not: a service that reads a message and
publishes a new one keeps the headers and usually drops the properties.

Writing both is what makes a Ruby responder able to answer a Java requester and
the other way round. The two halves of the family did not start out agreeing,
and a requester that wrote only one of them went unanswered by half the fleet.

Neither name is in the reserved `x-acemq-` namespace, and that is deliberate
rather than an oversight. That namespace belongs to the engine and is kept away
from what a handler sees — [the envelope](envelope.md) strips it — so a
responder could never read `acemq-reply-to` if it lived in there. `acemq-error`,
the header that carries a responder's failure home, is an application header for
the same reason.

```ruby
Patterns.reply_address(message)   # => the header, or the property, or ""
message.reply_to                  # => the native AMQP property alone
```

Both are readable from an ordinary `consume` handler, which is how you write a
responder that this library did not build for you.

## The reply queue

Without `reply_to:`, a requester generates one:

```ruby
prices = Patterns::Requester.new(mq, to: "price.requests")
prices.reply_queue   # => "acemq-reply-1f8c…"
```

It is declared classic, transient, auto-deleting and **exclusive**, and every one
of those words is doing work. Exclusive ties the queue to this connection, so the
broker deletes it when the process dies — which is the orphaned-queue problem
solved without a timer. Java sets `x-expires` for the same reason; Ruby does not
need to, because an exclusive queue cannot outlive the connection that made it.

Classic, because it could be nothing else. RabbitMQ refuses an exclusive or
auto-delete quorum queue outright, so this is the one place the library's
[quorum default](topology.md#queue-types) would have declared something the
broker will not accept. It is written down in `declare_reply_queue` rather than
left to fail, because the broker's refusal does not mention reply queues and
reads like a bug in your own code.

A **named** reply queue is left to that default, so it comes out durable and
quorum:

```ruby
prices = Patterns::Requester.new(mq, to: "price.requests", reply_to: "price.replies")
```

Which is what makes it safe to name a queue your [topology](topology.md) also
declares: both declarations then say the same thing, and the second is accepted
rather than refused with `PRECONDITION_FAILED`. Name one only when replies have
to survive a restart; otherwise the generated one is the right shape, because a
reply queue that outlives its requester collects answers nobody is waiting for.

**Build one requester and keep it.** It holds a queue and a running consumer, so
one per request is a queue per request, and a broker with ten thousand
auto-delete queues on it is a real way to run out of memory with nothing
obviously wrong.

### The reply consumer has no retry policy

Whatever the connection's `retry_policy` is, the consumer reading the reply queue
is started with `RetryPolicy.none`:

```ruby
@consumer = connection.consume(@reply_queue, retry_policy: RetryPolicy.none) { … }
```

That handler cannot meaningfully fail — it hands a message to a waiter and
accepts — so a policy could only ever add [rung queues](reliability.md) to an
ephemeral reply queue that is about to be deleted. `acemq-reply-1f8c….retry.40s`
is not a queue anybody wants to find on their broker.

## What `call` can raise

```ruby
begin
  prices.call({ "sku" => "X-1" })
rescue AceMQ::AMQP::Patterns::RequestTimedOut => e
  # "no reply to 376c1d3b-… arrived within 5.0 seconds"
rescue AceMQ::AMQP::Patterns::ResponderFailed => e
  # "the responder failed: KeyError: key not found: \"nope\""
end
```

`ResponderFailed` means the far end answered and the answer was a failure. The
message is the responder's exception class and message, carried home in
`acemq-error`. It is a string rather than a marshalled exception on purpose:
the far end may be Java, and an exception class name is the most that can cross
a language boundary honestly.

**`RequestTimedOut` is the absence of an answer, not evidence that nothing
happened.** The request may still be queued, may be running right now, or may
have been handled with the reply lost on the way back. There is no way from here
to tell those apart, and a retry is a decision about whether the responder is
idempotent rather than a reflex. Where the work takes money or sends an email, a
timeout is a question for a human or for the
[idempotency store](patterns.md#idempotency), not for a loop.

The correlation id is generated inside `call` rather than taken from the caller,
which is why you cannot set one. A caller reusing a correlation id across two
requests would have them answer each other.

A reply that arrives after its caller gave up is **dropped**. The reply consumer
looks up the waiter, finds nobody, and moves on rather than blocking — blocking
there would stall every other caller waiting on the same queue. Nothing counts
that drop, which is the one piece of instrumentation this library does not have
and Java does; see [what is still not
counted](#what-is-still-not-counted) below.

## Writing a responder

```ruby
responder = Patterns.serve(mq, "price.requests") do |message|
  { "price" => catalogue.price(message.payload["sku"]) }
end
```

The block returns **the answer rather than an `Ack`**, which is the one place
this library's handlers do not look like every other handler in it. A responder's
job is to answer, and what to do with the request afterwards follows from whether
it could:

| The block | The caller gets | The request is |
|---|---|---|
| returns a value | that value | accepted |
| raises | `ResponderFailed` | rejected, and dead-lettered |
| returns, but the reply will not publish | nothing — it times out | retried |

Raising sends the failure back before settling, because somebody blocked on a
reply should learn that it failed rather than wait out their timeout. Then the
request is **rejected rather than retried**: replying and then retrying would
answer the same caller twice.

The third row is the one worth sitting with. If the handler succeeded and the
reply publish failed, the work is done and the answer did not get out, so the
message is retried — and the work runs again. That is the concrete reason a
responder that changes anything should be idempotent, and it is not hypothetical:
a broker that drops a connection between the handler returning and the reply
being confirmed produces exactly this.

A request that names **no reply address at all** is rejected with a `FatalError`
and dead-lettered rather than retried, because retrying cannot make a reply queue
appear. Anything arriving on `price.requests.dlq` with that error is a caller
using `publish` where it meant to use a requester.

### Options go through to `consume`

`Patterns.serve` passes everything it does not recognise to
[`Connection#consume`](consuming.md), so the whole of that method is available:

```ruby
Patterns.serve(mq, "price.requests", prefetch: 10, concurrency: 4) do |message|
  { "price" => catalogue.price(message.payload["sku"]) }
end
```

**A responder handles one request at a time by default**, and every caller behind
a slow one is waiting. `concurrency:` is the first setting to reach for when
request-reply feels slow, and it is the same trade as anywhere else: throughput
in exchange for the order the broker offered the messages in, which a responder
almost never cares about.

## Shutting down

```ruby
prices.close      # stops consuming replies; the generated queue goes with it
responder.cancel  # stops delivery, then waits for handlers already running
```

`Responder#cancel` waits up to 30 seconds for in-flight handlers by default — it
is the consumer's own `cancel`, forwarded. That wait matters more for a responder
than for an ordinary consumer: a request being answered right now has somebody
blocked on the other side, and dropping it turns their call into a timeout they
will not be able to explain. A responder forwards `running?` and `queue` too, and
`responder.consumer` is the consumer itself for anything else.

`mq.close` cancels every consumer on the connection, requesters' reply consumers
included, so a process that closes its connection at exit does not need to
remember each one. Closing a requester twice is harmless.

## What is counted

### The round trip, from the caller's side

```ruby
metrics = Telemetry::Registry.new
mq = AceMQ::AMQP::Connection.open(url, telemetry: metrics)
```

Every `call` raises two series, tagged with `routing.key`, `message.type` and
`outcome`:

| | |
|---|---|
| `acemq.request.total` | round trips, by outcome — `answered`, `timed_out` or `failed` |
| `acemq.request.duration` | seconds, the same tags; **how long the caller waited** |

The duration is the number nothing else can give you. The publish is already
counted and so is the responder's delivery, but the caller's wait spans two
queues, two processes and somebody else's work, and no single message's metrics
see all of it. Set your timeouts from this and from
`acemq.consume.duration` on the responder's queue.

`timed_out` is kept apart from `failed` on purpose: it says a reply did not
arrive in time, not that anything went wrong. The request may still be queued,
still being handled, or long since done with the reply lost on the way back —
which is why resending is only safe against an idempotent responder. A duration
distribution for `timed_out` sitting exactly on the deadline is a deadline set
too short, not a broken responder.

The label names carry Java's and .NET's dots so that one dashboard panel covers
all of them. Prometheus does not allow a dot in a label name — a single bad line
makes the whole scrape unparseable — so `to_prometheus` renders them as
`routing_key` and `message_type`, which is what Go writes and what a dashboard
will be asking for.

### The two numbers a responder keeps

```ruby
responder = Patterns.serve(mq, "price.requests") { |m| price(m.payload) }

responder.answered      # requests answered, counted before each reply left
responder.unanswerable  # requests that named nowhere to reply
```

The same two numbers Java's `Responder.answered()` and `unanswerable()` report,
with the same promise about when they may be read: **never a wait**. Both exist
before the responder subscribes, so a request the broker hands over during
start-up — what a queue with a backlog looks like from in here — is counted like
any other. Code that sleeps before reading one is working around a defect that is
not here.

`answered` is incremented **before** the reply is published. The other order
reads more naturally and is wrong: it leaves a window in which the reply is in
the caller's hands and the responder still says nothing has been answered, which
is a dashboard reporting an idle service that is demonstrably working. A publish
that fails hands its increment back, so this counts replies that were *sent*
rather than replies that were attempted.

A responder that raised is counted in `answered` here, and this is the one place
Ruby's number differs from Java's. A Ruby responder answers that request — the
failure goes back in `acemq-error` and the caller raises `ResponderFailed` rather
than waiting out its deadline — and a reply that was sent is a request that was
answered. Java's responder does not reply at all in that case and so does not
count it; the divergence is in what the two *do* with a failure, not in what the
counter means. Split the two apart with `acemq.consume.total`, where the same
delivery is `acked` or `rejected`.

`unanswerable` above zero means a caller is using `publish` where it meant to use
a requester. Nothing can answer such a request, and nothing about redelivering it
would make a reply address appear, so it is dead-lettered once rather than
looped.

### And the ordinary metrics

A requester and a responder are still built out of `publish` and `consume`, so
they are counted by those too, under the queues they use:

| | |
|---|---|
| `acemq.publish.total{outcome="confirmed"}` | requests going out, and replies going back |
| `acemq.consume.total{queue="price.requests",outcome="acked"}` | requests answered |
| `acemq.consume.total{queue="price.requests",outcome="rejected"}` | requests that raised, or that named nowhere to reply |
| `acemq.consume.duration{queue="price.requests"}` | how long answering takes — the responder's half of a caller's timeout |

See [metrics, tracing and health](observability.md) for the registry and the
Prometheus rendering.

Tracing has a span shape made for this. The OpenTelemetry adapter does not know
when you are making a request, because `call` is publish-then-wait like anything
else, so you say so:

```ruby
tracing = Telemetry::OpenTelemetry.new.install(mq)

quote = tracing.request("price.requests") { prices.call({ "sku" => "X-1" }) }
```

That opens a `CLIENT` span — client rather than producer, because a reader has to
know this span's duration includes somebody else's work — and closes it with
`answered`, `timed_out` or `failed`. `timed_out` is deliberately **not** marked
as an error: a reply that did not arrive in time is usually a responder's queue
being long rather than a failure of this process, and a trace view that fills
with red for slow responders stops meaning anything. The exception still reaches
you, and you are free to decide it was one.

### What is still not counted

Java's `Requester` also exposes `unmatched()`: replies that arrived with nobody
waiting. **Ruby has no such number.** A reply to a request that already gave up
is dropped silently, and that count rising alongside timeouts is the signature of
a responder slower than its callers expect — nothing broken, the timeout simply
wrong. `acemq.request.total{outcome="timed_out"}` is most of that signal now, and
reading it against `acemq.consume.duration` on the responder's queue is the rest.

## Related

- [Publishing](publishing.md) and [consuming](consuming.md) — what a requester
  and a responder are built out of
- [Patterns](patterns.md#request-and-reply) — the short version, alongside the
  other twelve
- [Retries, dead letters and shutdown](reliability.md) — where a rejected
  request goes
- [Idempotency](patterns.md#idempotency) — what decides whether a timeout may be
  retried
- [Metrics, tracing and health](observability.md)
