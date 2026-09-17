# Tutorial 4 — Seeing what happens

Three tutorials of machinery, and no way to tell whether any of it is working.
This one ends with a service that answers `/metrics` and `/health`, and a trace
that follows one message from the process that published it into the process that
handled it. About twenty minutes.

## Step 1 — Turn the metrics on

They cost one argument:

```ruby
require "acemq/amqp"

include AceMQ::AMQP

metrics = Telemetry::Registry.new
mq = Connection.open("amqp://guest:guest@localhost:5672", telemetry: metrics)
```

Without `telemetry:` the connection gets `Telemetry::None`, whose methods do
nothing and return `nil`. That is the default because a library that forced a
metrics backend on every process would be a library with a dependency, and this
gem has none.

`Telemetry::Registry` is the one that ships: counters, gauges and timings in
memory, with a Prometheus renderer. It is also what the library's own specs
assert against, which is worth knowing — "what did the retry engine decide" is a
plain equality check rather than a log to read.

Run tutorial 2's failing consumer with it attached and look:

```ruby
pp metrics.counts
```

```ruby
{"acemq.publish.total{exchange=,outcome=confirmed}" => 1,
 "acemq.consume.total{outcome=dead_lettered,queue=orders.new}" => 1,
 "acemq.messages.dead.lettered.total{outcome=dead_lettered,queue=orders.new}" => 1}
```

Everything is already instrumented. There is nothing to add to a handler.

## Step 2 — What the names mean

| | |
|---|---|
| `acemq.publish.total` | by `exchange` and `outcome` — `confirmed`, `unroutable` or `failed` |
| `acemq.consume.total` | by `queue` and `outcome` — **exactly one series per delivery** |
| `acemq.consume.duration` | seconds, by `queue` and `outcome`; the handler and the interceptors together |
| `acemq.consume.attempts` | by `queue`; which go each delivery was, sampled on the way **in** |
| `acemq.consume.in.flight` | a gauge, per queue |
| `acemq.messages.retried.total` | the `outcome=retried` deliveries, under a name of their own |
| `acemq.messages.dead.lettered.total` | likewise for `outcome=dead_lettered` |
| `acemq.messages.set.aside.failed` | by `queue` and `target` |
| `acemq.retry.rung.missing` | by `queue` |

These are Java's names, and Go's, and .NET's, and Python's. A dashboard written
against one service reads the same against the next, whichever language it is in.

Two of them repay a second look.

**`acemq.consume.total` gets exactly one series per delivery**, and the `outcome`
is what the consumer really did — not what the handler asked for. A handler
returning `Ack.retry` on its last attempt is counted `dead_lettered`, because
that is what happened to the message. The naive version increments `retried` on
the way to the dead-letter queue and then counts it again, and a dashboard built
on that says twice as much work happened as did.

**`acemq.consume.attempts` is two numbers in one.** Its sample count is how many
deliveries this consumer was handed, taken *before* the codec and the handler — so
a handler that never returns is still counted, which is the number you want when
the question is "why is this queue not draining". Its distribution is how many
goes those deliveries are taking, so a rising mean is a dependency starting to
struggle well before anything reaches the dead letters.

## Step 3 — Serve them

In a real service this is two routes on whatever already answers HTTP:

```ruby
get("/metrics") { metrics.to_prometheus }
get("/health")  { report = mq.health; [report.up? ? 200 : 503, JSON.dump(report.to_h)] }
```

This tutorial has no web framework, and WEBrick left the standard library in Ruby
3.0, so here it is over a socket — which also makes the point that there is
nothing to it:

```ruby
require "socket"
require "json"

def respond(socket, status, content_type, body)
  socket.print("HTTP/1.1 #{status}\r\nContent-Type: #{content_type}\r\n" \
               "Content-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n#{body}")
end

server = TCPServer.new(9292)

Thread.new do
  loop do
    socket = server.accept
    path = socket.gets.to_s.split[1]
    socket.gets until socket.gets.to_s.strip.empty?    # drain the request headers

    case path
    when "/metrics"
      respond(socket, "200 OK", "text/plain; version=0.0.4", metrics.to_prometheus)
    when "/health"
      report = mq.health
      respond(socket, report.up? ? "200 OK" : "503 Service Unavailable",
              "application/json", JSON.dump(report.to_h))
    else
      respond(socket, "404 Not Found", "text/plain", "")
    end
    socket.close
  end
end
```

```
$ curl -s localhost:9292/metrics
# TYPE acemq_consume_total counter
acemq_consume_total{outcome="acked",queue="orders.new"} 1
# TYPE acemq_publish_total counter
acemq_publish_total{exchange="",outcome="confirmed"} 1
```

`to_prometheus` does the renaming the exposition format requires — dots become
underscores, tags become labels — and nothing else. Every other AceMQ library
serves a metrics path and a health path, so one scrape configuration and one probe
cover a fleet that is not all in the same language.

## Step 4 — Health, and the answer in the middle

```ruby
pp mq.health.to_h
```

```ruby
{"status" => "up",
 "checked" => "2026-09-17T15:09:27Z",
 "parts" => {"consumers" => 1, "consumers_running" => 1,
             "queues" => ["orders.new"], "round_trip_ms" => 8}}
```

Three statuses, and the third is the interesting one:

```ruby
consumer.cancel
report = mq.health
report.status     # => :degraded
report.detail     # => "1 of 1 consumers has stopped"
```

A connection that is open with a consumer that has stopped is **degraded**, not
up and not down. A readiness probe that called that "up" would leave a pod in
service that is not consuming anything; one that called it "down" would have the
orchestrator restart a process whose connection is fine. Degraded is the honest
answer, and what you do with it — 200 or 503 — is a decision about your
deployment rather than about the library.

`running?` is asked of the subscription rather than remembered, because the two
can disagree: a channel closed by the broker stops delivery without anything in
this process being told, and a flag set in `cancel` would report "running" for a
consumer that had been deaf for an hour.

`health` costs a round trip to the broker, which is why it belongs on a probe
rather than in a request handler.

## Step 5 — Follow one message across two processes

Counters say how many messages failed. A trace says which one, and what it was
waiting on.

```ruby
require "acemq/amqp/telemetry/open_telemetry"

tracing = Telemetry::OpenTelemetry.new
tracing.install(mq)
```

That is the whole setup. `install` registers it on both sides of the connection —
it is an [interceptor](interceptors.md) rather than a telemetry observer, because
a span has to know when a publish or a handler starts *and* when it ends.
Everything it does could have been written outside this gem, which is the point of
the seam.

It needs `opentelemetry-api`, required lazily and named in the error if it is
missing, and an SDK configured the way you would configure it for anything else.

Three spans:

| Span | Kind | When |
|---|---|---|
| `<destination> publish` | PRODUCER | a publish |
| `<queue> process` | CONSUMER | a handler running |
| `<destination> request` | CLIENT | a [request/reply](request-reply.md) round trip |

**The join is the point.** The span covering a handler is a child of the span
that published the message, even though the two ran in different processes and
minutes apart, because the trace context travels *in the message*:

```
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

`traceparent` and `tracestate` are the W3C names and are deliberately not
`x-acemq-` prefixed: other tooling already knows them, and a private name would
make these traces invisible to everything that did not know to look for ours. The
Java library writes the same two, so a Ruby consumer joins a Java producer's trace
without either side being configured for the other.

On the way in, the parent is read out of **the message's own headers**, not out of
whatever the current thread happened to be doing. Ambient context is the fallback,
not the source — a join across processes and minutes is the entire reason to trace
a message system.

### The one span you have to open yourself

A request/reply round trip is a publish and a wait, and the library cannot tell
that from any other publish. So you say so:

```ruby
quote = tracing.request("price.requests") { prices.call({ "sku" => "X-1" }) }
```

CLIENT rather than PRODUCER, because that span's duration includes somebody
else's work, and a reader who cannot tell the two apart cannot tell a slow broker
from a slow responder. It closes with `answered`, `timed_out` or `failed` —
and **`timed_out` is not marked as an error**, deliberately. A reply that did not
arrive in time is usually a responder's queue being long rather than a failure of
this process, and a trace view that fills with red for slow responders stops
meaning anything. The exception still reaches you.

## Step 6 — Reading it when something is wrong

A queue is not draining. In order:

1. **`acemq.consume.in.flight`** — at the prefetch ceiling means handlers are
   running and slow; at zero means nothing is being delivered, which is a
   subscription or a binding, not a handler.
2. **`acemq.consume.attempts`, sample count** — rising while nothing is acked
   means deliveries are arriving and not finishing. Its mean rising means they
   are finishing, badly, several times each.
3. **`acemq.consume.duration{outcome}`** — the outcome tag is on it because a p99
   that mixes work which succeeded with work which failed is a number about
   neither.
4. **`acemq.messages.retried.total` against `...dead.lettered.total`** — retries
   without dead letters is a dependency that recovers; both rising together is
   one that does not.
5. **The trace**, for the message itself. By this point you know which queue and
   roughly when, which is enough to find one.

And two that mean your *topology* is wrong rather than your handlers:

- **`acemq.retry.rung.missing`** — the ladder is not on the broker, so every long
  wait is happening in memory and a restart loses it.
- **`acemq.messages.set.aside.failed`** — a dead letter that could not be
  written. The message was rejected rather than left unsettled, so the delivery
  ended once instead of being redelivered for ever, and this counter is the only
  sign the path leaves.

Those two are [the two worth an alert](observability.md#the-two-worth-an-alert),
because nothing else shows them and neither will wake anybody on its own.

## Step 7 — What it costs, and what is still invisible

The registry is a Hash behind a mutex: a counter is a rendered key string, a
lookup and an add, and a timing keeps a count, a sum, a minimum and a maximum
rather than a histogram. Percentiles are the thing you give up for that, which is
the trade a Prometheus scrape was going to make anyway.

An observer that raises does not take a message with it. Everything in the library
reports through `Telemetry::Reporter`, which wraps whatever you pass, swallows
what it raises, and warns **once per metric** — an observer that has been raising
since a deploy is worth knowing about, and the same line ten thousand times a
second is not. A metrics backend that is full, slow or misconfigured is a problem
for whoever runs the metrics; turning it into undelivered messages would make
observability the thing that caused the outage.

What is **not** instrumented, so you know not to look for it:

- **Request/reply has no counters of its own.** No `timed_out`, no `unmatched`,
  no `answered`, no `unanswerable` — Java has all four and Ruby has none. What
  you get is the ordinary publish and consume series under the queues involved,
  plus whatever `tracing.request` records. See [what is not
  counted](request-reply.md#what-is-not-counted).
- **Streams** are consumed through the ordinary consumer, so they are counted
  like any other queue. There is no metric for a consumer's offset or for how far
  behind the end of a stream it is.
- **The outbox relay** reports through its own `on_error:` callback rather than
  through a counter.

## What you have

A service that publishes, consumes, retries, dead-letters, deduplicates and says
so. That is the whole of what these four tutorials set out to build.

## Next

The guide, which goes deeper on every part of it:

- [Metrics, tracing and health](observability.md) — this tutorial's subject in
  full, including the Prometheus rendering rules and how to combine health checks
- [Interceptors](interceptors.md) — the seam the tracing adapter is built on, and
  how to add a tenant, a log scope or a size limit to every message
- [Request and reply](request-reply.md) and [streams](streams.md) — the two
  subjects the tutorials do not cover
- [Testing without a broker](testing.md) — asserting on all of the above without
  Docker
