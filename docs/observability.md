# Metrics, tracing and health

```ruby
metrics = Telemetry::Registry.new
mq = Connection.open(url, telemetry: metrics)

# whatever already answers HTTP in this process
get("/acemq-metrics") { metrics.to_prometheus }
get("/acemq-health")  { report = mq.health; [report.up? ? 200 : 503, report.to_h.to_json] }
```

Those two paths are the ones every AceMQ library serves, so a scrape
configuration or a probe written for one service works against the next.

## The metrics

The names are shared with Java, Go, .NET and Python, so a dashboard or an alert
written for one reads the same against another:

| | |
|---|---|
| `acemq.messages.published` | by `exchange` |
| `acemq.messages.publish.failed` | by `exchange`, including a publish an interceptor refused |
| `acemq.messages.consumed` | by `queue`, counted on the way **in** |
| `acemq.messages.accepted` / `.retried` / `.rejected` | by `queue` — what handlers decided |
| `acemq.messages.dead.lettered` | out of attempts, too old, or refused fatally |
| `acemq.messages.parked` | nothing could decode it |
| `acemq.handler.duration` | seconds; handler and interceptors together |
| `acemq.messages.in.flight` | a gauge, per queue |
| `acemq.retry.rung.missing` | see [below](#the-one-worth-an-alert) |

`consumed` is counted on the way in rather than on the way out, so a handler that
never returns is still a message this consumer was given — which is the number
you want when the question is why a queue is not draining.

`handler.duration` is the handler **and** the interceptors together. That is the
number worth having: it is how long a message occupied one of this consumer's
prefetch slots, and an interceptor that is slow costs exactly as much as a
handler that is.

## No dependency on a metrics gem

An observer is anything answering three methods:

```ruby
def count(metric, delta = 1, **labels)
def observe(metric, seconds, **labels)
def gauge(metric, value, **labels)
```

Depending on a metrics gem would put every service using this library on the
same one, and that choice belongs to the application. So an adapter is about
fifteen lines:

```ruby
class StatsDObserver
  def count(metric, delta = 1, **labels)  = StatsD.count(metric, delta, tags: tags(labels))
  def observe(metric, seconds, **labels)  = StatsD.timing(metric, seconds, tags: tags(labels))
  def gauge(metric, value, **labels)      = StatsD.gauge(metric, value, tags: tags(labels))

  private

  def tags(labels) = labels.map { |name, value| "#{name}:#{value}" }
end

mq = Connection.open(url, telemetry: StatsDObserver.new)
```

**Anything an observer raises is swallowed** and reported once per metric on
stderr. A metrics backend that is down is not a reason to stop delivering
messages, and a complaint per message would bury the log that says so.

With no `telemetry:` at all, the counters go to a null observer that does
nothing — there is no branch through the hot path asking whether anybody is
watching.

## The built-in registry

`Telemetry::Registry` is a working in-memory implementation, for when the
numbers themselves are what is wanted:

```ruby
metrics = Telemetry::Registry.new
mq = Connection.open(url, telemetry: metrics)

metrics[Telemetry::PUBLISHED, exchange: "orders-events"]   # => 3
metrics.counts                                             # => { "acemq.messages.published{exchange=orders-events}" => 3, … }
metrics.gauges
metrics.timings["acemq.handler.duration{queue=orders.new}"].to_s
# => "12 in 0.481s (mean 0.0401s)"
```

A timing keeps count, sum, min and max rather than a histogram — enough to
answer "is it slower than it was" without storing every observation in a process
that was not asked to be a metrics store.

Labels are flattened into the key and **sorted**, because the same labels given
in a different order have to produce the same key. Otherwise one counter quietly
becomes several and the total is wrong in a way nobody notices.

It is also what makes the counters testable without a broker:

```ruby
expect(metrics[Telemetry::DEAD_LETTERED, queue: "orders.new"]).to eq(1)
```

### to_prometheus

```ruby
puts metrics.to_prometheus
```

```
# TYPE acemq_messages_published counter
acemq_messages_published{exchange="orders-events"} 3
# TYPE acemq_handler_duration summary
acemq_handler_duration_count{queue="orders.new"} 12
acemq_handler_duration_sum{queue="orders.new"} 0.481
# TYPE acemq_handler_duration_min gauge
acemq_handler_duration_min{queue="orders.new"} 0.011
# TYPE acemq_handler_duration_max gauge
acemq_handler_duration_max{queue="orders.new"} 0.209
```

Dots and dashes become underscores, which is what Prometheus requires.

**It is a string and not a Rack app on purpose.** This library has no web
framework and should not choose one; every Ruby service already has something
answering HTTP, and one more route in it is less than one more server.

Serve it on a port the ingress does not publish. What a service publishes and
how long its handlers take is more than an anonymous caller should be able to
learn.

### The one worth an alert

`acemq.retry.rung.missing`.

A retry long enough to be handed to the broker checks that its rung queue is
really there before publishing into it — a publish into a queue nobody declared
is dropped without a word, and a retry that simply stops existing is the one
failure nothing else here would show.

When the rung is missing the wait happens in the consumer instead, so nothing is
lost; what is lost is the reason the rung exists, since a restart mid-wait now
turns a five-minute backoff into none. The check is one round trip per rung for
the life of a consumer, and only on the retry path. See
[reliability](reliability.md#when-a-rung-is-missing).

## Tracing

Counters say how many messages failed. A trace says which one, and what it was
waiting on.

```ruby
tracing = AceMQ::AMQP::Telemetry::OpenTelemetry.new
tracing.install(mq)
```

That registers it on both sides of the connection — it is an
[interceptor](interceptors.md), not a telemetry observer, because a span wraps a
publish or a handler and so has to know when one starts and when it ends.
Everything below could have been written outside this gem.

### The join

The point of tracing a message system is that the span covering a handler is a
child of the span that published the message, even though the two ran in
different processes minutes apart. The trace travels in the message:

```
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

`traceparent` and `tracestate` are the W3C names and are deliberately **not**
`x-acemq-` prefixed. Other tooling already knows them, and a private name would
make these traces invisible to everything that did not know to look for ours.
The Java library writes the same two, so a Ruby consumer joins a Java producer's
trace without either side being configured for the other.

On the way in, the parent is read out of **the message's own headers**, not out
of whatever this thread happened to be doing. Ambient context is the fallback,
not the source — that join across processes and minutes is the entire point.

### Spans

| Span | Kind | When |
| --- | --- | --- |
| `<destination> publish` | PRODUCER | a publish |
| `<queue> process` | CONSUMER | a handler running |
| `<destination> request` | CLIENT | a request/reply round trip |

CLIENT for a request rather than PRODUCER because that span waits for an answer,
so its duration includes somebody else's work — a reader who cannot tell the two
apart cannot tell a slow broker from a slow responder. It is the one span this
library does not open for you, because request and reply is a call you make:

```ruby
tracing.request("pricing.quote") { requester.call(order) }
```

The publish inside it becomes its child, so one trace covers the question and
the sending of it.

### Attributes

The OpenTelemetry messaging conventions, and the same set the Java adapter
writes, so existing tooling recognises them without configuration:

```
messaging.system                             rabbitmq
messaging.destination.name                   the exchange, or the queue
messaging.operation                          publish | process | request
messaging.message.id                         the envelope's id
messaging.message.conversation_id            the envelope's correlation id
messaging.rabbitmq.destination.routing_key   on a publish
messaging.acemq.message_type                 the envelope's type
messaging.acemq.attempt                      on a delivery
messaging.acemq.outcome                      what happened
```

An attribute with nothing to say is left out rather than written empty.

### Outcomes, and which of them are errors

`unroutable`, `failed` and `dead_lettered` set the span status to error.
`acked`, `retried`, `rejected`, `confirmed` and `answered` do not. A message
that will be tried again has not failed yet, and a message a handler refused on
purpose is the system working; marking either as an error is how a trace view
fills with red and stops meaning anything.

A retry the handler marked `FatalError` is reported as `dead_lettered` rather
than `retried`, because that is what the consumer will actually do with it.

**The outcome is what the consumer decided, not what the handler asked for.**
An `Ack` cannot know whether there is an attempt left to spend, so a handler
asking for a retry on its last attempt is reported as `dead_lettered` — the
message is going to the dead-letter queue, and a span saying `retried` would
send whoever is looking for it to the wrong queue. The consumer works the
decision out before the interceptors run and puts it on the context as a
[`Settlement`](interceptors.md#the-settlement), which is where the adapter reads
it.

### Events, not spans

```
outbox.publish_failed
pipeline.run_finished
message.retried
message.dead_lettered
```

Events on the span that is already open. A zero-length span at the end of a
trace adds a row and no information. The two the consume path knows about are
raised for you; the other two are methods you call.

`message.retried` carries `messaging.acemq.retry_delay_ms` — the delay the retry
policy really chose, which is the number that says whether the message comes
back in a second or in an hour. `message.dead_lettered` carries
`messaging.acemq.reason`, the same sentence written onto the message as
`x-acemq-error`, so a dead letter found in the queue and the trace that produced
it can be matched up by eye. It is raised for a rejection as well as for a
give-up: both go to the dead-letter queue, and only the word on the span keeps
them apart.

The two you call yourself:

```ruby
tracing.outbox_publish_failed(exchange: "orders", reason: error.message)
tracing.pipeline_run_finished(pipeline: "fulfilment", step: "pick",
                              outcome: "completed", age: seconds)
```

Both do nothing when no span is open, which is a legitimate answer: an outbox
relay on its own thread has nothing to hang an event on, and opening a span for
the event alone would produce exactly the zero-length span this avoids.

### Publishing outside the library

```ruby
tracing.propagation_headers   # => { "traceparent" => "00-…" }
```

For an outbox relay writing rows, or a job that hands work to something else,
that still wants its messages to join the trace it is running in.

### The gem

`opentelemetry-api` is not a runtime dependency — this gem declares none — and
is required at the moment one of these adapters is built, raising
`DependencyMissing` naming the gem when it is absent:

```ruby
gem "opentelemetry-api", "~> 1.8"
gem "opentelemetry-sdk", "~> 1.10"   # and something to export with
```

A process that counts messages and traces none of them installs neither.

## Health

```ruby
report = mq.health

report.status      # => :up, :degraded or :down
report.up?
report.detail      # => "" or why it is not up
report.checked_at  # => Time
report.parts       # => { "consumers" => 3, "consumers_running" => 3,
                   #      "queues" => ["orders.new", "orders.parked"],
                   #      "round_trip_ms" => 4 }
report.to_h        # the shape every AceMQ library renders at /acemq-health
```

`to_h` looks like this:

```json
{ "status": "up",
  "checked": "2026-09-07T09:14:22Z",
  "parts": { "consumers": 3, "consumers_running": 3,
             "queues": ["orders.new", "orders.parked"], "round_trip_ms": 4 } }
```

`queues` is there because "one consumer has stopped" is not actionable and
"nothing is reading `orders.new`" is.

**The broker is checked by declaring a queue and deleting it again**, because
that is the cheapest thing AMQP offers that actually proves the round trip. An
open socket answers the same as a healthy broker right up until something is
asked of it. The queue is named for the moment, so two instances running the
check cannot collide, and it is removed straight afterwards.

That costs a round trip, so this belongs on a readiness probe rather than in a
request handler. Let the probe's interval decide how often.

### Degraded

**A consumer that has stopped under a live connection is `:degraded`**, not
`:down`.

The process can still publish and its other consumers still work, so failing the
probe would take something doing most of its job out of rotation. But a queue
with nothing reading it is a real fault and has to be visible, which is what
`:degraded` is for. Alert on it; do not fail readiness on it.

`:down` is for a connection that is closed or a broker that did not answer, and
the reason is in `detail`.

### Combining checks

```ruby
report = Health.aggregate(
  Health::Check.new("broker", mq),
  DatabaseCheck.new,
  RedisCheck.new
)
```

A check is anything answering `name` and `check`, where `check` returns a
`Health::Report`. The combined status is the **worst** of them: a service that
cannot reach its broker is not ready however healthy the rest of it is, and
`detail` names the ones that were not up.

They run on threads rather than in turn, so a slow one does not add its latency
to the others, and one that raises becomes a `:down` part rather than an
exception out of a readiness probe — a probe that raises tells the orchestrator
nothing at all.

## Wiring it up

Rack, since most Ruby services have one somewhere:

```ruby
class AceMQEndpoints
  def initialize(mq, metrics)
    @mq = mq
    @metrics = metrics
  end

  def call(env)
    case env["PATH_INFO"]
    when "/acemq-metrics"
      [200, { "content-type" => "text/plain; version=0.0.4" }, [@metrics.to_prometheus]]
    when "/acemq-health"
      report = @mq.health
      [report.up? ? 200 : 503, { "content-type" => "application/json" },
       [JSON.generate(report.to_h)]]
    else
      [404, {}, []]
    end
  end
end
```

Liveness and readiness are different questions and this answers the second. A
liveness probe that failed because the broker was down would restart every
instance of the service at once, which does not help anybody reach a broker that
is not there.

## Next

- [Reliability](reliability.md) — what the dead-letter and rung counters are
  counting
- [Interceptors](interceptors.md) — for a measurement this library does not take
- [Security](security.md) — the port these are served on is a decision
