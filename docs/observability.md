# Metrics and health

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
