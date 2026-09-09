# Interceptors

The seam for the things every message in an organisation needs and no library
can guess: a tenant, a trace context, a log scope, a size limit, a metric.
Without a seam they end up copied into every call site, where one of them is
always the one that forgot.

```ruby
mq.intercept_publish { |context| context.set_header("tenant", Current.tenant) }

mq.intercept_consume do |context|
  raise FatalError, "not our tenant" unless serves?(context.envelope.headers["tenant"])
end
```

Both return the connection, so they chain, and both are meant to be called at
start-up.

## The contexts

A **`PublishContext`** is the message before it is encoded:

| | |
|---|---|
| `exchange` | where it is going — writable |
| `routing_key` | — writable |
| `envelope` | — writable |
| `payload` | the Ruby object, before the codec sees it — writable |
| `set_header(name, value)` | replaces the envelope with one carrying the header |

Every one of them can be changed, which is the difference between intercepting
and observing. An interceptor can redirect a message as well as decorate it, and
can rewrite the payload while it is still a Ruby object rather than patching
bytes afterwards.

A **`ConsumeContext`** is the message after it is decoded and before the handler
sees it:

| | |
|---|---|
| `envelope` | — writable |
| `queue`, `payload`, `body`, `content_type`, `redelivered?` | read-only |
| `settlement` | what the consumer is about to do — set before `after_handle` |
| `set_header(name, value)` | as above |

What an interceptor leaves on the envelope is what the handler receives **and**
what any dead letter is written with. A header the handler saw and the
dead-letter queue did not would be missing exactly when somebody goes looking
for it.

`set_header` still refuses the reserved `x-acemq-` names — see
[the envelope](envelope.md#application-headers).

### The settlement

An `Ack` says what the handler asked for. A **`Settlement`** says what the
consumer is going to do about it, which is not the same thing:

| | |
|---|---|
| `outcome` | `acked`, `rejected`, `retried` or `dead_lettered` |
| `delay` | seconds until the retry, when it is one |
| `reason` | the sentence written onto the message, when it is not |
| `acked?`, `retried?`, `dead_letters?` | the three questions worth asking |

It is on the context by the time `after_handle` runs, and it is the only place
two things can be read at all. The first is the delay: it is chosen by the
[retry policy](reliability.md) while the delivery is being settled, so nothing
before that point knows it. The second is what happens on the last attempt — a
handler asking for a retry with no attempts left is dead-lettered, and an
interceptor reading the ack alone reports a message that was dropped as one that
is coming back.

```ruby
def after_handle(context, _ack)
  settlement = context.settlement
  return unless settlement.dead_letters?

  Alerts.raise("#{context.queue} dropped #{context.envelope.id}: #{settlement.reason}")
end
```

`rejected` is kept apart from `dead_lettered` although both end in the
dead-letter queue: a message the handler refused on purpose is the system
working, and one that ran out of attempts is not.

The library reads it from the same place. The four outcomes are the four
[outcome counters](observability.md#the-four-outcome-counters-are-what-the-consumer-decided)
and the four values of the span's `messaging.acemq.outcome` attribute, so a
counter, a span and an interceptor looking at one delivery all say the same
word.

## Blocks and objects

A block is the common case: one hook, on the way in. An object is the full case,
answering whichever of these it cares about:

| publishing | consuming |
|---|---|
| `before_publish(context)` | `before_handle(context)` |
| `after_confirm(context)` | `after_handle(context, ack)` |
| `on_error(context, failure)` | `on_error(context, failure)` |
| `order` | `order` |

```ruby
class Timing
  def order = -100

  def before_handle(context)
    Thread.current[:started] = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def after_handle(context, ack)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - Thread.current[:started]
    StatsD.timing("handler", elapsed, tags: ["queue:#{context.queue}", "ack:#{ack}"])
  end
end

mq.intercept_consume(Timing.new)
```

Nothing here needs anything private. An interceptor is registered through a
public method and handed a context whose every field is public, which is the
same rule the [patterns](patterns.md) follow and the only way to know a seam is
wide enough: everything in this library that could have been an interceptor
could have been written by somebody outside it.

## Raising means different things in different places

On purpose.

**From `before_publish` it stops the publish**, and the caller sees the
exception. That is the point of intercepting rather than observing — a message
that must not go out is stopped once, here, rather than in every publisher. The
publish is counted as `acemq.messages.publish.failed`, because it did not reach
the broker, which is what that metric is about.

**From `before_handle` the handler never runs** and the delivery is treated
exactly as a failed handler would be: retried, then dead-lettered. An
interceptor that refuses a message has to be willing for that message to reach
the dead-letter queue, which is the honest outcome — the alternative is
acknowledging something nothing processed. `FatalError` still means what it
means, so an interceptor refusing for a reason no retry can fix says so:

```ruby
mq.intercept_consume do |context|
  raise FatalError, "not our tenant" unless serves?(context)
end
```

**From `after_confirm`, `after_handle` or `on_error` it is reported on stderr
and otherwise ignored.** By then the message has been sent or the delivery
settled, and an exception cannot un-send or un-settle it. Letting it out would
report a successful publish as a failed one, or skip the teardowns that had not
run yet.

## Order

Lower `order` runs first, and interceptors with the same order run in the order
they were registered. On the way **out** of a handler the order is reversed, so
a pair that opens something on the way in and closes it on the way out nests
properly: the first to open is the last to close.

```ruby
mq.intercept_consume(LogScope.new)   # order -100: outermost
mq.intercept_consume(TenantCheck.new) # order 0
```

`order:` on the registration overrides whatever the object says, which is how an
interceptor from a gem gets put where you need it without editing the gem:

```ruby
mq.intercept_consume(SomeGem::Tracing.new, order: -1000)
```

A registration with neither an `order:` keyword nor an `order` method sits at 0.

## Threads

An interceptor is called on whichever thread is publishing or handling, so one
that keeps state has to be safe to call from several at once. The `Timing`
example above uses a thread-local for exactly that reason; a plain instance
variable would have two messages overwriting each other's start time under any
concurrency at all.

Registration itself is safe at any time. A publisher reads the list at the
moment it publishes rather than copying it, so an interceptor added later does
apply to publishers that already exist — but a message already on its way will
not see it. Register at start-up and this never comes up.

## What to reach for instead

An interceptor runs around *every* message on the connection. When you want to
wrap one handler, that is a [pipeline](patterns.md#pipelines-and-middleware):

```ruby
mq.consume("orders.new", &Patterns.chain(
  ->(message) { place(message.payload) },
  Patterns.with_timeout(10),
  Patterns.with_idempotency(store)
))
```

Interceptors are cross-cutting policy; pipelines are one consumer's business.
Putting a timeout in an interceptor would apply it to a queue whose handler is
meant to take four minutes.

## Next

- [Publishing](publishing.md) — what `publish` returns is what the interceptors
  left
- [Patterns: pipelines](patterns.md#pipelines-and-middleware) — the per-handler
  version
- [Metrics, tracing and health](observability.md) — the metrics you would otherwise write
  an interceptor for
