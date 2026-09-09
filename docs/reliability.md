# Retries, dead letters and shutdown

A handler returns `Ack.retry`, and a policy decides whether there is another
attempt and how long to wait. Where that wait happens depends on how long it is,
and that is the part worth reading.

```ruby
policy = RetryPolicy.exponential(5, 1, 60).give_up_after(6 * 3600)

mq = Connection.open(url, retry_policy: policy)
```

**Without a policy the default is `RetryPolicy.none`** — one delivery — so a
retry against an unconfigured connection dead-letters immediately. That is a
great deal easier to explain than a message going round the broker as fast as it
can be handed back.

## Policies

```ruby
RetryPolicy.none                            # one delivery, no retry
RetryPolicy.fixed(5, 10)                    # 5 attempts, 10s apart
RetryPolicy.exponential(5, 1, 60)           # 5 attempts, 1s doubling, capped at 60s
RetryPolicy.exponential(5, 1, 60).with_jitter(0.2)
RetryPolicy.exponential(5, 1, 60).give_up_after(6 * 3600)
```

`schedule` shows the delays without jitter, which is what to read when deciding
whether a policy is the one you meant. Four numbers are easier to argue with
than three parameters:

```ruby
RetryPolicy.exponential(5, 1, 60).schedule    # => [1.0, 2.0, 4.0, 8.0]
RetryPolicy.exponential(6, 10, 300).schedule  # => [10.0, 20.0, 40.0, 80.0, 160.0]
```

Five attempts means four delays. The last attempt has nothing after it.

**Jitter moves a delay both ways.** One-sided jitter only ever delays, which
turns a thundering herd into a slower thundering herd rather than spreading it.

**`give_up_after` is a limit on age, not on attempts**, and it is the honest one
when a queue has been paused: a message can be on attempt one and four days old.
It reads `envelope.first_seen`, so it survives everything — a restart, a
different consumer, a [replay](patterns.md#replay).

The arithmetic is identical in Java, Go, .NET and Python. The same policy
produces the same schedule in all five, which is what lets two services on one
queue behave the same way.

## The attempt count travels on the message

`Ack.retry` **republishes** the message with `x-acemq-attempt` advanced. It does
not requeue it.

That is not an optimisation, it is the only arrangement that works. A requeue
hands the broker back the bytes it was given, so the count would have to live in
the consumer — and then a fleet of consumers each counts its own, a message that
moves between them is for ever on attempt one, and a restart forgets everything
anyway. A policy of five attempts would retry indefinitely.

The trade is real and worth knowing: a retried message goes to the **back** of
its queue rather than the front.

`delivery.redelivered?` is the broker's separate opinion — it has offered these
bytes before, which happens after a consumer crash without any republish. The
two are different questions and both are on the message.

## Where the delay is waited

**Under 30 seconds, the consumer waits.** It holds one prefetch slot and sleeps,
with jitter. A wait lost to a restart costs seconds.

**At or above 30 seconds, the broker waits.** The message is published into a
rung queue — `{queue}.retry.{delay}` — whose `x-message-ttl` is the delay and
whose dead-letter target is the queue it came from, so the broker returns it
when the time is up with nothing running.

The line matters because a consumer that sleeps through a five-minute backoff
loses the **whole** wait when it restarts — the broker redelivers at once. That
is a correctness bug rather than a throughput one. Below the threshold a lost
wait costs seconds and a queue per rung of a schedule that finishes in the time
it takes to notice is not worth what it costs the broker.

```ruby
policy = RetryPolicy.exponential(6, 10, 300)   # 10s, 20s, 40s, 80s, 160s

Topology.new
        .queue("orders.new", dead_letter: true, retry_policy: policy)
        .apply(mq)                             # declares .retry.40s, .retry.80s, .retry.160s

mq.consume("orders.new", retry_policy: policy) { |message| … }
```

The 10s and 20s delays get no rung; the consumer waits those. `retry_threshold:`
moves the line, on both the topology and the consumer, **and the two have to
agree**.

`retry_threshold: 0` reads as "from zero, so everything" and means the opposite:
it switches the broker off, and no rung queue is declared at all. That is the
sense Java, Go, .NET and Python all use, and it is the only spelling there is for
"never use the broker" — "always use the broker" is any threshold below the
shortest delay in the schedule. It is one of the rows in
`spec/fixtures/contract-fixtures.json`, so all five libraries are held to it.

Thirty seconds is the default in all five, and a delay is held on a rung when the
threshold is above zero, the delay is above zero, and the delay is at or past the
threshold. Nothing else about the schedule changes it.

Jitter applies only below the threshold. Above it the spread comes free: each
message's time-to-live starts when it enters the rung, so a fleet that failed
over ten seconds is released over ten seconds without anybody arranging it.

**Per-message TTL is never used**, and it is worth saying why, because it looks
like the flexible answer. RabbitMQ expires messages only from the head of a
queue, so one long wait sitting at the front holds back every shorter one behind
it, and the delays that come out bear no relation to the ones that went in.

## The shape a rung has on the broker

The same in Java, Go, .NET, Python and Ruby, because two services consuming one
queue declare the same rung by name — and a rung declared with different
arguments answers the second one `PRECONDITION_FAILED`, leaving it unable to
consume at all:

```
acemq.retry                        direct, durable
acemq.dlx                          direct, durable

orders.new.retry.40s               classic, durable
                                   x-message-ttl              40000
                                   x-dead-letter-exchange     acemq.retry
                                   x-dead-letter-routing-key  orders.new

orders.new        -> acemq.retry -> orders.new           (an expired rung comes home)
orders.new.dlq    -> acemq.dlx   -> orders.new.dlq
orders.new.parked -> acemq.dlx   -> orders.new.parked
```

Nothing consumes a rung, and nothing should: a consumer on one would take the
message before its time-to-live expired, which is the entire wait.

A rung is **classic**, said out loud rather than left to the
[default](topology.md#queue-types), because the queue type is part of the
argument table the broker compares and all five libraries declare it classic.

### Why not the default exchange

It would route a rung home by queue name with no exchange and no binding at all,
and that is genuinely one fewer thing to forget. It is not what is done, for two
reasons.

The first is agreement: five libraries have to declare one table, and this is
the one Java has always declared.

The second is that the default exchange cannot be bound, listed or given a
policy, so the path a retry takes home would exist only inside a queue argument,
invisible from the broker. `acemq.retry` puts that path in the topology where it
can be reviewed, and lets a service be granted write on one exchange rather than
on every queue in the vhost.

The cost is the binding, and it is real: **an expired message with nothing bound
to carry it is dropped silently.** So the binding is never optional and never
deferred — `RetryLadder#declare` and `Topology#retry_ladder` each declare the
exchange, the rungs and the binding together, in that order, and a consumer
declares them again before it subscribes.

### When a rung is missing

A retry long enough to be handed to the broker checks that its rung queue is
really there before publishing into it. A publish into a queue nobody declared
is dropped without a word, and a retry that simply stops existing is the one
failure here nothing else would show.

When the rung is missing the wait happens in the consumer instead, so nothing is
lost; what is lost is the reason the rung exists, since a restart mid-wait now
turns a five-minute backoff into none. It is counted as
**`acemq.retry.rung.missing`, and that metric is worth an alert** — a topology
that was never applied looks exactly like one that was until this happens.

The check is one round trip per rung for the life of a consumer. A rung that
exists does not stop existing, so the answer is remembered; a missing one is
asked about again, so a topology applied while the consumer is running starts
being used.

## The names

```ruby
Naming.dead_letter_queue("orders.new")   # => "orders.new.dlq"
Naming.parked_queue("orders.new")        # => "orders.new.parked"
Naming.retry_queue("orders.new", 40)     # => "orders.new.retry.40s"

Naming::RETRY_EXCHANGE                   # => "acemq.retry"
Naming::DEAD_LETTER_EXCHANGE             # => "acemq.dlx"
```

The dead-letter exchange can be pointed elsewhere per topology
(`Topology.new(dead_letter_exchange: "team.dlx")`), because only this library's
own queue arguments name it. The retry exchange cannot: it is written into the
rung's argument table, which is the table everybody has to agree on.

Every name here is pinned by `spec/fixtures/contract-fixtures.json`, along with
the one place the five libraries do not yet agree: a sub-second rung is
`orders.new.retry.0s` in Ruby, Go and Python and `orders.new.retry.500ms` in
Java. It is unreachable behind the default threshold, and it is
[recorded rather than resolved](testing.md#the-disagreements-it-records).

## Who declares what

Two halves, and they overlap on purpose.

A **`Topology`** is the half a deployment applies before anything runs: the
exchange a service reads from, its queue, the queue's dead-letter arguments, and
the retry ladder when the queue is declared with a policy. It is a plan somebody
reviewed, and it is where all of this belongs.

A **consumer** declares its own share of it again as it starts, through
`RetryLadder#declare`:

| | `Topology` | consumer |
|---|---|---|
| `orders` (the exchange it reads from) | yes | no |
| `orders.new` (the source queue) | yes | no |
| `orders.new` bound to `orders` | yes | no |
| `acemq.retry` | yes | yes, when there are rungs |
| `orders.new.retry.40s` and the rest | yes | yes, when there are rungs |
| `orders.new` bound to `acemq.retry` | yes | yes, when there are rungs |
| `acemq.dlx` | yes | **always** |
| `orders.new.dlq`, `orders.new.parked` | yes | **always** |
| both bound to `acemq.dlx` | yes | **always** |

The overlap costs nothing: declaring a queue that already exists with the same
arguments is how AMQP is meant to be used, and the arguments are the same
arguments — classic, durable, empty table — so a service that applied its
topology first and then started a consumer gets a duplicate declaration rather
than a `PRECONDITION_FAILED`, and so does one that did it the other way round.

What the overlap buys is the case where the topology was never applied. **A
consumer that gives up republishes to `{queue}.dlq` through the default
exchange, and the default exchange drops what it cannot route without a word** —
no return, no failed confirm, nothing in a log. On a broker where a deployment
step was missed, the dead letter that would have told you so is the thing that
disappears. Declaring the queue at start-up is what makes it findable.

The dead-letter half is declared **whether or not there is a retry policy**, and
the retry half only when there are rungs. `RetryPolicy.none` is the library
default and gives up on the first failure, so a consumer with no policy at all
is the one that dead-letters soonest; hanging the declaration off having
somewhere to retry would leave exactly that consumer with nowhere to give up. A
retry exchange, on the other hand, is of no use to a consumer with no rung to
publish into, so it is not declared for one.

Two things a consumer does **not** declare: the source queue, which belongs to
whoever set the service up and whose type and arguments this library would have
to guess at — a guess of classic against a quorum queue is a `PRECONDITION_FAILED`
that stops the consumer starting at all — and anything bound to it beyond the
one binding that brings an expired message home. The two dead-letter bindings
are to queues the consumer declared itself, so nothing here needs the source
queue to have been declared by the same call.

`acemq.dlx` goes out under its shared name even for a `Topology` that renamed
its dead-letter exchange. The rename is a per-topology courtesy for a shared
vhost; a consumer has not been shown the topology and has no way to know.

This is **ADR-032**, and it brought Ruby into line with Java, which had always
done it this way. All five libraries now agree; the fixture marks the shared
entries `both`, and `spec/contract_spec.rb` fails if either half stops
declaring them.

## Giving up

A message is dead-lettered when the policy has no attempt left, or the message
is older than `give_up_after` allows, or the handler returned `Ack.reject`, or
the handler marked its retry reason `FatalError`:

```ruby
Ack.retry(FatalError.new("no such SKU"))   # honoured as a rejection
raise FatalError, "no such SKU"            # the same thing, said normally
```

Honouring the mark rather than the request is the entire point of having it.

The message is **republished** to `{queue}.dlq` with the reason in
`x-acemq-error`, and the original is then acknowledged.

All four routes end in the same queue, and the counters keep them apart:
`Ack.reject` counts as `acemq.messages.rejected`, the other three as
`acemq.messages.dead.lettered`, and neither is also counted as a retry.
`Ack.park` is not one of these routes at all: it ends in `{queue}.parked` and
counts as `acemq.messages.parked`. See
[observability](observability.md#the-outcome-counters-are-what-the-consumer-decided).

Acknowledging a failure looks wrong and is what makes it reliable: the message
is already safely somewhere else, so the original is a copy that has been dealt
with. Rejecting it instead would either requeue it into a hot loop or hand it to
whatever dead-lettering the queue happens to carry — and neither of those can
write down *why*, which is the one thing whoever finds it needs.

The reason reads like this in `x-acemq-error`:

```
gave up after 5 attempts: RuntimeError: the warehouse is down
gave up on a message older than 21600.0 seconds: Timeout::Error: …
rejected by the handler: no such SKU
retrying cannot help: AceMQ::AMQP::FatalError: no such SKU
```

## Parked messages

A body **no codec can read** goes to `{queue}.parked` rather than `{queue}.dlq`,
and is counted as `acemq.messages.parked`.

So does a message a handler answers with `Ack.park`:

```ruby
mq.consume("orders.new") do |message|
  next Ack.park("schema version #{message.envelope.version}") unless known?(message)
  place(message.payload)
  Ack.accept
end
```

Same queue, same counter, same `parked` on the span. The engine parks what it
could not decode; `Ack.park` is for the handler that got further and still knows
the message is unreadable — a version this service was never taught, a field that
is not a date where a date has to be. Without it such a handler had to reject the
message into the dead letters, which is exactly the mixing the parking queue
exists to prevent.

A message that failed five times and a message nothing could read are different
problems: one is a bug in the handler or a bad day for a downstream service, the
other is a producer sending something this consumer was never taught. Mixing
them means somebody sorts them by hand.

### When a message cannot be set aside at all

Republishing to `{queue}.dlq` or `{queue}.parked` can itself fail, and a queue
that was never declared is the usual reason. Ruby does not settle the delivery in
that case: the publish failure escapes the handler, nothing is acknowledged, and
the broker redelivers the message when the channel closes. Nothing is lost, and
the message keeps coming back until the queue exists.

`acemq.messages.set.aside.failed` is counted when this happens, labelled with the
`queue` and the `target` that could not be reached. It is worth an alert: from
outside, a message redelivered forever because its dead-letter queue is missing
is indistinguishable from a handler failing forever on the same message, and this
counter is the only thing that says which it is.

Go and Python differ here — they reject the message to the broker instead, so the
delivery is settled either way — but they raise the same counter, so an alert
written once reads the same against all three.

For this to work, a codec has to raise `DecodeError` rather than its own
exception class — see [codecs](serialization.md#writing-your-own).

## Getting them back

```ruby
result = Patterns.replay(mq, from: "orders.new.dlq", exchange: "orders-events",
                         limit: 500) { |envelope, _body| envelope.error.include?("timeout") }
```

[Replay](patterns.md#replay) is the whole of that story, including why a
replayed message goes back on attempt one.

## Shutdown

```ruby
consumer.cancel(timeout: 30)   # stop delivery, then wait for handlers
mq.close                       # the same, for every consumer, then the socket
```

`cancel` stops the subscription **first** and then waits, so nothing new arrives
while it drains. `in_flight` says how many are still being worked on.

`close` stops every consumer even when one of them refuses, closes the socket
either way, and raises the first refusal afterwards. Stopping at the first
failure would leave the rest running and the socket open, so a shutdown that
went slightly wrong would become a process that will not exit — which is a worse
problem than whatever the first consumer objected to.

A message being handled when a process is killed outright is not lost. It was
never acknowledged, so the broker offers it again — to this consumer or another
— with the same attempt number, because nothing republished it. That is why
anything that changes something wants to be
[idempotent](patterns.md#idempotency).

## What is not here

**A reconnection layer of its own.** Recovery is bunny's, on bunny's terms:
`Transport.open` passes the URL and your options straight through, so bunny's
`automatically_recover` default applies and anything you want changed about it —
`network_recovery_interval`, `recovery_attempts`, turning it off — is a keyword
on `Connection.open`, which forwards what it does not recognise to the
transport. Nothing here redeclares a topology after a recovery or re-runs
`RetryLadder#declare`.

So do not assume a broker restart is invisible. `mq.health` is what says whether
this process is still doing its job — see
[metrics and health](observability.md) — and a readiness probe on it is what
turns a consumer that quietly stopped into an instance the orchestrator
replaces.

**Exactly-once delivery**, which is not a thing any broker offers. What is here
is at-least-once with a guard against duplicates; see
[idempotency](patterns.md#idempotency) for where the remaining gap is.

## Next

- [Consuming](consuming.md) — where `retry_policy:` is passed
- [Exchanges, queues and bindings](topology.md) — declaring the rungs
- [Patterns: replay](patterns.md#replay) — putting dead letters back
- [Metrics, tracing and health](observability.md) — `acemq.retry.rung.missing` and the
  dead-letter counters
