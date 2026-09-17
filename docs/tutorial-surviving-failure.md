# Tutorial 2 — Surviving failure

[Tutorial 1](tutorial-first-message.md) had a handler that always worked. This
one has a handler that does not, and by the end you will have retries that do not
block a consumer, a dead-letter queue you can read, and a way to put its contents
back. About twenty-five minutes.

Same broker as before:

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

## Step 1 — Watch it fail

```ruby
require "acemq/amqp"

include AceMQ::AMQP

mq = Connection.open("amqp://guest:guest@localhost:5672")

Topology.new.queue("orders.new").apply(mq)

mq.consume("orders.new") do |message|
  raise "the warehouse is not answering"
end

mq.publish({ "order_id" => "A-1" }, to: "orders.new")
sleep 2
puts "still on the queue: #{mq.message_count("orders.new")}"
mq.close
```

Note what the topology does **not** say: no `dead_letter:`, no retry policy.

```
still on the queue: 0
```

Zero. The message is gone from `orders.new`, and nothing in that program said
where it went. Look:

```ruby
puts "dead letters: #{mq.message_count("orders.new.dlq")}"   # => 1
```

`orders.new.dlq` exists, and it has your message on it, and **you never declared
it**. That is worth understanding rather than being pleased about.

What happened: a handler that raises is treated as `Ack.retry`. The connection's
default policy is `RetryPolicy.none`, which allows one delivery, so there was no
attempt left to spend and the consumer gave up. Giving up means republishing to
`orders.new.dlq` by name, through the default exchange — and **the default
exchange drops what it cannot route, without a word**. No return, no failed
confirm, nothing in a log. On a broker where a deployment step was missed, the
dead letter that would have told you so would be the thing that disappeared.

So the consumer declares `{queue}.dlq` and `{queue}.parked` for itself, before it
subscribes and whether or not it has a retry policy. It is the one piece of
topology this library will not let you get wrong, because getting it wrong is
invisible. [Who declares what](reliability.md#who-declares-what) has the full
division, including the two things a consumer will *not* declare for you.

## Step 2 — Read the dead letter, and declare it anyway

The reason is on the message:

```ruby
delivery = mq.pull("orders.new.dlq")
puts delivery.headers["x-acemq-error"]
```

```
gave up after 1 attempts: RuntimeError: the warehouse is not answering
```

The exception class is in there because the handler raised rather than returning
an `Ack`. Compare that with what step 6 produces, and you have the argument for
the four acks in one line.

`pull` takes one message off without subscribing, which is what you want when you
are inspecting a dead-letter queue rather than draining it. **The delivery comes
back unacknowledged**, and settling it is your job: leave it alone and the broker
holds it until the connection closes, then gives it back. A tool that dies half
way through a pass therefore returns everything it was holding, which is the
behaviour you want from anything pointed at a queue of failures.

Now declare it properly anyway:

```ruby
Topology.new.queue("orders.new", dead_letter: true).apply(mq)
```

That declares the same two queues, and adds the part a consumer cannot: it puts
`x-dead-letter-exchange: acemq.dlx` and `x-dead-letter-routing-key: orders.new.dlq`
**on `orders.new` itself**. The consumer's own give-up path does not need those —
it republishes by name. What needs them is everything that dead-letters a message
without asking this library: a message expiring on a TTL, a queue hitting a length
limit, something rejecting a delivery that is not one of your handlers. Without
them those messages are dropped by the broker; with them they land beside your
dead letters.

Declaring a queue twice with the same arguments is how AMQP is meant to be used,
so the overlap costs nothing whichever order it happens in.

## Step 3 — The answer that looks right

The obvious fix is to wait and try again:

```ruby
mq.consume("orders.new") do |message|
  begin
    warehouse.reserve(message.payload)
    Ack.accept
  rescue Timeout::Error
    sleep 60
    retry
  end
end
```

Do not. That handler holds one of the consumer's prefetch slots for a minute,
and twenty messages failing the same way stop the consumer entirely — the
warehouse is down for a minute and your service is down until somebody restarts
it. Worse, the wait is in memory: a deploy in the middle of it loses the whole
minute and the message comes straight back.

What you want is for the *broker* to do the waiting.

## Step 4 — A retry policy

```ruby
policy = RetryPolicy.exponential(5, 1, 60)
policy.schedule   # => [1.0, 2.0, 4.0, 8.0]

mq = Connection.open("amqp://guest:guest@localhost:5672", retry_policy: policy)
```

Five attempts, one second doubling, capped at sixty. **Five attempts means four
delays** — the last attempt has nothing after it.

`schedule` is worth calling before you commit to a policy. Four numbers are
easier to argue with than three parameters, and it needs no broker: it is
arithmetic, and the same arithmetic in all five libraries.

Now the handler says what it wants instead of raising:

```ruby
mq.consume("orders.new") do |message|
  if warehouse.reserve(message.payload)
    Ack.accept
  else
    Ack.retry("the warehouse said no")
  end
end
```

`Ack.retry` **republishes** the message with `x-acemq-attempt` advanced. It does
not requeue it, and that is not an optimisation: a requeue hands the broker back
the bytes it was given, so the attempt count would have to live in the consumer —
and then a fleet of consumers each counts its own, a message that moves between
them is for ever on attempt one, and a restart forgets everything anyway.

The trade is that a retried message goes to the **back** of its queue.

After five attempts the message goes to `orders.new.dlq` with
`gave up after 5 attempts: the warehouse said no` on it. The reason your handler
gave is the reason on the dead letter, which is the whole argument for returning
an `Ack` instead of raising.

## Step 5 — Put the long waiting in the broker

A policy alone still waits inside the consumer. Declare the ladder and it stops:

```ruby
policy = RetryPolicy.exponential(6, 10, 300)   # 10s, 20s, 40s, 80s, 160s

Topology.new
        .queue("orders.new", dead_letter: true, retry_policy: policy)
        .apply(mq)

mq.consume("orders.new", retry_policy: policy) { |message| … }
```

That declares three queues you did not name: `orders.new.retry.40s`,
`.retry.80s` and `.retry.160s`. Look at the management UI and you will see them.

**Under 30 seconds the consumer waits; at 30 or above the broker does.** The 10s
and 20s delays get no rung, because a wait that short costs seconds when a
restart loses it, and a queue per rung of a schedule that finishes in the time it
takes to notice is not worth what it costs the broker. The 40s, 80s and 160s
delays each get a classic queue whose `x-message-ttl` is the delay and whose
dead-letter target is `orders.new`, so the broker holds the message with nothing
running and returns it when the time is up.

That line is a correctness boundary, not a tuning knob. A consumer that sleeps
through a five-minute backoff loses the **whole** wait when it restarts, and the
broker redelivers at once.

Two things to get right:

- **The topology and the consumer must agree** on `retry_threshold:`. A consumer
  publishing into `orders.new.retry.40s` that nobody declared is a message
  dropped by the default exchange — which is why the consumer checks the rung is
  there first and counts `acemq.retry.rung.missing` when it is not, falling back
  to waiting in memory.
- **`retry_threshold: 0` means the opposite of what it reads like.** "From zero,
  so everything" is not what it does: it switches the broker off entirely and no
  rung is declared at all. It is the only spelling there is for "never use the
  broker", and all five libraries use that sense.

[Retries, dead letters and shutdown](reliability.md) has the exact shape these
queues have on the broker, and why per-message TTL is never used.

## Step 6 — Say which failures are worth retrying

Not every failure deserves five goes. An order with no customer id will not
acquire one on the third attempt:

```ruby
mq.consume("orders.new") do |message|
  next Ack.reject("no customer id") if message.payload["customer_id"].nil?

  warehouse.reserve(message.payload) ? Ack.accept : Ack.retry("the warehouse said no")
end
```

`Ack.reject` skips the ladder entirely and goes straight to `orders.new.dlq`.
The whole point of the four acks is that the handler is the only thing that knows
which kind of failure this is:

| | |
|---|---|
| `Ack.accept` | done |
| `Ack.retry(reason)` | transient; spend an attempt |
| `Ack.reject(reason)` | permanent; dead-letter it now |
| `Ack.park(reason)` | nothing here can decide; a human should look |

`Ack.park` puts the message on `orders.new.parked` instead, which exists so that
"this failed five times" and "nothing could deal with this" are two different
queues. Whoever drains the dead letters should not have to sort them by hand.

Deep inside a call stack where returning an `Ack` is awkward, raise `FatalError`
instead — it is treated exactly as `Ack.reject`, and it is how a handler says
"stop now" without having to know how many attempts remain.

## Step 7 — The failure no retry can fix

There is a third destination, and you get it without asking. A message the codec
cannot decode is **parked**, not dead-lettered and not retried:

```ruby
mq.publish("this is not JSON", to: "orders.new", codec: StringCodec.new)
sleep 1
puts mq.message_count("orders.new.parked")            # => 1
puts mq.pull("orders.new.parked").headers["x-acemq-error"]
```

```
could not be decoded: this message is not JSON: unexpected token 'this' at line 1 column 1
```

A body that will not decode decodes no better next time. It never reaches your
handler, so your handler never gets to have an opinion, and parking it keeps it
away from the dead letters that are about handlers failing.

The interesting version of this failure is a producer that moved ahead of you.
With [Avro](serialization.md) and a schema registry, a consumer says which
version it was written against and the writer's schema is resolved onto it:

```ruby
registry = Patterns::InMemorySchemaRegistry.new

v1 = JSON.dump({ "type" => "record", "name" => "Order",
                 "fields" => [{ "name" => "id", "type" => "string" }] })
v2 = JSON.dump({ "type" => "record", "name" => "Order",
                 "fields" => [{ "name" => "id", "type" => "string" },
                              { "name" => "currency", "type" => "string",
                                "default" => "EUR" }] })

# the producer, still on v1
writer = AvroCodec.registered(registry, subject: "orders", schema: v1)
body   = writer.encode({ "id" => "A-1" })

# the consumer, redeployed against v2 and still able to read what v1 wrote,
# because the field v1 does not have carries a default
reader = AvroCodec.registered(registry, subject: "orders", schema: v1, reader_schema: v2)

reader.decode(body, writer.content_type)
# => { "id" => "A-1", "currency" => "EUR" }
```

`reader_schema:` is the point. Without it a consumer decodes onto whatever schema
it was constructed with and a producer one version ahead is a parked message;
with it, Avro's own resolution rules apply and a field added with a default is
filled in. When the two schemas genuinely cannot be resolved, the error says so
by name rather than leaving you to work it out from a hex dump. `.NET`'s
`ReaderSchema`, Go's `avro.ReadAs` and Python's `reader_schema=` are the same
thing spelled for those languages — see
[codecs](serialization.md#what-ships).

## Step 8 — Put them back

A dead-letter queue you cannot drain is a queue that grows. When the warehouse is
back:

```ruby
result = Patterns.replay(mq, from: "orders.new.dlq", routing_key: "orders.new")
puts result    # => "moved 12, skipped 0 (drained)"
```

`replay` pulls from the dead-letter queue and republishes, stamping each message
with `acemq-replayed-from`, `acemq-replayed-at` and an incrementing
`acemq-replay-count` so that a message going round for the third time is visible
as such.

**A replayed message goes back on attempt one**, with the reason that
dead-lettered it cleared. That is the only thing that works: a message
dead-lettered on the last attempt of a five-attempt policy would otherwise arrive
back on attempt five and be given up on before your handler ever saw it — so an
operator who has just fixed the bug moves two thousand messages from the
dead-letter queue to the dead-letter queue. Pass `restart: false` to put back
exactly what was there, which is what an audit wants. The identity is untouched
either way: same id, same correlation, same `first_seen`, so a policy with
`give_up_after` still gives up on age. The fix was for the bug, not for the clock.

A block decides which messages go, which is what makes a replay something you can
do in stages:

```ruby
Patterns.replay(mq, from: "orders.new.dlq", routing_key: "orders.new") do |envelope, _body|
  envelope.error.include?("warehouse")     # leave the rest where they are
end
# => "moved 1, skipped 1 (drained)"
```

Skipping matters more than it looks. Replay everything and the messages that were
rejected on purpose — the order with no customer id — go straight back, get
rejected again, and land on the dead-letter queue while the pass is still
running, where the same pass picks them up a second time. Filter, or empty the
permanent failures first.

`replay` refuses to replay a queue onto itself, which is the loop everybody
writes once. `limit:` and `deadline:` bound the run, so replaying a hundred
thousand dead letters does not become an operation nobody can stop.

## Step 9 — All together

```ruby
require "acemq/amqp"
require "acemq/amqp/patterns"

include AceMQ::AMQP

mq = Connection.open("amqp://guest:guest@localhost:5672", origin: "warehouse@laptop")

policy = RetryPolicy.exponential(4, 1, 10)      # 1s, 2s, 4s — all in the consumer

Topology.new
        .queue("orders.new", dead_letter: true, retry_policy: policy)
        .apply(mq)

warehouse_is_down = true
attempts = Hash.new(0)

consumer = mq.consume("orders.new", retry_policy: policy) do |message|
  id = message.payload["order_id"]
  attempts[id] += 1
  puts "#{id}: attempt #{message.envelope.attempt}"

  next Ack.reject("no customer id") if message.payload["customer_id"].nil?
  next Ack.retry("the warehouse is not answering") if warehouse_is_down

  puts "#{id}: reserved"
  Ack.accept
end

mq.publish_all([{ "order_id" => "A-1", "customer_id" => "C-9" },
                { "order_id" => "A-2" }],                       # no customer
               to: "orders.new", type: "order.placed.v2")

sleep 10
puts "dead letters: #{mq.message_count("orders.new.dlq")}"

held = []
held << delivery while (delivery = mq.pull("orders.new.dlq"))
held.each do |d|
  puts "  #{d.headers["x-acemq-id"]}: #{d.headers["x-acemq-error"]}"
  d.nack(requeue: true)
end

warehouse_is_down = false
puts Patterns.replay(mq, from: "orders.new.dlq", routing_key: "orders.new") { |e, _b|
  e.error.include?("warehouse")
}

sleep 3
consumer.cancel
mq.close
```

```
A-1: attempt 1
A-2: attempt 1
A-1: attempt 2
A-1: attempt 3
A-1: attempt 4
dead letters: 2
  589beae6-…: rejected by the handler: no customer id
  a2057f5d-…: gave up after 4 attempts: the warehouse is not answering
A-1: attempt 1
A-1: reserved
moved 1, skipped 1 (drained)
```

A-2 was rejected on its first attempt and never retried, and the filter left it
on the dead-letter queue where it belongs — it will not acquire a customer id by
being replayed. A-1 spent all four attempts, came back through the replay **on
attempt one**, and succeeded.

Two things about that output are not guarantees. The handler runs on the
transport's threads, so its lines interleave with the main thread's however the
scheduler feels — here the replay finished printing after the message it moved
had already been handled. And the two dead letters come back in whatever order
the broker offers them, which is not the order they failed in. Do not write an
assertion that depends on either.

Pulling into an array **before** requeueing anything is not fussiness. A requeued
message goes back to the head of the queue, so a loop that pulls one and requeues
it immediately pulls the same message again, for ever. Take the whole pass first,
then put it back — that is the shape `Patterns.replay` uses internally, and for
the same reason.

## Step 10 — What to watch in production

| | |
|---|---|
| `acemq.messages.dead.lettered.total{queue}` | the number that means work is being lost. Alert on the rate |
| `acemq.consume.attempts{queue}` | its mean rising is a dependency starting to struggle, before anything reaches the dead letters |
| `acemq.messages.set.aside.failed{queue,target}` | a dead letter that could not be written — the queue was deleted underneath a running consumer, or the broker refused the copy |
| `acemq.retry.rung.missing{queue}` | the ladder is not on the broker, so long waits are happening in memory |

The last two are [the two worth an
alert](observability.md#the-two-worth-an-alert), because they are failures of
your *topology* rather than of your handlers, and nothing else shows them. When
`set.aside.failed` moves, the message is rejected to the broker rather than left
unsettled, which means the delivery ends once instead of being redelivered until
somebody notices — so this counter is genuinely the only sign that path leaves.

## Next

[Tutorial 3 — Never processing twice](tutorial-exactly-once.md). Everything on
this page retries, and every retry is a chance to do the work twice.

Or, from the guide: [retries, dead letters and shutdown](reliability.md), which
is this tutorial's subject in full.
