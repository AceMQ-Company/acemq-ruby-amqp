# The envelope

Every AceMQ message carries one. It is the part that crosses languages: a Java
producer writes these headers and a Ruby consumer reads them back, unchanged, or
the fixtures fail.

```ruby
envelope = Envelope.new(type: "order.placed.v2", origin: "checkout@pod-7")
envelope.to_headers("order.placed")
# => { "x-acemq-id" => "9f2c…", "x-acemq-type" => "order.placed.v2",
#      "x-acemq-version" => 1, "x-acemq-correlation" => "9f2c…",
#      "x-acemq-attempt" => 1, "x-acemq-first-seen" => 1741785600000,
#      "x-acemq-origin" => "checkout@pod-7" }
```

## The fields

| Header | Reader | |
|---|---|---|
| `x-acemq-id` | `id` | the message identifier, and the default idempotency key |
| `x-acemq-type` | `type` | the logical type, falling back to the routing key |
| `x-acemq-version` | `version` | schema version, from 1 |
| `x-acemq-correlation` | `correlation_id` | defaults to the id, so a chain has something to copy |
| `x-acemq-causation` | `causation_id` | the message that caused this one |
| `x-acemq-attempt` | `attempt` | delivery attempt, from 1 |
| `x-acemq-first-seen` | `first_seen` | epoch **milliseconds** of the first publish |
| `x-acemq-origin` | `origin` | `service@host` |
| `x-acemq-error` | `error` | why it was dead-lettered |
| `x-acemq-claim` | `claim` | where the payload is, when it is stored outside the message |

Plus `headers`, which is your own — see [application headers](#application-headers).

**`id`** is a UUID unless you supply one. It is what
[idempotency](patterns.md#idempotency) keys on by default, and the thing to log
when you log anything.

**`correlation_id` defaults to the id.** That default is what makes a chain
possible without anybody remembering to start one: the first message in a
business action correlates to itself, and every message caused by it copies the
value forward. A field that started empty would need whoever published the first
message to know they were first.

**`causation_id`** is the id of the message that caused this one — the immediate
parent, where correlation is the whole tree. Together they answer "what else
happened because of this" and "what caused this", which are different questions
and are usually asked in that order.

**`first_seen` is milliseconds on the wire and a `Time` in Ruby.** Milliseconds
because that is what Java writes and what an AMQP header table can carry
without ambiguity; a `Time` in the object because comparing epoch integers in
application code is how a comparison ends up off by a factor of a thousand.
`age` gives the difference in seconds:

```ruby
envelope.age   # => 4.21
```

That is what a retry policy's `give_up_after` reads. A message can be on attempt
one and four days old, which is what happens when a queue is paused, and giving
up on **age** as well as attempts is the honest limit for it.

**`claim`** is for a payload too large to put in a message — the message carries
a reference and the bytes live somewhere else. The field is on the wire and
reserved, and it is the application's own note about where a payload lives.
Putting the payload aside and fetching it back is the
[claim check](patterns.md#the-claim-check), which works on the body rather than
on this header: a codec only ever sees bytes, and a header can be stripped by a
shovel or a federation link where the body cannot.

## Frozen, and changed with `with`

An `Envelope` is frozen at construction, and so is its `headers` hash.

```ruby
outgoing = incoming.envelope.with(type: "shipment.requested",
                                  causation_id: incoming.envelope.id)
```

`with` returns a new envelope with those fields replaced. Nothing mutates one,
which matters more than it sounds: an envelope is read by the consumer, handed
to every interceptor, used to build a dead letter and compared against what went
out. A handler that could edit one in place would be editing the copy the
dead-letter path is about to write.

Two envelopes are `==` when every field matches, `headers` included.

## Application headers

Your own headers go in `headers:` and are kept apart from the reserved ones:

```ruby
mq.publish(event, to: "order.placed", exchange: "orders-events",
           headers: { "tenant" => "acme", "trace-id" => trace.id })
```

A reserved name in there **raises** rather than being dropped:

```ruby
Envelope.new(headers: { "x-acemq-id" => "mine" })
# => ArgumentError
```

Silently discarding a header somebody set is worse than saying no. It is a bug
that shows up as an absence, in another service, weeks later.

The rule runs the other way too: unknown `x-acemq-` names arriving from a newer
version of another language's library are **not** handed back as yours.
`Headers.reserved?` is the whole test, and the `x-acemq-` prefix is reserved as
a namespace rather than as a list, so a field added to the contract next year
does not start appearing in application code that predates it.

That is also why the patterns' own headers are *not* in that namespace —
`acemq-reply-to`, `acemq-routing-slip`, `acemq-replayed-from` and the rest are
ordinary application headers, because a responder or a slip handler has to be
able to read them and the engine keeps `x-acemq-` away from handlers.

## The route

`x-acemq-route`, `x-acemq-route-position` and `x-acemq-route-id` are the one
exception to the paragraph above, and they are reserved because Java made them
so: they are how a message travels a
[declared pipeline](patterns.md#declared-pipelines). An envelope carries them
opaquely, in `route`, and passes them through every hop:

```ruby
envelope.route
# => { "x-acemq-route" => "validate,charge,ship",
#      "x-acemq-route-position" => 1,
#      "x-acemq-route-id" => "b0c1…" }
```

The envelope does not know what they mean; `Patterns::RoutingSlip` is what reads
and writes them. It holds them because they are reserved names, and reserved
names cannot go in `headers:` — a pattern that needed to write one would
otherwise have nowhere to put it.

## Reading one off a delivery

```ruby
envelope = Envelope.from_headers(delivery.headers, delivery.routing_key)
```

Anything missing takes its default, and **anything unreadable takes its default
too**. A producer that wrote `x-acemq-attempt` as the string `"3"` has still
sent a message, and refusing to deliver it would hand the application an outage
rather than a message. `type` falls back to the routing key, which is what a
message from a producer that never set one should be called.

The consumer does this for you; `from_headers` is for a tool reading a
dead-letter queue with `pull`, and for tests.

## The contract, pinned

`spec/fixtures/envelope-fixtures.json` was produced by the Java implementation
and is shared with Go, .NET and Python. A spec reads each fixture and writes it
back: no header gained, none lost, none renamed, and the millisecond timestamps
still integers on the way out.

That file is the definition of "the same wire contract". A port that hand-copies
one out of documentation acquires a difference nobody notices until two
languages disagree in production, and the difference is always in the field
nobody thought was interesting.

## Next

- [Publishing](publishing.md) — setting the fields
- [Interceptors](interceptors.md) — stamping every envelope in one place
- [Reliability](reliability.md) — `attempt`, `first_seen` and `error` on the
  retry path
