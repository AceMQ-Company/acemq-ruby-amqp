# Tutorial 1 — Your first message

By the end of this you will have a program that declares its own exchange and
queue, publishes an order, consumes it, and then publishes a thousand of them in
one round trip. About fifteen minutes.

You need a broker. If you have not started one:

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

And a Gemfile, as [the tutorials index](tutorials.md#before-you-start) sets out.

## Step 1 — Open a connection

```ruby
require "acemq/amqp"

include AceMQ::AMQP

mq = Connection.open("amqp://guest:guest@localhost:5672",
                     origin: "checkout@#{Socket.gethostname}")
```

`include AceMQ::AMQP` at the top of a file is how the documentation is written,
because `Connection`, `Topology`, `Ack` and `RetryPolicy` read better than the
fully qualified names at every call site. Nothing depends on it — qualify them if
you would rather.

`origin:` is what gets stamped on every message this connection publishes. It is
optional, and the default names the machine but not the service, which is the
most an unconfigured library can honestly say. Set it: `origin` is the field
somebody reads first when a message arrives from a fleet and the question is
which of forty pods sent it.

A connection is a socket and a heartbeat. **One per process** is the usual shape;
opening one per publish is how a service ends up with a thousand of them and a
broker that refuses the next.

`bunny` is required at this line and not before. If it is missing, the error says
so by name rather than failing as a `NameError` three frames deeper.

## Step 2 — Declare where messages go

```ruby
Topology.new
        .exchange("orders-events", :topic)
        .queue("orders.new", dead_letter: true)
        .binding("orders.new", "orders-events", "order.#")
        .apply(mq)
```

Four lines, three ideas.

An **exchange** is where you publish. A **queue** is where messages wait. A
**binding** joins them, and the binding is the piece that decides what this queue
wants: `order.#` matches `order.placed`, `order.cancelled` and anything else
starting `order.`.

You could skip all of it. `mq.publish(order, to: "orders.new")` with no exchange
uses the default exchange, which routes by queue name, and it works. It is also a
poor arrangement between services, because the publisher then has to know the
name of the consumer's queue. A named exchange lets the consumer decide what it
wants, and lets a second consumer arrive without the publisher changing a line.

`dead_letter: true` declares `orders.new.dlq` alongside the queue and points
`orders.new` at it. Nothing in this tutorial fails, so nothing will land there —
[tutorial 2](tutorial-surviving-failure.md) is about that queue.

`Topology` is validated before anything is sent: a binding to an exchange you did
not declare, a queue named twice with different arguments, a blank name. It fails
in your process with a sentence, rather than halfway through a sequence of
declarations with a channel already closed. `mq.apply(topology)` is the same
thing written the other way round.

A queue declared here is **durable and quorum**, which is the default because
`x-queue-type` is part of a queue's identity to the broker: two services sharing
`orders.new` have to declare the same kind, or the second one is refused with
`PRECONDITION_FAILED` and cannot consume at all. See
[queue types](topology.md#queue-types).

## Step 3 — Consume

```ruby
mq.consume("orders.new") do |message|
  puts "got #{message.payload["order_id"]} (#{message.envelope.type})"
  Ack.accept
end
```

The block is the handler, and **it must return an `Ack`**. Four of them exist:

| | |
|---|---|
| `Ack.accept` | done; the broker may forget it |
| `Ack.retry(reason)` | try again, if the policy has an attempt left |
| `Ack.reject(reason)` | this will never work; dead-letter it |
| `Ack.park(reason)` | set it aside for a human |

Returning an `Ack` rather than raising is the point. A handler that raises has
told you *that* something went wrong; a handler that returns `Ack.retry("the
warehouse is not answering")` has told you what to do about it, and the reason
travels onto the message when it is eventually given up on.

A handler that raises anyway is treated as `Ack.retry`, so an unexpected
exception does not silently lose the message. `FatalError` is the exception, and
is treated as `Ack.reject` — raising it is how a handler says "stop now" without
having to know how many attempts remain.

**The block runs on the transport's threads, not this one.** `consume` returns
immediately, handing back a `Consumer`; it does not block.

## Step 4 — Publish

```ruby
sent = mq.publish({ "order_id" => "A-1", "total" => 42_00 },
                  to: "order.placed", exchange: "orders-events",
                  type: "order.placed.v2")

puts "published #{sent.id}"
```

`to:` is the routing key, `exchange:` is where it goes, and everything after that
describes [the envelope](envelope.md) — the metadata AceMQ puts on every message
and every other library in the family reads back.

`publish` returns the envelope **that actually went on the wire**, which is what
the [interceptors](interceptors.md) left rather than what you handed in. That is
the id to log if you are going to log one.

It also **waits for the broker to confirm**. A publish that returns has been
accepted by the broker; one that could not be is a `PublishError` rather than a
silence, because a publisher that returns normally having sent nothing is a
failure you find out about from the consumer's absence.

## Step 5 — All together

```ruby
require "acemq/amqp"
require "socket"

include AceMQ::AMQP

mq = Connection.open("amqp://guest:guest@localhost:5672",
                     origin: "checkout@#{Socket.gethostname}")

Topology.new
        .exchange("orders-events", :topic)
        .queue("orders.new", dead_letter: true)
        .binding("orders.new", "orders-events", "order.#")
        .apply(mq)

done = Queue.new

mq.consume("orders.new") do |message|
  puts "got #{message.payload["order_id"]}"
  puts "  type       #{message.envelope.type}"
  puts "  id         #{message.envelope.id}"
  puts "  from       #{message.envelope.origin}"
  puts "  attempt    #{message.envelope.attempt}"
  puts "  first seen #{message.envelope.first_seen}"
  done << :ok
  Ack.accept
end

mq.publish({ "order_id" => "A-1", "total" => 42_00 },
           to: "order.placed", exchange: "orders-events",
           type: "order.placed.v2")

done.pop
mq.close
```

```
got A-1
  type       order.placed.v2
  id         6f1a2c9e-…
  from       checkout@your-laptop
  attempt    1
  first seen 2026-09-17 09:31:02 -0600
```

`first_seen` is a `Time`, printed here in your local zone. It travels as epoch
milliseconds under `x-acemq-first-seen`, which is what makes it the same instant
when a Java consumer reads it.

The `Queue` is only there so the script can exit once the message has arrived.
A real service would not have one; it would call `consume` at start-up and stay
running.

`mq.close` stops every consumer **and waits for the handlers already running**,
so a message being worked on when the process is asked to stop is acknowledged
rather than returned to the queue for somebody else to redo. It waits twenty
seconds for all of them together — one deadline for the whole drain, sized to
fit inside an orchestrator's grace period — and `mq.close(timeout: 8)` picks
another. See [shutdown](reliability.md#shutdown).

## Step 6 — A thousand of them

Change the last part to publish a batch:

```ruby
orders = (1..1_000).map { |n| { "order_id" => "A-#{n}", "total" => n * 100 } }

envelopes = mq.publish_all(orders, to: "order.placed", exchange: "orders-events",
                           type: "order.placed.v2")

puts "published #{envelopes.size}, first #{envelopes.first.id}"
```

A loop around `publish` would work, and would be a thousand round trips to the
broker: each one waits for its own confirm before the next is written, and nearly
all of that time is a network with nothing on it. `publish_all` hands every
message over first and waits for all the confirms together, which is what
publisher confirms were designed for and what makes a thousand-message publish
take about as long as one.

The envelopes come back **in the order the payloads were given**, whatever order
the broker confirmed them in, so a result can be matched to the payload that
produced it without anybody sorting anything. Each message gets its own envelope
and its own trip through the interceptors, exactly as a single publish does.

Two things to know before you reach for it in earnest:

**It is not atomic**, and no library can make it so — AMQP has no way to publish
a hundred messages such that all or none arrive. What it does instead is say how
much did arrive: a partly failed batch raises a `PublishError` reading `3 of 500
messages were not confirmed; 497 were.` That sentence matters, because a caller
told only "it failed" republishes hundreds of messages that are already on a
queue.

**A thousand messages may be unconfirmed at once**, and no more. A batch larger
than that is written in waves of a thousand rather than all at once, so the array
you hand in can be as long as you like without this process and the broker
holding all of it — `max_outstanding_publishes:` moves the number. What the
ceiling does not bound is time: the whole batch holds this connection's
publishing channel from its first message to its last confirm, so any other
thread publishing on this connection waits that long.

[Publishing](publishing.md#publishing-a-batch) has the rest.

## What you did not have to do

- **Set a message id, a timestamp or a type.** The envelope is built for you, and
  what it carries is identical in all five libraries.
- **Write an acknowledgement.** The `Ack` your handler returned is turned into
  the right AMQP frame, including the parts that only matter when it fails.
- **Handle a decoding step.** JSON in, JSON out; the content type goes on the
  wire and is read back. Nine [codecs](serialization.md) ship, and the connection
  takes any of them.
- **Declare a dead-letter queue by hand**, or work out what arguments a quorum
  queue wants.

## Next

[Tutorial 2 — Surviving failure](tutorial-surviving-failure.md), where the
handler stops working and the interesting half of the library starts.

Or, from the guide: [publishing](publishing.md), [consuming](consuming.md), and
[the envelope](envelope.md) — every field on the thing you printed in step 5.
