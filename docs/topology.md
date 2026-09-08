# Exchanges, queues and bindings

```ruby
Topology.new
        .exchange("orders-events", :topic)
        .queue("orders.new", dead_letter: true)
        .binding("orders.new", "orders-events", "order.#")
        .apply(mq)
```

A `Topology` is a **description**, and `apply` is the only part that talks to a
broker. Building one costs nothing, so it can be a constant loaded at boot;
applying it is what a deployment step or a start-up hook does.

That split is the point of the class. A shape you can build without a connection
is a shape you can print, review, diff and test — and a test that checks what
would be declared needs no Docker.

## Exchanges

```ruby
topology.exchange("orders-events", :topic)
topology.exchange("audit", :fanout, durable: true)
topology.exchange("routing", :headers, arguments: { "alternate-exchange" => "unrouted" })
```

Durable by default. The kind is `:direct`, `:topic`, `:fanout` or `:headers`,
and it is required — there is no sensible default, and guessing one would put
the wrong exchange in the broker under a name that is now taken.

## Queues

```ruby
topology.queue("orders.new")
topology.queue("orders.new", dead_letter: true)
topology.queue("orders.new", dead_letter: true, retry_policy: policy)
topology.queue("replies", durable: false, exclusive: true, auto_delete: true)
```

Durable by default. `dead_letter: true` is doing more than one thing, on
purpose: it puts the dead-letter arguments on the queue itself, adds
`orders.new.dlq`, adds the shared `acemq.dlx` exchange and binds the two
together. Four declarations to get one behaviour right, and the sort of thing
that goes wrong when it has to be typed out — a dead-letter argument pointing at
an exchange nobody declared drops messages silently.

`retry_policy:` adds the rungs that policy needs. The rungs *are*
`policy.schedule`, a finite list of delays known before anything is published,
which is why the topology can declare them up front rather than a consumer
discovering them one failure at a time. See
[retries, dead letters and shutdown](reliability.md) for what a rung is and why
it exists.

Handing the topology the **policy** rather than a list of delays is what keeps
the two from drifting. A queue whose consumer runs a policy the topology was
never told about has rungs that do not match its waits, and the symptom of that
is a retry which quietly never comes back.

## Queue types

**A durable, named queue is a quorum queue** unless something says otherwise —
here, and in `Connection#declare_queue`.

The reason is interop rather than taste. RabbitMQ treats `x-queue-type` as part
of a queue's *identity*, so a queue that exists as a quorum queue answers
`PRECONDITION_FAILED` to anybody declaring it classic — and that anybody is the
second service to start, in whichever language it happens to be written. Java
declares a source queue quorum, Java is the one with deployments, so the other
four libraries declare it quorum too and a Ruby service can share `orders` with
a Java one.

```ruby
topology.queue("orders.new")                        # quorum
topology.queue("scratch", queue_type: :classic)     # classic, because you said so
topology.queue("events", arguments: { "x-queue-type" => "stream" })
```

The kind can be said three ways and they are read in that order: `queue_type:`,
an `x-queue-type` you put in the argument table yourself (which is how
[`Patterns.declare_stream`](patterns.md#streams) declares a stream), and finally
the default. Two of them disagreeing is refused rather than resolved, since
either answer would be a guess at which one was meant.

**A queue that goes away on its own is classic, because it can be nothing else.**
RabbitMQ refuses to replicate an exclusive, auto-deleting or transient queue, so
those stay classic whatever the default says:

```ruby
topology.queue("replies", durable: false, exclusive: true, auto_delete: true)
# declare queue replies (classic, transient, auto-delete, exclusive)
```

Asking for quorum *and* one of those flags is a `QueueTypeError` rather than a
silent downgrade:

```
queue "replies" cannot be a quorum queue while it is exclusive: RabbitMQ only
replicates a queue that outlives the connection that declared it. Leave it
classic, or drop the flag.
```

A classic queue carries **no `x-queue-type` at all**, which is what Java sends
and therefore what the broker has to be told. An argument table that differs
from another library's is a `PRECONDITION_FAILED` for whichever service declares
second, and adding `x-queue-type=classic` where Java sends nothing would be
exactly that difference.

Retry rungs are always classic, said out loud rather than left to the default,
for the same reason: Java, Go, .NET and Python all declare them classic, and a
rung declared quorum here would be refused to every one of them. See
[reliability](reliability.md#the-shape-a-rung-has-on-the-broker).

## Bindings

```ruby
topology.binding("orders.new", "orders-events", "order.#")
topology.binding("audit.all", "audit")            # fanout: no routing key
```

`binding(queue, exchange, routing_key = "")`. Both ends have to be things this
topology declares — a binding to a queue nothing creates is a
[problem](#validation), not a declaration.

## Validation

```ruby
topology.problems   # => ["binding orders.new -> orders-events names a queue nothing declares"]
topology.validate!  # raises TopologyError on the first problem
```

`apply` and `plan` both call `validate!` first, so a topology that cannot be
right never reaches the broker half-declared. What it catches: blank names,
duplicate declarations, an exchange with no kind, and a binding naming a queue
or an exchange the topology does not declare.

What it cannot catch is drift — a queue that already exists on the broker with
different arguments. AMQP reports that as `PRECONDITION_FAILED` when you declare
it, and `apply` passes the refusal on rather than swallowing it. That refusal
means this service and the broker disagree about what a queue is, and carrying
on would leave the disagreement in place with nobody told.

## The plan

```ruby
puts topology.plan
```

```
declare exchange orders-events (topic)
declare exchange acemq.dlx (direct)
declare queue orders.new (quorum, durable, x-dead-letter-exchange=acemq.dlx, x-dead-letter-routing-key=orders.new.dlq)
declare queue orders.new.dlq (classic, durable)
bind orders.new.dlq to acemq.dlx on "orders.new.dlq"
bind orders.new to orders-events on "order.#"
```

An array of strings, produced without connecting to anything. It is a statement
of intent rather than a diff against the broker — AMQP gives a client no way to
enumerate what is there without the management API, and a plan that quietly
guessed would be worse than one honest about what it is. Worth putting in a
deployment log all the same: it is the only place the whole shape appears at
once.

`to_s` gives the same thing with a summary line, and says `invalid:` for each
problem rather than raising, so an object dumped into a log is readable even
when it is wrong.

## Applying it

```ruby
topology.apply(mq)     # or mq.apply(topology), which is the same call
```

Exchanges, then queues, then the bindings between them — the order a broker
needs. It stops at the first failure.

`apply` takes anything answering `declare_exchange`, `declare_queue` and `bind`,
which is a `Connection` in production and can be a fake in a test. See
[testing without a broker](testing.md).

Declaring is idempotent as long as nothing about the declaration changes, so
applying the same topology on every start-up is the ordinary arrangement rather
than something to guard against.

## Naming

The suffixes and the shared exchanges are constants, and they are the same in
Java, Go, .NET and Python:

```ruby
Naming.dead_letter_queue("orders.new")   # => "orders.new.dlq"
Naming.parked_queue("orders.new")        # => "orders.new.parked"
Naming.retry_queue("orders.new", 40)     # => "orders.new.retry.40s"

Naming::DEAD_LETTER_EXCHANGE            # => "acemq.dlx"
Naming::RETRY_EXCHANGE                  # => "acemq.retry"
```

The dead-letter exchange can be pointed elsewhere per topology, because only
this library's own queue arguments name it:

```ruby
Topology.new(dead_letter_exchange: "team.dlx")
```

The retry exchange cannot. It is written into a rung's argument table, and that
table is the thing five libraries have to agree on — a rung declared with
different arguments answers the second service `PRECONDITION_FAILED`, leaving it
unable to consume at all.

## Declaring outside a topology

`Connection` has the single declarations for the cases a topology does not fit —
a tool, a test, a queue whose name is only known at runtime:

```ruby
mq.declare_exchange("orders-events", kind: :topic)
mq.declare_queue("orders.new")                        # quorum, as above
mq.declare_queue("scratch", queue_type: :classic)
mq.bind(queue: "orders.new", exchange: "orders-events", routing_key: "order.#")

mq.queue_exists?("orders.new")
mq.message_count("orders.new")
mq.delete_queue("scratch")
```

Each declaration goes on a channel of its own, because a refused declaration
kills the channel it was made on. Sharing one would mean a single
`PRECONDITION_FAILED` took every later declaration down with it, and the error
nobody could then explain is the second one.

## Next

- [Retries, dead letters and shutdown](reliability.md) — rungs, the retry
  exchange, and what `dead_letter: true` is wiring up
- [Consuming](consuming.md) — the queue has to exist before a consumer starts
- [Patterns: streams](patterns.md#streams) — a queue that keeps what it has
  delivered
