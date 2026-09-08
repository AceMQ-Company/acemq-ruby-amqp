# Testing without a broker

Most of this library has nothing to do with a broker, and the parts most worth
testing are exactly those parts: the envelope, the retry arithmetic, the
topology, the codecs, the patterns.

Be straight about one thing first. **There is no in-memory transport in the
gem**, the way the Go library has `memory://`. What there is instead is a seam
narrow enough that a fake takes twenty lines, and this page shows the one the
library's own specs use.

## The contract, with nothing running

No connection, no broker, no Docker:

```ruby
envelope = Envelope.new(type: "order.placed.v2", origin: "checkout@pod-7")
envelope.to_headers("order.placed")
# => { "x-acemq-id" => …, "x-acemq-type" => "order.placed.v2", … }

Envelope.from_headers(headers, "order.placed").attempt

RetryPolicy.exponential(5, 1, 60).schedule    # => [1.0, 2.0, 4.0, 8.0]
RetryPolicy.exponential(5, 1, 60).next_delay(3, 0, jitter: false)

Naming.dead_letter_queue("orders.new")        # => "orders.new.dlq"
Naming.retry_queue("orders.new", 40)          # => "orders.new.retry.40s"

RetryLadder.for("orders.new", policy).queues
# => ["orders.new.retry.40s", "orders.new.retry.80s", "orders.new.retry.160s"]

Topology.new.exchange("orders-events", :topic)
            .queue("orders.new", dead_letter: true)
            .plan
```

`plan` and `problems` are the two to remember. A topology can be asserted
against without anything being declared anywhere:

```ruby
it "wires the dead-letter queue up" do
  plan = Topology.new.queue("orders.new", dead_letter: true).plan
  expect(plan).to include('bind orders.new.dlq to acemq.dlx on "orders.new.dlq"')
end
```

## The seam

`Connection.new` takes a **transport** rather than a URL:

```ruby
mq = Connection.new(transport: FakeTransport.new,
                    retry_policy: RetryPolicy.exponential(5, 1, 60),
                    telemetry: Telemetry::Registry.new)
```

That is what `Connection.open` does after it has built one, and it is public for
exactly this reason. A transport is anything answering:

| | |
|---|---|
| `publish(exchange:, routing_key:, body:, content_type:, message_id:, headers:, persistent:)` | |
| `subscribe(queue, prefetch:, concurrency:, tag:, arguments:, &handler)` | returns something answering `open?`, `stop`, `close`, `cancel` |
| `declare_exchange(name, kind:, durable:, auto_delete:, arguments:)` | |
| `declare_queue(name, durable:, auto_delete:, exclusive:, arguments:)` | |
| `bind(queue:, exchange:, routing_key:)` | |
| `pull(queue)` | a `Delivery`, or nil |
| `message_count(queue)`, `queue_exists?(name)`, `delete_queue(name)` | |
| `open?`, `close` | |

Nothing above that seam knows what a channel is. Settling travels with the
delivery as `on_ack` and `on_nack` rather than as a delivery tag the consumer
would have to hand back to the right channel — which is precisely what lets a
Hash stand in for a broker.

## A fake that records

The library's own `spec/support/fake_transport.rb` is a broker that is only a
Hash. It records what was published and delivers nothing, which is everything
the retry engine's arithmetic needs:

```ruby
transport = FakeTransport.new
mq = Connection.new(transport: transport, retry_policy: RetryPolicy.fixed(3, 1))

mq.publish({ "order_id" => "A-1" }, to: "orders.new")

expect(transport.published.last.routing_key).to eq("orders.new")
expect(transport.published.last.headers["x-acemq-type"]).to eq("orders.new")
```

`published_to(queue)` is the assertion that gets used most, because the retry
engine's whole observable behaviour is *which queue did this end up in*:

```ruby
expect(transport.published_to("orders.new.retry.40s").size).to eq(1)
expect(transport.published_to("orders.new.dlq").first.headers["x-acemq-error"])
  .to include("gave up after 3 attempts")
```

It can also be told a queue is missing, which is how the
[rung-missing](reliability.md#when-a-rung-is-missing) path is tested without
arranging for a broker to be misconfigured:

```ruby
transport.missing!("orders.new.retry.40s")
```

## A fake that routes

The patterns need more than a recorder. Request and reply is not a shape you can
see without a reply coming back, and a pipeline is not one you can see without
the next stage receiving anything. `spec/support/loopback_transport.rb` routes
in-process: default exchange to the queue of that name, named exchanges by their
bindings.

Its delivery is **synchronous, on the publishing thread**, and that is
deliberate rather than a shortcut. Real brokers are not synchronous, and the
difference buys the thing that matters in a test suite: a test that has to wait
for another thread to notice something is a test that is sometimes flaky and
always slow.

What it cannot show is concurrency, which is what the integration specs are for.

Neither file ships in the gem. Copy them, or write your own against the table
above — the seam is the contract, not the fakes.

## Testing what a handler decides

The narrowest tests need no transport at all, because a handler is a block that
takes a `Message` and returns an `Ack`:

```ruby
handler = ->(message) { warehouse.reserve(message.payload) ? Ack.accept : Ack.retry("no stock") }

message = Message.new(payload: { "sku" => "X-1" },
                      envelope: Envelope.new(type: "order.placed.v2"),
                      routing_key: "orders.new",
                      content_type: "application/json",
                      body: '{"sku":"X-1"}',
                      redelivered: false)

expect(handler.call(message)).to be_accept
```

The patterns are the same shape — they wrap a handler and hand back a handler —
so a pipeline can be exercised the same way:

```ruby
wrapped = Patterns.chain(handler, Patterns.with_idempotency(store))
expect(wrapped.call(message)).to be_accept
expect(wrapped.call(message)).to be_accept    # a duplicate is accepted, not rejected
```

## Asserting on metrics

`Telemetry::Registry` is an in-memory observer, which makes "what did the retry
engine decide" a plain assertion:

```ruby
metrics = Telemetry::Registry.new
mq = Connection.new(transport: transport, telemetry: metrics,
                    retry_policy: RetryPolicy.fixed(3, 1))
# …
expect(metrics[Telemetry::DEAD_LETTERED, queue: "orders.new"]).to eq(1)
expect(metrics[Telemetry::RETRIED, queue: "orders.new"]).to eq(2)
```

See [metrics and health](observability.md).

## When you do need a broker

Some things only a real broker proves: that a message survives the wire, that a
rung's time-to-live actually expires it into the right exchange, that a
`PRECONDITION_FAILED` is raised where this library expects one, and that a TLS
connection which should fail does.

Those specs are tagged `:integration` and skipped unless a broker is named, so a
laptop with no Docker still runs everything else:

```bash
bundle exec rspec                       # the contract and the retry engine

ACEMQ_TEST_BROKER=amqp://guest:guest@localhost:5672 bundle exec rspec
```

The TLS examples need a broker with a TLS listener and the authority that signed
its certificate, and skip when they have neither. See
[security](security.md#testing-it).

The [SQL-backed stores](patterns.md#sql-backed-stores) need no environment for
the ordinary run: they are tested against SQLite in memory, which is a real
database with real transactions, and a stub of a transaction could not test the
one thing the outbox store claims. Naming a PostgreSQL database runs the same
examples against it too, because the dialect is a separate proof from the logic:

```bash
ACEMQ_TEST_POSTGRES=postgres://user:pass@localhost:5432/acemq_test bundle exec rspec
```

## The cross-language fixtures

`spec/fixtures/envelope-fixtures.json` was produced by the Java implementation
and is shared with Go, .NET and Python. A spec reads each one and writes it
back: no header gained, none lost, none renamed.

That file is the definition of "the same wire contract", and it is checked here
rather than assumed. If you are extending the envelope, that spec is the one to
watch — a change that passes everything else and fails this one is a change that
would have been found by another language, in production.

## Next

- [The envelope](envelope.md) — what the fixtures pin
- [Reliability](reliability.md) — the arithmetic the fakes exist to test
- [Patterns](patterns.md) — every one of them is testable this way
