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

Two of them are generated by the Java implementation and carried byte for byte
by Go, .NET, Python and Ruby alike. A [third](#a-third-fixture-for-the-codecs)
belongs to this library and pins the codecs against bytes Java and Go wrote.

`spec/fixtures/envelope-fixtures.json` is the wire contract: a spec reads each
case and writes it back, and no header may be gained, lost or renamed on the way
through.

`spec/fixtures/contract-fixtures.json` is the behaviour contract — the retry
schedule, the queue names and the topology. `spec/contract_spec.rb` reads it and
holds this library to every section of it:

| Section | What is checked |
| --- | --- |
| `retrySchedules` | five named policies, their unjittered delays, which of those go on rung queues, and a table of retry-or-give-up decisions across the attempt and age limits |
| `jitter` | the factor, that it moves a delay both ways, the bounds it stays inside, and that a wait held in the broker is never jittered because it names a queue |
| `brokerWaitThreshold` | thirty rows of delay, threshold and where the wait is spent |
| `naming` | `.dlq`, `.parked` and every rung name |
| `rungArguments` | each rung queue's three arguments — the count as well as the values |
| `topology` | every exchange, queue and binding, and which half declares it |
| `queueTypeDefaults` | quorum for a source queue, classic for the queues this library owns around it, durable throughout |

### Why it is not just a comparison

A spec that reads a number out of the fixture and compares it to the same number
read out of the library proves only that both can parse JSON. So the expectations
are derived wherever they can be: the doubling is checked as doubling, the
threshold table is recomputed from the rule the fixture states in prose before
this library is asked anything, and the rung names are rendered a second time by
an implementation that shares no code with `Naming`.

The reason for all of this is a real one. Java shipped `exponential` with a
multiplier of five and 10% jitter through ten releases while the other four
libraries doubled with 20%, so `exponential(5, 1s, 1m)` meant `1s, 5s, 25s, 60s`
in one library and `1s, 2s, 4s, 8s` in the rest. Every suite was green
throughout, because each library tested its own arithmetic against its own
expectations.

### The disagreements it records

A fixture that only recorded agreement would be a fixture somebody had quietly
edited. These are the places the five libraries do not yet agree, asserted here
as they actually stand rather than smoothed over:

- **Sub-second rung names.** Java renders a 500ms rung `orders.new.retry.500ms`;
  Ruby, Go and Python render `orders.new.retry.0s`. Unreachable behind the
  default thirty-second threshold, and not the same queue either way.
- **No age limit.** Ruby, Go, Python and .NET spell "never give up on age" as
  zero; Java has no such value and uses 365 days. A message exactly a year old is
  dead-lettered by a Java consumer and retried by the other four.
- **The jitter floor.** Java floors a jittered wait at one millisecond; the other
  four floor at zero. It only shows for a delay of a millisecond or two at a
  jitter factor near one.

Who declares the dead-letter queues used to be a fourth entry here. The fixture
marks `acemq.dlx`, `{queue}.dlq`, `{queue}.parked` and the two bindings that
reach them `both`, meaning a topology declares them before anything runs and a
consumer declares them again as it starts. Ruby declared only the retry half,
as Go and .NET did. **ADR-032** settled it in Java's favour and all five
libraries now declare both halves: the union is identical on a broker somebody
set up properly, and the difference only shows on one where a step was missed —
a consumer that gives up republishes to `{queue}.dlq` through the default
exchange, which drops what it cannot route without a word. Losing the evidence
is worst exactly when there is a mistake to find. See
[Reliability](reliability.md#who-declares-what) for the split as it now stands.

### A third fixture, for the codecs

`spec/fixtures/codec-samples.json` is different from the other two in one way
worth knowing: it is not shared with the four other libraries, and it is not
generated by Java alone. It carries one order encoded twelve times — once per
format, by the Java library's codecs and by the Go library's — so that
`spec/codec_formats_spec.rb` can decode bytes those libraries really wrote
rather than bytes this one wrote a moment earlier. `scripts/codec-samples.sh`
in the workspace regenerates it, calling those libraries' own codec classes.

The reason is the same one behind the other two, one level down. A codec that
encodes and decodes its own output proves nothing about reading another
language's message: both halves of the test share whatever the codec got wrong.
Every difference these samples turned up was invisible to a round trip — Jackson
quotes TOML strings with `'` where BurntSushi uses `"`, Jackson wraps a list in
an element of its own where `encoding/xml` repeats the element, and the two YAML
writers indent lists differently.

It records a disagreement too. Java and .NET gate an Avro codec's accept set on
which framing it writes, so a fixed-schema codec refuses
`application/vnd.acemq.avro` and a registered one refuses `avro/binary`; Go
accepts both in either mode. The bytes of the two framings are
indistinguishable, and a fixed-schema codec reading a framed message decodes the
identifier as the first field without throwing — so this library follows Java
and .NET, and the spec says so.

A second disagreement, about how a fixed-schema codec tells the two framings
apart in the bytes, has been **resolved rather than recorded**. Java refused any
body of five or more bytes beginning with `0x00`, in case it was a framed
message, and this library copied the check. It is the wrong rule: a legitimate
Avro body begins with a zero byte whenever its first field encodes to zero — an
empty string, a `0`, a `false`, branch 0 of a union — so the check refused real
messages to catch a framing the content type had already named. The content type
now decides, and the leading-zero guess applies only when nothing was said at
all; Python implemented that first and Java has been changed to match. The
example that pins it encodes an order with an empty `orderId`, whose first byte
really is `0x00`, and reads it back under `avro/binary`.

### Keeping the copies identical

The fixtures work only for as long as every library's copy is the same file. A
copy that has drifted is worse than no fixture at all: the library carrying it
still passes its own suite, agreeing with the wrong file in private.
`scripts/check-fixtures.sh` in the workspace compares all five by checksum and
fails on a single changed character.

## Next

- [The envelope](envelope.md) — what the wire fixture pins
- [Reliability](reliability.md) — the arithmetic the contract fixture pins
- [Patterns](patterns.md) — every one of them is testable this way
