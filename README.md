# AceMQ for Ruby

[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-3.1%2B-CC342D)](#requirements)
[![brokers](https://img.shields.io/badge/broker-RabbitMQ-lightgrey)](#requirements)

A Ruby client for AceMQ messaging over AMQP, speaking the same wire contract as
the [Java](https://github.com/AceMQ-Company/acemq-java-amqp),
[Go](https://github.com/AceMQ-Company/acemq-go-amqp),
[.NET](https://github.com/AceMQ-Company/acemq-dotnet-amqp) and
[Python](https://github.com/AceMQ-Company/acemq-python-amqp) libraries: the same
reserved headers, the same defaults, the same retry arithmetic. A Ruby consumer
reads what a Java producer writes, and the fixtures the Java implementation
produces pin that rather than leaving it to be discovered in production.

> **Status: in build.** The contract layer and the AMQP transport — connect,
> publish, consume, retry, dead-letter — are implemented and tested, against
> the shared fixtures and against a real broker, and so are the [patterns
> above it](#patterns): idempotency, the outbox, request and reply, replay,
> ordering, consumer groups, routing slips, sagas, scheduling, pipelines,
> schemas and streams.
> [TLS and credentials](#tls-and-credentials) are in, including a private
> certificate authority, mutual TLS and
> [development certificates that cannot reach
> production](#development-certificates-and-why-they-cannot-reach-production).
> So are [encrypted bodies](#encrypted-bodies) and an
> [OpenTelemetry adapter](#tracing). Nothing is published to RubyGems yet.

## What is here today

```ruby
require "acemq/amqp"

include AceMQ::AMQP

mq = Connection.open("amqp://guest:guest@localhost:5672",
                     origin: "checkout@pod-7",
                     retry_policy: RetryPolicy.exponential(5, 1, 60))

Topology.new
        .exchange("orders-events", :topic)
        .queue("orders.new", dead_letter: true)
        .binding("orders.new", "orders-events", "order.#")
        .apply(mq)

mq.publish({ "order_id" => "A-1" }, to: "order.placed",
           exchange: "orders-events", type: "order.placed.v2")

mq.consume("orders.new") do |message|
  message.payload          # => { "order_id" => "A-1" }
  message.envelope.attempt # => which delivery this is
  warehouse.reserve(message.payload) ? Ack.accept : Ack.retry("the warehouse said no")
end
```

The contract on its own, with no broker anywhere:

```ruby
# What travels with a message, and what another language will read back.
envelope = Envelope.new(type: "order.placed.v2", origin: "checkout@pod-7")
envelope.to_headers("order.placed")

# The same schedule Java, Go, .NET and Python produce for the same policy.
policy = RetryPolicy.exponential(5, 1, 60)
policy.schedule                      # => [1.0, 2.0, 4.0, 8.0]

Naming.dead_letter_queue("orders.new")   # => "orders.new.dlq"
```

The gem has **no runtime dependencies**. Reading an AceMQ envelope should not
require installing a broker client, so `bunny` is required lazily by the
transport, at the moment a connection is opened, and named in the error if it
is not there. Add it to your own Gemfile to use the transport:

```ruby
gem "acemq-amqp"
gem "bunny", "~> 2.23"
```

### Codecs

JSON, string and bytes, and the five formats the Java and Go libraries also
ship, because a Java or Go service publishing one of those was until now a
message this library could not decode:

```ruby
Codecs.names   # => ["bytes", "json", "string", "toml", "xml", "yaml"]

mq = Connection.open(url, codec: YAMLCodec.new)
mq = Connection.open(url, codec: CompositeCodec.new(JSONCodec.new, YAMLCodec.new))

ProtobufCodec.new(Acme::Order)                             # application/x-protobuf
AvroCodec.of(schema_json)                                  # avro/binary
AvroCodec.registered(registry, subject: "order.placed",    # …/vnd.acemq.avro
                     schema: schema_json)
```

Each reads more content types than it writes — `application/x-yaml`,
`text/yaml`, `text/x-yaml` and `…+yaml` as well as `application/yaml` — because
a producer in another stack uses whichever spelling its own library picked. Only
`JSONCodec` and `BytesCodec` answer for a message with no content type at all.

The gem still declares no runtime dependencies: YAML is Psych, the TOML reader
and writer are written into the gem, and XML, Protobuf and Avro require REXML,
`google-protobuf` and `avro` lazily and name the gem to install. REXML ships
with Ruby but has been a *bundled* gem since Ruby 3.4, so a Bundler process
needs it in its Gemfile:

```ruby
gem "rexml", "~> 3.3"            # for XMLCodec, under Bundler
gem "google-protobuf", "~> 4.29" # for ProtobufCodec
gem "avro", "~> 1.12"            # for AvroCodec
```

**`YAMLCodec` parses with `Psych.safe_load`, never `YAML.load`**, because a
message body is untrusted input and `YAML.load` on one is remote code execution.
`Date`, `Time` and `DateTime` are permitted by default since Java and Go both
write timestamps; `Symbol` and YAML aliases have to be asked for. **`XMLCodec`
refuses a `<!DOCTYPE>` outright**, which is not configurable. See
[docs/serialization.md](docs/serialization.md).

### Encrypted bodies

`EncryptedCodec` wraps any of the above and encrypts what it produced, so the
broker, its disk, its backups and its management interface hold ciphertext:

```ruby
keys  = Keyring.of("orders-2026-09", Keys.from_base64(ENV["ACEMQ_KEY"]))
codec = EncryptedCodec.wrapping(JSONCodec.new, keys)

mq = Connection.open(url, codec: codec)

EncryptedCodec.key_id_of(body)   # => "orders-2026-06", from the bytes, without the key
```

AES-GCM through Ruby's OpenSSL binding, a fresh nonce per message, and the
framing the Java library writes:

```
0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
```

The key identifier is in the clear in front of the ciphertext, which is what
makes rotation possible — a consumer reads which key a message needs rather than
assuming the current one — and the header is the GCM associated data, so an
identifier altered in flight makes the message fail to open rather than opening
as something else. A `Keyring` holds the key that writes and every key that
still has to read.

**Go and .NET write different bytes under the same content type.** Go omits the
magic byte and uses a two-byte length; .NET omits it too and uses AES-CBC with
an HMAC tag. This library interoperates with Java and with nothing else yet.
[docs/serialization.md](docs/serialization.md#the-other-libraries-do-not-agree-about-this-yet)
has the table.

## What is identical, and what is not

**Identical**, because a message crosses languages: the reserved header names
and their types, the defaults applied when they are absent, the retry schedule
arithmetic, the `{queue}.dlq` / `{queue}.parked` / `{queue}.retry.{delay}`
naming, the kind of queue each of those is declared as, and the rules for
giving up.

**Not identical**, deliberately: the API shape. Go gets `ctx`, .NET gets
`IAsyncEnumerable`, Python gets dataclasses, and Ruby gets keyword arguments,
frozen value objects and `?` predicates. Forcing a Java shape onto Ruby
produces a library nobody enjoys using. The contract is portable; the
ergonomics are native.

### The envelope

| Header | |
|---|---|
| `x-acemq-id` | The message identifier, and the default idempotency key |
| `x-acemq-type` | The logical type, falling back to the routing key |
| `x-acemq-version` | Schema version, from 1 |
| `x-acemq-correlation` | Defaults to the id, so a chain has something to copy |
| `x-acemq-causation` | The message that caused this one |
| `x-acemq-attempt` | Delivery attempt, from 1 |
| `x-acemq-first-seen` | Epoch **milliseconds** of the first publish |
| `x-acemq-origin` | `service@host` |
| `x-acemq-error` | Why it was dead-lettered |
| `x-acemq-claim` | Where the payload is, when it is stored outside the message |

Application headers are kept apart from these. A reserved name in your own
headers raises rather than being dropped — silently discarding a header
somebody set is worse than saying no — and unknown `x-acemq-` names from a
newer version of another language's library are not handed back as yours.

### Retry

```ruby
policy = RetryPolicy.exponential(5, 1, 60).give_up_after(6 * 3600)
```

`schedule` shows the delays without jitter, which is what to read when deciding
whether a policy is the one you meant. Jitter moves a delay **both ways**:
one-sided jitter only ever delays, which turns a thundering herd into a slower
thundering herd.

Giving up on **age** as well as attempts is the honest limit when a queue has
been paused — a message can be on attempt one and four days old.

### What the consumer actually does

`Ack.retry` returns the message to the broker with `x-acemq-attempt` advanced.
The count travels **on the message**, because a requeue hands the broker back
the bytes it was given: counting in the consumer instead is wrong the moment a
second one exists, since a message that moves between them is for ever on
attempt one, and a restart forgets everything anyway. The trade is that a
retried message goes to the back of its queue rather than the front.

**Where the delay is waited depends on how long it is.** Under 30 seconds by
default, the consumer waits, holding one prefetch slot. At or above that, the
message is published into a rung queue — `{queue}.retry.{delay}` — whose
`x-message-ttl` is the delay and whose dead-letter target is the queue it came
from, so the broker returns it when the time is up with nothing running:

```ruby
policy = RetryPolicy.exponential(6, 10, 300)   # 10s, 20s, 40s, 80s, 160s

Topology.new
        .queue("orders.new", dead_letter: true, retry_policy: policy)
        .apply(mq)                             # declares .retry.40s, .retry.80s, .retry.160s

mq.consume("orders.new", retry_policy: policy) { |message| ... }
```

The rungs *are* `policy.schedule`, a finite list known before anything is
published, which is why the topology can declare them up front rather than a
consumer discovering them one failure at a time. Hand `queue` the policy and it
works them out; `retry_threshold:` moves the line, on both the topology and the
consumer, and the two have to agree.

### The shape a rung has on the broker

The same in Java, Go, .NET, Python and Ruby, because two services consuming one
queue declare the same rung by name — and a rung declared with different
arguments answers the second one `PRECONDITION_FAILED`, leaving it unable to
consume at all:

```
acemq.retry                        direct, durable
acemq.dlx                          direct, durable

orders.new                         quorum, durable
                                   x-queue-type               quorum
                                   x-dead-letter-exchange     acemq.dlx
                                   x-dead-letter-routing-key  orders.new.dlq

orders.new.retry.40s               classic, durable
                                   x-message-ttl              40000
                                   x-dead-letter-exchange     acemq.retry
                                   x-dead-letter-routing-key  orders.new

orders.new.dlq                     classic, durable
orders.new.parked                  classic, durable

orders.new       -> acemq.retry -> orders.new          (an expired rung comes home)
orders.new.dlq   -> acemq.dlx   -> orders.new.dlq
orders.new.parked-> acemq.dlx   -> orders.new.parked
```

The default exchange would route a rung home by queue name with no exchange and
no binding at all, and that is genuinely one fewer thing to forget. It is not
what is done, for two reasons. The first is agreement: five libraries have to
declare one table, and this is the one Java has always declared. The second is
that the default exchange cannot be bound, listed or given a policy, so the path
a retry takes home would exist only inside a queue argument, invisible from the
broker; `acemq.retry` puts that path in the topology where it can be reviewed,
and lets a service be granted write on one exchange rather than on every queue
in the vhost.

The cost is the binding, and it is real: an expired message with nothing bound
to carry it is dropped silently. So the binding is never optional and never
deferred — `RetryLadder#declare` and `Topology#retry_ladder` each declare the
exchange, the rungs and the binding together, and a consumer declares them again
before it subscribes.

A consumer declares `acemq.dlx`, `{queue}.dlq`, `{queue}.parked` and the two
bindings to them as well, and does so whether or not it has a retry policy. It
republishes to `{queue}.dlq` through the default exchange when it gives up, and
the default exchange drops what it cannot route without a word, so on a broker
where the topology was never applied the dead letter that would have reported
the mistake is the thing that disappears. Everything in the table above except
`orders.new` itself is therefore declared twice on a properly deployed broker,
with the same arguments both times, which is a duplicate declaration and not a
`PRECONDITION_FAILED`. The source queue is the one thing a consumer never
declares: it belongs to whoever set the service up, and guessing its type wrong
is a `PRECONDITION_FAILED` that stops the consumer starting at all. See
[who declares what](docs/reliability.md#who-declares-what).

The names live in `Naming::RETRY_EXCHANGE` and `Naming::DEAD_LETTER_EXCHANGE`.
The dead-letter exchange can be pointed elsewhere per topology
(`Topology.new(dead_letter_exchange: "team.dlx")`), because only this library's
own queue arguments name it. The retry exchange cannot, because it is written
into the rung's argument table, which is the table everybody has to agree on.

### A source queue is a quorum queue

`Topology#queue` and `mq.declare_queue` declare a **durable quorum queue**, the
same default as `declareQueue` in Java, and for the same reason the rung table
is identical: `x-queue-type` is part of a queue's identity to the broker. A Java
service declaring `orders` as quorum and a Ruby service declaring it as classic
do not disagree politely — whichever starts second is answered
`PRECONDITION_FAILED` and consumes nothing at all. A quorum queue is also the
answer to the failure a classic queue turns into lost messages: it is replicated,
so it survives losing the node its leader was on.

**Three kinds of queue stay classic, deliberately**: the retry rungs,
`{queue}.dlq` and `{queue}.parked`. Java declares all three classic, so this
library does too — and a rung's whole behaviour is a time-to-live expiring into
an exchange, which is the plainest thing a classic queue does.

**Anything exclusive, auto-deleting or transient is classic because it can be
nothing else.** RabbitMQ refuses to replicate a queue that goes away on its own,
so a health probe's queue and a generated reply queue stay classic whatever the
default says. Asking for both at once — `queue_type: :quorum` with
`exclusive: true` — raises `QueueTypeError` here rather than reaching the broker,
whose own refusal is `invalid property 'exclusive-owner'` and mentions neither
quorum queues nor the flag that caused it.

```ruby
Topology.new
        .queue("orders.new", dead_letter: true)   # quorum; .dlq classic
        .queue("scratch", queue_type: :classic)   # classic, because you said so
        .apply(mq)

Patterns.declare_stream(mq, "events")             # x-queue-type: stream, untouched
```

> **Upgrading:** a queue that already exists as classic **cannot be redeclared as
> quorum**. The broker refuses the declaration, and there is no conversion. Drain
> it and recreate it under the new type, or keep it classic explicitly with
> `queue_type: :classic` until you can.

Ruby keeps its own 30-second threshold, which Java does not have: Java gives
every delay in a schedule a rung. Below 30 seconds a wait lost to a restart
costs seconds, and a queue per rung of a schedule that finishes in the time it
takes to notice is not worth what it costs the broker.

A consumer that sleeps through a five-minute backoff loses the whole wait when
it restarts — the broker redelivers at once — which is a correctness bug rather
than a throughput one. Below the threshold, a lost wait costs seconds and a
queue per rung is not worth it. Jitter applies only below the threshold; above
it the spread comes free, because each message's time-to-live starts when it
enters the rung, so a fleet that failed over ten seconds is released over ten
seconds.

Per-message TTL is never used, and it is worth saying why, because it looks like
the flexible answer: RabbitMQ expires messages only from the head of a queue, so
one long wait sitting at the front holds back every shorter one behind it, and
the delays that come out bear no relation to the ones that went in.

When the policy has no attempt left — or the message is older than the policy
allows, or the handler marked the reason `FatalError` — the message is
republished to `{queue}.dlq` with the reason in `x-acemq-error`, and the
original is then acknowledged. Acknowledging a failure looks wrong and is what
makes it reliable: the message is already safely somewhere else, so the original
is a copy that has been dealt with. Rejecting it instead would either requeue it
into a hot loop or hand it to whatever dead-lettering the queue happens to carry
— and neither of those can write down *why*, which is the one thing whoever
finds it needs.

A body no codec can read goes to `{queue}.parked` rather than `{queue}.dlq`. A
message that failed five times and a message nothing could read are different
problems, and mixing them means somebody sorts them by hand.

Without a policy, `Connection` uses `RetryPolicy.none` — one delivery — so a
retry against an unconfigured connection dead-letters immediately. That is a
great deal easier to explain than a message going round the broker as fast as it
can be handed back.

## Interceptors

The seam for the things every message in an organisation needs and no library
can guess: a tenant, a trace context, a log scope, a size limit, a metric.
Without one they end up copied into every call site, where one of them is
always the one that forgot.

```ruby
mq.intercept_publish { |context| context.set_header("tenant", Current.tenant) }

mq.intercept_consume do |context|
  raise FatalError, "not our tenant" unless serves?(context.envelope.headers["tenant"])
end
```

A `PublishContext` is the message before it is encoded — `exchange`,
`routing_key`, `envelope`, `payload` — and every one of them can be changed, so
an interceptor can redirect a message as well as decorate it, and can rewrite
the payload while it is still a Ruby object rather than patching bytes. A
`ConsumeContext` is the message after it is decoded and before the handler sees
it: `queue`, `envelope`, `payload`, `body`, `content_type`, `redelivered?`. What
an interceptor leaves on the envelope is what the handler receives *and* what
any dead letter is written with — a header the handler saw and the dead-letter
queue did not would be missing exactly when somebody goes looking for it.
`set_header` still refuses the reserved `x-acemq-` names.

A block is the common case. An object is the full one, answering whichever of
these it cares about:

| publishing | consuming |
|---|---|
| `before_publish(context)` | `before_handle(context)` |
| `after_confirm(context)` | `after_handle(context, ack)` |
| `on_error(context, failure)` | `on_error(context, failure)` |
| `order` | `order` |

**Raising means different things in different places, on purpose.** From
`before_publish` it *stops the publish* and the caller sees the exception —
that is the point of intercepting rather than observing, and a message that
must not go out is stopped once, here, rather than in every publisher. From
`before_handle` the handler never runs and the delivery is treated exactly as a
failed handler would be: retried, then dead-lettered. An interceptor that
refuses a message has to be willing for that message to reach the dead-letter
queue, which is the honest outcome — the alternative is acknowledging something
nothing processed. `FatalError` still means what it means. From `after_confirm`,
`after_handle` or `on_error` it is reported on stderr and otherwise ignored: the
message has been sent or the delivery settled, and letting the exception out
would report a successful publish as a failed one.

Lower `order` runs first, equal orders run in registration order, and the way
out of a handler is reversed, so a pair that opens something on the way in and
closes it on the way out nests properly.

Nothing here needs anything private. An interceptor is registered through a
public method and handed a context whose every field is public, which is the
same rule the patterns follow and the only way to know a seam is wide enough.
An interceptor is called on whatever thread is publishing or handling, so one
that keeps state has to be safe to call from several at once.

## Telemetry and health

```ruby
metrics = Telemetry::Registry.new
mq = Connection.open(url, telemetry: metrics)

# whatever already answers HTTP in this process
get("/acemq-metrics") { metrics.to_prometheus }
get("/acemq-health")  { report = mq.health; [report.up? ? 200 : 503, report.to_h.to_json] }
```

The metric names are shared with Java, Go, .NET and Python, so a dashboard or an
alert written for one service reads the same against the next:

| | |
|---|---|
| `acemq.messages.published` | by `exchange` |
| `acemq.messages.publish.failed` | including a publish an interceptor refused |
| `acemq.messages.consumed` | by `queue`, counted on the way in |
| `acemq.messages.accepted` / `.retried` / `.rejected` | what handlers decided |
| `acemq.messages.dead.lettered` | out of attempts, too old, or refused fatally |
| `acemq.messages.parked` | nothing could decode it |
| `acemq.handler.duration` | seconds, handler and interceptors together |
| `acemq.messages.in.flight` | a gauge, per queue |
| `acemq.retry.rung.missing` | see below |

**No dependency on a metrics gem.** An observer is anything answering `count`,
`observe` and `gauge`; depending on one would put every service using this
library on the same one, and that choice belongs to the application.
`Telemetry::Registry` is a working in-memory implementation with a
`to_prometheus` renderer for when the numbers themselves are what is wanted. It
is a string and not a Rack app on purpose — this library has no web framework
and should not choose one. Serve it on a port the ingress does not publish: what
a service publishes and how long its handlers take is more than an anonymous
caller should be able to learn.

Anything an observer raises is swallowed and reported once per metric on stderr.
A metrics backend that is down is not a reason to stop delivering messages.

### Tracing

Counters say how many messages failed. A trace says which one:

```ruby
tracing = Telemetry::OpenTelemetry.new
tracing.install(mq)                      # registers on both sides of the connection

tracing.request("pricing.quote") { requester.call(order) }
```

Spans are `<destination> publish` (PRODUCER), `<queue> process` (CONSUMER) and
`<destination> request` (CLIENT — that span waits for an answer, so its duration
means something different), with the OpenTelemetry messaging attributes the Java
adapter writes. The trace travels in `traceparent` and `tracestate`, the W3C
names, deliberately **not** `x-acemq-` prefixed: other tooling already knows
them, and the Java library writes the same two, so a Ruby consumer joins a Java
producer's trace with neither side configured for the other.

The consumer span's parent comes out of **the message's own headers**, not out
of ambient context. That join, across processes and minutes, is the entire point
of tracing a message system.

`unroutable`, `failed` and `dead_lettered` make a span an error; `acked`,
`retried` and `rejected` do not — a message that will be tried again has not
failed yet. Retries, dead letters, outbox failures and finished pipeline runs
are *events* on the span already open, because a zero-length span at the end of a
trace adds a row and no information.

`opentelemetry-api` is not a runtime dependency. It is required at the moment an
adapter is built and names itself when it is absent, the way bunny does:

```ruby
gem "opentelemetry-api", "~> 1.8"
```

See [docs/observability.md](docs/observability.md#tracing).

**`acemq.retry.rung.missing` is worth an alert.** A retry long enough to be
handed to the broker checks that its rung queue is really there before
publishing into it — a publish into a queue nobody declared is dropped without a
word, and a retry that simply stops existing is the one failure nothing else
here would show. When the rung is missing the wait happens in the consumer
instead, so nothing is lost; what is lost is the reason the rung exists, since a
restart mid-wait now turns a five-minute backoff into none. The check is one
round trip per rung for the life of a consumer, and only on the retry path.

`mq.health` returns a report: `:up`, `:degraded` or `:down`, with `parts`
carrying how many consumers there are, how many are still running, and how long
the broker took to answer. The broker is checked by declaring a queue and
deleting it again, because that is the cheapest thing AMQP offers that actually
proves the round trip — an open socket answers the same as a healthy broker
right up until something is asked of it. A **consumer that has stopped under a
live connection is `:degraded`**, not `:down`: the process can still publish and
its other consumers still work, so failing the probe would take out something
doing most of its job, but a queue with nothing reading it is a real fault and
has to be visible. `Health.aggregate` combines the connection's check with the
application's own — anything answering `name` and `check` — runs them on
threads, and takes the worst answer.

## TLS and credentials

An `amqps://` URL is encrypted and the broker is verified against the machine's
trust store. That is the whole configuration for a broker whose certificate came
from a public authority:

```ruby
mq = Connection.open("amqps://broker.example.com:5671",
                     credentials: Credentials.from_env)
```

`Credentials` exists so the password does not have to be in the URL. A URL is the
one piece of configuration that gets printed — into error messages, structured
logs, `ps` output, whatever the deployment tool echoes back — and a password that
has been through any of those has to be rotated. Passed separately it never takes
the trip, and the object itself renders as `username="app" secret=[REDACTED]`
through `inspect`, `to_s` and `%p` alike, so dumping the object that holds it
produces nothing worth stealing.

```ruby
Credentials.of(username: "app", password: ENV.fetch("MQ_PASSWORD"))
Credentials.from_env(username_variable: "MQ_USER", password_variable: "MQ_PASSWORD")
Credentials.from_file("/run/secrets/mq")     # a mounted Kubernetes or Docker secret
Credentials.token(oauth_access_token)        # RabbitMQ's OAuth 2 mechanism

# A block is called at connection time rather than at start-up, which is what a
# secret rotated underneath a running process needs.
Connection.open(url, credentials: -> { Credentials.from_file("/run/secrets/mq") })
```

A broker with its own certificate authority — which is most brokers that are not
on the public internet — is described by a `Security`:

```ruby
mq = Connection.open("amqps://broker.internal:5671",
                     security: Security.verified(certificate_authority: "certs/ca.pem"),
                     credentials: Credentials.from_env)
```

Naming an authority narrows trust to that authority alone; the system store is
then not consulted at all. That is the point. A certificate from a public
authority is not evidence that the thing answering is *your* broker, and the
hundreds of authorities a machine trusts by default are hundreds of ways to be
wrong.

For a broker that authenticates clients by certificate rather than by password —
RabbitMQ's `EXTERNAL` mechanism, or any listener configured with `verify_peer`
and `fail_if_no_peer_cert` — add the client's own pair. They go together, and one
without the other is refused where it is configured rather than at the handshake,
because a handshake failure names neither file:

```ruby
Security.verified(certificate_authority: "certs/ca.pem",
                  certificate: "certs/client.crt", key: "certs/client.key")
```

There is one more constructor and it is deliberately awkward to reach:

```ruby
Security.without_verifying_the_broker(because: "the CI broker's certificate is thrown away nightly")
```

It encrypts and then accepts any certificate at all, which means the traffic
cannot be read by somebody watching the network and nothing stops that somebody
from *being* the broker: they present whatever certificate they like, the
connection opens, and it hands over the login and every message afterwards —
encrypted the whole way, to them. There is no symptom. The long name is so it
cannot be typed by accident or skimmed past in a review, and `because:` is
required so the circumstance ends up in the code rather than in somebody's memory
of a conversation. Prefer `certificate_authority:`, which is about four seconds
more work and is correct.

`Security.disabled` is plaintext, which is what `amqp://` already means and what
a broker on the same machine can reasonably have.

### Why this is a class and not three keyword arguments

bunny does not verify the broker's certificate when it is given a URL. Not
"verifies weakly" — does not verify. A URL string is parsed by `AMQ::Settings`,
which merges in its own defaults, and one of those defaults is `verify: false`;
bunny reads that as an explicit instruction and sets `VERIFY_NONE`. So
`Bunny.new("amqps://broker:5671")` encrypts the traffic, accepts a certificate
the connecting process could have made up thirty seconds ago, and reports itself
as `tls?` throughout. Nothing warns, because from bunny's side somebody asked for
this.

Every mode here therefore states `verify_peer` outright rather than leaving it
unsaid, and the integration spec proves the difference the only way it can be
proved: by connecting to a broker whose certificate does not check out and
requiring that the connection *fail*. bunny on its own opens it.

The same reach fixes a quieter one. bunny pins its TLS context's minimum and
maximum version to the same constant, defaulting both to TLS 1.2, so a broker and
a client that could have agreed on 1.3 settle for 1.2; `Security#configure` lifts
the ceiling before the session starts.

### Development certificates, and why they cannot reach production

```console
$ ./scripts/acemq-certs.rb --out .tls --broker localhost --days 30
```

Writes a certificate authority, a broker certificate and a client certificate,
under the same names Go's `acemq-certs` and .NET's `AceMq.Amqp.DevCerts` write,
plus a `rabbitmq.conf` that serves TLS from them. `DevelopmentCertificates`
is the same thing from Ruby. Keys are `0600` and everything is short-lived,
because a development certificate that never expires is one that outlives the
reason it was created.

Every certificate they generate carries `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST`
in its subject organisation, and **this library refuses one that does, however
trust is configured — unverified mode included**. A self-signed authority that
drifts into production is *worse* than no encryption, because everything looks
protected and nothing is verified. Both halves are checked: an authority or
client certificate configured in this process is refused when the connection is
made, and one the broker presents is refused during the handshake. Java, Go and
.NET refuse the same marker.

```ruby
Security.verified(certificate_authority: ".tls/ca.crt")
        .allowing_development_certificates
```

A separate, visible step rather than a keyword, for the same reason
`without_verifying_the_broker` has a long name: it has to be legible in a diff.
It weakens nothing else — verification stays on and a certificate that does not
verify is still refused. See [docs/security.md](docs/security.md#development-certificates).

## Patterns

The things everybody writes on top of a message queue, written once. Required
separately, because none of it is protocol and the core stays the contract and
the transport:

```ruby
require "acemq/amqp/patterns"
```

Most of them wrap a handler and hand back a handler. That goes to `consume`
unchanged, so the retry policy, the dead-lettering and the envelope are all
still whatever you configured — a pattern that took over the consumer would have
to reimplement them, and then there would be two retry engines to keep in step.

| | |
|---|---|
| [Idempotency](#idempotency) | do a message's work once, however often it arrives |
| [Outbox](#outbox) | decide to send and send, with no gap in between |
| [Claim check](#claim-check) | keep a large payload off the broker |
| [Request and reply](#request-and-reply) | ask a question and wait for the answer |
| [Replay](#replay) | put dead letters back, once the fix is out |
| [Ordering](#ordering) | keep some messages in order without serialising all of them |
| [Consumer groups](#consumer-groups) | start a set of workers together, and stop them together |
| [Routing slips](#routing-slips) | let the message carry its own itinerary |
| [Sagas](#sagas) | undo the steps that worked when a later one does not |
| [Scheduling](#scheduling) | deliver a message later, without a scheduler |
| [Pipelines](#pipelines-and-middleware) | wrap a handler; chain one service to the next |
| [Schemas](#schemas) | remember what a message used to look like |
| [Streams](#streams) | a queue that keeps what it has delivered |
| [SQL-backed stores](#sql-backed-stores) | the three of those, in a database that outlives the process |

### Idempotency

```ruby
store = Patterns::InMemoryIdempotencyStore.new(window: 6 * 3600)

mq.consume("orders.new", &Patterns.idempotent(store) do |message|
  warehouse.reserve(message.payload)
  Ack.accept
end)
```

A duplicate is **accepted**, not rejected: the work was done, so the message has
been handled, and dead-lettering it would raise an alarm about something that
went right. A handler that does not accept has its key forgotten, so its retry
can actually run.

`key:` takes the key from the payload instead of the message id, for when two
different messages carry the same order and doing the order twice is the thing
to prevent.

A store is anything answering `first_time?(key)` and `forget(key)`, and
`first_time?` has to be atomic. `InMemoryIdempotencyStore` is right behind one
worker and wrong the moment there are two — each has its own memory, so both are
told they are first. The store worth having is your own database, written in the
same transaction as the work; that is also the only arrangement that closes the
gap between the handler finishing and the acknowledgement reaching the broker,
which is why this is a guard against duplicates rather than exactly-once.

### Outbox

```ruby
store = Patterns::InMemoryOutboxStore.new

db.transaction do
  orders.insert(order)
  store.add(Patterns.record(mq, event, to: "order.placed",
                            exchange: "orders-events", type: "order.placed.v2"))
end

relay = Patterns::OutboxRelay.new(mq, store, interval: 1).start
```

A service that writes to a database and then publishes has two things that can
fail independently. Crash between them and the work is committed with nobody
told; publish first and fail to commit, and the world has been told about
something that did not happen. Writing the message into the same transaction as
the work removes the gap — both commit or neither does — and the relay publishes
what was committed.

A record holds encoded bytes rather than an object, because it outlives the
process that wrote it and the class may not survive the deployment that happens
while it waits. Its envelope is built by the same rules `publish` uses, so a
message that went through the outbox is indistinguishable on the wire from one
that did not.

The relay is deliberately at-least-once: a record is removed only after the
broker has confirmed it, so a crash in between sends it again. Consumers of
anything sent this way need to be idempotent, which is why the pattern above is
in the same library. Removing first would lose messages instead, and an absence
cannot be recognised the way a duplicate can.

`sweep` is public, so an application can flush its outbox at the end of a
request rather than up to an interval later, and a test can drive a relay
without waiting for a tick. A store is anything answering `add`, `pending` and
`mark_published`, and it is only worth having if `add` can join the caller's
transaction — a store that opens its own connection has the gap back, in a
place that looks like it has been dealt with.

### Claim check

```ruby
store = Patterns::FilesystemClaimCheckStore.new("/mnt/claims")
checked = Patterns::ClaimCheckCodec.wrapping(JSONCodec.new, store)

mq = Connection.open(url, codec: checked)
mq.publish(document, to: "document.stored")   # 40 MB does not go near the broker
```

A scanned medical report is tens of megabytes. Putting it on a queue fills the
broker's memory, copies it to every bound queue, makes a dead-letter queue
impossible to inspect, and turns a broker into a filesystem with worse tools.
What travels instead is a claim check — the payload goes to a store, and the
message carries the key.

Below the threshold the payload travels inline, exactly as it would without this
codec: offloading a two-hundred-byte message turns one broker round trip into a
store round trip *and* a broker round trip, so an unconditional claim check
makes the common case slower to fix the rare one. The default is 64 KiB, and
`threshold: 0` offloads everything.

Three bytes say which of the two a message is:

```
0xAC  0x01  0x00  payload   inline, and identical to what the delegate wrote
0xAC  0x01  0x01  key       a claim check
```

The framing carries that rather than a header, because a header can be stripped
by a shovel or a federation link and the body cannot. What follows the marker on
a claim check is the store's key as bare UTF-8 — no URI, no scheme — and these
are the same three bytes and the same 64 KiB the Java library writes, so a Ruby
consumer pointed at the same store reads what a Java publisher checked in. A
body with no framing at all is read as the delegate would read it, which is what
makes it safe to put this codec in front of a queue that already has messages in
it.

`ClaimCheckCodec.key_of(body)` reads which object a message needs without
fetching it, which is the difference between a five-minute check of a
dead-letter queue and restoring a backup.

A store answers `put`, `get` and `delete`; `get` returns `nil` for a key it no
longer holds, and the codec turns that into a `DecodeError` explaining that the
store's retention has to outlast every queue, every dead-letter queue and any
replay somebody might do by hand. `InMemoryClaimCheckStore` is for tests only —
it holds payloads in the publisher's own memory, so a consumer in another
process finds nothing. `FilesystemClaimCheckStore` renames a temporary file into
place so a fast reader never sees a truncated payload, and checks a key from the
wire before it becomes a path segment.

### Request and reply

```ruby
Patterns.serve(mq, "price.requests") do |message|
  { "price" => catalogue.price(message.payload["sku"]) }   # the answer, not an Ack
end

prices = Patterns::Requester.new(mq, to: "price.requests", timeout: 5)
prices.call({ "sku" => "X-1" })   # => { "price" => 1299 }
prices.close
```

Messaging is asynchronous and this is a synchronous shape drawn on top of it,
which is a real cost rather than a free convenience: a caller blocked on a reply
holds a thread, a connection and a deadline, and a queue that backs up turns
into a service that stops responding. Reach for it where a caller genuinely
cannot go on without the answer, and publish an event otherwise.

A requester is meant to be kept and reused — it holds a queue and a consumer,
so one per request means a queue per request. Without `reply_to:` it generates
an exclusive, transient, auto-deleting queue that goes away with the process; a
reply queue that outlived its requester would collect answers nobody is waiting
for. That one is classic, necessarily — the broker replicates nothing that
disappears with its connection. A named `reply_to:` queue is an ordinary durable
queue and gets the ordinary quorum default, so naming a queue a topology also
declares is safe.

The responder's block returns the answer rather than an `Ack`, and raising sends
the failure back to the caller: somebody blocked on a reply should learn that it
failed rather than wait out the timeout. Having answered, the request is settled
rather than retried, because replying and then retrying would answer twice.

A timeout says an answer did not arrive. It says nothing about whether the work
was done, which is why a request that changes anything wants an idempotent
responder.

Two headers carry this, `acemq-reply-to` and `acemq-error`. They are application
headers on purpose: the `x-acemq-` namespace belongs to the engine and is kept
away from what a handler sees, so a responder could never read them if they
lived there.

### Replay

```ruby
result = Patterns.replay(mq, from: "orders.new.dlq", exchange: "orders-events",
                         limit: 500) do |envelope, _body|
  envelope.error.include?("timeout")
end

result.to_s   # => "moved 37, skipped 463 (drained)"
```

The thing somebody actually does at three in the morning: a dead-letter queue
has two thousand messages in it, the fix is deployed, and they need to go back
through — but not all of them, and not silently. The block decides which go, so
a replay can be done in stages; `reason` is `:drained`, `:limit` or `:deadline`,
because "moved 500" means something quite different when the limit was 500.

Messages the block declines are **held unacknowledged** for the length of the
pass rather than returned one at a time. Returning one immediately does not
work: the broker puts it back at the head of the queue, so the next read hands
over the same message and everything behind it is never seen. The broker still
has the held ones, so a tool that dies half way through returns them rather than
losing them.

**A replayed message goes back on attempt one**, with `x-acemq-error` cleared.
Anything else does nothing that can be seen from outside: a message
dead-lettered on the last attempt of a five-attempt policy arrives back on
attempt five, the consumer gives up on it before the handler is called, and two
thousand messages move from the dead-letter queue to the dead-letter queue. Pass
`restart: false` to put back exactly what was there — for an audit, or for a
queue read by something that counts attempts itself. The identity is untouched
either way: same id, same correlation, same `x-acemq-first-seen`, so giving up
on **age** still applies, which is right — the fix was for the bug, not for the
clock.

Each replayed message is stamped with `acemq-replayed-from`, `acemq-replayed-at`
and `acemq-replay-count`, so a consumer that needs to treat them differently
can and one that does not is unaffected. A message is acknowledged only after
the broker has confirmed the new copy: a crash in that gap replays it twice,
which is the right way round for a dead-letter queue.

Replaying a queue through the default exchange with no routing key is refused
rather than allowed to loop. A dead letter's routing key is the dead-letter
queue, so it would publish every message straight back onto the queue it was
read from, for ever, and the only sign would be a queue that never empties.

### Ordering

```ruby
mq.consume("orders.new", concurrency: 16,
           &Patterns.ordered("x-order-id") { |message| apply(message.payload) })
```

A queue delivers in order and a consumer with concurrency above one stops
honouring that. Usually the right trade; the wrong one where a later message
about the same thing must not overtake an earlier one — an "order cancelled"
arriving before the "order placed" it cancels. This buys ordering per key while
keeping concurrency across keys.

The key is a header name, or anything callable for a key that lives in the
payload; `Patterns.by_correlation` keeps one business action's messages in
sequence. A message whose key comes out empty is handled with no ordering,
because there is nothing to order it against.

**What it does not do**: it orders the handling of messages already delivered.
It cannot reorder ones the broker delivered out of order, and with several
consumers on one queue it orders only within each process. Ordering across
processes is a routing decision — a consistent hash exchange, or a queue per
partition:

```ruby
Patterns.partitioned_routing_key("orders", order_id, 8)   # => "orders.3"
```

`Patterns.partition` is FNV-1a rather than Ruby's own `hash`, and that is the
point: Ruby randomises string hashes per process, so two workers would disagree
about where a key belongs, and so would a Go publisher and a Ruby consumer.

### Consumer groups

```ruby
group = Patterns::ConsumerGroup.new(mq, "orders.new", size: 4) do |message|
  place(message.payload)
  Ack.accept
end

at_exit { group.close }
```

Starting workers by hand means remembering to stop every one, and a partial
shutdown leaves messages held by a consumer nobody is waiting for. A group is
also sized from configuration, which is the number most often changed after a
service is running.

**Concurrency, or a group?** `concurrency:` runs several handlers on one
consumer and one channel. A group runs several consumers, each with its own
channel and prefetch. Reach for the group when handlers are slow enough that
one channel's prefetch becomes the limit, or when a fair share across processes
matters: the broker round-robins between consumers, so four here compete evenly
with four in another instance where one consumer with concurrency four would
not.

If a later consumer will not start, the ones already running are stopped before
the failure is passed on — a half-started group holds messages nothing is going
to handle. Closing stops every consumer even when one refuses, and raises the
refusal afterwards.

### Routing slips

```ruby
slip = Patterns::RoutingSlip.new
                            .step("orders-events", "order.validate", name: "validate")
                            .step("orders-events", "order.charge",   name: "charge")
                            .step("orders-events", "order.ship",     name: "ship")

slip.start(mq, order)

# in the charging service
mq.consume("charge-queue", &Patterns.follow_slip(mq) do |message|
  charge(message.payload)     # the payload to send onwards
end)
```

The alternative to a central orchestrator: the route is decided once, by
whoever started the work, and travels with the message as an
`acemq-routing-slip` header. Each service does its part and sends the message
to the next stop.

What it costs: no single place says what the whole route is at runtime, so a
route that is wrong is discovered one hop at a time. Worth it when the steps
vary per message, and not worth it when every message goes the same way — a
fixed chain of consumers is simpler and easier to follow.

Completed steps are carried rather than dropped, so a slip that fails half way
says how far it got, which is exactly what whoever finds it in a dead-letter
queue is asking. The message is accepted only once the next one is out, so a
failure to publish retries the step — which is why a step that changes anything
should be idempotent. A slip that will not parse is fatal rather than retried:
it will not parse next time either.

### Sagas

```ruby
booking = Patterns::Saga.named("place-order") do |saga|
  saga.step("take-payment") { |order| payments.charge(order) }
      .compensate_with      { |order| payments.refund(order) }
  saga.step("reserve-stock") { |order| inventory.reserve(order) }
      .compensate_with       { |order| inventory.release(order) }
  saga.step("book-courier") { |order| couriers.book(order) }
end

result = booking.run(order)
result.compensated?   # a step failed and the earlier ones were undone
result.failed_at      # "book-courier"
result.unresolved?    # something could not be undone -- alert on this
```

A sequence of steps where each one knows how to undo itself. If `book-courier`
raises, the stock is released and the payment refunded, in that order, and the
result says so. Reverse order because that is the order the world was changed
in. A step with no compensation is skipped rather than refused: one that only
read something needs no undoing.

Nothing here touches a broker. It returns a result rather than raising, because
a failed saga is not an exceptional condition to a caller that has to decide
what happens next. When a compensation itself fails the remaining ones still
run — stopping leaves more undone than continuing — and the step is collected
into `unresolved`, which is the set of real-world effects a person now has to
reconcile.

Not a distributed transaction: after `take-payment` the money really has moved,
and the refund is a new fact rather than an erasure of the old one. And not
durable — a crash midway leaves it half-applied with nothing to resume it.

### Scheduling

```ruby
Patterns::Scheduler.on(mq) do |scheduler|
  scheduler.in(4 * 3600, invoice, to: "invoice.due", exchange: "billing")
  scheduler.at(renewal_date, policy, to: "policy.renew", exchange: "policies")
end
```

Delivering a message later, with no scheduler process and no plugin. Not a
per-message time to live, because a classic queue expires messages only at its
head: put a four-hour message in and a one-minute message behind it, and the
one-minute message is delivered in four hours, with nothing reporting it.

Instead, a ladder of queues each with a uniform time to live —
`acemq.schedule.{1h,10m,1m,10s,1s}` — dead-lettering into `acemq.schedule.due`,
where the scheduler either delivers the message or puts it in the largest rung
that does not overshoot. Every message in a rung has the same delay, so
head-of-line expiry is harmless. A one-day delay costs twenty-four hops and a
one-minute delay costs one.

Every name, argument and header is shared with the Java, Go, .NET and Python
libraries, because two services scheduling on one broker declare the same
queues. The payload is encoded once and carried as bytes from then on, with its
content type in `x-schedule-content-type` and put back on the message that is
finally delivered: a scheduler that decoded would acquire opinions about message
formats it has no business having.

The control consumer is subscribed on the transport rather than through
`consume`, so it does not declare `acemq.schedule.due.dlq` and `.parked` — two
queues in every deployment that nothing writes to and nobody reads.

### Pipelines and middleware

```ruby
mq.consume("orders.new", &Patterns.chain(
  ->(message) { place(message.payload) },
  Patterns.with_logging { |line| logger.info(line) },
  Patterns.with_timeout(10),
  Patterns.with_idempotency(store)
))
```

The order reads outside-in: the first one named is the outermost, so logging
here records what the timeout and the idempotency guard decided.

`with_timeout` **reports** an overrun rather than interrupting one, and reports
it as a retry whatever the handler said about itself — retrying work that may
have succeeded risks doing it twice, and accepting work that may have failed
loses it, and only the first of those is a problem you can solve. Ruby's
`Timeout.timeout` would interrupt, by raising inside whatever line the handler
happened to be on, which can leave a transaction half-written; and the message
is held until the handler returns either way, so there is nothing to gain.

There is no `with_recovery`. An exception is the ordinary failure channel in
Ruby and the consumer already turns one into a retry; a wrapper that rejected
on every exception would send a database outage straight to the dead-letter
queue.

Chaining services into a pipeline:

```ruby
mq.consume("orders.new", &Patterns.then_publish(mq, to: "shipment.requested",
                                                exchange: "shipping-events") do |message|
  message.payload["digital"] ? nil : { "order_id" => message.payload["order_id"] }
end)
```

Returning `nil` publishes nothing and accepts the message, which is how a step
says "this one does not continue" without inventing an empty message for the
next service to work out how to ignore. The correlation goes forward and the
causation records what produced what.

### Schemas

```ruby
registry = Patterns::InMemorySchemaRegistry.new
schema = registry.register("order.placed", "avro", definition)

registry.latest("order.placed")
registry.by_id(schema.id)
```

A producer and a consumer have to agree about what a message means, and they
are deployed on different afternoons. A registry lets the message carry a small
identifier instead of its whole shape.

Registering the same definition twice returns the same identifier rather than
making a second version — otherwise a service that registers on every start
adds a version per restart. The fingerprint is SHA-256 of the exact bytes, so
two definitions differing only in whitespace count as different: normalising
would need a parser per format, and a registry that quietly treated two
definitions as one because it mis-parsed them would be worse than a strict one.

A lookup that finds nothing raises rather than returning an empty definition.
`InMemorySchemaRegistry` is for tests and for seeing the shape of the thing —
nothing is shared between processes, which is the entire point of a registry.
Nothing here puts anything on the wire yet: which header carries a schema
identifier is a cross-language contract, and one invented here would be one the
other AceMQ libraries could not read.

### Streams

```ruby
Patterns.declare_stream(mq, "events", max_age: 7 * 24 * 3600, max_bytes: 10 * 1024**3)

Patterns.read_stream(mq, "events", offset: Patterns::StreamOffset.first,
                     prefetch: 100, name: "projection-1") do |message|
  project(message.payload)
  Ack.accept
end
```

A stream does not remove a message when somebody reads it, so several consumers
read the same stream independently and a new one can start from the beginning.
Positions: `first`, `next` (the default), `last`, `at(offset)`, `since(time)`.

**Acknowledging does not remove the message** — it advances this consumer's
position, so restarting from `next` carries on rather than re-reading. Rejecting
does not dead-letter it either, because there is nothing to remove it from: a
message that cannot be handled has to be dealt with by the handler, and the
stream moves on regardless. Nothing is lost, and nothing is retried for you.

That is why `read_stream` uses `RetryPolicy.none` whatever the connection
carries. A retry republishes, and republishing onto a stream appends a second
copy rather than redelivering the first, so a projection reading it would see
the message twice.

Retention is unbounded by default, which for a stream means until the disk is
full — a mistake an ordinary queue cannot make. Set at least one limit on
anything that will run for long.

### SQL-backed stores

The idempotency store, the outbox and the schema registry each shipped with an
in-memory implementation and a comment saying why it is not the one to use.
`SQLIdempotencyStore`, `SQLOutboxStore` and `SQLSchemaRegistry` are the other
one.

```ruby
store = Patterns::SQLOutboxStore.new(connection: db)
store.create_schema            # development and tests only

db.transaction do
  orders.insert(order)
  store.add(Patterns.record(mq, event, to: "order.placed"), connection: db)
end
```

**The connection handed to `add` is the whole point.** A store that opened its
own connection would commit the insert on its own, and a business write that
rolled back afterwards would leave a message queued for something that never
happened — the exact fault the pattern was adopted to prevent, now harder to
notice because the code looks right. So the insert goes on your connection,
inside your transaction, and the store neither commits it nor closes it: roll
the transaction back and the message is not in the outbox either. `connection:`
may also be a callable given once, which is the shape a framework that binds a
connection per request wants; `relay:` is a separate connection for the relay's
own background work.

**This gem still declares no runtime dependencies.** There is no driver here,
only a seam three methods wide — `run(sql, params)`, `placeholder(index)`,
`constraint_violation?(error)` — and `SQL.connect` recognises what it is handed
by the methods that object answers rather than by its class. A
`SQLite3::Database` and a `PG::Connection` both work as they are.

**What has actually been run:** SQLite, by the ordinary specs (the sqlite3 gem
is a development dependency, so they need no environment), and PostgreSQL, by
`spec/integration/postgres_spec.rb` when `ACEMQ_TEST_POSTGRES` names one.
Nothing else — MySQL and the rest are "should work, has not been run", which is
a different claim.

Records are claimed under a **lease** rather than a lock, so a relay that dies
mid-batch releases them without anybody intervening, and the claim is decided by
an update's row count rather than by the select that preceded it. The
idempotency store holds a key the same way, which is what stops a consumer that
died mid-handler from having silently deleted a message; it answers a third
method, `confirm`, that `Patterns.idempotent` calls after a handler accepts.

## Requirements

Ruby 3.1 or newer. RabbitMQ, and the `bunny` gem, for the transport. Nothing
else, unless you reach for [a codec that needs one](#codecs) — `rexml`,
`google-protobuf`, `avro` — or the [tracing adapter](#tracing), which wants
`opentelemetry-api`. Each of those says so by name when it is missing.

Encryption and the development certificates need nothing at all: both are
written against Ruby's own OpenSSL binding.

## Development

```bash
bundle install
bundle exec rspec        # the contract and the retry engine, no broker needed
bundle exec rubocop

# Anything that needs a broker is tagged :integration and skipped unless this
# is set, so a laptop with no Docker still runs everything else.
ACEMQ_TEST_BROKER=amqp://guest:guest@localhost:5672 bundle exec rspec

# The SQL-backed stores are tested against SQLite by the ordinary run, and
# against PostgreSQL as well when there is one to point at. "Written for
# PostgreSQL" and "run against PostgreSQL" are different claims.
ACEMQ_TEST_POSTGRES=postgres://user:pass@localhost:5432/acemq_test bundle exec rspec
```

The TLS examples need a broker with a TLS listener and the authority that signed
its certificate, and skip when they have neither. They are worth running before
touching anything in `security.rb`, because they are the only place that proves a
connection which should fail does:

```bash
ACEMQ_TEST_BROKER_TLS=amqps://guest:guest@localhost:5671 \
ACEMQ_TEST_BROKER_CA=certs/ca.crt \
bundle exec rspec --tag integration

# When the broker is configured with verify_peer and fail_if_no_peer_cert, add
# the client's pair and the mutual-TLS examples run too.
ACEMQ_TEST_BROKER_CLIENT_CERT=certs/client.crt \
ACEMQ_TEST_BROKER_CLIENT_KEY=certs/client.key \
...
```

If you have no such broker, `./scripts/acemq-certs.rb` writes the certificates
and a `rabbitmq.conf` that serves TLS from them. The examples that matter there
are the ones asserting a refusal, so they need a broker presenting a certificate
that carries the development marker — which is exactly what that command writes.

The fixtures under `spec/fixtures/` are produced by the Java implementation and
shared with Go, .NET and Python. They are the definition of "the same contract",
and they are checked here rather than assumed. `envelope-fixtures.json` pins the
headers; `contract-fixtures.json` pins the retry schedules, the jitter bounds,
where a wait is spent, the queue names, a rung's arguments, the declared topology
and the queue types, and `spec/contract_spec.rb` holds this library to all of it
— including the four places the five libraries do not yet agree, which it asserts
as they stand rather than smoothing over. See [testing without a
broker](docs/testing.md#the-cross-language-fixtures).

### The documentation site

```bash
bundle exec bash .github/scripts/build-docs-site.sh   # needs pandoc
open site/index.html
```

The README rendered as the front page plus a YARD reference under `site/api/`,
built from the comments in `lib/`. `docs.yml` publishes it to GitHub Pages on
every push to `main` that touches the README, `lib/` or `docs/`. The script
checks that every internal link resolves, because a published page pointing at a
404 is a failure this site family has had before and nothing noticed.

### Releasing

`release.yml` runs on a `v*` tag: it checks the tag is a `0.1.x` version and that
`AceMQ::AMQP::VERSION` agrees with it, runs the specs and RuboCop, builds the
gem, checks the built gem carries what the gemspec's glob was supposed to
include, installs it into a clean `GEM_HOME` and requires it there — which is the
one thing the specs cannot catch, since they satisfy every `require` from this
repository's own Gemfile. It then runs the integration specs against a broker and
only afterwards pushes to RubyGems.

The push uses **RubyGems trusted publishing**: the runner exchanges a GitHub
identity token for an API key that lives for minutes, so there is no long-lived
secret in this repository to leak, rotate or forget. It needs a trusted publisher
registered on rubygems.org once — repository `AceMQ-Company/acemq-ruby-amqp`,
workflow `release.yml`, environment `rubygems` — and rubygems.org will register a
*pending* one for a gem that has never been pushed, which is what this gem needs,
since `v0.1.0` was tagged before there was any way to publish and is not
installable by anybody.

A manual `workflow_dispatch` runs every check and stops short of publishing, so
the workflow can be tried out without spending a version number.

## Licence

Apache-2.0. RabbitMQ is a trademark of Broadcom Inc.; this project is not
affiliated with it.
