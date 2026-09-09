# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Added

- **`Patterns::Pipeline`, and the routing slip in both of the family's forms.**
  Ruby wrote the itinerary as `acemq-routing-slip`, a JSON document naming an
  exchange and a routing key per step. Java writes `x-acemq-route`: the ordered
  step names of a declared pipeline, a position, and a run identifier, resolved
  by the consumer against a pipeline it declared. A Java step could not read a
  Ruby slip and a Ruby step could not read a Java route, so the two halves of
  the family could not be steps of one pipeline.

  Both are available in all five libraries now. **Ruby keeps the JSON slip as
  its default** — three of the five write it and it is the self-describing one —
  and reads either:

  ```ruby
  orders = Patterns::Pipeline.new("orders", %w[validate charge ship])
  mq.apply(orders.topology)

  mq.consume(orders.queue_for("charge"), &orders.follow(mq) { |m| charge(m.payload) })
  orders.start(mq, order)
  ```

  A slip keeps the shape it arrived in, which is what lets a Ruby step sit in
  the middle of a Java-declared pipeline: the message is handed on as
  `x-acemq-route` with the position advanced and the run identifier untouched,
  and the next Java step reads it without knowing a Ruby service was involved.
  `Patterns.follow_slip(mq, pipeline:, write:)` asks for a shape explicitly;
  `write: RoutingSlip::ROUTE` re-resolves every step against the pipeline,
  because the header will carry only names and the exchange has to be the one
  the pipeline declared.

  The naming is Java's exactly — a direct exchange named for the pipeline, the
  step name as the routing key, `pipeline.step` as the queue — because a Ruby
  consumer that got any of it wrong would be listening where no Java service
  publishes. `spec/patterns/routing_slip_spec.rb` follows a message built from
  Java-shaped headers rather than from this library's own writer.

- **`Envelope#route`.** The `x-acemq-route` headers, carried opaquely and passed
  through every hop. An envelope has to hold them because they are reserved
  names and so cannot go in `headers:`, and it does not have to know what they
  mean: `Patterns::RoutingSlip` is what reads and writes them.

- **`acemq.pipeline.run.total` and `acemq.pipeline.run.duration`**, tagged with
  `pipeline`, `step` and an outcome of `completed` or `ended_early`. This is the
  seam the last release documented as not worth opening while nothing owned a
  pipeline's name; `Patterns::Pipeline` owns one, so it now costs a name and a
  step rather than an invention. The duration is the age of the envelope rather
  than the time in the last step, so it is the whole run. A bare JSON slip
  reports neither — an itinerary assembled per message has no identity to tag
  with, and a metric tagged with an empty name is worse than no metric.

- **Payload encryption.** `EncryptedCodec` wraps any other codec and encrypts
  what it produced with AES-GCM, so the broker, its disk, its backups and its
  management interface hold ciphertext. `Keyring` holds more than one key so a
  key can be rotated while messages written with the old one are still queued,
  and `EncryptedCodec.key_id_of` tells an operator which key a message needs
  without holding any of them.

  The framing is the Java library's, byte for byte:

  ```
  0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
  ```

  The header is the GCM associated data, so a key identifier altered in flight
  makes the message fail to open rather than opening as something else. The
  content type is `application/vnd.acemq.encrypted`, deliberately not a `+json`
  type whatever the plaintext underneath is. `spec/crypto_spec.rb` pins the
  layout against a fixed key, a fixed nonce and a fixed plaintext, and reads a
  body produced by the Java library's own `EncryptedCodec`.

  **The other libraries do not agree with each other about this format yet.**
  Go writes no magic byte and a two-byte big-endian key identifier length;
  .NET writes no magic byte, a 16-byte IV, AES-256-CBC and a 32-byte
  HMAC-SHA-256 tag. All three put the same content type on the message. This
  library reads and writes Java's, which is the only one of the three whose
  first byte identifies the format at all.
- **Development certificates, and a refusal to trust one.** `Security` now
  refuses any certificate carrying `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST`,
  however trust is configured — unverified mode included, which is the
  configuration one is likeliest to slip through. Both halves are checked: a
  certificate authority or client certificate configured in this process is
  refused when the connection is made, and one the broker presents is refused
  during the handshake. `Security#allowing_development_certificates` is the
  visible, deliberate opt-in, and it weakens nothing else.

  `DevelopmentCertificates.generate` writes the authority, broker certificate
  and client certificate a local TLS broker needs, under the same six filenames
  as Go's `acemq-certs` and .NET's `AceMq.Amqp.DevCerts`, plus a
  `rabbitmq.conf` that serves TLS from them. `./scripts/acemq-certs.rb` is the
  command-line form. Nothing is added to the gem: Ruby's OpenSSL binding writes
  the certificates and the script is not installed on anybody's PATH.
- **An OpenTelemetry adapter.** `Telemetry::OpenTelemetry` emits spans for
  publishes and deliveries and joins them across the broker:
  `tracing.install(mq)` registers it on both sides of a connection. The trace
  travels in `traceparent` and `tracestate` — the W3C names, deliberately not
  `x-acemq-` prefixed, because other tooling already recognises them and the
  Java library writes the same two.

  Spans are named `<destination> publish`, `<queue> process` and
  `<destination> request`, with PRODUCER, CONSUMER and CLIENT kinds; CLIENT for
  a request because that span waits for an answer and its duration means
  something different as a result. The consumer span's parent is extracted from
  the message's own headers rather than from ambient context, which is the
  entire point of tracing a message system. `unroutable`, `failed` and
  `dead_lettered` set the span status to error; `acked`, `retried` and
  `rejected` do not. A retry, a dead letter, an outbox failure and a finished
  pipeline run are events on the span already open rather than spans of their
  own.

  `opentelemetry-api` is a development dependency, required lazily, and raises
  `DependencyMissing` naming the gem when it is absent. The gemspec still
  declares no runtime dependencies. The specs assert on spans emitted through
  the SDK's in-memory exporter rather than on doubles.

- **Five more codecs: YAML, TOML, XML, Protocol Buffers and Avro.** The gem
  shipped JSON, string and bytes, while Java and Go both shipped all five of
  these — so a Java or Go service publishing any of them produced a message
  this library could not decode. The gap was interoperability rather than
  capability, and the specs are written to prove that rather than to prove a
  round trip: every codec is checked against bytes generated by the Java and Go
  libraries' own codecs, carried in `spec/fixtures/codec-samples.json` and
  regenerated by `acemq-amqp-libraries/scripts/codec-samples.sh`.
  - `YAMLCodec` writes `application/yaml` and also reads `application/x-yaml`,
    `text/yaml`, `text/x-yaml` and any `…+yaml` type.
  - `TOMLCodec` writes `application/toml` and also reads `text/toml` and any
    `…+toml` type.
  - `XMLCodec` writes `application/xml` and also reads `text/xml` and any
    `…+xml` type.
  - `ProtobufCodec` writes `application/x-protobuf` and also reads
    `application/protobuf`, `application/vnd.google.protobuf` and any
    `…+protobuf` type.
  - `AvroCodec` writes `avro/binary` with a fixed schema and
    `application/vnd.acemq.avro` with a registry, and reads `application/avro`
    and any `…+avro` type in either mode.

  None of the five answers for a message whose sender set no content type,
  which stays `JSONCodec`'s alone. `Codecs.names` is now `bytes, json, string,
  toml, xml, yaml` — the same six names Java and Go know. Protobuf and Avro are
  deliberately not among them: both are built around a message type or a
  schema, and a name in configuration cannot carry one.
- **The gem still declares no runtime dependencies.** YAML is Psych, a default
  gem, and the TOML reader and writer are written here because Ruby has no TOML
  parser in its standard library. The three that need a gem — REXML for XML,
  `google-protobuf`, `avro` — are required lazily and raise `DependencyMissing`
  naming the gem to install, the way the transport already does with bunny.
  REXML is worth calling out: it ships with Ruby but has been a *bundled* gem
  rather than a default one since Ruby 3.4, so a Bundler process needs
  `gem "rexml"` in its Gemfile even though a plain `ruby` does not.
- `DependencyMissing` has moved from `transport.rb` to `ack.rb`, beside
  `FatalError`, because it is no longer only the transport's. The constant is
  unchanged: `AceMQ::AMQP::DependencyMissing`.
- **`Telemetry::OpenTelemetry#outbox_published(lag:)`**, which writes
  `messaging.acemq.outbox_lag_ms` onto whatever span is open. An attribute rather
  than an event, because it measures the publish that is happening rather than
  something that happened during it, and nothing at all when no span is open —
  the same shape and the same attribute name the Java, Go and Python adapters
  use. Ruby was the only library without it, so a lag panel built on the others
  had a hole where the Ruby services should have been.
- **`Ack.park`.** A handler can now say a message is unreadable and have it go to
  `{queue}.parked` instead of `{queue}.dlq`. The engine has always parked a body
  no codec could decode; a handler that got further and still knew the message
  was unreadable — a schema version this service was never taught, a field that
  is not a date where a date has to be — had no way to ask for the same thing,
  and had to reject it into the dead letters. That is exactly the mixing the
  parking queue exists to prevent: somebody draining dead letters after an
  outage had to sort the messages that were never going to work from the ones
  that failed while a database was down.

  It settles to `{queue}.parked`, counts as `acemq.messages.parked` — the same
  counter the decode path raises, and counted once — and writes
  `messaging.acemq.outcome = "parked"` on the span. `Settlement::PARKED` is a
  fifth outcome alongside `acked`, `retried`, `rejected` and `dead_lettered`;
  `Settlement#parked?` is new and `#dead_letters?` is false for it, because a
  parked message goes to a queue of its own on purpose. Go and Python are adding
  the same action under the same name.
- **`acemq.messages.set.aside.failed`**, counted when the republish to
  `{queue}.dlq` or `{queue}.parked` is itself refused, labelled with the `queue`
  and the `target` that could not be reached. Go and Python already had it.

  What Ruby does with such a message is unchanged and is now written down: the
  publish failure escapes the handler, the delivery is never settled, and the
  broker redelivers it when the channel closes. Nothing is lost. But from
  outside, a message redelivered forever because its dead-letter queue was never
  declared looks exactly like a handler failing forever on the same message, and
  this counter was the only thing missing to tell the two apart. Go and Python
  reject the message to the broker instead, so the delivery is settled either
  way; they raise this same counter, so one alert reads the same against all
  three.
- **AMQP's own `reply-to` property**, on both sides. `Connection#publish` takes
  `reply_to:`, `Message#reply_to` reads what arrived, and both are carried
  through `Delivery`, `PublishContext`, the consumer's republish and a replay.

### Changed

- **Every metric was renamed onto Java's vocabulary, and every existing Ruby
  dashboard breaks.** Java's `MetricNames` is the family vocabulary; Go, Python
  and Ruby have all moved onto it. Ruby's names and Java's were entirely
  disjoint, so no dashboard could read both and this library's claim that "the
  names are shared with Java, Go, .NET and Python" was simply false. It is true
  now.

  The shape changed as well as the spelling. Where Ruby had one counter per
  outcome, there is now one counter carrying an `outcome` tag, which is how a
  dashboard sums the whole and breaks it down without knowing the list of
  outcomes in advance:

  | was | is now |
  |---|---|
  | `acemq.messages.published{exchange}` | `acemq.publish.total{exchange,outcome="confirmed"}` |
  | `acemq.messages.publish.failed{exchange}` | `acemq.publish.total{exchange,outcome="failed"}` |
  | `acemq.messages.consumed{queue}` | `acemq.consume.attempts{queue}` — its **sample count** |
  | `acemq.messages.accepted{queue}` | `acemq.consume.total{queue,outcome="acked"}` |
  | `acemq.messages.retried{queue}` | `acemq.consume.total{queue,outcome="retried"}`, and `acemq.messages.retried.total{queue}` |
  | `acemq.messages.rejected{queue}` | `acemq.consume.total{queue,outcome="rejected"}` |
  | `acemq.messages.dead.lettered{queue}` | `acemq.consume.total{queue,outcome="dead_lettered"}`, and `acemq.messages.dead.lettered.total{queue}` |
  | `acemq.messages.parked{queue}` | `acemq.consume.total{queue,outcome="parked"}` |
  | `acemq.handler.duration{queue}` | `acemq.consume.duration{queue,outcome}` |
  | `acemq.messages.in.flight{queue}` | `acemq.consume.in.flight{queue}` |
  | `acemq.messages.set.aside.failed{queue,target}` | unchanged |
  | `acemq.retry.rung.missing{queue}` | unchanged |

  The last two are unchanged because Java adopted **Ruby's** names for them,
  along with the `target` tag and the `parked` outcome: the traffic was not all
  one way.

  **The property that made the previous release worth having survives.** Exactly
  one `acemq.consume.total` series goes up per delivery, tagged with what the
  consumer really decided rather than what the handler asked for, and it is read
  off the same `Settlement` the span's `messaging.acemq.outcome` attribute is.
  A new example asserts it outcome by outcome, and another asserts that
  `Settlement`'s words and `Telemetry::Outcome`'s are the same strings, so the
  two lists cannot drift.

  `acemq.messages.retried.total` and `acemq.messages.dead.lettered.total` are
  kept as standalone counters alongside the tag, as Java keeps them: a retry
  rate and a dead-letter rate are the two numbers most often wanted without a
  tag filter.

  `acemq.messages.consumed` was counted on the way in, and that signal is not
  lost — it is the sample count of `acemq.consume.attempts`, which is Java's
  metric for the same moment and says which attempt each delivery was as well.

  The constants moved with the strings: `Telemetry::PUBLISHED` and
  `PUBLISH_FAILED` are `PUBLISH_TOTAL` with a `Telemetry::Outcome` value;
  `ACCEPTED`, `RETRIED`, `REJECTED`, `DEAD_LETTERED` and `PARKED` are
  `CONSUME_TOTAL` with one; `HANDLER_DURATION` is `CONSUME_DURATION` and
  `IN_FLIGHT` is `CONSUME_IN_FLIGHT`. There is no compatibility shim and nothing
  is emitted under both names — a library quietly writing both would double every
  counter for anybody who had already moved.

- **`to_prometheus` renders label names Prometheus will accept.** Prometheus
  allows `[a-zA-Z_][a-zA-Z0-9_]*` in a label name and nothing else, so the
  family's `routing.key` and `message.type` tags are illegal ones — and a single
  unparseable line does not lose one series, it makes the whole scrape fail and
  loses every metric the process publishes. Label names now go through the same
  rule the metric name always did, so they are scraped as `routing_key` and
  `message_type`, which is what Go settled on.

- **A routing-slip step that returns nothing ends the run.** `Patterns.follow_slip`
  used to publish whatever the block returned; returning `nil` now publishes
  nothing and accepts the message, which is the rule
  `Patterns.then_publish` already followed and the one Java's pipeline counts as
  `ended_early`. A step that decides a message goes no further is making a
  decision, not failing.

- **`observe` is documented as a distribution rather than a timer.** Its second
  argument is `value`, not `seconds`: durations still go through it in seconds,
  and `acemq.consume.attempts` puts an attempt number through the same method.
  The arity is unchanged, so an existing observer keeps working.

- **A requester writes the reply address twice, and a responder reads either
  one.** `Patterns::Requester` now sets AMQP's native `reply-to` property as well
  as the `acemq-reply-to` header, to the same queue, and `Patterns.serve` reads
  the header first and falls back to the property.

  **This is what makes request and reply work across the family.** Java and .NET
  wrote and read the native property; Go, Python and Ruby wrote and read the
  header. A Java or .NET requester and a Ruby responder could not talk to each
  other at all, in either direction, and nothing in any suite covered it. All
  five libraries are making the same change: write both, read either, header
  first. Header first because it is the half that survives a service which reads
  a message and publishes a new one — such a service keeps the headers and
  usually drops the properties. On a message this library produced the two always
  agree, so the order decides anything only for a request that came from
  somewhere else. `Patterns.reply_address(message)` is the rule on its own.

  A request carrying neither is still dead-lettered, and the reason now says so:
  "carries neither the acemq-reply-to header nor a reply-to property".
- **`Telemetry::OpenTelemetry::Scope#failed` sets the outcome.** Recording a
  failure now writes `messaging.acemq.outcome = "failed"` as well as the
  exception and the error status, unless an outcome was already named explicitly
  — in which case the explicit one wins, which is what keeps `timed_out` on a
  request deadline from being overwritten by the exception that carried it.

  It used to write no outcome at all. A span that said nothing where the counter
  said `failed` is the same disagreement between a metric and a trace this
  library treats as a defect everywhere else: a dashboard shows the failures and
  the trace backend, queried for `messaging.acemq.outcome = "failed"`, finds none
  of the spans behind them. Java has just been fixed for exactly this; Python
  already did it.
- `parked` joins `unroutable`, `failed` and `dead_lettered` as an outcome that
  marks a span an error. A message nothing could read will never be processed,
  which is at least as bad as one that ran out of attempts.

- **A fixed-schema `AvroCodec` reads the content type before it reads the
  bytes.** `AvroCodec#decode` now takes an optional content type, and it decides:
  `avro/binary`, `application/avro` and any `…+avro` type are read as a
  fixed-schema body with no further checking, `application/vnd.acemq.avro` is
  refused as the registry framing, and only a message that named no Avro type at
  all falls back to the old guess — five or more bytes beginning with `0x00` are
  refused as probably framed.

  The guess used to be the whole rule, copied from Java. It refuses legitimate
  messages: an Avro body begins with a zero byte whenever its first field
  encodes to zero — an empty string, a `0`, a `false`, branch 0 of a union — so
  a real record was being refused to catch a framing the sender had already
  named. Python implemented the rule above first and Java has been changed to
  match, so all three now read the same bytes the same way.
  `CompositeCodec` passes the content type down to any candidate whose `decode`
  takes one, so a composite holding an Avro codec gets the same answer.
- **The OpenTelemetry adapter reports what the consumer decided, not what the
  handler asked for.** A handler asking for a retry with no attempts left is
  dead-lettered by the consumer; the span said `outcome=retried` and no
  `message.dead_lettered` event was raised at all, so a trace backend queried
  for dead letters found nothing. The span now says `dead_lettered` and the
  event carries the reason written onto the message. `message.retried` carries
  `messaging.acemq.retry_delay_ms` — the delay the retry policy actually chose,
  which is only known while the delivery is being settled.

  The seam is a `Settlement` — `outcome`, `delay`, `reason` — worked out by the
  consumer before the interceptors run and left on `ConsumeContext#settlement`,
  so any interceptor can read what is about to happen rather than infer it from
  the ack. `rejected` stays a separate outcome from `dead_lettered` although
  both end in the dead-letter queue, and the dead-letter event is raised for
  both. Nothing about this is OpenTelemetry-specific and nothing is required
  until an adapter is built: `opentelemetry-api` remains a lazily required
  optional gem, and the gemspec still declares no runtime dependencies.
- **The outcome counters classify by the `Settlement` too, so a counter and a
  span for the same delivery agree.** They classified by the `Ack` the handler
  returned, one layer below the fix above and with the same fault: an `Ack`
  cannot know whether there is an attempt left to spend. A message that ran out
  of attempts incremented `acemq.messages.retried` on its way to the dead-letter
  queue and was counted again as `acemq.messages.dead.lettered`, and a rejection
  was counted as both `rejected` and `dead.lettered`.

  `acemq.messages.accepted`, `.retried`, `.rejected` and `.dead.lettered` are now
  the four `Settlement` outcomes, one counter each, and **exactly one goes up per
  delivery** — read off the same decision the span's `messaging.acemq.outcome`
  attribute is read off. `spec/telemetry_open_telemetry_spec.rb` runs each of the
  four outcomes through a real consumer with a registry and the tracing adapter
  both attached, and asserts the word on the span and the single counter that
  moved name the same thing.

  **On an existing dashboard** `acemq.messages.retried` falls and
  `acemq.messages.dead.lettered` rises by the same amount, and `dead.lettered`
  stops counting handler rejections. Nothing about where a message goes has
  changed, so a step in those series at deploy time is the upgrade and not an
  incident. A panel adding `retried` and `dead.lettered` together to get
  "failures" was double-counting and should be rebuilt on `dead.lettered` alone.
  .NET, Go and Python make the same correction.
- **The outbox relay's `on_error:` can be told where the record was going.** It
  was handed the exception and nothing else, so a callback could not say which
  exchange an outbox was stuck on and could not fill the
  `messaging.destination.name` the `outbox.publish_failed` event wants. A
  callback declaring `exchange:` and `routing_key:` now receives them; a
  one-argument callback is called exactly as before, decided from the callable
  itself, so nothing existing has to change. Both are empty strings when the
  store itself raised and no record had been claimed. `sweep` still raises
  whatever the broker or the store raised.
- **A request that went unanswered is `timed_out` on its span, not nothing.**
  `Telemetry::OpenTelemetry#request` set no `messaging.acemq.outcome` at all when
  its block raised and marked every failure an error, so a timed-out request and
  a broken connection were indistinguishable on a trace and both were red. A
  `Patterns::RequestTimedOut` now writes `timed_out` and leaves the status unset
  — a timeout is the absence of a reply rather than a failure of this process,
  which is how Java and Go report it — and anything else writes `failed` and is
  an error. `timed_out` was already in the shared vocabulary and was the only
  word in it Ruby could reach and did not write. (`unroutable` remains
  unreachable: it needs a mandatory publish and `basic.return`, which this
  transport does not use.)

### Security

- **`YAMLCodec` parses with `Psych.safe_load`, never `YAML.load`.** A message
  body is input from another machine, and `YAML.load` on one is remote code
  execution — the body names the class to instantiate. The cost of that lands
  on legitimate messages, so it is spent deliberately: `Date`, `Time` and
  `DateTime` are permitted by default, because Java and Go both write
  timestamps into YAML and refusing them would reject messages the other
  libraries send daily. `Symbol` is not, because no other language sends one;
  pass it in `permitted_classes:` for a Ruby-to-Ruby queue. Anchors and aliases
  are refused unless `aliases: true`, because the billion-laughs expansion
  needs nothing else and neither Jackson nor gopkg.in/yaml.v3 writes them.
  `YAMLCodec#encode` also refuses to write a payload Psych can only spell as
  `!ruby/object:`, which would be a message no other AceMQ library could read.
- **`XMLCodec` refuses a document type declaration outright**, before parsing.
  A DTD is how an XML body reads files off the machine handling it, opens
  connections on its behalf, and expands a few bytes into a heap full of them.
  This is not configurable; the Java codec disables DTD support for the same
  reason.

- **Sagas.** `Patterns::Saga` runs an ordered set of steps, each with an
  optional compensation, and compensates the completed ones in reverse when a
  later step raises. Nothing is published and no header is set — only the
  semantics are shared with Java. A completed step with no compensation is
  skipped rather than treated as a failure; a compensation that itself raises
  does not stop the others, and its step name is collected into
  `SagaResult#unresolved`, which `unresolved?` reports and which is the list to
  alert on. It returns a frozen `SagaResult` rather than raising, because a
  caller needs the compensation report more than a backtrace.
- **A scheduler.** `Patterns::Scheduler` delivers a message later, as a ladder
  of uniform time-to-live queues rather than a per-message expiration — a
  classic queue expires messages only at its head, so one long wait at the
  front holds back every shorter one behind it. The exchange `acemq.schedule`,
  the five rungs `acemq.schedule.{1h,10m,1m,10s,1s}` with exactly
  `x-message-ttl`, `x-dead-letter-exchange` and `x-dead-letter-routing-key`,
  the control queue `acemq.schedule.due`, and the four headers
  `x-schedule-exchange`, `x-schedule-routing-key`, `x-schedule-due-at` (epoch
  milliseconds) and `x-schedule-content-type` are the ones Java writes, so a
  Ruby service and a Java service scheduling on one broker declare the same
  topology instead of refusing each other's. The headers deliberately avoid the
  reserved `x-acemq-` prefix, which the envelope refuses outright.
  `Scheduler.declare(mq)` declares the topology without starting a consumer.
- The scheduler's control consumer is subscribed on the transport rather than
  through `Connection#consume`, so that it does **not** declare
  `acemq.schedule.due.dlq` and `acemq.schedule.due.parked` — a consumer
  declares its dead-letter queues at start-up, and every service running a
  scheduler would otherwise leak two queues nothing writes to and nobody reads.
  It reads raw bytes and never decodes a payload; a message reaching the
  control queue without a scheduler's headers is dropped and counted in
  `Scheduler#malformed`.

## [0.3.0] - 2026-09-08

### Added

- **The claim check.** `Patterns::ClaimCheckCodec` wraps any codec and puts a
  payload over 64 KiB into a store, sending the store's key instead;
  `Patterns::InMemoryClaimCheckStore` and
  `Patterns::FilesystemClaimCheckStore` are the two that ship. The framing —
  `0xAC 0x01 0x00` for an inline payload, `0xAC 0x01 0x01` for a key — and the
  64 KiB threshold are the ones the Java library writes, so a Ruby consumer
  pointed at the same store reads what a Java publisher checked in. Until now
  `x-acemq-claim` was a reserved header this library could read and never
  produce.
- **Database-backed stores.** `Patterns::SQLOutboxStore`,
  `Patterns::SQLIdempotencyStore` and `Patterns::SQLSchemaRegistry`, written
  against a three-method connection seam rather than against a driver — the gem
  still declares no runtime dependencies. `SQLOutboxStore#add` takes the
  caller's own connection so the message insert and the business write commit
  together, which is the property the in-memory outbox is criticised in its own
  comment for lacking. Run against SQLite by the ordinary specs and against
  PostgreSQL when `ACEMQ_TEST_POSTGRES` names one.

- **The cross-language contract fixture.**
  `spec/fixtures/contract-fixtures.json` is generated by the Java library and
  carried byte for byte by Go, .NET, Python and Ruby.
  `spec/contract_spec.rb` reads it and holds this library to every section: the
  five retry schedules with their delays, rungs and give-up decisions; the
  jitter bounds, sampled rather than read back; thirty rows of where a wait is
  spent; the `.dlq`, `.parked` and rung names; the three arguments a rung is
  declared with, counted as well as compared; the whole declared topology, half
  from `Topology` and half from the consumer; and the queue type every name
  gets. Expectations are derived where they can be, so that a spec agreeing
  with itself is not mistaken for five libraries agreeing with each other.

### Changed

- **A consumer declares its dead-letter queues at start-up** — `acemq.dlx`,
  `{queue}.dlq`, `{queue}.parked` and the two bindings that reach them — in
  addition to the retry exchange, the rungs and the binding home it already
  declared. A consumer that gives up republishes to `{queue}.dlq` through the
  default exchange, which drops what it cannot route without a word, so on a
  broker where the topology was never applied the dead letter that would have
  reported the mistake was the thing that vanished. The dead-letter half goes
  out whether or not there is a retry policy, since `RetryPolicy.none` gives up
  on the first failure; the retry exchange is still declared only when there
  are rungs to reach through it. Applying a `Topology` first and then starting
  a consumer, or the other way round, declares the same queues with the same
  arguments and is not a `PRECONDITION_FAILED` either way. Java has always done
  this; ADR-032 brought the other four libraries into line, and the contract
  fixture already marked these five entries `both`.
- **`retry_threshold: 0` now means no rung queue at all**, where it previously
  sent every retry to the broker. Zero reads as "from zero, so everything" and
  is defined the other way round in Java, Go, .NET and Python: it switches the
  broker off, which is the only spelling there is for "never use the broker",
  since "always" is any threshold below the shortest delay in the schedule.
  Ruby was the only library reading it backwards, and the contract fixture is
  what found it. A threshold left at its default, or set to any positive
  number, behaves exactly as before.
- `Patterns.idempotent` calls `confirm(key)` after a handler accepts, on a store
  that answers it. A store whose rows outlive the process has to tell a key left
  by work that finished from a key left by a consumer that died; one with no
  `confirm` — `InMemoryIdempotencyStore` — is never asked, so nothing that
  worked before behaves differently.
- `OutboxRelay` calls `mark_failed(id, reason)` when a publish raises, on a
  store that answers it, then re-raises as before. It is what counts the attempt
  and gives up the record's lease.

## [0.2.0] — 2026-09-07

> ### ⚠ Migrating: a retry rung now returns through `acemq.retry`
>
> A rung queue is declared with `x-dead-letter-exchange` set to `acemq.retry`,
> where 0.1.0 used `""` (the default exchange). **A rung that already exists
> with the old argument cannot be redeclared with the new one** — AMQP forbids
> changing a queue's arguments in place, so the declare is refused with
> `PRECONDITION_FAILED`.
>
> This only affects a broker that has already run 0.1.0 with a retry policy
> whose delays reach 30 seconds. Delete the `{queue}.retry.*` queues and let
> them be declared again; they hold nothing but messages waiting to be retried,
> and anything in them at the time is lost, so drain first if that matters.
>
> The change exists because the other four libraries all use `acemq.retry`, and
> two services on one queue that disagree about a rung's arguments cannot both
> consume it.

> ### ⚠ Migrating: a source queue is now declared as a quorum queue
>
> `Topology#queue` and `Connection#declare_queue` send `x-queue-type: quorum`,
> where 0.1.0 sent no `x-queue-type` at all and therefore declared a classic
> queue. **A queue that already exists as classic cannot be redeclared as
> quorum** — there is no conversion, and the declare is refused with
> `PRECONDITION_FAILED`. Drain the queue and recreate it as quorum, or keep it
> classic deliberately with `queue_type: :classic` until you can.
>
> The rungs, `{queue}.dlq` and `{queue}.parked` are unchanged: they were classic
> and stay classic, as they are in Java. Anything exclusive, auto-deleting or
> transient — a health probe's queue, a generated reply queue — also stays
> classic, because RabbitMQ refuses to replicate a queue that goes away on its
> own; asking for quorum *and* one of those flags now raises `QueueTypeError`
> instead of reaching the broker.
>
> The change exists because Java declares a source queue as a quorum queue and
> Java is the library with deployments. A Java service and a Ruby service that
> both declare `orders` have to agree on its type or the second one to start
> cannot consume at all.

### Added

- **TLS and credentials.** `amqps://` with verification on, a custom CA, client
  certificates for mutual TLS, and credentials supplied apart from the URL so a
  password need never be embedded in a connection string. `Credentials` renders
  as `[REDACTED]` through `inspect` and `to_s`. Verification can be turned off
  only through `Security.without_verifying_the_broker(because:)`, which demands
  a reason.
- **Interceptors.** An ordered, composable seam around publishing and handling
  with writable contexts, registered by object or block, so cross-cutting work
  is written once rather than in every handler.
- **Telemetry and health.** Metric names identical to the Go, Java and .NET
  libraries; a three-method observer contract; an in-memory registry with a
  `to_prometheus` renderer and no dependency on any metrics gem. `mq.health`
  answers up, down or degraded, and its broker probe cleans up after itself.
- **The pattern library**: idempotency, outbox, request/reply, replay, ordered
  handling, consumer groups, routing slips, pipelines, a schema registry and
  stream reading.
- **A way to install it.** A release workflow publishing to RubyGems through
  trusted publishing, and a docs workflow publishing the reference to GitHub
  Pages. Until 0.2.0 the `v0.1.0` tag existed and nobody could install it.

### Changed

- **A rung returns through the named `acemq.retry` exchange** rather than the
  default exchange, with one binding `{queue} -> acemq.retry -> {queue}`, and
  dead letters route through `acemq.dlx`. See the migration note above.
- **A durable source queue is a quorum queue**, in `Topology#queue` and in
  `Connection#declare_queue`, matching Java's `declareQueue`. `queue_type:`
  takes `:classic`, `:quorum` or `:stream`; an `x-queue-type` already in
  `arguments` is honoured, which is how `Patterns.declare_stream` works; and the
  retry rungs, `{queue}.dlq` and `{queue}.parked` stay classic. See the
  migration note above.
- **A replay resets `x-acemq-attempt` to 1** unless `restart: false`. A message
  dead-lettered on its last attempt would otherwise be dead-lettered again
  before a handler ever saw it.
- `Topology#retry_ladder` now requires the source queue to be declared in the
  same topology, because it adds a binding to it.

### Fixed

- **bunny does not verify the broker's certificate when given an `amqps://`
  URL.** `AMQ::Settings.configure` merges `verify: false`, which bunny reads as
  an instruction and turns into `VERIFY_NONE` — measured against a broker with a
  private authority as `verify_result=19`, connection opened anyway. Every mode
  now sets `verify_peer` explicitly.
- `Connection#close` cancelled consumers with `consumers.each(&:cancel)`, so one
  consumer raising left the socket open.
- `replay(from: dlq)` onto its own queue looped for ever; it is now refused.
