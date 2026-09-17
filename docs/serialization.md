# Codecs

A codec turns a payload into bytes on the way out and back into an object on the
way in, and says what content type it writes. JSON is the default, because it is
the one format every AceMQ library has without an extra dependency.

```ruby
mq = Connection.open(url)                          # JSONCodec.new
mq = Connection.open(url, codec: StringCodec.new)  # for a whole connection
mq.publish(png, to: "thumbnails", codec: BytesCodec.new)   # for one message
mq.consume("thumbnails", codec: BytesCodec.new) { … }      # for one consumer
```

## What ships

**`JSONCodec`** — `application/json`. The default.

```ruby
JSONCodec.new                          # string keys
JSONCodec.new(symbolize_names: true)   # symbol keys on the way in
```

String keys by default: a key that came off the wire is data, and turning
arbitrary data into symbols is how a process leaks memory on a Ruby that does
not collect them. Keys go on the wire exactly as the payload spells them — a
codec that quietly renamed `total_cents` to `totalCents` would be deciding the
cross-language contract on your behalf. Spell them the way the other services
already read them.

It reads `application/json`, `text/json`, any `…+json` media type, **and a
message whose sender set no content type at all**. That last one matters: JSON
is the default format, an untyped message is far more likely to be JSON than
anything else, and something has to be willing to read it.

**`BytesCodec`** — `application/octet-stream`. Passes bodies through untouched,
and `can_decode?` answers true for everything. For a payload that is already
encoded, for reading a message this process has no type for, and for
[replay](patterns.md#replay): the bytes that were committed are the bytes that
should go back, and re-encoding through a class that has since gained a field
would produce something else.

**`StringCodec`** — `text/plain; charset=utf-8`. Text, as text.

**`YAMLCodec`** — `application/yaml`. For a message a person reads as much as a
program does: a configuration change broadcast to a fleet, a deployment
instruction, a command replayed by hand from a dead-letter queue. Costs more to
parse than JSON and is a poor choice for high volume. See [safe by
default](#yaml-is-parsed-safely-and-that-costs-something) below, which is not
optional and does change what arrives.

**`TOMLCodec`** — `application/toml`. The same audience as YAML with the
ambiguity removed. One way to write a string, no significant indentation, and no
Norway problem: in YAML `country: NO` is the boolean false, and in TOML an
unquoted `NO` is a parse error. A TOML document is a table, so the payload has
to be a `Hash` at the top level; a list or a number is refused rather than
written as something no parser reads back. `nil` is refused too — TOML has no
null, and dropping the key silently is worse.

**`XMLCodec`** — `application/xml`. For talking to something that already speaks
XML and will not be rewritten. XML has no types, so every leaf comes back a
`String`. The root element's name is dropped on the way in and chosen on the way
out — `XMLCodec.new(root: "order")`, `message` by default — because a Hash does
not carry a name the way a Java class does. Elements repeated under one parent
become an `Array`, attributes become ordinary keys, and **a body carrying a
`<!DOCTYPE>` is refused before it is parsed**, which is not configurable.

**`ProtobufCodec`** — `application/x-protobuf`. Small and fast, unreadable
without the schema. Built around one generated message type, because protobuf
bytes describe nothing about themselves:

```ruby
codec = ProtobufCodec.new(Acme::Order)   # the class protoc generated
```

**`AvroCodec`** — `avro/binary` with a fixed schema, `application/vnd.acemq.avro`
with a registry. Compact, and unlike protobuf able to resolve a writer's schema
against a reader's, which is what lets a producer add a field without every
consumer being redeployed the same afternoon:

```ruby
AvroCodec.of(schema_json)                                   # avro/binary
AvroCodec.registered(registry, subject: "order.placed",     # …vnd.acemq.avro
                     schema: schema_json)
```

**`reader_schema:` is where that resolution actually happens.** A registered
codec resolves every message onto the schema it holds, and by default that is
the same schema it writes with. Where the two want to be different — a consumer
still written against `v1` in a process that publishes `v3` — say so:

```ruby
AvroCodec.registered(registry, subject: "order.placed",
                     schema: v3_json, reader_schema: v1_json)
```

The codec then publishes `v3` and registers `v3` under the subject, while every
message it reads is resolved onto `v1` from whatever version wrote it. That is
not the same as passing `v1` as `schema:`: a codec that both reads and publishes
would then register `v1` as a new version of the subject and walk the subject
backwards.

What resolution buys is Avro's rather than this library's. A field the writer
added that the reader has never heard of is skipped, rather than shifting every
field after it and handing back a record whose every value is wrong. A field the
reader expects that the writer never sent is filled in from the reader's own
default — a value that was never on the wire at all, which is the difference
between resolving two schemas and re-parsing one. A change Avro cannot resolve —
a field whose type changed, a field added without a default — raises
`DecodeError`, which is fatal, naming both schemas and quoting the writer's in
full, because that is the one nobody has in front of them: it was registered by
another process, possibly in another language. A reader schema on a
fixed-schema codec is refused rather than accepted and ignored, because there is
no writer's schema there to resolve against.

Every library has this. Java spells it `registered(registry, readerSchema)`,
.NET `ReaderSchema`, Go `avro.ReadAs(schema)` and Python `reader_schema=`, which
is the spelling followed here.

`registered` frames each message with one zero byte, four bytes of schema
identifier big-endian, then the body — Confluent's layout, so messages written
here are read by their clients and by the Java, Go and .NET libraries, and the
other way round. Each mode claims **only its own** content type, because the two
framings are indistinguishable in the bytes: a fixed-schema codec reading a
framed message would decode the identifier as the first field, without throwing,
and hand back a record whose every value is wrong.

Which is why **the content type decides**, and the bytes only get a vote when
nothing else does. Handed `avro/binary`, `application/avro` or any `…+avro`
type, a fixed-schema codec reads the body as one and does not second-guess it;
handed `application/vnd.acemq.avro`, it refuses, because that is the framing it
cannot read. Only when the content type is absent, or names no Avro type at all,
does it fall back to guessing — a body of five or more bytes beginning with
`0x00` is refused as probably framed, and the refusal names the content type
that would settle it.

That last rule used to be the *only* rule, here and in Java. It is wrong on its
own: an Avro body begins with a zero byte whenever its first field encodes to
zero — an empty string, a `0`, a `false`, branch 0 of a union — so a codec that
refused every one of them was refusing ordinary messages to catch a framing the
sender had already named. Java and Python read it the way described above, and
so does this.

`ProtobufCodec` and `AvroCodec` need a gem — `google-protobuf` and `avro`. Both
are required lazily and say which to install, so the gem still declares no
runtime dependencies. `XMLCodec` needs REXML, which ships with Ruby but has been
a *bundled* gem rather than a default one since Ruby 3.4: a plain `ruby` has it
and a Bundler process needs `gem "rexml"` in its Gemfile. YAML is Psych, a
default gem, and the TOML reader and writer are written into this gem because
Ruby's standard library has no TOML parser.

**`CompositeCodec`** — for a queue carrying more than one format, during a
migration or where several producers were written years apart:

```ruby
codec = CompositeCodec.new(JSONCodec.new, YAMLCodec.new, TOMLCodec.new, XMLCodec.new)
```

The **first** codec is what it writes. Reading offers the message to the
candidates in order and takes the first that manages it, so order matters where
two overlap — put the more specific first, since `BytesCodec` answers for
everything and would win from anywhere in the list.

A content type narrows the candidates to the codecs that claim it. No content
type leaves every codec a candidate, because a sender that said nothing has
ruled nothing out; guessing one format and failing on the rest would turn a
silent producer into a queue of dead letters. When nothing can read it, the
`DecodeError` names each candidate and what it said, because "could not decode"
on its own tells whoever is reading the log nothing they can act on.

## What each codec reads

The content type a codec **writes** is one string. What it **accepts** is wider,
because a producer in another stack uses whichever spelling its own library
picked, and a message refused here is a message that should have been readable.
These are the same accept sets the Java and Go libraries use.

| Codec | Writes | Also reads |
| --- | --- | --- |
| `JSONCodec` | `application/json` | `text/json`, `…+json`, **and no content type at all** |
| `YAMLCodec` | `application/yaml` | `application/x-yaml`, `text/yaml`, `text/x-yaml`, `…+yaml` |
| `TOMLCodec` | `application/toml` | `text/toml`, `…+toml` |
| `XMLCodec` | `application/xml` | `text/xml`, `…+xml` |
| `ProtobufCodec` | `application/x-protobuf` | `application/protobuf`, `application/vnd.google.protobuf`, `…+protobuf` |
| `AvroCodec` | `avro/binary` *or* `application/vnd.acemq.avro` | `application/avro`, `…+avro`; the other framing's type, never |
| `StringCodec` | `text/plain; charset=utf-8` | any `text/…` |
| `BytesCodec` | `application/octet-stream` | everything, including no content type |

**Only `JSONCodec` and `BytesCodec` answer for a message with no content type.**
YAML parses JSON quite happily, so a YAML codec that volunteered would give the
right value while recording that a YAML message had arrived — the sort of wrong
found much later. Protobuf and Avro are worse: their bytes are not recognisable,
and they parse into nonsense more often than they fail.

## Schema resolution

Handed the writer's schema and the reader's, Avro reconciles the two: a field the
writer added that the reader does not declare is skipped, and a field the writer
omitted is filled in from the reader's default. Handed only the writer's, there is
nothing to reconcile, and the record arrives in the shape it was written.

So there is one rule, and it is the same rule in all five AceMQ libraries:

> **Resolution happens when the library has a reader schema to resolve onto.**

What differs between the languages is where a reader schema comes from, and
therefore how often there is one. Nothing about the bytes differs.

| Library | Resolves | Where its reader schema comes from |
|---|---|---|
| Go | When asked | A Go struct carries no schema, so there is nothing to resolve onto until the caller passes `avro.ReaderSchema(...)` |
| Java | Sometimes | A generated `SpecificRecord` class carries a schema of its own, and `AvroCodec.registered(registry, readerSchema)` is handed one. A `GenericRecord` through a plain registry codec asks for nothing in particular, so the reader schema is the writer's and nothing resolves |
| .NET | By default | The codec is constructed with a schema, so there is always one to resolve onto unless the caller declines it: `Registered(registry, schema, readerSchema)` reads against a different schema than it writes, and `WithoutReaderSchema()` leaves the codec none at all |
| Python | Always | The codec is constructed with a schema |
| Ruby | Always | The codec is constructed with a schema |

This is not an inconsistency waiting to be flattened. A library that resolves and
a library that does not are both right about the same bytes — they are answering
different questions, because only one of them was told what the reader expects.

**The case that bites is a field the writer removed that the reader declares with
a default.** With resolution, the field arrives carrying that default. Without it,
the field is simply absent: a missing key, whatever the language calls one. A
consumer written against the reader schema then reads a value that was never on
the wire, or fails to read a field it is sure it declared, and which of those
happens is decided entirely by whether a reader schema was in play.

The other direction is the one people expect to be dangerous and is not. A field
the writer added that the reader does not declare is skipped under resolution and
present without it, and either way the fields the reader does declare come back
correct — the unknown field does not shift the ones after it.

Both cases are pinned, with the bytes, in
`spec/fixtures/avro-resolution-fixtures.json`, which every AceMQ library carries
a copy of. It records the decoded value under each behaviour, as `resolved` and
`writerShape`, and which library lands on which.

### Asking for resolution in Ruby

```ruby
# Resolves: reader_schema: defaults to the schema given, so this codec both
# writes and reads order.placed v3, and resolves anything older onto it.
codec = AvroCodec.registered(registry, subject: "order.placed", schema: v3_json)

# Resolves onto a schema of its own: publishes and registers v3, reads
# everything — whatever version wrote it — as v1.
consumer = AvroCodec.registered(registry, subject: "order.placed",
                                schema: v3_json, reader_schema: v1_json)
```

Being able to redeploy a producer without its consumers is the whole point of
putting a schema id on the front of the message, and resolution is the half of
that which happens on the read side. Ruby is in the `resolved` column either way,
so there is nothing to reach for: leaving `reader_schema:` out means the schema
given is both the one written and the one read, and a registry codec here can
never be left without one.

One edge worth knowing before you meet it: `reader_schema:` on a **fixed-schema**
codec — `AvroCodec.of(schema_json)` — is refused with an `ArgumentError` rather
than accepted and ignored. A fixed-schema codec reads what it writes by
definition, since nothing in those bytes says what wrote them, so there is no
writer's schema for a reader schema to resolve against and the setting would
quietly do nothing. The `writerShape` column is still reachable from Ruby, by
passing the writer's own schema as `reader_schema:`; `spec/codec_avro_resolution_spec.rb`
asserts both columns against the shared fixture that way.

## Proving it, rather than asserting it

A codec that encodes and decodes its own output proves nothing about reading
another language's message: both halves of the test share whatever the codec got
wrong. So `spec/fixtures/codec-samples.json` carries one order encoded twelve
times — once per format, by the Java library's codecs and by the Go library's —
and every codec here is tested against those bytes. What that turned up is the
argument for doing it: Jackson quotes TOML strings with `'` and BurntSushi with
`"`; Jackson wraps a list in an element of its own and `encoding/xml` repeats the
element; the two YAML writers indent lists differently. A round trip would have
shown none of it.


## Writing your own

A codec is anything answering four methods. **Not a base class to inherit
from** — that is what lets a codec from another gem work here without depending
on this one:

```ruby
class MessagePackCodec
  def content_type = "application/msgpack"
  def encode(payload) = MessagePack.pack(payload)

  def decode(body)
    MessagePack.unpack(body)
  rescue MessagePack::MalformedFormatError => e
    raise AceMQ::AMQP::DecodeError, "this message is not MessagePack: #{e.message}"
  end

  def can_decode?(content_type)
    content_type.to_s.downcase.start_with?("application/msgpack")
  end
end
```

Two things to get right:

**Raise `DecodeError` and `EncodeError`, not your library's own.** The consumer
treats a `DecodeError` as "nothing could read this" and
[parks](reliability.md#parked-messages) the message; an exception it does not
recognise is a handler failure and gets retried. A malformed body retried five
times is five identical failures and a dead letter that says the wrong thing.

Both inherit from `FatalError`, which is the same reason: retrying bytes that
will not parse is retrying a decision, not a transient fault.

**`decode` may take one argument or two.** A codec that chooses by content type
declares `decode(body, content_type)`; a plain one declares `decode(body)`. The
consumer asks the codec's arity rather than assuming, so either shape works, and
`CompositeCodec` passes the content type down to whichever candidate takes one —
which is how the Avro codec inside a composite still learns which framing it is
being handed.

The connection checks a codec when it is built, not when the first message
arrives:

```ruby
Connection.open(url, codec: Object.new)
# => ArgumentError: … is not a codec: it needs content_type, encode, decode and can_decode?
```

A codec missing a method should be a mistake at start-up rather than a mystery
at three in the morning.

## The registry

For choosing a codec by name from configuration:

```ruby
Codecs.names                  # => ["bytes", "json", "string", "toml", "xml", "yaml"]
Codecs.build("json")          # => a new JSONCodec

Codecs.register("msgpack") { MessagePackCodec.new }
Connection.open(url, codec: Codecs.build(ENV.fetch("MQ_CODEC", "json")))
```

Those six names mean the same six things in Java and Go, so a deployment that
sets `ACEMQ_CODEC` does not have to be rewritten per language.

**`protobuf` and `avro` are deliberately not names.** Both codecs are built
around a message type or a schema, a string in configuration cannot carry one,
and the Java library leaves them out of its own registry for the same reason.
Build them in code and pass the codec.

`register` takes a block that builds one rather than an instance, so every
caller gets its own — a codec that turned out to keep state would otherwise be
shared across every connection in the process. Registering a name twice replaces
the first, which is what lets a test override a default rather than having to
work around it.

## A codec that wraps a codec

`Patterns::ClaimCheckCodec` takes any of the above and puts a payload over 64
KiB into a store, sending the key instead. It is a codec like the others —
`Connection.open(url, codec: checked)` — and it keeps the delegate's content
type, because a claim-checked document is still a document. See
[the claim check](patterns.md#the-claim-check).

## YAML is parsed safely, and that costs something

A message body is input from another machine. `YAML.load` on one is remote code
execution — `!ruby/object:` and its relatives are how a body names the class to
instantiate, and a queue is exactly where a body from somewhere unexpected
arrives. `YAMLCodec` uses `Psych.safe_load`, which builds only what it has been
told about, and the cost of that lands on legitimate messages:

```ruby
YAMLCodec.new                                      # Date, Time, DateTime; no aliases
YAMLCodec.new(permitted_classes: [Symbol])         # for a Ruby-to-Ruby queue
YAMLCodec.new(aliases: true)                       # for a producer that uses anchors
```

- **Dates and timestamps arrive.** They are YAML scalar types, not Ruby ones, so
  `safe_load` refuses them unless the class is permitted. Java and Go both write
  timestamps into YAML, and refusing them would reject messages the other
  libraries send every day, so `Date`, `Time` and `DateTime` are permitted by
  default.
- **Symbols do not.** No other language sends one, so `Symbol` has to be asked
  for. Turning arbitrary remote text into symbols is a decision worth making on
  purpose rather than one a default makes for you.
- **Anchors and aliases are refused.** The billion-laughs expansion needs
  nothing else to take a consumer down, and neither Jackson nor gopkg.in/yaml.v3
  writes them, so refusing costs no interoperability. Pass `aliases: true` where
  the producer is trusted and really does use them.

On the way out, `encode` refuses a payload Psych can only write as
`!ruby/object:` — that is a message no other AceMQ library can read, and the
exact tag this codec refuses on the way in. Publish a Hash, an Array or a scalar.
One thing it does **not** rewrite: a symbol key goes on the wire as `:name`,
because that is how YAML spells a Ruby symbol and this library does not quietly
rename keys. No other language reads `:name` as `name` — spell keys as strings.

## Encrypting the body

`EncryptedCodec` wraps any of the codecs above and encrypts what it produced,
so the broker, its disk, its backups and everybody who can read its management
interface see ciphertext:

```ruby
keys = AceMQ::AMQP::Keyring.of("orders-2026-09", AceMQ::AMQP::Keys.from_base64(ENV["KEY"]))
codec = AceMQ::AMQP::EncryptedCodec.wrapping(AceMQ::AMQP::JSONCodec.new, keys)

mq = AceMQ::AMQP::Connection.open(url, codec: codec)
```

It wraps a delegate rather than serialising anything itself, so choosing a
format and choosing to encrypt stay independent: JSON in, AES-GCM out, and Avro
just as well. AES-GCM through Ruby's OpenSSL binding — nothing here hand-rolls a
cipher — with a fresh nonce per message from OpenSSL's own source. Reusing a
nonce under GCM does not weaken the encryption, it forfeits it, so a counter is
not an option however tempting it looks.

### What is on the wire

```
0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
```

**The key identifier travels in the clear**, and that is what makes rotation
possible: a consumer reads which key a message needs rather than assuming the
current one, so a new key can be introduced while messages written with the old
one are still queued. Putting it in an AMQP header instead would have been
tidier and would have lost it — headers are dropped by shovels, rewritten by
federation, and absent from a message recovered out of a backup, and a
ciphertext whose key nobody can name is gone.

The header is authenticated but not encrypted: GCM binds it as associated data,
so an altered key identifier makes the message fail to open rather than quietly
opening as something else.

The content type is `application/vnd.acemq.encrypted`, deliberately not
`…+json` whatever the plaintext underneath is. A `+json` suffix is a promise
that the bytes on the wire are JSON, and every JSON-aware consumer reads it that
way; these bytes are ciphertext.

### Rotating a key

A keyring writes with one key and reads with all of them:

```ruby
keys = AceMQ::AMQP::Keyring.new(
  AceMQ::AMQP::EncryptionKey.new("orders-2026-09", september),  # writes
  AceMQ::AMQP::EncryptionKey.new("orders-2026-06", june)        # still on some queue
)

keys.use("orders-2026-09")   # change what writes
keys.ids                     # => ["orders-2026-06", "orders-2026-09"]; never the keys
```

Add the new key everywhere first, so every consumer can read it, and only then
make it current somewhere. A keyring is anything answering `current` and
`key_for`, so one backed by a key management service is a small class rather
than a fork of this one — cache in it, because `key_for` is called for every
message decoded.

### Reading a queue you can no longer read

```ruby
AceMQ::AMQP::EncryptedCodec.key_id_of(body)   # => "orders-2026-06"
```

From the bytes alone, without the key. A dead-letter queue full of ciphertext is
normally a key that was retired too early rather than anything wrong with the
messages, and this answers that question for an operator who holds none of them.

### What it does not do

- **The broker can no longer read the message, and neither can the people who
  operate it.** Decide what they do instead before turning this on: the answer
  is usually a small internal tool holding the keyring rather than the
  management interface.
- **Encryption is not authorisation.** Every service holding the keyring can
  read every message encrypted with those keys. The granularity is the key, so
  separate audiences mean separate keys.
- **It does not authenticate the sender.** Anybody holding the key can write a
  message this codec will happily decrypt.
- **It does not hide the routing.** Exchange, routing key, headers and message
  size stay in the clear, and for many systems the routing key is the sensitive
  part.

A body that will not decrypt raises `DecodeError`, which is fatal — the same
bytes fail the same way next time — and the message says which key it named and
nothing else. It never contains the plaintext or the key, and it says the same
thing for a wrong key as for a tampered message, because GCM cannot tell them
apart and an error that could would be an oracle.

### The other libraries write the same bytes

Java, Go, .NET and Python all write `application/vnd.acemq.encrypted`, and all
four frame it the way this library does:

| | magic | key id length | IV / nonce | cipher | tag |
| --- | --- | --- | --- | --- | --- |
| **Java, Ruby, Go, .NET, Python** | `0xAE` | 1 byte | 12-byte nonce | AES-GCM | 16 bytes, AAD = header |
| .NET up to its 0.3.0 — read-only, nothing writes it | none | 1 byte | 16-byte IV | AES-256-CBC | 32-byte HMAC-SHA-256 |

A Ruby consumer reads a Java, Go, .NET or Python producer's encrypted messages
and every one of them reads Ruby's, given the same key, and `spec/crypto_spec.rb`
pins the exact layout — the same vector the other four test suites pin.

**.NET used to be the exception and no longer is.** It wrote AES-256-CBC with a
separate HMAC-SHA-256 up to its own 0.3.0 and moved to AES-GCM in this framing in
0.5.0, so a .NET producer and a Ruby consumer can share a key. What does not
cross is .NET's own *old* bodies: its codec still reads them so a queue holding
them can be drained, nothing writes them any more, and no other library has ever
read them. The two are told apart rather than guessed at — this framing begins
`0xAE`, that one begins `0x01`.

This library writes one framing and reads one framing. A body that does not begin
`0xAE` is refused as what it is rather than reported as a decryption failure, so
a consumer pointed at a plaintext queue is told what has actually happened.

## Next

- [Publishing](publishing.md) and [consuming](consuming.md) — where a codec is
  chosen
- [Reliability](reliability.md#parked-messages) — what happens to a body nothing
  could decode
- [The envelope](envelope.md) — the metadata, which is not the codec's business
