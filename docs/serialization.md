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

`registered` frames each message with one zero byte, four bytes of schema
identifier big-endian, then the body — Confluent's layout, so messages written
here are read by their clients and by the Java, Go and .NET libraries, and the
other way round. Each mode claims **only its own** content type, because the two
framings are indistinguishable in the bytes: a fixed-schema codec reading a
framed message would decode the identifier as the first field, without throwing,
and hand back a record whose every value is wrong.

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
consumer asks the codec's arity rather than assuming, so either shape works.

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

### The other libraries do not agree about this yet

Java, Go and .NET all write `application/vnd.acemq.encrypted` and all three
write different bytes:

| | magic | key id length | IV / nonce | cipher | tag |
| --- | --- | --- | --- | --- | --- |
| **Java, Ruby** | `0xAE` | 1 byte | 12-byte nonce | AES-GCM | 16 bytes, AAD = header |
| Go | none | 2 bytes, big-endian | 12-byte nonce | AES-GCM | 16 bytes, AAD = header |
| .NET | none | 1 byte | 16-byte IV | AES-256-CBC | 32-byte HMAC-SHA-256 |

This library writes Java's, which is the only one of the three whose first byte
identifies the format at all. A Ruby consumer reads a Java producer's encrypted
messages and neither reads Go's or .NET's, and `spec/crypto_spec.rb` pins the
exact layout so that whoever converges the other two has something to converge
against.

## Next

- [Publishing](publishing.md) and [consuming](consuming.md) — where a codec is
  chosen
- [Reliability](reliability.md#parked-messages) — what happens to a body nothing
  could decode
- [The envelope](envelope.md) — the metadata, which is not the codec's business
