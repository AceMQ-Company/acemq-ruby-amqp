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

**`CompositeCodec`** — for a queue carrying more than one format, during a
migration or where several producers were written years apart:

```ruby
codec = CompositeCodec.new(JSONCodec.new, StringCodec.new)
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
Codecs.names                  # => ["bytes", "json", "string"]
Codecs.build("json")          # => a new JSONCodec

Codecs.register("msgpack") { MessagePackCodec.new }
Connection.open(url, codec: Codecs.build(ENV.fetch("MQ_CODEC", "json")))
```

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

## What is not here

No YAML, TOML, Avro or Protocol Buffers codec, which the Go library has as
separate modules. Each of those is a dependency, and this gem has none: a codec
you can write in fifteen lines against the gem you were going to install anyway
is a better trade than a gem that pulls in five parsers to use one.

Encryption of message bodies is not here either. There is no cross-language
contract for it yet, and one invented here would be one the other AceMQ
libraries could not read.

## Next

- [Publishing](publishing.md) and [consuming](consuming.md) — where a codec is
  chosen
- [Reliability](reliability.md#parked-messages) — what happens to a body nothing
  could decode
- [The envelope](envelope.md) — the metadata, which is not the codec's business
