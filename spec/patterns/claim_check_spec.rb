# frozen_string_literal: true

# Copyright 2026 AceMQ.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "fileutils"
require "tmpdir"

require "acemq/amqp/patterns"

RSpec.describe AceMQ::AMQP::Patterns::ClaimCheckCodec do
  let(:store) { AceMQ::AMQP::Patterns::InMemoryClaimCheckStore.new }
  let(:codec) { described_class.wrapping(AceMQ::AMQP::JSONCodec.new, store) }

  # Larger than the 64 KiB threshold, and JSON so the delegate is doing real
  # work rather than passing bytes through.
  def large(size = described_class::DEFAULT_THRESHOLD)
    { "scan" => "x" * size }
  end

  describe "the wire contract, which is shared with the other AceMQ libraries" do
    # These are the values read out of
    # acemq-java-amqp/acemq-amqp-patterns/src/main/java/org/acemq/amqp/patterns/
    # ClaimCheckCodec.java. A message this codec writes has to be one a Java
    # consumer can read, so the numbers are pinned here rather than left to be
    # discovered when the two are pointed at the same queue.
    it "frames with the same three bytes Java writes" do
      expect(described_class::MAGIC).to eq(0xAC)
      expect(described_class::VERSION).to eq(0x01)
      expect(described_class::INLINE).to eq(0x00)
      expect(described_class::CHECKED).to eq(0x01)
      expect(described_class::HEADER).to eq(3)
    end

    it "offloads at 64 KiB, the same threshold Java defaults to" do
      expect(described_class::DEFAULT_THRESHOLD).to eq(64 * 1024)
      expect(described_class::DEFAULT_THRESHOLD).to eq(65_536)
    end

    it "writes 0xAC 0x01 0x00 in front of an inline payload" do
      body = codec.encode({ "id" => "A-1" })

      expect(body.bytes.first(3)).to eq([0xAC, 0x01, 0x00])
      # The rest is byte-for-byte what the delegate wrote, so a consumer that
      # strips three bytes has the delegate's own output and nothing else.
      expect(body.byteslice(3..)).to eq('{"id":"A-1"}')
    end

    it "writes 0xAC 0x01 0x01 in front of a claim check" do
      body = codec.encode(large)

      expect(body.bytes.first(3)).to eq([0xAC, 0x01, 0x01])
    end

    it "carries the store's key as bare UTF-8, with no URI or scheme around it" do
      # The part that decides whether two languages can exchange a large
      # message. Java writes the key the store returned, encoded as UTF-8, and
      # nothing else: no acemq://, no claim:, no length prefix. Anything
      # wrapped around it here would be something a Java consumer would hand
      # to its store verbatim and not find.
      body = codec.encode(large)
      key = body.byteslice(3..).force_encoding(Encoding::UTF_8)

      expect(key).to match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/)
      expect(key.encoding).to eq(Encoding::UTF_8)
      expect(store.get(key)).not_to be_nil
    end

    it "reads a frame another language wrote, given only the bytes" do
      # Assembled by hand rather than by this codec, which is the only way to
      # show that a message from elsewhere is readable. This is exactly what
      # Java's frame(CHECKED, key) produces.
      key = store.put('{"scan":"from java"}')
      foreign = +"\xAC\x01\x01".b
      foreign << key.b

      expect(codec.decode(foreign)).to eq({ "scan" => "from java" })
    end

    it "reads an inline frame another language wrote" do
      foreign = +"\xAC\x01\x00".b
      foreign << '{"scan":"small"}'

      expect(codec.decode(foreign)).to eq({ "scan" => "small" })
    end
  end

  describe "deciding whether to offload" do
    it "leaves a payload below the threshold in the message" do
      body = codec.encode({ "id" => "A-1" })

      expect(described_class.key_of(body)).to be_nil
      expect(store.size).to eq(0)
    end

    it "offloads a payload at the threshold, not merely above it" do
      # Java's test is `encoded.length < threshold` for inline, so a payload of
      # exactly the threshold goes to the store. An off-by-one here is a
      # message Java would read as a claim check and Ruby as a payload.
      at_threshold = described_class.wrapping(AceMQ::AMQP::BytesCodec.new, store, threshold: 10)

      expect(described_class.key_of(at_threshold.encode("x" * 9))).to be_nil
      expect(described_class.key_of(at_threshold.encode("x" * 10))).not_to be_nil
    end

    it "counts bytes rather than characters" do
      # Nine characters, eighteen bytes. A threshold is a statement about what
      # the broker has to carry, and the broker carries bytes.
      counted = described_class.wrapping(AceMQ::AMQP::BytesCodec.new, store, threshold: 12)

      expect(described_class.key_of(counted.encode("é" * 9))).not_to be_nil
    end

    it "offloads everything when the threshold is zero" do
      everything = described_class.wrapping(AceMQ::AMQP::JSONCodec.new, store, threshold: 0)
      everything.encode({ "id" => "A-1" })

      expect(store.size).to eq(1)
    end

    it "refuses a negative threshold" do
      expect { described_class.wrapping(AceMQ::AMQP::JSONCodec.new, store, threshold: -1) }
        .to raise_error(ArgumentError, /cannot be negative/)
    end
  end

  describe "round trips" do
    it "returns an inline payload unchanged" do
      expect(codec.decode(codec.encode({ "id" => "A-1" }))).to eq({ "id" => "A-1" })
    end

    it "returns an offloaded payload unchanged" do
      payload = large
      expect(codec.decode(codec.encode(payload))).to eq(payload)
    end

    it "keeps the text encoding of an offloaded payload" do
      # A payload that came back from a store is bytes, and handing those to a
      # text codec would give the caller a string whose encoding depended on
      # whether it happened to be large. The accent is what makes the
      # difference visible.
      text = described_class.wrapping(AceMQ::AMQP::StringCodec.new, store, threshold: 4)
      decoded = text.decode(text.encode("café au lait"))

      expect(decoded).to eq("café au lait")
      expect(decoded.encoding).to eq(Encoding::UTF_8)
    end

    it "carries bytes that are not text through the store untouched" do
      bytes = described_class.wrapping(AceMQ::AMQP::BytesCodec.new, store, threshold: 4)
      blob = [0x89, 0x50, 0x4E, 0x47, 0xFF, 0xFE].pack("C*")

      expect(bytes.decode(bytes.encode(blob)).bytes).to eq(blob.bytes)
    end
  end

  describe "messages this codec did not write" do
    it "reads a body with no framing as the delegate would" do
      # What makes it safe to put this codec in front of a live queue: the
      # messages already in it were written without the framing, and they still
      # have to be readable afterwards.
      expect(codec.decode('{"id":"A-1"}')).to eq({ "id" => "A-1" })
    end

    it "does not mistake a payload that happens to start with 0xAC for a frame" do
      passthrough = described_class.wrapping(AceMQ::AMQP::BytesCodec.new, store)
      # 0xAC 0x01 0x07: the magic and the version, but not a kind this codec
      # writes, so it is a payload rather than a frame.
      body = [0xAC, 0x01, 0x07, 0x42].pack("C*")

      expect(passthrough.decode(body).bytes).to eq([0xAC, 0x01, 0x07, 0x42])
    end

    it "reads a body shorter than the framing as a payload" do
      expect(codec.decode("1")).to eq(1)
    end
  end

  describe "a payload the store no longer holds" do
    it "fails fatally, and says why the store is the place to look" do
      body = codec.encode(large)
      store.delete(described_class.key_of(body))

      expect { codec.decode(body) }
        .to raise_error(AceMQ::AMQP::DecodeError, /retention has to outlast/)
    end

    it "is fatal rather than retryable, because the payload is not coming back" do
      body = codec.encode(large)
      store.clear

      expect { codec.decode(body) }.to raise_error(AceMQ::AMQP::FatalError)
    end
  end

  describe "reading a key without fetching it" do
    it "gives an operator the key from a dead-lettered message" do
      body = codec.encode(large)

      expect(described_class.key_of(body)).to be_a(String)
      expect(store.get(described_class.key_of(body))).not_to be_nil
    end

    it "is nil for an inline message and for one this codec did not write" do
      expect(described_class.key_of(codec.encode({ "id" => "A-1" }))).to be_nil
      expect(described_class.key_of('{"id":"A-1"}')).to be_nil
      expect(described_class.key_of("")).to be_nil
    end
  end

  describe "what it presents itself as" do
    it "keeps the delegate's content type, because the message is still that" do
      expect(codec.content_type).to eq("application/json")
      expect(codec.can_decode?("application/json")).to be(true)
      expect(codec.can_decode?("text/csv")).to be(false)
    end

    it "passes the content type on to a delegate that chooses by it" do
      # A {CompositeCodec} decides what to read a message with from the content
      # type the sender set, so a wrapper that swallowed it would turn a
      # multi-format queue into a queue of dead letters.
      composite = AceMQ::AMQP::CompositeCodec.new(AceMQ::AMQP::StringCodec.new,
                                                  AceMQ::AMQP::JSONCodec.new)
      wrapped = described_class.wrapping(composite, store, threshold: 4)
      body = wrapped.encode('{"id":"A-1"}')

      expect(described_class.key_of(body)).not_to be_nil
      expect(wrapped.decode(body, "application/json")).to eq({ "id" => "A-1" })
      expect(wrapped.decode(body, "text/plain")).to eq('{"id":"A-1"}')
    end

    it "refuses a delegate that is not a codec, at construction rather than at first use" do
      expect { described_class.wrapping(Object.new, store) }
        .to raise_error(ArgumentError, /is not a codec/)
    end
  end
end

RSpec.describe AceMQ::AMQP::Patterns::InMemoryClaimCheckStore do
  subject(:store) { described_class.new }

  it "hands back what it was given" do
    key = store.put("some bytes")

    expect(store.get(key)).to eq("some bytes")
  end

  it "issues a different key every time, even for identical payloads" do
    expect(store.put("same")).not_to eq(store.put("same"))
  end

  it "copies what it is handed, so a caller reusing its buffer cannot change it" do
    buffer = +"first"
    key = store.put(buffer)
    buffer << " and more"

    expect(store.get(key)).to eq("first")
  end

  it "copies what it hands back" do
    key = store.put("original")
    store.get(key) << " changed"

    expect(store.get(key)).to eq("original")
  end

  it "answers nil for a key it does not hold, rather than raising" do
    expect(store.get("nothing-here")).to be_nil
  end

  it "forgets a deleted payload" do
    key = store.put("bytes")
    store.delete(key)

    expect(store.get(key)).to be_nil
    expect(store.size).to eq(0)
  end
end

RSpec.describe AceMQ::AMQP::Patterns::FilesystemClaimCheckStore do
  let(:dir) { Dir.mktmpdir("acemq-claims") }
  let(:store) { described_class.new(File.join(dir, "payloads")) }

  after { FileUtils.remove_entry(dir) }

  it "creates the directory it was given" do
    expect(Dir).to exist(store.directory)
  end

  it "round trips a payload through a file" do
    key = store.put("a scanned report")

    expect(store.get(key)).to eq("a scanned report")
    expect(File.binread(File.join(store.directory, key))).to eq("a scanned report")
  end

  it "leaves no partial file behind" do
    store.put("a scanned report")

    expect(Dir.children(store.directory).grep(/partial/)).to be_empty
  end

  it "answers nil for a key it does not hold" do
    expect(store.get("a1b2c3d4-0000-0000-0000-000000000000")).to be_nil
  end

  it "forgets a deleted payload, and deleting one twice is not an error" do
    key = store.put("bytes")
    store.delete(key)
    store.delete(key)

    expect(store.get(key)).to be_nil
  end

  it "refuses a key that would escape the directory" do
    # A key arriving on a message is whatever a publisher put there, and it
    # becomes a path segment. Fatal rather than retryable: it will be the same
    # key on the fourth attempt.
    expect { store.get("../../etc/passwd") }
      .to raise_error(AceMQ::AMQP::FatalError, /checked rather than trusted/)
  end

  it "refuses an empty key and one with a separator in it" do
    expect { store.get("") }.to raise_error(AceMQ::AMQP::FatalError)
    expect { store.delete("nested/key") }.to raise_error(AceMQ::AMQP::FatalError)
  end

  it "carries a payload between two stores on the same directory" do
    # Which is the whole difference from the in-memory one: a consumer in
    # another process opens the same directory and finds the payload.
    key = store.put("shared")
    consumer_side = described_class.new(store.directory)

    expect(consumer_side.get(key)).to eq("shared")
  end
end
