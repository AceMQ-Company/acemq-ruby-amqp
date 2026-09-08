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

require "acemq/amqp"

# Named at the top level the way the codec specs do, because reading an example
# should not mean unpicking a let block first.
Keys = AceMQ::AMQP::Keys
Keyring = AceMQ::AMQP::Keyring
EncryptionKey = AceMQ::AMQP::EncryptionKey

# The framing is the contract, so most of what follows asserts bytes rather than
# behaviour. A codec that encrypts and decrypts its own output perfectly and
# writes a different header from Java's is a codec that works alone and nowhere
# else, and nothing but a byte-level assertion catches that.
RSpec.describe AceMQ::AMQP::EncryptedCodec do
  # Fixed rather than generated, so the fixture below can be decrypted.
  KEY_BASE64 = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="

  # Produced by the Java library's own EncryptedCodec — the real class, not a
  # transcription of it — encrypting the bytes of {"order_id":"J-1"} under the
  # key above with the identifier "orders-2026-09". It is here so that the one
  # claim worth making about this file, that Ruby reads what Java writes, is
  # checked on every run rather than the day somebody remembers to try it.
  JAVA_CIPHERTEXT =
    "rgEOb3JkZXJzLTIwMjYtMDkZ6oOeOit8INgv1PYqgbslXIGMrsp6Xb3TZvv4mhVeUd+ADdKVmbENwVYo1px0"

  let(:secret) { Keys.from_base64(KEY_BASE64) }
  let(:keyring) { Keyring.of("orders-2026-09", secret) }
  let(:codec) { described_class.wrapping(AceMQ::AMQP::JSONCodec.new, keyring) }
  let(:payload) { { "order_id" => "A-1" } }

  describe "the framing" do
    let(:body) { codec.encode(payload) }

    it "starts with the magic byte and the version, so a message from elsewhere is refused" do
      expect(body.getbyte(0)).to eq(0xAE)
      expect(body.getbyte(1)).to eq(0x01)
    end

    it "carries the key identifier's length in a single byte" do
      expect(body.getbyte(2)).to eq("orders-2026-09".bytesize)
    end

    it "carries the key identifier in the clear, which is what makes rotation possible" do
      expect(body.byteslice(3, 14)).to eq("orders-2026-09")
    end

    it "puts a twelve byte nonce after the header and a sixteen byte tag at the end" do
      plaintext = AceMQ::AMQP::JSONCodec.new.encode(payload)
      # 3 header bytes + 14 identifier + 12 nonce + the plaintext + a 16 byte tag.
      expect(body.bytesize).to eq(3 + 14 + 12 + plaintext.bytesize + 16)
    end

    it "is binary: a body forced into UTF-8 is a body that will not survive a codec" do
      expect(body.encoding).to eq(Encoding::BINARY)
    end
  end

  describe "the content type" do
    it "is the one every AceMQ library writes" do
      expect(codec.content_type).to eq("application/vnd.acemq.encrypted")
    end

    it "is deliberately not a +json type, whatever the plaintext underneath is" do
      expect(codec.content_type).not_to include("json")
    end

    it "answers for its own type and for nothing else" do
      expect(codec.can_decode?("application/vnd.acemq.encrypted")).to be(true)
      expect(codec.can_decode?("application/vnd.acemq.encrypted; charset=utf-8")).to be(true)
      expect(codec.can_decode?("application/json")).to be(false)
      # Not an untyped message either: trying to decrypt plaintext reports the
      # failure as a decode error and sends whoever is debugging it the wrong way.
      expect(codec.can_decode?("")).to be(false)
    end
  end

  # The five libraries do not agree on this format yet — Go writes no magic byte
  # and a two-byte length, .NET writes no magic byte, a 16-byte IV and
  # AES-CBC with a 32-byte HMAC — and all three put the same content type on the
  # message. This block is what the other two have to be converged against: a
  # complete frame built from a fixed key, a fixed nonce and a fixed plaintext,
  # so "does your implementation write these bytes?" is a question with a yes or
  # no answer rather than an afternoon of reading each other's source.
  #
  # Built with raw OpenSSL rather than with this codec, so it is not the codec
  # asserted against itself, and confirmed to decrypt under the Java library's
  # own EncryptedCodec.
  describe "the pinned byte layout" do
    # key       0x00 0x01 ... 0x1F     (the 32 bytes of KEY_BASE64)
    # key id    "orders-2026-09"
    # nonce     0xA0 0xA1 ... 0xAB
    # plaintext {"order_id":"A-1"}
    VECTOR_HEX = "ae010e6f72646572732d323032362d3039" \
                 "a0a1a2a3a4a5a6a7a8a9aaab" \
                 "9d3a135f21ae70e00b01a5e9253bedef52d1a7f3e6" \
                 "1950b1d99f4bb3fb7c19e71b17"

    let(:vector) { [VECTOR_HEX].pack("H*") }

    it "is 3 header, 14 identifier, 12 nonce, 18 of ciphertext and a 16 byte tag" do
      expect(vector.bytesize).to eq(3 + 14 + 12 + 18 + 16)
    end

    it "puts the magic byte, the version and the identifier length in the first three" do
      expect(vector.byteslice(0, 3).unpack("C3")).to eq([0xAE, 0x01, 14])
    end

    it "puts the identifier next, in UTF-8, unencrypted" do
      expect(vector.byteslice(3, 14)).to eq("orders-2026-09")
    end

    it "puts the nonce after the identifier" do
      expect(vector.byteslice(17, 12).unpack("C*")).to eq((0xA0..0xAB).to_a)
    end

    it "decrypts to the plaintext it was built from" do
      expect(codec.decode(vector)).to eq({ "order_id" => "A-1" })
    end

    # Nothing else about the frame pins the associated data, because a codec
    # that authenticated the wrong bytes would still round trip against itself.
    # This recomputes the whole frame from outside the library, taking the
    # header as the associated data, and requires the bytes to match exactly.
    it "authenticates the header and nothing else" do
      written = codec.encode({ "order_id" => "A-1" })
      nonce = written.byteslice(17, 12)
      header = written.byteslice(0, 17)

      cipher = OpenSSL::Cipher.new("aes-256-gcm")
      cipher.encrypt
      cipher.key = secret
      cipher.iv = nonce
      cipher.auth_data = header
      sealed = cipher.update('{"order_id":"A-1"}') + cipher.final

      expect(written).to eq(header + nonce + sealed + cipher.auth_tag(16))
    end
  end

  describe "reading what the other libraries write" do
    it "decrypts a body the Java library produced" do
      expect(codec.decode(JAVA_CIPHERTEXT.unpack1("m0"))).to eq({ "order_id" => "J-1" })
    end

    it "names the key a Java body was written with without holding it" do
      expect(described_class.key_id_of(JAVA_CIPHERTEXT.unpack1("m0"))).to eq("orders-2026-09")
    end
  end

  describe "a round trip" do
    it "gives back what it was given" do
      expect(codec.decode(codec.encode(payload))).to eq(payload)
    end

    it "encrypts whatever the delegate produced rather than serialising anything itself" do
      bytes = described_class.wrapping(AceMQ::AMQP::StringCodec.new, keyring)
      expect(bytes.decode(bytes.encode("a line of text"))).to eq("a line of text")
    end

    it "draws a fresh nonce for every message" do
      nonces = Array.new(50) { codec.encode(payload).byteslice(17, 12) }
      expect(nonces.uniq.size).to eq(50)
    end

    it "produces different bytes for the same payload, which is what a fresh nonce means" do
      expect(codec.encode(payload)).not_to eq(codec.encode(payload))
    end
  end

  describe "the associated data" do
    # The header is authenticated but not encrypted. This is the assertion that
    # proves it: without the header as associated data, a key identifier swapped
    # in flight would decrypt perfectly well under the substituted name.
    it "binds the key identifier to the ciphertext" do
      keyring.add(EncryptionKey.new("some-other-name", secret))
      body = codec.encode(payload)
      substituted = described_class.header_for(EncryptionKey.new("some-other-name", secret)) +
                    body.byteslice(17, body.bytesize - 17)

      expect { codec.decode(substituted) }.to raise_error(AceMQ::AMQP::DecodeError)
    end
  end

  describe "a body that will not decrypt" do
    let(:other) { Keyring.of("orders-2026-09", Keys.generate) }

    it "says the same thing for the wrong key as for a tampered message" do
      body = codec.encode(payload)
      tampered = body.dup
      tampered.setbyte(20, tampered.getbyte(20) ^ 0xFF)

      wrong_key = described_class.wrapping(AceMQ::AMQP::JSONCodec.new, other)
      messages = [
        message_from { wrong_key.decode(body) },
        message_from { codec.decode(tampered) }
      ]
      expect(messages.uniq.size).to eq(1)
    end

    it "never puts the plaintext in the message" do
      secretish = { "card" => "4111111111111111" }
      body = codec.encode(secretish)
      body.setbyte(30, body.getbyte(30) ^ 0xFF)

      expect(message_from { codec.decode(body) }).not_to include("4111")
    end

    it "never puts the key in the message" do
      body = codec.encode(payload)
      body.setbyte(30, body.getbyte(30) ^ 0xFF)

      expect(message_from { codec.decode(body) }).not_to include(secret.unpack1("H*")[0, 8])
    end

    it "is fatal rather than retryable, because the same bytes fail the same way next time" do
      body = codec.encode(payload)
      body.setbyte(30, body.getbyte(30) ^ 0xFF)

      expect { codec.decode(body) }.to raise_error(AceMQ::AMQP::FatalError)
    end

    it "says a message is not this codec's when the framing is not there" do
      expect { codec.decode('{"order_id":"A-1"}') }
        .to raise_error(AceMQ::AMQP::DecodeError, /not written by EncryptedCodec/)
    end

    it "says a truncated message is truncated" do
      body = codec.encode(payload)
      expect { codec.decode(body.byteslice(0, 25)) }
        .to raise_error(AceMQ::AMQP::DecodeError, /too short/)
    end

    it "names the key a message wanted when the keyring does not hold it" do
      body = codec.encode(payload)
      elsewhere = described_class.wrapping(AceMQ::AMQP::JSONCodec.new,
                                           Keyring.of("retired-key", Keys.generate))

      expect { elsewhere.decode(body) }
        .to raise_error(AceMQ::AMQP::DecodeError, /orders-2026-09.*retired-key/m)
    end
  end

  describe ".key_id_of" do
    it "reads the identifier out of a body without the key" do
      expect(described_class.key_id_of(codec.encode(payload))).to eq("orders-2026-09")
    end

    it "is nil for anything this codec did not write" do
      expect(described_class.key_id_of('{"a":1}')).to be_nil
      expect(described_class.key_id_of("")).to be_nil
      expect(described_class.key_id_of([0xAE, 0x02, 3, 97, 98, 99].pack("C*"))).to be_nil
      expect(described_class.key_id_of([0xAE, 0x01, 0].pack("C*"))).to be_nil
      expect(described_class.key_id_of([0xAE, 0x01, 9, 97].pack("C*"))).to be_nil
    end
  end

  describe "what it will not say" do
    it "keeps the key out of the codec's own description" do
      expect(codec.to_s).to include("orders-2026-09")
      expect(codec.to_s).not_to include(secret[0, 4])
    end
  end

  def message_from
    yield
    raise "expected a failure and got none"
  rescue AceMQ::AMQP::DecodeError => e
    e.message
  end
end

RSpec.describe AceMQ::AMQP::Keys do
  it "generates a 256-bit key by default" do
    expect(described_class.generate.bytesize).to eq(32)
  end

  it "generates the other two AES lengths when asked" do
    expect(described_class.generate(bits: 128).bytesize).to eq(16)
    expect(described_class.generate(bits: 192).bytesize).to eq(24)
  end

  it "refuses a length AES does not have" do
    expect { described_class.generate(bits: 512) }
      .to raise_error(ArgumentError, /128, 192 or 256/)
  end

  it "draws a different key every time" do
    expect(Array.new(20) { described_class.generate }.uniq.size).to eq(20)
  end

  it "refuses key material that is the wrong length rather than padding it" do
    expect { described_class.from_bytes("short") }
      .to raise_error(ArgumentError, /16, 24 or 32 bytes/)
  end

  it "points at a key derivation function, because that is what a passphrase needs" do
    expect { described_class.from_bytes("hunter2") }.to raise_error(ArgumentError, /PBKDF2/)
  end

  it "round trips through Base64, which is how a key arrives from a secret store" do
    key = described_class.generate
    expect(described_class.from_base64(described_class.to_base64(key))).to eq(key)
  end

  it "does not put the value in the message when Base64 will not decode" do
    expect { described_class.from_base64("not base64 at all!!") }
      .to raise_error(ArgumentError) { |e| expect(e.message).not_to include("not base64") }
  end
end

RSpec.describe AceMQ::AMQP::EncryptionKey do
  let(:secret) { AceMQ::AMQP::Keys.generate }

  it "refuses an empty identifier, because it is what a reader looks the key up by" do
    expect { described_class.new("", secret) }.to raise_error(ArgumentError, /cannot be empty/)
  end

  it "refuses an identifier longer than the single length byte holds" do
    expect { described_class.new("k" * 256, secret) }
      .to raise_error(ArgumentError, /at most 255 bytes/)
  end

  it "takes an identifier of exactly 255 bytes" do
    expect(described_class.new("k" * 255, secret).id.bytesize).to eq(255)
  end

  it "never shows the key" do
    key = described_class.new("orders-2026-09", secret)
    expect(key.to_s).to eq("EncryptionKey{id=orders-2026-09}")
    expect(key.inspect).not_to include(secret[0, 4])
  end

  it "names the cipher for the key's length" do
    expect(described_class.new("k", AceMQ::AMQP::Keys.generate(bits: 128)).cipher_name)
      .to eq("aes-128-gcm")
    expect(described_class.new("k", AceMQ::AMQP::Keys.generate(bits: 256)).cipher_name)
      .to eq("aes-256-gcm")
  end
end

RSpec.describe AceMQ::AMQP::Keyring do
  let(:june) { AceMQ::AMQP::EncryptionKey.new("orders-2026-06", AceMQ::AMQP::Keys.generate) }
  let(:september) { AceMQ::AMQP::EncryptionKey.new("orders-2026-09", AceMQ::AMQP::Keys.generate) }

  it "writes with the first key it was given" do
    expect(described_class.new(september, june).current).to eq(september)
  end

  it "reads with every key it holds, which is what rotation needs" do
    ring = described_class.new(september, june)
    expect(ring.key_for("orders-2026-06")).to eq(june)
    expect(ring.ids).to eq(%w[orders-2026-06 orders-2026-09])
  end

  it "adds a key without making it the one that writes" do
    ring = described_class.new(september)
    ring.add(june)
    expect(ring.current).to eq(september)
    expect(ring.key_for("orders-2026-06")).to eq(june)
  end

  it "changes which key writes" do
    ring = described_class.new(june, september)
    expect(ring.use("orders-2026-09").current).to eq(september)
  end

  it "refuses to write with a key it does not hold" do
    expect { described_class.new(june).use("nothing") }.to raise_error(ArgumentError, /no key/)
  end

  it "needs at least one key" do
    expect { described_class.new }.to raise_error(ArgumentError, /at least one key/)
  end

  it "says which keys it holds when a message names one it does not" do
    expect { described_class.new(june).key_for("orders-2026-09") }
      .to raise_error(AceMQ::AMQP::DecodeError, /orders-2026-06/)
  end

  it "never shows the keys" do
    ring = described_class.new(september, june)
    expect(ring.to_s)
      .to eq("Keyring{current=orders-2026-09, holds=orders-2026-06, orders-2026-09}")
    expect(ring.inspect).not_to include(september.secret[0, 4])
  end

  it "is anything answering current and key_for" do
    fixed = Struct.new(:key) do
      def current = key
      def key_for(_id) = key
    end.new(september)

    codec = AceMQ::AMQP::EncryptedCodec.wrapping(AceMQ::AMQP::JSONCodec.new, fixed)
    expect(codec.decode(codec.encode({ "a" => 1 }))).to eq({ "a" => 1 })
  end

  it "refuses something that is not a keyring at all" do
    expect { AceMQ::AMQP::EncryptedCodec.wrapping(AceMQ::AMQP::JSONCodec.new, Object.new) }
      .to raise_error(ArgumentError, /not a keyring/)
  end
end
