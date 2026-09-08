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

JSONCodec = AceMQ::AMQP::JSONCodec
BytesCodec = AceMQ::AMQP::BytesCodec
StringCodec = AceMQ::AMQP::StringCodec
CompositeCodec = AceMQ::AMQP::CompositeCodec
Codecs = AceMQ::AMQP::Codecs
DecodeError = AceMQ::AMQP::DecodeError

RSpec.describe AceMQ::AMQP::JSONCodec do
  it "writes the content type Java and .NET write" do
    # The content type is what tells a consumer in another language which
    # codec to reach for, so it is contract rather than preference.
    expect(described_class.new.content_type).to eq("application/json")
  end

  it "goes round" do
    codec = described_class.new
    expect(codec.decode(codec.encode({ "orderId" => "A-1", "totalCents" => 250 })))
      .to eq({ "orderId" => "A-1", "totalCents" => 250 })
  end

  it "leaves keys spelled the way the payload spells them" do
    # A codec that renamed total_cents to totalCents would be deciding the
    # cross-language contract on the caller's behalf, in a place nobody reads.
    expect(described_class.new.encode({ "total_cents" => 1 })).to eq('{"total_cents":1}')
  end

  it "comes back with symbols when asked, and strings when not" do
    expect(described_class.new(symbolize_names: true).decode('{"a":1}')).to eq({ a: 1 })
    expect(described_class.new.decode('{"a":1}')).to eq({ "a" => 1 })
  end

  it "treats a body that is not JSON as fatal" do
    # Bytes do not improve with age. Retrying this produces the same failure
    # on every attempt, so it must not read as retryable.
    expect { described_class.new.decode("not json at all") }.to raise_error(DecodeError)
    expect(DecodeError.ancestors).to include(AceMQ::AMQP::FatalError)
  end

  it "answers for json media types, and for a message with no content type" do
    codec = described_class.new
    ["application/json", "application/json; charset=utf-8", "text/json",
     "application/vnd.acemq.order+json", ""].each do |type|
      expect(codec.can_decode?(type)).to be(true), "expected to claim #{type.inspect}"
    end
    ["text/plain", "application/octet-stream", "application/xml"].each do |type|
      expect(codec.can_decode?(type)).to be(false), "expected to refuse #{type.inspect}"
    end
  end
end

RSpec.describe AceMQ::AMQP::BytesCodec do
  it "hands back exactly what arrived" do
    # Replaying a dead letter depends on this: the bytes that were committed
    # are the bytes that should go back out.
    body = "\x00\x01not text at all"
    expect(described_class.new.decode(body)).to eq(body)
  end

  it "claims every content type, which is why it has to be asked for by name" do
    expect(described_class.new.can_decode?("application/json")).to be(true)
    expect(described_class.new.can_decode?("")).to be(true)
  end
end

RSpec.describe AceMQ::AMQP::StringCodec do
  it "reads text and refuses everything else" do
    codec = described_class.new
    expect(codec.can_decode?("text/plain; charset=utf-8")).to be(true)
    expect(codec.can_decode?("application/json")).to be(false)
  end

  it "does not claim a message with no content type" do
    # JSON does claim it, and an untyped message is far more likely to be JSON.
    # A text codec claiming it too would take those messages away from the
    # codec that can actually read them.
    expect(described_class.new.can_decode?("")).to be(false)
  end
end

RSpec.describe AceMQ::AMQP::CompositeCodec do
  let(:codec) { described_class.new(JSONCodec.new, StringCodec.new) }

  it "writes with the first codec" do
    expect(codec.content_type).to eq("application/json")
    expect(codec.encode({ "a" => 1 })).to eq('{"a":1}')
  end

  it "reads with whichever codec claims the content type" do
    expect(codec.decode('{"a":1}', "application/json")).to eq({ "a" => 1 })
    expect(codec.decode("a line of a log", "text/plain")).to eq("a line of a log")
  end

  it "treats every codec as a candidate when the sender set no content type" do
    # A sender that said nothing has ruled nothing out. Guessing one format and
    # failing on the rest would turn a silent producer into a queue of dead
    # letters.
    expect(codec.decode('{"a":1}')).to eq({ "a" => 1 })
    expect(codec.decode("a line of a log")).to eq("a line of a log")
  end

  it "says what it tried when nothing could read it" do
    expect { codec.decode("{ not json", "application/json") }
      .to raise_error(DecodeError, %r{application/json})
  end

  it "names what it holds when the content type matches nothing" do
    expect { codec.decode("anything", "application/xml") }
      .to raise_error(DecodeError, %r{application/xml.*application/json, text/plain}m)
  end

  it "refuses to be built empty" do
    expect { described_class.new }.to raise_error(ArgumentError, /at least one codec/)
  end

  it "refuses something that is not a codec" do
    expect { described_class.new(Object.new) }.to raise_error(ArgumentError, /not a codec/)
  end
end

RSpec.describe AceMQ::AMQP::Codecs do
  it "knows the six names the other libraries know" do
    # A deployment that names a format in configuration should not have to be
    # rewritten per language. Protobuf and Avro are not among them in any
    # library: both are built around a message type or a schema, and a name in
    # configuration cannot carry one.
    expect(Codecs.names).to eq(%w[bytes json string toml xml yaml])
  end

  it "builds by name" do
    expect(Codecs.build("json")).to be_a(JSONCodec)
    expect(Codecs.build("bytes")).to be_a(BytesCodec)
    expect(Codecs.build("string")).to be_a(StringCodec)
  end

  it "says what it does know when asked for something it does not" do
    expect do
      Codecs.build("msgpack")
    end.to raise_error(ArgumentError, /known: bytes, json, string, toml, xml, yaml/)
  end

  it "lets a name be taken over, which is what makes a default overridable" do
    Codecs.register("json") { StringCodec.new }
    expect(Codecs.build("json")).to be_a(StringCodec)
  ensure
    Codecs.register("json") { JSONCodec.new }
  end
end
