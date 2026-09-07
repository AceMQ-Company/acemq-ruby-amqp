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

require "acemq/amqp/patterns"

RSpec.describe AceMQ::AMQP::Patterns::InMemorySchemaRegistry do
  subject(:registry) { described_class.new }

  let(:v1) { '{"type":"record","name":"OrderPlaced","fields":[{"name":"id","type":"string"}]}' }
  let(:v2) do
    '{"type":"record","name":"OrderPlaced","fields":[{"name":"id","type":"string"},' \
      '{"name":"total","type":"long"}]}'
  end

  it "gives a schema an identifier and a version" do
    schema = registry.register("order.placed", "avro", v1)

    expect(schema.id).to eq(1)
    expect(schema.version).to eq(1)
    expect(schema.subject).to eq("order.placed")
    expect(schema.format).to eq("avro")
    expect(schema.fingerprint).to eq(AceMQ::AMQP::Patterns.fingerprint(v1))
  end

  it "returns the same schema when the same definition is registered again" do
    # A service that registers its schemas on every start would otherwise add a
    # version per restart, and a week later the subject has three hundred
    # identical versions.
    first = registry.register("order.placed", "avro", v1)
    again = registry.register("order.placed", "avro", v1)

    expect(again.id).to eq(first.id)
    expect(again.version).to eq(1)
    expect(registry.versions("order.placed").size).to eq(1)
  end

  it "counts versions within a subject and identifiers across all of them" do
    registry.register("order.placed", "avro", v1)
    registry.register("order.placed", "avro", v2)
    other = registry.register("order.shipped", "avro", v1)

    expect(registry.versions("order.placed").map(&:version)).to eq([1, 2])
    expect(other.version).to eq(1)
    expect(other.id).to eq(3)
  end

  it "finds the newest version of a subject" do
    registry.register("order.placed", "avro", v1)
    registry.register("order.placed", "avro", v2)

    expect(registry.latest("order.placed").version).to eq(2)
    expect(registry.latest("order.placed").definition).to eq(v2)
  end

  it "finds a schema by the identifier a message would carry" do
    schema = registry.register("order.placed", "avro", v1)

    expect(registry.by_id(schema.id).definition).to eq(v1)
  end

  it "raises rather than handing back an empty definition" do
    # A consumer reading a message whose schema it cannot find has a real
    # problem — usually a producer registered against a different registry —
    # and carrying on with nothing would turn that into a silently wrong
    # message.
    expect { registry.by_id(99) }
      .to raise_error(AceMQ::AMQP::Patterns::SchemaNotFound, /no schema with id 99/)
    expect { registry.latest("nobody.registered.this") }
      .to raise_error(AceMQ::AMQP::Patterns::SchemaNotFound, /nobody.registered.this/)
  end

  it "says a subject nobody registered has no versions, rather than raising" do
    # "What versions are there" has a sensible answer for a subject nobody has
    # registered; "which schema is this" does not.
    expect(registry.versions("nobody.registered.this")).to be_empty
  end

  it "refuses a schema with no subject or nothing in it" do
    expect { registry.register("", "avro", v1) }
      .to raise_error(ArgumentError, /needs a subject/)
    expect { registry.register("order.placed", "avro", "") }
      .to raise_error(ArgumentError, /a definition/)
  end

  it "treats definitions that differ only in whitespace as different schemas" do
    # Normalising would need a parser per format, and a registry that quietly
    # treated two definitions as one because it mis-parsed them would be worse
    # than one that is strict.
    first = registry.register("order.placed", "avro", v1)
    spaced = registry.register("order.placed", "avro", "#{v1} ")

    expect(spaced.id).not_to eq(first.id)
    expect(spaced.version).to eq(2)
  end

  it "fingerprints with SHA-256 of the exact bytes" do
    expect(AceMQ::AMQP::Patterns.fingerprint(""))
      .to eq("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
  end

  it "reads as something worth putting in a log" do
    expect(registry.register("order.placed", "avro", v1).to_s)
      .to eq("order.placed v1 (avro, id 1)")
  end
end
