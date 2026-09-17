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
require "acemq/amqp/patterns/schema"

# The versions of one message type, in the order somebody would actually write
# them: a field added with a default, and then that field gone again.
#
# All three keep the full name, because two record schemas that disagree about
# it are not two versions of anything and Avro says so before resolution begins.
AVRO_V1 = <<~JSON
  {"type": "record", "name": "Order", "namespace": "acme",
   "fields": [{"name": "orderId", "type": "string"},
              {"name": "total", "type": "double"}]}
JSON

AVRO_V2 = <<~JSON
  {"type": "record", "name": "Order", "namespace": "acme",
   "fields": [{"name": "orderId", "type": "string"},
              {"name": "total", "type": "double"},
              {"name": "tier", "type": "string", "default": "bronze"}]}
JSON

# The change Avro cannot resolve: a field whose type moved under it.
AVRO_RETYPED = <<~JSON
  {"type": "record", "name": "Order", "namespace": "acme",
   "fields": [{"name": "orderId", "type": "long"},
              {"name": "total", "type": "double"}]}
JSON

# The other one: a field added without a default, so a message written before it
# existed carries nothing to put there.
AVRO_NO_DEFAULT = <<~JSON
  {"type": "record", "name": "Order", "namespace": "acme",
   "fields": [{"name": "orderId", "type": "string"},
              {"name": "total", "type": "double"},
              {"name": "tier", "type": "string"}]}
JSON

RSpec.describe AceMQ::AMQP::AvroCodec do
  subject(:reader) do
    described_class.registered(registry, subject: "acme.Order",
                                         schema: AVRO_V1, reader_schema: AVRO_V2)
  end

  let(:registry) { AceMQ::AMQP::Patterns::InMemorySchemaRegistry.new }

  # A producer that has moved on, sharing the registry the way two processes
  # pointed at the same registry would.
  let(:producer) do
    described_class.registered(registry, subject: "acme.Order", schema: AVRO_V2)
  end

  let(:v1_producer) do
    described_class.registered(registry, subject: "acme.Order", schema: AVRO_V1)
  end

  describe "resolving a writer's schema onto a reader's" do
    it "fills in a field the writer never sent from the reader's default" do
      # The case the whole feature exists for: the consumer was redeployed
      # first, and the producer has not started sending `tier` yet.
      body = v1_producer.encode({ "orderId" => "A-1", "total" => 12.5 })

      expect(reader.decode(body, v1_producer.content_type))
        .to eq({ "orderId" => "A-1", "total" => 12.5, "tier" => "bronze" })
    end

    it "ignores a field the writer added that the reader has never heard of" do
      # The same pair the other way round, which is the case that used to
      # break consumers: without resolution the extra field shifts everything
      # after it and the record comes back wrong rather than short.
      old = described_class.registered(registry, subject: "acme.Order",
                                                 schema: AVRO_V2, reader_schema: AVRO_V1)
      body = producer.encode({ "orderId" => "A-2", "total" => 3.0, "tier" => "gold" })

      expect(old.decode(body, producer.content_type))
        .to eq({ "orderId" => "A-2", "total" => 3.0 })
    end

    it "is real resolution rather than a re-parse, so the default is the reader's own" do
      # A re-parse against the writer's schema would hand back exactly what was
      # sent. What comes back here is a field that was never on the wire at all.
      body = v1_producer.encode({ "orderId" => "A-3", "total" => 1.0 })

      expect(reader.decode(body, v1_producer.content_type).fetch("tier")).to eq("bronze")
      expect(body).not_to include("bronze")
    end

    it "resolves whichever version wrote the message, on the same codec" do
      # Two producers, one consumer, one registry. This is what a rolling
      # deployment looks like from the reader's side.
      old_body = v1_producer.encode({ "orderId" => "A-4", "total" => 2.0 })
      new_body = producer.encode({ "orderId" => "A-5", "total" => 4.0, "tier" => "silver" })

      expect(reader.decode(old_body, v1_producer.content_type).fetch("tier")).to eq("bronze")
      expect(reader.decode(new_body, producer.content_type).fetch("tier")).to eq("silver")
    end
  end

  describe "a change Avro cannot resolve" do
    it "refuses a field whose type changed, and names both schemas" do
      body = v1_producer.encode({ "orderId" => "A-6", "total" => 5.0 })
      broken = described_class.registered(registry, subject: "acme.Order",
                                                    schema: AVRO_V1,
                                                    reader_schema: AVRO_RETYPED)

      expect { broken.decode(body, v1_producer.content_type) }
        .to raise_error(AceMQ::AMQP::DecodeError) { |error|
          expect(error.message).to include("written against acme.Order")
          expect(error.message).to include("this codec reads acme.Order")
          expect(error.message).to include("incompatible change rather than an evolution")
          # The writer's schema in full, because it is the one nobody has in
          # front of them: it was registered by another process.
          expect(error.message).to include('"name":"orderId","type":"string"')
        }
    end

    it "refuses a field added without a default, which fails as no schema mismatch at all" do
      # Avro raises a plain AvroError here rather than a SchemaMatchException,
      # so this is the case that proves the codec asks the schemas rather than
      # reading the exception's class.
      body = v1_producer.encode({ "orderId" => "A-7", "total" => 6.0 })
      broken = described_class.registered(registry, subject: "acme.Order",
                                                    schema: AVRO_V1,
                                                    reader_schema: AVRO_NO_DEFAULT)

      expect { broken.decode(body, v1_producer.content_type) }
        .to raise_error(AceMQ::AMQP::DecodeError, /incompatible change rather than/)
    end

    it "is fatal, because the same bytes fail the same way next time" do
      body = v1_producer.encode({ "orderId" => "A-8", "total" => 7.0 })
      broken = described_class.registered(registry, subject: "acme.Order",
                                                    schema: AVRO_V1,
                                                    reader_schema: AVRO_RETYPED)

      expect { broken.decode(body, v1_producer.content_type) }
        .to raise_error(AceMQ::AMQP::FatalError)
    end

    it "still says a body is not Avro when the body is the problem" do
      # Truncated bytes are not a schema disagreement, and saying they were
      # would send whoever reads the message to change a schema for nothing.
      expect { reader.decode("\x00\x00\x00\x00\x01not avro", producer.content_type) }
        .to raise_error(AceMQ::AMQP::DecodeError, /not Avro that reads as acme.Order/)
    end
  end

  describe "what a reader schema does not change" do
    it "writes the same bytes with one as without" do
      # The reader schema is read-side only. A codec that wrote differently
      # because of one would break every consumer it was meant to protect.
      payload = { "orderId" => "A-9", "total" => 8.0 }

      expect(reader.encode(payload)).to eq(v1_producer.encode(payload))
    end

    it "writes and registers the schema it was given, not the one it reads" do
      reader.encode({ "orderId" => "A-10", "total" => 9.0 })
      registered = registry.by_id(1)

      expect(registered.format).to eq("avro")
      expect(registered.definition).to include('"total"')
      expect(registered.definition).not_to include('"tier"')
    end

    it "claims the registry content type, as a registered codec does" do
      expect(reader.content_type).to eq("application/vnd.acemq.avro")
      expect(reader.registered?).to be(true)
    end

    it "leaves the registered path without one reading exactly as it did" do
      # Omitting reader_schema has to keep meaning what it meant: the schema
      # given is both the one written and the one read.
      plain = described_class.registered(registry, subject: "acme.Order", schema: AVRO_V2)
      payload = { "orderId" => "A-11", "total" => 10.0, "tier" => "gold" }

      expect(plain.decode(plain.encode(payload), plain.content_type)).to eq(payload)
    end

    it "leaves the fixed-schema path alone" do
      fixed = described_class.of(AVRO_V1)
      payload = { "orderId" => "A-12", "total" => 11.0 }

      expect(fixed.content_type).to eq("avro/binary")
      expect(fixed.registered?).to be(false)
      expect(fixed.decode(fixed.encode(payload), "avro/binary")).to eq(payload)
    end

    it "refuses a reader schema on a codec with no registry to resolve against" do
      # A fixed-schema codec reads what it writes by definition, so a reader
      # schema there would be a setting that quietly did nothing.
      expect { described_class.new(schema: AVRO_V1, reader_schema: AVRO_V2) }
        .to raise_error(ArgumentError, /only means something with a registry/)
    end

    it "refuses a reader schema that is not a schema" do
      expect do
        described_class.registered(registry, subject: "acme.Order",
                                             schema: AVRO_V1, reader_schema: "{nonsense")
      end.to raise_error(ArgumentError, /not a usable Avro schema/)
    end
  end
end
