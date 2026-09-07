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

require "json"
require "acemq/amqp"

Headers = AceMQ::AMQP::Headers
Envelope = AceMQ::AMQP::Envelope

FIXTURES = JSON.parse(File.read(File.join(__dir__, "fixtures", "envelope-fixtures.json")))

RSpec.describe AceMQ::AMQP::Envelope do
  it "is held to the fixtures the other languages are held to" do
    # Produced by the Java implementation and shared with Go, .NET and Python.
    # If the contract ever moves, every language should hear it from its own
    # test suite rather than from production.
    expect(FIXTURES["generatedBy"]).to eq("acemq-java-amqp FixtureGen")
    expect(FIXTURES["cases"]).not_to be_empty
  end

  FIXTURES["cases"].each do |fixture|
    context "the #{fixture["case"]} case" do
      let(:expected) { fixture["headers"] }
      let(:envelope) { Envelope.from_headers(fixture["headers"], fixture["routingKey"]) }

      it "reads every header Java wrote" do
        expect(envelope.id).to eq(expected[Headers::ID])
        expect(envelope.type).to eq(expected[Headers::TYPE])
        expect(envelope.version).to eq(expected[Headers::VERSION])
        expect(envelope.correlation_id).to eq(expected[Headers::CORRELATION])
        expect(envelope.attempt).to eq(expected[Headers::ATTEMPT])
        expect(envelope.origin).to eq(expected.fetch(Headers::ORIGIN, ""))
      end

      it "reads first-seen as epoch milliseconds" do
        # Not seconds, and not an ISO string. Out by a factor of a thousand
        # puts every message in 1970 or the far future, and age-based give-up
        # then either never fires or always does.
        expect(Envelope.millis(envelope.first_seen)).to eq(expected[Headers::FIRST_SEEN])
      end

      it "writes back what came in" do
        written = envelope.to_headers(fixture["routingKey"])
        expected.each do |name, value|
          expect(written).to include(name), "#{name} was not written back"
          expect(written[name]).to eq(value), "#{name} changed in the round trip"
        end
      end
    end
  end

  it "keeps the application's headers apart from ours" do
    envelope = Envelope.from_headers(
      { Headers::ID => "abc", Headers::ATTEMPT => 3, "tenant" => "acme",
        "x-acemq-something-newer" => "from a later version" },
      "orders.placed"
    )

    # What an application gets back is its own, and nothing that would be
    # written twice if it handed them to a message it publishes.
    expect(envelope.headers).to eq({ "tenant" => "acme" })
    expect(envelope.attempt).to eq(3)
  end

  it "refuses a reserved name in the application's headers" do
    expect { Envelope.new(headers: { "x-acemq-id" => "mine now" }) }
      .to raise_error(ArgumentError, /x-acemq-id/)
  end

  it "correlates a new message to itself" do
    # The point of a correlation id is that a chain shares one, so the message
    # that starts a chain gives every hop after it something to copy.
    envelope = Envelope.new(id: "the-first-one")
    expect(envelope.correlation_id).to eq("the-first-one")
    expect(envelope.to_headers[Headers::CORRELATION]).to eq("the-first-one")
  end

  it "falls back to the routing key for the type" do
    expect(Envelope.new.to_headers("order.placed")[Headers::TYPE]).to eq("order.placed")
    expect(Envelope.new(type: "order.placed.v2").to_headers("order.placed")[Headers::TYPE])
      .to eq("order.placed.v2")
  end

  it "leaves an empty value out rather than writing it empty" do
    written = Envelope.new(id: "i").to_headers("k")

    # A header carrying "" is a header somebody has to write a special case for.
    [Headers::CAUSATION, Headers::ORIGIN, Headers::ERROR, Headers::CLAIM].each do |name|
      expect(written).not_to include(name)
    end
  end

  it "does not lose a message whose header is the wrong type" do
    # A producer in another language sending the attempt as a string is wrong,
    # and turning its bug into our outage helps nobody.
    expect(Envelope.from_headers({ Headers::ID => "x", Headers::ATTEMPT => "4" }).attempt)
      .to eq(4)
    expect(Envelope.from_headers({ Headers::ID => "x", Headers::ATTEMPT => "soon" }).attempt)
      .to eq(1)
  end

  it "measures age from when the message was first published" do
    envelope = Envelope.new(first_seen: Time.now - 3600)

    # Attempts say nothing about how long a message has waited: a paused queue
    # produces messages on attempt one that are days old.
    expect(envelope.age).to be_within(60).of(3600)
  end

  it "cannot be changed underneath a log line" do
    envelope = Envelope.new(id: "one")
    later = envelope.with(attempt: 2)

    expect(envelope.attempt).to eq(1)
    expect(later.attempt).to eq(2)
    expect(later.id).to eq("one")
    expect(envelope).to be_frozen
  end
end
