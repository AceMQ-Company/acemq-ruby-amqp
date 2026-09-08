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

RSpec.describe AceMQ::AMQP::QueueType do
  def resolve(name = "orders.new", **options)
    described_class.resolve(name: name, **options)
  end

  it "makes a durable, named queue a quorum queue" do
    # The default in Java, which is the library with deployments, and therefore
    # the default in this one: the two have to declare orders.new the same way
    # or the second service to start cannot consume it.
    expect(resolve).to eq([:quorum, { "x-queue-type" => "quorum" }])
  end

  it "sends no x-queue-type at all for a classic queue" do
    # Not "x-queue-type" => "classic". Java sends the argument only for quorum
    # and stream, and an argument table that differs from Java's is the
    # PRECONDITION_FAILED this is all about.
    expect(resolve(requested: :classic)).to eq([:classic, {}])
  end

  it "keeps a caller's own arguments alongside the kind" do
    type, arguments = resolve(arguments: { "x-max-length" => 1000 })

    expect(type).to eq(:quorum)
    expect(arguments).to eq("x-max-length" => 1000, "x-queue-type" => "quorum")
  end

  it "reads the kind out of the arguments when that is where it was said" do
    # How Patterns.declare_stream declares a stream, and it must not be
    # overruled by a default that knows nothing about it.
    expect(resolve(arguments: { "x-queue-type" => "stream" }))
      .to eq([:stream, { "x-queue-type" => "stream" }])
  end

  it "falls back to classic for anything the broker would not replicate" do
    flags = [{ exclusive: true, durable: false }, { auto_delete: true }, { durable: false }]
    flags.each { |flag| expect(resolve(**flag).first).to eq(:classic) }
  end

  it "refuses a quorum queue that cannot be one, rather than letting the broker refuse it" do
    # The broker's own refusal for this is "invalid property 'exclusive-owner'",
    # which does not mention quorum queues and reads like a bug in the caller.
    expect { resolve(requested: :quorum, exclusive: true) }
      .to raise_error(AceMQ::AMQP::QueueTypeError, /quorum queue while it is exclusive/)
    expect { resolve(requested: :stream, auto_delete: true) }
      .to raise_error(AceMQ::AMQP::QueueTypeError, /stream queue while it is auto-delete/)
  end

  it "refuses a kind said twice and said differently" do
    # Either answer would be a guess at which of the two was meant.
    expect do
      resolve(requested: :quorum, arguments: { "x-queue-type" => "classic" })
    end.to raise_error(AceMQ::AMQP::QueueTypeError, /pick one/)
  end

  it "accepts a kind said twice and said the same" do
    expect(resolve(requested: :stream, arguments: { "x-queue-type" => "stream" }))
      .to eq([:stream, { "x-queue-type" => "stream" }])
  end

  it "refuses a kind RabbitMQ has never heard of" do
    expect { resolve(requested: :lazy) }
      .to raise_error(AceMQ::AMQP::QueueTypeError, /unknown queue type/)
  end

  it "leaves the caller's argument table alone" do
    # A topology holds its arguments and prints them; resolving twice, or
    # printing after applying, must not find them changed underneath.
    arguments = { "x-max-length" => 10 }
    resolve(arguments: arguments)

    expect(arguments).to eq("x-max-length" => 10)
  end
end
