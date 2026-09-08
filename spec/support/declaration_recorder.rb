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

# What a topology or a retry ladder would do to a broker, written down.
#
# FakeTransport records declarations as the argument hashes they arrived in,
# which is the right shape for asking "was this call made". The conformance
# fixture asks a different question — "what would the broker end up holding" —
# so this normalises instead: one entry per name, the queue kind resolved the
# way a connection resolves it, and +x-queue-type+ taken back out of the
# argument table because the fixture keeps the kind in a field of its own.
class DeclarationRecorder
  Exchange = Struct.new(:name, :type, :durable, keyword_init: true)
  Queue = Struct.new(:name, :type, :durable, :arguments, keyword_init: true)
  Binding = Struct.new(:queue, :exchange, :routing_key, keyword_init: true)

  attr_reader :exchanges, :queues, :bindings

  def initialize
    @exchanges = {}
    @queues = {}
    @bindings = []
  end

  def declare_exchange(name, kind: "direct", durable: true, **_rest)
    @exchanges[name] = Exchange.new(name: name, type: kind.to_s, durable: durable)
  end

  # The kind is resolved here rather than believed, because the two halves of a
  # topology arrive by different routes: {AceMQ::AMQP::Topology} has already
  # settled +x-queue-type+ before it applies anything, and
  # {AceMQ::AMQP::RetryLadder} declares straight through a transport with only
  # the +queue_type+ keyword to go on. Resolving both the same way is what makes
  # the two halves comparable.
  def declare_queue(name, queue_type: nil, durable: true, auto_delete: false,
                    exclusive: false, arguments: {})
    type, table = AceMQ::AMQP::QueueType.resolve(
      name: name, requested: queue_type, durable: durable, exclusive: exclusive,
      auto_delete: auto_delete, arguments: arguments
    )
    @queues[name] = Queue.new(
      name: name, type: type.to_s, durable: durable,
      arguments: table.reject { |key, _| key.to_s == AceMQ::AMQP::QueueType::ARGUMENT }
    )
  end

  def bind(queue:, exchange:, routing_key: "")
    @bindings << Binding.new(queue: queue, exchange: exchange, routing_key: routing_key)
  end

  # Whether this recorder saw a binding of exactly that shape.
  def bound?(queue, exchange, routing_key)
    @bindings.any? do |binding|
      binding.queue == queue && binding.exchange == exchange &&
        binding.routing_key == routing_key
    end
  end

  # Every name this recorder was asked to declare, exchanges and queues alike.
  def names = @exchanges.keys + @queues.keys
end
