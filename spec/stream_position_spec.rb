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

# Where a recovered stream subscription carries on from. The broker-level proof is
# spec/integration/stream_recovery_spec.rb; this is the arithmetic.
RSpec.describe AceMQ::AMQP::StreamPosition do
  subject(:position) { described_class.new }

  def deliver(offset)
    position.track(AceMQ::AMQP::Delivery.new(headers: { "x-stream-offset" => offset }))
  end

  it "keeps the subscription's own start when nothing was delivered" do
    expect(position.resume).to be_nil
  end

  it "resumes one past the newest settled offset" do
    [0, 1, 2].each { |offset| deliver(offset).ack }

    expect(position.resume).to eq(3)
  end

  it "resumes at the oldest offset still unsettled" do
    deliveries = [5, 6, 7].map { |offset| deliver(offset) }
    deliveries[0].nack
    deliveries[2].ack

    expect(position.resume).to eq(6)
  end

  it "gives the same answer to every attempt of one recovery" do
    deliver(4)

    expect([position.resume, position.resume]).to eq([4, 4])
  end

  it "does not let an old copy settled after the recovery move it forward" do
    old = deliver(9)
    deliver(5)
    position.resume
    old.ack

    expect(position.resume).to eq(5)
  end

  it "settles and still hands the acknowledgement on" do
    acked = false
    delivery = position.track(AceMQ::AMQP::Delivery.new(
                                headers: { "x-stream-offset" => 1 }, on_ack: -> { acked = true }
                              ))
    delivery.ack

    expect([acked, position.resume]).to eq([true, 2])
  end
end
