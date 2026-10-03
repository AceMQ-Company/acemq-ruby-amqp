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

# What bunny is allowed to replay when a connection comes back.
#
# bunny 3.x records every declaration for replay on recovery, against the channel it
# was made on. This library declares queues, exchanges and bindings on a short-lived
# channel of their own and closes it — deliberately, because a refused declaration
# kills the channel it was made on. Recovery then tried to replay those declarations
# on a channel closed minutes earlier, and logged one of these per entity, per
# recovery:
#
#   Caught an exception while recovering exchange acemq.dlx:
#     #<Bunny::ChannelAlreadyClosed: cannot use a closed channel! Channel id: 1>
#
# Measured against a three-node cluster with every connection forced shut every 15s:
# six such errors per recovery, every recovery, for ever.
RSpec.describe AceMQ::AMQP::RecoverLiveChannelsOnly do
  subject(:filter) { described_class.new }

  # Stands in for a Bunny::RecordedExchange and friends, which answer `channel`.
  def entity(open:) = Struct.new(:channel).new(Struct.new(:open?).new(open))

  let(:live) { entity(open: true) }
  let(:gone) { entity(open: false) }

  it "replays what was declared on a channel that is still open" do
    expect(filter.filter_exchanges([live])).to eq([live])
    expect(filter.filter_queues([live])).to eq([live])
  end

  it "drops what was declared on a channel this library has closed" do
    expect(filter.filter_exchanges([live, gone])).to eq([live])
    expect(filter.filter_queues([live, gone])).to eq([live])
  end

  it "filters bindings the same way, and takes a Set" do
    expect(filter.filter_queue_bindings(Set[live, gone])).to eq([live])
    expect(filter.filter_exchange_bindings(Set[live, gone])).to eq([live])
  end

  # The one that must never be filtered, and filtering it was far worse than the bug
  # this class fixes. A consumer's channel can read as closed at the moment the filter
  # runs, so asking the same question of it dropped the subscription from recovery: the
  # first version of this took a standing load's thread count from 52 to 4 and stopped
  # the client consuming, deliveries frozen at 715 while publishing climbed past
  # 17,000. A library that silently stops consuming is the failure this one exists to
  # prevent.
  it "never filters consumers, whatever their channel says" do
    consumers = [live, gone]

    expect(filter.filter_consumers(consumers)).to eq(consumers)
  end

  # A filter is not the place to decide that an entity bunny recorded is unrecoverable
  # for a reason it does not understand.
  it "keeps anything it cannot ask" do
    bare = Object.new
    no_channel = Struct.new(:channel).new(nil)
    odd = Struct.new(:channel).new(Object.new)

    expect(filter.filter_queues([bare, no_channel, odd])).to eq([bare, no_channel, odd])
  end
end
