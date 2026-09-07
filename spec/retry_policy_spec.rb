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

RetryPolicy = AceMQ::AMQP::RetryPolicy
Naming = AceMQ::AMQP::Naming

RSpec.describe AceMQ::AMQP::RetryPolicy do
  it "doubles, and produces the same numbers as the other languages" do
    # A message retried by a Ruby consumer and then by a Go one must not wait
    # different amounts for the same attempt.
    expect(RetryPolicy.exponential(5, 1, 60).schedule).to eq([1.0, 2.0, 4.0, 8.0])
  end

  it "holds the ceiling" do
    expect(RetryPolicy.exponential(6, 1, 4).schedule).to eq([1.0, 2.0, 4.0, 4.0, 4.0])
  end

  it "gives one delivery when there is no retry" do
    expect(RetryPolicy.none.schedule).to eq([])
    expect(RetryPolicy.none.next_delay(1)).to be_nil
  end

  it "waits the same every time when fixed" do
    expect(RetryPolicy.fixed(4, 30).schedule).to eq([30.0, 30.0, 30.0])
  end

  it "has no next delay after the last attempt" do
    policy = RetryPolicy.exponential(3, 1)

    expect(policy.next_delay(1)).not_to be_nil
    expect(policy.next_delay(2)).not_to be_nil
    # The third delivery is the last. Asking for a fourth is how a message is
    # retried for ever by a library that counts wrong.
    expect(policy.next_delay(3)).to be_nil
  end

  it "gives up on an old message however few attempts it has had" do
    policy = RetryPolicy.exponential(10, 1).give_up_after(3600)

    expect(policy.next_delay(1, 3540)).not_to be_nil
    # One attempt, four hours old: a paused queue produces exactly this, and
    # delivering it now helps nobody.
    expect(policy.next_delay(1, 4 * 3600)).to be_nil
  end

  it "jitters both ways, inside the factor" do
    policy = RetryPolicy.exponential(2, 10)
    delays = Array.new(200) { policy.next_delay(1) }

    expect(delays).to all(be_between(8.0, 12.0))
    # Genuinely either side: jitter that only ever delays turns a thundering
    # herd into a slower thundering herd.
    expect(delays.any? { |d| d < 10 }).to be true
    expect(delays.any? { |d| d > 10 }).to be true
  end

  it "never returns a negative delay" do
    reckless = RetryPolicy.new(max_attempts: 2, initial_delay: 1, jitter_factor: 5.0)
    expect(Array.new(200) { reckless.next_delay(1) }).to all(be >= 0)
  end
end

RSpec.describe AceMQ::AMQP::Naming do
  it "sends a message that cannot be handled somewhere predictable" do
    # Convention rather than protocol, which is why it must be identical
    # everywhere: an operator looking for the dead letters of orders.new should
    # not have to know which language gave up on them.
    expect(Naming.dead_letter_queue("orders.new")).to eq("orders.new.dlq")
    expect(Naming.parked_queue("orders.new")).to eq("orders.new.parked")
  end

  it "names a retry queue for its delay" do
    # The wait is fixed at declaration by x-message-ttl, so a policy with four
    # different waits needs four queues, and the name tells them apart.
    expect(Naming.retry_queue("orders.new", 30)).to eq("orders.new.retry.30s")
    expect(Naming.retry_queue("orders.new", 300)).to eq("orders.new.retry.5m")
    expect(Naming.retry_queue("orders.new", 7200)).to eq("orders.new.retry.2h")
    expect(Naming.retry_queue("orders.new", 90)).to eq("orders.new.retry.90s")
  end
end

RSpec.describe AceMQ::AMQP::Ack do
  it "says what it is" do
    expect(AceMQ::AMQP::Ack.accept).to be_accept
    expect(AceMQ::AMQP::Ack.retry.to_s).to eq("retry")
    expect(AceMQ::AMQP::Ack.reject(StandardError.new("no")).error.message).to eq("no")
  end
end
