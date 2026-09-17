# frozen_string_literal: true

# A broker that takes messages and answers for them only when it is told to.
#
# {FakeTransport} and {LoopbackTransport} confirm a publish as they are called,
# which cannot tell a pipelined batch from a loop of single sends: both pass.
# This one hands nothing back until a test says so, one message at a time, so
# the only way to get every message across is to publish every message before
# waiting for any of them — and the round trip per message that
# {AceMQ::AMQP::Connection#publish_all} exists to avoid cannot come back
# unnoticed.
#
# Everything that is not publishing raises. This exists to hold confirms open,
# and a test that reached one of the other methods would be testing something
# else.
class HeldConfirmsTransport
  # How long a test waits before deciding a message is never coming. Long
  # enough that a loaded machine does not fail the suite, short enough that a
  # regression fails it rather than hanging it.
  PATIENCE = 5

  def initialize
    @lock = Mutex.new
    @sent = []
    @held = []
    @closed = false
  end

  # Takes the message and waits for this test to answer for it, which is what a
  # publisher waiting on its own confirm does. Raises what it was answered
  # with, because that is the single-publish contract: one message, one answer,
  # and a failure comes out as an exception.
  def publish(**message)
    answer = await(accept(message), message)
    raise answer if answer.is_a?(StandardError)

    answer
  end

  # Takes every message first and only then waits for the answers, in the order
  # the messages were given whatever order they are answered in.
  def publish_all(messages)
    indexes = messages.map { |message| accept(message) }
    indexes.each_with_index.map { |index, position| await(index, messages[position]) }
  end

  # What has been handed over so far, in the order it arrived.
  def sent = @lock.synchronize { @sent.dup }

  def bodies = sent.map(&:body)

  # Waits for +count+ messages to reach this broker. The assertion, in the
  # pipelining test: nothing has been answered, so a publisher that waited for
  # each confirm in turn would still be sitting on its first message and this
  # would time out.
  def wait_until_sent(count, timeout: PATIENCE)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until sent.size >= count
      if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        raise "only #{sent.size} of #{count} messages reached the broker within #{timeout}s"
      end

      sleep(0.005)
    end
    nil
  end

  # Answers the nth message the broker was given.
  def confirm(index) = answer(index, :ok)

  # Answers it the way a broker that would not take the message does.
  def refuse(index, reason = "the broker would not confirm it")
    answer(index, AceMQ::AMQP::PublishError.new(reason))
  end

  # Answers it the way a mandatory publish that reached no queue does.
  def unroutable(index, reason = "312 NO_ROUTE")
    answer(index, AceMQ::AMQP::PublishError.new(reason, unroutable: true))
  end

  def open? = !@closed
  def close = @closed = true
  def queue_exists?(_name) = true

  def subscribe(*, **, &) = raise(publishing_only)
  def pull(*) = raise(publishing_only)
  def declare_queue(*, **) = raise(publishing_only)
  def declare_exchange(*, **) = raise(publishing_only)
  def bind(**) = raise(publishing_only)
  def message_count(_queue) = raise(publishing_only)
  def delete_queue(_name) = raise(publishing_only)

  private

  def publishing_only = NotImplementedError.new("this broker only takes publishes")

  def accept(message)
    @lock.synchronize do
      @sent << FakeTransport::Published.new(
        exchange: message[:exchange], routing_key: message[:routing_key],
        body: message[:body], content_type: message[:content_type],
        message_id: message[:message_id], headers: message[:headers],
        reply_to: message[:reply_to], mandatory: message[:mandatory]
      )
      @held << Thread::Queue.new
      @held.size - 1
    end
  end

  def await(index, message)
    answer = @lock.synchronize { @held[index] }.pop
    answer == :ok ? message[:message_id] : answer
  end

  def answer(index, result)
    held = @lock.synchronize { @held[index] }
    raise "nothing has been published at #{index} yet" if held.nil?

    held.push(result)
    nil
  end
end
