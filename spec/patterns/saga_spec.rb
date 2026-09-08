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

RSpec.describe "a saga" do
  Saga = AceMQ::AMQP::Patterns::Saga

  # Everything a step does is recorded here, in the order it happened, which is
  # the only thing worth asserting about a saga: the order the world was
  # changed in and the order it was changed back.
  let(:log) { [] }

  # take-payment and reserve-stock can be undone; book-courier is the last step
  # and needs no compensation, which is the shape the pattern is for.
  def booking(fail_at: nil, unfixable: [])
    Saga.named("place-order") do |saga|
      %w[take-payment reserve-stock book-courier].each do |name|
        saga.step(name) do |order|
          raise "the #{name} service said no" if name == fail_at

          log << "did #{name} to #{order}"
        end
        next if name == "book-courier"

        saga.compensate_with do |order|
          raise "the #{name} service will not undo it" if unfixable.include?(name)

          log << "undid #{name} to #{order}"
        end
      end
    end
  end

  describe "when every step works" do
    it "runs them in order and says so" do
      result = booking.run("A-1")

      expect(log).to eq(["did take-payment to A-1", "did reserve-stock to A-1",
                         "did book-courier to A-1"])
      expect(result).to be_complete
      expect(result).not_to be_compensated
      expect(result.completed).to eq(%w[take-payment reserve-stock book-courier])
      expect(result.failed_at).to be_nil
      expect(result.failure).to be_nil
      expect(result.unresolved).to be_empty
      expect(result).not_to be_unresolved
    end
  end

  describe "when a step fails" do
    it "compensates the completed steps in reverse order" do
      result = booking(fail_at: "book-courier").run("A-2")

      expect(log).to eq(["did take-payment to A-2", "did reserve-stock to A-2",
                         "undid reserve-stock to A-2", "undid take-payment to A-2"])
      expect(result).to be_compensated
      expect(result).not_to be_complete
      expect(result.failed_at).to eq("book-courier")
      expect(result.completed).to eq(%w[take-payment reserve-stock])
    end

    it "carries the failure rather than raising it" do
      result = booking(fail_at: "reserve-stock").run("A-3")

      expect(result.failure).to be_a(RuntimeError)
      expect(result.failure.message).to eq("the reserve-stock service said no")
      expect(result.failed_at).to eq("reserve-stock")
      expect(result.completed).to eq(["take-payment"])
    end

    it "does not run the steps after the one that failed" do
      booking(fail_at: "reserve-stock").run("A-4")

      expect(log).not_to include("did book-courier to A-4")
    end

    it "skips a completed step that has no compensation" do
      # book-courier completed and has nothing to undo. That is legitimate --
      # a step that only read something needs no compensation -- so it is
      # passed over rather than treated as a failure to compensate.
      saga = Saga.named("read-then-write") do |s|
        s.step("look-it-up") { log << "looked" }
        s.step("write-it-down") { raise "the disk is full" }
      end
      result = saga.run(nil)

      expect(result.completed).to eq(["look-it-up"])
      expect(result.unresolved).to be_empty
      expect(result).not_to be_unresolved
    end
  end

  describe "when a compensation itself fails" do
    it "still runs the remaining compensations" do
      result = booking(fail_at: "book-courier", unfixable: ["reserve-stock"]).run("A-5")

      # reserve-stock refused, and take-payment was undone anyway. Stopping at
      # the first refusal would have left the customer's money as well as their
      # stock.
      expect(log).to eq(["did take-payment to A-5", "did reserve-stock to A-5",
                         "undid take-payment to A-5"])
      expect(result.unresolved).to eq(["reserve-stock"])
      expect(result).to be_unresolved
    end

    it "collects every one that failed, in the order they were attempted" do
      result = booking(fail_at: "book-courier",
                       unfixable: %w[take-payment reserve-stock]).run("A-6")

      expect(log).to eq(["did take-payment to A-6", "did reserve-stock to A-6"])
      expect(result.unresolved).to eq(%w[reserve-stock take-payment])
      expect(result).to be_unresolved
      expect(result.completed).to eq(%w[take-payment reserve-stock])
    end

    it "is the difference between a tidy failure and one somebody has to fix" do
      tidy = booking(fail_at: "book-courier").run("A-7")

      expect(tidy).to be_compensated
      expect(tidy).not_to be_unresolved
    end
  end

  describe "the result" do
    it "is frozen, because it describes something that already happened" do
      result = booking.run("A-8")

      expect(result).to be_frozen
      expect(result.completed).to be_frozen
      expect(result.unresolved).to be_frozen
      expect { result.completed << "invented" }.to raise_error(FrozenError)
    end

    it "reads as what happened" do
      expect(booking.run("A-9").to_s)
        .to eq("SagaResult{place-order completed: take-payment -> reserve-stock -> " \
               "book-courier}")
      expect(booking(fail_at: "book-courier", unfixable: ["take-payment"]).run("A-10").to_s)
        .to eq("SagaResult{place-order failed at book-courier, compensated take-payment, " \
               "reserve-stock, UNRESOLVED take-payment}")
    end
  end

  describe "building one" do
    it "names its steps in order" do
      expect(booking.step_names).to eq(%w[take-payment reserve-stock book-courier])
      expect(booking.to_s)
        .to eq("Saga{place-order: take-payment -> reserve-stock -> book-courier}")
    end

    it "is frozen once built, so a saga cannot grow a step while it is running" do
      expect(booking).to be_frozen
    end

    it "hands back the builder when no block is given" do
      builder = Saga.named("two-step")
      builder.step("one") { log << "one" }.compensate_with { log << "undo one" }
      saga = builder.build

      expect(saga.step_names).to eq(["one"])
    end

    it "refuses two steps with the same name" do
      expect do
        Saga.named("ambiguous") do |saga|
          saga.step("charge") { nil }
          saga.step("charge") { nil }
        end
      end.to raise_error(ArgumentError, /already has a step called "charge"/)
    end

    it "refuses a compensation before there is anything to compensate" do
      expect { Saga.named("early") { |saga| saga.compensate_with { nil } } }
        .to raise_error(ArgumentError, /no step to compensate yet/)
    end

    it "refuses a saga with no steps" do
      expect { Saga.named("empty") { |_saga| nil } }
        .to raise_error(ArgumentError, /has no steps/)
    end

    it "refuses a step with no work in it" do
      expect { Saga.named("thoughtless") { |saga| saga.step("do-something") } }
        .to raise_error(ArgumentError, /needs a block to do the work/)
    end

    it "refuses a saga with no name" do
      expect { Saga.named("") { |saga| saga.step("one") { nil } } }
        .to raise_error(ArgumentError, /needs a name/)
    end
  end

  describe "what it does not catch" do
    it "lets anything that is not a StandardError through, uncompensated" do
      # A SignalException or an Interrupt means the process is going away. A
      # compensation running on the way out of one is a compensation nobody can
      # be sure finished, which is worse than one that plainly did not run.
      saga = Saga.named("interrupted") do |s|
        s.step("charge") { log << "charged" }
        s.compensate_with { log << "refunded" }
        s.step("ship") { raise Interrupt }
      end

      expect { saga.run(nil) }.to raise_error(Interrupt)
      expect(log).to eq(["charged"])
    end
  end
end
