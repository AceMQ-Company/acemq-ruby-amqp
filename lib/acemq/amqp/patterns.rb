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

require_relative "../amqp"
require_relative "patterns/idempotency"
require_relative "patterns/outbox"
require_relative "patterns/request_reply"
require_relative "patterns/replay"
require_relative "patterns/ordered"
require_relative "patterns/consumer_group"

module AceMQ
  module AMQP
    # The things everybody writes on top of a message queue, written once.
    #
    # None of this is protocol. A handler that refuses to do the same work
    # twice, a table of messages written in the transaction that decided to
    # send them, a reply that finds its way back to the caller who is waiting
    # for it: every service that needs one writes it again, subtly differently,
    # and the differences are where the bugs are.
    #
    # Required separately from +acemq/amqp+, which stays the contract and the
    # transport:
    #
    #   require "acemq/amqp/patterns"
    #
    # A pattern here composes with the ordinary API rather than replacing it. A
    # wrapped handler is still a handler and goes to {Connection#consume}
    # unchanged, which means the retry policy, the dead-lettering and the
    # envelope are all still whatever the caller configured — a pattern that
    # took over the consumer would have to reimplement them, and then there
    # would be two retry engines to keep in step.
    module Patterns
    end
  end
end
