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

require_relative "amqp/version"
require_relative "amqp/headers"
require_relative "amqp/envelope"
require_relative "amqp/retry_policy"
require_relative "amqp/naming"
require_relative "amqp/ack"

# AceMQ for Ruby.
#
# A client for AceMQ messaging over AMQP, speaking the same wire contract as
# the Java, Go, .NET and Python libraries: the same reserved headers, the same
# defaults, the same retry arithmetic. A Ruby consumer reads what a Java
# producer writes, and the fixtures the Java implementation produces pin that
# rather than leaving it to be discovered in production.
#
# The API shape is Ruby's, deliberately. The contract is portable; the
# ergonomics are native.
