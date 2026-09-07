# frozen_string_literal: true

require_relative "lib/acemq/amqp/version"

Gem::Specification.new do |spec|
  spec.name = "acemq-amqp"
  spec.version = AceMQ::AMQP::VERSION
  spec.authors = ["AceMQ"]

  spec.summary = "AceMQ messaging over AMQP, speaking the same wire contract as " \
                 "the Java, Go, .NET and Python libraries"
  spec.description = "A Ruby client for AceMQ messaging over AMQP: the same reserved " \
                     "headers, the same defaults and the same retry arithmetic as the " \
                     "other AceMQ libraries, so a Ruby consumer reads what a Java " \
                     "producer writes."
  spec.homepage = "https://acemq.org"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.1"

  spec.metadata = {
    "source_code_uri" => "https://github.com/AceMQ-Company/acemq-ruby-amqp",
    "bug_tracker_uri" => "https://github.com/AceMQ-Company/acemq-ruby-amqp/issues",
    "rubygems_mfa_required" => "true"
  }

  spec.files = Dir["lib/**/*.rb", "LICENSE", "README.md"]
  spec.require_paths = ["lib"]

  # The contract layer has no runtime dependencies at all. Reading an AceMQ
  # envelope should not require installing a broker client, so bunny arrives
  # with the transport rather than as a condition of using any of this.
end
