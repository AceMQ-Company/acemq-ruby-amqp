#!/usr/bin/env ruby
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

# Development certificates, from the Ruby library's own generator.
#
# The same six files under the same names as Go's `acemq-certs` and .NET's
# `AceMq.Amqp.DevCerts`, so ../scripts/tls-broker.sh can call this instead:
#
#   ./scripts/acemq-certs.rb --out .tls --broker localhost --days 90
#
# A script rather than a gem executable, deliberately. Installing this gem
# should not put a certificate-writing command on somebody's PATH, and the
# people who want one have the repository checked out.

require "optparse"

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "acemq/amqp"

MARKER = AceMQ::AMQP::DevelopmentCertificates::MARKER

options = {
  directory: "certs",
  broker_host: "localhost",
  days: AceMQ::AMQP::DevelopmentCertificates::DEFAULT_VALIDITY_DAYS,
  broker_certificate_directory:
    AceMQ::AMQP::DevelopmentCertificates::DEFAULT_BROKER_CERTIFICATE_DIRECTORY,
  broker_config: true
}

parser = OptionParser.new do |parse|
  parse.banner = "acemq-certs — development certificates for talking to a broker\n\n" \
                 "Usage: ./scripts/acemq-certs.rb [options]\n\n"
  parse.on("--out DIR", "where to write them (default: certs)") { |v| options[:directory] = v }
  parse.on("--broker HOST", "the name the server certificate names (default: localhost)") do |v|
    options[:broker_host] = v
  end
  parse.on("--days N", Integer, "how long they are valid (default: 30)") do |v|
    options[:days] = v
  end
  parse.on("--broker-certs DIR", "the path rabbitmq.conf points at (default: /certs)") do |v|
    options[:broker_certificate_directory] = v
  end
  parse.on("--no-broker-config", "do not write rabbitmq.conf") do
    options[:broker_config] = false
  end
  parse.on("-h", "--help", "this") do
    warn(parse.help)
    warn("\nEverything written is stamped #{MARKER.inspect} and the library\n" \
         "refuses it unless Security#allowing_development_certificates is called.")
    exit 0
  end
end
parser.parse!(ARGV)

begin
  result = AceMQ::AMQP::DevelopmentCertificates.generate(**options)
rescue StandardError => e
  warn("could not write the certificates: #{e.message}")
  exit 1
end

puts "wrote development certificates to #{result.directory}"
puts "  ca.crt       trust this: Security.verified(certificate_authority: \"ca.crt\")"
puts "  server.crt   the broker's certificate"
puts "  server.key   the broker's key"
puts "  client.crt   for EXTERNAL authentication, with client.key"
puts "  rabbitmq.conf mount at /etc/rabbitmq/rabbitmq.conf" if result.broker_config
puts
puts "valid until #{result.expiry.strftime("%Y-%m-%d")}. Stamped " \
     "#{MARKER.inspect},"
puts "so the library refuses them unless you call allowing_development_certificates."
