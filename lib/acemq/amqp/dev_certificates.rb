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

require "fileutils"
require "openssl"

require_relative "security"

module AceMQ
  module AMQP
    # Generates a throwaway certificate authority and the certificates a local
    # broker and client need, so that running with TLS on a developer's machine
    # is one command rather than an afternoon with +openssl+.
    #
    #   AceMQ::AMQP::DevelopmentCertificates.generate(directory: "certs")
    #
    # or from a checkout:
    #
    #   ./scripts/acemq-certs.rb --out certs --broker localhost --days 30
    #
    # == The marker
    #
    # Everything produced here carries
    # <tt>ACEMQ DEVELOPMENT ONLY - DO NOT TRUST</tt> in its subject
    # organisation, and {Security} refuses a certificate carrying it however
    # trust is configured — unverified mode included — unless
    # {Security#allowing_development_certificates} says otherwise. That is the
    # point: a self-signed authority that drifts into production is *worse* than
    # no encryption, because everything looks protected and nothing is verified.
    # These fail closed instead.
    #
    # The marker goes in the organisation rather than the organisational unit,
    # which is where Go and .NET put it and where every tool that prints a
    # subject shows it. The Java library puts it in +OU+, so a Java-generated
    # certificate is caught here too — the check is on the whole subject and the
    # whole issuer — but a certificate generated *here* matches what
    # +scripts/tls-broker.sh+ greps for.
    #
    # == What it writes
    #
    # The six files the other libraries' generators write, under the same names,
    # so this generator and Go's +acemq-certs+ are interchangeable:
    #
    #   ca.crt      the authority, to trust
    #   ca.key      its key, so more certificates can be signed later
    #   server.crt  the broker's certificate
    #   server.key  the broker's key
    #   client.crt  a client certificate, for EXTERNAL authentication
    #   client.key  its key
    #
    # and, unless asked not to, a +rabbitmq.conf+ that serves TLS from them.
    #
    # Keys are written 0600. A development key is still a key, and one
    # world-readable in a repository checkout is a habit worth not forming.
    #
    # Certificates are short-lived by default. A development certificate that
    # never expires is one that outlives the reason it was created.
    #
    # == Elliptic curve, not RSA
    #
    # P-256, as the Go generator uses. RSA-4096 as the Java generator uses is
    # equally fine and takes several seconds per key in Ruby, which is a long
    # time to wait for something whose whole promise is that it is one command.
    module DevelopmentCertificates
      # Written into every subject, and refused by {Security} unless explicitly
      # allowed. The same string in all five languages.
      MARKER = Security::DEVELOPMENT_MARKER

      # The authority's common name, shared with Go and .NET.
      AUTHORITY_NAME = "AceMQ development CA"

      # The client certificate's common name, shared with Go and .NET.
      CLIENT_NAME = "acemq-client"

      # Short on purpose.
      DEFAULT_VALIDITY_DAYS = 30

      # Where rabbitmq.conf will look, which is where the certificates are
      # mounted inside the broker's container rather than where they are here.
      DEFAULT_BROKER_CERTIFICATE_DIRECTORY = "/certs"

      # The curve. P-256 is what Go's generator uses and what every broker and
      # every client in this decade supports.
      CURVE = "prime256v1"

      # What {generate} wrote.
      Result = Struct.new(:directory, :authority, :server, :client, :expiry, :files,
                          :broker_config, keyword_init: true)

      class << self
        # Writes a certificate authority, a broker certificate and a client
        # certificate.
        #
        # Existing files are overwritten. These are development certificates
        # with a short life, so regenerating them is the expected way to deal
        # with expiry rather than something to guard against — but regenerating
        # changes the authority, which invalidates anything already trusting it,
        # including a broker somebody is still pointing at.
        #
        # @param directory [String] where to write them; created if absent
        # @param broker_host [String] the name the broker's certificate is
        #   issued for, and the name a client must connect by for verification
        #   to succeed
        # @param days [Integer] how long the certificates last
        # @param broker_certificate_directory [String] the path rabbitmq.conf
        #   points at
        # @param broker_config [Boolean] whether to write rabbitmq.conf
        # @return [Result]
        def generate(directory: "certs", broker_host: "localhost",
                     days: DEFAULT_VALIDITY_DAYS,
                     broker_certificate_directory: DEFAULT_BROKER_CERTIFICATE_DIRECTORY,
                     broker_config: true)
          FileUtils.mkdir_p(directory)
          # Backdated an hour. A machine whose clock is a few minutes behind the
          # one that generated these would otherwise reject them as not yet
          # valid, which is a confusing way to spend a morning.
          life = ((Time.now - 3600)..(Time.now + (days * 24 * 60 * 60)))

          ca_key, authority = authority_pair(life)
          server_key, server = leaf_pair(broker_host, "serverAuth", ca_key, authority, life,
                                         alternative_names(broker_host))
          client_key, client = leaf_pair(CLIENT_NAME, "clientAuth", ca_key, authority, life,
                                         nil)

          written = write(directory,
                          "ca" => [authority, ca_key],
                          "server" => [server, server_key],
                          "client" => [client, client_key])
          config = File.join(directory, "rabbitmq.conf") if broker_config
          if config
            written << write_file(config, broker_configuration(broker_certificate_directory),
                                  0o644)
          end

          Result.new(directory: directory, authority: authority, server: server, client: client,
                     expiry: life.end, files: written, broker_config: config)
        end

        # A +rabbitmq.conf+ that serves TLS from what {generate} wrote.
        #
        # +verify_peer+ with +fail_if_no_peer_cert+ off means the broker will
        # use a client certificate if one is presented and will not insist on
        # it, which is what suits a development broker that is also reached by
        # password.
        #
        # @param certificate_directory [String] the path inside the broker
        # @return [String]
        def broker_configuration(certificate_directory = DEFAULT_BROKER_CERTIFICATE_DIRECTORY)
          <<~CONF
            # Written by AceMQ::AMQP::DevelopmentCertificates. Development only.
            listeners.ssl.default = 5671

            ssl_options.cacertfile = #{certificate_directory}/ca.crt
            ssl_options.certfile   = #{certificate_directory}/server.crt
            ssl_options.keyfile    = #{certificate_directory}/server.key
            ssl_options.verify     = verify_peer
            ssl_options.fail_if_no_peer_cert = false

            # The plaintext listener stays on so a laptop can use either.
            listeners.tcp.default = 5672
          CONF
        end

        private

        # pathlen:0 — this authority signs leaves and nothing that signs
        # anything else, so a leaked development key cannot mint an
        # intermediate and pass it off as part of the chain.
        def authority_pair(life)
          key = OpenSSL::PKey::EC.generate(CURVE)
          name = subject(AUTHORITY_NAME)
          certificate = shell(name, name, key, life)
          add_extensions(certificate, certificate,
                         "basicConstraints" => ["CA:TRUE,pathlen:0", true],
                         "keyUsage" => ["keyCertSign,cRLSign,digitalSignature", true],
                         "subjectKeyIdentifier" => ["hash", false])
          [key, certificate.sign(key, OpenSSL::Digest.new("SHA256"))]
        end

        def leaf_pair(name, usage, authority_key, authority, life, alternative_names)
          key = OpenSSL::PKey::EC.generate(CURVE)
          certificate = shell(subject(name), authority.subject, key, life)
          wanted = { "basicConstraints" => ["CA:FALSE", true],
                     "keyUsage" => ["digitalSignature,keyEncipherment", true],
                     "extendedKeyUsage" => [usage, false],
                     "subjectKeyIdentifier" => ["hash", false] }
          wanted["subjectAltName"] = [alternative_names, false] if alternative_names
          add_extensions(certificate, authority, wanted)
          [key, certificate.sign(authority_key, OpenSSL::Digest.new("SHA256"))]
        end

        def shell(subject, issuer, key, life)
          certificate = OpenSSL::X509::Certificate.new
          # Version 2 is X.509 v3, counted from zero. Anything less carries no
          # extensions, so no subject alternative name and no basic constraints.
          certificate.version = 2
          certificate.serial = OpenSSL::BN.rand(128)
          certificate.subject = subject
          certificate.issuer = issuer
          certificate.public_key = key
          certificate.not_before = life.begin
          certificate.not_after = life.end
          certificate
        end

        # The marker goes in the organisation, where it is visible in every tool
        # that prints a subject and where the library looks for it.
        def subject(common_name)
          OpenSSL::X509::Name.new([["O", MARKER], ["CN", common_name]])
        end

        def add_extensions(certificate, issuer, wanted)
          factory = OpenSSL::X509::ExtensionFactory.new
          factory.subject_certificate = certificate
          factory.issuer_certificate = issuer
          wanted.each do |name, (value, critical)|
            certificate.add_extension(factory.create_extension(name, value, critical))
          end
          certificate
        end

        # Hostname verification reads the subject alternative name and ignores
        # the common name entirely. Without these the certificate verifies as
        # valid and is still rejected for the host, which looks like a library
        # bug and is not.
        #
        # A broker reached as localhost is very often reached as 127.0.0.1 as
        # well, and a certificate covering only one of them fails in a way that
        # reads like a trust problem rather than a naming one.
        def alternative_names(broker_host)
          names = ["DNS:#{broker_host}"]
          names << "DNS:localhost" unless broker_host == "localhost"
          names << "DNS:rabbitmq" unless broker_host == "rabbitmq"
          (names + ["IP:127.0.0.1", "IP:::1"]).join(",")
        end

        # Keys are written 0600 and certificates 0644, which is the split every
        # one of these generators makes.
        def write(directory, pairs)
          pairs.flat_map do |name, (certificate, key)|
            [write_file(File.join(directory, "#{name}.crt"), certificate.to_pem, 0o644),
             write_file(File.join(directory, "#{name}.key"), key.to_pem, 0o600)]
          end
        end

        def write_file(path, contents, mode)
          File.write(path, contents)
          # Best effort: a filesystem without POSIX permissions simply skips it.
          begin
            File.chmod(mode, path)
          rescue StandardError
            nil
          end
          path
        end
      end
    end
  end
end
