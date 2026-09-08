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
require "stringio"
require "tmpdir"

require "acemq/amqp"

# Two claims are being made here and they pull in opposite directions: these
# certificates have to work — a broker must accept them and a client must verify
# against them — and they have to be impossible to use by accident anywhere that
# matters. The first half is asserted by verifying the chain; the second half by
# {AceMQ::AMQP::Security} refusing them, which is the other file's job and is
# checked here too because the two only mean anything together.
RSpec.describe AceMQ::AMQP::DevelopmentCertificates do
  # One generation, read by every example. Two elliptic-curve keys and a
  # signature each is not slow, but it is not free either, and nothing below
  # changes what was written.
  DEV_CERTS_DIR = Dir.mktmpdir("acemq-dev-certs")
  DEV_CERTS = described_class.generate(directory: DEV_CERTS_DIR, broker_host: "localhost",
                                       days: 30)

  after(:context) { FileUtils.remove_entry(DEV_CERTS_DIR) }

  describe "the files" do
    # The names the Go and .NET generators write, so tls-broker.sh can call
    # either. A generator that produced the same certificates under different
    # names would be a generator nothing else can be pointed at.
    it "writes the six the other generators write, plus a broker configuration" do
      expect(DEV_CERTS.files.map { |path| File.basename(path) })
        .to contain_exactly("ca.crt", "ca.key", "server.crt", "server.key", "client.crt",
                            "client.key", "rabbitmq.conf")
    end

    it "leaves the private keys readable only by their owner" do
      %w[ca.key server.key client.key].each do |name|
        mode = File.stat(File.join(DEV_CERTS_DIR, name)).mode & 0o777
        expect(mode).to eq(0o600), "#{name} is #{format("%o", mode)}"
      end
    end

    it "writes a rabbitmq.conf that serves TLS from what it wrote" do
      conf = File.read(File.join(DEV_CERTS_DIR, "rabbitmq.conf"))

      expect(conf).to include("listeners.ssl.default = 5671")
      expect(conf).to include("ssl_options.cacertfile = /certs/ca.crt")
      expect(conf).to include("ssl_options.certfile   = /certs/server.crt")
      expect(conf).to include("ssl_options.keyfile    = /certs/server.key")
      # The plaintext listener stays on so a laptop can use either.
      expect(conf).to include("listeners.tcp.default = 5672")
    end

    it "leaves rabbitmq.conf out when asked to" do
      Dir.mktmpdir do |directory|
        result = described_class.generate(directory: directory, broker_config: false)

        expect(result.broker_config).to be_nil
        expect(File.exist?(File.join(directory, "rabbitmq.conf"))).to be(false)
      end
    end
  end

  describe "the marker" do
    it "is the same string the other four libraries look for" do
      expect(described_class::MARKER).to eq("ACEMQ DEVELOPMENT ONLY - DO NOT TRUST")
      expect(described_class::MARKER).to eq(AceMQ::AMQP::Security::DEVELOPMENT_MARKER)
    end

    # In the organisation, which is where Go and .NET put it, where every tool
    # printing a subject shows it, and where scripts/tls-broker.sh greps for it.
    it "is in the subject organisation of all three certificates" do
      [DEV_CERTS.authority, DEV_CERTS.server, DEV_CERTS.client].each do |certificate|
        expect(certificate.subject.to_a.assoc("O")[1]).to eq(described_class::MARKER)
      end
    end

    it "is in the issuer of the leaves as well, so a leaf alone still shows it" do
      [DEV_CERTS.server, DEV_CERTS.client].each do |certificate|
        expect(certificate.issuer.to_s).to include(described_class::MARKER)
      end
    end
  end

  describe "the certificates themselves" do
    it "signs the broker and the client with the authority it generated" do
      store = OpenSSL::X509::Store.new
      store.add_cert(DEV_CERTS.authority)

      expect(store.verify(DEV_CERTS.server)).to be(true)
      expect(store.verify(DEV_CERTS.client)).to be(true)
    end

    it "makes the authority an authority, and nothing else one" do
      expect(extension(DEV_CERTS.authority, "basicConstraints")).to eq("CA:TRUE, pathlen:0")
      expect(extension(DEV_CERTS.server, "basicConstraints")).to eq("CA:FALSE")
    end

    # Hostname verification reads the subject alternative name and ignores the
    # common name entirely, so a certificate without these verifies as valid and
    # is still refused for the host — which looks like a library bug and is not.
    it "names localhost and the loopback addresses on the broker certificate" do
      names = extension(DEV_CERTS.server, "subjectAltName")

      expect(names).to include("DNS:localhost")
      expect(names).to include("IP Address:127.0.0.1")
    end

    it "gives the broker server authentication and the client client authentication" do
      expect(extension(DEV_CERTS.server, "extendedKeyUsage"))
        .to eq("TLS Web Server Authentication")
      expect(extension(DEV_CERTS.client, "extendedKeyUsage"))
        .to eq("TLS Web Client Authentication")
    end

    it "names the broker host it was asked for" do
      Dir.mktmpdir do |directory|
        result = described_class.generate(directory: directory, broker_host: "rabbit.internal",
                                          broker_config: false)

        expect(result.server.subject.to_a.assoc("CN")[1]).to eq("rabbit.internal")
        expect(extension(result.server, "subjectAltName")).to include("DNS:rabbit.internal")
      end
    end

    # Short on purpose. A development certificate that never expires is one that
    # outlives the reason it was created.
    it "expires soon, and is valid a little before now so a slow clock still works" do
      expect(DEV_CERTS.server.not_after).to be_within(120).of(Time.now + (30 * 24 * 60 * 60))
      expect(DEV_CERTS.server.not_before).to be < Time.now
    end

    it "draws a different serial number for each" do
      serials = [DEV_CERTS.authority, DEV_CERTS.server, DEV_CERTS.client].map(&:serial)
      expect(serials.uniq.size).to eq(3)
    end
  end

  # The half that stops one of these reaching production. Without it the
  # generator is a convenience that quietly makes a security hole cheaper to dig.
  describe "what the library does with one" do
    let(:authority) { File.join(DEV_CERTS_DIR, "ca.crt") }
    let(:certificate) { File.join(DEV_CERTS_DIR, "client.crt") }
    let(:key) { File.join(DEV_CERTS_DIR, "client.key") }
    let(:login) { AceMQ::AMQP::Credentials.of(username: "u", password: "p") }

    it "recognises one by its subject or its issuer" do
      expect(AceMQ::AMQP::Security.development_certificate?(DEV_CERTS.authority)).to be(true)
      expect(AceMQ::AMQP::Security.development_certificate?(DEV_CERTS.server)).to be(true)
      expect(AceMQ::AMQP::Security.development_certificate?(unmarked)).to be(false)
      expect(AceMQ::AMQP::Security.development_certificate?(nil)).to be(false)
    end

    it "refuses to trust the authority when a connection is being made" do
      security = AceMQ::AMQP::Security.verified(certificate_authority: authority)

      expect { security.to_transport_options }
        .to raise_error(AceMQ::AMQP::ConfigurationError, /DEVELOPMENT ONLY/)
    end

    it "refuses a client certificate carrying it too" do
      security = AceMQ::AMQP::Security.verified(certificate: certificate, key: key)

      expect { security.to_transport_options }
        .to raise_error(AceMQ::AMQP::ConfigurationError, /client certificate/)
    end

    it "refuses it in unverified mode, where it would otherwise be likeliest to slip through" do
      security = AceMQ::AMQP::Security.without_verifying_the_broker(
        because: "a spec", certificate: certificate, key: key
      )

      expect { security.to_transport_options }
        .to raise_error(AceMQ::AMQP::ConfigurationError, /DEVELOPMENT ONLY/)
    end

    it "says what to do about it rather than only saying no" do
      security = AceMQ::AMQP::Security.verified(certificate_authority: authority)

      expect { security.to_transport_options }
        .to raise_error(/allowing_development_certificates/)
    end

    it "accepts it once somebody has said that is what they meant" do
      security = AceMQ::AMQP::Security.verified(certificate_authority: authority)
                                      .allowing_development_certificates

      expect(security.allowing_development_certificates?).to be(true)
      expect(security.to_transport_options[:tls_ca_certificates]).to eq([authority])
    end

    it "keeps the opt-in through a copy, so credentials do not quietly undo it" do
      security = AceMQ::AMQP::Security.verified(certificate_authority: authority)
                                      .allowing_development_certificates
                                      .with_credentials(login)

      expect(security.allowing_development_certificates?).to be(true)
    end

    it "says so in its description, because it is the sort of thing a log should show" do
      security = AceMQ::AMQP::Security.verified.allowing_development_certificates
      expect(security.to_s).to include("developmentCertificates=allowed")
    end
  end

  # The handshake half. What the broker presents cannot be checked without a
  # broker, so what is checked here is the callback the context is given: that
  # it refuses a marked certificate, that it is installed even in unverified
  # mode, and that it does not otherwise change what each mode means.
  describe "the verify callback" do
    def store_holding(certificate)
      Struct.new(:current_cert).new(certificate)
    end

    it "refuses a marked certificate whether or not the chain checked out" do
      refuse = AceMQ::AMQP::Security.marker_refusing_callback(true)

      expect(silenced { refuse.call(true, store_holding(DEV_CERTS.server)) }).to be(false)
      expect(silenced { refuse.call(false, store_holding(DEV_CERTS.server)) }).to be(false)
    end

    it "keeps OpenSSL's own verdict for anything unmarked, when verifying" do
      refuse = AceMQ::AMQP::Security.marker_refusing_callback(true)

      expect(refuse.call(true, store_holding(unmarked))).to be(true)
      expect(refuse.call(false, store_holding(unmarked))).to be(false)
    end

    # Unverified means unverified. Raising the verify mode to get the callback
    # consulted at all must not quietly turn verification on.
    it "accepts anything unmarked when not verifying, however the chain looked" do
      refuse = AceMQ::AMQP::Security.marker_refusing_callback(false)

      expect(refuse.call(false, store_holding(unmarked))).to be(true)
    end

    it "says no rather than raising when it cannot read the certificate at all" do
      broken = Struct.new(:nothing) do
        def current_cert = raise("no certificate here")
      end.new(nil)

      refuse = AceMQ::AMQP::Security.marker_refusing_callback(true)
      expect(refuse.call(true, broken)).to be(false)
    end

    it "explains itself on stderr, because OpenSSL will not" do
      refuse = AceMQ::AMQP::Security.marker_refusing_callback(true)
      said = capture_stderr { refuse.call(true, store_holding(DEV_CERTS.server)) }

      expect(said).to include("ACEMQ DEVELOPMENT ONLY")
      expect(said).to include("allowing_development_certificates")
    end
  end

  def extension(certificate, oid)
    certificate.extensions.find { |e| e.oid == oid }&.value
  end

  # A certificate that is real, valid and has nothing to do with development.
  def unmarked
    key = OpenSSL::PKey::EC.generate("prime256v1")
    name = OpenSSL::X509::Name.parse("/O=A Real Company/CN=broker.example.com")
    certificate = OpenSSL::X509::Certificate.new
    certificate.version = 2
    certificate.serial = 1
    certificate.subject = name
    certificate.issuer = name
    certificate.public_key = key
    certificate.not_before = Time.now - 60
    certificate.not_after = Time.now + 3600
    certificate.sign(key, OpenSSL::Digest.new("SHA256"))
  end

  # The callback explains itself on stderr. Every example but the one asserting
  # that would otherwise print it, and a spec run that prints a warning per
  # example is a spec run nobody reads the end of.
  def silenced
    was = $stderr
    $stderr = StringIO.new
    yield
  ensure
    $stderr = was
  end

  def capture_stderr
    was = $stderr
    $stderr = StringIO.new
    yield
    $stderr.string
  ensure
    $stderr = was
  end
end
