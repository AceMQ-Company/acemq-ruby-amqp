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

require "tempfile"

require "acemq/amqp"

# What this class decides is which of two quite different connections gets made,
# and the difference is invisible from outside: an unverified TLS connection
# looks exactly like a verified one until somebody is reading the traffic. So
# the options hash is written out in full below rather than spot-checked, and
# every mode asserts verify_peer explicitly — the whole reason this class exists
# is that leaving verify_peer unsaid is what turns verification off.
RSpec.describe AceMQ::AMQP::Security do
  let(:secret) { "hunter2" }
  let(:credentials) { AceMQ::AMQP::Credentials.of(username: "app", password: secret) }

  # A file that exists, for the paths that are checked at construction. Its
  # contents do not matter: nothing here opens a socket, so OpenSSL never reads
  # it.
  let(:pem) do
    file = Tempfile.new(["acemq", ".pem"])
    file.write("-- not really a certificate --")
    file.close
    file
  end

  after { pem.unlink }

  describe ".for_url" do
    it "verifies an amqps:// broker without being asked" do
      security = described_class.for_url("amqps://broker:5671")

      expect(security.mode).to eq(described_class::VERIFIED)
      expect(security).to be_encrypted
      expect(security).to be_verifying
    end

    it "leaves an amqp:// broker in plaintext, which is what the scheme says" do
      security = described_class.for_url("amqp://broker:5672")

      expect(security.mode).to eq(described_class::DISABLED)
      expect(security).not_to be_encrypted
    end

    it "reads the scheme however it was capitalised or padded" do
      expect(described_class.for_url("  AMQPS://broker:5671  ")).to be_verifying
    end
  end

  describe "#to_transport_options" do
    # bunny given a URL sets VERIFY_NONE, because AMQ::Settings merges
    # verify: false into the options it parses out of one and bunny reads that
    # as an instruction. Stating verify_peer is the fix, so the assertion is on
    # the key being there and true rather than on a connection working — a
    # connection that does not verify works perfectly.
    it "tells bunny to verify, which it will not do on its own" do
      expect(described_class.verified.to_transport_options).to eq(tls: true, verify_peer: true)
    end

    it "narrows trust to a named certificate authority" do
      options = described_class.verified(certificate_authority: pem.path).to_transport_options

      expect(options).to eq(tls: true, verify_peer: true, tls_ca_certificates: [pem.path])
    end

    it "takes several authorities, for a fleet mid-way through a rotation" do
      options = described_class.verified(certificate_authority: [pem.path, pem.path])
                               .to_transport_options

      expect(options[:tls_ca_certificates]).to eq([pem.path, pem.path])
    end

    it "presents a client certificate and its key for mutual TLS" do
      options = described_class.verified(certificate: pem.path, key: pem.path)
                               .to_transport_options

      expect(options).to eq(tls: true, verify_peer: true, tls_cert: pem.path, tls_key: pem.path)
    end

    it "asks for no encryption at all when it is disabled" do
      expect(described_class.disabled.to_transport_options).to eq(tls: false)
    end

    it "says outright that it is not verifying, when it is not" do
      security = described_class.without_verifying_the_broker(because: "a laptop broker")

      expect(security.to_transport_options).to eq(tls: true, verify_peer: false)
    end

    it "carries the login, so the password never goes near the URL" do
      security = described_class.verified(credentials: credentials)

      expect(security.to_transport_options)
        .to eq(tls: true, verify_peer: true, username: "app", password: secret)
    end

    # Resolution at connection time is the point of accepting a block: a secret
    # rotated underneath a running process is only useful if something reads it
    # again.
    it "resolves a block each time it is asked, not once at configuration" do
      reads = 0
      source = lambda do
        reads += 1
        AceMQ::AMQP::Credentials.of(username: "app", password: "#{secret}-#{reads}")
      end
      security = described_class.disabled(credentials: source)

      expect(security.to_transport_options[:password]).to eq("#{secret}-1")
      expect(security.to_transport_options[:password]).to eq("#{secret}-2")
    end
  end

  describe ".without_verifying_the_broker" do
    # The reason is required so the circumstance that made this acceptable ends
    # up in the code rather than in somebody's memory of a conversation.
    it "insists on a reason" do
      expect { described_class.without_verifying_the_broker(because: "  ") }
        .to raise_error(AceMQ::AMQP::ConfigurationError, /needs a reason/)
    end

    it "keeps the reason where a reviewer will find it" do
      security = described_class.without_verifying_the_broker(because: "kind broker, cruel net")

      expect(security.reason).to eq("kind broker, cruel net")
      expect(security.inspect).to include("kind broker, cruel net")
    end

    # No URL and no default arrives here. Getting an unverified connection takes
    # somebody writing this method name out.
    it "is never what a URL means on its own" do
      unverified = described_class::UNVERIFIED

      expect(described_class.for_url("amqps://b:5671").mode).not_to eq(unverified)
      expect(described_class.for_url("amqp://b:5672").mode).not_to eq(unverified)
    end
  end

  describe "checking files when they are configured rather than when they are used" do
    it "names the certificate authority it could not read" do
      expect { described_class.verified(certificate_authority: "/no/such/ca.pem") }
        .to raise_error(AceMQ::AMQP::ConfigurationError,
                        %r{certificate authority /no/such/ca.pem})
    end

    it "names the client certificate it could not read" do
      expect { described_class.verified(certificate: "/no/such/client.pem", key: pem.path) }
        .to raise_error(AceMQ::AMQP::ConfigurationError, %r{client certificate /no/such/client})
    end

    # Half a pair is a configuration that cannot work, and the handshake failure
    # it would otherwise produce names neither file.
    it "refuses a client certificate with no key" do
      expect { described_class.verified(certificate: pem.path) }
        .to raise_error(AceMQ::AMQP::ConfigurationError, /needs both/)
    end

    it "refuses a key with no client certificate" do
      expect { described_class.verified(key: pem.path) }
        .to raise_error(AceMQ::AMQP::ConfigurationError, /needs both/)
    end
  end

  describe ".for_connection" do
    it "falls back to what the URL asks for" do
      expect(described_class.for_connection("amqps://broker:5671")).to be_verifying
    end

    it "attaches credentials passed alongside a security" do
      security = described_class.for_connection(
        "amqps://broker:5671",
        security: described_class.verified(certificate_authority: pem.path),
        credentials: credentials
      )

      expect(security.certificate_authority).to eq([pem.path])
      expect(security.resolved_credentials).to eq(credentials)
    end

    # Two answers, neither obviously the one that was meant. Refusing is the
    # only response that cannot log in as the wrong account.
    it "refuses credentials given twice" do
      already_carrying = described_class.disabled(credentials: credentials)

      expect do
        described_class.for_connection("amqp://broker:5672", security: already_carrying,
                                                             credentials: credentials)
      end.to raise_error(AceMQ::AMQP::ConfigurationError, /both/)
    end
  end

  describe "#inspect" do
    it "never renders the secret" do
      security = described_class.verified(credentials: credentials)

      expect(security.inspect).not_to include(secret)
      expect(security.inspect).to include("app", "[REDACTED]")
    end

    # Asking a block for its answer because something logged an object would
    # mean inspect could reach a secret store, and could fail.
    it "never calls a credentials block" do
      called = false
      security = described_class.disabled(credentials: -> { called = true })

      expect(security.inspect).to include("supplied at connection time")
      expect(called).to be(false)
    end

    it "shows which authority is trusted, which is what a bad connection is read by" do
      security = described_class.verified(certificate_authority: pem.path)

      expect(security.inspect).to include(pem.path)
    end
  end

  describe "#configure" do
    # A context that remembers what was set on it, because OpenSSL's own has
    # writers for these and mostly no readers, so there is no way to ask a real
    # one what it was told. That it works on a real one is proved by the
    # integration spec, which reads the version off a socket that actually
    # negotiated and watches a marked certificate be refused by a real broker.
    def recording_context
      context = Object.new
      context.instance_variable_set(:@versions, {})
      context.define_singleton_method(:versions) { @versions }
      context.define_singleton_method(:min_version=) { |v| @versions[:min] = v }
      context.define_singleton_method(:max_version=) { |v| @versions[:max] = v }
      context.define_singleton_method(:verify_mode=) { |v| @versions[:verify_mode] = v }
      context.define_singleton_method(:verify_hostname=) { |v| @versions[:verify_hostname] = v }
      context.define_singleton_method(:verify_callback=) { |v| @versions[:verify_callback] = v }
      context
    end

    # A stand-in for the two bunny methods this reaches through, written out
    # rather than mocked so the spec fails if the shape of that reach changes.
    def session_holding(context)
      transport = Object.new
      transport.define_singleton_method(:configure_tls_context) { |&block| block.call(context) }
      session = Object.new
      session.define_singleton_method(:transport) { transport }
      session
    end

    # bunny pins its context's minimum and maximum to the same version, and its
    # default for both is TLS 1.2, so a broker and a client that could have
    # agreed on 1.3 settle for 1.2. This is the only thing that cannot be said
    # through the options hash.
    it "lifts the ceiling bunny pins to a single version" do
      context = recording_context

      described_class.verified.configure(session_holding(context))

      expect(context.versions[:min]).to eq(described_class::MINIMUM_TLS_VERSION)
      expect(context.versions[:max]).to eq(described_class::MAXIMUM_TLS_VERSION)
    end

    it "leaves a plaintext connection alone" do
      context = recording_context

      described_class.disabled.configure(session_holding(context))

      expect(context.versions).to be_empty
    end

    # Reaching past bunny's documented surface, so a bunny that no longer offers
    # it must leave a working connection rather than an exception.
    it "does nothing when bunny offers no such hook" do
      expect { described_class.verified.configure(Object.new) }.not_to raise_error
    end

    it "installs the check that refuses a development certificate" do
      context = recording_context

      described_class.verified.configure(session_holding(context))

      expect(context.versions[:verify_callback]).to respond_to(:call)
    end

    # The awkward case, and the one worth spelling out. bunny sets VERIFY_NONE
    # for an unverified connection, and OpenSSL does not act on a verify
    # callback's answer in that mode — "the handshake will be continued
    # regardless of the verification result" — so the mode has to be raised for
    # the callback to be consulted at all. Hostname checking is then turned off
    # explicitly, because raising the mode would otherwise switch it on and
    # quietly change what unverified means.
    it "raises the verify mode in unverified mode, and turns hostname checking off" do
      context = recording_context

      described_class.without_verifying_the_broker(because: "a spec")
                     .configure(session_holding(context))

      expect(context.versions[:verify_mode]).to eq(OpenSSL::SSL::VERIFY_PEER)
      expect(context.versions[:verify_hostname]).to be(false)
      expect(context.versions[:verify_callback]).to respond_to(:call)
    end

    it "leaves the mode alone when verifying, because bunny already set it" do
      context = recording_context

      described_class.verified.configure(session_holding(context))

      expect(context.versions).not_to have_key(:verify_mode)
    end

    it "installs nothing once somebody has said it is a development broker" do
      context = recording_context

      described_class.verified.allowing_development_certificates
                     .configure(session_holding(context))

      expect(context.versions).not_to have_key(:verify_callback)
    end
  end
end
