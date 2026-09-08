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
require "uri"

require "acemq/amqp"

# TLS, against a broker that is really speaking it.
#
# A unit spec can prove that the right options were assembled, and that is worth
# having, but it cannot prove the thing that matters. An unverified TLS
# connection and a verified one are identical from the application's side: both
# open, both carry messages, both report themselves as encrypted. So every
# example here reads the answer off the socket that actually negotiated — the
# protocol version, the cipher, the certificate the broker presented and the
# chain it presented it with — rather than off the configuration that asked for
# it. And the example that matters most is the one where the connection is
# expected to *fail*, because a connection that succeeds when it should not is
# exactly what a broken verification looks like.
#
#   ACEMQ_TEST_BROKER_TLS=amqps://guest:guest@localhost:5671 \
#   ACEMQ_TEST_BROKER_CA=certs/ca.crt \
#   bundle exec rspec --tag integration
#
# ACEMQ_TEST_BROKER_CLIENT_CERT and ACEMQ_TEST_BROKER_CLIENT_KEY are set as well
# when the broker demands a client certificate, and the mutual-TLS examples run
# only then.
RSpec.describe "over TLS, against a real broker", :integration do
  # Read through methods rather than into locals so that the helpers below can
  # see them: a local in this block is closed over by an example, but not by a
  # def.
  def broker = ENV.fetch("ACEMQ_TEST_BROKER_TLS", nil)
  def authority = ENV.fetch("ACEMQ_TEST_BROKER_CA", nil)
  def client_certificate = ENV.fetch("ACEMQ_TEST_BROKER_CLIENT_CERT", nil)
  def client_key = ENV.fetch("ACEMQ_TEST_BROKER_CLIENT_KEY", nil)

  before do
    skip "set ACEMQ_TEST_BROKER_TLS and ACEMQ_TEST_BROKER_CA to run these" if broker.nil?
  end

  # The client certificate goes into every connection when the broker demands
  # one, because a broker configured with fail_if_no_peer_cert refuses the
  # handshake without it and every example below would then be testing the same
  # refusal.
  def client_pair
    { certificate: client_certificate, key: client_key }
  end

  def security(**overrides)
    AceMQ::AMQP::Security.verified(**client_pair, **overrides)
  end

  def connect(**overrides)
    AceMQ::AMQP::Connection.open(broker, origin: "rspec@rbit-tls",
                                         security: security(**overrides))
  end

  # bunny's own socket, which is where the truth is. Everything above it will
  # happily report a connection as encrypted whether or not anybody checked who
  # it was encrypted to.
  def negotiated(connection)
    connection.transport.session.transport.socket
  end

  describe "the connection the library actually makes" do
    let(:mq) { connect(certificate_authority: authority) }

    after { mq.close if broker }

    # Read off the socket rather than off the options: "configured for TLS" and
    # "speaking TLS" are different claims and only the second one is a defence.
    it "negotiates TLS, and a version worth negotiating" do
      socket = negotiated(mq)

      expect(socket).to be_a(OpenSSL::SSL::SSLSocket)
      expect(socket.ssl_version).to match(/\ATLSv1\.[23]\z/)
      expect(socket.cipher.first).not_to be_empty
      expect(mq.transport.session.uses_tls?).to be(true)
    end

    # bunny pins its context's minimum and maximum to the same version, so
    # without Security#configure lifting the ceiling this is TLS 1.2 against a
    # broker that offers 1.3. Asserted because a silent downgrade is the kind of
    # regression nothing else notices.
    it "reaches TLS 1.3 when the broker offers it" do
      offered = `openssl s_client -connect #{host_and_port} -brief </dev/null 2>&1`
      skip "this broker does not offer TLS 1.3" unless offered.include?("TLSv1.3")

      expect(negotiated(mq).ssl_version).to eq("TLSv1.3")
    end

    it "verified the broker against the authority it was given" do
      socket = negotiated(mq)

      # 0 is X509_V_OK. Any other value is a chain that did not check out, and
      # the connection having opened anyway is precisely the failure this whole
      # file exists to catch.
      expect(socket.verify_result).to eq(0)
      expect(socket.peer_cert).not_to be_nil

      root = socket.peer_cert_chain.last.subject.to_s
      expect(root).to eq(OpenSSL::X509::Certificate.new(File.read(authority)).subject.to_s)
    end
  end

  # The one that proves verification is on. Without a certificate authority the
  # broker's self-signed chain reaches nothing the machine trusts, so a client
  # that is really checking cannot open this connection. bunny on its own does
  # open it, which is the bug {AceMQ::AMQP::Security} exists to close.
  it "refuses a broker it cannot verify" do
    expect { connect }.to raise_error(AceMQ::AMQP::TransportError, /certificate verify failed/)
  end

  it "will not verify a broker against the wrong authority" do
    wrong = Tempfile.new(["wrong-ca", ".pem"])
    wrong.write(unrelated_authority)
    wrong.close

    expect { connect(certificate_authority: wrong.path) }
      .to raise_error(AceMQ::AMQP::TransportError, /certificate verify failed/)
  ensure
    wrong&.unlink
  end

  describe "mutual TLS" do
    before do
      skip "this broker does not ask for a client certificate" if client_certificate.nil?
    end

    it "presents the client certificate the broker demands" do
      mq = connect(certificate_authority: authority)

      expect(mq.transport).to be_open
      expect(negotiated(mq).ssl_version).to match(/\ATLSv1\.[23]\z/)
    ensure
      mq&.close
    end

    # The other half of the proof: the certificate is not decoration. A broker
    # with fail_if_no_peer_cert turns a connection without one away, so this
    # failing is what says the certificate above was really sent and really
    # checked.
    it "cannot connect without it" do
      bare = AceMQ::AMQP::Security.verified(certificate_authority: authority)

      expect { AceMQ::AMQP::Connection.open(broker, security: bare) }
        .to raise_error(AceMQ::AMQP::TransportError)
    end
  end

  describe "credentials kept out of the URL" do
    # The whole point of Credentials: this URL carries no password, so nothing
    # that logs it — including the error message the transport builds two lines
    # into a failure — has one to leak.
    it "logs in with credentials supplied separately" do
      mq = AceMQ::AMQP::Connection.open(
        url_without_credentials(broker),
        security: security(certificate_authority: authority),
        credentials: AceMQ::AMQP::Credentials.of(username: "guest", password: "guest")
      )

      expect(mq.transport).to be_open
    ensure
      mq&.close
    end

    it "reports a wrong password without repeating it" do
      wrong = AceMQ::AMQP::Credentials.of(username: "guest", password: "not-the-password")
      opening = lambda do
        AceMQ::AMQP::Connection.open(url_without_credentials(broker),
                                     security: security(certificate_authority: authority),
                                     credentials: wrong)
      end

      expect(&opening).to raise_error(AceMQ::AMQP::TransportError) do |error|
        expect(error.message).not_to include("not-the-password")
      end
    end
  end

  describe "messages over the encrypted connection" do
    let(:mq) { connect(certificate_authority: authority) }
    let(:queue) { "rbit.tls.round-trip" }

    # The queue, plus the two a consumer declares for itself at start-up so
    # that a message it cannot handle has somewhere to land. All three are
    # removed and asserted absent, so a run of this file leaves the broker
    # holding exactly what it held before. A spec that leaves a queue behind is
    # a spec that passes on a broker it has already changed.
    let(:queues) do
      [queue, AceMQ::AMQP::Naming.dead_letter_queue(queue),
       AceMQ::AMQP::Naming.parked_queue(queue)]
    end

    before do
      next if broker.nil?

      expect(mq.transport.queue_exists?(queue)).to be(false)
      # Durable, because RabbitMQ 4 refuses a transient non-exclusive queue and
      # answers the declaration by closing the connection.
      mq.transport.declare_queue(queue)
    end

    after do
      next if broker.nil?

      queues.each do |name|
        mq.transport.delete_queue(name)
      rescue AceMQ::AMQP::TransportError
        # Not there is the state this wanted anyway.
        nil
      end
      expect(queues.map { |name| mq.transport.queue_exists?(name) }).to all(be(false))
      mq.close
    end

    # Consuming before publishing, as the plain-AMQP integration spec does.
    # The other order is a race with bunny rather than with the broker: a queue
    # that already has a message on it is drained the instant the subscription
    # is accepted, and the delivery can reach bunny's reader loop before bunny
    # has finished putting the consumer in the table it looks the tag up in.
    it "carries a message there and back with its envelope intact" do
      received = nil
      mq.consume(queue) do |message|
        received = message
        AceMQ::AMQP::Ack.accept
      end

      sent = mq.publish({ "order_id" => "A-1" }, to: queue, type: "order.placed.v1")

      # Polling rather than a condition variable, for the same reason the other
      # integration spec polls: the handler runs on bunny's threads, and a test
      # that deadlocks tells you nothing at all.
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
      while received.nil? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        sleep(0.05)
      end

      expect(received).not_to be_nil
      expect(received.id).to eq(sent.id)
      expect(received.payload).to eq("order_id" => "A-1")
      expect(received.envelope.type).to eq("order.placed.v1")
    end
  end

  # A certificate authority that is real, valid and has nothing to do with this
  # broker — the honest shape of the "encrypted to somebody else" mistake.
  def unrelated_authority
    key = OpenSSL::PKey::RSA.new(2048)
    name = OpenSSL::X509::Name.parse("/O=AceMQ Ruby Spec/CN=An Authority This Broker Never Met")
    certificate = OpenSSL::X509::Certificate.new
    certificate.version = 2
    certificate.serial = 1
    certificate.subject = name
    certificate.issuer = name
    certificate.public_key = key.public_key
    certificate.not_before = Time.now - 60
    certificate.not_after = Time.now + 3600
    certificate.sign(key, OpenSSL::Digest.new("SHA256"))
    certificate.to_pem
  end

  def url_without_credentials(url)
    url.sub(%r{(://)[^/@]*@}, '\1')
  end

  def host_and_port
    uri = URI.parse(broker)
    "#{uri.host}:#{uri.port || 5671}"
  end
end
