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

require "openssl"

require_relative "credentials"

module AceMQ
  module AMQP
    # How a connection reaches the broker safely: whether it is encrypted,
    # which authority the broker is checked against, and who is logging in.
    #
    #   security = Security.verified(certificate_authority: "certs/ca.pem")
    #   mq = Connection.open("amqps://broker:5671", security: security,
    #                        credentials: Credentials.from_env)
    #
    # An +amqps://+ URL gets {verified} without anybody asking, so the ordinary
    # case needs none of this. The class is what a broker with a private
    # certificate authority, or one that authenticates clients by certificate,
    # or one whose password must not be in the URL, is configured with.
    #
    # There are three modes and they are named rather than being a boolean,
    # because "secure: false" does not say which of two quite different things
    # it means. {verified} encrypts and checks who answered.
    # {without_verifying_the_broker} encrypts and does not check.
    # {disabled} does not encrypt.
    #
    # This file has no dependency on bunny and does not know a connection
    # exists, so it can be built and tested without a broker in the room. What
    # it knows about bunny is confined to {#to_transport_options} and
    # {#configure}, which are the two seams the transport uses.
    #
    # == What this is actually fixing
    #
    # bunny does not verify the broker's certificate when it is given a URL.
    # Not "verifies weakly" — does not verify. A URL string is parsed by
    # +AMQ::Settings+, which merges in its own defaults, and one of those
    # defaults is +verify: false+; bunny then reads that as an explicit
    # instruction and sets +VERIFY_NONE+. So +Bunny.new("amqps://broker:5671")+
    # encrypts the traffic and accepts any certificate at all, including one
    # the connecting process just made up, and reports itself as +tls?+ the
    # whole time. Nothing warns, because from bunny's point of view somebody
    # asked for this.
    #
    # Every mode here therefore states +verify_peer+ outright rather than
    # leaving it unsaid, which is why {#to_transport_options} always carries it.
    class Security
      # Encrypt, and check that the certificate chains to a trusted authority.
      VERIFIED = :verified

      # Encrypt, and accept whatever certificate turns up. See
      # {without_verifying_the_broker} before using this.
      UNVERIFIED = :unverified

      # Do not encrypt.
      DISABLED = :disabled

      # What every certificate the AceMQ development tooling generates carries
      # in its subject.
      #
      # A certificate holding it is refused on every path, including
      # {UNVERIFIED}, unless {#allowing_development_certificates} says
      # otherwise. The point is that a development certificate reaching a
      # production broker should be an error rather than a thing that quietly
      # works because somebody turned verification off to get past a different
      # problem. The same string in Java, Go, .NET and here.
      DEVELOPMENT_MARKER = "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST"

      # The lowest version this library will negotiate.
      #
      # 1.0 and 1.1 have been deprecated since 2021 and RabbitMQ turns them off
      # by default; a broker that still offers them is a broker to fix rather
      # than a broker to reach down to.
      MINIMUM_TLS_VERSION = OpenSSL::SSL::TLS1_2_VERSION

      # The highest version this library will negotiate, when OpenSSL has it.
      #
      # bunny pins the version by setting the context's minimum *and* maximum to
      # the same constant, and its default for both is TLS 1.2, so a broker and
      # a client that could have agreed on 1.3 quietly settle for 1.2 instead.
      # {#configure} lifts the ceiling, which is the only reason that method
      # exists.
      MAXIMUM_TLS_VERSION = if defined?(OpenSSL::SSL::TLS1_3_VERSION)
                              OpenSSL::SSL::TLS1_3_VERSION
                            else
                              MINIMUM_TLS_VERSION
                            end

      attr_reader :mode, :certificate_authority, :certificate, :key, :credentials, :reason

      # Prefer the three named constructors. This is public so a caller holding
      # a mode can build one, and so {#with_credentials} can copy.
      #
      # @param mode [Symbol] {VERIFIED}, {UNVERIFIED} or {DISABLED}
      # @param certificate_authority [String, Array<String>, nil] PEM file paths
      # @param certificate [String, nil] path to a client certificate, PEM
      # @param key [String, nil] path to that certificate's private key, PEM
      # @param credentials [Credentials, #call, nil] the broker login
      # @param reason [String, nil] why verification is off, when it is
      # @param allow_development_certificates [Boolean] whether a certificate
      #   carrying {DEVELOPMENT_MARKER} is acceptable here. See
      #   {#allowing_development_certificates}, which is how it is normally set
      def initialize(mode:, certificate_authority: nil, certificate: nil, key: nil,
                     credentials: nil, reason: nil, allow_development_certificates: false)
        @mode = mode
        @certificate_authority = readable_files(certificate_authority, "certificate authority")
        @certificate = readable_file(certificate, "client certificate")
        @key = readable_file(key, "client private key")
        @credentials = credentials
        @reason = reason
        @allow_development_certificates = allow_development_certificates
        check_client_certificate_is_a_pair!
        freeze
      end

      # Encrypt, and verify the broker against a certificate authority.
      #
      # With no +certificate_authority+ the machine's own trust store is used,
      # which is what a broker with a certificate from a public authority needs
      # and nothing else does. Naming a file narrows trust to that authority
      # alone — the system store is then not consulted at all, which is the
      # point: a certificate from a public authority is not evidence that the
      # thing answering is *your* broker, and the hundreds of authorities a
      # machine trusts by default are hundreds of ways to be wrong.
      #
      # +certificate+ and +key+ are the client's own, for a broker that
      # authenticates clients by certificate rather than by password
      # (RabbitMQ's EXTERNAL mechanism). They go together: one without the other
      # is refused here rather than producing a handshake failure later.
      #
      # @param certificate_authority [String, Array<String>, nil] PEM file paths
      # @param certificate [String, nil]
      # @param key [String, nil]
      # @param credentials [Credentials, #call, nil]
      # @raise [ConfigurationError] when a file named here cannot be read
      def self.verified(certificate_authority: nil, certificate: nil, key: nil,
                        credentials: nil)
        new(mode: VERIFIED, certificate_authority: certificate_authority,
            certificate: certificate, key: key, credentials: credentials)
      end

      # Plaintext. Everything, including the password used to log in, crosses
      # the network readable.
      #
      # Reasonable against a broker on the same machine. Against one on the
      # other side of a network you do not entirely control, it is not.
      def self.disabled(credentials: nil)
        new(mode: DISABLED, credentials: credentials)
      end

      # Encrypt, and accept any certificate the broker presents.
      #
      # ============================= READ THIS =============================
      # This turns off the only check that distinguishes your broker from
      # anything else that can answer on that address. The traffic cannot be
      # read by somebody watching the network — and nothing stops that somebody
      # from *being* the broker. They present any certificate they like, this
      # library accepts it, and the connection then hands over the password in
      # the AMQP login and every message afterwards. It is encrypted the whole
      # way, to them. There is no symptom: the connection succeeds, the
      # messages flow, and the only evidence is on the attacker's disk.
      #
      # It exists because there is exactly one situation where it is the honest
      # answer: reaching a development broker whose self-signed certificate you
      # have not got round to trusting, on a machine where being wrong costs
      # nothing. In that situation +certificate_authority:+ on {verified} is
      # about four seconds more work and is correct, so prefer it even there.
      #
      # It is deliberately awkward to reach. The name is long and says what it
      # does, so it cannot be typed by accident and cannot be skimmed past in a
      # review; +because:+ is required and must say something, so the reason
      # lands in the code rather than in whoever's memory; and it never becomes
      # the default for any URL, so no configuration change can arrive at it
      # without somebody having written this line. If you are reading it in a
      # diff, the question to ask is not whether it works — it always works,
      # that is the problem — but what stops this process reaching a production
      # broker.
      # =====================================================================
      #
      # @param because [String] why verification is off here
      # @raise [ConfigurationError] when no reason is given
      def self.without_verifying_the_broker(because:, certificate: nil, key: nil,
                                            credentials: nil)
        reason = because.to_s.strip
        if reason.empty?
          raise ConfigurationError,
                "without_verifying_the_broker needs a reason: pass because: with the " \
                "circumstance that makes an unverified broker acceptable here, so the " \
                "next person to read this line does not have to guess"
        end

        new(mode: UNVERIFIED, certificate: certificate, key: key,
            credentials: credentials, reason: reason)
      end

      # What a URL asks for on its own.
      #
      # +amqps://+ means encrypted and verified, because that is what the scheme
      # promises everywhere else it appears and a client that quietly meant
      # something weaker is a client nobody can reason about. +amqp://+ means
      # plaintext, which is what it says.
      #
      # @param url [String]
      # @param credentials [Credentials, #call, nil]
      def self.for_url(url, credentials: nil)
        if url.to_s.strip.downcase.start_with?("amqps://")
          verified(credentials: credentials)
        else
          disabled(credentials: credentials)
        end
      end

      # The security a connection should use, given what the caller passed.
      #
      # Both arguments are optional and independent: +security+ describes the
      # channel, +credentials+ describe the login, and either can be given
      # without the other. Passing credentials twice is refused rather than
      # silently resolved in one direction, because the two answers differ and
      # neither is obviously the one that was meant.
      #
      # @api private
      def self.for_connection(url, security: nil, credentials: nil)
        security ||= for_url(url)
        return security if credentials.nil?

        unless security.credentials.nil?
          raise ConfigurationError,
                "credentials were passed both to the connection and to its Security; " \
                "put them in one place so there is no question which login is used"
        end

        security.with_credentials(credentials)
      end

      # Whether anything is encrypted.
      def encrypted? = mode != DISABLED

      # Whether the broker's certificate is checked against an authority.
      def verifying? = mode == VERIFIED

      # Whether a client certificate is presented.
      def client_certificate? = !certificate.nil?

      # Whether a certificate carrying {DEVELOPMENT_MARKER} is acceptable.
      def allowing_development_certificates? = @allow_development_certificates

      # A copy that accepts the certificates the AceMQ development tooling
      # generates.
      #
      #   security = Security.verified(certificate_authority: ".tls/ca.crt")
      #                      .allowing_development_certificates
      #
      # A separate, visible step rather than a keyword on the constructors, and
      # for the same reason {without_verifying_the_broker} has a long name: it
      # has to be legible in a diff. Turning it on is saying "the broker this
      # process reaches is a development broker", and the whole value of the
      # marker is that saying so is a decision somebody made on purpose.
      #
      # It does not weaken anything else. Verification stays on, the authority
      # stays whatever it was, and a certificate that does not verify is still
      # refused — this only stops the marker itself being the reason.
      #
      # @return [Security]
      def allowing_development_certificates
        copy(allow_development_certificates: true)
      end

      # Whether a certificate is one the AceMQ development tooling generated.
      #
      # The subject *and* the issuer, because a leaf signed by a marked
      # authority is a development certificate whether or not it says so
      # itself — and in a chain where only the leaf is presented, the issuer is
      # the only place the marker appears.
      #
      # @param certificate [OpenSSL::X509::Certificate, nil]
      # @return [Boolean]
      def self.development_certificate?(certificate)
        return false if certificate.nil?

        marker = DEVELOPMENT_MARKER.upcase
        [certificate.subject, certificate.issuer]
          .any? { |name| name.to_s.upcase.include?(marker) }
      rescue StandardError
        # A certificate that will not describe itself is not evidence of the
        # marker. Whatever else is wrong with it, the handshake will say so.
        false
      end

      # The login, resolved now.
      #
      # Resolution happens here rather than in the constructor so a block that
      # re-reads a rotating secret is called at connection time, which is the
      # only time its answer is worth anything.
      #
      # @return [Credentials, nil]
      def resolved_credentials = Credentials.resolve(credentials)

      # A copy with a different login.
      def with_credentials(other) = copy(credentials: other)

      # The bunny options this describes.
      #
      # +verify_peer+ is stated in every mode, including the ones where it is
      # what bunny would have done anyway. Leaving it unsaid is what causes the
      # problem described at the top of this file: bunny reads the absence of an
      # opinion from a URL as an instruction not to verify.
      #
      # @return [Hash]
      # @raise [ConfigurationError] when this process was configured with a
      #   development certificate and has not said that is what it meant
      def to_transport_options
        check_development_certificates!
        options = { tls: encrypted? }
        options[:verify_peer] = verifying? if encrypted?
        options[:tls_ca_certificates] = certificate_authority if certificate_authority
        if client_certificate?
          options[:tls_cert] = certificate
          options[:tls_key] = key
        end
        options.merge(resolved_credentials&.to_transport_options || {})
      end

      # Settles the TLS versions, and the refusal of development certificates,
      # on a session that has not started yet.
      #
      # Everything else this class configures goes through the options hash;
      # this cannot, because bunny builds its SSL context from that hash and
      # pins the minimum and the maximum to the same version, and the only way
      # to say "1.2 or better" rather than "1.2 exactly" is to reach the context
      # afterwards. The session must not have been started: the context is used
      # once, when the socket is wrapped.
      #
      # Guarded on both sides because it is reaching past bunny's documented
      # surface. A bunny that no longer offers this leaves the connection on
      # bunny's own pin, which is a version this library considers acceptable
      # anyway — so the failure mode is a slightly older protocol, not a
      # connection that will not open.
      #
      # @param session [Bunny::Session]
      def configure(session)
        return unless encrypted?
        return unless session.respond_to?(:transport)

        transport = session.transport
        return unless transport.respond_to?(:configure_tls_context)

        transport.configure_tls_context do |context|
          context.min_version = MINIMUM_TLS_VERSION
          context.max_version = MAXIMUM_TLS_VERSION
          refuse_development_certificates(context)
        end
        nil
      end

      # Never the credentials' secret — {Credentials#to_s} sees to that — and
      # never anything else that would be worth stealing. Certificate paths are
      # shown because a connection that trusted the wrong authority is
      # diagnosed by reading which file it trusted.
      def to_s
        parts = ["mode=#{mode}"]
        parts << "authority=#{Array(certificate_authority).join(",")}" if certificate_authority
        parts << "clientCertificate=#{certificate}" if client_certificate?
        parts << "credentials=(#{credentials_description})" unless credentials.nil?
        parts << "because=#{reason.inspect}" if reason
        parts << "developmentCertificates=allowed" if allowing_development_certificates?
        parts.join(" ")
      end

      def inspect
        "#<AceMQ::AMQP::Security #{self}>"
      end

      # The verify callback that refuses the marker and decides nothing else.
      #
      # In verifying mode OpenSSL's own verdict stands for everything that is
      # not marked; in unverified mode everything that is not marked is
      # accepted, which is what unverified meant before this existed.
      #
      # It never raises. An exception from inside a verify callback is caught by
      # OpenSSL and turned into a bare "certificate verify failed", so the
      # reason is written to stderr instead, immediately above the failure.
      #
      # @param verifying [Boolean] whether the chain is being checked as well
      # @return [Proc]
      # @api private
      def self.marker_refusing_callback(verifying)
        lambda do |trusted, store|
          certificate = store.current_cert
          if development_certificate?(certificate)
            complain_about(certificate)
            next false
          end

          verifying ? trusted : true
        rescue StandardError
          # A callback that raises is a callback whose answer OpenSSL cannot
          # read, and the safe reading of "I do not know" is no.
          false
        end
      end

      # @api private
      def self.complain_about(certificate)
        warn("acemq: the broker presented a certificate marked " \
             "#{DEVELOPMENT_MARKER.inspect} (subject #{certificate.subject}). It was " \
             "generated for development and is not trusted, however this connection is " \
             "otherwise configured. If this really is a development broker, say so with " \
             "Security#allowing_development_certificates.")
      end

      # Whether a PEM file holds a certificate carrying the marker.
      #
      # @api private
      def self.development_certificate_file?(path)
        return false if path.nil?

        File.read(path)
            .scan(/-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----/m)
            .any? { |pem| development_certificate?(OpenSSL::X509::Certificate.new(pem)) }
      rescue StandardError
        # Unreadable, or not a certificate at all. Neither is the marker, and
        # both are reported by something better placed to explain them.
        false
      end

      private

      # A copy of this one with some fields replaced.
      def copy(**changes)
        self.class.new(
          mode: changes.fetch(:mode, mode),
          certificate_authority: changes.fetch(:certificate_authority, certificate_authority),
          certificate: changes.fetch(:certificate, certificate),
          key: changes.fetch(:key, key),
          credentials: changes.fetch(:credentials, credentials),
          reason: changes.fetch(:reason, reason),
          allow_development_certificates: changes.fetch(:allow_development_certificates,
                                                        allowing_development_certificates?)
        )
      end

      # Refuses, at the handshake, a broker whose certificate carries the
      # marker — in every mode, which is the part that takes some doing.
      #
      # In {UNVERIFIED} mode bunny sets the context to +VERIFY_NONE+, and
      # OpenSSL does not consult a verify callback's answer at all in that mode:
      # "the handshake will be continued regardless of the verification result".
      # So the mode is raised to +VERIFY_PEER+ here and the callback then
      # accepts everything the way +VERIFY_NONE+ did — everything except the
      # marker. Hostname checking is turned off explicitly, because raising the
      # mode would otherwise switch it on and change what unverified means.
      #
      # This is the same shape Go uses: +InsecureSkipVerify+ with a
      # +VerifyPeerCertificate+ that refuses the marker, for exactly the reason
      # that unverified is the configuration a development certificate is most
      # likely to slip through.
      def refuse_development_certificates(context)
        return if allowing_development_certificates?

        unless verifying?
          context.verify_mode = OpenSSL::SSL::VERIFY_PEER
          context.verify_hostname = false if context.respond_to?(:verify_hostname=)
        end

        # Read once rather than per certificate: the callback runs on the
        # handshake's thread and must be cheap and must not reach for anything
        # that could have changed underneath it.
        verifying = verifying?
        context.verify_callback = self.class.marker_refusing_callback(verifying)
      end

      # Refuses a certificate authority or a client certificate that was
      # generated for development, before anything opens a socket.
      #
      # The handshake check above covers what the *broker* presents. This covers
      # what this process was configured with, and it is the half that can be
      # caught without a broker in the room: a deployment pointed at
      # +certs/ca.crt+ from somebody's laptop is a deployment that trusts an
      # authority anybody can regenerate.
      #
      # At {#to_transport_options} rather than in the constructor, unlike the
      # file readability checks. A constructor that refused would make
      # +Security.verified(ca).allowing_development_certificates+ impossible to
      # write, because the refusal would happen before the sentence finished.
      # This is the moment a connection is being made, which is the moment the
      # question is actually being asked.
      def check_development_certificates!
        return if allowing_development_certificates?

        offending = [[certificate_authority, "certificate authority"],
                     [certificate, "client certificate"]].flat_map do |paths, what|
          Array(paths).select { |path| self.class.development_certificate_file?(path) }
                      .map { |path| "the #{what} #{path}" }
        end
        return if offending.empty?

        raise ConfigurationError,
              "#{offending.join(" and ")} carries #{DEVELOPMENT_MARKER.inspect}. It was " \
              "generated by the AceMQ development tooling and is refused here, because an " \
              "authority anybody can regenerate is not one to verify a production broker " \
              "against. If this really is a development broker, say so with " \
              "Security#allowing_development_certificates."
      end

      # Says a login is configured without asking for it. Resolving a block
      # here would run somebody's secret-fetching code because a logger dumped
      # an object, which is not a thing +inspect+ should be able to cause.
      def credentials_description
        credentials.is_a?(Credentials) ? credentials.to_s : "supplied at connection time"
      end

      # A client certificate without its key, or a key without its certificate,
      # is a configuration that cannot work. Caught here because the alternative
      # is a TLS handshake failure whose message names neither file.
      def check_client_certificate_is_a_pair!
        return if certificate.nil? == key.nil?

        missing, given = certificate.nil? ? %w[certificate key] : %w[key certificate]
        raise ConfigurationError,
              "a client #{given} was given without its #{missing}; mutual TLS needs both, " \
              "and a broker asked to check half a pair refuses the connection"
      end

      # Paths are checked now, while there is a stack that says which setting
      # was wrong, rather than at connection time when the error comes back from
      # OpenSSL naming a file it could not open and nothing about why anybody
      # wanted it.
      def readable_file(path, what)
        return nil if path.nil?

        path = path.to_s
        unless File.file?(path) && File.readable?(path)
          raise ConfigurationError, "cannot read the #{what} #{path}"
        end

        path
      end

      def readable_files(paths, what)
        return nil if paths.nil?

        files = Array(paths).map { |path| readable_file(path, what) }
        files.empty? ? nil : files.freeze
      end
    end
  end
end
