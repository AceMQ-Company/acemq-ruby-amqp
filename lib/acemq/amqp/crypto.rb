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

require_relative "ack"
require_relative "codec"
require_relative "credentials"

module AceMQ
  module AMQP
    # Getting an AES key from the shapes one usually arrives in.
    #
    # Keys are plain binary strings here rather than a wrapper class, because
    # that is what OpenSSL wants and what a secret store hands back. The
    # wrapper worth having is {EncryptionKey}, which pairs a key with the name
    # messages will refer to it by.
    module Keys
      # The three lengths AES takes. Anything else is refused rather than
      # padded or hashed into shape, because both would make a weak key look
      # like a strong one.
      SIZES = [16, 24, 32].freeze

      # A new key from the platform's secure random.
      #
      # @param bits [Integer] 128, 192 or 256
      # @return [String] binary key material
      def self.generate(bits: 256)
        bytes = bits / 8
        unless SIZES.include?(bytes)
          raise ArgumentError, "an AES key is 128, 192 or 256 bits and this asked for #{bits}"
        end

        OpenSSL::Random.random_bytes(bytes)
      end

      # @param bytes [String] 16, 24 or 32 bytes of key material
      # @return [String] the same bytes, checked
      def self.from_bytes(bytes)
        material = bytes.to_s.dup.force_encoding(Encoding::BINARY)
        return material if SIZES.include?(material.bytesize)

        raise ArgumentError,
              "an AES key is 16, 24 or 32 bytes and this is #{material.bytesize}. If it came " \
              "from a passphrase it needs a key derivation function such as PBKDF2 or Argon2 " \
              "rather than being used as it stands."
      end

      # The usual way a key arrives from a secret store or the environment.
      #
      # @param text [String] key material, Base64 encoded
      # @return [String] the key
      def self.from_base64(text)
        decoded =
          begin
            text.to_s.strip.unpack1("m0")
          rescue ArgumentError => e
            # Deliberately without the value. Whatever that string was, it was
            # meant to be a key, and a key does not belong in a message that
            # will be logged.
            raise ArgumentError, "could not read a key from Base64: #{e.message}"
          end
        from_bytes(decoded)
      end

      # For writing a generated key into a secret store.
      #
      # @param key [String] binary key material
      # @return [String] Base64, no line breaks
      def self.to_base64(key) = [key.to_s].pack("m0")
    end

    # A key and the name messages will refer to it by.
    #
    # The identifier is the part that makes rotation possible. It travels in
    # every message, so a message written last month can still be read by a
    # service whose current key is a different one, without anybody having to
    # know when the change happened.
    #
    # *Identifiers are public.* They sit in the clear in front of the
    # ciphertext where anyone holding the message can read them, which is the
    # point — the reader has to know what to ask the key store for before it
    # can decrypt anything. Name them for the key, not for what they protect:
    # +orders-2026-09+ rather than +customer-card-numbers+.
    class EncryptionKey
      # The identifier goes in the framing behind a single length byte, so this
      # is what fits. Long enough for a name and a date; short enough that it
      # is not a place to put a comment.
      MAX_ID_BYTES = 255

      # What OpenSSL calls AES-GCM at each key length.
      CIPHERS = { 16 => "aes-128-gcm", 24 => "aes-192-gcm", 32 => "aes-256-gcm" }.freeze

      attr_reader :id, :secret

      # @param id [String] what messages written with this key will name
      # @param secret [String] 16, 24 or 32 bytes; 32 unless there is a reason
      # @raise [ArgumentError] when the identifier or the key will not do
      def initialize(id, secret)
        @id = id.to_s
        check_id!
        @secret = Keys.from_bytes(secret)
        freeze
      end

      # What OpenSSL should be asked for to use this key.
      #
      # @return [String]
      def cipher_name = CIPHERS.fetch(@secret.bytesize)

      # The identifier as it goes on the wire.
      #
      # @return [String] UTF-8 bytes
      # @api private
      def id_bytes = @id.dup.force_encoding(Encoding::BINARY)

      def ==(other) = other.is_a?(EncryptionKey) && other.id == @id && other.secret == @secret
      alias eql? ==
      def hash = [@id, @secret].hash

      # The identifier and nothing else. The key must never appear in a log.
      def to_s = "EncryptionKey{id=#{@id}}"
      def inspect = "#<AceMQ::AMQP::EncryptionKey id=#{@id.inspect}>"

      private

      def check_id!
        if @id.empty?
          raise ArgumentError,
                "a key identifier cannot be empty: it is what a reader looks the key up by"
        end
        if @id.include?("\0")
          raise ArgumentError,
                "a key identifier cannot contain a null byte: it is read back out of the " \
                "framing by length, and a null is how one name becomes two"
        end

        length = @id.bytesize
        return if length <= MAX_ID_BYTES

        raise ArgumentError,
              "a key identifier is at most #{MAX_ID_BYTES} bytes and this one is #{length}. " \
              "It travels in front of every message, so it is a name rather than a description."
      end
    end

    # The keys a service can write with and the keys it can read with.
    #
    # Those are not the same set, and that asymmetry is the whole of key
    # rotation. A service writes with exactly one key — the current one — and
    # must be able to read with every key any message still in flight was
    # written with. Rotating means adding a key and making it current, while
    # the old ones stay readable until nothing written with them is left.
    #
    #   keys = Keyring.new(EncryptionKey.new("orders-2026-09", now),
    #                      EncryptionKey.new("orders-2026-06", june))
    #
    # The first key is the one that writes. The order rotation happens in is:
    # add the new key everywhere first, so every consumer can read it, and only
    # then make it current somewhere.
    #
    # A keyring is anything answering +current+ and +key_for+, so one backed by
    # a key management service is a small class rather than a fork of this one.
    # Implementations should cache: +key_for+ is called for every message
    # decoded, and a key service charged per call will notice.
    class Keyring
      # @param keys [Array<EncryptionKey>] at least one; the first writes
      def initialize(*keys)
        raise ArgumentError, "a keyring needs at least one key" if keys.empty?

        @lock = Mutex.new
        @keys = {}
        keys.each { |key| add(key) }
        @current = keys.first.id
      end

      # A keyring holding one key, which both writes and reads.
      #
      # @param id [String]
      # @param secret [String]
      # @return [Keyring]
      def self.of(id, secret) = new(EncryptionKey.new(id, secret))

      # @raise [ArgumentError] when +candidate+ is not a keyring
      # @api private
      def self.check!(candidate)
        return candidate if candidate.respond_to?(:current) && candidate.respond_to?(:key_for)

        raise ArgumentError,
              "#{candidate.inspect} is not a keyring: it needs current and key_for"
      end

      # Puts a key on the ring without making it the one used for writing.
      #
      # @param key [EncryptionKey]
      # @return [Keyring] self
      def add(key)
        unless key.is_a?(EncryptionKey)
          raise ArgumentError, "a keyring holds EncryptionKey, not #{key.class}"
        end

        @lock.synchronize do
          @keys[key.id] = key
          @current ||= key.id
        end
        self
      end

      # Makes a key the one new messages are written with.
      #
      # @param id [String]
      # @return [Keyring] self
      def use(id)
        @lock.synchronize do
          unless @keys.key?(id.to_s)
            raise ArgumentError, "there is no key #{id.to_s.inspect} on this keyring"
          end

          @current = id.to_s
        end
        self
      end

      # The key new messages are written with.
      #
      # Consulted per message, so a keyring that reloads from a secret store
      # can change what this returns and the next message uses the new key.
      #
      # @return [EncryptionKey]
      def current
        @lock.synchronize do
          key = @keys[@current]
          raise ConfigurationError, "this keyring has no current key" if key.nil?

          key
        end
      end

      # The key a message named.
      #
      # @param id [String] an identifier read out of a message
      # @return [EncryptionKey]
      # @raise [DecodeError] when this keyring does not hold it. Fatal rather
      #   than retryable: the same bytes name the same absent key next time
      def key_for(id)
        @lock.synchronize do
          key = @keys[id.to_s]
          # Naming what is held is safe — identifiers travel in the clear
          # anyway — and it is the difference between "the key was retired
          # while messages were still queued" and "this message came from a
          # service using a different keyring", which have different fixes.
          if key.nil?
            raise DecodeError,
                  "this message was encrypted with key #{id.to_s.inspect}, which is not on " \
                  "this keyring; it holds #{@keys.keys.sort.join(", ")}"
          end

          key
        end
      end

      # The identifiers, for a health endpoint or a line at start-up. Never the
      # keys.
      #
      # @return [Array<String>]
      def ids = @lock.synchronize { @keys.keys.sort }

      def to_s = "Keyring{current=#{@lock.synchronize { @current }}, holds=#{ids.join(", ")}}"
      def inspect = "#<AceMQ::AMQP::Keyring #{self}>"
    end

    # Encrypts whatever another codec produced, so the broker holds ciphertext.
    #
    #   keys = Keyring.of("orders-2026-09", Keys.generate)
    #   codec = EncryptedCodec.wrapping(JSONCodec.new, keys)
    #
    #   mq = Connection.open(url, codec: codec)
    #
    # It wraps a delegate rather than serialising anything itself, so choosing
    # a format and choosing to encrypt stay independent: JSON in, AES-GCM out,
    # and Avro just as well.
    #
    # == What is on the wire
    #
    #   0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
    #
    # Byte for byte what the Java library writes, and what it reads. *The key
    # identifier is in the message, in the clear.* That is deliberate, and it
    # is what makes rotation possible: a consumer reads which key a message
    # needs rather than assuming the current one, so a new key can be
    # introduced while messages written with the old one are still queued.
    # Putting it in an AMQP header instead would have been tidier and would
    # have lost it — headers are dropped by shovels, rewritten by federation,
    # and absent from a message recovered out of a backup, and a ciphertext
    # whose key nobody can name is gone.
    #
    # The header is authenticated but not encrypted: GCM binds it as associated
    # data, so an altered key identifier fails to decrypt rather than quietly
    # decrypting as something else.
    #
    # == What this does not do
    #
    # The broker can no longer read the message, and neither can the people who
    # operate it. *Decide what they do instead before turning this on*: a
    # dead-letter queue full of ciphertext is a queue nobody can triage, and
    # the answer is usually a small internal tool holding the keyring rather
    # than the management interface. {key_id_of} tells an operator which key a
    # message needs without holding any of them.
    #
    # Encryption is not authorisation. Every service holding the keyring can
    # read every message encrypted with those keys; the granularity is the key,
    # so separate audiences mean separate keys. Nor does it authenticate the
    # sender: anybody holding the key can write a message this codec will
    # happily decrypt.
    #
    # Nor does it hide the routing. Exchange, routing key, headers and message
    # size stay in the clear, and for many systems the routing key is the
    # sensitive part.
    class EncryptedCodec
      # Deliberately not +...+json+, whatever the plaintext underneath is.
      #
      # A +json+ suffix is a promise that the bytes on the wire are JSON, and
      # every JSON-aware consumer reads it that way. These bytes are
      # ciphertext. Naming them +json+ makes the JSON codec volunteer to decode
      # them, which is how a message ends up failing in a parser rather than
      # being refused by a codec that knows it cannot help.
      CONTENT_TYPE = "application/vnd.acemq.encrypted"

      # Marks the framing as this codec's, so a message from elsewhere is
      # refused rather than decrypted.
      MAGIC = 0xAE

      # Version 1. Present so a later framing can be told apart from this one
      # by its first two bytes.
      VERSION = 0x01

      # Fixed at 12, which is the size GCM is defined for. Any other length is
      # hashed into shape by the construction and loses the guarantee that two
      # different nonces stay different.
      NONCE_BYTES = 12

      # A 128-bit authentication tag, as Java and Go both write.
      TAG_BYTES = 16

      # The magic, the version and the length byte.
      PREFIX_BYTES = 3

      # @param delegate [#encode, #decode] the codec whose output is encrypted
      # @param keyring [#current, #key_for] the keys to write with and read with
      # @return [EncryptedCodec]
      def self.wrapping(delegate, keyring) = new(delegate, keyring)

      def initialize(delegate, keyring)
        @delegate = Codec.check!(delegate)
        @keyring = Keyring.check!(keyring)
        @delegate_takes_content_type = @delegate.method(:decode).arity != 1
        freeze
      end

      # The wrapped codec, whose output is what gets encrypted.
      attr_reader :delegate

      def content_type = CONTENT_TYPE

      # @param payload [Object] anything the delegate will encode
      # @return [String] the framed ciphertext, binary
      # @raise [EncodeError] when it cannot be encrypted
      def encode(payload)
        key = @keyring.current
        header = self.class.header_for(key)
        # A fresh nonce per message, from OpenSSL's own source. Reusing one
        # under GCM does not weaken the encryption, it forfeits it: two
        # messages under the same key and nonce leak their difference outright,
        # so a counter is not an option however tempting it looks.
        nonce = OpenSSL::Random.random_bytes(NONCE_BYTES)
        plaintext = @delegate.encode(payload)

        cipher = OpenSSL::Cipher.new(key.cipher_name)
        cipher.encrypt
        cipher.key = key.secret
        cipher.iv = nonce
        cipher.auth_data = header
        sealed = cipher.update(plaintext.to_s) + cipher.final
        header + nonce + sealed + cipher.auth_tag(TAG_BYTES)
      rescue OpenSSL::OpenSSLError => e
        # Without the payload. An exception that helpfully printed what could
        # not be encrypted would write the plaintext to the log, which is the
        # one place it was never supposed to reach.
        raise EncodeError,
              "could not encrypt a #{payload.class} with key #{key.id.inspect}: #{e.class}"
      end

      # @param body [String] the bytes as they arrived
      # @return [Object] what the delegate made of the plaintext
      # @raise [DecodeError] when it was not written by this codec, names a key
      #   this keyring does not hold, or will not decrypt
      def decode(body)
        bytes = body.to_s.dup.force_encoding(Encoding::BINARY)
        key_id = self.class.key_id_of(bytes)
        if key_id.nil?
          raise DecodeError,
                "this message was not written by EncryptedCodec: it does not start with the " \
                "framing this codec writes. A consumer configured to decrypt has been " \
                "pointed at a queue carrying plaintext."
        end

        header_length = PREFIX_BYTES + key_id.bytesize
        if bytes.bytesize < header_length + NONCE_BYTES + TAG_BYTES
          raise DecodeError,
                "this message is too short to hold a nonce and an authentication tag, so it " \
                "was truncated somewhere between being written and being read."
        end

        decode_inner(unseal(bytes, header_length, @keyring.key_for(key_id)))
      end

      # Only its own. Volunteering for anything else means trying to decrypt
      # plaintext and reporting the failure as a decode error, which sends
      # whoever is debugging it in precisely the wrong direction.
      def can_decode?(content_type)
        content_type.to_s.downcase.start_with?(CONTENT_TYPE)
      end

      # Reads which key a message needs, without needing the key.
      #
      # For the operator looking at a dead-letter queue they can no longer
      # read. The identifier is in the clear in front of the ciphertext, so
      # this answers "which key does this need?" from the bytes alone — which
      # is usually the question, because a queue full of undecryptable messages
      # is normally a key that was retired too early rather than anything wrong
      # with the messages.
      #
      # @param body [String] a message body
      # @return [String, nil] the key identifier, or nil when this was not
      #   written by this codec
      def self.key_id_of(body)
        bytes = body.to_s.dup.force_encoding(Encoding::BINARY)
        return nil if bytes.bytesize < PREFIX_BYTES
        return nil unless bytes.getbyte(0) == MAGIC && bytes.getbyte(1) == VERSION

        length = bytes.getbyte(2)
        return nil if length.zero? || bytes.bytesize < PREFIX_BYTES + length

        bytes.byteslice(PREFIX_BYTES, length).force_encoding(Encoding::UTF_8)
      end

      # The bytes GCM authenticates but does not encrypt.
      #
      # @api private
      def self.header_for(key)
        id = key.id_bytes
        [MAGIC, VERSION, id.bytesize].pack("C3") + id
      end

      def to_s = "EncryptedCodec{#{@delegate}, key=#{@keyring.current.id}}"
      def inspect = "#<AceMQ::AMQP::EncryptedCodec #{self}>"

      private

      # Decrypts, or says so without saying which of the two reasons it was.
      #
      # GCM authenticates as well as encrypts, so a wrong key and a tampered
      # ciphertext fail in exactly the same call. Nothing here tells them
      # apart, and nothing here should: an error that distinguished them would
      # be an oracle telling whoever is poking at the queue whether their guess
      # at the key was the part that was wrong.
      def unseal(bytes, header_length, key)
        header = bytes.byteslice(0, header_length)
        nonce = bytes.byteslice(header_length, NONCE_BYTES)
        from = header_length + NONCE_BYTES
        sealed = bytes.byteslice(from, bytes.bytesize - from - TAG_BYTES)
        tag = bytes.byteslice(bytes.bytesize - TAG_BYTES, TAG_BYTES)

        cipher = OpenSSL::Cipher.new(key.cipher_name)
        cipher.decrypt
        cipher.key = key.secret
        cipher.iv = nonce
        # The tag length is fixed by the slicing above rather than by
        # +auth_tag_len=+, which OpenSSL 3 refuses outright for GCM — the
        # length there is implied by the tag handed in. That is exactly why the
        # slicing has to be arithmetic rather than "whatever is left": OpenSSL
        # verifies against however many bytes it is given, so a message whose
        # tag had been trimmed to four would otherwise verify against a
        # sixteenth of the work. {#decode} refuses anything shorter than a
        # nonce and a full tag before this is reached.
        cipher.auth_tag = tag
        cipher.auth_data = header
        cipher.update(sealed) + cipher.final
      rescue OpenSSL::OpenSSLError
        raise DecodeError,
              "this message did not decrypt with key #{key.id.inspect}. Either that is not " \
              "the key it was written with, or it was altered after it was written."
      end

      # A codec that chooses by content type needs to be told one; a plain one
      # has no use for it. The inner codec's type is not on the wire — saying
      # "this is encrypted JSON" tells an observer more than they need — so a
      # composite delegate is handed nothing and tries its candidates in order.
      def decode_inner(plaintext)
        return @delegate.decode(plaintext, "") if @delegate_takes_content_type

        @delegate.decode(plaintext)
      end
    end
  end
end
