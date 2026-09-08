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
require "securerandom"

require_relative "../ack"
require_relative "../codec"

module AceMQ
  module AMQP
    # Keeping large payloads off the broker.
    #
    # A scanned medical report is tens of megabytes. Putting it on a queue is
    # possible and is a mistake: it fills the broker's memory, it is copied to
    # every bound queue, it makes a dead-letter queue impossible to inspect, and
    # it turns a broker into a filesystem with worse tools. What travels instead
    # is a *claim check* — the payload goes to a store, and the message carries
    # the key.
    module Patterns
      # Where a payload too large for a broker actually goes.
      #
      # A store is anything answering three methods:
      #
      #   put(content)  # stores bytes, returns the key the message will carry
      #   get(key)      # the bytes, or nil when the store no longer holds them
      #   delete(key)   # removes them
      #
      # Three methods, so a store in front of S3, Azure Blob Storage, a
      # filesystem or a database table is a small class. Nothing here knows
      # about messaging: the store holds bytes under a key and hands them back,
      # and {ClaimCheckCodec} is what turns that into a claim check on the wire.
      #
      # A duck type rather than a class to inherit from, for the same reason as
      # {IdempotencyStore}: the store somebody actually wants is their own
      # object storage, and asking them to subclass something from a messaging
      # library to get there is asking for the wrong thing.
      #
      # == Retention is the part that goes wrong
      #
      # The store and the queue have different lifetimes, and nothing enforces a
      # relationship between them. A message replayed a month later carries a
      # key, and if the store expired that key the replay produces a message
      # nobody can read — *worse than a lost message, because it looks like a
      # message* and fails deep inside a consumer rather than visibly.
      #
      # So the store's retention must exceed every retention that could bring a
      # message back: queue TTLs, dead-letter queues, and however long somebody
      # might sit on a message before replaying it by hand. When in doubt,
      # longer.
      #
      # An implementation is shared by every publisher and consumer on a
      # connection, so it has to be safe to call from several threads.
      module ClaimCheckStore
      end

      # A claim-check store in a hash.
      #
      # *Not for production, and the reason is the point of the pattern.* The
      # payloads are held in the publisher's own memory — which is where they
      # were going to be anyway, so this takes them off the broker and does
      # nothing else. A claim check that does not outlive the process that wrote
      # it is a message nobody else can read, and every consumer in another
      # process gets "the claim check is not in the store". It is lost on
      # restart too, which turns every message still in a queue into one that
      # can never be read.
      #
      # It is genuinely useful in a test, where the publisher and the consumer
      # are the same process and the thing being proved is the framing rather
      # than the storage. Anything else wants a store the processes share.
      class InMemoryClaimCheckStore
        def initialize
          @contents = {}
          @lock = Mutex.new
        end

        # Stores a payload.
        #
        # @param content [String] the bytes
        # @return [String] the key the message will carry
        def put(content)
          key = SecureRandom.uuid
          # Copied, because the caller owns the string it handed over and a
          # codec is entitled to reuse a buffer. A store that keeps somebody
          # else's string is a store whose contents change after they were
          # stored. +b+ makes the copy and settles the encoding in one step.
          @lock.synchronize { @contents[key] = content.to_s.b }
          key
        end

        # Redeems a claim check.
        #
        # @param key [String] what the message carried
        # @return [String, nil] the payload, or nil when the store no longer
        #   holds it — which is retention having expired underneath a message
        #   that outlived it
        def get(key)
          @lock.synchronize { @contents[key.to_s] }&.dup
        end

        # Removes a payload.
        def delete(key)
          @lock.synchronize { @contents.delete(key.to_s) }
          nil
        end

        # How many payloads are held.
        def size = @lock.synchronize { @contents.size }

        # Empties the store, which is what a test between cases wants.
        def clear
          @lock.synchronize { @contents.clear }
          nil
        end
      end

      # A claim-check store on a filesystem.
      #
      # Useful where the filesystem is shared and durable — an NFS mount, a
      # persistent volume — and the honest middle ground between a hash and
      # object storage. On a container's local disk it is
      # {InMemoryClaimCheckStore} with extra steps: the consumer is on another
      # host and finds nothing.
      #
      # Object storage is the usual right answer, and a store in front of S3 or
      # Azure Blob Storage is three short methods. This one exists because
      # "write it to the mount everything already has" is a real deployment and
      # not a bad one.
      #
      # == Writes are atomic
      #
      # The payload is written to a temporary file and renamed into place.
      # Without that, a consumer fast enough to read the key before the writer
      # finished gets a truncated payload and a parse error somewhere
      # unhelpful — and messaging is exactly the arrangement that makes a
      # consumer that fast normal rather than unlikely. +rename+ within one
      # directory is atomic, so a reader sees the whole payload or no payload.
      #
      # A filesystem that cannot be written to raises whatever the filesystem
      # raised, unwrapped: +Errno::EACCES+ already names the path and the
      # problem, and a library exception in front of it would only hide both.
      class FilesystemClaimCheckStore
        # A key reaches the filesystem as a path segment, so it is checked
        # rather than trusted. Every key this store issues is a UUID; one
        # arriving from a message is whatever a publisher put there, and
        # <tt>../../etc/passwd</tt> is a key too.
        SAFE_KEY = /\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z/

        # @param directory [String] where payloads are written; created if it is
        #   not there
        def initialize(directory)
          @directory = directory.to_s
          FileUtils.mkdir_p(@directory)
        end

        # The directory payloads are written to.
        attr_reader :directory

        # Stores a payload, atomically.
        #
        # @param content [String] the bytes
        # @return [String] the key the message will carry
        def put(content)
          key = SecureRandom.uuid
          staging = File.join(@directory, "#{key}.partial")
          begin
            File.binwrite(staging, content.to_s)
            # Moved into place rather than written in place, so a reader either
            # finds the whole payload or finds nothing.
            File.rename(staging, File.join(@directory, key))
          ensure
            # A failed write leaves a .partial nobody will ever claim. Removing
            # it here is the difference between a directory that fills with
            # debris and one that does not, and rm_f on a name the rename has
            # already taken away is a no-op rather than an error.
            FileUtils.rm_f(staging)
          end
          key
        end

        # Redeems a claim check.
        #
        # @param key [String] what the message carried
        # @return [String, nil] the payload, or nil when the store no longer
        #   holds it
        def get(key)
          File.binread(path_for(key))
        rescue Errno::ENOENT
          # Absent rather than failed: a key the store no longer holds is a
          # retention answer, and the codec turns it into a message that
          # explains itself.
          nil
        end

        # Removes a payload.
        def delete(key)
          FileUtils.rm_f(path_for(key))
          nil
        end

        private

        def path_for(key)
          key = key.to_s
          unless SAFE_KEY.match?(key)
            # Fatal rather than retryable: a key that is not one this store
            # issued will not become one on the fourth attempt, and a message
            # carrying it has to stop rather than circle.
            raise FatalError,
                  "#{key.inspect} is not a key this store issued. A key becomes a path " \
                  "segment, so one arriving from a message is checked rather than trusted."
          end

          File.join(@directory, key)
        end
      end

      # A codec that puts a large payload aside and sends the key instead.
      #
      #   store = Patterns::FilesystemClaimCheckStore.new("/mnt/claims")
      #   checked = Patterns::ClaimCheckCodec.wrapping(JSONCodec.new, store)
      #
      #   mq = Connection.open(url: url, codec: checked)
      #   mq.publish(document, to: "document.stored")
      #
      # == Only when it is worth it
      #
      # Below {DEFAULT_THRESHOLD} the payload travels inline, exactly as it
      # would without this codec. That matters more than it sounds: offloading a
      # two-hundred-byte message turns one broker round trip into a store round
      # trip *and* a broker round trip, so an unconditional claim check makes
      # the common case slower to fix the rare one.
      #
      # The framing therefore says which of the two it is, and a consumer
      # handles both without being told. That is what allows the threshold to be
      # changed, or this codec to be introduced, without a flag day: messages
      # written before the change are still readable after it.
      #
      # == What is on the wire
      #
      #   0xAC  0x01  0x00  payload   inline, and identical to what the delegate wrote
      #   0xAC  0x01  0x01  key       a claim check
      #
      # Three bytes, and the third is how a consumer decides whether it is
      # holding a payload or a reference to one. Nothing else distinguishes
      # them, which is why the framing rather than a header is the contract: a
      # header can be stripped by a shovel or a federation link, and the body
      # cannot.
      #
      # These are the same three bytes the Java, and any other AceMQ, claim
      # check writes, and the key is the store's key as bare UTF-8 — not a URI,
      # not a scheme, nothing wrapped around it. A Ruby consumer pointed at the
      # same store therefore reads a document a Java publisher checked in, and
      # the other way round.
      #
      # The content type is the delegate's, unchanged — unlike encryption, where
      # the bytes really are something else. A claim-checked message is still a
      # document; it is a document that is somewhere else, and a consumer that
      # lacks the store gets a clear failure rather than a parser error.
      class ClaimCheckCodec
        # Marks this codec's framing.
        MAGIC = 0xAC

        # The framing version. One byte, so a later shape can be introduced
        # without every consumer having to be redeployed the same afternoon.
        VERSION = 0x01

        # The payload is in the message.
        INLINE = 0x00

        # The message carries a key, and the payload is in the store.
        CHECKED = 0x01

        # How many bytes the framing takes.
        HEADER = 3

        # Below this, payloads travel inline.
        #
        # 64 KiB: comfortably above an ordinary event and comfortably below the
        # size at which a broker starts to care. RabbitMQ will accept far
        # larger, which is the problem — nothing refuses a 40 MB message, it
        # simply makes everything worse afterwards.
        DEFAULT_THRESHOLD = 64 * 1024

        # @param delegate [#encode] the codec that turns payloads into bytes
        # @param store [ClaimCheckStore] where large payloads go
        # @param threshold [Integer] payloads of at least this many bytes are
        #   offloaded; zero offloads everything, which is occasionally what a
        #   store-backed audit trail wants
        # @return [ClaimCheckCodec]
        def self.wrapping(delegate, store, threshold: DEFAULT_THRESHOLD)
          new(delegate, store, threshold: threshold)
        end

        # @see .wrapping
        def initialize(delegate, store, threshold: DEFAULT_THRESHOLD)
          @delegate = Codec.check!(delegate)
          @store = store
          unless threshold.is_a?(Integer) && !threshold.negative?
            raise ArgumentError, "a threshold cannot be negative, was #{threshold.inspect}"
          end

          @threshold = threshold
          freeze
        end

        # The wrapped codec, whose output is what gets stored or inlined.
        attr_reader :delegate

        # Bytes at or above which a payload is offloaded.
        attr_reader :threshold

        # The delegate's. A claim-checked document is still a document.
        def content_type = @delegate.content_type

        # Encodes a payload, offloading it when it is large enough to be worth
        # it.
        #
        # @param payload [Object]
        # @return [String] the framed bytes to publish
        def encode(payload)
          encoded = @delegate.encode(payload).to_s
          # bytesize rather than length. A string of 40,000 characters with an
          # accent in it is more than 40,000 bytes on the wire, and the
          # threshold is a statement about what the broker has to carry.
          return frame(INLINE, encoded) if encoded.bytesize < @threshold

          frame(CHECKED, @store.put(encoded).to_s)
        end

        # Decodes a message, fetching the payload when it was offloaded.
        #
        # @param body [String] the bytes as they arrived
        # @param content_type [String] what the sender said, when the delegate
        #   wants to know
        # @return [Object] the payload
        # @raise [DecodeError] when the store no longer holds the payload
        def decode(body, content_type = "")
          body = body.to_s
          # Written before this codec was introduced, or by a publisher that
          # does not use it. Reading it as the delegate would is the only useful
          # answer, and it is what makes adding a claim check to a live queue
          # safe.
          return delegate_decode(body, content_type) unless self.class.framed?(body)

          rest = body.byteslice(HEADER, body.bytesize - HEADER)
          rest = fetch(rest.force_encoding(Encoding::UTF_8)) if body.getbyte(2) == CHECKED

          delegate_decode(restore_encoding(rest), content_type)
        end

        # Whatever the delegate accepts. A claim check does not change what the
        # message is.
        def can_decode?(content_type) = @delegate.can_decode?(content_type)

        # Reads the key a message refers to, without fetching it.
        #
        # For the operator looking at a dead-letter queue: which object does
        # this need, and is it still in the store? Answering that from the
        # message alone is the difference between a five-minute check and
        # restoring a backup.
        #
        # @param body [String] a message body
        # @return [String, nil] the key, or nil when the payload travelled
        #   inline or this codec did not write the message
        def self.key_of(body)
          body = body.to_s
          return nil unless framed?(body) && body.getbyte(2) == CHECKED

          body.byteslice(HEADER, body.bytesize - HEADER).force_encoding(Encoding::UTF_8)
        end

        # Whether a body carries this codec's framing.
        #
        # @param body [String]
        # @return [Boolean]
        def self.framed?(body)
          body = body.to_s
          return false if body.bytesize < HEADER

          body.getbyte(0) == MAGIC && body.getbyte(1) == VERSION &&
            [INLINE, CHECKED].include?(body.getbyte(2))
        end

        def to_s = "ClaimCheckCodec(#{@delegate.class.name}, above #{@threshold} bytes)"

        private

        def fetch(key)
          content = @store.get(key)
          return content if content

          # Fatal rather than retryable, and said at length because the cause is
          # never where somebody looks first: the payload was removed while a
          # message referring to it was still deliverable.
          raise DecodeError,
                "the claim check #{key.inspect} is not in the store, so this message " \
                "cannot be read. The payload was removed while a message referring to " \
                "it was still deliverable -- the store's retention has to outlast every " \
                "queue, every dead-letter queue, and any replay somebody might do by hand."
        end

        # A codec that chooses by content type needs to be told it; a plain one
        # has no use for it. Asked of the delegate rather than assumed, so
        # wrapping a {CompositeCodec} works as well as wrapping a plain one.
        def delegate_decode(body, content_type)
          return @delegate.decode(body) if @delegate.method(:decode).arity == 1

          @delegate.decode(body, content_type)
        end

        # Slicing bytes out of a frame, or reading them back from a store,
        # produces a binary string. Handing that to a text codec would give the
        # caller back a payload whose encoding depends on whether it was
        # offloaded, so the label is restored when the bytes support it. Only
        # the label changes; the bytes are untouched, which is what keeps a
        # binary payload safe.
        def restore_encoding(bytes)
          text = bytes.dup.force_encoding(Encoding::UTF_8)
          text.valid_encoding? ? text : bytes
        end

        def frame(kind, rest)
          framed = String.new(encoding: Encoding::BINARY)
          framed << MAGIC << VERSION << kind
          framed << rest.b
        end
      end
    end
  end
end
