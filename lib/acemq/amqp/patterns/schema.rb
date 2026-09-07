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

require "digest"

module AceMQ
  module AMQP
    # Remembering what a message used to look like.
    #
    # A producer and a consumer have to agree about what a message means, and
    # they are deployed on different afternoons. A registry lets the message
    # carry a small identifier instead of its whole shape, and the consumer look
    # the shape up — which is what allows a producer to add a field without
    # every consumer being redeployed the same day.
    module Patterns
      # A lookup that found nothing.
      #
      # Its own class rather than a nil: a consumer reading a message whose
      # schema it cannot find has a real problem — usually a producer registered
      # against a different registry — and carrying on with an empty definition
      # would turn that into a silently wrong message rather than an error.
      class SchemaNotFound < StandardError; end

      # One version of a message's shape.
      SchemaDefinition = Struct.new(
        :id, :subject, :version, :format, :definition, :fingerprint, :registered_at,
        keyword_init: true
      ) do
        def to_s = "#{subject} v#{version} (#{format}, id #{id})"
      end

      # Message shapes, kept in this process.
      #
      # A registry is anything answering +register+, +by_id+, +latest+ and
      # +versions+. This one is for tests and for a single service that wants
      # the shape of the thing. It is not a registry in the sense that matters:
      # nothing is shared between processes, so a consumer cannot look up a
      # schema a producer registered elsewhere, which is the entire point of
      # having one. Use a database-backed registry, or Confluent's, for anything
      # real — the wire framing this library uses is compatible with theirs.
      #
      # Nothing here puts anything on the wire. Which header carries a schema
      # identifier is a cross-language contract, and it is not one AceMQ has
      # agreed yet; a header invented here would be one the Java, Go, .NET and
      # Python libraries could not read, which is worse than none.
      class InMemorySchemaRegistry
        def initialize
          @lock = Mutex.new
          @by_id = {}
          @by_subject = Hash.new { |subjects, name| subjects[name] = [] }
          @by_fingerprint = {}
          @next_id = 1
        end

        # Records a schema and returns it with an identifier.
        #
        # The same definition registered twice returns the same identifier
        # rather than making a second version. Without that, a service that
        # registers its schemas on every start adds a version per restart, and
        # a week later the subject has three hundred identical versions.
        #
        # @param subject [String] groups the versions of one message type,
        #   conventionally the type itself: "order.placed"
        # @param format [String] "avro", "protobuf", "json-schema" — whatever
        #   the schema is written in. Nothing here interprets it.
        # @param definition [String] the schema
        # @return [SchemaDefinition]
        def register(subject, format, definition)
          if subject.to_s.empty? || definition.to_s.empty?
            raise ArgumentError, "a schema needs a subject and a definition"
          end

          fingerprint = Patterns.fingerprint(definition)
          @lock.synchronize do
            @by_fingerprint["#{subject}/#{fingerprint}"] ||= add(subject, format, definition,
                                                                 fingerprint)
          end
        end

        # The schema an identifier names.
        #
        # @raise [SchemaNotFound]
        def by_id(id)
          @lock.synchronize { @by_id[id] } ||
            raise(SchemaNotFound, "no schema with id #{id}")
        end

        # The newest version of a subject.
        #
        # @raise [SchemaNotFound]
        def latest(subject)
          @lock.synchronize { @by_subject[subject].last } ||
            raise(SchemaNotFound, "no schema for subject #{subject.inspect}")
        end

        # Every version of a subject, oldest first. Empty when there are none,
        # because "what versions are there" has a sensible answer for a subject
        # nobody has registered and "which schema is this" does not.
        def versions(subject)
          @lock.synchronize { @by_subject[subject].dup }
        end

        private

        def add(subject, format, definition, fingerprint)
          schema = SchemaDefinition.new(
            id: @next_id, subject: subject, version: @by_subject[subject].size + 1,
            format: format, definition: definition, fingerprint: fingerprint,
            registered_at: Time.now.utc
          )
          @next_id += 1
          @by_id[schema.id] = schema
          @by_subject[subject] << schema
          schema
        end
      end

      # Hashes a schema definition.
      #
      # SHA-256 of the exact bytes, so two definitions differing only in
      # whitespace hash differently and count as different schemas. Normalising
      # would need a parser per format, and a registry that quietly treated two
      # definitions as one because it mis-parsed them would be worse than one
      # that is strict.
      def self.fingerprint(definition)
        Digest::SHA256.hexdigest(definition.to_s)
      end
    end
  end
end
