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

module AceMQ
  module AMQP
    # A security setting that cannot be honoured.
    #
    # Raised while a connection is being configured rather than while it is
    # being used, and deliberately not a {TransportError}: a certificate file
    # that is not there will not be there on the next attempt either, so
    # anything that retries transport failures must not retry this one.
    #
    # It lives here rather than beside the other errors in +transport.rb+
    # because {Credentials} and {Security} are the files that raise it, and
    # neither of them is allowed to know that a transport exists.
    class ConfigurationError < StandardError; end

    # A username and a secret the broker will accept.
    #
    # This exists so a password does not have to live in a connection string.
    # A URL is the one piece of configuration that gets printed: it goes in
    # error messages, in structured logs, in +ps+ output when it arrives as an
    # argument, and in whatever the deployment tool echoes back. Once a password
    # has been through any of those it has to be rotated, and the rotation is
    # the expensive part. Credentials passed separately never take that trip.
    #
    #   mq = Connection.open("amqp://broker:5672",
    #                        credentials: Credentials.from_env)
    #
    # The second reason this is a type rather than two strings is {#inspect}. A
    # bare string reaches a log the moment anything dumps the object holding it
    # — an exception, a +p+ left in during debugging, a structured logger that
    # serialises its context. This class renders as its username and the word
    # +[REDACTED]+, whichever of +inspect+, +to_s+ or +%p+ asks, so the accident
    # produces something useless rather than something to rotate.
    class Credentials
      # What replaces the secret in every rendering of this object.
      REDACTED = "[REDACTED]"

      # The account name, or an empty string for a bearer token.
      attr_reader :username

      # @param username [String, nil]
      # @param password [String, nil] the password or token
      def initialize(username: nil, password: nil)
        @username = username.to_s
        @secret = password.to_s
        freeze
      end

      # A username and password.
      def self.of(username:, password:)
        new(username: username, password: password)
      end

      # A bearer token with no username, which is how OAuth 2 against RabbitMQ
      # is presented: the token is the password and the username is ignored.
      def self.token(token)
        new(password: token)
      end

      # Credentials read from the environment.
      #
      # The environment is where an orchestrator puts a secret, and reading it
      # here rather than interpolating it into a URL somewhere in a start-up
      # script is the difference between a password that stays in the process
      # and one that ends up in the command line.
      #
      # @param username_variable [String] the variable holding the account name
      # @param password_variable [String] the variable holding the secret
      # @raise [ConfigurationError] when neither variable is set
      def self.from_env(username_variable: "ACEMQ_USERNAME",
                        password_variable: "ACEMQ_PASSWORD")
        username = ENV.fetch(username_variable, nil)
        password = ENV.fetch(password_variable, nil)
        if username.nil? && password.nil?
          raise ConfigurationError,
                "neither #{username_variable} nor #{password_variable} is set in the " \
                "environment, so there are no credentials to read"
        end

        new(username: username, password: password)
      end

      # Credentials read from a file, which is how a mounted Kubernetes secret
      # or a Docker secret arrives.
      #
      # The file holds the password alone, or +username:password+ when it
      # carries both. Trailing whitespace is trimmed, because a file written by
      # an editor almost always ends in a newline, and a password with a newline
      # on the end is refused by the broker with an error that says nothing
      # about newlines.
      #
      # Read now rather than remembered as a path: a caller that wants the file
      # re-read on every connection — a sidecar rotating the secret underneath
      # it — passes a block instead, which {resolve} calls at connection time.
      #
      # @param path [String]
      # @param username [String, nil] when the file holds the secret alone
      # @raise [ConfigurationError] when the file cannot be read or says nothing
      def self.from_file(path, username: nil)
        contents = begin
          File.read(path)
        rescue SystemCallError, IOError => e
          raise ConfigurationError, "cannot read the credentials file #{path}: #{e.message}"
        end

        from_file_contents(contents.sub(/[ \t\r\n]+\z/, ""), path, username)
      end

      # Whatever a caller supplied, as {Credentials} or nil.
      #
      # A block or anything else answering +call+ is invoked here, at the moment
      # the connection is made. That is the whole point of accepting one: a
      # password rotated by a sidecar is only useful if something asks for it
      # again rather than remembering what it read at start-up.
      #
      # @param source [Credentials, #call, nil]
      # @raise [ConfigurationError] when it is none of those
      def self.resolve(source)
        case source
        when nil then nil
        when Credentials then source
        else
          resolved = source.respond_to?(:call) ? source.call : source
          unless resolved.is_a?(Credentials)
            raise ConfigurationError,
                  "credentials must be an AceMQ::AMQP::Credentials, or something " \
                  "answering #call that returns one, not #{describe(source)}"
          end

          resolved
        end
      end

      # A short description of a value for an error message, naming its class
      # rather than printing it — the thing that was passed in place of
      # credentials is quite likely to be the password itself.
      #
      # @api private
      def self.describe(value)
        value.nil? ? "nil" : "a #{value.class}"
      end

      # Splits what a credentials file held.
      #
      # @api private
      def self.from_file_contents(contents, path, username)
        raise ConfigurationError, "the credentials file #{path} is empty" if contents.empty?
        return of(username: username, password: contents) unless username.nil?

        account, separator, secret = contents.partition(":")
        if separator.empty?
          raise ConfigurationError,
                "the credentials file #{path} holds no username and none was given; " \
                "write it as username:password, or pass username: to from_file"
        end

        of(username: account, password: secret)
      end

      private_class_method :from_file_contents

      # The password or token. Do not log the result.
      #
      # Named +secret+ rather than +password+ because it is also where a bearer
      # token lives, and a reader who sees +password+ on a token is entitled to
      # assume the token was the wrong thing to put there.
      attr_reader :secret

      # Whether these carry nothing at all.
      def empty? = username.empty? && secret.empty?

      # Whether these are a token rather than an account.
      def token? = username.empty? && !secret.empty?

      # The bunny options these describe.
      #
      # The username is left out when there is none, rather than sent as an
      # empty string: a token is authenticated by the token, and overwriting
      # whatever the URL said with "" would take a working connection apart for
      # no reason.
      #
      # @return [Hash]
      def to_transport_options
        options = {}
        options[:username] = username unless username.empty?
        options[:password] = secret unless secret.empty?
        options
      end

      # Two credentials are the same when both halves match.
      def ==(other)
        other.is_a?(Credentials) && other.username == username && other.secret == secret
      end
      alias eql? ==

      def hash = [self.class, username, secret].hash

      # Never the secret. See the class comment for why this matters more than
      # it looks like it should.
      def inspect
        "#<AceMQ::AMQP::Credentials #{self}>"
      end

      # Never the secret either. Both are overridden because interpolation uses
      # one and +p+ uses the other, and a leak needs only whichever was missed.
      def to_s
        token? ? "token=#{REDACTED}" : "username=#{username.inspect} secret=#{REDACTED}"
      end
    end
  end
end
