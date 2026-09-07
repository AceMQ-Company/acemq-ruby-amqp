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

RSpec.describe AceMQ::AMQP::Credentials do
  # The same string everywhere below, so that one search of a failing run's
  # output answers whether anything leaked it.
  let(:secret) { "hunter2" }

  it "carries a username and a secret" do
    credentials = described_class.of(username: "app", password: secret)

    expect(credentials.username).to eq("app")
    expect(credentials.secret).to eq(secret)
    expect(credentials).not_to be_empty
    expect(credentials).not_to be_token
  end

  it "carries a bearer token with no username" do
    credentials = described_class.token(secret)

    expect(credentials.username).to eq("")
    expect(credentials.secret).to eq(secret)
    expect(credentials).to be_token
  end

  it "is frozen, so nothing can be edited into it after it has been checked" do
    expect(described_class.of(username: "app", password: secret)).to be_frozen
  end

  # The reason this class exists rather than two strings. Every rendering is
  # asserted separately because a leak needs only whichever one was missed:
  # inspect is what p and a failing expectation use, to_s is what interpolation
  # uses, and the default Object#inspect prints every instance variable, which
  # would be the secret.
  describe "never rendering the secret" do
    let(:credentials) { described_class.of(username: "app", password: secret) }

    it "keeps it out of inspect" do
      expect(credentials.inspect).not_to include(secret)
      expect(credentials.inspect).to include("app", "[REDACTED]")
    end

    it "keeps it out of to_s" do
      expect(credentials.to_s).not_to include(secret)
    end

    it "keeps it out of string interpolation" do
      expect("logging in as #{credentials}").not_to include(secret)
    end

    it "keeps it out of %p, which is what a formatter reaches for" do
      expect(format("%p", credentials)).not_to include(secret)
    end

    it "keeps it out of a container that is dumped whole" do
      context = { broker: "amqps://broker:5671", credentials: credentials }

      expect(context.inspect).not_to include(secret)
      expect([credentials].to_s).not_to include(secret)
    end

    it "keeps a token out of every rendering too" do
      token = described_class.token(secret)

      expect(token.inspect).not_to include(secret)
      expect(token.to_s).not_to include(secret)
      expect(token.inspect).to include("token", "[REDACTED]")
    end
  end

  describe ".from_env" do
    around do |example|
      ENV["ACEMQ_SPEC_USERNAME"] = "app"
      ENV["ACEMQ_SPEC_PASSWORD"] = secret
      example.run
    ensure
      ENV.delete("ACEMQ_SPEC_USERNAME")
      ENV.delete("ACEMQ_SPEC_PASSWORD")
    end

    it "reads the named variables" do
      credentials = described_class.from_env(username_variable: "ACEMQ_SPEC_USERNAME",
                                             password_variable: "ACEMQ_SPEC_PASSWORD")

      expect(credentials).to eq(described_class.of(username: "app", password: secret))
    end

    it "says so when neither variable is set, rather than logging in as nobody" do
      expect do
        described_class.from_env(username_variable: "ACEMQ_SPEC_ABSENT_USER",
                                 password_variable: "ACEMQ_SPEC_ABSENT_PASSWORD")
      end.to raise_error(AceMQ::AMQP::ConfigurationError, /ACEMQ_SPEC_ABSENT_USER/)
    end
  end

  describe ".from_file" do
    def with_file(contents)
      file = Tempfile.new("acemq-credentials")
      file.write(contents)
      file.close
      yield file.path
    ensure
      file&.unlink
    end

    it "reads a secret written on its own" do
      with_file(secret) do |path|
        expect(described_class.from_file(path, username: "app"))
          .to eq(described_class.of(username: "app", password: secret))
      end
    end

    # An editor writes a newline whether anybody wanted one or not, and a
    # password with a newline on the end fails the login with an error that
    # says nothing about newlines.
    it "trims the newline an editor left on the end" do
      with_file("#{secret}\n") do |path|
        expect(described_class.from_file(path, username: "app").secret).to eq(secret)
      end
    end

    it "reads username:password when the file carries both" do
      with_file("app:#{secret}\n") do |path|
        expect(described_class.from_file(path)).to eq(described_class.of(username: "app",
                                                                         password: secret))
      end
    end

    it "refuses a file with no username when none was given" do
      with_file(secret) do |path|
        expect { described_class.from_file(path) }
          .to raise_error(AceMQ::AMQP::ConfigurationError, /holds no username/)
      end
    end

    it "refuses an empty file rather than logging in with an empty password" do
      with_file("\n") do |path|
        expect do
          described_class.from_file(path)
        end.to raise_error(AceMQ::AMQP::ConfigurationError, /is empty/)
      end
    end

    it "names the file it could not read" do
      expect { described_class.from_file("/no/such/secret") }
        .to raise_error(AceMQ::AMQP::ConfigurationError, %r{/no/such/secret})
    end
  end

  describe ".resolve" do
    it "passes credentials through" do
      credentials = described_class.of(username: "app", password: secret)

      expect(described_class.resolve(credentials)).to equal(credentials)
    end

    it "gives nil for nil, which is how no credentials are expressed" do
      expect(described_class.resolve(nil)).to be_nil
    end

    # The reason a block is accepted at all: a secret rotated underneath a
    # running process is only useful if something asks for it again.
    it "calls anything that answers call, at the moment it is asked" do
      reads = 0
      source = lambda do
        reads += 1
        described_class.of(username: "app", password: "#{secret}-#{reads}")
      end

      expect(described_class.resolve(source).secret).to eq("#{secret}-1")
      expect(described_class.resolve(source).secret).to eq("#{secret}-2")
    end

    # Naming the class rather than printing the value, because the thing most
    # likely to be passed here by mistake is the password itself.
    it "refuses something else without printing it" do
      expect { described_class.resolve(secret) }
        .to raise_error(AceMQ::AMQP::ConfigurationError) { |error| expect(error.message).not_to include(secret) }
    end
  end

  describe "#to_transport_options" do
    it "gives bunny a username and a password" do
      expect(described_class.of(username: "app", password: secret).to_transport_options)
        .to eq(username: "app", password: secret)
    end

    # Sending an empty username would overwrite whatever the URL said, which
    # takes a working connection apart to say nothing.
    it "leaves the username alone for a token" do
      expect(described_class.token(secret).to_transport_options).to eq(password: secret)
    end
  end
end
