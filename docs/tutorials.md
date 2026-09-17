# Tutorials

Step by step, in order, each one ending with something that runs.

The [guide](index.md) explains how a thing works and why it is that way. These
are the other shape: start with nothing, finish with a working service, and
understand what you typed by the end rather than before the beginning.

| | | | |
|---|---|---|---|
| 1 | [Your first message](tutorial-first-message.md) | Connect, declare, publish, consume — one message and then a thousand | 15 min |
| 2 | [Surviving failure](tutorial-surviving-failure.md) | Retries that do not block, dead letters, and replaying them | 25 min |
| 3 | [Never processing twice](tutorial-exactly-once.md) | Idempotency, the outbox, and why "exactly once" is a lie | 25 min |
| 4 | [Seeing what happens](tutorial-observability.md) | Metrics and traces, and reading them when something is wrong | 20 min |

Each builds on the one before it, and each is a single file you can run with
`ruby`. Nothing is left as an exercise.

They are the same four subjects taught by the Java, .NET, Go and Python
tutorials, so tutorial 3 is about the same thing in every language. The code is
not a translation: Ruby gets keyword arguments, blocks that return an `Ack`, and
frozen value objects, for the reasons the [overview](index.md#what-is-identical-and-what-is-not)
gives.

## Before you start

```ruby
# Gemfile
source "https://rubygems.org"

gem "acemq-amqp", git: "https://github.com/AceMQ-Company/acemq-ruby-amqp"
gem "bunny", "~> 2.23"
```

```bash
bundle install
```

Ruby 3.1 or newer. `bunny` is a separate gem because the library requires it
lazily, at the moment a connection is opened — a process that only builds
envelopes or checks a retry schedule needs no broker client at all. Nothing is
published to rubygems.org yet, which is why that is a git source; the
[releasing section of the README](https://github.com/AceMQ-Company/acemq-ruby-amqp#releasing)
says what has to happen before it is not.

### You need a broker

All four tutorials use a real RabbitMQ:

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

The management UI is then at <http://localhost:15672>, `guest` / `guest`, and
tutorials 2 and 4 ask you to look at it.

This is a place Ruby has less than Java does, and it is worth knowing rather than
discovering. The Java library ships a test artifact with an in-memory broker
behind a `memory://` URL, so its first two tutorials need no Docker. **This gem
ships no test transport.** What it ships instead is the seam: `Connection.new`
takes a transport object rather than a URL, and anything answering the handful of
methods in that seam is one. The library's own `spec/support/fake_transport.rb` and
`spec/support/loopback_transport.rb` are two such objects, they are what the
specs run against, and they are copyable — but they are not in the gem, so a
tutorial cannot ask you to `require` them. See
[testing without a broker](testing.md#the-seam) once you have finished here.

## If you would rather read code

The [examples repository](https://github.com/AceMQ-Company/acemq-ruby-amqp-examples)
holds runnable programs, verified by CI on every commit. Tutorials teach;
examples demonstrate. Start here, go there when you want to see a whole system
rather than one idea.

## Where these stop

The tutorials cover the four things every service needs. The guide covers the
rest, and two subjects in particular have pages of their own rather than a
tutorial: [request and reply](request-reply.md), because it is a shape to reach
for rarely and understand fully, and [streams](streams.md), because everything
tutorial 2 teaches about failure works differently on one.
