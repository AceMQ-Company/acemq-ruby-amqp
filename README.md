# AceMQ for Ruby

[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-3.1%2B-CC342D)](#requirements)
[![brokers](https://img.shields.io/badge/broker-RabbitMQ-lightgrey)](#requirements)

A Ruby client for AceMQ messaging over AMQP, speaking the same wire contract as
the [Java](https://github.com/AceMQ-Company/acemq-java-amqp),
[Go](https://github.com/AceMQ-Company/acemq-go-amqp),
[.NET](https://github.com/AceMQ-Company/acemq-dotnet-amqp) and
[Python](https://github.com/AceMQ-Company/acemq-python-amqp) libraries: the same
reserved headers, the same defaults, the same retry arithmetic. A Ruby consumer
reads what a Java producer writes, and the fixtures the Java implementation
produces pin that rather than leaving it to be discovered in production.

> **Status: in build.** The contract layer — envelope, retry schedule, naming,
> acknowledgement model — is implemented and tested against the shared
> fixtures. The transport is next. Nothing is published to RubyGems yet.

## What is here today

```ruby
require "acemq/amqp"

include AceMQ::AMQP

# What travels with a message, and what another language will read back.
envelope = Envelope.new(type: "order.placed.v2", origin: "checkout@pod-7")
envelope.to_headers("order.placed")

# The same schedule Java, Go, .NET and Python produce for the same policy.
policy = RetryPolicy.exponential(5, 1, 60)
policy.schedule                      # => [1.0, 2.0, 4.0, 8.0]

Naming.dead_letter_queue("orders.new")   # => "orders.new.dlq"
```

The contract layer has **no runtime dependencies**. Reading an AceMQ envelope
should not require installing a broker client, so `bunny` arrives with the
transport rather than as a condition of using any of this.

## What is identical, and what is not

**Identical**, because a message crosses languages: the reserved header names
and their types, the defaults applied when they are absent, the retry schedule
arithmetic, the `{queue}.dlq` / `{queue}.parked` / `{queue}.retry.{delay}`
naming, and the rules for giving up.

**Not identical**, deliberately: the API shape. Go gets `ctx`, .NET gets
`IAsyncEnumerable`, Python gets dataclasses, and Ruby gets keyword arguments,
frozen value objects and `?` predicates. Forcing a Java shape onto Ruby
produces a library nobody enjoys using. The contract is portable; the
ergonomics are native.

### The envelope

| Header | |
|---|---|
| `x-acemq-id` | The message identifier, and the default idempotency key |
| `x-acemq-type` | The logical type, falling back to the routing key |
| `x-acemq-version` | Schema version, from 1 |
| `x-acemq-correlation` | Defaults to the id, so a chain has something to copy |
| `x-acemq-causation` | The message that caused this one |
| `x-acemq-attempt` | Delivery attempt, from 1 |
| `x-acemq-first-seen` | Epoch **milliseconds** of the first publish |
| `x-acemq-origin` | `service@host` |
| `x-acemq-error` | Why it was dead-lettered |
| `x-acemq-claim` | Where the payload is, when it is stored outside the message |

Application headers are kept apart from these. A reserved name in your own
headers raises rather than being dropped — silently discarding a header
somebody set is worse than saying no — and unknown `x-acemq-` names from a
newer version of another language's library are not handed back as yours.

### Retry

```ruby
policy = RetryPolicy.exponential(5, 1, 60).give_up_after(6 * 3600)
```

`schedule` shows the delays without jitter, which is what to read when deciding
whether a policy is the one you meant. Jitter moves a delay **both ways**:
one-sided jitter only ever delays, which turns a thundering herd into a slower
thundering herd.

Giving up on **age** as well as attempts is the honest limit when a queue has
been paused — a message can be on attempt one and four days old.

## Requirements

Ruby 3.1 or newer. RabbitMQ for the transport, once it lands.

## Development

```bash
bundle install
bundle exec rspec        # the contract, no broker needed
bundle exec rubocop
```

The fixtures under `spec/fixtures/` are produced by the Java implementation and
shared with Go, .NET and Python. They are the definition of "the same wire
contract", and they are checked here rather than assumed.

## Licence

Apache-2.0. RabbitMQ is a trademark of Broadcom Inc.; this project is not
affiliated with it.
