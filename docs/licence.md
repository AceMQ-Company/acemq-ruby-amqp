# Licence and warranty

AceMQ for Ruby is [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
You may use it in production, commercially, without asking and without paying.
The text as shipped is [here](LICENSE).

## No warranty

The licence disclaims warranties and limits liability — sections 7 and 8. In
plain terms: this is provided as it is, and if it loses your messages that is
your risk to have taken.

That is not a formality to skim. It is a young library: pre-1.0, with an API
still free to change, and its own documentation says which parts have been
proven against a real broker and which have not. Read
[testing](testing.md) for what runs without one and what only an integration
spec can show, and [reliability](reliability.md#what-is-not-here) for what this
library does not do for you.

If you need somebody accountable for it working, that is what
[Enterprise support](https://acemq.com) is for. The library is complete and free
without it, and is not crippled to sell it.

## What you must do

Keep the licence and the copyright notice with any copy or derivative, and state
what you changed. That is the whole obligation.

## Dependencies

**The gem has no runtime dependencies.** Reading an AceMQ envelope, checking a
retry schedule or building a topology requires nothing but Ruby's standard
library.

| | |
|---|---|
| `bunny` | MIT. Michael Klishin and contributors. Required **lazily**, by the transport, at the moment a connection is opened — and named in the error if it is not there. It goes in your own Gemfile. |

Everything under `spec/` — RSpec, RuboCop, YARD — is development only and no
concern of anything that installs this gem.

A process that only builds envelopes never loads a broker client. See
[the overview](index.md).

## Trademarks

RabbitMQ is a trademark of Broadcom Inc. and/or its subsidiaries. Ruby is a
trademark of Yukihiro Matsumoto. AceMQ is an independent project, affiliated
with and endorsed by neither.
