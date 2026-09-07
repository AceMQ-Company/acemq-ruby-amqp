# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [0.2.0] — 2026-09-07

> ### ⚠ Migrating: a retry rung now returns through `acemq.retry`
>
> A rung queue is declared with `x-dead-letter-exchange` set to `acemq.retry`,
> where 0.1.0 used `""` (the default exchange). **A rung that already exists
> with the old argument cannot be redeclared with the new one** — AMQP forbids
> changing a queue's arguments in place, so the declare is refused with
> `PRECONDITION_FAILED`.
>
> This only affects a broker that has already run 0.1.0 with a retry policy
> whose delays reach 30 seconds. Delete the `{queue}.retry.*` queues and let
> them be declared again; they hold nothing but messages waiting to be retried,
> and anything in them at the time is lost, so drain first if that matters.
>
> The change exists because the other four libraries all use `acemq.retry`, and
> two services on one queue that disagree about a rung's arguments cannot both
> consume it.

### Added

- **TLS and credentials.** `amqps://` with verification on, a custom CA, client
  certificates for mutual TLS, and credentials supplied apart from the URL so a
  password need never be embedded in a connection string. `Credentials` renders
  as `[REDACTED]` through `inspect` and `to_s`. Verification can be turned off
  only through `Security.without_verifying_the_broker(because:)`, which demands
  a reason.
- **Interceptors.** An ordered, composable seam around publishing and handling
  with writable contexts, registered by object or block, so cross-cutting work
  is written once rather than in every handler.
- **Telemetry and health.** Metric names identical to the Go, Java and .NET
  libraries; a three-method observer contract; an in-memory registry with a
  `to_prometheus` renderer and no dependency on any metrics gem. `mq.health`
  answers up, down or degraded, and its broker probe cleans up after itself.
- **The pattern library**: idempotency, outbox, request/reply, replay, ordered
  handling, consumer groups, routing slips, pipelines, a schema registry and
  stream reading.
- **A way to install it.** A release workflow publishing to RubyGems through
  trusted publishing, and a docs workflow publishing the reference to GitHub
  Pages. Until 0.2.0 the `v0.1.0` tag existed and nobody could install it.

### Changed

- **A rung returns through the named `acemq.retry` exchange** rather than the
  default exchange, with one binding `{queue} -> acemq.retry -> {queue}`, and
  dead letters route through `acemq.dlx`. See the migration note above.
- **A replay resets `x-acemq-attempt` to 1** unless `restart: false`. A message
  dead-lettered on its last attempt would otherwise be dead-lettered again
  before a handler ever saw it.
- `Topology#retry_ladder` now requires the source queue to be declared in the
  same topology, because it adds a binding to it.

### Fixed

- **bunny does not verify the broker's certificate when given an `amqps://`
  URL.** `AMQ::Settings.configure` merges `verify: false`, which bunny reads as
  an instruction and turns into `VERIFY_NONE` — measured against a broker with a
  private authority as `verify_result=19`, connection opened anyway. Every mode
  now sets `verify_peer` explicitly.
- `Connection#close` cancelled consumers with `consumers.each(&:cancel)`, so one
  consumer raising left the socket open.
- `replay(from: dlq)` onto its own queue looped for ever; it is now refused.
