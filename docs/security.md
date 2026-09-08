# Security

An `amqps://` URL is encrypted **and the broker is verified** against the
machine's trust store. That is the whole configuration for a broker whose
certificate came from a public authority:

```ruby
mq = Connection.open("amqps://broker.example.com:5671",
                     credentials: Credentials.from_env)
```

That sentence is the reason this page is long. It is not what the underlying
driver does on its own.

## bunny does not verify the broker when it is given a URL

Not "verifies weakly" — does not verify.

A URL string handed to bunny is parsed by `AMQ::Settings`, which merges in its
own defaults, and one of those defaults is `verify: false`. bunny reads that as
an explicit instruction and sets `VERIFY_NONE`. So:

```ruby
Bunny.new("amqps://broker.example.com:5671")   # not this library
```

encrypts the traffic, accepts a certificate the connecting process could have
made up thirty seconds ago, and reports itself as `tls?` throughout. Nothing
warns, because from bunny's point of view somebody asked for this.

What that costs is the entire value of TLS. Encryption without verification
means the traffic cannot be read by somebody watching the network — and nothing
stops that somebody from *being* the broker. They present whatever certificate
they like, the connection opens, and the client hands over its password in the
AMQP login and every message afterwards, encrypted the whole way, to them. There
is no symptom. The connection succeeds, the messages flow, and the only evidence
is on the attacker's disk.

**Every mode in this library therefore states `verify_peer` outright** rather
than leaving it unsaid. `Security#to_transport_options` always carries it, which
is why `Security` is a class and not three keyword arguments: there has to be
one place that cannot forget.

The integration spec proves the difference the only way it can be proved — by
connecting to a broker whose certificate does not check out and requiring that
the connection **fail**. bunny on its own opens it.

The same reach fixes a quieter problem. bunny pins its TLS context's minimum and
maximum version to the same constant, defaulting both to TLS 1.2, so a broker
and a client that could have agreed on 1.3 settle for 1.2. `Security#configure`
lifts the ceiling before the session starts: minimum 1.2, maximum 1.3 where
OpenSSL has it. It is guarded on both sides, because it reaches past bunny's
documented surface — a bunny that no longer offers the hook leaves the
connection on bunny's own pin, which is a version this library considers
acceptable anyway. The failure mode is a slightly older protocol, not a
connection that will not open.

## The three modes

Named rather than boolean, because "secure: false" does not say which of two
quite different things it means.

```ruby
Security.verified(certificate_authority: nil, certificate: nil, key: nil, credentials: nil)
Security.without_verifying_the_broker(because: "…")
Security.disabled
```

**`verified`** encrypts and checks who answered. This is what an `amqps://` URL
gets on its own.

**`disabled`** is plaintext, which is what `amqp://` already means and what a
broker on the same machine can reasonably have. Everything — including the
password used to log in — crosses the network readable.

**`without_verifying_the_broker`** is the third one, and it is deliberately
awkward to reach. See [below](#the-third-mode).

Predicates: `encrypted?`, `verifying?`, `client_certificate?`.

## A private certificate authority

Which is most brokers that are not on the public internet:

```ruby
mq = Connection.open("amqps://broker.internal:5671",
                     security: Security.verified(certificate_authority: "certs/ca.pem"),
                     credentials: Credentials.from_env)
```

Naming an authority **narrows** trust to that authority alone; the system store
is then not consulted at all. That is the point, not a limitation. A certificate
from a public authority is not evidence that the thing answering is *your*
broker, and the hundreds of authorities a machine trusts by default are hundreds
of ways to be wrong.

`certificate_authority:` takes a path or an array of paths, for a rollover where
two authorities are valid at once. A file that cannot be read raises
`ConfigurationError` while the connection is being configured, not while it is
being used — a certificate file that is not there will not be there on the next
attempt either, so anything retrying transport failures must not retry this.

## Mutual TLS

For a broker that authenticates clients by certificate rather than by password —
RabbitMQ's `EXTERNAL` mechanism, or any listener configured with `verify_peer`
and `fail_if_no_peer_cert`:

```ruby
Security.verified(certificate_authority: "certs/ca.pem",
                  certificate: "certs/client.crt",
                  key: "certs/client.key")
```

They go together, and one without the other is refused where it is configured
rather than at the handshake, because a handshake failure names neither file.

## The third mode

```ruby
Security.without_verifying_the_broker(
  because: "the CI broker's certificate is thrown away nightly")
```

It encrypts and then accepts any certificate at all. Everything in
[the bunny section](#bunny-does-not-verify-the-broker-when-it-is-given-a-url)
applies: the traffic cannot be read by somebody watching the network, and
nothing stops that somebody from being the broker.

The long name is so it cannot be typed by accident and cannot be skimmed past in
a review. `because:` is required and must say something, so the circumstance
ends up in the code rather than in somebody's memory of a conversation, and an
empty reason raises. It never becomes the default for any URL, so no
configuration change can arrive at it without somebody having written the line.

If you are reading it in a diff, the question to ask is not whether it works —
it always works, that is the problem — but what stops this process reaching a
production broker.

There is exactly one situation where it is the honest answer: a development
broker whose self-signed certificate you have not got round to trusting, on a
machine where being wrong costs nothing. Even there,
`certificate_authority:` on `verified` is about four seconds more work and is
correct.

## Development certificates

Running with TLS on your own machine needs a certificate authority, a broker
certificate and a client certificate, and generating them by hand with `openssl`
is an afternoon nobody has. This writes them:

```console
$ ./scripts/acemq-certs.rb --out .tls --broker localhost --days 30
```

or from Ruby:

```ruby
AceMQ::AMQP::DevelopmentCertificates.generate(directory: ".tls", broker_host: "localhost")
```

Seven files, under the same names Go's `acemq-certs` and .NET's
`AceMq.Amqp.DevCerts` write, so the three generators are interchangeable:
`ca.crt`, `ca.key`, `server.crt`, `server.key`, `client.crt`, `client.key`, and
a `rabbitmq.conf` that serves TLS from them. Keys are written `0600`.
Certificates are short-lived by default, because one that never expires is one
that outlives the reason it was created.

### They cannot reach production

Every certificate these generate carries

```
ACEMQ DEVELOPMENT ONLY - DO NOT TRUST
```

in its subject organisation, and **this library refuses one that does, however
trust is configured — unverified mode included**. That is the point: a
self-signed authority that drifts into production is *worse* than no
encryption, because everything looks protected and nothing is verified. These
fail closed instead. Java, Go and .NET all refuse the same marker.

Both halves are checked, because they are different mistakes:

- **What this process was configured with.** A certificate authority or client
  certificate carrying the marker is refused when the connection is made,
  before a socket is opened. A deployment pointed at `certs/ca.crt` from
  somebody's laptop is a deployment that trusts an authority anybody can
  regenerate.
- **What the broker presented.** Read during the handshake and refused there.
  In unverified mode this takes some doing: bunny sets the context to
  `VERIFY_NONE`, and OpenSSL does not act on a verify callback's answer in that
  mode at all — so the mode is raised to `VERIFY_PEER` and the callback then
  accepts everything `VERIFY_NONE` did, everything except the marker.
  Hostname checking is turned off explicitly so that raising the mode does not
  quietly change what unverified means. Go does the same thing with
  `InsecureSkipVerify` and a `VerifyPeerCertificate`, and for the same reason:
  unverified is the configuration a development certificate is likeliest to
  slip through, because everything else has already been turned off.

OpenSSL reports whatever the last chain error was, so the exception says
`certificate verify failed` and not why anybody said no. The reason is written
to stderr immediately above it, naming the marker and what to do about it.

### Saying that is what you meant

```ruby
security = Security.verified(certificate_authority: ".tls/ca.crt")
                   .allowing_development_certificates
```

A separate, visible step rather than a keyword on the constructors, for the same
reason `without_verifying_the_broker` has a long name: it has to be legible in a
diff. It weakens nothing else — verification stays on, the authority stays
whatever it was, and a certificate that does not verify is still refused. This
only stops the marker itself being the reason.

## Credentials

`Credentials` exists so the password does not have to be in the URL.

A URL is the one piece of configuration that gets printed: into error messages,
into structured logs, into `ps` output when it arrives as an argument, and into
whatever the deployment tool echoes back. Once a password has been through any
of those it has to be rotated, and the rotation is the expensive part. Passed
separately it never takes the trip.

```ruby
Credentials.of(username: "app", password: ENV.fetch("MQ_PASSWORD"))
Credentials.from_env                                              # ACEMQ_USERNAME / ACEMQ_PASSWORD
Credentials.from_env(username_variable: "MQ_USER", password_variable: "MQ_PASSWORD")
Credentials.from_file("/run/secrets/mq")                          # username:password
Credentials.from_file("/run/secrets/mq-password", username: "app")
Credentials.token(oauth_access_token)                             # RabbitMQ's OAuth 2 mechanism
```

`from_file` is how a mounted Kubernetes or Docker secret arrives. The file holds
`username:password`, or the password alone when `username:` is given. Trailing
whitespace is trimmed, because a file written by an editor almost always ends in
a newline and a password with a newline on the end is refused by the broker with
an error that says nothing about newlines.

`token` has no username: a bearer token is the password, and the account name is
ignored. `token?` says which kind you are holding.

### Redaction

The object renders as `username="app" secret=[REDACTED]` through `inspect`,
`to_s` and `%p` alike:

```ruby
p Credentials.of(username: "app", password: "hunter2")
# => #<AceMQ::AMQP::Credentials username="app" secret=[REDACTED]>
```

A bare string reaches a log the moment anything dumps the object holding it — an
exception, a `p` left in during debugging, a structured logger that serialises
its context. The accident should produce something useless rather than something
to rotate. The reader is called `secret` rather than `password` because it is
also where a token lives.

The URL is redacted too, in every error the transport raises:
`amqp://app:***@broker:5672`.

### Reading them fresh

A block is called at connection time rather than at start-up, which is what a
secret rotated underneath a running process needs:

```ruby
Connection.open(url, credentials: -> { Credentials.from_file("/run/secrets/mq") })
```

Anything answering `call` works. It is resolved at the moment the connection is
made — and, deliberately, not when a `Security` is merely printed:
`Security#inspect` says `credentials=(supplied at connection time)` rather than
running somebody's secret-fetching code because a logger dumped an object.

### Where they go

`security:` describes the channel, `credentials:` describe the login, and either
can be given without the other:

```ruby
Connection.open(url, credentials: Credentials.from_env)
Connection.open(url, security: Security.verified(certificate_authority: "ca.pem"))
Connection.open(url, security: Security.verified(certificate_authority: "ca.pem"),
                     credentials: Credentials.from_env)

# and, equivalently
Connection.open(url, security: Security.verified(certificate_authority: "ca.pem",
                                                 credentials: Credentials.from_env))
```

Passing them **twice** — once to the connection and once inside the `Security` —
is refused with `ConfigurationError` rather than silently resolved in one
direction. The two answers differ and neither is obviously the one that was
meant.

## What Security is safe to log

```ruby
puts security
# => mode=verified authority=certs/ca.pem clientCertificate=certs/client.crt credentials=(username="app" secret=[REDACTED])
```

Never the secret. Certificate **paths** are shown deliberately: a connection
that trusted the wrong authority is diagnosed by reading which file it trusted,
and a path is not a credential.

## Testing it

The TLS examples need a broker with a TLS listener and the authority that signed
its certificate, and skip when they have neither:

```bash
ACEMQ_TEST_BROKER_TLS=amqps://guest:guest@localhost:5671 \
ACEMQ_TEST_BROKER_CA=certs/ca.crt \
bundle exec rspec --tag integration

# with verify_peer and fail_if_no_peer_cert on the broker, the mutual-TLS
# examples run too
ACEMQ_TEST_BROKER_CLIENT_CERT=certs/client.crt \
ACEMQ_TEST_BROKER_CLIENT_KEY=certs/client.key \
...
```

They are worth running before touching anything in `security.rb`, because they
are the only place that proves a connection which should fail does.

## Next

- [Getting started](getting-started.md) — where `security:` and `credentials:`
  are passed
- [Metrics, tracing and health](observability.md) — serve them on a port the ingress does
  not publish
- [Licence](licence.md) — provided without warranty, this page included
