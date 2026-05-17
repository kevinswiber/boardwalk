# v1 Roadmap

Everything explicitly deferred from v0. Authoritative list — anything
not here is either done in v0 or has been intentionally killed.

This is appended to over time. Items reference the question doc and
number they came from (`v2#Q14`, `v3#Q24`) so the rationale stays
discoverable.

---

## Operational hardening

### Subscription deduplication on the cloud — `v3#Q24`
N WS clients on the cloud subscribing to the same hub topic open N
HTTP/2 streams cloud→hub. Should be 1 stream with refcounted client
fanout. Mirror the original Zetta's PubSub deduplication. Touches
`zetta-http::ws` + `PeerAcceptors`.

### Per-stream backpressure on forwarded events — `v3#Q23`
The cloud's hub→client forwarder reads NDJSON from the tunnel and
pushes into an unbounded WS mpsc. Slow client = unbounded growth.
Bound to ~256, drop oldest with a warn-log on overflow.

### Duplicate peer-name handling — `v3#Q22`
Two hubs both calling themselves "hub" linking to the same cloud:
second overwrites first silently. Reject the second WS upgrade with
`409 Conflict` and a clear error body.

### Tunnel cancellation hygiene — `v3#Q31`
When a cloud-side forwarded subscription aborts (WS unsubscribe / WS
client closes), the cloud's HTTP/2 stream to the hub should
`RST_STREAM` so the hub stops producing. Verify and tighten.

### Graceful peer-disconnect behavior — `v3#Q32`
Currently: hub goes offline → cloud's `SendRequest` errors → cloud
returns 502 → hub eventually reconnects (backoff). Probably correct
but needs a longer-running test to confirm there's no leak.

### TLS integration test — `v3#Q21`
Stand up a rustls-fronted axum listener with a self-signed cert,
trust it from a test-only verifier extension hook, run the existing
peer-link test through `https://`. (Note: as of v0.2, peer TLS uses
`rustls-platform-verifier` against the OS trust store, so the test
will need a separate path — or we generate a real cert via the
platform CA store mechanism.)

---

## Protocol completeness

### Forwarded events: cleanup on connection drop
When the H2 connection drops mid-event-stream, the cloud's WS client
should receive a clear `error` or `unsubscribe-ack`. Currently the
stream just dies silently.

### `/events` subprotocol negotiation — `v3#Q25`
Cloud's WS upgrade should negotiate `zetta-events/1`. Cheap. M10
polish.

### `POST /servers/{name}/events/unsubscribe` parity
The original Zetta defines this as a way to cancel a subscription via
HTTP (not WS). We don't have it. Probably never needed since v0 uses
either multiplex WS unsubscribe or stream RST_STREAM, but worth a
once-over for protocol parity.

---

## Platform features

### Persistent device + peer registry — `v2#Q15`, `v3#Q28`
`zetta_registry` exists and is unit-tested but not wired into the
runtime. Persist devices on `add_device` / on scout discovery; restore
on boot. Same for peer records (status, last-seen, connection_id).

### Scouts — `v2#Q13`, `v3#Q27`
Static `use_device` is enough for many use cases; scouts come back
when a real driver needs dynamic discovery (e.g., mDNS, USB). Trait
exists; `ScoutCtx` is a placeholder.

### Apps — `v2#Q14`, `v3#Q26`
`server.observe([q1, q2], |dev_a, dev_b| ...)`. Cross-query
observation infra: parse CaQL once, track per-query device sets,
fire callback when all sets non-empty, re-fire on changes. About
half a day; needs `ServerHandle` populated.

### Hubless device registration — `v2#Q16`, `v3#Q29`
`POST /servers/{name}/devices` is currently 501. To wire properly:

```rust
Zetta::new()
    .register_factory("led", |args| -> Box<dyn Device> { ... })
    .listen(...)
```

API design needs input before implementing.

---

## Developer experience

### Macros: `#[device]`, `#[transition]`, `#[app]` — `v2#Q12`, `v3#Q30`
Drop the verbose hand-written `Device` impl. The shape is sketched in
`docs/07-api-ergonomics.md`. Land when there are ≥3 real drivers
written by hand and the boilerplate starts hurting.

### Embedded admin UI — `v2#Q8`
Originally defaulted to "no UI in v0", and we're keeping that. If
ever: a small leptos/yew SPA at `/_ui`.

---

## Cross-cutting / housekeeping

### Tracing instrumentation pass
Most paths log at the right level, but it hasn't been audited. M10
polish.

### Crate name on crates.io
`zetta` may be taken. Check before first publish; `zetta-rs` is the
fallback per `v1#Q9`.

### CI
Nothing wired. GitHub Actions workflow: `cargo fmt --check`, `cargo
clippy`, `cargo test --workspace`. Probably also `cargo deny check`
once a deny.toml is in.

### Per-platform peer-link test
Right now `cargo test` runs on macOS dev box. We use
`rustls-platform-verifier`, so the integration test paths will behave
differently on Linux/Windows. Worth a CI matrix early.

---

## Things explicitly NOT in v1

These were considered and decided against — included so future-me
doesn't reopen them by mistake.

- **Interop with original Node.js Zetta peer protocol** (`v1#Q5`).
  Clean break confirmed.
- **HTTP/2 server push for events** (`v1#Q3`). Long-lived response
  bodies are the v0+ approach. Server push is deprecated.
- **Bidirectional querying over a single peer connection** (`v1#Q2`).
  One direction per link is the protocol. If you want bidirectional,
  both peers `link()`.
