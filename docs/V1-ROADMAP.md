# v1 Roadmap

Everything still deferred from v0. Items reference the original
question (`v2#Q14`, `v3#Q24`) so the rationale stays discoverable.

## What was deferred but is now done in v0.x

- ✅ `v3#Q24` Cloud-side subscription deduplication (one HTTP/2 stream
  per `(peer, topic)`, fanned via `tokio::sync::broadcast`).
- ✅ `v3#Q23` Bounded backpressure on forwarded events (via the
  broadcast channel's lagging behavior at `BROADCAST_BUFFER = 256`).
- ✅ `v3#Q28` Persistent device registry (`Zetta::persist(path)` —
  device IDs stable across restarts).
- ✅ `v3#Q26` App support (`Zetta::use_app(impl App)` +
  `ServerHandle::query` + `DeviceProxy`).
- ✅ CI: `cargo fmt`, `cargo clippy -D warnings`, `cargo test`
  across Linux + macOS.
- ✅ TLS uses `rustls-platform-verifier` (OS trust store).

## Operational hardening

### TLS integration test — `v3#Q21`
Stand up a rustls-fronted axum listener with a self-signed cert
(via `rcgen`). Add a `dangerous-test-tls` feature on `zetta-tunnel`
that swaps in a no-verify `ServerCertVerifier` so the test can trust
the self-signed cert without poking the OS trust store. Run the
existing peer-link test through `wss://`.

### Duplicate peer-name handling — `v3#Q22`
Two hubs both calling themselves "hub" linking to the same cloud:
second overwrites first silently. Reject the second WS upgrade with
`409 Conflict` and a clear error body.

### Tunnel cancellation hygiene — `v3#Q31`
When a forwarded subscription aborts (WS unsubscribe / client close),
the cloud's HTTP/2 stream to the hub should `RST_STREAM` so the hub
stops producing. Verify and tighten.

### Graceful peer-disconnect behavior — `v3#Q32`
Currently: hub goes offline → cloud's `SendRequest` errors → cloud
returns 502 → hub eventually reconnects. Probably correct but needs a
longer-running test to confirm no leak.

### Per-platform peer-link test in CI
`rustls-platform-verifier` behaves differently per OS. CI matrix
already covers Linux + macOS. Add Windows once we have a Windows-aware
test harness.

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
WS unsubscribe or H2 RST_STREAM, but worth a once-over.

### Stream subscriptions on devices (beyond `state`)
Today only `state` is auto-published when monitored. Devices that
declare `.stream("intensity", ...)` need a story for actually
publishing values to that topic. `DeviceCtx::publish` exists but
isn't wired into the `Device::run` path.

## Platform features

### Scouts — `v2#Q13`, `v3#Q27`
Static `use_device` is enough for many use cases; scouts come back
when a real driver needs dynamic discovery (mDNS, USB, etc.). The
`Scout` trait exists but `ScoutCtx` is a placeholder and there's no
`use_scout` builder method.

### Hubless device registration — `v2#Q16`, `v3#Q29`
`POST /servers/{name}/devices` is currently 501. To wire properly:

```rust
Zetta::new()
    .register_factory("led", |args| -> Box<dyn Device> { ... })
    .listen(...)
```

API design needs input before implementing.

### Persist peer records too
Right now `zetta-registry` persists device records (via
`Zetta::persist`) but `PeerRecord` is not persisted on
connect/disconnect. Useful for "show me peers that have ever connected"
in the UI/CLI.

### Multi-device observe (the original `server.observe([q1, q2], cb)`)
The current `ServerHandle::query` returns a snapshot at call time.
The original Zetta fires a callback when ALL queries are satisfied
and re-fires when device sets change. Useful for apps that bridge
multiple device types.

## Developer experience

### Macros: `#[device]`, `#[transition]`, `#[app]` — `v2#Q12`, `v3#Q30`
Drop the verbose hand-written `Device` impl. The shape is sketched in
`docs/07-api-ergonomics.md`. Land when there are ≥3 real drivers
written by hand and the boilerplate starts hurting.

### Embedded admin UI — `v2#Q8`
Originally defaulted to "no UI in v0", and we're keeping that. If
ever: a small leptos/yew SPA at `/_ui`.

### Better `serve_with_shutdown`
`Zetta::listen` blocks until the listener stops. Worth adding
`listen_until(signal: impl Future)` for clean shutdown in long-running
processes.

## Cross-cutting / housekeeping

### Tracing instrumentation pass
Most paths log at the right level, but it hasn't been audited. M10
polish.

### Crate name on crates.io
`zetta` may be taken. Check before first publish; `zetta-rs` is the
fallback per `v1#Q9`.

### `cargo deny` config
Audit dependencies for licenses, advisories, duplicates. Add a
`deny.toml` and a CI step.

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
