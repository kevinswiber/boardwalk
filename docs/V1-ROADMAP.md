# v1 Roadmap

Everything still deferred from v0. Items reference the original
question (`v2#Q14`, `v3#Q24`) so the rationale stays discoverable.

## What was deferred but is now done in v0.x

- ✅ `v3#Q24` Cloud-side subscription deduplication.
- ✅ `v3#Q23` Bounded backpressure on forwarded events
  (`BROADCAST_BUFFER = 256`).
- ✅ `v3#Q28` Persistent device registry (`Zetta::persist(path)`).
- ✅ `v3#Q26` App support (`Zetta::use_app(impl App)` +
  `ServerHandle::query` + `DeviceProxy`).
- ✅ `v3#Q27` Scouts (`Zetta::use_scout(impl Scout)`,
  `ScoutCtx::discover` for runtime device registration).
- ✅ `v3#Q29` Hubless device registration via
  `Zetta::register_factory(type_name, |args| ...)`. POST
  /servers/{name}/devices wired with peer-forward fall-through.
- ✅ `v3#Q21` TLS integration test via `dangerous-test-tls` feature
  on `zetta-tunnel` + rcgen self-signed cert.
- ✅ Graceful shutdown via `Zetta::listen_until(addr, signal)`.
- ✅ `cargo deny` configured + CI step.
- ✅ Multi-device observe: `ServerHandle::observe(queries, callback)`
  fires when all queries are satisfied.
- ✅ Transition-dispatch macro: `zetta_core::transitions! { ... }`
  in `Device` impl removes the `Box::pin(async move { match ... })`
  boilerplate. (Full `#[device]` proc-macro deferred — see below.)
- ✅ CI: `cargo fmt`, `cargo clippy -D warnings`, `cargo test`
  across Linux + macOS, plus `cargo deny check`.
- ✅ TLS uses `rustls-platform-verifier` (OS trust store).

## Operational hardening

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

### Persist peer records too
Right now `zetta-registry` persists device records (via
`Zetta::persist`) but `PeerRecord` is not persisted on
connect/disconnect. Useful for "show me peers that have ever connected"
in the UI/CLI.

### Re-firing observe
Today `ServerHandle::observe` is single-shot — fires once when all
queries are satisfied. The original Zetta re-fires when device sets
change. Worth adding `observe_loop` that re-runs the callback on every
device-set change.

## Developer experience

### Full proc-macro `#[device]` / `#[transition]` / `#[app]`
The current `transitions! { ... }` macro_rules helper removes the
transition-dispatch boilerplate but `Device for X { ... }` still has
to be written by hand (config, state, etc.). A proc-macro could
collapse the whole impl into:

```rust
#[device]
impl Led {
    #[config] fn config(&self, cfg: &mut DeviceConfig) { ... }
    #[state] fn state(&self) -> &str { ... }
    #[transition] async fn turn_on(&mut self) -> Result<()> { ... }
    #[transition] async fn turn_off(&mut self) -> Result<()> { ... }
}
```

Needs a `zetta-macros` crate. Land when there are ≥3 real drivers
written by hand and the remaining boilerplate hurts.

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
