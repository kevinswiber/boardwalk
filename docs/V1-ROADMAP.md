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
- ✅ `v3#Q22` Duplicate peer-name handling: cloud returns 409 Conflict
  when a second hub tries to claim a name already in use.
- ✅ `v3#Q25` `/events` WS upgrade negotiates `zetta-events/1`
  subprotocol.
- ✅ `v3#Q31` Tunnel cancellation hygiene: WS unsubscribe aborts the
  cloud-side H2 driver task; dropping the body sends RST_STREAM
  upstream (test: `unsubscribe_tears_down_forwarded_stream`).
- ✅ Graceful shutdown via `Zetta::listen_until(addr, signal)`.
- ✅ `cargo deny` configured + CI step.
- ✅ Multi-device observe: `ServerHandle::observe(queries, callback)`
  fires when all queries are satisfied; `observe_loop` re-fires on
  device-set changes.
- ✅ Transition-dispatch macro: `zetta_core::transitions! { ... }`
  in `Device` impl removes the `Box::pin(async move { match ... })`
  boilerplate.
- ✅ Full proc-macro `#[device]` + `#[transition]` on `zetta-macros`
  (collapses the whole `Device` impl).
- ✅ CI: `cargo fmt`, `cargo clippy -D warnings`, `cargo test`
  across Linux + macOS + Windows, plus `cargo deny check`.
- ✅ TLS uses `rustls-platform-verifier` (OS trust store).
- ✅ Peer records persisted on first confirm (alongside device records)
  when `Zetta::persist(path)` is enabled.
- ✅ Device-declared streams: `DeviceCtx::publish` wired through
  `BusSink` from `Device::on_start`, so devices can emit on topics
  beyond `state`.
- ✅ Forwarded-events cleanup: cloud emits a 502 error frame when the
  upstream H2 stream closes mid-subscription.
- ✅ `POST /servers/{name}/events/unsubscribe` parity stub
  (forwards to peer or returns 202).
- ✅ Tracing instrumentation pass for transitions, WS subscribe/
  unsubscribe, and peer-forward request paths.

## Still open

### Graceful peer-disconnect behavior — `v3#Q32`
Currently: hub goes offline → cloud's `SendRequest` errors → cloud
returns 502 → hub eventually reconnects. Probably correct but needs a
longer-running test to confirm no leak (connection task count over
many disconnect/reconnect cycles).

### Embedded admin UI — `v2#Q8`
Originally defaulted to "no UI in v0", and we're keeping that. If
ever: a small leptos/yew SPA at `/_ui`.

### Crate name on crates.io
`zetta` may be taken. Check before first publish; `zetta-rs` is the
fallback per `v1#Q9`.

### `#[app]` proc-macro
`#[device]` collapses the `Device` impl. `App` is simpler (single
async fn) but a `#[app]` macro could still cut some boilerplate for
configurable apps. Defer until ≥3 real apps exist and the shape is
obvious.

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
