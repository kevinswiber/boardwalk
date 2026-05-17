# Open Questions (after v0 implementation)

These came up during M1-M8 implementation. As before, each has a
**Default** I'll move forward with if no answer comes back.

---

### Q11. M7-full: actually forward peer queries through the tunnel?

Right now the peer tunnel handshakes (initiator dials, acceptor
confirms, both sides hold the H2 connection open). But the cloud
doesn't yet *use* the open H2 client to query the hub's devices.

To finish "the cloud's API exposes the hub's devices" we need:

- The cloud's router, when serving `/servers/{hub-name}/...`, dispatches
  through the hub's H2 `SendRequest`.
- A `PeerRegistry` (in-memory) the cloud's router consults to find the
  right peer connection by name.
- Streaming: the cloud's `GET /events` topic `hub/...` forwards to the
  hub's `/servers/hub/events?topic=...` and pipes the long-body
  response back to the WS client.

Estimated ~½ day. Want me to do it now, or wait?

**Default:** wait for your sign-off. Closing this loop is what makes
peering useful, but the test coverage so far is thin, and you may want
to chew on the protocol shape first.

---

### Q12. Macros: `#[device]` / `#[transition]`

The current driver shape is a hand-written impl:

```rust
impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) { ... }
    fn state(&self) -> &str { ... }
    fn transition<'a>(&'a mut self, name: &'a str, input: TransitionInput)
        -> BoxFuture<'a, Result<(), DeviceError>>
    { Box::pin(async move { match name { ... } }) }
}
```

A proc macro could collapse this to `#[transition] async fn turn_on(&mut self) {...}`
methods plus an auto-derived dispatcher.

**Default:** skip for v0. The verbose form is clear enough for the
mock drivers we have. Revisit if/when we have ≥3 real drivers and the
boilerplate becomes friction.

---

### Q13. Scouts (`zetta_core::Scout`)

Scouts aren't wired into the server builder yet. The trait exists
(`Scout::run(self, ScoutCtx)`) but `Zetta::use_scout(...)` doesn't
exist, and even if it did, `ScoutCtx` is a placeholder.

The original's primary use case is "device protocol discovers things
over time (Bluetooth, USB, mDNS)" — most contemporary IoT/edge
deployments don't run on a hub that does protocol scanning anymore.
For most ports of this concept, static `use_device` is enough.

**Default:** punt. Static registration only in v0. We can resurrect
scouts as a feature if a real driver needs them.

---

### Q14. Apps (`zetta_core::App`)

Same shape as scouts — trait exists, `ServerHandle` is a placeholder,
no `use_app` on the builder. The original lets you do:

```rust
server.observe([query1, query2], |dev_a, dev_b| async { ... }).await;
```

This needs the cross-query observer infrastructure on `ServerHandle`,
which is real work (CaQL query → device observation → callback fanout).

**Default:** punt to follow-up milestone. The dusk-to-dawn example
isn't critical for proving the core port is sound.

---

### Q15. Device registry persistence

`zetta_registry::Registry` is implemented (redb-backed device + peer
tables with tests) but is **not actually wired into the runtime**.
Devices added via `.use_device()` live only in memory; restart loses
state.

Original Zetta persists devices keyed by id so a scout can re-attach to
a known device. Without scouts, we don't really need this in v0 — but
we should still let drivers store per-instance state somewhere.

**Default:** wire `Registry` into the runtime in v0.1 alongside scout
support. v0 stays in-memory.

---

### Q16. Hubless device registration

The original has `POST /servers/{name}/devices` for "register a device
from outside via HTTP". The Siren action is rendered in our responses
but the route is not implemented.

**Default:** add a stub that returns `501 Not Implemented` for v0; flag
as a follow-up. Adding it properly is reasonable but touches
`DeviceConfig` semantics (we'd need to instantiate a device for a type
we only know about by string).

---

### Q17. Strict subprotocol enforcement

The peer upgrade route accepts any `Upgrade: websocket`; it doesn't
*require* the `Sec-WebSocket-Protocol: zetta-peer/2` token. Should it?

**Default:** require it. This is a clean break (Q5 from the v1 doc)
and rejecting non-token traffic is cheap protection.

---

### Q18. PeerClient shutdown

`PeerClient::spawn` runs forever with infinite reconnect. The returned
`JoinHandle` can be aborted, but there's no cooperative shutdown. For
graceful drains we'd add a `CancellationToken` or similar.

**Default:** add it in M10 polish; for v0 the abort-the-handle behavior
is acceptable.

---

### Q19. Tunnel crate boundary

`zetta-tunnel` currently exports primitives (`dial_initiator`,
`build_upgrade_response`); the actual h2-on-upgrade work happens in
`zetta-peer` because zetta-tunnel can't depend on axum/hyper-util's
service stack without becoming heavy. There's a slight code smell.

**Default:** acceptable as is. If we ever ship a third tunnel user
beyond zetta-peer, refactor; otherwise leave it.

---

### Q20. TLS

The peer dial only supports `http://` / `ws://`. Real deployments need
`https://` / `wss://` with rustls. The dependency is in the workspace
manifest but not used yet.

**Default:** add `rustls` to the peer dial in the next milestone. Don't
ship v0 without it.
