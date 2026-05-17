# Roadmap

Ordered by dependency. Each milestone is a recognizable working piece
that can be demoed end-to-end at its level.

## M0 — Workspace + tunnel proof of concept

- [ ] Cargo workspace skeleton with stub crates so `cargo check`
      passes for every crate (no real logic).
- [ ] **Standalone PoC in `examples/tunnel-poc/`**: two tasks, a
      `tokio::io::duplex` pair between them. Task A calls
      `h2::server::handshake`. Task B calls `h2::client::handshake`.
      B sends `GET /hello`; A returns 200 OK with body `world`. Then
      reverse it: A also sends a request to B via a separate
      stream? No — h2 is half-reversed: only the side that handshaked
      as client can send requests. **Important to validate.**
      (See Q2 in questions — we are choosing single-direction here.)
- [ ] Same PoC but with a real TCP socket + WebSocket upgrade:
      bind a server, connect a client, WS handshake, drop framing,
      h2 handshake, exchange `GET /_initiate_peer/{uuid}`, ping
      both ways.

This milestone proves the riskiest assumption. Estimated ~1-2 days.

## M1 — `zetta-core` types

- [ ] `Device` trait, `DeviceConfig` builder, `Transition`.
- [ ] `StreamHandle<T>` and the `monitor` / `stream` config methods.
- [ ] In-process tests: register a device, drive a transition, observe
      state change, send to a stream.

## M2 — `zetta-siren`

- [ ] Types + serde derives.
- [ ] `rels` constants.
- [ ] Helpers: `Entity::self_link(href)`, `Entity::add_action(...)`.
- [ ] Round-trip tests against captured JSON from the Node version.

## M3 — `zetta-events`

- [ ] `EventBus` with subscription registry.
- [ ] `TopicPattern` parser (literal/`*`/`**`/regex segments).
- [ ] `?ql=` filter parse (placeholder until M4).
- [ ] Publish/match unit tests.

## M4 — `zetta-caql`

- [ ] Lexer + grammar in chumsky.
- [ ] AST.
- [ ] Evaluator over `serde_json::Value`.
- [ ] Fuzzy tests against examples from the wiki.

Once M4 lands we can finalize the `?ql=` filter integration in
`zetta-events`.

## M5 — `zetta-registry`

- [ ] Redb-backed device + peer tables.
- [ ] CRUD + watch (notify on changes).
- [ ] Migration story for v1 schema bump (probably "wipe and re-scout"
      for v0).

## M6 — `zetta-http`

- [ ] Routes per [04-siren-modeling.md](04-siren-modeling.md).
- [ ] Content-negotiation: `vnd.siren+json` ↔ `application/json`.
- [ ] WS upgrade for `/events` (multiplex protocol) and per-stream WS.
- [ ] Tests against a mock `ServerCore`.

## M7 — `zetta-tunnel` + `zetta-peer`

- [ ] `zetta-tunnel` lifts M0 PoC into a reusable primitive.
- [ ] `PeerClient`: dial out, WS upgrade, h2 server, plug into router.
- [ ] `PeerSocket`: accept WS, drop framing, h2 client, subscribe.
- [ ] Reconnect/backoff, h2 PING keepalive.

## M8 — `zetta-server` + `zetta` façade

- [ ] Builder API.
- [ ] `use_(...)` blanket impls for device/scout/app.
- [ ] `listen(addr)` driver.
- [ ] Example servers: `examples/hello-led`, `examples/peer-link`.

## M9 — Macros (optional but nice)

- [ ] `#[device]` proc macro generating `Device` impls.
- [ ] `#[transition]` for individual methods.
- [ ] `#[app]` for free-function apps.

## M10 — Polish

- [ ] Tracing throughout.
- [ ] Operator-visible diagnostics (peer status, subscription counts).
- [ ] README + tutorial that mirrors the original Hello World.

## Approximate ordering

```
M0 ─┬─ M1 ─┬─ M2 ─┬─ M3 ─┬─ M4 ─┐
    │      │      │      │      │
    └──────┴──────┴──────┴──────┼─ M5 ─ M6 ─ M7 ─ M8 ─ M9 ─ M10
                                 │
                                 (independent of M3/M4)
```

M0 first, everything else can mostly proceed in parallel after that.
