# zetta-rs

A Rust port of [Zetta](https://github.com/zettajs/zetta) — a hypermedia
server framework that models things as state machines, exposes them
over Siren HTTP, multiplexes telemetry over WebSockets, and tunnels
HTTP requests back through outbound connections so devices behind NATs
can be reached from anywhere.

**Status: design + scaffold.** The riskiest piece — role-reversed HTTP/2
over an arbitrary stream — has a working proof of concept
(`examples/tunnel-poc`). Real implementation begins next per the
roadmap.

## Read first

- [docs/00-overview.md](docs/00-overview.md) — what we're porting and
  what we're skipping.
- [docs/01-architecture.md](docs/01-architecture.md) — crate layout and
  how the pieces talk.
- [docs/02-protocol-peer.md](docs/02-protocol-peer.md) — the
  WebSocket-upgrade-then-HTTP/2 peer tunnel (SPDY replacement).
- [docs/07-api-ergonomics.md](docs/07-api-ergonomics.md) — what the
  developer-facing API will look like.
- [docs/09-questions.md](docs/09-questions.md) — open questions for
  review.

## Try the tunnel PoC

```
cargo run --bin tunnel-poc
```

Two tasks, one in-memory duplex pipe, role-reversed HTTP/2 between them.
Proves the peer protocol's central trick is feasible in stock Rust
without any forked dependencies.

## Workspace layout

```
crates/                         core building-block crates
  zetta-core/                   Device, Scout, App, runtime traits (no deps on transport)
  zetta-siren/                  Siren types + serde
  zetta-caql/                   Calypso Query Language (stub; M4)
  zetta-events/                 Event bus + multiplex WS protocol (stub; M3)
  zetta-registry/               redb-backed device + peer registry (stub; M5)
  zetta-http/                   axum router emitting Siren (stub; M6)
  zetta-tunnel/                 WS → role-reversed HTTP/2 primitive (stub; M7)
  zetta-peer/                   Peer client / peer socket (stub; M7)
  zetta-server/                 Top-level builder (stub; M8)
  zetta/                        Re-export façade

drivers/
  zetta-mock-led/               Mock LED used by tests + examples

examples/
  tunnel-poc/                   ✅ Working PoC — role-reversed HTTP/2
  hello-led/                    Stub server with a mock LED
```

## License

Apache-2.0, matching the original Zetta.
