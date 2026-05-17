# boardwalk-rs — Design Overview

## What we're porting

Zetta is an API-first server originally written in Node.js. The wire shape
that matters for the port (independent of its original IoT framing):

1. A **device** is a state machine. It has typed state, named transitions,
   typed inputs, and one or more **streams** (telemetry channels).
2. The server exposes every device as a hypermedia ([Siren][siren]) HTTP
   resource. Crawlable links + side-effecting actions = state-machine
   transitions over HTTP.
3. The server multiplexes **WebSocket event streams** so a client can
   subscribe to many topics (`{server}/{type}/{id}/{stream}`, with
   wildcards, regex, and a query DSL) on one socket.
4. Two server instances can **peer**. The hub opens an outbound WebSocket
   to the cloud, both sides drop the WS framing, and the cloud then
   speaks HTTP requests back to the hub over that single connection. The
   cloud's view of the hub's devices is via plain HTTP through this
   reverse tunnel.
5. A query language ([CaQL][caql]) selects devices: `where type = "led"`,
   `where state = "on" and type = "motion"`, etc.
6. **Apps** are user code that runs inside the server, observes device
   queries (`server.observe([query], |d| { ... })`), and reacts.

We are porting (1)-(6). IoT-specific framing in the original docs
("scouts looking for hardware over Bluetooth", "BeagleBone GPIO", etc.)
does not need to come along. The framework is general: any process that
can model itself as a state machine + telemetry streams works.

## Why now / why Rust

- The original Node implementation depended on SPDY for the reverse
  tunnel. SPDY is dead; HTTP/2 ships everywhere, and HTTP/2 servers and
  clients in Rust (`h2`) accept arbitrary `AsyncRead + AsyncWrite`
  streams. The "role-reversed protocol over a tunnel" trick is more
  natural in 2026 than it was in 2014.
- Rust gives a typed builder API for state machines that the JS version
  had to fake with strings.
- Static binaries on edge hardware (Raspberry Pi, etc.) and constant
  memory profile are real wins for the platform's stated use case.

## Goals (v0)

- `boardwalk_core` library with a typed `Device` trait and `DeviceBuilder`,
  a `Scout` trait, and an `App` trait.
- `boardwalk_http` axum-based HTTP server emitting Siren JSON.
- `boardwalk_ws` multiplexed WebSocket event protocol (sub-protocol from
  the wiki's "Multiplexed-WebSocket-Streams").
- `boardwalk_peer` peer linking: outbound (initiator) and accept (acceptor)
  with HTTP/2 role reversal over an upgraded WebSocket tunnel.
- `boardwalk_caql` parser + evaluator for CaQL (subset sufficient for
  device/stream filtering).
- `boardwalk_registry` persistent device + peer metadata via `redb`.
- One mock driver (`boardwalk-mock-led`) to validate end-to-end.

## Non-goals (v0)

- Browser client. The original Zetta browser is JS; not reimplementing.
- Drop-in compatibility with existing Node Zetta NPM driver modules. A
  driver is a foreign-language plugin; staying inside Rust at this stage.
- Cluster mode / horizontal sharding. Single-process for now.
- Authentication/authorization layer. The original had none built-in
  (TLS only). We'll leave a hook for it but not ship one.

## Document index

- [01-architecture.md](01-architecture.md) — Crate layout, module
  boundaries, key types.
- [02-protocol-peer.md](02-protocol-peer.md) — The reverse-tunnel handshake,
  what changes from SPDY → HTTP/2.
- [03-protocol-events.md](03-protocol-events.md) — Multiplexed WebSocket
  event sub-protocol.
- [04-siren-modeling.md](04-siren-modeling.md) — Hypermedia resource shapes.
- [05-caql.md](05-caql.md) — Query language semantics + parser plan.
- [06-dependencies.md](06-dependencies.md) — Crate choices and rationale.
- [07-api-ergonomics.md](07-api-ergonomics.md) — Public Rust API sketch.
- [08-roadmap.md](08-roadmap.md) — Ordered work plan with milestones.
- [09-questions.md](09-questions.md) — Open questions for review.

[siren]: https://github.com/kevinswiber/siren
[caql]: https://github.com/kevinswiber/caql
