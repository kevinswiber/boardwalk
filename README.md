# zetta-rs

A Rust port of [Zetta](https://github.com/zettajs/zetta) — a hypermedia
server framework that models things as state machines, exposes them
over Siren HTTP, multiplexes telemetry over WebSockets, and tunnels
HTTP requests back through outbound connections so devices behind NATs
can be reached from anywhere.

**Status: v0 working.** A single-process Zetta server runs end-to-end:
typed device state machines, Siren HTTP API, multiplex WebSocket event
streams, and CaQL filtering. Peering (the original's reverse HTTP
tunnel over a WebSocket upgrade) is implemented at the handshake level
— a hub can dial a cloud, both sides flip into HTTP/2 with reversed
roles, and the cloud confirms the connection. Forwarding the cloud's
HTTP requests through the tunnel to query the hub's devices is the
next milestone (see `docs/10-questions-v2.md`).

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

## Try it

Boot the example server:

```
cargo run --bin hello-led
```

Then poke at it:

```
curl http://127.0.0.1:1337/
curl http://127.0.0.1:1337/servers/hub
DEV=$(curl -s http://127.0.0.1:1337/servers/hub | jq -r '.entities[0].properties.id')
curl http://127.0.0.1:1337/servers/hub/devices/$DEV
curl -d 'action=turn-on' http://127.0.0.1:1337/servers/hub/devices/$DEV
```

Subscribe to the LED's state stream over the multiplex WS endpoint:

```
wscat -c ws://127.0.0.1:1337/events
> {"type":"subscribe","topic":"hub/led/<device-id>/state"}
```

Or the original PoC, which proves role-reversed HTTP/2 over an
arbitrary stream:

```
cargo run --bin tunnel-poc
```

## Workspace layout

```
crates/                         core building-block crates
  zetta-core/                   Device + DeviceConfig + Transition + state-machine types
  zetta-siren/                  Siren types + serde + ergonomic builders
  zetta-caql/                   Calypso Query Language — lexer, parser, evaluator
  zetta-events/                 Event bus + topic matching + multiplex WS protocol
  zetta-registry/               redb-backed device + peer registry
  zetta-http/                   axum router emitting Siren; multiplex WS endpoint;
                                peer upgrade route; transition dispatch
  zetta-tunnel/                 WS upgrade + h2 prior-knowledge primitives
  zetta-peer/                   PeerClient (initiator side) + PeerAcceptors
  zetta-server/                 Top-level Zetta builder + .listen()
  zetta/                        Re-export façade

drivers/
  zetta-mock-led/               Mock LED used by tests + examples

examples/
  tunnel-poc/                   Role-reversed HTTP/2 over a duplex pipe
  hello-led/                    Boots a real Zetta server with a mock LED
```

## What's implemented

- **Devices**: typed `Device` trait with state, allowed transitions per
  state, transition dispatch with input fields, monitored properties.
- **Siren**: full hypermedia rendering for root, server, device,
  metadata, search-results, peer-management entities.
- **CaQL**: `select` projections + `where` predicates with `and/or/not`,
  comparison ops, `like`, `in`, list literals, nested paths.
- **Events**: subscription-based bus with topic patterns (literal, `*`,
  `**`, `{regex}`), per-event CaQL filters via `?ql=...` topic suffix,
  per-subscription limits, auto-cleanup on closed receivers.
- **Multiplex WebSocket** at `/events` per the wiki sub-protocol:
  subscribe, unsubscribe, ping/pong, event, subscribe-ack.
- **Peer tunnel**: WS-upgrade-then-HTTP/2 handshake with the cloud
  driving the HTTP/2 client and the hub serving the HTTP/2 server. Test
  verifies the full handshake round-trip.
- **Builder**: `Zetta::new().name("hub").use_device(Led).link("...").listen(addr)`.

## What's not yet (see docs/10-questions-v2.md)

- Forwarding cloud → hub HTTP queries through the open peer tunnel.
- Scouts (dynamic device discovery).
- Apps (`server.observe([q], |dev| ...)`).
- Persistent device registry (the redb tables exist, they're just not
  wired into the runtime yet).
- `POST /servers/{name}/devices` to register hubless devices.
- TLS for peer dials.

## License

Apache-2.0, matching the original Zetta.
