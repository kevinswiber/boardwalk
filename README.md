# boardwalk

A hypermedia server framework for federated services, started as a
Rust port of [Zetta](https://github.com/zettajs/zetta) and evolving
independently from there. Models things as state machines, exposes them
over Siren HTTP, multiplexes telemetry over WebSockets, and tunnels
HTTP requests back through outbound connections so services behind NATs
can be reached from anywhere.

**Status: v0 working with peering.** A single-process Boardwalk server runs
end-to-end: typed device state machines, Siren HTTP API, multiplex
WebSocket event streams, CaQL filtering. The full reverse-tunnel
peering protocol works: a hub dials a cloud, both flip into HTTP/2
with reversed roles, the cloud confirms the connection, and then the
cloud's HTTP and WebSocket APIs proxy through the tunnel — a curl to
the cloud's `/servers/hub/devices/{id}` returns the hub's LED, a POST
transitions it, and a cloud-side WS subscription to `hub/...` topics
receives events streamed back from the hub. TLS for peer dials is
supported via rustls + aws-lc-rs.

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
  boardwalk-core/                   Device + DeviceConfig + Transition + state-machine types
  boardwalk-siren/                  Siren types + serde + ergonomic builders
  boardwalk-caql/                   Calypso Query Language — lexer, parser, evaluator
  boardwalk-events/                 Event bus + topic matching + multiplex WS protocol
  boardwalk-registry/               redb-backed device + peer registry
  boardwalk-http/                   axum router emitting Siren; multiplex WS endpoint;
                                peer upgrade route; transition dispatch
  boardwalk-tunnel/                 WS upgrade + h2 prior-knowledge primitives
  boardwalk-peer/                   PeerClient (initiator side) + PeerAcceptors
  boardwalk-server/                 Top-level Boardwalk builder + .listen()
  boardwalk/                        Re-export façade

drivers/
  boardwalk-mock-led/               Mock LED used by tests + examples

examples/
  tunnel-poc/                   Role-reversed HTTP/2 over a duplex pipe
  hello-led/                    Boots a real Boardwalk server with a mock LED
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
- **Builder**: `Boardwalk::new().name("hub").use_device(Led).link("...").listen(addr)`.

## What's next

Everything explicitly deferred from v0 lives in
[`docs/V1-ROADMAP.md`](docs/V1-ROADMAP.md) — operational hardening,
scouts, apps, registry persistence, hubless device registration,
macros, CI, etc. That's the working backlog.

## License

Apache-2.0, matching upstream Zetta.
