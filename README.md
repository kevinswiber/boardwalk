# boardwalk

[![Crates.io](https://img.shields.io/crates/v/boardwalk)](https://crates.io/crates/boardwalk)
[![docs.rs](https://img.shields.io/docsrs/boardwalk)](https://docs.rs/boardwalk)
[![CI](https://github.com/kevinswiber/boardwalk/actions/workflows/ci.yml/badge.svg)](https://github.com/kevinswiber/boardwalk/actions/workflows/ci.yml)

A hypermedia server framework for federated services. Boardwalk models
things as state machines, exposes them as discoverable Siren resources
over HTTP, multiplexes telemetry over WebSockets, and tunnels HTTP
requests back through outbound peer connections so services behind a
NAT stay reachable from anywhere else on your fleet.

Boardwalk started as a Rust port of [Zetta](https://github.com/zettajs/zetta)
and is evolving independently from there.

## Install

```toml
[dependencies]
boardwalk = "0.0.1"
tokio = { version = "1", features = ["full"] }
```

## Quick start

```rust,no_run
use boardwalk::{Boardwalk, Device, DeviceConfig, DeviceError, TransitionInput};
use futures::future::BoxFuture;

#[derive(Default)]
struct Led { on: bool }

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .state(self.state())
            .when("off", &["turn-on"])
            .when("on", &["turn-off"])
            .monitor("state");
    }
    fn state(&self) -> &str { if self.on { "on" } else { "off" } }
    fn transition<'a>(
        &'a mut self,
        name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async move {
            match name {
                "turn-on"  => { self.on = true;  Ok(()) }
                "turn-off" => { self.on = false; Ok(()) }
                other      => Err(DeviceError::Invalid(other.into())),
            }
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Boardwalk::new()
        .name("hub")
        .use_device(Led::default())
        .listen("127.0.0.1:1337".parse()?)
        .await
}
```

```sh
curl http://127.0.0.1:1337/servers/hub
DEV=$(curl -s http://127.0.0.1:1337/servers/hub | jq -r '.entities[0].properties.id')
curl -d 'action=turn-on' http://127.0.0.1:1337/servers/hub/devices/$DEV
```

## What's in the box

- **Devices as state machines** — typed `Device` trait with allowed
  transitions per state, input fields, monitored properties.
- **Siren hypermedia API** — root, server, device, type metadata, and
  search-result resources, with cross-server peer links.
- **Multiplex WebSocket events** at `/events` — subscribe to topics
  with `*` / `**` / regex patterns, optional CaQL filters.
- **CaQL** — small query DSL for selecting and projecting devices.
- **Peer tunnel** — hub dials cloud via an outbound WebSocket; both
  sides upgrade to HTTP/2 with reversed roles. The cloud then proxies
  HTTP and forwards event subscriptions back through the same socket.
- **TLS** — peer dials use rustls + the OS trust store via
  `rustls-platform-verifier`.
- **Persistence** — opt-in via `Boardwalk::persist(path)`. Stable
  device IDs across restarts, peer record persistence.

## Run the example

```
cargo run --bin hello-led
```

There's also a tunnel proof-of-concept that drives role-reversed
HTTP/2 over an arbitrary duplex stream:

```
cargo run --bin tunnel-poc
```

## Crate layout

The published API surface is the `boardwalk` facade. Everything else
is split across narrowly-scoped crates so driver authors and embedders
can pick what they need:

| Crate                  | What it does                                                       |
| ---------------------- | ------------------------------------------------------------------ |
| [`boardwalk`]          | Re-export facade — start here                                      |
| [`boardwalk-core`]     | `Device`, `Scout`, `App` traits + runtime types                    |
| [`boardwalk-siren`]    | Siren entity / action / link / field types and serde               |
| [`boardwalk-caql`]     | Calypso Query Language — parser + evaluator                        |
| [`boardwalk-events`]   | Event bus, topic matching, multiplex WebSocket protocol            |
| [`boardwalk-registry`] | redb-backed persistent device + peer registries                    |
| [`boardwalk-http`]     | axum router emitting Siren; WS endpoint; peer upgrade route        |
| [`boardwalk-tunnel`]   | WebSocket upgrade + HTTP/2-prior-knowledge tunnel primitives       |
| [`boardwalk-peer`]     | Outbound peer client and inbound peer acceptor                     |
| [`boardwalk-macros`]   | `#[device]` / `#[transition]` proc macros                          |
| [`boardwalk-server`]   | `Boardwalk::new()…listen()` builder                                |

[`boardwalk`]: https://crates.io/crates/boardwalk
[`boardwalk-core`]: https://crates.io/crates/boardwalk-core
[`boardwalk-siren`]: https://crates.io/crates/boardwalk-siren
[`boardwalk-caql`]: https://crates.io/crates/boardwalk-caql
[`boardwalk-events`]: https://crates.io/crates/boardwalk-events
[`boardwalk-registry`]: https://crates.io/crates/boardwalk-registry
[`boardwalk-http`]: https://crates.io/crates/boardwalk-http
[`boardwalk-tunnel`]: https://crates.io/crates/boardwalk-tunnel
[`boardwalk-peer`]: https://crates.io/crates/boardwalk-peer
[`boardwalk-macros`]: https://crates.io/crates/boardwalk-macros
[`boardwalk-server`]: https://crates.io/crates/boardwalk-server

## Docs

- [Design overview](docs/00-overview.md) — what we're porting and what
  we're skipping from upstream Zetta.
- [Architecture](docs/01-architecture.md) — crate layout and how the
  pieces talk.
- [Peer protocol](docs/02-protocol-peer.md) — WebSocket-upgrade then
  HTTP/2-prior-knowledge tunnel.
- [API ergonomics](docs/07-api-ergonomics.md) — builder + driver shape.
- [v1 roadmap](docs/V1-ROADMAP.md) — what's still open.

## License

Apache-2.0. See [LICENSE](LICENSE).
