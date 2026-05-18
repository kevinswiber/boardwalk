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
boardwalk = "0.2"
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

## Module layout

The whole library lives in the single [`boardwalk`] crate. The most
commonly used types — `Boardwalk`, `Device`, `DeviceConfig`,
`DeviceError`, `TransitionInput`, `App`, `Scout`, `ServerHandle`,
`device`/`transition` macros — are re-exported at the crate root.
Submodules expose lower-level surface for embedders:

| Module                 | What's in it                                                        |
| ---------------------- | ------------------------------------------------------------------- |
| `boardwalk::core`      | `Device`, `Scout`, `App` traits, transitions, streams               |
| `boardwalk::siren`     | Siren entity / action / link / field types + serde                  |
| `boardwalk::query`     | Runtime-owned query AST + evaluator (`Query`, `Predicate`, …)       |
| `boardwalk::caql`      | Calypso Query Language — text syntax that parses into `query::Query` |
| `boardwalk::events`    | Event bus, topic matching, multiplex WebSocket protocol             |
| `boardwalk::registry`  | redb-backed persistent device + peer registries                     |
| `boardwalk::http`      | axum router emitting Siren; WS endpoint; peer upgrade route         |
| `boardwalk::tunnel`    | WebSocket-upgrade + HTTP/2-prior-knowledge tunnel primitives        |
| `boardwalk::peer`      | Outbound peer client and inbound peer acceptor                      |
| `boardwalk::server`    | `Boardwalk::new()…listen()` builder                                 |

The `#[device]` and `#[transition]` proc macros ship in the separate
[`boardwalk-macros`] crate (a Rust requirement — proc-macro crates can't
live alongside non-macro code) and are re-exported by `boardwalk`.

[`boardwalk`]: https://crates.io/crates/boardwalk
[`boardwalk-macros`]: https://crates.io/crates/boardwalk-macros

## Docs

- [Getting started](docs/getting-started.md) — install, write a
  driver, run the server, talk to it.
- [Devices](docs/devices.md) — `Device` trait, properties, streams,
  scouts, persistence.
- [Peers](docs/peers.md) — reverse-tunnel hub ↔ cloud setup.
- [CaQL](docs/caql.md) — query DSL reference.

## License

Apache-2.0. See [LICENSE](LICENSE).
