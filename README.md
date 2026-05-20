# boardwalk

[![Crates.io](https://img.shields.io/crates/v/boardwalk)](https://crates.io/crates/boardwalk)
[![docs.rs](https://img.shields.io/docsrs/boardwalk)](https://docs.rs/boardwalk)
[![CI](https://github.com/kevinswiber/boardwalk/actions/workflows/ci.yml/badge.svg)](https://github.com/kevinswiber/boardwalk/actions/workflows/ci.yml)

A hypermedia server framework for federated services. Boardwalk models
addressable things as `Resource`s, drives executable `Actor`s inside a
`Node`, exposes their `ResourceSnapshot`s as discoverable Siren
resources over HTTP, multiplexes telemetry over WebSockets, and tunnels
HTTP requests back through outbound peer connections so services behind
a NAT stay reachable from anywhere else on your fleet.

Boardwalk started as a Rust port of [Zetta](https://github.com/zettajs/zetta)
and is evolving independently from there.

## Install

```toml
[dependencies]
boardwalk = "0.2"
tokio = { version = "1", features = ["full"] }
```

## Quick start

From this workspace, the LED example is a small resource/actor fixture.
It registers an actor, queries it through the runtime handle, invokes a
transition, and prints the explicitly published state event.

```sh
cargo run -p hello-led
```

## What's in the box

- **Resource / Actor runtime** — read-only `Resource`s, executable
  `Actor`s, per-node directories, lifecycle hooks, bounded transition
  execution, and `TransitionOutcome` results.
- **Resource HTTP routes** — `/resources`, `/resources/{id}`, and
  `/resources/{id}/transitions/{transition}` render Siren around
  `ResourceSnapshot`; peer-scoped routes mirror that vocabulary under
  `/servers/{name}/resources`.
- **Multiplex WebSocket events** at `/events` — subscribe to topics
  with `*` / `**` / regex patterns, optional `?ql=<caql>` filters,
  explicit actor publishes, and bounded slow-consumer policies.
- **CaQL** — small query DSL for selecting and projecting resources.
- **Job-runner example** — runnable `Boardwalk` HTTP server showing
  `JobQueue` / `Job` resources, async transition acceptance, explicit
  progress/log/lifecycle streams, replay, and `SlowConsumerPolicy::Coalesce`.
- **Peer tunnel** — hub dials cloud via an outbound WebSocket; both
  sides upgrade to HTTP/2 with reversed roles. The cloud then proxies
  HTTP and forwards event subscriptions back through the same socket.
- **TLS** — peer dials use rustls + the OS trust store via
  `rustls-platform-verifier`.

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

The whole library lives in the single [`boardwalk`] crate. The
Resource/Actor model lives behind a small crate-root facade, with
curated `runtime`, `query`, `caql`, and `events` modules for focused
subdomains.
The crate root exports the authoring facade: `Boardwalk`, `Resource`,
`Actor`, `NodeBuilder`, `ResourceSnapshot`, transition metadata, and
stream specs. Curated submodules cover focused domains:

Use `Boardwalk::new().use_actor(...).listen(...)` when you want the
supplied HTTP, WebSocket, and peer routes. That builder creates the
same actor-backed `Node` runtime that `NodeBuilder` exposes for
in-process use.

| Module               | What's in it                                                        |
| -------------------- | ------------------------------------------------------------------- |
| `boardwalk::runtime` | Node runtime, handles, lifecycle contexts, Resource/Actor traits     |
| `boardwalk::query`   | Runtime-owned query AST + evaluator (`Query`, `Predicate`, …)       |
| `boardwalk::caql`    | Calypso Query Language — text syntax that parses into `query::Query` |
| `boardwalk::events`  | Event bus, envelopes, topic matching, slow-consumer policies         |

The `#[actor]` and `#[transition]` proc macros ship in the separate
[`boardwalk-macros`] crate (a Rust requirement — proc-macro crates can't
live alongside non-macro code) and are re-exported by `boardwalk`.

[`boardwalk`]: https://crates.io/crates/boardwalk
[`boardwalk-macros`]: https://crates.io/crates/boardwalk-macros

## Docs

- [Getting started](docs/getting-started.md) — run the Resource/Actor
  LED example and drive it through `/resources`.
- [Resources and actors](docs/resources.md) — `Resource`, `Actor`,
  `Node`, `ResourceSnapshot`, transitions, streams, and the
  job-runner example.
- [Events](docs/events.md) — multiplex WebSocket and NDJSON event
  wire protocol.
- [Peers](docs/peers.md) — reverse-tunnel hub ↔ cloud setup.
- [CaQL](docs/caql.md) — query DSL reference.

## License

Apache-2.0. See [LICENSE](LICENSE).
