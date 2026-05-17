# Architecture

## Crate layout (cargo workspace)

```
zetta-rs/
├── Cargo.toml               # workspace
├── crates/
│   ├── zetta-core/          # Device, Scout, App traits; runtime types
│   ├── zetta-siren/         # Siren types (Entity/Link/Action/Field) + serde
│   ├── zetta-caql/          # CaQL parser + evaluator
│   ├── zetta-events/        # Pub/sub bus + multiplexed WS sub-protocol
│   ├── zetta-registry/      # Device + peer registries (redb-backed)
│   ├── zetta-http/          # Axum router emitting Siren over HTTP/1.1 & HTTP/2
│   ├── zetta-peer/          # Outbound peer client + inbound peer socket
│   ├── zetta-tunnel/        # WS-upgrade → role-reversed h2 tunnel primitive
│   ├── zetta-server/        # Top-level builder (`Zetta::new().use_(...).listen()`)
│   └── zetta/               # Re-export façade crate
├── drivers/
│   └── zetta-mock-led/      # Sample driver for end-to-end testing
├── examples/
│   ├── hello-led/           # Mock LED server
│   └── peer-link/           # Two processes linking
└── docs/
```

The split into many small crates is deliberate: it keeps cyclic
dependencies impossible, makes individual pieces testable in isolation,
and lets users depend on `zetta-core` to write a driver without pulling
in axum or h2.

## Dependency direction

```
zetta            (façade)
  └─ zetta-server
       ├─ zetta-http     ──┐
       │    └─ zetta-siren │   ┌─ zetta-events
       ├─ zetta-peer    ──┼───┤
       │    └─ zetta-tunnel  │   ┌─ zetta-registry
       ├─ zetta-events   ───┘   │
       ├─ zetta-registry  ──────┘
       └─ zetta-core   (used by everything above and by drivers)

zetta-caql is leaf, used by zetta-events (topic filters) and zetta-http
  (`?ql=` query string parsing).
zetta-siren is leaf, used by zetta-http only.
```

No cycles. `zetta-core` deliberately has zero dependencies on transport
or storage — it defines `Device`, `Scout`, `App`, `Transition`,
`StreamHandle`, and a small runtime trait so drivers can compile without
the rest of the world.

## Key types (sketch)

### `zetta-core`

```rust
pub trait Device: Send + Sync + 'static {
    fn init(&mut self, cfg: &mut DeviceConfig);
}

pub struct DeviceConfig {
    // chainable: .type_("led").state("off").name("..").when(...).map(..)
}

#[async_trait]
pub trait Transition: Send + Sync {
    type Input: DeserializeOwned + Send;
    type Output: Serialize + Send;
    async fn run(&self, dev: &mut DeviceState, input: Self::Input)
        -> Result<Self::Output, TransitionError>;
}

pub trait Scout: Send + Sync + 'static {
    fn init(self, ctx: ScoutCtx) -> BoxFuture<'static, Result<()>>;
}

pub trait App: Send + Sync + 'static {
    fn init(self, server: ServerHandle) -> BoxFuture<'static, Result<()>>;
}
```

Open question: do we make `Device` a trait or a builder-only construct
(value type with attached transition closures)? See
[09-questions.md](09-questions.md) Q1.

### `zetta-events`

A single `EventBus` per server instance. Topics are strings parsed into
`StreamTopic { server, device_type, device_id, stream }`. Subscriptions
hold `tokio::sync::mpsc::Sender<Event>`. Topic matching supports
wildcards (`*`, `**`), regex (`{...}`), and trailing `?ql=...` CaQL
filters as in the original.

### `zetta-tunnel`

The piece that replaces node-spdy's role swap:

```rust
/// Initiator side: we opened the outbound WebSocket.
/// After the upgrade, *we* host the HTTP/2 server.
pub async fn initiator_into_h2_server<S>(stream: S, /*...*/)
    -> Result<h2::server::Connection<S, bytes::Bytes>>
where S: AsyncRead + AsyncWrite + Unpin + Send + 'static;

/// Acceptor side: we received the WebSocket upgrade.
/// After it, *we* are the HTTP/2 client driving requests at the initiator.
pub async fn acceptor_into_h2_client<S>(stream: S, /*...*/)
    -> Result<(h2::client::SendRequest<bytes::Bytes>,
              h2::client::Connection<S, bytes::Bytes>)>
where S: AsyncRead + AsyncWrite + Unpin + Send + 'static;
```

The `S` is the raw TCP (or TLS) stream extracted via
`hyper::upgrade::on(response).await`. Critically, we **do not** keep
WebSocket framing past the 101 Switching Protocols response — matching
the original protocol which states "After this handshake completes, the
server-to-server protocol no longer uses WebSocket protocol framing".
We use WS as a tunnel-establishment fiction so HTTP-aware proxies and
firewalls let the connection through.

### `zetta-peer`

```rust
pub struct PeerClient { /* outbound; backoff, reconnect */ }
pub struct PeerSocket { /* inbound; tracks subscriptions */ }
```

`PeerClient` holds an `h2::server::Connection` (after upgrade) and a
`hyper::server::conn::http2::Builder` configured to serve the local
axum `Router`. Routing the inbound request `GET /_initiate_peer/{id}`
is what completes the handshake.

`PeerSocket` holds a `h2::client::SendRequest`. To stream events to the
acceptor, the original used SPDY server push. h2 server push has been
deprecated in browsers but is still functional in the `h2` crate — and
we control both sides. We have a design choice here, recorded in
[09-questions.md](09-questions.md) Q3:

- (a) Use h2 server push exactly as SPDY server push was used.
- (b) Use long-lived response bodies — the acceptor's `GET /servers/X/events`
  to the initiator returns a streaming response body chunked with event
  records. This is simpler, idiomatic, and doesn't depend on a deprecated
  feature.

Recommendation: (b). It is what most modern reverse-tunnel projects do
and the `h2` library makes it ergonomic.

### `zetta-server`

```rust
pub struct Zetta { /* fields private */ }

impl Zetta {
    pub fn new() -> Self;
    pub fn name(self, name: impl Into<String>) -> Self;
    pub fn use_<P: ZettaPlugin>(self, p: P) -> Self;
    pub fn link(self, url: impl Into<String>) -> Self;
    pub async fn listen(self, addr: SocketAddr) -> Result<()>;
}

pub trait ZettaPlugin {
    fn install(self, b: &mut Builder);
}

// Blanket impls so .use_(MyDevice) and .use_(MyApp) and .use_(MyScout)
// all work via type-class dispatch.
```

## Runtime topology (single process)

```
                ┌────────────────────────────────────────────┐
                │              Tokio runtime                  │
                │                                              │
   HTTP client ─┼──► Hyper/Axum ──► Router ──► Resource handlers
                │                       │           │
                │                       │           ▼
   WS client  ──┼──► Axum WS ──► EventBus ──► Device state machines
                │                       ▲           ▲
                │                       │           │
   Peer (in)  ──┼──► WS upgrade ──► h2::client (acceptor side)
                │                       │
                │                       ▼
                │     (acceptor sends HTTP requests to initiator)
                │
   Peer (out) ──┼──► WS connect ──► h2::server (initiator side)
                │                       │
                │                       ▼
                │      Router (same one!) serves the inbound H2 requests
                └────────────────────────────────────────────┘
```

The single `axum::Router` (or equivalent `tower::Service`) is the source
of truth for routing. Both:
- The outward-facing HTTP/1.1 + HTTP/2 listener (axum's normal path).
- The reverse-tunneled HTTP/2 from a peer acceptor

…are served by the same `Service`. This is what gives "your devices
appear at the cloud" its simplicity — the cloud is just an HTTP client
to your router, transported over a connection it didn't open.
