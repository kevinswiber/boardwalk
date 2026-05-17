# Public API ergonomics (Rust)

This is the developer-facing surface the original sells with:

```js
boardwalk()
  .name('hub')
  .use(LED)
  .link('https://hello-boardwalk.example.com/')
  .listen(1337)
```

Goal: keep that feel, type-safely.

## Server bootstrap

```rust
use boardwalk::Boardwalk;
use boardwalk_mock_led::Led;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Boardwalk::new()
        .name("hub")
        .use_(Led::default())
        .link("https://hello-boardwalk.example.com/")
        .listen("0.0.0.0:1337").await
}
```

`use_` is generic over a `BoardwalkPlugin` trait. Blanket impls cover
`Device`, `Scout`, and `App`, so the caller never types the trait
explicitly.

## Defining a Device — the recommended path

```rust
use boardwalk::{device, Device, Transition};
use serde::{Serialize, Deserialize};

#[derive(Default)]
pub struct Led { pub state: LedState }

#[derive(Serialize, Deserialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LedState {
    #[default] Off,
    On,
}

#[device]
impl Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
           .state(self.state)
           .when(LedState::Off, &["turn-on"])
           .when(LedState::On,  &["turn-off"])
           .monitor("state");
    }

    #[transition]
    async fn turn_on(&mut self) -> Result<()> {
        self.state = LedState::On;
        Ok(())
    }

    #[transition]
    async fn turn_off(&mut self) -> Result<()> {
        self.state = LedState::Off;
        Ok(())
    }
}
```

`#[device]` is a macro from `boardwalk-macros`. It:

1. Generates a `Device for Led` impl that wires `config` and registers
   each `#[transition]` method by its kebab-case name (`turn_on` →
   `turn-on`).
2. Inspects each transition signature, derives Siren fields from its
   inputs (excluding `&mut self`), and validates the inputs against
   `serde_json` deserialization at request-handle time.
3. Generates the metadata response at `/servers/{name}/meta/{type}`.

This is the macro-heavy ergonomic path. There is also a builder-only
non-macro path for users who don't want proc macros (see Q1 in
questions).

## Defining an App

```rust
use boardwalk::{app, App, ServerHandle, query};

#[app]
async fn dusk_to_dawn(server: ServerHandle) -> anyhow::Result<()> {
    let photocell = server.query("where type = 'photocell'").await?.one();
    let led       = server.query("where type = 'led'").await?.one();

    photocell.stream("intensity").for_each(|m| async {
        if m.data::<f64>()? < 0.5 {
            if led.available("turn-on") { led.call("turn-on").await?; }
        } else {
            if led.available("turn-off") { led.call("turn-off").await?; }
        }
        Ok(())
    }).await?;

    Ok(())
}

// Usage:
Boardwalk::new()
    .name("hub")
    .use_(Led::default())
    .use_(Photocell::default())
    .use_(dusk_to_dawn)
    .listen("0.0.0.0:1337").await?;
```

`server.query("...")` parses CaQL once and returns a `Query`, which
yields a `Vec<DeviceProxy>` or a single device (`.one()` panics if not
exactly one). `server.observe([q1, q2], async fn)` is the more general
form when the app is composing multiple device queries.

## Streaming a value out of a Device

```rust
#[device]
impl Photocell {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("photocell")
           .state("ready")
           .stream("intensity", |handle: StreamHandle<f64>| async move {
               let mut tick = tokio::time::interval(Duration::from_secs(1));
               loop {
                   tick.tick().await;
                   handle.send(rand::random::<f64>())?;
               }
           });
    }
}
```

A `StreamHandle<T>` is a typed sender; type info propagates to the
client metadata API.

## Linking

```rust
Boardwalk::new()
    .name("hub")
    .link("https://cloud.example.com")
    .listen(...)
```

- One or more `link(url)` calls register outbound peers.
- Each peer runs a `PeerClient` with the backoff/keepalive described
  in [02-protocol-peer.md](02-protocol-peer.md).
- Inbound peers (acceptors) are wired automatically: any incoming WS
  upgrade on `/peers/{name}` is treated as a peer handshake.

## Errors

Driver authors return `Result<T, boardwalk::DeviceError>` from transitions.
`DeviceError` is a small enum (`Invalid`, `Conflict`, `Internal`)
that maps to HTTP statuses (400, 409, 500). For app authors,
`anyhow::Result` is the default ergonomic.

## Builder vs macros

The macro path (`#[device]`, `#[transition]`) is the recommended one
but never required. The same `Led` can be written as:

```rust
struct Led { state: LedState }

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
           .state(self.state)
           .when(LedState::Off, &["turn-on"])
           .when(LedState::On,  &["turn-off"])
           .map_async("turn-on", |this: &mut Led| async move {
               this.state = LedState::On; Ok(())
           })
           .map_async("turn-off", |this: &mut Led| async move {
               this.state = LedState::Off; Ok(())
           });
    }
}
```

The macro version is strictly preferable because it gives compile-time
validation that `when` references states that exist and that the
mapped methods take inputs matching the declared fields — but the
escape hatch must exist.
