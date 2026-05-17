# Getting started

This walks through running a Boardwalk server with a single device, then
hitting it with curl and a WebSocket client. Five minutes end-to-end.

## Install

```toml
[dependencies]
boardwalk = "0.1"
futures = "0.3"
tokio = { version = "1", features = ["full"] }
anyhow = "1"
```

## A minimal driver

A device is a state machine plus a name, type, and a set of transitions
allowed per state. The `Device` trait below is everything you need to
serve an LED.

```rust,no_run
use boardwalk::{Boardwalk, Device, DeviceConfig, DeviceError, TransitionInput};
use futures::future::BoxFuture;

#[derive(Default)]
struct Led { on: bool }

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .name("kitchen")
            .state(self.state())
            .when("off", &["turn-on"])
            .when("on", &["turn-off"])
            .monitor("state");
    }

    fn state(&self) -> &str {
        if self.on { "on" } else { "off" }
    }

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

Run it:

```
cargo run
```

## Poke at it

The server speaks Siren hypermedia (`application/vnd.siren+json`).
Discover the device, then drive a transition:

```
curl -s http://127.0.0.1:1337/servers/hub | jq .
DEV=$(curl -s http://127.0.0.1:1337/servers/hub | jq -r '.entities[0].properties.id')

# read current state
curl -s http://127.0.0.1:1337/servers/hub/devices/$DEV | jq '.properties.state'
# "off"

# transition it
curl -s -d 'action=turn-on' http://127.0.0.1:1337/servers/hub/devices/$DEV \
  | jq '.properties.state'
# "on"
```

Transition payloads are form-urlencoded. The required field is `action`;
anything else becomes the transition's input.

## Subscribe to events

The server exposes a multiplexed WebSocket endpoint at `/events`. Any
device property declared via `.monitor(...)` is published when it
changes. With a CLI like `wscat`:

```
wscat -c ws://127.0.0.1:1337/events
> {"type":"subscribe","topic":"hub/led/<device-id>/state"}
< {"type":"subscribe-ack", ...}
< {"type":"event","topic":"hub/led/<device-id>/state","data":"on", ...}
```

Topic patterns support wildcards and regex — see [Devices](devices.md).

## Where next

- [Devices](devices.md) — full `Device` trait reference, properties,
  streams, scouts, apps.
- [Peers](peers.md) — hub-to-cloud reverse-tunnel federation.
- [CaQL](caql.md) — query DSL for filtering and projecting devices.
