# Getting started

The fastest way to touch the current Boardwalk API is the Resource /
Actor LED fixture. It builds a `Node`, registers an `Actor`, queries the
resource directory, invokes a transition, and reads the explicitly
published state event.

## Install

```toml
[dependencies]
boardwalk = "0.2"
tokio = { version = "1", features = ["full"] }
anyhow = "1"
```

## Run the workspace LED actor

From this workspace:

```sh
cargo run -p hello-led
```

The example uses `boardwalk_mock_led`, a workspace fixture from
`examples/hello-led/boardwalk-mock-led`. That fixture is not published as
an external crate; the code below is a tour of the example runtime flow,
not an external dependency block.

The example performs the same steps an application would:

```rust,ignore
use std::sync::Arc;

use boardwalk::TransitionInput;
use boardwalk::events::{SubscribeOpts, TopicPattern};
use boardwalk::runtime::{NodeBuilder, NodeHandle};
use boardwalk_mock_led::Led;

async fn example() -> anyhow::Result<()> {
    let node = Arc::new(NodeBuilder::new("hub").build());
    let id = node.register_actor(Led::default()).await?;

    let topic = format!("hub/led/{id}/state");
    let mut events = node.events().subscribe(
        TopicPattern::parse(&topic)?,
        SubscribeOpts::default(),
    );

    let handle = NodeHandle::new(node.clone());
    let led = handle
        .query("where kind = \"led\"")
        .await?
        .into_iter()
        .find(|resource| resource.id() == id)
        .expect("registered LED is queryable");

    led.transition("turn-on", TransitionInput::default()).await?;
    let event = events.rx.recv().await.expect("state event");
    assert_eq!(event.data, serde_json::json!("on"));
    Ok(())
}
```

The fixture's `Led` type implements `Resource` and uses `#[actor]`
transition dispatch. Its transition handler mutates state, calls
`TransitionCtx::publish("state", "resource.state.changed", 1, ...)`,
and returns `TransitionOutcome::Completed`.

## HTTP resource entry points

The current local resource entry points are:

```sh
GET  /resources
GET  /resources/{id}
POST /resources/{id}/transitions/{transition}
```

Clients should enter through the Siren representation and follow link
relations and action metadata instead of treating these paths as the
protocol boundary. `GET /resources?ql=<caql>` filters the collection
with CaQL. Transition requests use JSON input, and transition events
include `causationId` from the transition command plus `correlationId`
when the request carried `x-request-id`.

The reusable Boardwalk HTTP router is still being moved onto the actor
runtime. The `examples/job-runner` package owns an example-local HTTP
adapter today so the resource route flow can be exercised end to end.

## Where next

- [Resources and actors](resources.md) — `Resource`, `Actor`, `Node`,
  snapshots, transitions, streams, and the job-runner example.
- [Events](events.md) — event envelopes, slow-consumer policies, and
  the WebSocket / NDJSON wire protocols.
- [CaQL](caql.md) — query DSL for filtering and projecting resources.
- [Peers](peers.md) — hub-to-cloud reverse-tunnel federation.
