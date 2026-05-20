//! Stream contract: events are emitted only when the producer explicitly
//! publishes through `ActorCtx::publish` / `TransitionCtx::publish`. The
//! legacy device-side test (`device_publishes_to_declared_stream`)
//! exercises the same shape over the multiplex WS, and two
//! actor-pathway tests below pin the contract that a declared stream
//! does not auto-publish on a state diff.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use boardwalk::Boardwalk;
use boardwalk::core::{
    Device, DeviceConfig, DeviceCtx, DeviceError, ResourceSpec, StreamKind, StreamSpec,
    TransitionInput, TransitionOutcome,
};
use boardwalk::events::{SubscribeOpts, TopicPattern};
use boardwalk::http::ResourceSnapshot;
use boardwalk::runtime::{
    Actor, DynFuture, NodeBuilder, NodeHandle, Resource, ResourceCtx, ResourceError, TransitionCtx,
    TransitionError,
};
use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio_tungstenite::tungstenite::Message;

#[derive(Default)]
struct Photocell;

impl Device for Photocell {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("photocell")
            .name("Cell")
            .state("ready")
            .stream("intensity", StreamKind::Object);
    }
    fn state(&self) -> &str {
        "ready"
    }
    fn transition<'a>(
        &'a mut self,
        _name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async { Ok(()) })
    }
    fn on_start(&self, ctx: DeviceCtx) {
        tokio::spawn(async move {
            let mut counter = 0u32;
            loop {
                tokio::time::sleep(Duration::from_millis(30)).await;
                ctx.publish.publish("intensity", serde_json::json!(counter));
                counter += 1;
                if counter > 50 {
                    break;
                }
            }
        });
    }
}

#[tokio::test]
async fn device_publishes_to_declared_stream() {
    let built = Boardwalk::new()
        .name("hub")
        .use_actor(Photocell)
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, built.router).await.unwrap();
    });

    let server: Json = reqwest::get(format!("http://{addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = server["entities"][0]["properties"]["id"].as_str().unwrap();
    let topic = format!("hub/photocell/{id}/intensity");

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/events"))
        .await
        .unwrap();
    let sub = serde_json::json!({"type": "subscribe", "topic": topic});
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .unwrap();
    let _ack = ws.next().await.unwrap().unwrap();

    // Read one event.
    let evt = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("timeout")
        .unwrap()
        .unwrap();
    let evt: Json = match evt {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!(),
    };
    assert_eq!(evt["type"], "event");
    assert_eq!(evt["topic"], topic);
    assert!(evt["data"].is_number());
}

/// An actor that declares `state` in its `ResourceSpec::streams` but
/// never calls `ctx.publish`. Used to confirm that stream emission is
/// explicit: a state diff between snapshots must not synthesize an
/// envelope.
#[derive(Default)]
struct SilentLed {
    on: bool,
}

impl SilentLed {
    fn snap(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            id: "ignored".into(),
            kind: "led".into(),
            name: Some("LED".into()),
            state: Some(if self.on { "on".into() } else { "off".into() }),
            node: "ignored".into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: vec![],
            streams: vec![],
            revision: None,
            metadata: serde_json::Map::new(),
        }
    }
}

impl Resource for SilentLed {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some("LED".into()),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: vec![StreamSpec {
                name: "state".into(),
                kind: StreamKind::Object,
            }],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let snap = self.snap();
        Box::pin(async move { Ok(snap) })
    }
}

impl Actor for SilentLed {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            match name {
                "turn-on" => {
                    self.on = true;
                    Ok(TransitionOutcome::Completed {
                        output: None,
                        snapshot: self.snap(),
                    })
                }
                other => Err(TransitionError::NotAllowed(other.into())),
            }
        })
    }
}

/// An actor that declares `state` and emits an explicit
/// `resource.state.changed` envelope from inside `transition` via
/// `TransitionCtx::publish`. Used to confirm the explicit path is the
/// only path that produces an event.
#[derive(Default)]
struct EagerLed {
    on: bool,
}

impl EagerLed {
    fn snap(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            id: "ignored".into(),
            kind: "led".into(),
            name: Some("LED".into()),
            state: Some(if self.on { "on".into() } else { "off".into() }),
            node: "ignored".into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: vec![],
            streams: vec![],
            revision: None,
            metadata: serde_json::Map::new(),
        }
    }
}

impl Resource for EagerLed {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some("LED".into()),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: vec![StreamSpec {
                name: "state".into(),
                kind: StreamKind::Object,
            }],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let snap = self.snap();
        Box::pin(async move { Ok(snap) })
    }
}

impl Actor for EagerLed {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            match name {
                "turn-on" => {
                    self.on = true;
                    ctx.publish("state", "resource.state.changed", 1, json!("on"))
                        .await
                        .expect("explicit publish must succeed");
                    Ok(TransitionOutcome::Completed {
                        output: None,
                        snapshot: self.snap(),
                    })
                }
                other => Err(TransitionError::NotAllowed(other.into())),
            }
        })
    }
}

async fn led_proxy(handle: &NodeHandle, id: &str) -> boardwalk::runtime::ResourceProxy {
    handle
        .query("where kind = \"led\"")
        .await
        .expect("CaQL parses")
        .into_iter()
        .find(|p| p.id() == id)
        .expect("led proxy")
}

/// A declared stream alone must not synthesize a `resource.state.changed`
/// envelope when the actor never calls `ctx.publish`, even if the
/// returned snapshot's `state` differs from the prior snapshot's.
#[tokio::test]
async fn declared_stream_without_publish_does_not_emit_magic_state_diff() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    let id = node
        .register_actor(SilentLed::default())
        .await
        .expect("register SilentLed");

    let topic = format!("hub/led/{id}/state");
    let mut sub = node.events().subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    let handle = NodeHandle::new(node.clone());
    let proxy = led_proxy(&handle, &id).await;
    proxy
        .transition("turn-on", TransitionInput::default())
        .await
        .expect("transition succeeds");

    let result = tokio::time::timeout(Duration::from_millis(200), sub.rx.recv()).await;
    assert!(
        result.is_err(),
        "no event must arrive when actor never publishes; got {result:?}"
    );
}

/// When the actor explicitly calls `TransitionCtx::publish`, the
/// declared stream delivers the envelope to subscribers.
#[tokio::test]
async fn explicit_publish_emits_state_change() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    let id = node
        .register_actor(EagerLed::default())
        .await
        .expect("register EagerLed");

    let topic = format!("hub/led/{id}/state");
    let mut sub = node.events().subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    let handle = NodeHandle::new(node.clone());
    let proxy = led_proxy(&handle, &id).await;
    proxy
        .transition("turn-on", TransitionInput::default())
        .await
        .expect("transition succeeds");

    let env = tokio::time::timeout(Duration::from_secs(1), sub.rx.recv())
        .await
        .expect("envelope arrives")
        .expect("subscription open");
    assert_eq!(env.payload_kind, "resource.state.changed");
    assert_eq!(env.data, json!("on"));
}
