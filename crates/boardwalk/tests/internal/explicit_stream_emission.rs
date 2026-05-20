//! Stream emission is explicit: declared streams do not synthesize events.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value as Json, json};

use crate::events::{SubscribeOpts, TopicPattern};
use crate::runtime::{
    Actor, DynFuture, NodeBuilder, NodeHandle, Resource, ResourceCtx, ResourceError,
    ResourceSnapshot, ResourceSpec, StreamKind, StreamSpec, TransitionCtx, TransitionError,
    TransitionInput, TransitionOutcome,
};

#[derive(Default)]
struct SilentLed {
    on: bool,
}

impl SilentLed {
    fn snapshot(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            id: "ignored".into(),
            kind: "led".into(),
            name: Some("LED".into()),
            state: Some(if self.on { "on".into() } else { "off".into() }),
            node: "ignored".into(),
            properties: Map::new(),
            labels: BTreeMap::new(),
            transitions: Vec::new(),
            streams: Vec::new(),
            revision: None,
            metadata: Map::new(),
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
        let snapshot = self.snapshot();
        Box::pin(async move { Ok(snapshot) })
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
                        snapshot: self.snapshot(),
                    })
                }
                other => Err(TransitionError::NotAllowed(other.into())),
            }
        })
    }
}

#[derive(Default)]
struct PublishingLed {
    on: bool,
}

impl PublishingLed {
    fn snapshot(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            id: "ignored".into(),
            kind: "led".into(),
            name: Some("LED".into()),
            state: Some(if self.on { "on".into() } else { "off".into() }),
            node: "ignored".into(),
            properties: Map::new(),
            labels: BTreeMap::new(),
            transitions: Vec::new(),
            streams: Vec::new(),
            revision: None,
            metadata: Map::new(),
        }
    }
}

impl Resource for PublishingLed {
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
        let snapshot = self.snapshot();
        Box::pin(async move { Ok(snapshot) })
    }
}

impl Actor for PublishingLed {
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
                        snapshot: self.snapshot(),
                    })
                }
                other => Err(TransitionError::NotAllowed(other.into())),
            }
        })
    }
}

async fn led_proxy(handle: &NodeHandle, id: &str) -> crate::runtime::ResourceProxy {
    handle
        .query("where kind = \"led\"")
        .await
        .expect("CaQL parses")
        .into_iter()
        .find(|p| p.id() == id)
        .expect("led proxy")
}

#[tokio::test]
async fn declared_stream_without_publish_does_not_emit_state_change() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    let id = node
        .register_actor(SilentLed::default())
        .await
        .expect("register actor");

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
        "no event must arrive when the actor never publishes; got {result:?}"
    );
}

#[tokio::test]
async fn explicit_publish_emits_state_change() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    let id = node
        .register_actor(PublishingLed::default())
        .await
        .expect("register actor");

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
    assert_eq!(env.data, Json::String("on".into()));
}
