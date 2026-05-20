//! Pins the runtime-side `NodeHandle` / `ResourceProxy` / `ActorProxy`
//! surface for app-facing Resource/Actor access.

use std::collections::BTreeMap;
use std::sync::Arc;

use boardwalk::runtime::query::Query;
use boardwalk::runtime::{
    Actor, DynFuture, NodeBuilder, NodeHandle, Resource, ResourceCtx, ResourceError, TransitionCtx,
    TransitionError,
};
use boardwalk::{ResourceSnapshot, ResourceSpec, TransitionInput, TransitionOutcome};

#[derive(Default)]
struct Counter {
    n: u32,
}

impl Resource for Counter {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "counter".into(),
            name: None,
            labels: BTreeMap::new(),
            property_schema: None,
            streams: vec![],
        }
    }
    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move {
            let mut props = serde_json::Map::new();
            props.insert("n".into(), serde_json::Value::from(self.n));
            Ok(ResourceSnapshot {
                id: "ignored".into(),
                kind: "counter".into(),
                name: None,
                state: Some("ready".into()),
                node: "test".into(),
                properties: props,
                labels: BTreeMap::new(),
                transitions: vec![],
                streams: vec![],
                revision: None,
                metadata: serde_json::Map::new(),
            })
        })
    }
}

impl Actor for Counter {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(
            async move { Err::<TransitionOutcome, _>(TransitionError::NotAllowed("stub".into())) },
        )
    }
}

#[tokio::test]
async fn node_handle_query_returns_resource_proxy() {
    let node = Arc::new(NodeBuilder::new("runner").build());
    let _ = node.register_actor(Counter { n: 1 }).await.unwrap();
    let _ = node.register_actor(Counter { n: 2 }).await.unwrap();

    let handle = NodeHandle::new(node);
    let matches = handle
        .query(r#"where kind = "counter""#)
        .await
        .expect("query parses");
    assert_eq!(matches.len(), 2);
}

#[tokio::test]
async fn resource_proxy_snapshot_returns_resource_snapshot() {
    let node = Arc::new(NodeBuilder::new("runner").build());
    let id = node.register_actor(Counter { n: 7 }).await.unwrap();
    let handle = NodeHandle::new(node);

    let proxies = handle.query(r#"where kind = "counter""#).await.unwrap();
    let proxy = proxies.into_iter().next().unwrap();
    let snap = proxy.snapshot().await.expect("snapshot succeeds");
    assert_eq!(snap.id, id);
    assert_eq!(snap.kind, "counter");
    assert_eq!(snap.properties.get("n"), Some(&serde_json::json!(7)));
}

#[tokio::test]
async fn actor_proxy_transition_returns_transition_outcome() {
    let node = Arc::new(NodeBuilder::new("runner").build());
    let _ = node.register_actor(Counter::default()).await.unwrap();
    let handle = NodeHandle::new(node);

    let proxy = handle
        .query(r#"where kind = "counter""#)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let result = proxy.transition("noop", TransitionInput::default()).await;
    match result {
        Err(TransitionError::NotAllowed(_)) => {}
        other => panic!("expected NotAllowed, got {other:?}"),
    }
}

#[test]
fn query_types_are_reachable_from_runtime_public_surface() {
    let q: Query = Default::default();
    let _: &Query = &q;
}
