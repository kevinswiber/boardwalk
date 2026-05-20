//! Pins for the `Node` runtime and its `ResourceDirectory`.

use std::collections::BTreeMap;
use std::sync::Arc;

use boardwalk::runtime::{
    Actor, DynFuture, NodeBuilder, RequestCtx, Resource, ResourceCtx, ResourceError, TransitionCtx,
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
        Box::pin(async move {
            Err::<TransitionOutcome, _>(TransitionError::NotAllowed("test stub".into()))
        })
    }
}

#[tokio::test]
async fn node_builder_builds_named_node_with_shared_event_bus_and_directory() {
    let node = NodeBuilder::new("runner").build();
    assert_eq!(node.id(), "runner");
    assert!(node.resources().await.is_empty());
    let registry_via_node = node.stream_registry();
    let registry_via_bus = node.events().stream_registry();
    assert!(
        registry_via_node.same_instance(registry_via_bus),
        "events bus and node must share one StreamRegistry"
    );
}

#[tokio::test]
async fn resource_directory_rejects_duplicate_ids() {
    let node = NodeBuilder::new("runner").build();
    let id = node
        .register_actor(Counter::default())
        .await
        .expect("first register succeeds");
    let err = node
        .register_with_id(id.clone(), Counter::default())
        .await
        .expect_err("duplicate id is rejected");
    match err {
        ResourceError::Internal(msg) => assert!(msg.contains("duplicate")),
        other => panic!("expected Internal(duplicate), got {other:?}"),
    }
}

#[tokio::test]
async fn resource_directory_lists_snapshots_in_registration_order() {
    let node = NodeBuilder::new("runner").build();
    let id_a = node.register_actor(Counter { n: 1 }).await.unwrap();
    let id_b = node.register_actor(Counter { n: 2 }).await.unwrap();
    let id_c = node.register_actor(Counter { n: 3 }).await.unwrap();

    let listed = node.resources().await;
    let ids: Vec<String> = listed.iter().map(|r| r.id.clone()).collect();
    assert_eq!(ids, vec![id_a, id_b, id_c]);
    assert_eq!(listed[0].properties.get("n"), Some(&serde_json::json!(1)));
    assert_eq!(listed[2].properties.get("n"), Some(&serde_json::json!(3)));
}

#[tokio::test]
async fn transition_ctx_register_actor_routes_through_node_directory() {
    let node = Arc::new(NodeBuilder::new("runner").build());
    let ctx = TransitionCtx::with_node(RequestCtx::default(), node.clone());
    let id = ctx
        .register_actor(Counter::default())
        .await
        .expect("registration through node should succeed");

    let listed = node.resources().await;
    let found = listed.iter().any(|r| r.id == id);
    assert!(found, "node should immediately reflect the new resource");
}
