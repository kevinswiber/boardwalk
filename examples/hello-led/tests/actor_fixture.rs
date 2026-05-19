use std::sync::Arc;
use std::time::Duration;

use boardwalk::core::{TransitionInput, TransitionOutcome};
use boardwalk::events::{SubscribeOpts, TopicPattern};
use boardwalk::runtime::{Actor, NodeBuilder, NodeHandle, Resource, TransitionError};
use boardwalk_mock_led::Led;
use serde_json::json;

fn assert_actor_fixture<T: Actor + Resource>() {}

#[test]
fn hello_led_fixture_builds_with_actor_api() {
    assert_actor_fixture::<Led>();

    let source = include_str!("../src/main.rs");
    assert!(
        source.contains("register_actor"),
        "hello-led should register the fixture through the actor runtime"
    );
    assert!(
        !source.contains("use_device"),
        "hello-led should not use the legacy device builder"
    );
}

#[tokio::test]
async fn led_state_change_uses_explicit_publish() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    let id = node
        .register_actor(Led::default())
        .await
        .expect("register LED actor");

    let topic = format!("hub/led/{id}/state");
    let mut sub = node.events().subscribe(
        TopicPattern::parse(&topic).expect("topic parses"),
        SubscribeOpts::default(),
    );

    let handle = NodeHandle::new(node);
    let proxy = handle
        .query("where kind = \"led\"")
        .await
        .expect("query parses")
        .into_iter()
        .find(|resource| resource.id() == id)
        .expect("registered LED is queryable");

    let outcome = proxy
        .transition("turn-on", TransitionInput::default())
        .await
        .expect("turn-on succeeds");
    match outcome {
        TransitionOutcome::Completed { snapshot, .. } => {
            assert_eq!(snapshot.id, id);
            assert_eq!(snapshot.node, "hub");
            assert_eq!(snapshot.kind, "led");
            assert_eq!(snapshot.state.as_deref(), Some("on"));
        }
        other => panic!("expected completed transition, got {other:?}"),
    }

    let env = tokio::time::timeout(Duration::from_secs(1), sub.rx.recv())
        .await
        .expect("state event arrives")
        .expect("subscription stays open");

    assert_eq!(env.resource_id, id);
    assert_eq!(env.resource_kind, "led");
    assert_eq!(env.stream, "state");
    assert_eq!(env.payload_kind, "resource.state.changed");
    assert_eq!(env.data, json!("on"));
}

#[tokio::test]
async fn led_turn_off_and_state_gates_are_pinned() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    let id = node
        .register_actor(Led::default())
        .await
        .expect("register LED actor");

    let topic = format!("hub/led/{id}/state");
    let mut sub = node.events().subscribe(
        TopicPattern::parse(&topic).expect("topic parses"),
        SubscribeOpts::default(),
    );

    let handle = NodeHandle::new(node);
    let proxy = handle
        .query("where kind = \"led\"")
        .await
        .expect("query parses")
        .into_iter()
        .find(|resource| resource.id() == id)
        .expect("registered LED is queryable");

    assert_not_allowed(
        proxy
            .transition("turn-off", TransitionInput::default())
            .await,
        "turn-off should be gated while off",
    );

    proxy
        .transition("turn-on", TransitionInput::default())
        .await
        .expect("turn-on succeeds");
    let on = tokio::time::timeout(Duration::from_secs(1), sub.rx.recv())
        .await
        .expect("on event arrives")
        .expect("subscription stays open");
    assert_eq!(on.data, json!("on"));

    assert_not_allowed(
        proxy
            .transition("turn-on", TransitionInput::default())
            .await,
        "turn-on should be gated while on",
    );

    let outcome = proxy
        .transition("turn-off", TransitionInput::default())
        .await
        .expect("turn-off succeeds");
    match outcome {
        TransitionOutcome::Completed { snapshot, .. } => {
            assert_eq!(snapshot.id, id);
            assert_eq!(snapshot.node, "hub");
            assert_eq!(snapshot.kind, "led");
            assert_eq!(snapshot.state.as_deref(), Some("off"));
        }
        other => panic!("expected completed transition, got {other:?}"),
    }

    let off = tokio::time::timeout(Duration::from_secs(1), sub.rx.recv())
        .await
        .expect("off event arrives")
        .expect("subscription stays open");
    assert_eq!(off.resource_id, id);
    assert_eq!(off.resource_kind, "led");
    assert_eq!(off.stream, "state");
    assert_eq!(off.payload_kind, "resource.state.changed");
    assert_eq!(off.data, json!("off"));
}

fn assert_not_allowed(result: Result<TransitionOutcome, TransitionError>, context: &str) {
    let err = result.expect_err(context);
    assert!(
        matches!(err, TransitionError::NotAllowed(_)),
        "{context}: expected NotAllowed, got {err:?}"
    );
}
