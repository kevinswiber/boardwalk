//! Confirms the reverse-index loop:
//!   - publish a state transition
//!   - recv the envelope
//!   - resolve its `event_id` to a `StreamId` via `StreamRegistry`
//!   - query the replay cache for the same envelope by `(stream, seq)`
//!
//! A future durable-history repository will replace this in-process
//! wiring.

use std::sync::Arc;

use super::actor_led_fixture::ActorLed;
use crate::events::{SubscribeOpts, TopicPattern};
use crate::http::Core;
use crate::runtime::{NodeBuilder, RequestCtx, TransitionInput};

async fn boot() -> (Arc<Core>, String) {
    let id = "actor-led".to_string();
    let node = Arc::new(
        NodeBuilder::new("hub")
            .register_with_id(id.clone(), ActorLed::default())
            .expect("actor registers")
            .build(),
    );
    (Core::from_node(node), id)
}

#[tokio::test]
async fn event_id_resolves_to_stream_via_registry() {
    let (core, id) = boot().await;
    let mut sub = core.subscribe_events(
        TopicPattern::parse(&format!("hub/led/{id}/state")).unwrap(),
        SubscribeOpts::default(),
    );

    core.run_resource_transition(
        &id,
        "turn-on",
        TransitionInput::default(),
        RequestCtx::default(),
    )
    .await
    .unwrap();

    let env = sub.rx.recv().await.expect("envelope delivered");
    assert_eq!(
        core.stream_registry.stream_for(&env.event_id),
        Some(env.stream_id.clone())
    );
}

#[tokio::test]
async fn event_id_then_replay_cache_returns_origin_envelope() {
    let (core, id) = boot().await;
    let mut sub = core.subscribe_events(
        TopicPattern::parse(&format!("hub/led/{id}/state")).unwrap(),
        SubscribeOpts::default(),
    );

    core.run_resource_transition(
        &id,
        "turn-on",
        TransitionInput::default(),
        RequestCtx::default(),
    )
    .await
    .unwrap();

    let env = sub.rx.recv().await.unwrap();
    let stream_id = core
        .stream_registry
        .stream_for(&env.event_id)
        .expect("reverse-index has the event_id");
    let events = core
        .bus
        .replay_cache()
        .events_after(&stream_id, env.sequence - 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_id, env.event_id);
    assert_eq!(events[0].sequence, env.sequence);
}
