//! Confirms the reverse-index loop:
//!   - publish a state transition
//!   - recv the envelope
//!   - resolve its `event_id` to a `StreamId` via `StreamRegistry`
//!   - query the replay cache for the same envelope by `(stream, seq)`
//!
//! A future durable-history repository will replace this in-process
//! wiring.

use std::sync::Arc;

use boardwalk::events::{SubscribeOpts, TopicPattern};
use boardwalk::http::{Core, CoreBuilder};
use boardwalk::runtime::RequestCtx;
use boardwalk::{Device, DeviceConfig, DeviceError, TransitionInput};
use futures::future::BoxFuture;
use uuid::Uuid;

#[derive(Default)]
struct Led {
    on: bool,
}

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .name("LED")
            .state(if self.on { "on" } else { "off" })
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
                "turn-on" => {
                    self.on = true;
                    Ok(())
                }
                "turn-off" => {
                    self.on = false;
                    Ok(())
                }
                other => Err(DeviceError::Invalid(format!("unknown {other}"))),
            }
        })
    }
}

async fn boot() -> (Arc<Core>, Uuid) {
    let mut b = CoreBuilder::new("hub");
    let id = b.add_device(Led::default());
    let core = b.build();
    (core, id)
}

#[tokio::test]
async fn event_id_resolves_to_stream_via_registry() {
    let (core, id) = boot().await;
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&format!("hub/led/{id}/state")).unwrap(),
        SubscribeOpts::default(),
    );

    core.run_transition(
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
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&format!("hub/led/{id}/state")).unwrap(),
        SubscribeOpts::default(),
    );

    core.run_transition(
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
