//! `Core::run_transition` mints `resource.state.changed` envelopes
//! through the shared `StreamRegistry`.

use std::sync::Arc;

use boardwalk::events::{SubscribeOpts, TopicPattern};
use boardwalk::http::{Core, CoreBuilder};
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

async fn boot_with_led() -> (Arc<Core>, Uuid) {
    let mut b = CoreBuilder::new("hub");
    let id = b.add_device(Led::default());
    let core = b.build();
    (core, id)
}

#[tokio::test]
async fn state_transition_publishes_envelope_with_resource_state_changed_kind() {
    let (core, id) = boot_with_led().await;
    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    core.run_transition(&id, "turn-on", TransitionInput::default())
        .await
        .expect("turn-on succeeds");

    let env = sub.rx.recv().await.expect("state-change envelope");
    assert_eq!(env.payload_kind, "resource.state.changed");
    assert_eq!(env.payload_version, 1);
    assert_eq!(env.data, serde_json::Value::String("on".to_string()));
}

#[tokio::test]
async fn successive_state_transitions_get_strictly_increasing_sequence() {
    let (core, id) = boot_with_led().await;
    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    for name in ["turn-on", "turn-off", "turn-on"] {
        core.run_transition(&id, name, TransitionInput::default())
            .await
            .expect("transition succeeds");
    }

    let a = sub.rx.recv().await.unwrap();
    let b = sub.rx.recv().await.unwrap();
    let c = sub.rx.recv().await.unwrap();
    assert_eq!(a.sequence, 1);
    assert_eq!(b.sequence, 2);
    assert_eq!(c.sequence, 3);
}

/// Pins the current causation gap: `Core::run_transition` does not
/// thread correlation/causation/trace context onto the envelope it
/// mints. The context-publish work flips this to populated and
/// updates this snapshot.
#[tokio::test]
async fn current_state_transition_envelope_has_empty_causation_chain() {
    let (core, id) = boot_with_led().await;
    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    core.run_transition(&id, "turn-on", TransitionInput::default())
        .await
        .unwrap();

    let env = sub.rx.recv().await.expect("state-change envelope");
    assert!(env.correlation_id.is_none());
    assert!(env.causation_id.is_none());
    assert!(env.trace_context.is_none());
}

#[tokio::test]
async fn state_transition_envelope_stream_id_uses_bw_uri_scheme() {
    let (core, id) = boot_with_led().await;
    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    core.run_transition(&id, "turn-on", TransitionInput::default())
        .await
        .unwrap();
    let env = sub.rx.recv().await.unwrap();
    let stream_id = env.stream_id.as_str();
    assert!(
        stream_id.starts_with("bw://hub/resources/"),
        "expected bw://hub/resources/... prefix; got {stream_id}"
    );
    assert!(
        stream_id.ends_with("/streams/state"),
        "expected /streams/state suffix; got {stream_id}"
    );
}
