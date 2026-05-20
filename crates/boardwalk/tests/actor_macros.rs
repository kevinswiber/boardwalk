//! Smoke test for the `#[actor]` proc-macro.
//!
//! Verifies that the macro generates an `Actor` trait impl whose
//! `transition` method dispatches kebab-cased wire names to the
//! `#[transition]`-marked inherent methods on the user type.

use std::collections::BTreeMap;

use boardwalk::runtime::{
    Actor, DynFuture, Resource, ResourceCtx, ResourceError, TransitionCtx, TransitionError,
};
use boardwalk::{ResourceSnapshot, ResourceSpec, TransitionInput, TransitionOutcome};
use serde_json::json;

pub struct Led {
    pub on: bool,
}

impl Resource for Led {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            ..Default::default()
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let state = if self.on { "on" } else { "off" }.to_string();
        let snapshot = ResourceSnapshot {
            id: "led/test".into(),
            kind: "led".into(),
            name: None,
            state: Some(state),
            node: "test".into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: Vec::new(),
            streams: Vec::new(),
            revision: None,
            metadata: serde_json::Map::new(),
        };
        Box::pin(async move { Ok(snapshot) })
    }
}

#[boardwalk::actor]
impl Led {
    #[boardwalk::transition]
    async fn turn_on(
        &mut self,
        _ctx: TransitionCtx,
        _input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        self.on = true;
        Ok(completed(self))
    }

    #[boardwalk::transition]
    async fn turn_off(
        &mut self,
        _ctx: TransitionCtx,
        _input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        self.on = false;
        Ok(completed(self))
    }
}

fn completed(led: &Led) -> TransitionOutcome {
    let state = if led.on { "on" } else { "off" }.to_string();
    TransitionOutcome::Completed {
        output: None,
        snapshot: ResourceSnapshot {
            id: "led/test".into(),
            kind: "led".into(),
            name: None,
            state: Some(state),
            node: "test".into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: Vec::new(),
            streams: Vec::new(),
            revision: None,
            metadata: serde_json::Map::new(),
        },
    }
}

#[tokio::test]
async fn actor_macro_generates_actor_impl_and_kebab_transition_names() {
    let mut led = Led { on: false };

    let ctx = TransitionCtx::new_test();
    let outcome = <Led as Actor>::transition(&mut led, ctx, "turn-on", TransitionInput::default())
        .await
        .expect("turn-on should succeed");

    match outcome {
        TransitionOutcome::Completed { snapshot, .. } => {
            assert_eq!(snapshot.state.as_deref(), Some("on"));
        }
        other => panic!("expected Completed outcome, got {other:?}"),
    }
    assert!(led.on);

    let ctx = TransitionCtx::new_test();
    let outcome = <Led as Actor>::transition(&mut led, ctx, "turn-off", TransitionInput::default())
        .await
        .expect("turn-off should succeed");
    match outcome {
        TransitionOutcome::Completed { snapshot, .. } => {
            assert_eq!(snapshot.state.as_deref(), Some("off"));
        }
        other => panic!("expected Completed outcome, got {other:?}"),
    }
    assert!(!led.on);

    let ctx = TransitionCtx::new_test();
    let err = <Led as Actor>::transition(&mut led, ctx, "explode", TransitionInput::default())
        .await
        .expect_err("unknown transition should fail");
    assert!(matches!(err, TransitionError::NotAllowed(_)));
}

#[derive(Default)]
pub struct Sensor {
    last_command_id: Option<String>,
    last_brightness: Option<i64>,
}

impl Resource for Sensor {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "sensor".into(),
            ..Default::default()
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let snapshot = ResourceSnapshot {
            id: "sensor/test".into(),
            kind: "sensor".into(),
            name: None,
            state: Some("idle".into()),
            node: "test".into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: Vec::new(),
            streams: Vec::new(),
            revision: None,
            metadata: serde_json::Map::new(),
        };
        Box::pin(async move { Ok(snapshot) })
    }
}

#[boardwalk::actor]
impl Sensor {
    #[boardwalk::transition]
    async fn calibrate(
        &mut self,
        ctx: TransitionCtx,
        input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        self.last_command_id = Some(ctx.command_id().as_str().to_string());
        self.last_brightness = input.fields.get("brightness").and_then(|v| v.as_i64());
        Ok(TransitionOutcome::Completed {
            output: None,
            snapshot: ResourceSnapshot {
                id: "sensor/test".into(),
                kind: "sensor".into(),
                name: None,
                state: Some("calibrated".into()),
                node: "test".into(),
                properties: serde_json::Map::new(),
                labels: BTreeMap::new(),
                transitions: Vec::new(),
                streams: Vec::new(),
                revision: None,
                metadata: serde_json::Map::new(),
            },
        })
    }

    #[boardwalk::transition]
    async fn refuse(
        &mut self,
        _ctx: TransitionCtx,
        _input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        Err(TransitionError::Conflict("sensor refuses".into()))
    }
}

#[tokio::test]
async fn transition_macro_accepts_context_and_json_input() {
    let mut sensor = Sensor::default();
    let mut fields = BTreeMap::new();
    fields.insert("brightness".to_string(), json!(75));
    let input = TransitionInput { fields };

    let ctx = TransitionCtx::new_test();
    let command_id = ctx.command_id().as_str().to_string();
    let outcome = <Sensor as Actor>::transition(&mut sensor, ctx, "calibrate", input)
        .await
        .expect("calibrate should succeed");

    assert!(matches!(outcome, TransitionOutcome::Completed { .. }));
    assert_eq!(sensor.last_command_id.as_deref(), Some(command_id.as_str()));
    assert_eq!(sensor.last_brightness, Some(75));
}

#[tokio::test]
async fn transition_macro_propagates_domain_errors_verbatim() {
    let mut sensor = Sensor::default();
    let ctx = TransitionCtx::new_test();
    let err = <Sensor as Actor>::transition(&mut sensor, ctx, "refuse", TransitionInput::default())
        .await
        .expect_err("refuse should propagate an error");
    match err {
        TransitionError::Conflict(msg) => assert_eq!(msg, "sensor refuses"),
        other => panic!("expected Conflict, got {other:?}"),
    }
}

// Regression: a `#[cfg(...)]`-gated `#[transition]` method must not
// leave a dangling match arm referencing a non-existent inherent
// method when the cfg evaluates to false. `cfg(any())` is never
// true, so `gated_off` is compiled out and its match arm in the
// generated `Actor::transition` is suppressed.
#[derive(Default)]
pub struct GatedActor;

impl Resource for GatedActor {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "gated".into(),
            ..Default::default()
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let snapshot = ResourceSnapshot {
            id: "gated/test".into(),
            kind: "gated".into(),
            name: None,
            state: None,
            node: "test".into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: Vec::new(),
            streams: Vec::new(),
            revision: None,
            metadata: serde_json::Map::new(),
        };
        Box::pin(async move { Ok(snapshot) })
    }
}

#[boardwalk::actor]
impl GatedActor {
    #[cfg(any())]
    #[boardwalk::transition]
    async fn gated_off(
        &mut self,
        _ctx: TransitionCtx,
        _input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        unreachable!("cfg(any()) is never true")
    }
}

#[tokio::test]
async fn cfg_gated_transitions_are_suppressed_from_dispatch() {
    let mut actor = GatedActor;
    let ctx = TransitionCtx::new_test();
    let err =
        <GatedActor as Actor>::transition(&mut actor, ctx, "gated-off", TransitionInput::default())
            .await
            .expect_err("gated-off should not be dispatchable when cfg is false");
    assert!(matches!(err, TransitionError::NotAllowed(_)));
}
