//! Small LED actor fixture used by examples and tests.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use boardwalk::runtime::{
    DynFuture, Resource, ResourceCtx, ResourceError, TransitionCtx, TransitionError,
};
use boardwalk::{
    Effect, ResourceSnapshot, ResourceSpec, SnapshotStreamSpec, StreamKind,
    StreamSpec as ResourceStreamSpec, TransitionAffordance, TransitionInput, TransitionOutcome,
    TransitionResultKind, TransitionSpec,
};
use serde_json::json;

#[derive(Default)]
pub struct Led {
    pub on: bool,
}

impl Led {
    fn state(&self) -> &'static str {
        if self.on { "on" } else { "off" }
    }

    fn transition_affordances(&self) -> Vec<TransitionAffordance> {
        let state = self.state();
        vec![
            transition("turn-on", "Turn on", "off", state == "off"),
            transition("turn-off", "Turn off", "on", state == "on"),
        ]
    }

    fn snapshot(&self) -> ResourceSnapshot {
        ResourceSnapshot::builder("led")
            .name("LED")
            .state(self.state())
            .transitions(self.transition_affordances())
            .streams(vec![SnapshotStreamSpec {
                name: "state".into(),
                kind: StreamKind::Object,
            }])
            .build()
    }
}

impl Resource for Led {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some("LED".into()),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: vec![ResourceStreamSpec {
                name: "state".into(),
                kind: StreamKind::Object,
            }],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let snap = self.snapshot();
        Box::pin(async move { Ok(snap) })
    }
}

#[boardwalk::actor]
impl Led {
    #[boardwalk::transition]
    async fn turn_on(
        &mut self,
        ctx: TransitionCtx,
        _input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        if self.on {
            return Err(TransitionError::NotAllowed(
                "`turn-on` is only available while the LED is off".into(),
            ));
        }

        self.on = true;
        ctx.publish("state", "resource.state.changed", 1, json!("on"))
            .await?;
        completed(&ctx, self)
    }

    #[boardwalk::transition]
    async fn turn_off(
        &mut self,
        ctx: TransitionCtx,
        _input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        if !self.on {
            return Err(TransitionError::NotAllowed(
                "`turn-off` is only available while the LED is on".into(),
            ));
        }

        self.on = false;
        ctx.publish("state", "resource.state.changed", 1, json!("off"))
            .await?;
        completed(&ctx, self)
    }
}

fn completed(ctx: &TransitionCtx, led: &Led) -> Result<TransitionOutcome, TransitionError> {
    ctx.completed(None, led.snapshot())
}

fn transition(
    name: impl Into<String>,
    title: impl Into<String>,
    allowed_state: impl Into<String>,
    available: bool,
) -> TransitionAffordance {
    let allowed_state = allowed_state.into();
    TransitionAffordance {
        spec: TransitionSpec {
            name: name.into(),
            title: Some(title.into()),
            allowed_states: vec![allowed_state.clone()],
            result: TransitionResultKind::Sync,
            effect: Effect::Unsafe,
            ..Default::default()
        },
        available,
        unavailable_reason: (!available)
            .then(|| format!("only available in `{allowed_state}` state")),
    }
}
