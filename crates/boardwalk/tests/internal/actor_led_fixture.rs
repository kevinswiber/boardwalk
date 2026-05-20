use std::collections::BTreeMap;

use serde_json::{Map, Value as Json};

use crate::runtime::{
    Actor, DynFuture, Resource, ResourceCtx, ResourceError, ResourceSnapshot, ResourceSpec,
    SnapshotStreamSpec, StreamKind, StreamSpec, TransitionAffordance, TransitionCtx,
    TransitionError, TransitionInput, TransitionOutcome, TransitionSpec,
};

#[derive(Default)]
pub(crate) struct ActorLed {
    on: bool,
}

impl ActorLed {
    fn snapshot(&self) -> ResourceSnapshot {
        let state = if self.on { "on" } else { "off" };
        let mut properties = Map::new();
        properties.insert("fixture".into(), Json::String("actor-led".into()));

        ResourceSnapshot {
            id: "runtime-assigned".into(),
            kind: "led".into(),
            name: Some("LED".into()),
            state: Some(state.into()),
            node: "runtime-assigned".into(),
            properties,
            labels: BTreeMap::new(),
            transitions: vec![
                TransitionAffordance {
                    spec: TransitionSpec {
                        name: "turn-on".into(),
                        allowed_states: vec!["off".into()],
                        ..Default::default()
                    },
                    available: !self.on,
                    unavailable_reason: self.on.then_some("already on".into()),
                },
                TransitionAffordance {
                    spec: TransitionSpec {
                        name: "turn-off".into(),
                        allowed_states: vec!["on".into()],
                        ..Default::default()
                    },
                    available: self.on,
                    unavailable_reason: (!self.on).then_some("already off".into()),
                },
            ],
            streams: vec![SnapshotStreamSpec {
                name: "state".into(),
                kind: "object".into(),
            }],
            revision: None,
            metadata: Map::new(),
        }
    }
}

impl Resource for ActorLed {
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
        Box::pin(async move { Ok(self.snapshot()) })
    }
}

impl Actor for ActorLed {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            match name {
                "turn-on" if !self.on => self.on = true,
                "turn-off" if self.on => self.on = false,
                other => {
                    return Err(TransitionError::NotAllowed(format!(
                        "transition `{other}` is not available"
                    )));
                }
            }

            let state = if self.on { "on" } else { "off" };
            ctx.publish(
                "state",
                "resource.state.changed",
                1,
                Json::String(state.into()),
            )
            .await?;

            Ok(TransitionOutcome::Completed {
                output: None,
                snapshot: self.snapshot(),
            })
        })
    }
}
