//! Private compatibility types for the current HTTP server adapter.
//!
//! Public users should build around `Resource`, `Actor`, and `Node`.
//! These `Device*` types remain crate-internal while the reusable HTTP
//! adapter still projects its old registration/runtime model into the
//! final Resource wire contract.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::runtime::{
    Actor, ActorCtx, FieldSpec, Resource, ResourceCtx, ResourceError, ResourceSnapshot,
    ResourceSpec, SnapshotStreamSpec, StateName, StreamKind, StreamSpec, TransitionAffordance,
    TransitionCtx, TransitionError, TransitionInput, TransitionName, TransitionOutcome,
    TransitionSpec, sanitize_properties,
};

/// Identifier assigned by the private server adapter to each resource.
pub type DeviceId = uuid::Uuid;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum DeviceError {
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("not allowed in current state: {0}")]
    NotAllowed(String),
    #[error("internal: {0}")]
    Internal(String),
}

/// What the private server adapter currently stores. Real wiring happens via `DeviceConfig`.
pub trait Device: Send + Sync + 'static {
    /// Called once at registration. Sets type, initial state, allowed
    /// transitions per state, and stream metadata.
    fn config(&self, cfg: &mut DeviceConfig);

    /// Current state name. Called by the runtime whenever a Siren
    /// representation is produced.
    fn state(&self) -> &str;

    /// Optional extra properties beyond the reserved Resource fields.
    /// Default: none.
    fn properties(&self) -> Map<String, Value> {
        Map::new()
    }

    /// Dispatch a transition by name. The runtime guards against
    /// transitions not allowed in the current state before calling.
    fn transition<'a>(
        &'a mut self,
        name: &'a str,
        input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>>;

    /// Called once after registration. Adapter implementations spawn background tasks
    /// here (e.g. periodic telemetry) using `ctx.publish` to push to
    /// declared streams. `&self` — implementations needing mutable state
    /// during these background tasks use interior mutability.
    fn on_start(&self, _ctx: DeviceCtx) {}
}

/// Builder accepted by `Device::config`; projected as Resource metadata.
#[allow(dead_code)]
#[derive(Default, Debug, Clone)]
pub struct DeviceConfig {
    pub type_: Option<String>,
    pub name: Option<String>,
    pub initial_state: Option<StateName>,
    pub state_transitions: BTreeMap<StateName, Vec<TransitionName>>,
    pub transitions: BTreeMap<TransitionName, TransitionSpec>,
    pub streams: Vec<StreamSpec>,
    pub monitored: Vec<String>,
}

#[allow(dead_code)]
impl DeviceConfig {
    pub fn type_(&mut self, ty: impl Into<String>) -> &mut Self {
        self.type_ = Some(ty.into());
        self
    }

    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub fn state(&mut self, state: impl Into<StateName>) -> &mut Self {
        self.initial_state = Some(state.into());
        self
    }

    pub fn when(&mut self, state: impl Into<StateName>, allow: &[&str]) -> &mut Self {
        let s = state.into();
        let names: Vec<String> = allow.iter().map(|s| s.to_string()).collect();
        // Ensure every transition referenced from `.when` exists in
        // the spec map, and that its `allowed_states` includes `s` so
        // the snapshot/render layer can answer "which states permit
        // this transition?" from `TransitionSpec` alone.
        for t in &names {
            let entry = self
                .transitions
                .entry(t.clone())
                .or_insert_with(|| TransitionSpec {
                    name: t.clone(),
                    ..Default::default()
                });
            if !entry.allowed_states.contains(&s) {
                entry.allowed_states.push(s.clone());
            }
        }
        self.state_transitions.insert(s, names);
        self
    }

    /// Declare a transition that takes additional fields (beyond the
    /// mandatory hidden `action` field). Without this call, transitions
    /// referenced from `.when` exist with no extra fields. When the
    /// transition is *already* declared (typically by an earlier
    /// `.when(...)` call), only `fields` is replaced — `allowed_states`
    /// and other metadata set up by `.when` survive.
    pub fn transition(
        &mut self,
        name: impl Into<TransitionName>,
        fields: Vec<FieldSpec>,
    ) -> &mut Self {
        let n: TransitionName = name.into();
        match self.transitions.entry(n.clone()) {
            std::collections::btree_map::Entry::Occupied(mut e) => {
                e.get_mut().fields = fields;
            }
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(TransitionSpec {
                    name: n,
                    fields,
                    ..Default::default()
                });
            }
        }
        self
    }

    /// Declare a stream the adapter resource will publish to.
    pub fn stream(&mut self, name: impl Into<String>, kind: StreamKind) -> &mut Self {
        self.streams.push(StreamSpec {
            name: name.into(),
            kind,
        });
        self
    }

    /// Auto-stream a property on the adapter resource — equivalent to the original's
    /// `config.monitor('color')`. The runtime publishes events whenever the
    /// property changes between transitions.
    pub fn monitor(&mut self, name: impl Into<String>) -> &mut Self {
        let n = name.into();
        self.streams.push(StreamSpec {
            name: n.clone(),
            kind: StreamKind::Object,
        });
        self.monitored.push(n);
        self
    }

    /// Allowed transitions in a given state. Empty if state is unknown.
    pub fn allowed_in(&self, state: &str) -> &[TransitionName] {
        self.state_transitions
            .get(state)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

impl<T: Device> Resource for T {
    fn spec(&self) -> ResourceSpec {
        let cfg = device_config(self);
        ResourceSpec {
            kind: cfg.type_.unwrap_or_else(|| "unknown".into()),
            name: cfg.name,
            labels: BTreeMap::new(),
            property_schema: None,
            streams: cfg.streams,
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move { Ok(device_resource_snapshot(self, "ignored", "ignored")) })
    }
}

impl<T: Device> Actor for T {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            let cfg = device_config(self);
            let prior_state = self.state().to_string();
            if !cfg.allowed_in(&prior_state).iter().any(|t| t == name) {
                return Err(TransitionError::NotAllowed(format!(
                    "transition `{name}` not allowed in state `{prior_state}`"
                )));
            }

            <T as Device>::transition(self, name, input)
                .await
                .map_err(device_transition_error)?;

            let new_state = self.state().to_string();
            if prior_state != new_state && cfg.monitored.iter().any(|stream| stream == "state") {
                let _ = ctx
                    .publish(
                        "state",
                        "resource.state.changed",
                        1,
                        Value::String(new_state),
                    )
                    .await;
            }

            let id = ctx.resource_id().unwrap_or("ignored");
            Ok(TransitionOutcome::Completed {
                output: None,
                snapshot: device_resource_snapshot(self, id, ctx.node()),
            })
        })
    }

    fn on_start<'a>(
        &'a mut self,
        ctx: ActorCtx,
    ) -> DynFuture<'a, Result<(), crate::runtime::ActorError>> {
        Box::pin(async move {
            let id =
                uuid::Uuid::parse_str(ctx.resource_id()).unwrap_or_else(|_| uuid::Uuid::new_v4());
            let device_ctx = DeviceCtx {
                id,
                type_: ctx.resource_kind().to_string(),
                publish: Arc::new(ActorStreamSink { ctx }),
            };
            <T as Device>::on_start(self, device_ctx);
            Ok(())
        })
    }
}

#[derive(Clone)]
struct ActorStreamSink {
    ctx: ActorCtx,
}

impl StreamSink for ActorStreamSink {
    fn publish(&self, stream: &str, data: Value) {
        let ctx = self.ctx.clone();
        let stream = stream.to_string();
        tokio::spawn(async move {
            let _ = ctx.publish(&stream, "resource.stream.data", 1, data).await;
        });
    }
}

fn device_config(device: &impl Device) -> DeviceConfig {
    let mut cfg = DeviceConfig::default();
    device.config(&mut cfg);
    cfg
}

fn device_resource_snapshot(device: &impl Device, id: &str, node: &str) -> ResourceSnapshot {
    let cfg = device_config(device);
    let kind = cfg.type_.clone().unwrap_or_else(|| "unknown".into());
    let state = device.state().to_string();
    let allowed: std::collections::BTreeSet<&str> =
        cfg.allowed_in(&state).iter().map(String::as_str).collect();
    let transitions = cfg
        .transitions
        .values()
        .map(|spec| TransitionAffordance {
            spec: spec.clone(),
            available: allowed.contains(spec.name.as_str()),
            unavailable_reason: None,
        })
        .collect();
    let streams = cfg
        .streams
        .iter()
        .map(|stream| SnapshotStreamSpec {
            name: stream.name.clone(),
            kind: match stream.kind {
                StreamKind::Object => "object".into(),
                StreamKind::Binary => "binary".into(),
            },
        })
        .collect();
    ResourceSnapshot {
        id: id.to_string(),
        kind,
        name: cfg.name,
        state: Some(state),
        node: node.to_string(),
        properties: sanitize_properties(device.properties()),
        labels: BTreeMap::new(),
        transitions,
        streams,
        revision: None,
        metadata: Map::new(),
    }
}

fn device_transition_error(err: DeviceError) -> TransitionError {
    match err {
        DeviceError::Invalid(msg) => TransitionError::InvalidInput(msg),
        DeviceError::Conflict(msg) => TransitionError::Conflict(msg),
        DeviceError::NotAllowed(msg) => TransitionError::NotAllowed(msg),
        DeviceError::Internal(msg) => TransitionError::Internal(msg),
    }
}

/// Context handed to an adapter resource's startup hook. Lets it publish
/// to declared streams.
#[allow(dead_code)]
pub struct DeviceCtx {
    pub id: DeviceId,
    pub type_: String,
    pub publish: Arc<dyn StreamSink>,
}

/// Erased sink used by `DeviceCtx::publish` so `boardwalk-core` doesn't have
/// to know what the event bus is. The runtime crate provides the impl.
#[allow(dead_code)]
pub trait StreamSink: Send + Sync {
    fn publish(&self, stream: &str, data: Value);
}

// Scout + App live in boardwalk-http (they need Core access).

// -- Future-pin helper -----------------------------------------------------

/// `BoxFuture` re-export so device implementations don't need a futures dependency.
#[allow(dead_code)]
pub type DynFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

#[cfg(test)]
mod tests {
    use super::*;

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
                    other => Err(DeviceError::Invalid(format!("unknown transition {other}"))),
                }
            })
        }
    }

    #[tokio::test]
    async fn led_transitions() {
        let mut led = Led { on: false };
        let mut cfg = DeviceConfig::default();
        led.config(&mut cfg);
        assert_eq!(cfg.type_.as_deref(), Some("led"));
        assert_eq!(cfg.initial_state.as_deref(), Some("off"));
        assert_eq!(cfg.allowed_in("off"), &["turn-on".to_string()]);
        assert!(cfg.streams.iter().any(|s| s.name == "state"));
        assert!(cfg.monitored.contains(&"state".to_string()));

        Device::transition(&mut led, "turn-on", TransitionInput::default())
            .await
            .unwrap();
        assert_eq!(led.state(), "on");

        let err = Device::transition(&mut led, "nope", TransitionInput::default())
            .await
            .unwrap_err();
        assert!(matches!(err, DeviceError::Invalid(_)));
    }

    #[tokio::test]
    async fn blanket_device_actor_transition_keeps_state_publish_best_effort() {
        let mut led = Led { on: false };
        let outcome = Actor::transition(
            &mut led,
            TransitionCtx::new_test(),
            "turn-on",
            TransitionInput::default(),
        )
        .await
        .expect("state publish failure should not fail transition");

        assert_eq!(led.state(), "on");
        match outcome {
            TransitionOutcome::Completed { snapshot, .. } => {
                assert_eq!(snapshot.state.as_deref(), Some("on"));
            }
            TransitionOutcome::Accepted { .. } => panic!("device transition should complete"),
        }
    }

    #[test]
    fn unknown_state_yields_empty_allowed() {
        let cfg = DeviceConfig::default();
        assert!(cfg.allowed_in("anything").is_empty());
    }
}
