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
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

/// Identifier assigned by the private server adapter to each resource.
pub type DeviceId = uuid::Uuid;

/// Wire-level identity of a transition (kebab-case in Siren responses).
pub type TransitionName = String;

/// Wire-level identity of a state.
pub type StateName = String;

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

/// Inputs to a transition, parsed at the HTTP boundary.
#[derive(Debug, Default, Clone)]
pub struct TransitionInput {
    pub fields: BTreeMap<String, Value>,
}

impl TransitionInput {
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }
    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.fields.get(name).and_then(Value::as_str)
    }
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

/// Stream kind hint, surfaced in metadata for clients.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamKind {
    /// JSON-serializable structured data.
    #[default]
    Object,
    /// Opaque binary frames.
    Binary,
}

#[derive(Debug, Default, Clone)]
pub struct StreamSpec {
    pub name: String,
    pub kind: StreamKind,
}

/// Field descriptor for a transition input (becomes a Siren field).
#[derive(Debug, Clone)]
pub struct FieldSpec {
    pub name: String,
    pub type_: String,
    pub title: Option<String>,
    pub value: Option<Value>,
}

/// How a transition's effect is delivered. `Sync` transitions return
/// the updated `ResourceSnapshot` directly; `AsyncJob` transitions
/// hand back a typed `JobHandle` that the caller follows on a job
/// resource.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum TransitionResultKind {
    #[default]
    Sync,
    AsyncJob,
}

/// Re-invocation contract for a transition.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Idempotency {
    #[default]
    None,
    Supported,
    Required,
}

/// Effect classification for the transition. Combines HTTP's safety and
/// idempotency axes into one ordinal: `Safe` is read-only (implies idempotent),
/// `UnsafeIdempotent` mutates but repeats cleanly (PUT/DELETE-like), and
/// `Unsafe` makes no idempotency claim (POST-like). For Idempotency-Key
/// participation, see [`Idempotency`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    Safe,
    UnsafeIdempotent,
    #[default]
    Unsafe,
}

#[derive(Debug, Default, Clone)]
pub struct TransitionSpec {
    pub name: TransitionName,
    pub title: Option<String>,
    pub allowed_states: Vec<StateName>,
    pub input_schema: Option<Value>,
    pub output_schema: Option<Value>,
    pub result: TransitionResultKind,
    pub idempotency: Idempotency,
    pub effect: Effect,
    pub required_scopes: Vec<String>,
    /// Renderer-only adapter for the current Siren `fields` surface.
    /// Will eventually be derived from `input_schema`; the field stays
    /// for now so existing form-based renders keep working.
    pub fields: Vec<FieldSpec>,
}

/// Declarative shape of a resource kind: stable identity, optional
/// property schema, and the streams it publishes.
#[derive(Debug, Default, Clone)]
pub struct ResourceSpec {
    pub kind: ResourceKind,
    pub name: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub property_schema: Option<Value>,
    pub streams: Vec<StreamSpec>,
}

/// Declarative shape of an actor: a resource plus the transitions it
/// accepts.
#[derive(Debug, Default, Clone)]
pub struct ActorSpec {
    pub resource: ResourceSpec,
    pub transitions: Vec<TransitionSpec>,
}

/// Typed handle for an async transition's downstream job resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobHandle {
    pub id: String,
    pub kind: ResourceKind,
    pub location: String,
    pub created: bool,
}

/// Typed return type for invoking a transition. `Sync` transitions
/// produce `Completed`; async ones produce `Accepted` with a typed
/// `JobHandle`.
#[derive(Debug, Clone)]
pub enum TransitionOutcome {
    Completed {
        output: Option<Value>,
        snapshot: crate::http::ResourceSnapshot,
    },
    Accepted {
        job: JobHandle,
        output: Option<Value>,
    },
}

/// Resource kind name (e.g. `"led"`, `"job"`). Currently a string;
/// A future revision may swap this for a richer type without
/// renaming usage.
pub type ResourceKind = String;

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

        led.transition("turn-on", TransitionInput::default())
            .await
            .unwrap();
        assert_eq!(led.state(), "on");

        let err = led
            .transition("nope", TransitionInput::default())
            .await
            .unwrap_err();
        assert!(matches!(err, DeviceError::Invalid(_)));
    }

    #[test]
    fn unknown_state_yields_empty_allowed() {
        let cfg = DeviceConfig::default();
        assert!(cfg.allowed_in("anything").is_empty());
    }
}
