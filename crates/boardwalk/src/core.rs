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
    FieldSpec, StateName, StreamKind, StreamSpec, TransitionInput, TransitionName, TransitionSpec,
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
