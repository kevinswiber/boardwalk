//! Core types for Zetta.
//!
//! This crate defines the abstract building blocks (Device, Scout, App,
//! transitions, streams) without committing to any transport or storage
//! backend. Drivers depend only on this crate.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Identifier assigned by the runtime to each device instance.
pub type DeviceId = uuid::Uuid;

/// Wire-level identity of a transition (kebab-case in Siren responses).
pub type TransitionName = String;

/// Wire-level identity of a state.
pub type StateName = String;

#[derive(Debug, Error)]
pub enum DeviceError {
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("internal: {0}")]
    Internal(String),
}

/// Marker trait every driver implements. Real wiring happens via the
/// `DeviceConfig` builder passed to `config`.
pub trait Device: Send + Sync + 'static {
    fn config(&self, cfg: &mut DeviceConfig);
}

/// Builder accepted by `Device::config`.
#[derive(Default)]
pub struct DeviceConfig {
    pub(crate) type_: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) initial_state: Option<StateName>,
    pub(crate) state_transitions: BTreeMap<StateName, Vec<TransitionName>>,
    // Transition handlers + stream registrations are filled in by the
    // server-facing crates; this crate keeps the schema minimal so it
    // stays dependency-free.
}

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

    pub fn when(
        &mut self,
        state: impl Into<StateName>,
        allow: &[&str],
    ) -> &mut Self {
        self.state_transitions
            .insert(state.into(), allow.iter().map(|s| s.to_string()).collect());
        self
    }
}

/// A scout discovers devices over a protocol or out-of-process source.
#[async_trait::async_trait]
pub trait Scout: Send + Sync + 'static {
    async fn run(self: Arc<Self>, ctx: ScoutCtx) -> Result<(), DeviceError>;
}

/// Context handed to scouts by the runtime.
#[derive(Clone)]
pub struct ScoutCtx {
    // Filled in once the server crate is wired up.
    _placeholder: (),
}

/// Boxed error returned from user-defined apps. Keeps zetta-core
/// dependency-light; downstream consumers can map their preferred
/// error library (anyhow, eyre, ...) into this.
pub type AppError = Box<dyn std::error::Error + Send + Sync>;

/// An App is user code that reacts to queries and orchestrates devices.
#[async_trait::async_trait]
pub trait App: Send + Sync + 'static {
    async fn run(self: Arc<Self>, server: ServerHandle) -> Result<(), AppError>;
}

/// Handle into the running server, given to apps.
#[derive(Clone)]
pub struct ServerHandle {
    _placeholder: (),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceProperties {
    pub id: DeviceId,
    #[serde(rename = "type")]
    pub type_: String,
    pub name: Option<String>,
    pub state: StateName,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_config_builder_collects_states() {
        let mut cfg = DeviceConfig::default();
        cfg.type_("led")
           .name("LED")
           .state("off")
           .when("off", &["turn-on"])
           .when("on", &["turn-off"]);

        assert_eq!(cfg.type_.as_deref(), Some("led"));
        assert_eq!(cfg.name.as_deref(), Some("LED"));
        assert_eq!(cfg.initial_state.as_deref(), Some("off"));
        assert_eq!(cfg.state_transitions["off"], vec!["turn-on".to_string()]);
        assert_eq!(cfg.state_transitions["on"], vec!["turn-off".to_string()]);
    }
}
