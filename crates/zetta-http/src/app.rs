//! App support: user code that reacts to device state.
//!
//! An app implements [`App::run`] and is handed a [`ServerHandle`] when
//! the server boots. The handle lets the app run CaQL queries against
//! the local registry and operate on devices.

use std::sync::Arc;

use serde_json::{Map, Value as Json};
use zetta_core::{DeviceError, DeviceId, TransitionInput};

use crate::core::Core;

pub type AppError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait::async_trait]
pub trait App: Send + Sync + 'static {
    async fn run(self: Arc<Self>, server: ServerHandle) -> Result<(), AppError>;
}

#[derive(Clone)]
pub struct ServerHandle {
    pub(crate) core: Arc<Core>,
}

impl ServerHandle {
    /// Construction is an internal concern; `zetta-server` is the one
    /// that materializes these for apps.
    #[doc(hidden)]
    pub fn new_internal(core: Arc<Core>) -> Self {
        Self { core }
    }

    /// Server name (the local instance's identity).
    pub fn name(&self) -> &str {
        &self.core.name
    }

    /// Run a CaQL query against the local device registry. Returns a
    /// `DeviceProxy` for each match. Invalid queries return an empty
    /// list and log a warning.
    pub async fn query(&self, ql: &str) -> Vec<DeviceProxy> {
        let q = match zetta_caql::parse(ql) {
            Ok(q) => q,
            Err(e) => {
                tracing::warn!(%ql, error = %e, "invalid CaQL in app query");
                return Vec::new();
            }
        };
        let devices = self.core.list_devices().await;
        devices
            .into_iter()
            .filter(|d| {
                let target = serde_json::json!({
                    "id": d.id.to_string(),
                    "type": d.type_,
                    "name": d.name,
                    "state": d.state,
                });
                zetta_caql::matches(&q, &target).unwrap_or(false)
            })
            .map(|d| DeviceProxy {
                core: self.core.clone(),
                id: d.id,
            })
            .collect()
    }

    /// Get a proxy by exact device id, if known.
    pub async fn device(&self, id: DeviceId) -> Option<DeviceProxy> {
        self.core.get_device(&id).await.map(|_| DeviceProxy {
            core: self.core.clone(),
            id,
        })
    }
}

/// Handle on a specific device that an app can read, transition, and
/// inspect.
#[derive(Clone)]
pub struct DeviceProxy {
    core: Arc<Core>,
    id: DeviceId,
}

impl DeviceProxy {
    pub fn id(&self) -> DeviceId {
        self.id
    }

    /// Current state name. `None` if the device has been removed.
    pub async fn state(&self) -> Option<String> {
        self.core.get_device(&self.id).await.map(|d| d.state)
    }

    /// Whether `transition` is currently allowed in the device's
    /// present state.
    pub async fn available(&self, transition: &str) -> bool {
        let Some(snap) = self.core.get_device(&self.id).await else {
            return false;
        };
        snap.config
            .allowed_in(&snap.state)
            .iter()
            .any(|t| t == transition)
    }

    /// Invoke a transition. Returns an error if the transition is not
    /// allowed in the current state.
    pub async fn call(&self, transition: &str, input: TransitionInput) -> Result<(), DeviceError> {
        self.core
            .run_transition(&self.id, transition, input)
            .await
            .map(|_| ())
    }

    /// Convenience: invoke a transition with no extra fields.
    pub async fn call_simple(&self, transition: &str) -> Result<(), DeviceError> {
        self.call(transition, TransitionInput::default()).await
    }

    /// Read a property value (or `None` if not present).
    pub async fn property(&self, name: &str) -> Option<Json> {
        let snap = self.core.get_device(&self.id).await?;
        snap.properties.get(name).cloned()
    }

    /// All properties as a JSON map.
    pub async fn properties(&self) -> Option<Map<String, Json>> {
        Some(self.core.get_device(&self.id).await?.properties)
    }
}
