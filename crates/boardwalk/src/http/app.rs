//! Private app/scout support for the server adapter.
//!
//! An app implements [`App::run`] and is handed a [`ServerHandle`] when
//! the server boots. The handle lets the app run CaQL queries against
//! the local resource registry through compatibility proxies.

// App/Scout compatibility is private until the actor-backed HTTP facade lands.
#![allow(dead_code)]

use std::sync::Arc;

use serde_json::{Map, Value as Json};
use uuid::Uuid;

use super::core::{Core, ResourceTransitionError};
use crate::core::{Device, DeviceConfig, DeviceError, DeviceId};
use crate::runtime::{RequestCtx, TransitionInput};

pub type AppError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait::async_trait]
pub trait App: Send + Sync + 'static {
    async fn run(self: Arc<Self>, server: ServerHandle) -> Result<(), AppError>;
}

/// A scout discovers private adapter resources over a protocol (mDNS,
/// USB, Bluetooth, etc.) and registers them with the running server.
#[async_trait::async_trait]
pub trait Scout: Send + Sync + 'static {
    async fn run(self: Arc<Self>, ctx: ScoutCtx) -> Result<(), DeviceError>;
}

/// Handle handed to each scout.
#[derive(Clone)]
pub struct ScoutCtx {
    core: Arc<Core>,
}

impl ScoutCtx {
    #[doc(hidden)]
    pub fn new_internal(core: Arc<Core>) -> Self {
        Self { core }
    }

    /// Server name (the local instance's identity).
    pub fn name(&self) -> &str {
        &self.core.name
    }

    /// Register a newly-discovered adapter resource with the running server.
    /// Returns the assigned resource ID. The resource is immediately
    /// visible via the HTTP API.
    pub async fn discover<D: Device + 'static>(&self, device: D) -> DeviceId {
        let id = Uuid::new_v4();
        let mut cfg = DeviceConfig::default();
        device.config(&mut cfg);
        self.core.register_device(id, cfg, Box::new(device)).await;
        id
    }

    /// Get a `ServerHandle` for inspecting existing resources (e.g. to
    /// avoid duplicate discovery).
    pub fn server(&self) -> ServerHandle {
        ServerHandle::new_internal(self.core.clone())
    }
}

#[derive(Clone)]
pub struct ServerHandle {
    pub(crate) core: Arc<Core>,
}

impl ServerHandle {
    /// Construction is an internal concern; `boardwalk-server` is the one
    /// that materializes these for apps.
    #[doc(hidden)]
    pub fn new_internal(core: Arc<Core>) -> Self {
        Self { core }
    }

    /// Server name (the local instance's identity).
    pub fn name(&self) -> &str {
        &self.core.name
    }

    /// Run a CaQL query against the local resource projection. Returns a
    /// compatibility proxy for each match. Invalid CaQL surfaces as
    /// `Err(AppError)` so callers can react explicitly.
    pub async fn query(&self, ql: &str) -> Result<Vec<DeviceProxy>, AppError> {
        let q = crate::caql::parse(ql)
            .map_err(|e| -> AppError { Box::new(std::io::Error::other(format!("caql: {e}"))) })?;
        let resources = self.core.list_resources().await;
        let mut out = Vec::with_capacity(resources.len());
        for snap in resources {
            let matched =
                crate::query::matches(&q, &snap.to_query_value()).map_err(|e| -> AppError {
                    Box::new(std::io::Error::other(format!("eval: {e}")))
                })?;
            if matched {
                out.push(DeviceProxy {
                    core: self.core.clone(),
                    id: snap.id,
                });
            }
        }
        Ok(out)
    }

    /// Get a proxy by exact resource id, if known.
    pub async fn device(&self, id: DeviceId) -> Option<DeviceProxy> {
        let id = id.to_string();
        self.core.get_resource(&id).await.ok().flatten()?;
        Some(DeviceProxy {
            core: self.core.clone(),
            id,
        })
    }

    /// Wait until *all* of `queries` have at least one matching resource,
    /// then invoke `callback` with one proxy per query (the first match
    /// in registration order). If a query never matches, `observe`
    /// waits forever (drop the future to cancel).
    ///
    /// Single-shot: call again to observe a fresh device set.
    pub async fn observe<F, Fut>(&self, queries: Vec<&str>, callback: F) -> Result<(), AppError>
    where
        F: FnOnce(Vec<DeviceProxy>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<(), AppError>> + Send,
    {
        let parsed: Vec<crate::query::Query> = queries
            .iter()
            .map(|q| crate::caql::parse(q))
            .collect::<Result<_, _>>()
            .map_err(|e| -> AppError { Box::new(std::io::Error::other(format!("caql: {e}"))) })?;
        let mut rx = self.core.device_changes.subscribe();
        loop {
            let resources = self.core.list_resources().await;
            let mut proxies = Vec::with_capacity(parsed.len());
            let mut ok = true;
            for q in &parsed {
                let m = resources.iter().find(|snap| {
                    let target = snap.to_query_value();
                    crate::query::matches(q, &target).unwrap_or(false)
                });
                match m {
                    Some(snap) => proxies.push(DeviceProxy {
                        core: self.core.clone(),
                        id: snap.id.clone(),
                    }),
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                return callback(proxies).await;
            }
            match rx.recv().await {
                Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    /// Continuous variant of [`observe`]. Fires `callback` every time
    /// the matching device set changes. The callback receives the same
    /// "one proxy per query" shape. Loops until the device-changes
    /// channel closes (i.e. the server shuts down).
    pub async fn observe_loop<F, Fut>(
        &self,
        queries: Vec<&str>,
        mut callback: F,
    ) -> Result<(), AppError>
    where
        F: FnMut(Vec<DeviceProxy>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<(), AppError>> + Send,
    {
        let parsed: Vec<crate::query::Query> = queries
            .iter()
            .map(|q| crate::caql::parse(q))
            .collect::<Result<_, _>>()
            .map_err(|e| -> AppError { Box::new(std::io::Error::other(format!("caql: {e}"))) })?;
        let mut rx = self.core.device_changes.subscribe();
        let mut prev: Option<Vec<String>> = None;
        loop {
            let resources = self.core.list_resources().await;
            let mut proxies = Vec::with_capacity(parsed.len());
            let mut ok = true;
            for q in &parsed {
                let m = resources.iter().find(|snap| {
                    let target = snap.to_query_value();
                    crate::query::matches(q, &target).unwrap_or(false)
                });
                match m {
                    Some(snap) => proxies.push(DeviceProxy {
                        core: self.core.clone(),
                        id: snap.id.clone(),
                    }),
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                let ids: Vec<String> = proxies.iter().map(|p| p.id().to_string()).collect();
                if prev.as_ref() != Some(&ids) {
                    callback(proxies).await?;
                    prev = Some(ids);
                }
            } else if prev.is_some() {
                // The set was satisfied before but is no longer; reset
                // so the next satisfying set re-fires.
                prev = None;
            }
            match rx.recv().await {
                Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }
}

/// Handle on a specific device that an app can read, transition, and
/// inspect.
#[derive(Clone)]
pub struct DeviceProxy {
    core: Arc<Core>,
    id: String,
}

impl DeviceProxy {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Current state name. `None` if the device has been removed.
    pub async fn state(&self) -> Option<String> {
        self.core
            .get_resource(&self.id)
            .await
            .ok()
            .flatten()
            .and_then(|snap| snap.state)
    }

    /// Whether `transition` is currently allowed in the device's
    /// present state.
    pub async fn available(&self, transition: &str) -> bool {
        let Some(snap) = self.core.get_resource(&self.id).await.ok().flatten() else {
            return false;
        };
        snap.transitions
            .iter()
            .any(|affordance| affordance.available && affordance.name() == transition)
    }

    /// Invoke a transition. Returns an error if the transition is not
    /// allowed in the current state.
    pub async fn call(&self, transition: &str, input: TransitionInput) -> Result<(), DeviceError> {
        self.core
            .run_resource_transition(&self.id, transition, input, RequestCtx::default())
            .await
            .map_err(device_error_from_transition)
            .map(|_| ())
    }

    /// Convenience: invoke a transition with no extra fields.
    pub async fn call_simple(&self, transition: &str) -> Result<(), DeviceError> {
        self.call(transition, TransitionInput::default()).await
    }

    /// Read a property value (or `None` if not present).
    pub async fn property(&self, name: &str) -> Option<Json> {
        let snap = self.core.get_resource(&self.id).await.ok().flatten()?;
        snap.properties.get(name).cloned()
    }

    /// All properties as a JSON map.
    pub async fn properties(&self) -> Option<Map<String, Json>> {
        Some(
            self.core
                .get_resource(&self.id)
                .await
                .ok()
                .flatten()?
                .properties,
        )
    }
}

fn device_error_from_transition(err: ResourceTransitionError) -> DeviceError {
    match err {
        ResourceTransitionError::InvalidId => DeviceError::Invalid("invalid resource id".into()),
        ResourceTransitionError::InvalidInput(msg) => DeviceError::Invalid(msg),
        ResourceTransitionError::NotAllowed(msg) => DeviceError::NotAllowed(msg),
        ResourceTransitionError::BackpressureRequired => {
            DeviceError::NotAllowed("backpressure required".into())
        }
        ResourceTransitionError::Timeout => DeviceError::NotAllowed("transition timed out".into()),
        ResourceTransitionError::Unavailable(msg) => DeviceError::NotAllowed(msg),
        ResourceTransitionError::Conflict(msg) => DeviceError::Conflict(msg),
        ResourceTransitionError::NotFound => DeviceError::Internal("resource not found".into()),
        ResourceTransitionError::Busy => DeviceError::Internal("resource actor is busy".into()),
        ResourceTransitionError::Internal(msg) => DeviceError::Internal(msg),
    }
}
