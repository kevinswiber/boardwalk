//! Top-level builder for assembling a Zetta server.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use url::Url;
use uuid::Uuid;
use zetta_core::{Device, DeviceConfig, DeviceError};
use zetta_http::{
    App, AppState, Core, CoreBuilder, DeviceRegistrar, DeviceRegistration, PeerHandler,
    PeerInitState, Scout, ScoutCtx, ServerHandle, router_with,
};
pub use zetta_peer::PeerAcceptors;
use zetta_peer::PeerClient;
use zetta_registry::{DeviceRecord, Registry};

pub struct Zetta {
    name: String,
    peers: Vec<Url>,
    devices: Vec<Box<dyn Device>>,
    apps: Vec<Arc<dyn App>>,
    scouts: Vec<Arc<dyn Scout>>,
    factories: HashMap<String, DeviceFactory>,
    persist_path: Option<PathBuf>,
}

/// Type-erased device factory used by `register_factory`.
type DeviceFactory =
    Arc<dyn Fn(HashMap<String, String>) -> Result<Box<dyn Device>, DeviceError> + Send + Sync>;

impl Default for Zetta {
    fn default() -> Self {
        Self::new()
    }
}

impl Zetta {
    pub fn new() -> Self {
        Self {
            name: "zetta".to_string(),
            peers: Vec::new(),
            devices: Vec::new(),
            apps: Vec::new(),
            scouts: Vec::new(),
            factories: HashMap::new(),
            persist_path: None,
        }
    }

    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = n.into();
        self
    }

    pub fn use_device<D: Device>(mut self, d: D) -> Self {
        self.devices.push(Box::new(d));
        self
    }

    pub fn use_app<A: App>(mut self, a: A) -> Self {
        self.apps.push(Arc::new(a));
        self
    }

    pub fn use_scout<S: Scout>(mut self, s: S) -> Self {
        self.scouts.push(Arc::new(s));
        self
    }

    /// Register a factory for hubless device registration. The factory
    /// receives the form fields from `POST /servers/{name}/devices`
    /// (minus the standard `type`/`id`/`name` fields, which are
    /// extracted separately) and returns a freshly-built device.
    pub fn register_factory<F>(mut self, type_name: impl Into<String>, factory: F) -> Self
    where
        F: Fn(HashMap<String, String>) -> Result<Box<dyn Device>, DeviceError>
            + Send
            + Sync
            + 'static,
    {
        self.factories.insert(type_name.into(), Arc::new(factory));
        self
    }

    pub fn link(mut self, url: impl AsRef<str>) -> Self {
        match Url::parse(url.as_ref()) {
            Ok(u) => self.peers.push(u),
            Err(e) => tracing::warn!(?e, url = url.as_ref(), "ignoring invalid peer url"),
        }
        self
    }

    /// Enable on-disk persistence of device + peer registries at the
    /// supplied path (single redb file). Without this call, the runtime
    /// is purely in-memory.
    pub fn persist(mut self, path: impl Into<PathBuf>) -> Self {
        self.persist_path = Some(path.into());
        self
    }

    /// Bind and serve. Blocks until the listener stops.
    pub async fn listen(self, addr: SocketAddr) -> anyhow::Result<()> {
        let built = self.build()?;
        tracing::info!(%addr, "zetta-rs listening");
        let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
        axum::serve(listener, built.router).await.context("serve")
    }

    /// Bind and serve until `signal` resolves. The HTTP listener stops
    /// accepting new connections, finishes in-flight requests, then
    /// returns. Peer-client tasks and app tasks are aborted on return.
    pub async fn listen_until<F: std::future::Future<Output = ()> + Send + 'static>(
        self,
        addr: SocketAddr,
        signal: F,
    ) -> anyhow::Result<()> {
        let built = self.build()?;
        tracing::info!(%addr, "zetta-rs listening (graceful)");
        let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
        let res = axum::serve(listener, built.router)
            .with_graceful_shutdown(signal)
            .await
            .context("serve");

        // Tear down background work.
        for t in built.peer_tasks {
            t.abort();
        }
        for t in built.app_tasks {
            t.abort();
        }
        for t in built.scout_tasks {
            t.abort();
        }
        res
    }

    /// Build the runtime + router + spawn peer clients without binding.
    /// Useful for integration tests.
    pub fn build(self) -> anyhow::Result<Built> {
        // Open the registry if persistence was requested. Device IDs
        // are then stable across restarts (keyed by type + name).
        let registry = self
            .persist_path
            .as_ref()
            .map(|p| Registry::open(p).context("opening registry"))
            .transpose()?
            .map(Arc::new);

        let mut builder = CoreBuilder::new(self.name.clone());
        for device in self.devices {
            let mut cfg = DeviceConfig::default();
            device.config(&mut cfg);
            let id = resolve_device_id(&registry, &cfg)?;
            builder.add_device_full(id, cfg, device);
        }
        let core: Arc<Core> = builder.build();

        let peer_init = PeerInitState::default();
        let acceptors = PeerAcceptors::new();
        if let Some(reg) = registry.as_ref() {
            acceptors.with_registry(reg.clone());
        }

        let handler: PeerHandler = {
            let acceptors = acceptors.clone();
            Arc::new(move |peer_name, connection_id, upgraded| {
                let acceptors = acceptors.clone();
                Box::pin(async move {
                    acceptors
                        .on_upgraded(peer_name, connection_id, upgraded)
                        .await;
                })
            })
        };

        let peer_senders: Arc<dyn zetta_http::PeerSenders> = Arc::new(acceptors.clone());
        let peer_streams = zetta_http::PeerStreamHub::new();

        // Hubless registration: build a registrar closure if any
        // factories are registered.
        let device_registrar: Option<DeviceRegistrar> =
            if self.factories.is_empty() {
                None
            } else {
                let factories = Arc::new(self.factories);
                let core_for = core.clone();
                let registry_for = registry.clone();
                Some(Arc::new(
                    move |reg: DeviceRegistration| -> futures::future::BoxFuture<
                        'static,
                        Result<uuid::Uuid, DeviceError>,
                    > {
                        let factories = factories.clone();
                        let core = core_for.clone();
                        let registry = registry_for.clone();
                        Box::pin(async move {
                            let factory = factories.get(&reg.type_).ok_or_else(|| {
                                DeviceError::Invalid(format!("unknown device type `{}`", reg.type_))
                            })?;
                            let device = factory(reg.fields)?;
                            let mut cfg = DeviceConfig::default();
                            device.config(&mut cfg);
                            if let Some(n) = reg.name.clone() {
                                cfg.name = Some(n);
                            }
                            let id = resolve_runtime_id(&registry, &reg.type_, &cfg, reg.id)?;
                            core.register_device(id, cfg, device).await;
                            Ok(id)
                        })
                    },
                ))
            };

        let state = AppState {
            core: core.clone(),
            peer_handler: Some(handler),
            peer_init: peer_init.clone(),
            peer_senders: Some(peer_senders),
            peer_streams: peer_streams.clone(),
            device_registrar,
        };
        let router = router_with(state);

        let mut peer_tasks = Vec::new();
        for url in self.peers {
            let local_name = self.name.clone();
            let pc = PeerClient::new(
                url,
                local_name,
                router.clone(),
                peer_init.clone(),
                core.clone(),
            );
            peer_tasks.push(pc.spawn());
        }

        // Spawn apps. The server handle is shared across them; each
        // app's `run` runs to completion in its own task. Errors are logged.
        let mut app_tasks = Vec::new();
        for app in self.apps {
            let handle = ServerHandle::new_internal(core.clone());
            let h = tokio::spawn(async move {
                if let Err(e) = app.run(handle).await {
                    tracing::warn!(error = %e, "app exited with error");
                }
            });
            app_tasks.push(h);
        }

        // Spawn scouts. Same shape — long-running, errors logged.
        let mut scout_tasks = Vec::new();
        for scout in self.scouts {
            let ctx = ScoutCtx::new_internal(core.clone());
            let h = tokio::spawn(async move {
                if let Err(e) = scout.run(ctx).await {
                    tracing::warn!(error = %e, "scout exited with error");
                }
            });
            scout_tasks.push(h);
        }

        Ok(Built {
            core,
            peer_tasks,
            app_tasks,
            scout_tasks,
            router,
            acceptors,
            peer_streams,
            registry,
        })
    }
}

/// Look up a stable device ID by (type, name) identity, or mint a new
/// one and persist the record.
fn resolve_device_id(registry: &Option<Arc<Registry>>, cfg: &DeviceConfig) -> anyhow::Result<Uuid> {
    let Some(reg) = registry.as_ref() else {
        return Ok(Uuid::new_v4());
    };
    let type_ = cfg.type_.as_deref().unwrap_or("unknown").to_string();
    let name = cfg.name.clone();
    if let Some(existing) = reg
        .find_device_by_identity(&type_, name.as_deref())
        .context("registry find")?
    {
        return Ok(existing.id);
    }
    let id = Uuid::new_v4();
    reg.put_device(&DeviceRecord {
        id,
        type_,
        name,
        properties: serde_json::Map::new(),
    })
    .context("registry put")?;
    Ok(id)
}

/// Runtime variant for hubless registration. Honors a caller-supplied
/// id; otherwise consults the registry for (type, name) identity.
fn resolve_runtime_id(
    registry: &Option<Arc<Registry>>,
    type_: &str,
    cfg: &DeviceConfig,
    explicit: Option<Uuid>,
) -> Result<Uuid, DeviceError> {
    if let Some(id) = explicit {
        if let Some(reg) = registry.as_ref() {
            let _ = reg.put_device(&DeviceRecord {
                id,
                type_: type_.to_string(),
                name: cfg.name.clone(),
                properties: serde_json::Map::new(),
            });
        }
        return Ok(id);
    }
    let Some(reg) = registry.as_ref() else {
        return Ok(Uuid::new_v4());
    };
    let found = reg
        .find_device_by_identity(type_, cfg.name.as_deref())
        .map_err(|e| DeviceError::Internal(format!("registry: {e}")))?;
    if let Some(rec) = found {
        return Ok(rec.id);
    }
    let id = Uuid::new_v4();
    reg.put_device(&DeviceRecord {
        id,
        type_: type_.to_string(),
        name: cfg.name.clone(),
        properties: serde_json::Map::new(),
    })
    .map_err(|e| DeviceError::Internal(format!("registry: {e}")))?;
    Ok(id)
}

/// Materialized server pieces, returned by `Zetta::build()`.
pub struct Built {
    pub core: Arc<Core>,
    pub peer_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub app_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub scout_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub router: axum::Router,
    pub acceptors: PeerAcceptors,
    pub peer_streams: zetta_http::PeerStreamHub,
    pub registry: Option<Arc<Registry>>,
}
