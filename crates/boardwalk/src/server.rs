//! Top-level builder for assembling a Boardwalk server.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use url::Url;
use uuid::Uuid;

use crate::http::{
    App, AppState, Core, PeerHandler, PeerInitState, ResourceRegistrar, ResourceRegistration,
    ResourceRegistrationError, Scout, ScoutCtx, ServerHandle, router_with,
};
use crate::peer::{PeerAcceptors, PeerClient};
use crate::registry::{DeviceRecord, Registry};
use crate::runtime::{
    Actor, ActorCtx, ActorError, DynFuture, Node, NodeBuilder, Resource, ResourceCtx,
    ResourceError, ResourceSnapshot, ResourceSpec, TransitionCtx, TransitionError, TransitionInput,
    TransitionOutcome,
};

pub struct Boardwalk {
    name: String,
    peers: Vec<Url>,
    actors: Vec<PendingActor>,
    actor_factories: HashMap<String, ActorFactory>,
    apps: Vec<Arc<dyn App>>,
    scouts: Vec<Arc<dyn Scout>>,
    persist_path: Option<PathBuf>,
}

type ActorFactory = Arc<
    dyn Fn(ResourceRegistration) -> Result<FactoryActor, ResourceRegistrationError> + Send + Sync,
>;

struct FactoryActor {
    inner: Box<dyn Actor>,
}

impl FactoryActor {
    fn new<A: Actor>(actor: A) -> Self {
        Self {
            inner: Box::new(actor),
        }
    }
}

impl Resource for FactoryActor {
    fn spec(&self) -> ResourceSpec {
        self.inner.spec()
    }

    fn snapshot<'a>(
        &'a self,
        ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        self.inner.snapshot(ctx)
    }
}

impl Actor for FactoryActor {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        self.inner.transition(ctx, name, input)
    }

    fn on_start<'a>(&'a mut self, ctx: ActorCtx) -> DynFuture<'a, Result<(), ActorError>> {
        self.inner.on_start(ctx)
    }

    fn on_stop<'a>(&'a mut self, ctx: ActorCtx) -> DynFuture<'a, Result<(), ActorError>> {
        self.inner.on_stop(ctx)
    }
}

type RegisterPendingActor =
    Box<dyn FnOnce(NodeBuilder, String) -> Result<NodeBuilder, ResourceError> + Send>;

struct PendingActor {
    spec: ResourceSpec,
    register: RegisterPendingActor,
}

impl PendingActor {
    fn new<A: Actor>(actor: A) -> Self {
        let spec = actor.spec();
        let register =
            Box::new(move |builder: NodeBuilder, id: String| builder.register_with_id(id, actor));
        Self { spec, register }
    }
}

impl Default for Boardwalk {
    fn default() -> Self {
        Self::new()
    }
}

impl Boardwalk {
    pub fn new() -> Self {
        Self {
            name: "boardwalk".to_string(),
            peers: Vec::new(),
            actors: Vec::new(),
            actor_factories: HashMap::new(),
            apps: Vec::new(),
            scouts: Vec::new(),
            persist_path: None,
        }
    }

    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = n.into();
        self
    }

    /// Register an actor with this Boardwalk instance.
    pub fn use_actor<A: Actor>(mut self, actor: A) -> Self {
        self.actors.push(PendingActor::new(actor));
        self
    }

    #[allow(dead_code)]
    pub(crate) fn register_actor_factory<A, F>(
        mut self,
        kind: impl Into<String>,
        factory: F,
    ) -> Self
    where
        A: Actor,
        F: Fn(ResourceRegistration) -> Result<A, ResourceRegistrationError> + Send + Sync + 'static,
    {
        self.actor_factories.insert(
            kind.into(),
            Arc::new(move |registration| Ok(FactoryActor::new(factory(registration)?))),
        );
        self
    }

    #[allow(dead_code)]
    pub(crate) fn use_app<A: App>(mut self, a: A) -> Self {
        self.apps.push(Arc::new(a));
        self
    }

    #[allow(dead_code)]
    pub(crate) fn use_scout<S: Scout>(mut self, s: S) -> Self {
        self.scouts.push(Arc::new(s));
        self
    }

    pub fn link(mut self, url: impl AsRef<str>) -> Self {
        match Url::parse(url.as_ref()) {
            Ok(u) => self.peers.push(u),
            Err(e) => tracing::warn!(?e, url = url.as_ref(), "ignoring invalid peer url"),
        }
        self
    }

    /// Enable on-disk persistence of resource + peer registries at the
    /// supplied path (single redb file). Without this call, the runtime
    /// is purely in-memory.
    pub fn persist(mut self, path: impl Into<PathBuf>) -> Self {
        self.persist_path = Some(path.into());
        self
    }

    /// Bind and serve. Blocks until the listener stops.
    pub async fn listen(self, addr: SocketAddr) -> anyhow::Result<()> {
        let built = self.build()?;
        tracing::info!(%addr, "boardwalk-rs listening");
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
        tracing::info!(%addr, "boardwalk-rs listening (graceful)");
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
        built.node.shutdown(Duration::from_secs(1)).await;
        res
    }

    /// Build the runtime + router + spawn peer clients without binding.
    /// Useful for crate-local integration tests.
    pub(crate) fn build(self) -> anyhow::Result<Built> {
        // Open the registry if persistence was requested. Resource
        // IDs are then stable across restarts (keyed by kind + name).
        let registry = self
            .persist_path
            .as_ref()
            .map(|p| Registry::open(p).context("opening registry"))
            .transpose()?
            .map(Arc::new);

        let mut node_builder = NodeBuilder::new(self.name.clone());
        for actor in self.actors {
            let id = resolve_resource_id(&registry, &actor.spec)?;
            node_builder = (actor.register)(node_builder, id)
                .map_err(|err| anyhow::anyhow!("register actor: {err:?}"))?;
        }
        let node = Arc::new(
            node_builder
                .try_build()
                .map_err(|err| anyhow::anyhow!("build node: {err:?}"))?,
        );
        let core: Arc<Core> = Core::from_node(node.clone());

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

        let peer_senders: Arc<dyn crate::http::PeerSenders> = Arc::new(acceptors.clone());
        let peer_streams = crate::http::PeerStreamHub::new();

        let resource_registrar = build_resource_registrar(
            self.actor_factories,
            node.clone(),
            core.clone(),
            registry.clone(),
        );

        let state = AppState {
            core: core.clone(),
            peer_handler: Some(handler),
            peer_init: peer_init.clone(),
            peer_senders: Some(peer_senders),
            peer_streams: peer_streams.clone(),
            resource_registrar,
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
            node,
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

fn build_resource_registrar(
    factories: HashMap<String, ActorFactory>,
    node: Arc<Node>,
    core: Arc<Core>,
    registry: Option<Arc<Registry>>,
) -> Option<ResourceRegistrar> {
    if factories.is_empty() {
        return None;
    }

    let factories = Arc::new(factories);
    Some(
        Arc::new(
            move |registration: ResourceRegistration| -> futures::future::BoxFuture<
                'static,
                Result<String, ResourceRegistrationError>,
            > {
                let factories = factories.clone();
                let node = node.clone();
                let core = core.clone();
                let registry = registry.clone();
                Box::pin(async move {
                    let kind = registration.kind.clone();
                    let explicit_id = registration.id;
                    let factory = factories.get(&kind).ok_or_else(|| {
                        ResourceRegistrationError::Invalid(format!(
                            "unknown resource kind `{kind}`"
                        ))
                    })?;
                    let actor = factory(registration)?;
                    let spec = actor.spec();
                    let id = resolve_registration_id(&registry, &spec, explicit_id)?;
                    node.register_with_id(id.to_string(), actor)
                        .await
                        .map_err(registration_resource_error)?;
                    persist_registered_resource(&registry, &spec, id)?;
                    core.notify_resource_registered();
                    Ok(id.to_string())
                })
            },
        ),
    )
}

/// Look up a stable resource ID by (kind, name) identity, or mint a new
/// one and persist the record.
fn resolve_resource_id(
    registry: &Option<Arc<Registry>>,
    spec: &ResourceSpec,
) -> anyhow::Result<String> {
    let Some(reg) = registry.as_ref() else {
        return Ok(Uuid::new_v4().to_string());
    };
    let type_ = spec.kind.clone();
    let name = spec.name.clone();
    if let Some(existing) = reg
        .find_device_by_identity(&type_, name.as_deref())
        .context("registry find")?
    {
        return Ok(existing.id.to_string());
    }
    let id = Uuid::new_v4();
    reg.put_device(&DeviceRecord {
        id,
        type_,
        name,
        properties: serde_json::Map::new(),
    })
    .context("registry put")?;
    Ok(id.to_string())
}

fn resolve_registration_id(
    registry: &Option<Arc<Registry>>,
    spec: &ResourceSpec,
    explicit: Option<Uuid>,
) -> Result<Uuid, ResourceRegistrationError> {
    if let Some(id) = explicit {
        return Ok(id);
    }
    let Some(reg) = registry.as_ref() else {
        return Ok(Uuid::new_v4());
    };
    if let Some(existing) = reg
        .find_device_by_identity(&spec.kind, spec.name.as_deref())
        .map_err(registration_registry_error)?
    {
        return Ok(existing.id);
    }
    Ok(Uuid::new_v4())
}

fn persist_registered_resource(
    registry: &Option<Arc<Registry>>,
    spec: &ResourceSpec,
    id: Uuid,
) -> Result<(), ResourceRegistrationError> {
    let Some(reg) = registry.as_ref() else {
        return Ok(());
    };
    reg.put_device(&DeviceRecord {
        id,
        type_: spec.kind.clone(),
        name: spec.name.clone(),
        properties: serde_json::Map::new(),
    })
    .map_err(registration_registry_error)
}

fn registration_registry_error(err: crate::registry::RegistryError) -> ResourceRegistrationError {
    ResourceRegistrationError::Internal(format!("registry: {err}"))
}

fn registration_resource_error(err: ResourceError) -> ResourceRegistrationError {
    match err {
        ResourceError::NotFound(id) => {
            ResourceRegistrationError::Invalid(format!("unknown resource `{id}`"))
        }
        ResourceError::Unavailable(msg) => ResourceRegistrationError::Internal(msg),
        ResourceError::Internal(msg) if msg.starts_with("duplicate resource id: ") => {
            ResourceRegistrationError::Conflict(msg)
        }
        ResourceError::Internal(msg) => ResourceRegistrationError::Internal(msg),
    }
}

/// Materialized server pieces, returned by `Boardwalk::build()`.
#[allow(dead_code)]
pub(crate) struct Built {
    pub(crate) core: Arc<Core>,
    pub(crate) node: Arc<Node>,
    pub(crate) peer_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub(crate) app_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub(crate) scout_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub(crate) router: axum::Router,
    pub(crate) acceptors: PeerAcceptors,
    pub(crate) peer_streams: crate::http::PeerStreamHub,
    pub(crate) registry: Option<Arc<Registry>>,
}
