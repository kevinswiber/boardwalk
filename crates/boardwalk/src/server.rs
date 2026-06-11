//! Top-level builder for assembling a Boardwalk server.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use url::Url;
use uuid::Uuid;

use crate::http::{
    AppState, Core, PeerHandler, PeerInitState, ResourceRegistrar, ResourceRegistration,
    ResourceRegistrationError, router_with,
};
use crate::peer::{
    PeerAcceptors, PeerAdmission, PeerAdmissionConfig, PeerClient, PeerLink, PeerLinkConfig,
    UnauthenticatedPeerPolicy,
};
use crate::persistence::{
    DefaultRepositories, IdentityKey, NodeConfigRecord, NodeConfigRepository, Repositories,
    ResourceIdentityRecord, ResourceSnapshotRecord, StorageError,
};
use crate::runtime::{
    Actor, ActorCtx, ActorError, DynFuture, Node, NodeBuilder, Resource, ResourceCtx,
    ResourceError, ResourceSnapshot, ResourceSpec, TransitionCtx, TransitionError, TransitionInput,
    TransitionOutcome,
};

pub struct Boardwalk {
    name: String,
    node_id: Option<String>,
    peers: Vec<Url>,
    peer_links: Vec<PeerLinkConfig>,
    accepted_peer_tokens: Vec<PeerAdmissionConfig>,
    unauthenticated_local_peers: Option<UnauthenticatedPeerPolicy>,
    actors: Vec<PendingActor>,
    actor_factories: HashMap<String, ActorFactory>,
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
    id: Option<String>,
    register: RegisterPendingActor,
}

impl PendingActor {
    fn new<A: Actor>(actor: A) -> Self {
        let spec = actor.spec();
        let register =
            Box::new(move |builder: NodeBuilder, id: String| builder.register_with_id(id, actor));
        Self {
            spec,
            id: None,
            register,
        }
    }

    fn with_id<A: Actor>(id: impl Into<String>, actor: A) -> Self {
        let spec = actor.spec();
        let register =
            Box::new(move |builder: NodeBuilder, id: String| builder.register_with_id(id, actor));
        Self {
            spec,
            id: Some(id.into()),
            register,
        }
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
            node_id: None,
            peers: Vec::new(),
            peer_links: Vec::new(),
            accepted_peer_tokens: Vec::new(),
            unauthenticated_local_peers: None,
            actors: Vec::new(),
            actor_factories: HashMap::new(),
            persist_path: None,
        }
    }

    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = n.into();
        self
    }

    pub fn node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = Some(id.into());
        self
    }

    /// Register an actor with this Boardwalk instance.
    pub fn use_actor<A: Actor>(mut self, actor: A) -> Self {
        self.actors.push(PendingActor::new(actor));
        self
    }

    /// Register an actor with a caller-supplied resource id.
    pub fn use_actor_with_id<A: Actor>(mut self, id: impl Into<String>, actor: A) -> Self {
        self.actors.push(PendingActor::with_id(id, actor));
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

    /// Dial a cloud as a local-development peer. The link sends no
    /// admission token, so the accepting cloud must call
    /// [`Boardwalk::allow_unauthenticated_local_peers`] (or configure
    /// token admission) for the upgrade to be admitted.
    ///
    /// # Panics
    /// Panics if `url` does not parse as a URL. Use [`PeerLink::new`]
    /// with [`Boardwalk::link_peer`] to handle invalid input as an
    /// error.
    pub fn link(mut self, url: impl AsRef<str>) -> Self {
        match Url::parse(url.as_ref()) {
            Ok(u) => self.peers.push(u),
            Err(e) => panic!("invalid peer url `{}`: {e}", url.as_ref()),
        }
        self
    }

    /// Dial a remote gateway as a peer using a validated link config:
    /// token credentials, node identity, and a requested capability
    /// set. Validation already happened at [`PeerLink`] construction,
    /// so this method cannot fail.
    pub fn link_peer(mut self, link: PeerLink) -> Self {
        self.peer_links.push(link.into_inner());
        self
    }

    /// Accept a shared-token peer on `/peers/{route_name}` at the
    /// default `resource.read` ceiling. Chain richer config through
    /// [`Boardwalk::accept_peer`] with [`PeerAdmission`] to widen the
    /// ceiling or pin a node id.
    ///
    /// # Panics
    /// Panics if `route_name` is not a valid route name. Use
    /// [`PeerAdmission::shared_token`] to handle invalid input as an
    /// error.
    pub fn accept_peer_token(
        self,
        route_name: impl Into<String>,
        token_id: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        let admission = PeerAdmission::shared_token(route_name, token_id, token)
            .unwrap_or_else(|err| panic!("invalid peer admission config: {err}"));
        self.accept_peer(admission)
    }

    /// Accept token-bound peer admission on `/peers/{route}` at the
    /// config's capability ceiling. Validation already happened at
    /// [`PeerAdmission`] construction, so this method cannot fail.
    pub fn accept_peer(mut self, admission: PeerAdmission) -> Self {
        self.accepted_peer_tokens.push(admission.into_inner());
        self
    }

    /// Accept peers on `/peers/{name}` without token-bound admission.
    /// Local development only: every admitted peer receives the
    /// local-development capability ceiling as its allowed and
    /// negotiated capabilities. Without this call (and with no
    /// `accept_peer_token` config), peer upgrades are refused with
    /// `403` before the WebSocket upgrade. The opt-in applies only
    /// while no token admission is configured; once any
    /// `accept_peer_token` entry exists, token-bound admission is
    /// required for all peers.
    pub fn allow_unauthenticated_local_peers(mut self) -> Self {
        self.unauthenticated_local_peers = Some(UnauthenticatedPeerPolicy::local_development());
        self
    }

    /// Enable on-disk persistence of resource, node, and peer records
    /// at the supplied path (single redb file). Without this call, the
    /// runtime is purely in-memory.
    pub fn persist(mut self, path: impl Into<PathBuf>) -> Self {
        self.persist_path = Some(path.into());
        self
    }

    /// Bind and serve. Blocks until the listener stops.
    pub async fn listen(self, addr: SocketAddr) -> anyhow::Result<()> {
        tracing::info!(%addr, "boardwalk-rs listening");
        let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
        self.listen_on(listener).await
    }

    /// Serve on an already-bound listener.
    pub async fn listen_on(self, listener: tokio::net::TcpListener) -> anyhow::Result<()> {
        let built = self.build()?;
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
        let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
        self.listen_until_on(listener, signal).await
    }

    /// Serve on an already-bound listener until `signal` resolves.
    pub async fn listen_until_on<F: std::future::Future<Output = ()> + Send + 'static>(
        self,
        listener: tokio::net::TcpListener,
        signal: F,
    ) -> anyhow::Result<()> {
        let built = self.build()?;
        if let Ok(addr) = listener.local_addr() {
            tracing::info!(%addr, "boardwalk-rs listening (graceful)");
        }
        let res = axum::serve(listener, built.router)
            .with_graceful_shutdown(signal)
            .await
            .context("serve");

        // Tear down background work.
        for t in built.peer_tasks {
            t.abort();
        }
        built.node.shutdown(Duration::from_secs(1)).await;
        res
    }

    /// Build the runtime + router + spawn peer clients without binding.
    /// Useful for crate-local integration tests.
    pub(crate) fn build(self) -> anyhow::Result<Built> {
        // Open the redb file if persistence was requested. Repository
        // records keep resource IDs stable across restarts.
        let repositories = self
            .persist_path
            .as_ref()
            .map(|p| DefaultRepositories::open(p).map_err(storage_unavailable_error))
            .transpose()?
            .map(Arc::new);

        let persisted_node_config = repositories
            .as_ref()
            .map(|repositories| {
                repositories
                    .node_config()
                    .get_local()
                    .map_err(storage_unavailable_error)
            })
            .transpose()?
            .flatten();
        let node_id = self
            .node_id
            .clone()
            .or_else(|| {
                persisted_node_config
                    .as_ref()
                    .map(|record| record.node_id.clone())
            })
            .unwrap_or_else(|| self.name.clone());
        let local_display_name = self.name.clone();
        let accepted_peer_tokens = self.accepted_peer_tokens.clone();
        let mut node_builder = NodeBuilder::new(node_id.clone());
        for actor in self.actors {
            let PendingActor { spec, id, register } = actor;
            let id = resolve_resource_id(repository_ref(&repositories), &spec, id)?;
            node_builder = register(node_builder, id)
                .map_err(|err| anyhow::anyhow!("register actor: {err:?}"))?;
        }
        let node = Arc::new(
            node_builder
                .try_build()
                .map_err(|err| anyhow::anyhow!("build node: {err:?}"))?,
        );
        persist_local_node_config(
            repository_ref(&repositories),
            &node_id,
            &local_display_name,
            &self.name,
        )?;
        let core: Arc<Core> = Core::from_node_with_name_and_persistence(
            self.name.clone(),
            node.clone(),
            repositories.clone(),
        );

        let peer_init = PeerInitState::default();
        let acceptors = PeerAcceptors::new();
        if let Some(repositories) = repositories.as_ref() {
            acceptors.with_repositories(repositories.clone());
        }

        let handler: PeerHandler = {
            let acceptors = acceptors.clone();
            Arc::new(move |admitted, upgraded| {
                let acceptors = acceptors.clone();
                Box::pin(async move {
                    acceptors.on_upgraded(admitted, upgraded).await;
                })
            })
        };

        let peer_senders: Arc<dyn crate::http::PeerSenders> = Arc::new(acceptors.clone());
        let peer_streams = crate::http::PeerStreamHub::new();

        let resource_registrar =
            build_resource_registrar(self.actor_factories, node.clone(), repositories.clone());

        let state = AppState {
            core: core.clone(),
            peer_handler: Some(handler),
            peer_init: peer_init.clone(),
            peer_senders: Some(peer_senders),
            peer_streams: peer_streams.clone(),
            peer_admission: Arc::new(accepted_peer_tokens),
            unauthenticated_local_peers: self.unauthenticated_local_peers.clone(),
            resource_registrar: resource_registrar.clone(),
        };
        let router = router_with(state);
        // The router served over tunnels this node dials carries the
        // tunnel-leg marker; the public listener serves the unmarked
        // `router`, so forwarded/attested caller headers are honored
        // only on the authenticated tunnel leg.
        let tunnel_router = router
            .clone()
            .layer(axum::Extension(crate::http::TunnelLeg));

        let mut peer_tasks = Vec::new();
        for url in self.peers {
            let local_name = self.name.clone();
            let pc = PeerClient::new(url, local_name, tunnel_router.clone(), peer_init.clone());
            peer_tasks.push(pc.spawn());
        }
        for mut link in self.peer_links {
            if link.local_node_id.is_none() {
                link = link.node_id(node_id.clone());
            }
            if link.local_node_name.is_none() {
                link = link.node_name(local_display_name.clone());
            }
            let pc = PeerClient::from_link(link, tunnel_router.clone(), peer_init.clone());
            peer_tasks.push(pc.spawn());
        }

        Ok(Built {
            core,
            node,
            peer_tasks,
            router,
            acceptors,
            peer_streams,
            peer_admission: self.accepted_peer_tokens,
            unauthenticated_local_peers: self.unauthenticated_local_peers,
            resource_registrar,
            repositories,
        })
    }
}

fn build_resource_registrar(
    factories: HashMap<String, ActorFactory>,
    node: Arc<Node>,
    repositories: Option<Arc<DefaultRepositories>>,
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
                let repositories = repositories.clone();
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
                    let id = resolve_or_create_resource_identity_uuid(
                        repository_ref(&repositories),
                        &spec,
                        explicit_id,
                    )
                    .map_err(registration_identity_error)?;
                    node.register_with_id(id.to_string(), actor)
                        .await
                        .map_err(registration_resource_error)?;
                    if let Ok(Some(snapshot)) = node.resource_snapshot(&id.to_string()).await {
                        persist_latest_resource_snapshot(repository_ref(&repositories), &snapshot);
                    }
                    Ok(id.to_string())
                })
            },
        ),
    )
}

/// Look up a stable resource ID by (kind, name) identity, or mint a new
/// one and persist the record.
fn resolve_resource_id(
    repositories: Option<&dyn Repositories>,
    spec: &ResourceSpec,
    explicit: Option<String>,
) -> anyhow::Result<String> {
    resolve_or_create_resource_identity(repositories, spec, explicit)
        .map_err(resource_identity_anyhow)
}

#[derive(Debug)]
enum ResourceIdentityError {
    Storage,
    Conflict(String),
}

fn resolve_or_create_resource_identity(
    repositories: Option<&dyn Repositories>,
    spec: &ResourceSpec,
    explicit: Option<String>,
) -> Result<String, ResourceIdentityError> {
    let Some(repositories) = repositories else {
        return Ok(explicit.unwrap_or_else(|| Uuid::new_v4().to_string()));
    };
    let identities = repositories.resource_identities();
    let identity_keys = resource_identity_keys(spec);

    if let Some(id) = explicit {
        if let Some(existing) = identities
            .get(&id)
            .map_err(resource_identity_storage_error)?
        {
            if resource_identity_record_matches(&existing, spec) {
                return Ok(id);
            }
            return Err(ResourceIdentityError::Conflict(format!(
                "resource id `{id}` is already registered"
            )));
        }
        if let Some(existing) = find_resource_identity(identities, &identity_keys)?
            && existing.id != id
        {
            return Err(ResourceIdentityError::Conflict(format!(
                "resource identity `{}` is already registered",
                resource_identity_label(spec)
            )));
        }
        put_resource_identity(identities, spec, &identity_keys, &id)?;
        return Ok(id);
    }

    if let Some(existing) = find_resource_identity(identities, &identity_keys)? {
        return Ok(existing.id);
    }

    let id = Uuid::new_v4().to_string();
    put_resource_identity(identities, spec, &identity_keys, &id)?;
    Ok(id)
}

fn resolve_or_create_resource_identity_uuid(
    repositories: Option<&dyn Repositories>,
    spec: &ResourceSpec,
    explicit: Option<Uuid>,
) -> Result<Uuid, ResourceIdentityError> {
    let id =
        resolve_or_create_resource_identity(repositories, spec, explicit.map(|id| id.to_string()))?;
    Uuid::parse_str(&id).map_err(|_| {
        ResourceIdentityError::Conflict(format!(
            "resource identity `{}` is already registered",
            resource_identity_label(spec)
        ))
    })
}

fn put_resource_identity(
    identities: &dyn crate::persistence::ResourceIdentityRepository,
    spec: &ResourceSpec,
    identity_keys: &[IdentityKey],
    id: &str,
) -> Result<(), ResourceIdentityError> {
    let now_ms = now_ms();
    identities
        .put(ResourceIdentityRecord {
            id: id.into(),
            kind: spec.kind.clone(),
            name: spec.name.clone(),
            identity_keys: identity_keys.to_vec(),
            labels: BTreeMap::new(),
            created_ms: now_ms,
            updated_ms: now_ms,
        })
        .map_err(resource_identity_storage_error)
}

fn find_resource_identity(
    identities: &dyn crate::persistence::ResourceIdentityRepository,
    identity_keys: &[IdentityKey],
) -> Result<Option<ResourceIdentityRecord>, ResourceIdentityError> {
    for key in identity_keys {
        if let Some(record) = identities
            .find_by_identity_key(key)
            .map_err(resource_identity_storage_error)?
        {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

fn resource_identity_record_matches(record: &ResourceIdentityRecord, spec: &ResourceSpec) -> bool {
    record.kind == spec.kind && record.name == spec.name
}

fn resource_identity_keys(spec: &ResourceSpec) -> Vec<IdentityKey> {
    match spec.name.as_deref() {
        Some(name) => vec![IdentityKey::static_name(spec.kind.clone(), name.to_owned())],
        None => vec![IdentityKey::static_unnamed(spec.kind.clone())],
    }
}

fn resource_identity_label(spec: &ResourceSpec) -> String {
    match &spec.name {
        Some(name) => format!("{}:{name}", spec.kind),
        None => spec.kind.clone(),
    }
}

fn resource_identity_anyhow(err: ResourceIdentityError) -> anyhow::Error {
    match err {
        ResourceIdentityError::Storage => anyhow::anyhow!("storage unavailable"),
        ResourceIdentityError::Conflict(msg) => anyhow::anyhow!(msg),
    }
}

fn registration_identity_error(err: ResourceIdentityError) -> ResourceRegistrationError {
    match err {
        ResourceIdentityError::Storage => {
            ResourceRegistrationError::Internal("storage unavailable".into())
        }
        ResourceIdentityError::Conflict(msg) => ResourceRegistrationError::Conflict(msg),
    }
}

fn resource_identity_storage_error(err: StorageError) -> ResourceIdentityError {
    match err {
        StorageError::Conflict(msg) => ResourceIdentityError::Conflict(msg),
        StorageError::Unavailable(_) | StorageError::Corrupt(_) | StorageError::Internal(_) => {
            ResourceIdentityError::Storage
        }
    }
}

fn storage_unavailable_error(_err: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!("storage unavailable")
}

fn repository_ref(repositories: &Option<Arc<DefaultRepositories>>) -> Option<&dyn Repositories> {
    repositories
        .as_deref()
        .map(|repositories| repositories as &dyn Repositories)
}

fn persist_local_node_config(
    repositories: Option<&dyn Repositories>,
    node_id: &str,
    display_name: &str,
    route_name: &str,
) -> anyhow::Result<()> {
    let Some(repositories) = repositories else {
        return Ok(());
    };
    repositories
        .node_config()
        .put(NodeConfigRecord {
            node_id: node_id.into(),
            display_name: display_name.into(),
            route_name: route_name.into(),
            updated_ms: now_ms(),
        })
        .map_err(storage_unavailable_error)
}

fn persist_latest_resource_snapshot(
    repositories: Option<&dyn Repositories>,
    snapshot: &ResourceSnapshot,
) {
    let Some(repositories) = repositories else {
        return;
    };
    let record = ResourceSnapshotRecord::latest(snapshot.clone(), now_ms());
    if let Err(err) = repositories.resource_snapshots().upsert_latest(record) {
        tracing::warn!(error = %err, resource_id = %snapshot.id, "failed to persist latest resource snapshot");
    }
}

fn now_ms() -> i64 {
    (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
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
    pub(crate) router: axum::Router,
    pub(crate) acceptors: PeerAcceptors,
    pub(crate) peer_streams: crate::http::PeerStreamHub,
    pub(crate) peer_admission: Vec<PeerAdmissionConfig>,
    pub(crate) unauthenticated_local_peers: Option<UnauthenticatedPeerPolicy>,
    pub(crate) resource_registrar: Option<ResourceRegistrar>,
    pub(crate) repositories: Option<Arc<DefaultRepositories>>,
}

impl Built {
    #[cfg(test)]
    pub(crate) fn repositories(&self) -> Option<&dyn Repositories> {
        repository_ref(&self.repositories)
    }
}
