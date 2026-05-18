//! The `Actor` trait, lifecycle hooks, and transition error model.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::context::{CommandId, RequestCtx};
use super::node::Node;
use super::resource::{DynFuture, Resource, ResourceError};
use crate::core::{TransitionInput, TransitionOutcome};

/// Per-transition context. Mints a fresh `CommandId` on construction
/// and carries the request correlation so envelopes published in the
/// handler can populate `causationId` and trace headers.
///
/// `node` is optional so test-only constructors and HTTP boundaries
/// that have not yet wired the runtime can still build a context.
#[derive(Clone)]
pub struct TransitionCtx {
    command_id: CommandId,
    request: RequestCtx,
    node_id: String,
    node: Option<Arc<Node>>,
}

impl std::fmt::Debug for TransitionCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransitionCtx")
            .field("command_id", &self.command_id)
            .field("request", &self.request)
            .field("node_id", &self.node_id)
            .field("node_attached", &self.node.is_some())
            .finish()
    }
}

impl TransitionCtx {
    pub fn new(request: RequestCtx, node: impl Into<String>) -> Self {
        Self {
            command_id: CommandId::new(),
            request,
            node_id: node.into(),
            node: None,
        }
    }

    /// Build a context backed by an `Arc<Node>` so
    /// `register_actor` can route through the node's directory.
    pub fn with_node(request: RequestCtx, node: Arc<Node>) -> Self {
        let node_id = node.id().to_string();
        Self {
            command_id: CommandId::new(),
            request,
            node_id,
            node: Some(node),
        }
    }

    /// Test-only constructor used by trait-shape compile tests.
    pub fn new_test() -> Self {
        Self::new(RequestCtx::default(), "test")
    }

    pub fn command_id(&self) -> &CommandId {
        &self.command_id
    }
    pub fn request(&self) -> &RequestCtx {
        &self.request
    }
    pub fn node(&self) -> &str {
        &self.node_id
    }

    /// Register an actor-created resource on the same node and return
    /// its newly assigned resource id. Requires a context built via
    /// `TransitionCtx::with_node`; otherwise returns `Internal`.
    pub async fn register_actor<A: Actor>(&self, actor: A) -> Result<String, TransitionError> {
        let Some(node) = self.node.as_ref() else {
            return Err(TransitionError::Internal(
                "TransitionCtx has no Node attached; build with TransitionCtx::with_node".into(),
            ));
        };
        node.register_actor(actor)
            .await
            .map_err(TransitionError::from)
    }
}

impl From<ResourceError> for TransitionError {
    fn from(err: ResourceError) -> Self {
        match err {
            ResourceError::NotFound(id) => TransitionError::ResourceNotFound(id),
            ResourceError::Unavailable(msg) => TransitionError::Internal(msg),
            ResourceError::Internal(msg) => TransitionError::Internal(msg),
        }
    }
}

/// Per-actor lifecycle context. Carries the node id, the resource id
/// assigned by the runtime, the kind, and the labels so the actor can
/// reason about its identity without reaching into HTTP state.
#[derive(Clone, Debug, Default)]
pub struct ActorCtx {
    node: String,
    resource_id: String,
    resource_kind: String,
    labels: BTreeMap<String, String>,
}

impl ActorCtx {
    pub fn new(
        node: impl Into<String>,
        resource_id: impl Into<String>,
        resource_kind: impl Into<String>,
        labels: BTreeMap<String, String>,
    ) -> Self {
        Self {
            node: node.into(),
            resource_id: resource_id.into(),
            resource_kind: resource_kind.into(),
            labels,
        }
    }

    /// Test-only constructor used by lifecycle-shape compile tests.
    pub fn new_test() -> Self {
        Self::default()
    }

    pub fn node(&self) -> &str {
        &self.node
    }
    pub fn resource_id(&self) -> &str {
        &self.resource_id
    }
    pub fn resource_kind(&self) -> &str {
        &self.resource_kind
    }
    pub fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }
}

/// Failure modes for `Actor::transition`. The HTTP boundary maps
/// these onto 400 / 405 / 409 / 503 / 504 / 404 / 500 in later phases.
#[derive(Debug)]
pub enum TransitionError {
    InvalidInput(String),
    NotAllowed(String),
    Conflict(String),
    Busy,
    BackpressureRequired,
    Timeout,
    ResourceNotFound(String),
    Internal(String),
}

/// Failure modes for `on_start` / `on_stop` lifecycle hooks. Carried
/// separately so the runtime can decide whether to retry, escalate,
/// or unregister the actor.
#[derive(Debug)]
pub enum ActorError {
    StartFailed(String),
    StopFailed(String),
    Internal(String),
}

/// Executable resource: drives state through transitions and owns
/// lifecycle hooks. `&mut self` on the hooks lets actors own state
/// without interior mutability; the runtime guarantees serialized
/// access in Phase 3.
pub trait Actor: Resource {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>>;

    /// Default no-op lifecycle hook. Override to start background
    /// tasks, open connections, or seed state.
    fn on_start<'a>(&'a mut self, _ctx: ActorCtx) -> DynFuture<'a, Result<(), ActorError>> {
        Box::pin(async { Ok(()) })
    }

    /// Default no-op lifecycle hook. Override to flush state and
    /// release external resources.
    fn on_stop<'a>(&'a mut self, _ctx: ActorCtx) -> DynFuture<'a, Result<(), ActorError>> {
        Box::pin(async { Ok(()) })
    }
}
