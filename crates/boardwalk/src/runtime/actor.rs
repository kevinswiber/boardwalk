//! The `Actor` trait, lifecycle hooks, and transition error model.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value as JsonValue;

use super::context::{
    CallerProvenance, CommandId, EmissionContext, EnvelopePlan, Publisher, RequestCtx,
};
use super::node::Node;
use super::resource::{DynFuture, Resource, ResourceError, ResourceSnapshot, TransitionOutcome};
use super::transition::TransitionInput;
use crate::events::{NodeId, PublishError, TraceContext};

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
    /// Identity of the actor servicing this transition. The executor
    /// populates it before calling `Actor::transition` so the handler
    /// can `publish` without rebuilding the resource address itself.
    actor: Option<ActorCtx>,
}

impl std::fmt::Debug for TransitionCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransitionCtx")
            .field("command_id", &self.command_id)
            .field("request", &self.request)
            .field("node_id", &self.node_id)
            .field("node_attached", &self.node.is_some())
            .field("actor", &self.actor)
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
            actor: None,
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
            actor: None,
        }
    }

    /// Test-only constructor used by trait-shape compile tests.
    pub fn new_test() -> Self {
        Self::new(RequestCtx::default(), "test")
    }

    /// Caller provenance for this invocation, as established by the
    /// runtime: local/anonymous by default, forwarded-with-admitted-peer
    /// when the request arrived over an authenticated peer tunnel.
    pub fn provenance(&self) -> &CallerProvenance {
        self.request.provenance()
    }

    /// Attach actor identity so `publish` can build envelopes addressed
    /// to the resource servicing this transition. Crate-private: only
    /// the executor calls this on its way to invoking the handler.
    pub(crate) fn with_actor(mut self, actor: ActorCtx) -> Self {
        self.actor = Some(actor);
        self
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

    pub fn resource_id(&self) -> Option<&str> {
        self.actor.as_ref().map(|actor| actor.resource_id())
    }

    pub fn resource_id_required(&self) -> Result<&str, TransitionError> {
        self.resource_id()
            .ok_or_else(|| TransitionError::Internal("TransitionCtx has no actor identity".into()))
    }

    pub fn completed(
        &self,
        output: Option<JsonValue>,
        mut snapshot: ResourceSnapshot,
    ) -> Result<TransitionOutcome, TransitionError> {
        snapshot.id = self.resource_id_required()?.to_string();
        snapshot.node = self.node().to_string();
        Ok(TransitionOutcome::Completed { output, snapshot })
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

    /// Publish an envelope on `stream` for this transition's resource.
    /// Populates `correlationId` from the inbound `x-request-id`,
    /// `causationId` from the minted `CommandId`, and `traceContext`
    /// from `traceparent` / `tracestate`. Returns `Internal` if the
    /// executor did not attach actor identity (only possible on
    /// test-only contexts that bypass the runtime).
    pub async fn publish(
        &self,
        stream: &str,
        payload_kind: &str,
        payload_version: u32,
        data: JsonValue,
    ) -> Result<(), TransitionError> {
        let actor = self.actor.as_ref().ok_or_else(|| {
            TransitionError::Internal("TransitionCtx has no actor identity".into())
        })?;
        self.publish_for_resource(
            &actor.resource_id,
            &actor.resource_kind,
            stream,
            payload_kind,
            payload_version,
            data,
        )
        .await
    }

    pub(crate) async fn publish_for_resource(
        &self,
        resource_id: &str,
        resource_kind: &str,
        stream: &str,
        payload_kind: &str,
        payload_version: u32,
        data: JsonValue,
    ) -> Result<(), TransitionError> {
        let actor = self.actor.as_ref().ok_or_else(|| {
            TransitionError::Internal("TransitionCtx has no actor identity".into())
        })?;
        let publisher = actor.publisher.as_ref().ok_or_else(|| {
            TransitionError::Internal("ActorCtx has no publisher attached".into())
        })?;
        let trace = self.request.traceparent().map(|tp| TraceContext {
            traceparent: tp.to_string(),
            tracestate: self.request.tracestate().map(String::from),
        });
        let node_id = NodeId::new(actor.node.clone());
        publisher
            .publish(
                EnvelopePlan {
                    node_id: &node_id,
                    resource_id,
                    resource_kind,
                    stream,
                    payload_kind,
                    payload_version,
                    data,
                },
                EmissionContext {
                    correlation: self.request.request_id(),
                    causation: Some(self.command_id.as_str()),
                    trace,
                },
            )
            .await
            .map_err(transition_publish_error)
    }
}

fn transition_publish_error(err: PublishError) -> TransitionError {
    TransitionError::Internal(format!("publish failed: {err}"))
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
#[derive(Clone, Default)]
pub struct ActorCtx {
    pub(crate) node: String,
    pub(crate) resource_id: String,
    pub(crate) resource_kind: String,
    pub(crate) labels: BTreeMap<String, String>,
    /// Bus + registry attached by the runtime so `publish` can mint
    /// envelopes through the shared `StreamRegistry`. `None` for
    /// hand-built contexts (test fixtures); `publish` returns
    /// `Internal` in that case.
    pub(crate) publisher: Option<Publisher>,
}

impl std::fmt::Debug for ActorCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorCtx")
            .field("node", &self.node)
            .field("resource_id", &self.resource_id)
            .field("resource_kind", &self.resource_kind)
            .field("labels", &self.labels)
            .field("publisher_attached", &self.publisher.is_some())
            .finish()
    }
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
            publisher: None,
        }
    }

    /// Attach a publisher so `publish` (and `TransitionCtx::publish`
    /// for any clone of this context) can mint envelopes through the
    /// shared registry. Crate-private: only the node sets this when
    /// spawning the actor.
    pub(crate) fn with_publisher(mut self, publisher: Publisher) -> Self {
        self.publisher = Some(publisher);
        self
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

    /// Publish an envelope on `stream` for this actor. Lifecycle
    /// emissions have no inbound request, so `correlationId`,
    /// `causationId`, and `traceContext` are all left `None`. Returns
    /// `Internal` if the runtime did not attach a publisher.
    pub async fn publish(
        &self,
        stream: &str,
        payload_kind: &str,
        payload_version: u32,
        data: JsonValue,
    ) -> Result<(), ActorError> {
        let publisher = self
            .publisher
            .as_ref()
            .ok_or_else(|| ActorError::Internal("ActorCtx has no publisher attached".into()))?;
        let node_id = NodeId::new(self.node.clone());
        publisher
            .publish(
                EnvelopePlan {
                    node_id: &node_id,
                    resource_id: &self.resource_id,
                    resource_kind: &self.resource_kind,
                    stream,
                    payload_kind,
                    payload_version,
                    data,
                },
                EmissionContext::default(),
            )
            .await
            .map_err(|e| ActorError::Internal(format!("publish failed: {e}")))
    }
}

/// Failure modes for `Actor::transition`. HTTP renderers can map these
/// onto 400 / 405 / 409 / 503 / 504 / 404 / 500 responses.
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

impl TransitionError {
    pub fn internal(msg: impl std::fmt::Display) -> Self {
        TransitionError::Internal(msg.to_string())
    }
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

impl ActorError {
    pub fn internal(msg: impl std::fmt::Display) -> Self {
        ActorError::Internal(msg.to_string())
    }
}

/// Executable resource: drives state through transitions and owns
/// lifecycle hooks. `&mut self` on the hooks lets actors own state
/// without interior mutability; the runtime serializes access to each
/// actor through a bounded command channel.
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

#[cfg(test)]
mod tests {
    use super::super::context::AdmittedPeer;
    use super::*;

    #[test]
    fn transition_ctx_defaults_to_local_anonymous_provenance() {
        let ctx = TransitionCtx::new_test();
        let provenance = ctx.provenance();
        assert!(provenance.is_local());
        assert!(provenance.peer().is_none());
        assert!(provenance.forwarded_by().is_none());
    }

    #[test]
    fn request_ctx_carries_forwarded_provenance_into_transition_ctx() {
        let peer = AdmittedPeer::new(
            "reviewer-respond",
            "peer-reviewer-respond-rs-1",
            Some("rs-1".to_string()),
            Some("node-reviewer-9".to_string()),
            None,
            vec![
                crate::peer::PeerCapability::ResourceRead,
                crate::peer::PeerCapability::TransitionInvoke,
            ],
            "00000000-0000-0000-0000-000000000001",
        );
        let request =
            RequestCtx::default().with_provenance(CallerProvenance::forwarded("cloud", Some(peer)));
        let ctx = TransitionCtx::new(request, "hub");

        let provenance = ctx.provenance();
        assert!(!provenance.is_local());
        assert_eq!(provenance.forwarded_by(), Some("cloud"));
        let peer = provenance.peer().expect("admitted caller");
        assert_eq!(peer.peer_id(), "peer-reviewer-respond-rs-1");
        assert_eq!(peer.node_id(), Some("node-reviewer-9"));
        assert!(peer.has_capability(crate::peer::PeerCapability::TransitionInvoke));
        assert_eq!(peer.connection_id(), "00000000-0000-0000-0000-000000000001");
    }

    #[test]
    fn transition_error_internal_wraps_display() {
        let err = TransitionError::internal("boom");
        assert!(matches!(err, TransitionError::Internal(s) if s == "boom"));
    }

    #[test]
    fn actor_error_internal_wraps_display() {
        let err = ActorError::internal("boom");
        assert!(matches!(err, ActorError::Internal(s) if s == "boom"));
    }

    #[test]
    fn internal_accepts_any_display_not_just_str() {
        let serde_err = serde_json::from_str::<u8>("\"x\"").unwrap_err();
        let err = TransitionError::internal(serde_err);
        assert!(matches!(err, TransitionError::Internal(_)));
    }

    #[test]
    fn resource_id_required_errors_without_actor_identity() {
        let ctx = TransitionCtx::new_test();
        let err = ctx.resource_id_required().unwrap_err();
        assert!(
            matches!(err, TransitionError::Internal(s) if s == "TransitionCtx has no actor identity")
        );
    }

    #[test]
    fn resource_id_required_returns_id_when_attached() {
        let actor = ActorCtx::new("node", "res-1", "job", Default::default());
        let ctx = TransitionCtx::new_test().with_actor(actor);
        assert_eq!(ctx.resource_id_required().unwrap(), "res-1");
    }

    #[test]
    fn completed_fills_snapshot_identity_from_context() {
        let actor = ActorCtx::new("runner", "job-1", "job", Default::default());
        let ctx = TransitionCtx::new(RequestCtx::default(), "runner").with_actor(actor);

        let outcome = ctx
            .completed(
                Some(serde_json::json!({ "accepted": true })),
                crate::runtime::ResourceSnapshot::builder("job")
                    .state("cancelled")
                    .build(),
            )
            .unwrap();

        let TransitionOutcome::Completed { output, snapshot } = outcome else {
            panic!("expected completed outcome");
        };
        assert_eq!(output, Some(serde_json::json!({ "accepted": true })));
        assert_eq!(snapshot.id, "job-1");
        assert_eq!(snapshot.node, "runner");
        assert_eq!(snapshot.kind, "job");
        assert_eq!(snapshot.state.as_deref(), Some("cancelled"));
    }

    #[test]
    fn completed_errors_without_actor_identity() {
        let ctx = TransitionCtx::new_test();
        let err = ctx
            .completed(
                None,
                crate::runtime::ResourceSnapshot::builder("job").build(),
            )
            .unwrap_err();

        assert!(
            matches!(err, TransitionError::Internal(s) if s == "TransitionCtx has no actor identity")
        );
    }
}
