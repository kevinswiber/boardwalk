//! Request and command context carried through transitions and
//! actor lifecycle.
//!
//! `RequestCtx` captures the W3C trace context (`traceparent`,
//! `tracestate`) and the `x-request-id` header so downstream code can
//! attach them to envelopes without re-parsing HTTP state.
//! `TransitionCtx` mints a fresh `CommandId` per call to use as
//! `causationId` on emitted envelopes.

use axum::http::HeaderMap;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::events::{
    CausationId, CorrelationId, ENVELOPE_VERSION, EventBus, EventEnvelope, NodeId, PublishError,
    StreamId, StreamRegistry, TraceContext,
};
use crate::peer::PeerCapability;

/// Read-only identity of an admitted peer caller, as established at
/// admission time on the gateway. Populated exclusively by the runtime
/// from admission state — never from client-supplied headers; public
/// code cannot construct or forge one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedPeer {
    route_name: String,
    peer_id: String,
    token_id: Option<String>,
    node_id: Option<String>,
    display_name: Option<String>,
    capabilities: Vec<PeerCapability>,
    connection_id: String,
}

impl AdmittedPeer {
    pub(crate) fn new(
        route_name: impl Into<String>,
        peer_id: impl Into<String>,
        token_id: Option<String>,
        node_id: Option<String>,
        display_name: Option<String>,
        capabilities: Vec<PeerCapability>,
        connection_id: impl Into<String>,
    ) -> Self {
        Self {
            route_name: route_name.into(),
            peer_id: peer_id.into(),
            token_id,
            node_id,
            display_name,
            capabilities,
            connection_id: connection_id.into(),
        }
    }

    /// Peer route this caller was admitted on (`/peers/{route}`).
    pub fn route_name(&self) -> &str {
        &self.route_name
    }

    /// Durable peer identity. Derived as `peer-{route}` (unauthenticated)
    /// or `peer-{route}-{token_id}` (token-bound): stable across
    /// reconnects of the same configured link; token rotation yields a
    /// new peer id.
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Token id the peer presented at admission, if token-bound.
    pub fn token_id(&self) -> Option<&str> {
        self.token_id.as_deref()
    }

    /// Self-asserted node id, pinned only if the admission config set
    /// `expected_node_id`. Not cryptographic identity.
    pub fn node_id(&self) -> Option<&str> {
        self.node_id.as_deref()
    }

    /// Human-readable display name the peer presented, if any.
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    /// The negotiated capability set (requested ∩ allowed) — the live
    /// grant for this connection, not the configured ceiling.
    pub fn capabilities(&self) -> &[PeerCapability] {
        &self.capabilities
    }

    /// Whether the negotiated set includes `capability`.
    pub fn has_capability(&self, capability: PeerCapability) -> bool {
        self.capabilities.contains(&capability)
    }

    /// Connection id of the admitted tunnel, for audit correlation
    /// with peer connection-status records.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
}

/// Where a transition invocation came from, as far as this node can
/// verify: local/direct ([`CallerProvenance::is_local`]), or forwarded
/// over an authenticated peer tunnel by a gateway
/// ([`CallerProvenance::forwarded_by`]), optionally with the
/// gateway-attested admitted caller ([`CallerProvenance::peer`]).
///
/// Provenance is populated only by the runtime. `ResourceCtx` does not
/// carry provenance in this release — no caller-identity-bearing path
/// reaches resource reads yet.
#[derive(Debug, Clone, Default)]
pub struct CallerProvenance {
    forwarded_by: Option<String>,
    peer: Option<AdmittedPeer>,
}

impl CallerProvenance {
    pub(crate) fn forwarded(gateway: impl Into<String>, peer: Option<AdmittedPeer>) -> Self {
        Self {
            forwarded_by: Some(gateway.into()),
            peer,
        }
    }

    /// True for direct local invocations (no authenticated peer-tunnel
    /// hop). Anonymous public HTTP callers are also local: this node
    /// saw no verifiable forwarding metadata.
    pub fn is_local(&self) -> bool {
        self.forwarded_by.is_none()
    }

    /// Route name of the gateway that forwarded this request over its
    /// authenticated tunnel leg, if any.
    pub fn forwarded_by(&self) -> Option<&str> {
        self.forwarded_by.as_deref()
    }

    /// Admitted caller identity; `None` means the caller is anonymous
    /// or the invocation is local.
    pub fn peer(&self) -> Option<&AdmittedPeer> {
        self.peer.as_ref()
    }
}

/// Opaque, stable string identifier for one in-flight transition
/// invocation. Used as `causationId` on emitted envelopes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandId(String);

impl CommandId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for CommandId {
    fn default() -> Self {
        Self::new()
    }
}

/// Request correlation lifted from HTTP headers. Values are stored
/// verbatim; validation belongs at the trace exporter, not here.
#[derive(Clone, Debug, Default)]
pub struct RequestCtx {
    traceparent: Option<String>,
    tracestate: Option<String>,
    request_id: Option<String>,
    provenance: CallerProvenance,
}

impl RequestCtx {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let pick = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        Self {
            traceparent: pick("traceparent"),
            tracestate: pick("tracestate"),
            request_id: pick("x-request-id"),
            provenance: CallerProvenance::default(),
        }
    }

    /// Attach caller provenance. Crate-private: only the http layer
    /// populates provenance, from admission state — never from
    /// client-supplied headers.
    pub(crate) fn with_provenance(mut self, provenance: CallerProvenance) -> Self {
        self.provenance = provenance;
        self
    }

    pub fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
    /// Caller provenance as established by the runtime (default:
    /// local/anonymous).
    pub fn provenance(&self) -> &CallerProvenance {
        &self.provenance
    }
}

/// Bus + registry pair attached to actor contexts so handlers can
/// publish envelopes through the same shared `StreamRegistry` the
/// rest of the runtime uses. Actors interact with this only through
/// `ActorCtx::publish` and `TransitionCtx::publish`.
#[derive(Clone)]
pub(crate) struct Publisher {
    bus: EventBus,
    registry: StreamRegistry,
}

impl Publisher {
    pub(crate) fn new(bus: EventBus, registry: StreamRegistry) -> Self {
        Self { bus, registry }
    }

    pub(crate) async fn publish(
        &self,
        plan: EnvelopePlan<'_>,
        ctx: EmissionContext<'_>,
    ) -> Result<(), PublishError> {
        publish_envelope(&self.bus, &self.registry, plan, ctx).await
    }
}

/// Addressing for an envelope. Bundled so the publish helpers stay
/// readable and clippy's `too_many_arguments` lint stays quiet.
pub(crate) struct EnvelopePlan<'a> {
    pub node_id: &'a NodeId,
    pub resource_id: &'a str,
    pub resource_kind: &'a str,
    pub stream: &'a str,
    pub payload_kind: &'a str,
    pub payload_version: u32,
    pub data: JsonValue,
}

/// Source of correlation/causation/trace context for an emission.
/// Lifecycle emissions (`on_start`/`on_stop`) leave all three `None`;
/// per-transition emissions carry the command id as `causationId` and
/// the inbound request headers as `correlationId`/`traceContext`.
#[derive(Default)]
pub(crate) struct EmissionContext<'a> {
    pub correlation: Option<&'a str>,
    pub causation: Option<&'a str>,
    pub trace: Option<TraceContext>,
}

/// Mint an envelope through `registry` and publish it on `bus`. Shared
/// by `ActorCtx::publish` and `TransitionCtx::publish` so lifecycle
/// and transition emissions build envelopes the same way.
pub(crate) async fn publish_envelope(
    bus: &EventBus,
    registry: &StreamRegistry,
    plan: EnvelopePlan<'_>,
    ctx: EmissionContext<'_>,
) -> Result<(), PublishError> {
    let stream_id = StreamId::for_resource(plan.node_id, plan.resource_id, plan.stream);
    let allocated = registry.allocate(&stream_id);
    let env = EventEnvelope {
        envelope_version: ENVELOPE_VERSION,
        event_id: allocated.event_id,
        node_id: plan.node_id.clone(),
        resource_id: plan.resource_id.to_string(),
        resource_kind: plan.resource_kind.to_string(),
        // Resource versioning is not yet wired in; emitted as 1 for now.
        resource_version: 1,
        stream_id,
        stream: plan.stream.to_string(),
        sequence: allocated.sequence,
        timestamp: time::OffsetDateTime::now_utc(),
        payload_kind: plan.payload_kind.to_string(),
        payload_version: plan.payload_version,
        payload_schema: None,
        correlation_id: ctx.correlation.map(|s| CorrelationId(s.to_string())),
        causation_id: ctx.causation.map(|s| CausationId(s.to_string())),
        trace_context: ctx.trace,
        data: plan.data,
    };
    bus.publish(env).await.map(|_| ())
}
