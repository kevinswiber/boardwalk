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
        }
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
