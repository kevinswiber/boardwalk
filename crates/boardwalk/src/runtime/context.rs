//! Request and command context carried through transitions and
//! actor lifecycle.
//!
//! `RequestCtx` captures the W3C trace context (`traceparent`,
//! `tracestate`) and the `x-request-id` header so downstream code can
//! attach them to envelopes without re-parsing HTTP state.
//! `TransitionCtx` mints a fresh `CommandId` per call to use as
//! `causationId` on emitted envelopes.

use axum::http::HeaderMap;
use uuid::Uuid;

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
