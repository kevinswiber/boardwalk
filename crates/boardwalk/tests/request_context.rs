//! Pins for the request/transition/actor context surface.
//!
//! Transitions need a stable command id, request correlation
//! (`traceparent`, `tracestate`, `x-request-id`), and a way to register
//! actor-created resources without poking into HTTP state. The actor
//! lifecycle context exposes the node/resource identity and labels.

use std::collections::BTreeMap;

use axum::http::{HeaderMap, HeaderValue};
use boardwalk::core::{ResourceSpec, TransitionInput, TransitionOutcome};
use boardwalk::http::ResourceSnapshot;
use boardwalk::runtime::{
    Actor, ActorCtx, DynFuture, RequestCtx, Resource, ResourceCtx, ResourceError, TransitionCtx,
    TransitionError,
};

#[test]
fn request_ctx_extracts_traceparent_tracestate_and_x_request_id() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "traceparent",
        HeaderValue::from_static("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
    );
    headers.insert(
        "tracestate",
        HeaderValue::from_static("rojo=00f067aa0ba902b7"),
    );
    headers.insert("x-request-id", HeaderValue::from_static("req-123"));

    let ctx = RequestCtx::from_headers(&headers);
    assert_eq!(
        ctx.traceparent(),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
    assert_eq!(ctx.tracestate(), Some("rojo=00f067aa0ba902b7"));
    assert_eq!(ctx.request_id(), Some("req-123"));
}

#[test]
fn request_ctx_handles_missing_headers() {
    let headers = HeaderMap::new();
    let ctx = RequestCtx::from_headers(&headers);
    assert!(ctx.traceparent().is_none());
    assert!(ctx.tracestate().is_none());
    assert!(ctx.request_id().is_none());
}

#[test]
fn transition_ctx_allocates_command_id_and_sets_causation() {
    let req = RequestCtx::default();
    let a = TransitionCtx::new(req.clone(), "hub");
    let b = TransitionCtx::new(req, "hub");
    let a_id = a.command_id().clone();
    let b_id = b.command_id().clone();
    assert_ne!(a_id, b_id, "each context mints a fresh command id");

    // The command id has a stable string form so it can be carried on
    // an envelope's `causationId`.
    let s = a_id.as_str().to_owned();
    assert!(!s.is_empty());
}

#[test]
fn actor_ctx_contains_node_and_resource_identity() {
    let mut labels = BTreeMap::new();
    labels.insert("owner".into(), "platform".into());
    let ctx = ActorCtx::new("hub", "job-1", "job", labels.clone());
    assert_eq!(ctx.node(), "hub");
    assert_eq!(ctx.resource_id(), "job-1");
    assert_eq!(ctx.resource_kind(), "job");
    assert_eq!(ctx.labels(), &labels);
}

struct StubActor;

impl Resource for StubActor {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "job".into(),
            name: None,
            labels: BTreeMap::new(),
            property_schema: None,
            streams: vec![],
        }
    }
    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(
            async move { Err::<ResourceSnapshot, _>(ResourceError::Unavailable("stub".into())) },
        )
    }
}

impl Actor for StubActor {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(
            async move { Err::<TransitionOutcome, _>(TransitionError::NotAllowed("stub".into())) },
        )
    }
}

#[test]
fn transition_ctx_exposes_resource_registration_service_signature() {
    let ctx = TransitionCtx::new(RequestCtx::default(), "hub");
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let result: Result<String, TransitionError> = ctx.register_actor(StubActor).await;
        // A context built without a `Node` returns Internal so the
        // call shape is pinned but no resource is actually registered.
        match result {
            Err(TransitionError::Internal(_)) => {}
            Ok(_) => panic!("test stub should not return Ok yet"),
            Err(other) => panic!("expected Internal stub error, got {other:?}"),
        }
    });
}
