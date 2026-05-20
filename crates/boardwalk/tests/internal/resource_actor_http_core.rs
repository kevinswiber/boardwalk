//! Router-level coverage for serving local resource routes from the actor runtime.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use serde_json::Value as JsonValue;
use tower::ServiceExt;

use crate::http::{Core, router};
use crate::runtime::{
    Actor, DynFuture, NodeBuilder, Resource, ResourceCtx, ResourceError, ResourceSnapshot,
    ResourceSpec, TransitionAffordance, TransitionCtx, TransitionError, TransitionInput,
    TransitionOutcome, TransitionSpec,
};

#[derive(Default)]
struct RuntimeLed {
    on: bool,
}

impl RuntimeLed {
    fn snapshot(&self) -> ResourceSnapshot {
        let state = if self.on { "on" } else { "off" };
        ResourceSnapshot {
            id: "placeholder".into(),
            kind: "led".into(),
            name: Some("Runtime LED".into()),
            state: Some(state.into()),
            node: "placeholder".into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: vec![
                TransitionAffordance {
                    spec: TransitionSpec {
                        name: "turn-on".into(),
                        ..Default::default()
                    },
                    available: !self.on,
                    unavailable_reason: self.on.then_some("already on".into()),
                },
                TransitionAffordance {
                    spec: TransitionSpec {
                        name: "turn-off".into(),
                        ..Default::default()
                    },
                    available: self.on,
                    unavailable_reason: (!self.on).then_some("already off".into()),
                },
            ],
            streams: Vec::new(),
            revision: None,
            metadata: serde_json::Map::new(),
        }
    }
}

impl Resource for RuntimeLed {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some("Runtime LED".into()),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: Vec::new(),
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move { Ok(self.snapshot()) })
    }
}

impl Actor for RuntimeLed {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            match name {
                "turn-on" if !self.on => {
                    self.on = true;
                    Ok(TransitionOutcome::Completed {
                        output: None,
                        snapshot: self.snapshot(),
                    })
                }
                "turn-off" if self.on => {
                    self.on = false;
                    Ok(TransitionOutcome::Completed {
                        output: None,
                        snapshot: self.snapshot(),
                    })
                }
                other => Err(TransitionError::NotAllowed(format!(
                    "transition `{other}` is not available"
                ))),
            }
        })
    }
}

#[derive(Default)]
struct RefreshUnavailableLed {
    led: RuntimeLed,
    unavailable: bool,
}

impl Resource for RefreshUnavailableLed {
    fn spec(&self) -> ResourceSpec {
        self.led.spec()
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move {
            if self.unavailable {
                return Err(ResourceError::Unavailable("runtime led unavailable".into()));
            }
            Ok(self.led.snapshot())
        })
    }
}

impl Actor for RefreshUnavailableLed {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            let outcome = self.led.transition(ctx, name, input).await?;
            self.unavailable = true;
            Ok(outcome)
        })
    }
}

struct UnavailableResource;

impl Resource for UnavailableResource {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "unavailable".into(),
            name: None,
            labels: BTreeMap::new(),
            property_schema: None,
            streams: Vec::new(),
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async { Err(ResourceError::Unavailable("resource unavailable".into())) })
    }
}

impl Actor for UnavailableResource {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async { Err(TransitionError::NotAllowed("unavailable".into())) })
    }
}

#[tokio::test]
async fn reusable_router_serves_actor_runtime_resources() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    node.register_with_id("runtime-led".into(), RuntimeLed::default())
        .await
        .expect("actor registers");
    let app = router(Core::from_node(node));

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/resources")
                .body(Body::empty())
                .expect("resources request builds"),
        )
        .await
        .expect("resources request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["entities"][0]["properties"]["id"], "runtime-led");
    assert_eq!(body["entities"][0]["properties"]["kind"], "led");

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/resources/runtime-led")
                .body(Body::empty())
                .expect("resource request builds"),
        )
        .await
        .expect("resource request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["properties"]["id"], "runtime-led");
    assert_eq!(body["properties"]["state"], "off");
    assert_eq!(body["actions"][0]["name"], "turn-on");

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/resources?ql=where%20kind%20%3D%20%22led%22")
                .body(Body::empty())
                .expect("query request builds"),
        )
        .await
        .expect("query request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["class"],
        serde_json::json!(["resources", "search-results"])
    );
    assert_eq!(body["entities"][0]["properties"]["id"], "runtime-led");

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/resources?ql=where%20kind%20%3D")
                .body(Body::empty())
                .expect("invalid query request builds"),
        )
        .await
        .expect("invalid query request completes");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["error"], "query-parse-error");

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/meta")
                .body(Body::empty())
                .expect("metadata request builds"),
        )
        .await
        .expect("metadata request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let transitions = body["entities"][0]["properties"]["transitions"]
        .as_array()
        .expect("metadata transitions");
    let names: Vec<&str> = transitions
        .iter()
        .map(|transition| transition["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["turn-on", "turn-off"]);

    let response = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/resources/runtime-led/transitions/turn-on")
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .expect("transition request builds"),
        )
        .await
        .expect("transition request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["snapshot"]["id"], "runtime-led");
    assert_eq!(body["snapshot"]["state"], "on");
}

#[tokio::test]
async fn actor_runtime_resource_unavailable_maps_to_503() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    node.register_with_id("offline".into(), UnavailableResource)
        .await
        .expect("actor registers");
    let app = router(Core::from_node(node));

    let response = app
        .oneshot(
            HttpRequest::builder()
                .uri("/resources/offline")
                .body(Body::empty())
                .expect("resource request builds"),
        )
        .await
        .expect("resource request completes");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response_json(response).await;
    assert_eq!(body["error"], "resource-unavailable");
}

#[tokio::test]
async fn unavailable_actor_remains_visible_in_list_query_and_meta() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    node.register_with_id("offline".into(), UnavailableResource)
        .await
        .expect("actor registers");
    let app = router(Core::from_node(node));

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/resources")
                .body(Body::empty())
                .expect("resources request builds"),
        )
        .await
        .expect("resources request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["entities"][0]["properties"]["id"], "offline");
    assert_eq!(body["entities"][0]["properties"]["kind"], "unavailable");
    assert!(body["entities"][0]["properties"]["state"].is_null());

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/resources?ql=where%20kind%20%3D%20%22unavailable%22")
                .body(Body::empty())
                .expect("query request builds"),
        )
        .await
        .expect("query request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["entities"][0]["properties"]["id"], "offline");

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/meta")
                .body(Body::empty())
                .expect("metadata request builds"),
        )
        .await
        .expect("metadata request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["entities"][0]["properties"]["kind"], "unavailable");
    assert_eq!(
        body["entities"][0]["properties"]["transitions"],
        serde_json::json!([])
    );

    let response = app
        .oneshot(
            HttpRequest::builder()
                .uri("/meta/unavailable")
                .body(Body::empty())
                .expect("metadata type request builds"),
        )
        .await
        .expect("metadata type request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["properties"]["kind"], "unavailable");
}

#[tokio::test]
async fn actor_runtime_transition_snapshot_unavailable_maps_to_503() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    node.register_with_id("flaky-led".into(), RefreshUnavailableLed::default())
        .await
        .expect("actor registers");
    let app = router(Core::from_node(node));

    let response = app
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/resources/flaky-led/transitions/turn-on")
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .expect("transition request builds"),
        )
        .await
        .expect("transition request completes");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response_json(response).await;
    assert_eq!(body["error"], "resource-unavailable");
}

async fn response_json(resp: axum::response::Response) -> JsonValue {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
