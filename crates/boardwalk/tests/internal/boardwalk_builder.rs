//! Boardwalk builder coverage for the actor-native reusable HTTP path.

use std::collections::BTreeMap;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request as HttpRequest, StatusCode, header};
use axum::response::Response;
use tower::ServiceExt;

use crate::Boardwalk;
use crate::runtime::{
    Actor, DynFuture, Resource, ResourceCtx, ResourceError, ResourceSnapshot, ResourceSpec,
    TransitionAffordance, TransitionCtx, TransitionError, TransitionInput, TransitionOutcome,
    TransitionSpec,
};

struct BuilderLed {
    name: &'static str,
    on: bool,
}

impl BuilderLed {
    fn named(name: &'static str) -> Self {
        Self { name, on: false }
    }

    fn snapshot(&self, id: &str, node: &str) -> ResourceSnapshot {
        ResourceSnapshot {
            id: id.into(),
            kind: "led".into(),
            name: Some(self.name.into()),
            state: Some(if self.on { "on" } else { "off" }.into()),
            node: node.into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: vec![TransitionAffordance {
                spec: TransitionSpec {
                    name: "turn-on".into(),
                    allowed_states: vec!["off".into()],
                    ..Default::default()
                },
                available: !self.on,
                unavailable_reason: None,
            }],
            streams: Vec::new(),
            revision: None,
            metadata: serde_json::Map::new(),
        }
    }
}

impl Resource for BuilderLed {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some(self.name.into()),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: Vec::new(),
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async { Ok(self.snapshot("ignored", "ignored")) })
    }
}

impl Actor for BuilderLed {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            match name {
                "turn-on" if !self.on => {
                    self.on = true;
                    Ok(TransitionOutcome::Completed {
                        output: None,
                        snapshot: self.snapshot(ctx.resource_id().unwrap_or("ignored"), ctx.node()),
                    })
                }
                other => Err(TransitionError::NotAllowed(format!(
                    "transition `{other}` not available"
                ))),
            }
        })
    }
}

#[tokio::test]
async fn peer_admission_config_boardwalk_builder_accepts_local_node_identity() {
    let built = Boardwalk::new()
        .name("cloud")
        .node_id("node-cloud-1")
        .build()
        .unwrap();

    assert_eq!(built.node.id(), "node-cloud-1");
}

#[tokio::test]
async fn peer_admission_config_local_node_identity_does_not_replace_route_name() {
    let built = Boardwalk::new()
        .name("cloud")
        .node_id("node-cloud-1")
        .build()
        .unwrap();

    assert_eq!(built.core.name, "cloud");
}

#[tokio::test]
async fn peer_admission_config_boardwalk_builder_stores_accepted_peer_token() {
    let built = Boardwalk::new()
        .name("cloud")
        .accept_peer_token("hub", "kid-1", "secret")
        .build()
        .unwrap();

    assert_eq!(built.peer_admission.len(), 1);
    assert_eq!(built.peer_admission[0].allowed_route_name.as_str(), "hub");
    assert_eq!(built.peer_admission[0].token_id, "kid-1");
}

#[tokio::test]
async fn boardwalk_build_serves_registered_actor_resources_and_transitions() {
    let built = Boardwalk::new()
        .name("hub")
        .use_actor_with_id("front-panel", BuilderLed::named("Builder LED"))
        .use_actor(BuilderLed::named("Aux LED"))
        .build()
        .expect("boardwalk builds from actor");
    let app = built.router.clone();

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

    let json = response_json(response).await;
    let entities = json["entities"].as_array().expect("resource entities");
    assert_eq!(entities.len(), 2);
    let builder = entities
        .iter()
        .find(|entity| entity["properties"]["name"] == "Builder LED")
        .expect("builder led entity");
    let id = builder["properties"]["id"]
        .as_str()
        .expect("resource id")
        .to_string();
    assert_eq!(id, "front-panel");
    assert_eq!(builder["properties"]["kind"], "led");
    assert_eq!(builder["properties"]["node"], "hub");

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri(format!("/resources/{id}"))
                .body(Body::empty())
                .expect("resource request builds"),
        )
        .await
        .expect("resource request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["properties"]["id"], id);
    assert_eq!(json["properties"]["node"], "hub");
    assert_eq!(json["properties"]["state"], "off");

    let response = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method(Method::POST)
                .uri(format!("/resources/{id}/transitions/turn-on"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .expect("transition request builds"),
        )
        .await
        .expect("transition request completes");
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["snapshot"]["id"], id);
    assert_eq!(json["snapshot"]["node"], "hub");
    assert_eq!(json["snapshot"]["state"], "on");

    built.node.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn boardwalk_listen_on_serves_supplied_listener() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let addr = listener.local_addr().expect("listener has local addr");
    let server = tokio::spawn(async move {
        Boardwalk::new()
            .name("hub")
            .use_actor_with_id("front-panel", BuilderLed::named("Builder LED"))
            .listen_on(listener)
            .await
    });

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/resources/front-panel");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut last_error = None;
    let response = loop {
        match client.get(&url).send().await {
            Ok(response) => break response,
            Err(err) => {
                last_error = Some(err);
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "listen_on did not accept requests on supplied listener: {last_error:?}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    };

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "listen_on should serve the supplied listener; last connect error: {last_error:?}"
    );
    server.abort();
}

async fn response_json(response: Response) -> serde_json::Value {
    let bytes = response_bytes(response).await;
    serde_json::from_slice(&bytes).expect("body is json")
}

async fn response_bytes(response: Response) -> bytes::Bytes {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body reads")
}

#[tokio::test]
async fn first_persisted_startup_generates_a_stable_node_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.redb");

    let first = Boardwalk::new().name("hub").persist(&path).build().unwrap();
    let first_id = first.node.id().to_string();
    assert_ne!(first_id, "hub", "generated id must not be the display name");
    assert!(
        uuid::Uuid::parse_str(&first_id).is_ok(),
        "generated id is a UUID: {first_id}"
    );
    drop(first);

    // Rename the node: identity must be sticky.
    let second = Boardwalk::new()
        .name("renamed-hub")
        .persist(&path)
        .build()
        .unwrap();
    assert_eq!(second.node.id().to_string(), first_id);
}

#[tokio::test]
async fn explicit_node_id_and_existing_records_take_precedence_over_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.redb");
    let built = Boardwalk::new()
        .name("hub")
        .node_id("node-hub-7f3a")
        .persist(&path)
        .build()
        .unwrap();
    assert_eq!(built.node.id().to_string(), "node-hub-7f3a");
    drop(built);
    // A pre-existing persisted record must win over generation on the
    // next boot.
    let again = Boardwalk::new().name("hub").persist(&path).build().unwrap();
    assert_eq!(again.node.id().to_string(), "node-hub-7f3a");
}

#[test]
fn non_persisted_nodes_keep_the_name_default() {
    let built = Boardwalk::new().name("hub").build().unwrap();
    assert_eq!(built.node.id().to_string(), "hub");
}
