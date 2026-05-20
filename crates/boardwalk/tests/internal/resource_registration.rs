//! Runtime resource registration through actor factories.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use futures::future::BoxFuture;
use serde_json::Value as Json;

use crate::Boardwalk;
use crate::runtime::{
    Actor, Resource, ResourceCtx, ResourceError, ResourceSnapshot, ResourceSpec,
    TransitionAffordance, TransitionCtx, TransitionError, TransitionInput, TransitionOutcome,
    TransitionSpec,
};

#[derive(Default)]
struct Led {
    name: Option<String>,
    on: bool,
}

impl Led {
    fn new(name: Option<String>) -> Self {
        Self { name, on: false }
    }

    fn snapshot(&self, id: &str, node: &str) -> ResourceSnapshot {
        ResourceSnapshot {
            id: id.into(),
            kind: "led".into(),
            name: self.name.clone(),
            state: Some(if self.on { "on" } else { "off" }.into()),
            node: node.into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: vec![
                TransitionAffordance {
                    spec: TransitionSpec {
                        name: "turn-on".into(),
                        allowed_states: vec!["off".into()],
                        ..Default::default()
                    },
                    available: !self.on,
                    unavailable_reason: None,
                },
                TransitionAffordance {
                    spec: TransitionSpec {
                        name: "turn-off".into(),
                        allowed_states: vec!["on".into()],
                        ..Default::default()
                    },
                    available: self.on,
                    unavailable_reason: None,
                },
            ],
            streams: Vec::new(),
            revision: None,
            metadata: serde_json::Map::new(),
        }
    }
}

impl Resource for Led {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: self.name.clone(),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: Vec::new(),
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> BoxFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async { Ok(self.snapshot("ignored", "ignored")) })
    }
}

impl Actor for Led {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            match name {
                "turn-on" if !self.on => {
                    self.on = true;
                    Ok(TransitionOutcome::Completed {
                        output: None,
                        snapshot: self.snapshot(ctx.resource_id().unwrap_or("ignored"), ctx.node()),
                    })
                }
                "turn-off" if self.on => {
                    self.on = false;
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

async fn serve(boardwalk: Boardwalk) -> SocketAddr {
    let built = boardwalk.build().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, built.router).await.unwrap();
    });
    addr
}

#[tokio::test]
async fn actor_factory_creates_resource_at_runtime() {
    let boardwalk = Boardwalk::new()
        .name("hub")
        .register_actor_factory("led", |registration| Ok(Led::new(registration.name)));
    let addr = serve(boardwalk).await;

    let resources: Json = reqwest::get(format!("http://{addr}/resources"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        resources
            .get("entities")
            .and_then(|e| e.as_array())
            .map(|a| a.is_empty())
            .unwrap_or(true)
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("kind=led&name=KitchenLED")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let dev: Json = resp.json().await.unwrap();
    let id = dev["properties"]["id"].as_str().unwrap().to_string();
    assert_eq!(dev["properties"]["kind"], "led");
    assert_eq!(dev["properties"]["name"], "KitchenLED");
    assert_eq!(dev["properties"]["node"], "hub");
    assert_eq!(dev["properties"]["state"], "off");

    let resources: Json = reqwest::get(format!("http://{addr}/resources"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entities = resources["entities"].as_array().unwrap();
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0]["properties"]["id"], id);
    assert_eq!(entities[0]["properties"]["name"], "KitchenLED");

    let resp = client
        .post(format!("http://{addr}/resources/{id}/transitions/turn-on"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["snapshot"]["id"], id);
    assert_eq!(body["snapshot"]["node"], "hub");
    assert_eq!(body["snapshot"]["state"], "on");
}

/// Pins the runtime registration form: `POST /resources` consumes the
/// `kind`, `id`, and `name` form fields and returns 201 Created with a
/// Siren resource.
#[tokio::test]
async fn actor_factory_registration_posts_kind_id_name_to_resources_route() {
    let boardwalk = Boardwalk::new()
        .name("hub")
        .register_actor_factory("led", |registration| Ok(Led::new(registration.name)));
    let addr = serve(boardwalk).await;

    let supplied_id = uuid::Uuid::new_v4();
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("kind=led&id={supplied_id}&name=PantryLED",))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["properties"]["id"], supplied_id.to_string());
    assert_eq!(body["properties"]["kind"], "led");
    assert_eq!(body["properties"]["name"], "PantryLED");
}

#[tokio::test]
async fn missing_kind_returns_400() {
    let boardwalk = Boardwalk::new()
        .name("hub")
        .register_actor_factory("led", |registration| Ok(Led::new(registration.name)));
    let addr = serve(boardwalk).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("name=Foo")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn unknown_kind_returns_400() {
    let boardwalk = Boardwalk::new()
        .name("hub")
        .register_actor_factory("led", |registration| Ok(Led::new(registration.name)));
    let addr = serve(boardwalk).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("kind=motion")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn json_registration_returns_415() {
    let boardwalk = Boardwalk::new()
        .name("hub")
        .register_actor_factory("led", |registration| Ok(Led::new(registration.name)));
    let addr = serve(boardwalk).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/json")
        .body(r#"{"kind":"led"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 415);
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["error"], "unsupported-media-type");
    assert_eq!(body["field"], "content-type");
}

#[tokio::test]
async fn no_actor_factories_returns_501() {
    let addr = serve(Boardwalk::new().name("hub")).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("kind=led")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 501);
}
