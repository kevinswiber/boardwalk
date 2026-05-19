//! Hubless resource registration via Boardwalk::register_factory.

use std::collections::HashMap;
use std::net::SocketAddr;

use boardwalk::{Boardwalk, Device, DeviceConfig, DeviceError, TransitionInput};
use futures::future::BoxFuture;
use serde_json::Value as Json;

#[derive(Default)]
struct Led {
    name: Option<String>,
    on: bool,
}

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .state(self.state())
            .when("off", &["turn-on"])
            .when("on", &["turn-off"]);
        if let Some(n) = &self.name {
            cfg.name(n.clone());
        }
    }
    fn state(&self) -> &str {
        if self.on { "on" } else { "off" }
    }
    fn transition<'a>(
        &'a mut self,
        name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async move {
            match name {
                "turn-on" => {
                    self.on = true;
                    Ok(())
                }
                "turn-off" => {
                    self.on = false;
                    Ok(())
                }
                _ => Err(DeviceError::Invalid("?".into())),
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
async fn register_factory_creates_device_at_runtime() {
    let boardwalk =
        Boardwalk::new()
            .name("hub")
            .register_factory("led", |args: HashMap<String, String>| {
                let _ = args;
                Ok(Box::new(Led::default()) as Box<dyn Device>)
            });
    let addr = serve(boardwalk).await;

    // Before any POST, no resources.
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

    // POST a registration.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("type=led&name=KitchenLED")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let dev: Json = resp.json().await.unwrap();
    assert_eq!(dev["properties"]["type"], "led");
    assert_eq!(dev["properties"]["name"], "KitchenLED");
    assert_eq!(dev["properties"]["state"], "off");

    // The resource now appears in the resource listing.
    let resources: Json = reqwest::get(format!("http://{addr}/resources"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entities = resources["entities"].as_array().unwrap();
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0]["properties"]["name"], "KitchenLED");
}

/// Pins the hubless registration form: `POST /resources` consumes the
/// `type`, `id`, and `name` form fields and returns 201 Created with a
/// Siren resource.
#[tokio::test]
async fn factory_registration_posts_type_id_name_to_resources_route() {
    let boardwalk = Boardwalk::new()
        .name("hub")
        .register_factory("led", |_args: HashMap<String, String>| {
            Ok(Box::new(Led::default()) as Box<dyn Device>)
        });
    let addr = serve(boardwalk).await;

    let supplied_id = uuid::Uuid::new_v4();
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("type=led&id={supplied_id}&name=PantryLED",))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["properties"]["id"], supplied_id.to_string());
    assert_eq!(body["properties"]["type"], "led");
    assert_eq!(body["properties"]["name"], "PantryLED");
}

#[tokio::test]
async fn missing_type_returns_400() {
    let boardwalk = Boardwalk::new()
        .name("hub")
        .register_factory("led", |_| Ok(Box::new(Led::default()) as Box<dyn Device>));
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
async fn unknown_type_returns_400() {
    let boardwalk = Boardwalk::new()
        .name("hub")
        .register_factory("led", |_| Ok(Box::new(Led::default()) as Box<dyn Device>));
    let addr = serve(boardwalk).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("type=motion")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn json_registration_returns_415() {
    let boardwalk = Boardwalk::new()
        .name("hub")
        .register_factory("led", |_| Ok(Box::new(Led::default()) as Box<dyn Device>));
    let addr = serve(boardwalk).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/json")
        .body(r#"{"type":"led"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 415);
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["error"], "unsupported-media-type");
    assert_eq!(body["field"], "content-type");
}

#[tokio::test]
async fn no_factories_returns_501() {
    let addr = serve(Boardwalk::new().name("hub")).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("type=led")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 501);
}
