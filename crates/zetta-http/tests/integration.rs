//! End-to-end integration test against the real router + a TCP listener.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::Value as Json;
use tokio_tungstenite::tungstenite::Message;
use zetta_core::{Device, DeviceConfig, DeviceError, TransitionInput};
use zetta_http::{router, Core, CoreBuilder};

#[derive(Default)]
struct Led {
    on: bool,
}

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .name("LED")
            .state(if self.on { "on" } else { "off" })
            .when("off", &["turn-on"])
            .when("on", &["turn-off"])
            .monitor("state");
    }
    fn state(&self) -> &str { if self.on { "on" } else { "off" } }
    fn transition<'a>(
        &'a mut self,
        name: &'a str,
        _input: TransitionInput,
    ) -> futures::future::BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async move {
            match name {
                "turn-on" => { self.on = true; Ok(()) }
                "turn-off" => { self.on = false; Ok(()) }
                other => Err(DeviceError::Invalid(format!("unknown {other}"))),
            }
        })
    }
}

async fn boot() -> (SocketAddr, Arc<Core>, tokio::task::JoinHandle<()>) {
    let mut b = CoreBuilder::new("hub");
    b.add_device(Led::default());
    let core = b.build();
    let app = router(core.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, core, handle)
}

#[tokio::test]
async fn root_returns_siren() {
    let (addr, _core, _h) = boot().await;
    let body = reqwest::get(format!("http://{addr}/")).await.unwrap().text().await.unwrap();
    let v: Json = serde_json::from_str(&body).unwrap();
    assert_eq!(v["class"], serde_json::json!(["root"]));
}

#[tokio::test]
async fn list_get_transition_flow() {
    let (addr, _core, _h) = boot().await;

    let server: Json = reqwest::get(format!("http://{addr}/servers/hub"))
        .await.unwrap().json().await.unwrap();
    let id = server["entities"][0]["properties"]["id"].as_str().unwrap().to_string();

    let dev: Json = reqwest::get(format!("http://{addr}/servers/hub/devices/{id}"))
        .await.unwrap().json().await.unwrap();
    assert_eq!(dev["properties"]["state"], "off");
    assert_eq!(dev["actions"][0]["name"], "turn-on");

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let dev: Json = resp.json().await.unwrap();
    assert_eq!(dev["properties"]["state"], "on");
    assert_eq!(dev["actions"][0]["name"], "turn-off");

    // Not allowed in current state.
    let resp = client
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send().await.unwrap();
    assert_eq!(resp.status(), 409);
}

#[tokio::test]
async fn ws_subscribe_receives_state_event() {
    let (addr, _core, _h) = boot().await;

    let server: Json = reqwest::get(format!("http://{addr}/servers/hub"))
        .await.unwrap().json().await.unwrap();
    let id = server["entities"][0]["properties"]["id"].as_str().unwrap().to_string();

    let url = format!("ws://{addr}/events");
    let (mut ws, _resp) = tokio_tungstenite::connect_async(url).await.unwrap();

    let topic = format!("hub/led/{id}/state");
    let sub = serde_json::json!({"type": "subscribe", "topic": topic});
    ws.send(Message::Text(sub.to_string().into())).await.unwrap();

    // Expect subscribe-ack.
    let ack = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await.unwrap().unwrap().unwrap();
    let ack: Json = match ack {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!("expected text"),
    };
    assert_eq!(ack["type"], "subscribe-ack");
    let sub_id = ack["subscriptionId"].as_u64().unwrap();
    assert_eq!(ack["topic"], topic);

    // Trigger a state change.
    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send().await.unwrap();

    // Read event.
    let evt = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await.unwrap().unwrap().unwrap();
    let evt: Json = match evt {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!("expected text"),
    };
    assert_eq!(evt["type"], "event");
    assert_eq!(evt["topic"], topic);
    assert_eq!(evt["data"], "on");
    assert_eq!(evt["subscriptionId"], sub_id);

    // Unsubscribe.
    let unsub = serde_json::json!({"type": "unsubscribe", "subscriptionId": sub_id});
    ws.send(Message::Text(unsub.to_string().into())).await.unwrap();
    let ack = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await.unwrap().unwrap().unwrap();
    let ack: Json = match ack {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!("expected text"),
    };
    assert_eq!(ack["type"], "unsubscribe-ack");
}

#[tokio::test]
async fn query_string_filters_devices() {
    let (addr, _core, _h) = boot().await;
    let url = format!("http://{addr}/servers/hub?ql={}", urlencoding::encode("where type = \"led\""));
    let resp: Json = reqwest::get(&url).await.unwrap().json().await.unwrap();
    assert_eq!(resp["class"], serde_json::json!(["server", "search-results"]));
    assert!(!resp["entities"].as_array().unwrap().is_empty());

    let url = format!("http://{addr}/servers/hub?ql={}", urlencoding::encode("where type = \"motion\""));
    let resp: Json = reqwest::get(&url).await.unwrap().json().await.unwrap();
    // Empty entities array is omitted from JSON when no matches; either absence or empty array is OK.
    let entities = resp.get("entities").and_then(|v| v.as_array());
    assert!(entities.map(|a| a.is_empty()).unwrap_or(true));
}
