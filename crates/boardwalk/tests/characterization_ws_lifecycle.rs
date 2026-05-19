//! Characterization tests for the multiplex events WebSocket protocol.
//!
//! Locks down the subscribe → ack → event → unsubscribe → ack cycle,
//! plus ping/pong, error framing for invalid topics, and the
//! `limit` auto-removal contract.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use boardwalk::http::{Core, CoreBuilder, router};
use boardwalk::{Device, DeviceConfig, DeviceError, TransitionInput};
use futures::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

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
    fn state(&self) -> &str {
        if self.on { "on" } else { "off" }
    }
    fn transition<'a>(
        &'a mut self,
        name: &'a str,
        _input: TransitionInput,
    ) -> futures::future::BoxFuture<'a, Result<(), DeviceError>> {
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

async fn device_id(addr: SocketAddr) -> String {
    let server: Json = reqwest::get(format!("http://{addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    server["entities"][0]["properties"]["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn open_ws(addr: SocketAddr) -> Ws {
    let url = format!("ws://{addr}/events");
    let (ws, _resp) = tokio_tungstenite::connect_async(url).await.unwrap();
    ws
}

async fn send_json(ws: &mut Ws, value: Json) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

/// Reads one text frame as JSON. Fails the test if no message arrives
/// within the supplied timeout.
async fn recv_json(ws: &mut Ws, timeout: Duration) -> Json {
    let msg = tokio::time::timeout(timeout, ws.next())
        .await
        .expect("expected a ws message before timeout")
        .expect("ws stream produced None")
        .expect("ws stream produced an error");
    match msg {
        Message::Text(t) => serde_json::from_str(&t).expect("ws message is not valid JSON"),
        other => panic!("expected text frame, got {other:?}"),
    }
}

async fn poll_json(ws: &mut Ws, timeout: Duration) -> Option<Json> {
    let msg = tokio::time::timeout(timeout, ws.next()).await.ok()??.ok()?;
    match msg {
        Message::Text(t) => Some(serde_json::from_str(&t).unwrap()),
        _ => None,
    }
}

async fn post_action(addr: SocketAddr, id: &str, action: &str) -> reqwest::StatusCode {
    let client = reqwest::Client::new();
    client
        .post(format!("http://{addr}/resources/{id}/transitions/{action}"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn subscribe_ack_includes_subscription_id_and_topic() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    let topic = format!("hub/led/{id}/state");
    send_json(&mut ws, json!({"type": "subscribe", "topic": topic})).await;

    let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(ack["type"], "subscribe-ack");
    assert_eq!(ack["topic"], topic);
    assert!(
        ack["subscriptionId"].as_u64().is_some(),
        "subscriptionId should be a non-negative integer, got {:?}",
        ack["subscriptionId"]
    );
}

#[tokio::test]
async fn state_event_after_transition_uses_published_topic_and_data() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    let topic = format!("hub/led/{id}/state");
    send_json(&mut ws, json!({"type": "subscribe", "topic": topic})).await;
    let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
    let sub_id = ack["subscriptionId"].as_u64().unwrap();

    assert_eq!(post_action(addr, &id, "turn-on").await, 200);

    let evt = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(evt["type"], "event");
    assert_eq!(evt["topic"], topic);
    assert_eq!(evt["data"], "on");
    assert_eq!(evt["subscriptionId"], sub_id);
}

#[tokio::test]
async fn unsubscribe_acks_and_no_more_events_arrive() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    let topic = format!("hub/led/{id}/state");
    send_json(&mut ws, json!({"type": "subscribe", "topic": topic})).await;
    let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
    let sub_id = ack["subscriptionId"].as_u64().unwrap();

    // Consume one event so we know the subscription is wired up.
    assert_eq!(post_action(addr, &id, "turn-on").await, 200);
    let evt = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(evt["type"], "event");

    // Unsubscribe and consume the ack.
    send_json(
        &mut ws,
        json!({"type": "unsubscribe", "subscriptionId": sub_id}),
    )
    .await;
    let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(ack["type"], "unsubscribe-ack");
    assert_eq!(ack["subscriptionId"], sub_id);

    // Trigger another transition; no further frames should arrive.
    assert_eq!(post_action(addr, &id, "turn-off").await, 200);
    let leftover = poll_json(&mut ws, Duration::from_millis(300)).await;
    assert!(
        leftover.is_none(),
        "expected no further events after unsubscribe, got {leftover:?}"
    );
}

#[tokio::test]
async fn ping_returns_pong_with_same_payload() {
    let (addr, _core, _h) = boot().await;
    let mut ws = open_ws(addr).await;

    send_json(&mut ws, json!({"type": "ping", "data": "abc"})).await;
    let pong = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(pong["type"], "pong");
    assert_eq!(pong["data"], "abc");
}

#[tokio::test]
async fn subscribe_with_invalid_topic_returns_error_message() {
    let (addr, _core, _h) = boot().await;
    let mut ws = open_ws(addr).await;

    send_json(&mut ws, json!({"type": "subscribe", "topic": ""})).await;
    let err = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(err["type"], "error");
    assert!(
        err.get("code").and_then(|v| v.as_u64()).is_some(),
        "error frames carry a numeric `code`, got {err:?}"
    );
}

#[tokio::test]
async fn subscription_limit_auto_removes_subscription() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    let topic = format!("hub/led/{id}/state");
    send_json(
        &mut ws,
        json!({"type": "subscribe", "topic": topic, "limit": 1}),
    )
    .await;
    let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(ack["type"], "subscribe-ack");

    // Two state changes — only the first should reach the client.
    assert_eq!(post_action(addr, &id, "turn-on").await, 200);
    assert_eq!(post_action(addr, &id, "turn-off").await, 200);

    let first = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(first["type"], "event");
    assert_eq!(first["data"], "on");

    let second = poll_json(&mut ws, Duration::from_millis(400)).await;
    assert!(
        second.is_none(),
        "expected limit:1 to auto-remove the subscription, got second frame {second:?}"
    );
}
