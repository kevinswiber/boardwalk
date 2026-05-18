//! Characterization tests pinning the WS event wire shape.
//!
//! Locks the top-level key set (legacy quintet plus the optional
//! envelope mirror), the epoch-milliseconds integer typing of
//! `timestamp`, and that the per-frame `subscriptionId` echoes the
//! one minted in the `subscribe-ack`.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

async fn post_action(addr: SocketAddr, id: &str, action: &str) -> reqwest::StatusCode {
    let client = reqwest::Client::new();
    client
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("action={action}"))
        .send()
        .await
        .unwrap()
        .status()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn subscribe_state_and_fire(addr: SocketAddr, ws: &mut Ws, id: &str) -> (u64, Json) {
    let topic = format!("hub/led/{id}/state");
    send_json(ws, json!({"type": "subscribe", "topic": topic})).await;
    let ack = recv_json(ws, Duration::from_secs(2)).await;
    let sub_id = ack["subscriptionId"]
        .as_u64()
        .expect("subscribe-ack should include numeric subscriptionId");

    assert_eq!(post_action(addr, id, "turn-on").await, 200);
    let evt = recv_json(ws, Duration::from_secs(2)).await;
    assert_eq!(evt["type"], "event", "expected `event` frame, got {evt:?}");
    (sub_id, evt)
}

#[tokio::test]
async fn event_wire_keys_include_legacy_and_envelope_fields() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    let (_sub_id, evt) = subscribe_state_and_fire(addr, &mut ws, &id).await;

    let obj = evt.as_object().expect("event frame is a JSON object");
    let actual: BTreeSet<&str> = obj.keys().map(|s| s.as_str()).collect();
    // Legacy quintet plus the optional envelope fields the local-bus
    // forwarder always populates.
    let expected: BTreeSet<&str> = [
        "type",
        "topic",
        "subscriptionId",
        "timestamp",
        "data",
        "eventId",
        "streamId",
        "sequence",
        "nodeId",
        "resourceId",
        "resourceKind",
        "payloadKind",
        "payloadVersion",
        "envelopeVersion",
        "isoTimestamp",
    ]
    .into_iter()
    .collect();
    assert_eq!(
        actual, expected,
        "WS event wire key set must include both legacy and envelope fields"
    );
}

#[tokio::test]
async fn event_wire_timestamp_is_epoch_milliseconds_i64() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    let before = now_ms();
    let (_sub_id, evt) = subscribe_state_and_fire(addr, &mut ws, &id).await;
    let after = now_ms();

    let ts = evt["timestamp"].as_i64().unwrap_or_else(|| {
        panic!(
            "timestamp must be an i64 epoch-ms integer; got {:?}",
            evt["timestamp"]
        )
    });
    let low = before - 5_000;
    let high = after + 5_000;
    assert!(
        ts >= low && ts <= high,
        "timestamp {ts} must lie within ±5s of now_ms (before={before}, after={after})"
    );
}

#[tokio::test]
async fn event_wire_envelope_fields_match_runtime_envelope() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    let (_sub_id, evt) = subscribe_state_and_fire(addr, &mut ws, &id).await;

    // Boot has one LED + one transition (turn-on). That's the first
    // event on the LED's `state` stream, so sequence == 1.
    assert_eq!(evt["sequence"], 1);
    assert_eq!(evt["envelopeVersion"], 1);
    assert_eq!(evt["payloadVersion"], 1);
    assert_eq!(evt["payloadKind"], "resource.state.changed");
    assert_eq!(evt["nodeId"], "hub");
    assert_eq!(evt["resourceKind"], "led");
    assert_eq!(evt["resourceId"], id);

    let stream_id = evt["streamId"]
        .as_str()
        .expect("streamId is a string")
        .to_string();
    let expected_stream_id = format!("bw://hub/resources/{id}/streams/state");
    assert_eq!(stream_id, expected_stream_id);

    let event_id = evt["eventId"].as_str().expect("eventId is a string");
    assert!(!event_id.is_empty(), "eventId must be a non-empty string");

    let iso = evt["isoTimestamp"]
        .as_str()
        .expect("isoTimestamp is a string");
    assert!(
        iso.contains('T'),
        "isoTimestamp should be RFC3339; got {iso}"
    );
}

#[tokio::test]
async fn event_wire_subscription_id_matches_subscribe_ack() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    let (sub_id, evt) = subscribe_state_and_fire(addr, &mut ws, &id).await;
    let frame_sub = evt["subscriptionId"]
        .as_u64()
        .expect("event frame subscriptionId must be a u64");
    assert_eq!(
        frame_sub, sub_id,
        "event frame subscriptionId must match the one minted in subscribe-ack"
    );
}
