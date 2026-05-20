//! Actor-backed HTTP event stream regression tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio_tungstenite::tungstenite::Message;

use super::actor_led_fixture::ActorLed;
use crate::events::{ENVELOPE_VERSION, EventEnvelope, EventId, NodeId, StreamId};
use crate::http::{Core, router};
use crate::runtime::NodeBuilder;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn boot() -> (SocketAddr, Arc<Core>, tokio::task::JoinHandle<()>) {
    let node = Arc::new(
        NodeBuilder::new("hub")
            .register_with_id("actor-led", ActorLed::default())
            .expect("actor registers")
            .build(),
    );
    let core = Core::from_node(node);
    let app = router(core.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, core, handle)
}

async fn resource_id(addr: SocketAddr) -> String {
    let server: Json = reqwest::get(format!("http://{addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let resource = &server["entities"][0];
    assert_eq!(resource["properties"]["fixture"], "actor-led");
    resource["properties"]["id"].as_str().unwrap().to_string()
}

async fn open_ws(addr: SocketAddr) -> Ws {
    let (ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{addr}/events"))
        .await
        .unwrap();
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

fn led_state_envelope(id: &str, seq: u64, value: &str) -> EventEnvelope {
    let node = NodeId::new("hub");
    let stream_id = StreamId::for_resource(&node, id, "state");
    EventEnvelope {
        envelope_version: ENVELOPE_VERSION,
        event_id: EventId::from_raw(format!("test-{seq}")),
        node_id: node,
        resource_id: id.to_string(),
        resource_kind: "led".into(),
        resource_version: 1,
        stream_id,
        stream: "state".into(),
        sequence: seq,
        timestamp: time::OffsetDateTime::UNIX_EPOCH,
        payload_kind: "resource.state.changed".into(),
        payload_version: 1,
        payload_schema: None,
        correlation_id: None,
        causation_id: None,
        trace_context: None,
        data: Json::String(value.to_string()),
    }
}

#[tokio::test]
async fn ws_rejects_subscribe_over_cap() {
    let (addr, core, _h) = boot().await;
    let id = resource_id(addr).await;
    let mut ws = open_ws(addr).await;

    for i in 0..64 {
        let topic = format!("hub/led/{id}/state-{i}");
        send_json(&mut ws, json!({"type": "subscribe", "topic": topic})).await;
        let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
        assert_eq!(ack["type"], "subscribe-ack", "subscribe {i} should ack");
    }

    let extra_topic = format!("hub/led/{id}/overflow");
    send_json(&mut ws, json!({"type": "subscribe", "topic": extra_topic})).await;
    let err = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(err["type"], "error");
    assert_eq!(err["code"], json!(429));
    assert!(
        err["message"]
            .as_str()
            .unwrap_or("")
            .contains("subscription cap"),
        "expected cap message, got {err:?}"
    );
    assert_eq!(core.bus.active_subscriptions(), 64);
}

#[tokio::test]
async fn http_ndjson_subscription_removed_on_body_drop() {
    let (addr, core, _h) = boot().await;
    let id = resource_id(addr).await;
    let topic = format!("hub/led/{id}/state");
    let url = format!("http://{addr}/servers/hub/events?topic={topic}");

    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET succeeds");
    assert_eq!(resp.status(), 200);

    tokio::time::sleep(Duration::from_millis(75)).await;
    assert_eq!(
        core.bus.active_subscriptions(),
        1,
        "NDJSON GET should register one bus subscription"
    );

    let mut stream = resp.bytes_stream();
    let _ = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
    drop(stream);

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        core.bus.active_subscriptions(),
        0,
        "subscription must be removed on body drop without a later publish"
    );
}

#[tokio::test]
async fn http_ndjson_emits_stream_gap_on_slow_consumer() {
    let (addr, core, _h) = boot().await;
    let id = resource_id(addr).await;
    let topic = format!("hub/led/{id}/state");

    let url = format!("http://{addr}/servers/hub/events?topic={topic}&outboundCapacity=1");
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET succeeds");
    assert_eq!(resp.status(), 200);
    let mut stream = resp.bytes_stream();

    tokio::time::sleep(Duration::from_millis(75)).await;
    assert_eq!(core.bus.active_subscriptions(), 1);

    core.bus
        .try_publish(led_state_envelope(&id, 1, "on"))
        .expect("first publish succeeds");
    core.bus
        .try_publish(led_state_envelope(&id, 2, "off"))
        .expect("overflow publish resolves");

    let mut buf: Vec<u8> = Vec::new();
    let mut lines: Vec<Json> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                let chunk: Bytes = chunk;
                buf.extend_from_slice(&chunk);
                while let Some(idx) = buf.iter().position(|b| *b == b'\n') {
                    let line = buf.drain(..=idx).collect::<Vec<u8>>();
                    let s =
                        std::str::from_utf8(&line[..line.len() - 1]).expect("ndjson line is utf8");
                    let v: Json = serde_json::from_str(s).expect("ndjson line is valid JSON");
                    lines.push(v);
                }
                if lines.iter().any(|v| v["type"] == "stream-gap") {
                    break;
                }
            }
            Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
        }
    }

    let gap = lines
        .iter()
        .find(|v| v["type"] == "stream-gap")
        .unwrap_or_else(|| panic!("expected a stream-gap line; got {lines:?}"));
    assert_eq!(gap["reason"], json!("slow_consumer"));
    assert_eq!(gap["terminated"], json!(true));
    assert_eq!(gap["lastDeliveredSequence"], json!(1));
}
