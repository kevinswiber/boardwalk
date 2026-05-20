//! Contract tests pinning the peer-forwarded NDJSON event
//! shape on `/servers/{name}/events?topic=...`. Each line carries the
//! legacy `{topic, timestamp, data}` keys plus the additive envelope
//! fields (`eventId`, `streamId`, `sequence`, etc.).

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::StreamExt;
use serde_json::{Value as Json, json};

use super::actor_led_fixture::ActorLed;
use crate::http::{Core, router};
use crate::runtime::NodeBuilder;

async fn boot_hub() -> (SocketAddr, Arc<Core>, tokio::task::JoinHandle<()>) {
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Opens the NDJSON stream against `/servers/hub/events?topic=...`,
/// gives the route a moment to subscribe, fires `turn-on`, and reads
/// the first complete line as JSON.
async fn first_ndjson_line(addr: SocketAddr, id: &str, topic: &str) -> Json {
    let url = format!("http://{addr}/servers/hub/events?topic={topic}");
    let client = reqwest::Client::new();
    let resp = client.get(&url).send().await.expect("GET succeeds");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "NDJSON GET should be 200"
    );
    let mut stream = resp.bytes_stream();

    // Give the subscription a moment to register before firing the event.
    tokio::time::sleep(Duration::from_millis(75)).await;
    assert_eq!(post_action(addr, id, "turn-on").await, 200);

    let mut buf: Vec<u8> = Vec::new();
    let read_one_line = async {
        loop {
            let chunk: Bytes = match stream.next().await {
                Some(Ok(b)) => b,
                Some(Err(e)) => panic!("ndjson stream error: {e}"),
                None => panic!("ndjson stream ended before first line"),
            };
            buf.extend_from_slice(&chunk);
            if let Some(idx) = buf.iter().position(|b| *b == b'\n') {
                let line = std::str::from_utf8(&buf[..idx])
                    .expect("ndjson line is utf8")
                    .to_string();
                return line;
            }
        }
    };
    let line = tokio::time::timeout(Duration::from_secs(2), read_one_line)
        .await
        .expect("expected first ndjson line within 2s");
    serde_json::from_str(&line).expect("ndjson line is valid JSON")
}

#[tokio::test]
async fn peer_ndjson_line_keys_include_legacy_and_envelope_fields() {
    let (addr, _core, _h) = boot_hub().await;
    let id = device_id(addr).await;
    let topic = format!("hub/led/{id}/state");

    let line = first_ndjson_line(addr, &id, &topic).await;
    let obj = line.as_object().expect("ndjson line is a JSON object");
    let actual: BTreeSet<&str> = obj.keys().map(|s| s.as_str()).collect();
    let expected: BTreeSet<&str> = [
        "topic",
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
        "peer NDJSON keys must include both legacy and envelope fields"
    );
}

#[tokio::test]
async fn peer_ndjson_timestamp_is_epoch_milliseconds_i64() {
    let (addr, _core, _h) = boot_hub().await;
    let id = device_id(addr).await;
    let topic = format!("hub/led/{id}/state");

    let before = now_ms();
    let line = first_ndjson_line(addr, &id, &topic).await;
    let after = now_ms();

    let ts = line["timestamp"]
        .as_i64()
        .unwrap_or_else(|| panic!("timestamp must be an i64; got {:?}", line["timestamp"]));
    let low = before - 5_000;
    let high = after + 5_000;
    assert!(
        ts >= low && ts <= high,
        "timestamp {ts} must lie within ±5s of now_ms (before={before}, after={after})"
    );
}

#[tokio::test]
async fn peer_ndjson_topic_matches_subscribed_topic() {
    let (addr, _core, _h) = boot_hub().await;
    let id = device_id(addr).await;
    let topic = format!("hub/led/{id}/state");

    let line = first_ndjson_line(addr, &id, &topic).await;
    assert_eq!(
        line["topic"],
        json!(topic),
        "ndjson `topic` must equal the subscribed topic"
    );
}
