//! When the HTTP NDJSON stream's bus subscription overflows under
//! `Lossless` semantics, the stream must emit a final structured
//! `stream-gap` line before EOF (instead of silently ending the
//! response body).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use boardwalk::events::{ENVELOPE_VERSION, EventEnvelope, EventId, NodeId, StreamId};
use boardwalk::http::{Core, CoreBuilder, router};
use boardwalk::{Device, DeviceConfig, DeviceError, TransitionInput};
use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use serde_json::{Value as Json, json};

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

async fn boot() -> (SocketAddr, Arc<Core>) {
    let mut b = CoreBuilder::new("hub");
    b.add_device(Led::default());
    let core = b.build();
    let app = router(core.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, core)
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
        data: serde_json::Value::String(value.to_string()),
    }
}

#[tokio::test]
async fn http_ndjson_emits_stream_gap_on_slow_consumer() {
    let (addr, core) = boot().await;
    let id = device_id(addr).await;
    let topic = format!("hub/led/{id}/state");

    // outboundCapacity=1 forces the bus side into the `Lossless`
    // overflow path the moment we publish a second time without the
    // stream reader having pulled the first envelope.
    let url = format!("http://{addr}/servers/hub/events?topic={topic}&outboundCapacity=1");
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET succeeds");
    assert_eq!(resp.status(), 200);
    let mut stream = resp.bytes_stream();

    // Wait until the subscription is wired up.
    tokio::time::sleep(Duration::from_millis(75)).await;
    assert_eq!(core.bus.active_subscriptions(), 1);

    // Publish two envelopes directly through the bus. The first lands
    // in the bus mpsc; the second finds the queue full and fires
    // slow_consumer.
    let _ = core.bus.try_publish(led_state_envelope(&id, 1, "on"));
    let _ = core.bus.try_publish(led_state_envelope(&id, 2, "off"));

    // Drain the NDJSON body until either a `stream-gap` line or EOF.
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
            Ok(Some(Err(_))) => break,
            Ok(None) => break,
            Err(_) => break,
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
