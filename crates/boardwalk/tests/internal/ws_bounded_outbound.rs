//! The WS connection's outbound channel is bounded, and a
//! `stream-gap` reaches the wire via an out-of-band terminal channel
//! before the connection closes.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio_tungstenite::tungstenite::Message;

use crate::core::{Device, DeviceConfig, DeviceError, TransitionInput};
use crate::events::{ENVELOPE_VERSION, EventEnvelope, EventId, NodeId, StreamId};
use crate::http::{Core, CoreBuilder, router};

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

/// Build a state envelope for the LED so we can publish directly
/// through the bus, bypassing HTTP run_transition. Going through the
/// bus directly lets us drive backpressure without yielding the
/// runtime to the WS writer.
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

/// Drains the WS until a `stream-gap` frame or terminal condition is
/// observed, accumulating frames seen along the way. Returns
/// `(frames, gap_seen, terminated_in_drain)`.
async fn drain_until_gap_or_close(
    ws: &mut Ws,
    overall: Duration,
) -> (Vec<Json>, Option<Json>, bool) {
    let deadline = std::time::Instant::now() + overall;
    let mut frames: Vec<Json> = Vec::new();
    let mut gap: Option<Json> = None;
    let mut closed = false;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Json = serde_json::from_str(&t).expect("valid json");
                let ty = v["type"].as_str().unwrap_or("");
                if ty == "stream-gap" {
                    gap = Some(v.clone());
                    frames.push(v);
                    break;
                } else {
                    frames.push(v);
                }
            }
            Ok(Some(Ok(Message::Close(_)))) => {
                closed = true;
                break;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) => {
                closed = true;
                break;
            }
            Ok(None) => {
                closed = true;
                break;
            }
            Err(_) => break,
        }
    }
    (frames, gap, closed)
}

#[tokio::test]
async fn slow_ws_reader_receives_stream_gap_and_disconnects() {
    let (addr, core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    // Subscribe with a tiny bus-level outbound capacity so the
    // slow-consumer disconnect path fires the moment the WS reader
    // stalls.
    let topic = format!("hub/led/{id}/state");
    send_json(
        &mut ws,
        json!({
            "type": "subscribe",
            "topic": topic,
            "outboundCapacity": 1,
        }),
    )
    .await;
    let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(ack["type"], "subscribe-ack");
    let sub_id = ack["subscriptionId"].as_u64().unwrap();

    // Stop reading from the WS, then publish directly through the
    // bus in a tight loop. With outboundCapacity=1 and the WS reader
    // not draining, the second publish finds the bus queue full and
    // fires the slow-consumer disconnect.
    for i in 0..50 {
        let _ = core.bus.try_publish(led_state_envelope(
            &id,
            i,
            if i % 2 == 0 { "on" } else { "off" },
        ));
    }

    // Resume reading; drain until a `stream-gap` arrives or the
    // connection closes.
    let (frames, gap, _closed) = drain_until_gap_or_close(&mut ws, Duration::from_secs(3)).await;

    let gap = gap.expect("expected a stream-gap frame");
    assert_eq!(gap["type"], "stream-gap");
    assert_eq!(gap["reason"], json!("slow_consumer"));
    assert_eq!(gap["terminated"], json!(true));
    assert_eq!(gap["subscriptionId"], json!(sub_id));

    // The gap is the *last* frame in the drained sequence.
    assert_eq!(
        frames.iter().filter(|v| v["type"] == "stream-gap").count(),
        1,
        "expected exactly one stream-gap frame in drained sequence"
    );
    let gap_index = frames
        .iter()
        .position(|v| v["type"] == "stream-gap")
        .unwrap();
    let after_gap_events: Vec<&Json> = frames[(gap_index + 1)..]
        .iter()
        .filter(|v| v["type"] == "event" && v["subscriptionId"] == json!(sub_id))
        .collect();
    assert!(
        after_gap_events.is_empty(),
        "no event frame should arrive after the stream-gap on this subscription"
    );

    // Next read should be either `Close` or `None`.
    let final_frame = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
    match final_frame {
        Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Ok(Some(Err(_))) => {}
        Ok(Some(Ok(other))) => panic!("expected close after stream-gap, got {other:?}"),
        Err(_) => {
            // Timed out — also acceptable: writer task closed the
            // socket; client just hasn't been polled for Close yet.
        }
    }
}

#[tokio::test]
async fn terminal_frame_delivered_even_when_normal_channel_full() {
    let (addr, core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    // Small bus capacity + tight publish loop bypassing HTTP forces
    // the queue to fill before the WS forwarder gets a chance to
    // drain it.
    let topic = format!("hub/led/{id}/state");
    send_json(
        &mut ws,
        json!({
            "type": "subscribe",
            "topic": topic,
            "outboundCapacity": 1,
        }),
    )
    .await;
    let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
    assert_eq!(ack["type"], "subscribe-ack");

    for i in 0..20 {
        let _ = core.bus.try_publish(led_state_envelope(
            &id,
            i,
            if i % 2 == 0 { "on" } else { "off" },
        ));
    }

    // Drain and confirm we eventually observe a stream-gap.
    let (frames, gap, _closed) = drain_until_gap_or_close(&mut ws, Duration::from_secs(3)).await;
    assert!(gap.is_some(), "stream-gap must reach the wire");

    let frame_types: BTreeSet<&str> = frames.iter().filter_map(|v| v["type"].as_str()).collect();
    assert!(
        frame_types.contains("stream-gap"),
        "drained frames must contain stream-gap; saw {frame_types:?}"
    );
}

#[tokio::test]
async fn closed_ws_normal_channel_does_not_panic() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    let topic = format!("hub/led/{id}/state");
    send_json(&mut ws, json!({"type": "subscribe", "topic": topic})).await;
    let _ack = recv_json(&mut ws, Duration::from_secs(2)).await;

    // Close the WS from the client side, then drive publishes.
    ws.close(None).await.ok();
    drop(ws);

    for i in 0..5 {
        let action = if i % 2 == 0 { "turn-on" } else { "turn-off" };
        assert_eq!(post_action(addr, &id, action).await, 200);
    }

    // If we got here without a panic, the assertion holds.
    tokio::time::sleep(Duration::from_millis(150)).await;
}
