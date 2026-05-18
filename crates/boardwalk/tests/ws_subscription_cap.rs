//! Each WS connection is capped at 64 simultaneously-active
//! subscriptions. Subscribe beyond the cap returns an
//! `OutboundMessage::Error { code: 429 }` and does not create the
//! subscription.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use boardwalk::http::{Core, CoreBuilder, router};
use boardwalk::{Device, DeviceConfig, DeviceError, TransitionInput};
use futures::future::BoxFuture;
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

#[tokio::test]
async fn ws_rejects_subscribe_over_cap() {
    let (addr, core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    // First 64 subscribes succeed.
    for i in 0..64 {
        let topic = format!("hub/led/{id}/state-{i}");
        send_json(&mut ws, json!({"type": "subscribe", "topic": topic})).await;
        let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
        assert_eq!(ack["type"], "subscribe-ack", "subscribe {i} should ack");
    }

    // 65th subscribe gets a 429 error and is not created.
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

    // Confirm the bus saw only the 64 successful subscribes.
    assert_eq!(core.bus.active_subscriptions(), 64);
}

#[tokio::test]
async fn ws_reclaims_cap_slot_when_local_forwarder_exits() {
    // A `limit: 1` subscription auto-removes from the bus after one
    // event. The WS dispatcher must reclaim the matching
    // `conn.local_subs` slot via the forwarder's `LocalTerminated`
    // back-channel; otherwise this connection's 64-cap would stay
    // pinned at its high-water mark even when all prior subs are
    // already dead.
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;
    let mut ws = open_ws(addr).await;

    // Stand up 5 limit:1 subscriptions to the LED's state stream.
    let topic = format!("hub/led/{id}/state");
    for _ in 0..5 {
        send_json(
            &mut ws,
            json!({"type": "subscribe", "topic": &topic, "limit": 1}),
        )
        .await;
        let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
        assert_eq!(ack["type"], "subscribe-ack");
    }

    // One transition fan-outs to all 5 limit:1 subs; each delivers
    // its one event, hits limit, and the bus auto-removes it.
    let _ = reqwest::Client::new()
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send()
        .await
        .unwrap();

    // Drain the 5 event frames the WS receives.
    for _ in 0..5 {
        let evt = recv_json(&mut ws, Duration::from_secs(2)).await;
        assert_eq!(evt["type"], "event");
    }

    // Give the dispatcher time to process all 5 `LocalTerminated`
    // events (each forwarder exited; bus_id was unsubscribed).
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Now stand up 64 fresh subscriptions on unique topics. With
    // proper slot reclaim, all 64 ack within cap. Without it, the
    // 60th would 429 (5 stale + 64 fresh > 64 cap).
    for i in 0..64 {
        let fresh = format!("hub/led/{id}/state-fresh-{i}");
        send_json(&mut ws, json!({"type": "subscribe", "topic": fresh})).await;
        let ack = recv_json(&mut ws, Duration::from_secs(2)).await;
        assert_eq!(
            ack["type"], "subscribe-ack",
            "subscribe-ack #{i} expected after slot reclaim, got {ack:?}"
        );
    }
}
