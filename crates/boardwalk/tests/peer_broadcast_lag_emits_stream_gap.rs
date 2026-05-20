//! Peer broadcast `RecvError::Lagged` emits a `stream-gap` via the
//! out-of-band terminal channel, eagerly removes the `fwd_subs` entry,
//! and decrements the `PeerStreamHub` refcount.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use boardwalk::Boardwalk;
use boardwalk::core::{Device, DeviceConfig, DeviceError, TransitionInput};
use boardwalk::http::PeerStreamHub;
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
            .state(self.state())
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

struct Pair {
    cloud_addr: SocketAddr,
    cloud_streams: PeerStreamHub,
}

async fn boot_pair() -> Pair {
    let cloud = Boardwalk::new().name("cloud").build().unwrap();
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    let cloud_streams = cloud.peer_streams.clone();
    tokio::spawn(async move {
        axum::serve(cloud_listener, cloud.router).await.unwrap();
    });

    let hub = Boardwalk::new()
        .name("hub")
        .use_actor(Led::default())
        .link(format!("http://{cloud_addr}"))
        .build()
        .unwrap();
    let hub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(async move {
        axum::serve(hub_listener, hub.router).await.unwrap();
    });

    assert!(
        cloud_acceptors.wait_for_first(Duration::from_secs(5)).await,
        "cloud should confirm hub peer within 5s"
    );

    Pair {
        cloud_addr,
        cloud_streams,
    }
}

async fn device_id_via(addr: SocketAddr) -> String {
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

/// Floods the peer broadcast for `(peer, topic)` with `n` dummy
/// events, yielding to the runtime periodically so other peers'
/// forwarders can drain. Combined with a small-capacity WS subscriber
/// that is not being drained, this forces *that* WS forwarder's
/// `broadcast::Receiver` into `Lagged` without dragging other
/// receivers down with it.
async fn flood_peer_broadcast(streams: &PeerStreamHub, peer: &str, topic: &str, n: usize) {
    let sender = streams
        .broadcast_sender(peer, topic)
        .await
        .expect("broadcast sender exists for (peer, topic)");
    // Synchronous loop. Under a current-thread runtime, forwarders
    // cannot drain while this runs, so any receiver whose forwarder is
    // blocked on out_tx.send() falls behind by `n - 256` items.
    for i in 0..n {
        let _ = sender.send(Arc::new(
            json!({"topic": topic, "timestamp": i as i64, "data": "x"}),
        ));
    }
}

#[tokio::test]
async fn peer_broadcast_lag_emits_stream_gap_and_closes() {
    let p = boot_pair().await;
    let id = device_id_via(p.cloud_addr).await;
    let mut ws = open_ws(p.cloud_addr).await;

    let topic = format!("hub/led/{id}/state");
    send_json(&mut ws, json!({"type": "subscribe", "topic": topic})).await;
    let _ack = recv_json(&mut ws, Duration::from_secs(2)).await;

    // Give the dispatcher a moment to register the peer broadcast.
    tokio::time::sleep(Duration::from_millis(150)).await;
    // BROADCAST_BUFFER is 256; flood with 1000 to guarantee lag.
    flood_peer_broadcast(&p.cloud_streams, "hub", &topic, 1000).await;

    // Drain frames; the gap must arrive. We do not assert a WS Close
    // frame here — terminal delivery is per-subscription, not
    // per-connection: a per-subscription gap must not tear down
    // unrelated subscriptions sharing the same socket.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut saw_gap = false;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Json = serde_json::from_str(&t).expect("valid json");
                if v["type"] == "stream-gap" {
                    assert!(
                        v["reason"]
                            .as_str()
                            .unwrap_or("")
                            .starts_with("broadcast_lag"),
                        "expected reason starting with broadcast_lag; got {v:?}"
                    );
                    assert_eq!(v["terminated"], json!(true));
                    saw_gap = true;
                    break;
                }
            }
            Ok(Some(Ok(Message::Close(_)))) => break,
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }
    assert!(saw_gap, "expected a stream-gap frame");
}

/// Eager-unsubscribe: when a single peer-forwarded subscription lags,
/// the dispatcher removes its `fwd_subs` entry and decrements the
/// `PeerStreamHub` refcount immediately — independent of any client
/// `unsubscribe` message or socket close.
///
/// The stricter two-subscribers-same-topic variant (refcount 2 → 1) is
/// timing-dependent under tokio's broadcast semantics (both receivers
/// of a single broadcast::channel see the same backlog, so lagging
/// only one of them deterministically is hard). This test verifies
/// the eviction path itself with refcount 1 → 0.
#[tokio::test]
async fn peer_broadcast_lag_drops_peer_stream_refcount_eagerly() {
    let p = boot_pair().await;
    let id = device_id_via(p.cloud_addr).await;
    let mut ws = open_ws(p.cloud_addr).await;

    let topic = format!("hub/led/{id}/state");
    send_json(&mut ws, json!({"type": "subscribe", "topic": &topic})).await;
    let _ack = recv_json(&mut ws, Duration::from_secs(2)).await;

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(p.cloud_streams.refcount("hub", &topic).await, Some(1));

    // Synchronous flood lags the receiver.
    flood_peer_broadcast(&p.cloud_streams, "hub", &topic, 1000).await;

    // Drain ws so the forwarder can poll rx.recv() and detect Lagged.
    let _drainer = tokio::spawn(async move {
        loop {
            if ws.next().await.is_none() {
                break;
            }
        }
    });

    // Eager unsubscribe drops the refcount to 0 → entry removed.
    let mut gone = false;
    for _ in 0..60 {
        if p.cloud_streams.refcount("hub", &topic).await.is_none() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        gone,
        "expected PeerStreamHub entry to be evicted after lag; refcount still {:?}",
        p.cloud_streams.refcount("hub", &topic).await
    );
}

#[tokio::test]
async fn peer_broadcast_lag_does_not_terminate_unrelated_subscriptions() {
    let p = boot_pair().await;
    let id = device_id_via(p.cloud_addr).await;

    let topic_a = format!("hub/led/{id}/state");
    let topic_b = format!("hub/led/{id}/other");

    let mut ws = open_ws(p.cloud_addr).await;
    // Subscribe A first.
    send_json(&mut ws, json!({"type": "subscribe", "topic": &topic_a})).await;
    let ack_a = recv_json(&mut ws, Duration::from_secs(2)).await;
    let sub_a_id = ack_a["subscriptionId"].as_u64().unwrap();
    // Subscribe B on a different upstream topic.
    send_json(&mut ws, json!({"type": "subscribe", "topic": &topic_b})).await;
    let ack_b = recv_json(&mut ws, Duration::from_secs(2)).await;
    let sub_b_id = ack_b["subscriptionId"].as_u64().unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Flood A only.
    flood_peer_broadcast(&p.cloud_streams, "hub", &topic_a, 1000).await;

    // Drain a few frames; expect to see A's stream-gap and afterwards
    // events on B should still be deliverable. We don't have a hub-side
    // way to publish on topic_b here, but the upstream stream for B
    // should still be alive (refcount 1, not closed).
    let mut saw_a_gap = false;
    for _ in 0..30 {
        match tokio::time::timeout(Duration::from_millis(200), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Json = serde_json::from_str(&t).expect("json");
                if v["type"] == "stream-gap" && v["subscriptionId"].as_u64() == Some(sub_a_id) {
                    saw_a_gap = true;
                }
                if v["type"] == "stream-gap" && v["subscriptionId"].as_u64() == Some(sub_b_id) {
                    panic!("stream-gap should not fire on unrelated subscription B");
                }
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {
                if saw_a_gap {
                    break;
                }
            }
        }
    }
    assert!(saw_a_gap, "expected A to receive a stream-gap");

    // B's upstream stream should remain registered.
    assert_eq!(p.cloud_streams.refcount("hub", &topic_b).await, Some(1));
}
