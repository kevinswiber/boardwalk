//! Characterization tests for the peer link: cloud advertises hub as
//! a peer, forwarded GETs reach the hub, peer subscriptions to the same
//! topic share one upstream stream, and the upstream is torn down when
//! the last subscriber leaves.

use std::net::SocketAddr;
use std::time::Duration;

use boardwalk::http::PeerStreamHub;
use boardwalk::{Boardwalk, Device, DeviceConfig, DeviceError, TransitionInput};
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
    hub_addr: SocketAddr,
    cloud_streams: PeerStreamHub,
}

/// Boots cloud + hub (with one LED) and waits until cloud has confirmed
/// the peer link.
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
        .use_device(Led::default())
        .link(format!("http://{cloud_addr}"))
        .build()
        .unwrap();
    let hub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hub_addr = hub_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(hub_listener, hub.router).await.unwrap();
    });

    assert!(
        cloud_acceptors.wait_for_first(Duration::from_secs(5)).await,
        "cloud should confirm hub peer within 5s"
    );

    Pair {
        cloud_addr,
        hub_addr,
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

#[tokio::test]
async fn cloud_root_advertises_peer_link_after_peer_dials() {
    let p = boot_pair().await;
    let root: Json = reqwest::get(format!("http://{}/", p.cloud_addr))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let links = root["links"].as_array().expect("links present on root");
    let peer_link = links
        .iter()
        .find(|l| {
            let rels: Vec<&str> = l["rel"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|v| v.as_str())
                .collect();
            rels.contains(&"https://rels.boardwalk.to/peer")
                && rels.contains(&"https://rels.boardwalk.to/server")
        })
        .expect("expected a link advertising hub as a peer server");

    assert_eq!(peer_link["title"], "hub");
    let href = peer_link["href"].as_str().unwrap();
    assert!(
        href.ends_with("/servers/hub"),
        "peer link href should target /servers/hub, got {href:?}"
    );
}

#[tokio::test]
async fn forwarded_get_device_returns_hub_data_through_cloud() {
    let p = boot_pair().await;
    let id = device_id_via(p.cloud_addr).await;

    let via_cloud: Json = reqwest::get(format!("http://{}/servers/hub/devices/{id}", p.cloud_addr))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let direct: Json = reqwest::get(format!("http://{}/servers/hub/devices/{id}", p.hub_addr))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // Properties (id, type, name, state) and class come from the
    // device snapshot and must match across forwarding paths.
    assert_eq!(via_cloud["properties"], direct["properties"]);
    assert_eq!(via_cloud["class"], direct["class"]);

    // Action affordances must match in name/method/fields/type. The
    // absolute `href` differs because the forwarded render uses the
    // tunnel-internal host; we deliberately do *not* pin href equality
    // here. The narrower comparison still catches regressions in the
    // forwarded action surface.
    fn action_stub(a: &Json) -> Json {
        json!({
            "name": a["name"],
            "method": a["method"],
            "type": a["type"],
            "fields": a["fields"],
        })
    }
    let cloud_actions: Vec<Json> = via_cloud["actions"]
        .as_array()
        .expect("actions array")
        .iter()
        .map(action_stub)
        .collect();
    let direct_actions: Vec<Json> = direct["actions"]
        .as_array()
        .expect("actions array")
        .iter()
        .map(action_stub)
        .collect();
    assert_eq!(cloud_actions, direct_actions);
}

#[tokio::test]
async fn forwarded_event_stream_shares_one_upstream_per_topic() {
    let p = boot_pair().await;
    let id = device_id_via(p.cloud_addr).await;
    let topic = format!("hub/led/{id}/state");

    let mut ws1 = open_ws(p.cloud_addr).await;
    let mut ws2 = open_ws(p.cloud_addr).await;
    send_json(&mut ws1, json!({"type": "subscribe", "topic": topic})).await;
    send_json(&mut ws2, json!({"type": "subscribe", "topic": topic})).await;
    let _ = recv_json(&mut ws1, Duration::from_secs(2)).await;
    let _ = recv_json(&mut ws2, Duration::from_secs(2)).await;

    // Allow the dedup hub to register both subscribers.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        p.cloud_streams.active_streams().await,
        1,
        "two subscribers to the same (peer, topic) should share one upstream"
    );

    // Trigger a state change at the hub; both clients should see it.
    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://{}/servers/hub/devices/{id}", p.hub_addr))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send()
        .await
        .unwrap();

    let e1 = recv_json(&mut ws1, Duration::from_secs(3)).await;
    let e2 = recv_json(&mut ws2, Duration::from_secs(3)).await;
    assert_eq!(e1["type"], "event");
    assert_eq!(e2["type"], "event");
    assert_eq!(e1["data"], "on");
    assert_eq!(e2["data"], "on");
}

#[tokio::test]
async fn last_unsubscribe_tears_down_shared_upstream() {
    let p = boot_pair().await;
    let id = device_id_via(p.cloud_addr).await;
    let topic = format!("hub/led/{id}/state");

    let mut ws1 = open_ws(p.cloud_addr).await;
    let mut ws2 = open_ws(p.cloud_addr).await;
    send_json(&mut ws1, json!({"type": "subscribe", "topic": topic})).await;
    send_json(&mut ws2, json!({"type": "subscribe", "topic": topic})).await;
    let ack1 = recv_json(&mut ws1, Duration::from_secs(2)).await;
    let ack2 = recv_json(&mut ws2, Duration::from_secs(2)).await;
    let sub1 = ack1["subscriptionId"].as_u64().unwrap();
    let sub2 = ack2["subscriptionId"].as_u64().unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(p.cloud_streams.active_streams().await, 1);

    send_json(
        &mut ws1,
        json!({"type": "unsubscribe", "subscriptionId": sub1}),
    )
    .await;
    let _ = recv_json(&mut ws1, Duration::from_secs(2)).await;

    // One subscriber remains — upstream should still be open.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        p.cloud_streams.active_streams().await,
        1,
        "upstream should remain while one subscriber is still attached"
    );

    send_json(
        &mut ws2,
        json!({"type": "unsubscribe", "subscriptionId": sub2}),
    )
    .await;
    let _ = recv_json(&mut ws2, Duration::from_secs(2)).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        p.cloud_streams.active_streams().await,
        0,
        "upstream should be torn down after the last subscriber leaves"
    );
}
