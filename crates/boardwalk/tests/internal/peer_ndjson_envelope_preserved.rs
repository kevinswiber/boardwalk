//! Envelope fields produced by the hub's `StreamRegistry` survive the
//! peer-forward NDJSON hop. A cloud-side GET against
//! `/servers/hub/events?topic=...` is round-tripped through the peer
//! tunnel and back; the first line carries the envelope.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use serde_json::{Value as Json, json};

use super::actor_led_fixture::ActorLed;
use crate::Boardwalk;

struct Pair {
    cloud_addr: SocketAddr,
    hub_addr: SocketAddr,
}

async fn boot_pair() -> Pair {
    let cloud = Boardwalk::new().name("cloud").build().unwrap();
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    tokio::spawn(async move {
        axum::serve(cloud_listener, cloud.router).await.unwrap();
    });

    let hub = Boardwalk::new()
        .name("hub")
        .use_actor(ActorLed::default())
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

async fn post_action(addr: SocketAddr, id: &str, action: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("http://{addr}/resources/{id}/transitions/{action}"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn peer_ndjson_line_envelope_fields_match_origin() {
    let p = boot_pair().await;
    let id = device_id_via(p.cloud_addr).await;
    let topic = format!("hub/led/{id}/state");

    let url = format!("http://{}/servers/hub/events?topic={topic}", p.cloud_addr);
    let resp = reqwest::Client::new().get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let mut stream = resp.bytes_stream();

    // Let the cloud subscribe + the upstream peer stream wire up.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(post_action(p.hub_addr, &id, "turn-on").await, 200);

    let mut buf: Vec<u8> = Vec::new();
    let line: String = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let chunk: Bytes = match stream.next().await {
                Some(Ok(b)) => b,
                Some(Err(e)) => panic!("ndjson stream error: {e}"),
                None => panic!("ndjson stream ended"),
            };
            buf.extend_from_slice(&chunk);
            if let Some(idx) = buf.iter().position(|b| *b == b'\n') {
                let s = std::str::from_utf8(&buf[..idx]).unwrap().to_string();
                return s;
            }
        }
    })
    .await
    .expect("first ndjson line within 3s");

    let v: Json = serde_json::from_str(&line).expect("valid json");
    assert_eq!(v["topic"], topic);
    assert_eq!(v["data"], "on");
    assert_eq!(v["nodeId"], "hub");
    assert_eq!(v["resourceKind"], "led");
    assert_eq!(v["resourceId"], id);
    assert_eq!(v["sequence"], 1);
    assert_eq!(v["payloadKind"], "resource.state.changed");
    assert_eq!(v["envelopeVersion"], 1);
    assert_eq!(
        v["streamId"],
        json!(format!("bw://hub/resources/{id}/streams/state"))
    );
    let event_id = v["eventId"].as_str().expect("eventId is a string");
    assert!(!event_id.is_empty());
}
