//! Integration test for the peer tunnel: hub links to cloud, cloud
//! confirms, then the cloud forwards a query through the tunnel.

use std::time::Duration;

use futures::{future::BoxFuture, SinkExt, StreamExt};
use serde_json::Value as Json;
use tokio_tungstenite::tungstenite::Message;
use zetta_core::{Device, DeviceConfig, DeviceError, TransitionInput};
use zetta_server::Zetta;

#[derive(Default)]
struct Led { on: bool }

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led").name("LED").state(self.state())
            .when("off", &["turn-on"]).when("on", &["turn-off"])
            .monitor("state");
    }
    fn state(&self) -> &str { if self.on { "on" } else { "off" } }
    fn transition<'a>(
        &'a mut self, name: &'a str, _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async move {
            match name {
                "turn-on" => { self.on = true; Ok(()) }
                "turn-off" => { self.on = false; Ok(()) }
                _ => Err(DeviceError::Invalid("?".into())),
            }
        })
    }
}

#[tokio::test]
async fn hub_links_to_cloud_and_cloud_forwards_queries() {
    // Boot cloud.
    let cloud = Zetta::new().name("cloud").build();
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    tokio::spawn(async move {
        axum::serve(cloud_listener, cloud.router).await.unwrap();
    });

    // Boot hub with an LED, linking to cloud.
    let hub = Zetta::new()
        .name("hub")
        .use_device(Led::default())
        .link(format!("http://{cloud_addr}"))
        .build();
    let hub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hub_addr = hub_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(hub_listener, hub.router).await.unwrap();
    });

    // Wait for cloud to confirm the peer.
    assert!(cloud_acceptors.wait_for_first(Duration::from_secs(5)).await,
        "cloud should have received a confirmed peer within 5s");

    // Cloud's root advertises hub as a peer.
    let root: Json = reqwest::get(format!("http://{cloud_addr}/"))
        .await.unwrap().json().await.unwrap();
    let links = root["links"].as_array().unwrap();
    let has_peer = links.iter().any(|l| {
        let rels: Vec<&str> = l["rel"].as_array().unwrap()
            .iter().filter_map(|v| v.as_str()).collect();
        rels.contains(&"http://rels.zettajs.io/peer") && l["title"] == "hub"
    });
    assert!(has_peer, "cloud root should advertise hub as peer: {root}");

    // Cloud forwards `/servers/hub` to the hub through the tunnel.
    let server: Json = reqwest::get(format!("http://{cloud_addr}/servers/hub"))
        .await.unwrap().json().await.unwrap();
    assert_eq!(server["properties"]["name"], "hub");
    let entities = server["entities"].as_array().expect("entities");
    assert!(!entities.is_empty(), "hub should have at least one device");
    let dev_id = entities[0]["properties"]["id"].as_str().unwrap().to_string();
    assert_eq!(entities[0]["properties"]["type"], "led");
    assert_eq!(entities[0]["properties"]["state"], "off");

    // Forward a transition POST.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{cloud_addr}/servers/hub/devices/{dev_id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "forwarded transition should succeed");
    let dev: Json = resp.json().await.unwrap();
    assert_eq!(dev["properties"]["state"], "on");

    // Forward GET device for verification.
    let dev: Json = reqwest::get(format!("http://{cloud_addr}/servers/hub/devices/{dev_id}"))
        .await.unwrap().json().await.unwrap();
    assert_eq!(dev["properties"]["state"], "on");

    // Direct check on the hub returns the same.
    let dev_direct: Json = reqwest::get(format!("http://{hub_addr}/servers/hub/devices/{dev_id}"))
        .await.unwrap().json().await.unwrap();
    assert_eq!(dev_direct["properties"]["state"], "on");
}

#[tokio::test]
async fn cloud_ws_forwards_peer_events() {
    let cloud = Zetta::new().name("cloud").build();
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    tokio::spawn(async move {
        axum::serve(cloud_listener, cloud.router).await.unwrap();
    });

    let hub = Zetta::new()
        .name("hub")
        .use_device(Led::default())
        .link(format!("http://{cloud_addr}"))
        .build();
    let hub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hub_addr = hub_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(hub_listener, hub.router).await.unwrap();
    });

    assert!(cloud_acceptors.wait_for_first(Duration::from_secs(5)).await);

    // Discover the LED's id via the cloud (which forwards to the hub).
    let server: Json = reqwest::get(format!("http://{cloud_addr}/servers/hub"))
        .await.unwrap().json().await.unwrap();
    let dev_id = server["entities"][0]["properties"]["id"].as_str().unwrap().to_string();

    // Connect a WS client to the CLOUD's /events.
    let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{cloud_addr}/events"))
        .await.unwrap();

    let topic = format!("hub/led/{dev_id}/state");
    let sub = serde_json::json!({"type": "subscribe", "topic": topic});
    ws.send(Message::Text(sub.to_string().into())).await.unwrap();

    // Read subscribe-ack.
    let ack = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await.unwrap().unwrap().unwrap();
    let ack: Json = match ack {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!(),
    };
    assert_eq!(ack["type"], "subscribe-ack");

    // Trigger the LED on the HUB directly.
    let client = reqwest::Client::new();
    let _ = client
        .post(format!("http://{hub_addr}/servers/hub/devices/{dev_id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send().await.unwrap();

    // The cloud WS should receive the event, forwarded through the tunnel.
    let evt = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await.expect("timeout waiting for forwarded event")
        .unwrap().unwrap();
    let evt: Json = match evt {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!(),
    };
    assert_eq!(evt["type"], "event", "expected event message, got {evt}");
    assert_eq!(evt["topic"], topic);
    assert_eq!(evt["data"], "on");
}
