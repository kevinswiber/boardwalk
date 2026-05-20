//! Integration test for the peer tunnel: hub links to cloud, cloud
//! confirms, then the cloud forwards a query through the tunnel.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::Value as Json;
use tokio_tungstenite::tungstenite::Message;

use super::actor_led_fixture::ActorLed;
use crate::Boardwalk;

#[tokio::test]
async fn hub_links_to_cloud_and_cloud_forwards_queries() {
    // Boot cloud.
    let cloud = Boardwalk::new().name("cloud").build().unwrap();
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    tokio::spawn(async move {
        axum::serve(cloud_listener, cloud.router).await.unwrap();
    });

    // Boot hub with an LED, linking to cloud.
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

    // Wait for cloud to confirm the peer.
    assert!(
        cloud_acceptors.wait_for_first(Duration::from_secs(5)).await,
        "cloud should have received a confirmed peer within 5s"
    );

    // Cloud's root advertises hub as a peer.
    let root: Json = reqwest::get(format!("http://{cloud_addr}/"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let links = root["links"].as_array().unwrap();
    let has_peer = links.iter().any(|l| {
        let rels: Vec<&str> = l["rel"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        rels.contains(&"https://rels.boardwalk.to/peer") && l["title"] == "hub"
    });
    assert!(has_peer, "cloud root should advertise hub as peer: {root}");

    // Cloud forwards `/servers/hub` to the hub through the tunnel.
    let server: Json = reqwest::get(format!("http://{cloud_addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(server["properties"]["name"], "hub");
    let entities = server["entities"].as_array().expect("entities");
    assert!(
        !entities.is_empty(),
        "hub should have at least one resource"
    );
    let resource_id = entities[0]["properties"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(entities[0]["properties"]["kind"], "led");
    assert_eq!(entities[0]["properties"]["state"], "off");
    assert_eq!(entities[0]["properties"]["fixture"], "actor-led");

    // Forward a transition POST through the cloud gateway.
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{cloud_addr}/servers/hub/resources/{resource_id}/transitions/turn-on"
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "forwarded transition should succeed");
    let outcome: Json = resp.json().await.unwrap();
    assert_eq!(outcome["snapshot"]["state"], "on");

    // Forward GET resource for verification.
    let dev: Json = reqwest::get(format!(
        "http://{cloud_addr}/servers/hub/resources/{resource_id}"
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(dev["properties"]["state"], "on");

    // Direct check on the hub returns the same.
    let dev_direct: Json = reqwest::get(format!("http://{hub_addr}/resources/{resource_id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dev_direct["properties"]["state"], "on");
}

#[tokio::test]
async fn cloud_dedups_peer_subscriptions() {
    let cloud = Boardwalk::new().name("cloud").build().unwrap();
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    let cloud_streams = cloud.peer_streams.clone();
    let cloud_router = cloud.router.clone();
    tokio::spawn(async move {
        axum::serve(cloud_listener, cloud_router).await.unwrap();
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

    assert!(cloud_acceptors.wait_for_first(Duration::from_secs(5)).await);

    // Discover resource id.
    let server: Json = reqwest::get(format!("http://{cloud_addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(server["entities"][0]["properties"]["fixture"], "actor-led");
    let resource_id = server["entities"][0]["properties"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let topic = format!("hub/led/{resource_id}/state");

    // Two WS clients subscribe to the same topic.
    let (mut ws1, _) = tokio_tungstenite::connect_async(format!("ws://{cloud_addr}/events"))
        .await
        .unwrap();
    let (mut ws2, _) = tokio_tungstenite::connect_async(format!("ws://{cloud_addr}/events"))
        .await
        .unwrap();
    let sub = serde_json::json!({"type": "subscribe", "topic": topic});
    ws1.send(Message::Text(sub.to_string().into()))
        .await
        .unwrap();
    ws2.send(Message::Text(sub.to_string().into()))
        .await
        .unwrap();

    // Drain subscribe-acks.
    for _ in 0..1 {
        let _ = ws1.next().await;
    }
    for _ in 0..1 {
        let _ = ws2.next().await;
    }

    // Let the dedup hub settle.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Both clients share a single underlying stream.
    assert_eq!(
        cloud_streams.active_streams().await,
        1,
        "two subscribers to the same (peer, topic) should share one stream"
    );
    let client = reqwest::Client::new();
    let _ = client
        .post(format!(
            "http://{hub_addr}/resources/{resource_id}/transitions/turn-on"
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    let read = |mut ws: tokio_tungstenite::WebSocketStream<_>| async move {
        let v = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .expect("timeout")
            .unwrap()
            .unwrap();
        match v {
            Message::Text(t) => serde_json::from_str::<Json>(&t).unwrap(),
            _ => panic!(),
        }
    };
    let e1 = read(ws1).await;
    let e2 = read(ws2).await;
    assert_eq!(e1["type"], "event");
    assert_eq!(e2["type"], "event");
    assert_eq!(e1["data"], "on");
    assert_eq!(e2["data"], "on");
}

#[tokio::test]
async fn unsubscribe_tears_down_forwarded_stream() {
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
        .use_actor(ActorLed::default())
        .link(format!("http://{cloud_addr}"))
        .build()
        .unwrap();
    let hub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(async move {
        axum::serve(hub_listener, hub.router).await.unwrap();
    });

    assert!(cloud_acceptors.wait_for_first(Duration::from_secs(5)).await);

    let server: Json = reqwest::get(format!("http://{cloud_addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let resource_id = server["entities"][0]["properties"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let topic = format!("hub/led/{resource_id}/state");

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{cloud_addr}/events"))
        .await
        .unwrap();
    let sub = serde_json::json!({"type": "subscribe", "topic": topic});
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .unwrap();

    // Drain subscribe-ack.
    let ack = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let ack: Json = match ack {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!(),
    };
    let sub_id = ack["subscriptionId"].as_u64().unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(cloud_streams.active_streams().await, 1);

    // Unsubscribe — cloud should drop the H2 body for this stream,
    // which sends RST_STREAM to the hub.
    let unsub = serde_json::json!({"type": "unsubscribe", "subscriptionId": sub_id});
    ws.send(Message::Text(unsub.to_string().into()))
        .await
        .unwrap();

    // Wait for the unsubscribe-ack.
    let _ack = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // After the last subscriber leaves, the (peer, topic) entry should
    // be torn down and the H2 stream cancelled.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        cloud_streams.active_streams().await,
        0,
        "forwarded stream should be torn down after last unsubscribe"
    );
}

#[tokio::test]
async fn cloud_ws_forwards_peer_events() {
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

    assert!(cloud_acceptors.wait_for_first(Duration::from_secs(5)).await);

    // Discover the LED's id via the cloud (which forwards to the hub).
    let server: Json = reqwest::get(format!("http://{cloud_addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let resource_id = server["entities"][0]["properties"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Connect a WS client to the CLOUD's /events.
    let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("ws://{cloud_addr}/events"))
        .await
        .unwrap();

    let topic = format!("hub/led/{resource_id}/state");
    let sub = serde_json::json!({"type": "subscribe", "topic": topic});
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .unwrap();

    // Read subscribe-ack.
    let ack = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let ack: Json = match ack {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!(),
    };
    assert_eq!(ack["type"], "subscribe-ack");

    // Trigger the LED on the HUB directly.
    let client = reqwest::Client::new();
    let _ = client
        .post(format!(
            "http://{hub_addr}/resources/{resource_id}/transitions/turn-on"
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    // The cloud WS should receive the event, forwarded through the tunnel.
    let evt = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for forwarded event")
        .unwrap()
        .unwrap();
    let evt: Json = match evt {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!(),
    };
    assert_eq!(evt["type"], "event", "expected event message, got {evt}");
    assert_eq!(evt["topic"], topic);
    assert_eq!(evt["data"], "on");
}
