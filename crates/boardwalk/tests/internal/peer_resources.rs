//! Contract tests for peer resource forwarding and shared upstream streams.
//!
//! Cloud advertises hub as a peer, forwarded resource GETs render cloud-facing
//! hrefs, peer subscriptions to the same topic share one upstream stream, and
//! the upstream is torn down when the last subscriber leaves.

use std::net::SocketAddr;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use http::StatusCode;
use serde_json::{Value as Json, json};
use tokio_tungstenite::tungstenite::Message;

use super::actor_led_fixture::ActorLed;
use crate::Boardwalk;
use crate::http::PeerStreamHub;
use crate::peer::{PeerAdmissionConfig, PeerLinkConfig};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Pair {
    cloud_addr: SocketAddr,
    hub_addr: SocketAddr,
    cloud_streams: PeerStreamHub,
}

/// Boots cloud + hub (with one LED) and waits until cloud has confirmed
/// the peer link.
async fn boot_pair() -> Pair {
    boot_pair_with_capabilities(None::<[&str; 0]>).await
}

async fn boot_cloud() -> SocketAddr {
    let cloud = Boardwalk::new().name("cloud").build().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, cloud.router).await.unwrap();
    });
    addr
}

async fn boot_pair_with_capabilities<I, S>(capabilities: Option<I>) -> Pair
where
    I: IntoIterator<Item = S> + Clone,
    S: AsRef<str>,
{
    let capability_names = capabilities.map(|names| {
        names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>()
    });
    let cloud = Boardwalk::new().name("cloud");
    let cloud = if let Some(capabilities) = capability_names.as_ref() {
        let admission = PeerAdmissionConfig::shared_token("hub", "kid-1", "secret")
            .unwrap()
            .allow(capabilities)
            .unwrap();
        cloud.accept_peer_admission_config(admission)
    } else {
        cloud
    }
    .build()
    .unwrap();
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    let cloud_streams = cloud.peer_streams.clone();
    tokio::spawn(async move {
        axum::serve(cloud_listener, cloud.router).await.unwrap();
    });

    let hub = Boardwalk::new().name("hub").use_actor(ActorLed::default());
    let hub = if let Some(capabilities) = capability_names.as_ref() {
        hub.link_peer(
            PeerLinkConfig::new(format!("http://{cloud_addr}"), "hub")
                .unwrap()
                .token("kid-1", "secret")
                .node_name("Kitchen Hub")
                .request_capabilities(capabilities)
                .unwrap(),
        )
    } else {
        hub.link(format!("http://{cloud_addr}"))
    }
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

#[tokio::test]
async fn peer_read_without_resource_read_is_403() {
    let p = boot_pair_with_capabilities(Some(["stream.subscribe"])).await;

    let response = reqwest::get(format!("http://{}/servers/hub/resources", p.cloud_addr))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn peer_management_is_hidden_without_peer_admin() {
    let addr = boot_cloud().await;

    let response = reqwest::get(format!("http://{addr}/peer-management"))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn root_does_not_link_peer_management_without_peer_admin() {
    let addr = boot_cloud().await;
    let root: Json = reqwest::get(format!("http://{addr}/"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(!links(&root).any(link_has_peer_management_rel));
}

#[tokio::test]
async fn root_does_not_render_peer_link_without_resource_read() {
    let p = boot_pair_with_capabilities(Some(["stream.subscribe"])).await;
    let root: Json = reqwest::get(format!("http://{}/", p.cloud_addr))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(!links(&root).any(link_has_peer_server_rel));
}

#[tokio::test]
async fn transition_forward_requires_transition_invoke() {
    let p = boot_pair_with_capabilities(Some(["resource.read"])).await;
    let id = resource_id_via(p.cloud_addr).await;

    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/servers/hub/resources/{id}/transitions/turn-on",
            p.cloud_addr
        ))
        .json(&json!({}))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let direct: Json = reqwest::get(format!("http://{}/resources/{id}", p.hub_addr))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(direct["properties"]["state"], "off");
}

#[tokio::test]
async fn remote_resource_suppresses_transition_actions_without_invoke_capability() {
    let p = boot_pair_with_capabilities(Some(["resource.read"])).await;
    let id = resource_id_via(p.cloud_addr).await;

    let resource: Json = reqwest::get(format!(
        "http://{}/servers/hub/resources/{id}",
        p.cloud_addr
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();

    assert!(actions(&resource).all(|action| action["class"] != json!(["transition"])));
}

#[tokio::test]
async fn remote_resource_suppresses_stream_links_without_subscribe_capability() {
    let p = boot_pair_with_capabilities(Some(["resource.read"])).await;
    let id = resource_id_via(p.cloud_addr).await;

    let resource: Json = reqwest::get(format!(
        "http://{}/servers/hub/resources/{id}",
        p.cloud_addr
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();

    assert!(!links(&resource).any(link_has_monitor_rel));
}

#[tokio::test]
async fn directed_peer_query_requires_resource_query_capability() {
    let p = boot_pair_with_capabilities(Some(["resource.read"])).await;

    let response = reqwest::get(format!(
        "http://{}/servers/hub?ql=where%20kind%20%3D%20%22led%22",
        p.cloud_addr
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn directed_peer_query_forwards_to_one_peer_when_allowed() {
    let p = boot_pair_with_capabilities(Some(["resource.read", "resource.query"])).await;

    let body: Json = reqwest::get(format!(
        "http://{}/servers/hub?ql=where%20kind%20%3D%20%22led%22",
        p.cloud_addr
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();

    assert_eq!(body["properties"]["ql"], "where kind = \"led\"");
    assert!(!body["entities"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn directed_peer_query_without_resource_read_suppresses_resource_read_links() {
    let p = boot_pair_with_capabilities(Some(["resource.query"])).await;

    let response = reqwest::get(format!(
        "http://{}/servers/hub?ql=where%20kind%20%3D%20%22led%22",
        p.cloud_addr
    ))
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Json = response.json().await.unwrap();

    let entities = body["entities"].as_array().expect("query entities");
    assert!(!entities.is_empty());
    for entity in entities {
        assert!(
            !links(entity).any(link_has_resource_read_rel),
            "query-only peer results should not advertise resource-read links: {entity}"
        );
    }
}

#[tokio::test]
async fn wildcard_federation_query_requires_explicit_policy_and_limit() {
    let p = boot_pair().await;

    let response = reqwest::get(format!(
        "http://{}/?server=*&ql=where%20kind%20%3D%20%22led%22",
        p.cloud_addr
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn peer_ndjson_stream_requires_stream_subscribe_capability() {
    let p = boot_pair_with_capabilities(Some(["resource.read"])).await;
    let id = resource_id_via(p.cloud_addr).await;
    let topic = format!("hub/led/{id}/state");

    let response = reqwest::get(format!(
        "http://{}/servers/hub/events?topic={topic}",
        p.cloud_addr
    ))
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(p.cloud_streams.active_streams().await, 0);
}

#[tokio::test]
async fn ws_peer_topic_requires_stream_subscribe_capability() {
    let p = boot_pair_with_capabilities(Some(["resource.read"])).await;
    let id = resource_id_via(p.cloud_addr).await;
    let topic = format!("hub/led/{id}/state");
    let mut ws = open_ws(p.cloud_addr).await;

    send_json(&mut ws, json!({"type": "subscribe", "topic": topic})).await;
    let denial = recv_json(&mut ws, Duration::from_secs(2)).await;

    assert_eq!(denial["type"], "error");
    assert_eq!(denial["code"], 403);
    assert_eq!(p.cloud_streams.active_streams().await, 0);
}

async fn resource_id_via(addr: SocketAddr) -> String {
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
async fn forwarded_resource_get_renders_cloud_external_hrefs() {
    let p = boot_pair().await;
    let id = resource_id_via(p.cloud_addr).await;

    let via_cloud: Json = reqwest::get(format!(
        "http://{}/servers/hub/resources/{id}",
        p.cloud_addr
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    let direct: Json = reqwest::get(format!("http://{}/resources/{id}", p.hub_addr))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(via_cloud["properties"], direct["properties"]);
    assert_eq!(via_cloud["properties"]["fixture"], "actor-led");
    assert_eq!(via_cloud["class"], direct["class"]);
    assert_all_hrefs_start_with(&via_cloud, &format!("http://{}/servers/hub", p.cloud_addr));
    assert_all_ws_hrefs_start_with(&via_cloud, &format!("ws://{}/servers/hub", p.cloud_addr));
}

#[tokio::test]
async fn forwarded_collection_root_and_event_links_use_cloud_external_origin() {
    let p = boot_pair().await;
    let collection: Json = reqwest::get(format!("http://{}/servers/hub/resources", p.cloud_addr))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(collection["class"], json!(["resources"]));
    assert_all_hrefs_start_with(&collection, &format!("http://{}/servers/hub", p.cloud_addr));
    assert_all_ws_hrefs_start_with(&collection, &format!("ws://{}/servers/hub", p.cloud_addr));
}

fn assert_all_hrefs_start_with(entity: &Json, base: &str) {
    for href in hrefs(entity) {
        if href.starts_with("ws://") || href.starts_with("wss://") {
            continue;
        }
        assert!(
            href.starts_with(base),
            "expected http href {href:?} to start with {base:?}; body: {entity}"
        );
    }
}

fn assert_all_ws_hrefs_start_with(entity: &Json, base: &str) {
    for href in hrefs(entity) {
        if !(href.starts_with("ws://") || href.starts_with("wss://")) {
            continue;
        }
        assert!(
            href.starts_with(base),
            "expected ws href {href:?} to start with {base:?}; body: {entity}"
        );
    }
}

fn hrefs(entity: &Json) -> Vec<&str> {
    let mut out = Vec::new();
    collect_hrefs(entity, &mut out);
    out
}

fn links(entity: &Json) -> impl Iterator<Item = &Json> {
    entity["links"].as_array().into_iter().flatten()
}

fn actions(entity: &Json) -> impl Iterator<Item = &Json> {
    entity["actions"].as_array().into_iter().flatten()
}

fn link_has_peer_server_rel(link: &Json) -> bool {
    link_rels(link).contains(&"https://rels.boardwalk.to/peer")
        && link_rels(link).contains(&"https://rels.boardwalk.to/server")
}

fn link_has_monitor_rel(link: &Json) -> bool {
    link_rels(link).contains(&"monitor")
        || link_rels(link).contains(&"https://rels.boardwalk.to/object-stream")
}

fn link_has_resource_read_rel(link: &Json) -> bool {
    let rels = link_rels(link);
    rels.contains(&"self")
        || rels.contains(&"up")
        || rels.contains(&"https://rels.boardwalk.to/resources")
}

fn link_has_peer_management_rel(link: &Json) -> bool {
    link_rels(link).contains(&"https://rels.boardwalk.to/peer-management")
}

fn link_rels(link: &Json) -> Vec<&str> {
    link["rel"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|rel| rel.as_str())
        .collect()
}

fn collect_hrefs<'a>(value: &'a Json, out: &mut Vec<&'a str>) {
    match value {
        Json::Object(map) => {
            if let Some(href) = map.get("href").and_then(|href| href.as_str()) {
                out.push(href);
            }
            for value in map.values() {
                collect_hrefs(value, out);
            }
        }
        Json::Array(values) => {
            for value in values {
                collect_hrefs(value, out);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn forwarded_event_stream_shares_one_upstream_per_topic() {
    let p = boot_pair().await;
    let id = resource_id_via(p.cloud_addr).await;
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
        .post(format!(
            "http://{}/resources/{id}/transitions/turn-on",
            p.hub_addr
        ))
        .json(&json!({}))
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
    let id = resource_id_via(p.cloud_addr).await;
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
