//! Characterization tests for the Siren hypermedia crawl.
//!
//! Snapshots the root → server → device → meta surfaces — link rels,
//! action shapes, content types, and embedded sub-entity classes — so
//! that refactors to the renderer cannot regress the wire contract
//! without an explicit test update.

use std::net::SocketAddr;
use std::sync::Arc;

use boardwalk::http::{Core, CoreBuilder, router};
use boardwalk::{Device, DeviceConfig, DeviceError, TransitionInput};
use serde_json::Value as Json;

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
    ) -> futures::future::BoxFuture<'a, Result<(), DeviceError>> {
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

/// Finds the first link whose `rel` array contains *all* of the given rels.
fn find_link_with_rels<'a>(entity: &'a Json, rels: &[&str]) -> Option<&'a Json> {
    entity.get("links")?.as_array()?.iter().find(|link| {
        let link_rels = link
            .get("rel")
            .and_then(|r| r.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        rels.iter().all(|needle| link_rels.contains(needle))
    })
}

#[tokio::test]
async fn root_advertises_self_server_peer_management_events_links() {
    let (addr, _core, _h) = boot().await;
    let body: Json = reqwest::get(format!("http://{addr}/"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["class"], serde_json::json!(["root"]));

    let self_link = find_link_with_rels(&body, &["self"]).expect("self link present");
    let self_href = self_link["href"].as_str().unwrap();
    assert!(
        self_href.ends_with('/'),
        "self href {self_href:?} should end with '/'"
    );

    let server_link = find_link_with_rels(&body, &["https://rels.boardwalk.to/server"])
        .expect("server link present");
    assert_eq!(server_link["title"], "hub");

    let _peer_management = find_link_with_rels(&body, &["https://rels.boardwalk.to/peer-management"])
        .expect("peer-management link present");

    let events_link = find_link_with_rels(&body, &["https://rels.boardwalk.to/events"])
        .expect("events link present");
    let events_href = events_link["href"].as_str().unwrap();
    assert!(
        events_href.starts_with("ws://") || events_href.starts_with("wss://"),
        "events href {events_href:?} should use ws scheme"
    );
}

#[tokio::test]
async fn root_query_devices_action_has_server_and_ql_fields() {
    let (addr, _core, _h) = boot().await;
    let body: Json = reqwest::get(format!("http://{addr}/"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let action = &body["actions"][0];
    assert_eq!(action["name"], "query-devices");
    assert_eq!(action["method"], "GET");
    assert_eq!(action["type"], "application/x-www-form-urlencoded");

    let field_names: Vec<&str> = action["fields"]
        .as_array()
        .expect("fields present")
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(field_names, vec!["server", "ql"]);
}

#[tokio::test]
async fn server_renders_query_register_actions_and_device_entities() {
    let (addr, _core, _h) = boot().await;
    let body: Json = reqwest::get(format!("http://{addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let classes: Vec<&str> = body["class"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(classes.contains(&"server"), "expected `server` class, got {classes:?}");
    assert_eq!(body["properties"]["name"], "hub");

    let actions = body["actions"].as_array().expect("actions present");
    let action_names: Vec<&str> = actions.iter().map(|a| a["name"].as_str().unwrap()).collect();
    assert!(action_names.contains(&"query-devices"));
    assert!(action_names.contains(&"register-device"));

    let entities = body["entities"].as_array().expect("entities present");
    assert_eq!(entities.len(), 1, "expected one embedded device entity");
    let device_classes: Vec<&str> = entities[0]["class"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(device_classes.contains(&"device"));
    assert!(device_classes.contains(&"led"));
}

#[tokio::test]
async fn device_renders_state_gated_transition_action_and_stream_links() {
    let (addr, _core, _h) = boot().await;

    // Discover device id via the server crawl.
    let server: Json = reqwest::get(format!("http://{addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = server["entities"][0]["properties"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let dev: Json = reqwest::get(format!("http://{addr}/servers/hub/devices/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let actions = dev["actions"].as_array().expect("actions present");
    assert_eq!(actions.len(), 1, "expected exactly one allowed transition in 'off' state");
    assert_eq!(actions[0]["name"], "turn-on");
    let hidden = actions[0]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["type"] == "hidden")
        .expect("hidden action field present");
    assert_eq!(hidden["name"], "action");
    assert_eq!(hidden["value"], "turn-on");

    let stream_link = find_link_with_rels(
        &dev,
        &[
            "monitor",
            "https://rels.boardwalk.to/object-stream",
        ],
    )
    .expect("stream link present");
    let href = stream_link["href"].as_str().unwrap();
    let decoded = urlencoding::decode(href).expect("href url-decodes");
    let expected = format!("topic=hub/led/{id}/state");
    assert!(
        decoded.contains(&expected),
        "stream href {decoded:?} should contain {expected:?}"
    );
}

#[tokio::test]
async fn meta_collection_renders_type_subentities_with_streams_and_transitions() {
    let (addr, _core, _h) = boot().await;
    let meta: Json = reqwest::get(format!("http://{addr}/servers/hub/meta"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let entities = meta["entities"].as_array().expect("meta entities present");
    let type_entity = entities
        .iter()
        .find(|e| {
            e["class"]
                .as_array()
                .map(|arr| arr.iter().any(|v| v == "type"))
                .unwrap_or(false)
        })
        .expect("at least one `type` sub-entity");

    assert_eq!(type_entity["properties"]["type"], "led");

    let streams: Vec<&str> = type_entity["properties"]["streams"]
        .as_array()
        .expect("streams array")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(
        streams.contains(&"state"),
        "expected stream `state`, got {streams:?}"
    );

    let transitions = type_entity["properties"]["transitions"]
        .as_array()
        .expect("transitions array");
    assert!(!transitions.is_empty(), "expected at least one transition");
    for t in transitions {
        assert!(
            t.get("name").and_then(|v| v.as_str()).is_some(),
            "each transition should have a `name` field, got {t:?}"
        );
    }
}

#[tokio::test]
#[allow(non_snake_case)]
async fn meta_type_endpoint_returns_minimal_self_only_entity__pending_replacement() {
    // Documents the current parity loss between `/meta` and
    // `/meta/{type}`: the per-type endpoint omits streams and
    // transitions and exposes only a `self` link. Assertions are
    // intentionally tight so that bringing the per-type endpoint to
    // parity with the collection breaks this test loudly.
    let (addr, _core, _h) = boot().await;
    let body: Json = reqwest::get(format!("http://{addr}/servers/hub/meta/led"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["class"], serde_json::json!(["type"]));
    assert_eq!(body["properties"]["type"], "led");

    let links = body["links"].as_array().expect("links array");
    assert_eq!(links.len(), 1, "expected exactly one link, got {links:?}");
    let only_link_rels: Vec<&str> = links[0]["rel"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(only_link_rels, vec!["self"]);
}
