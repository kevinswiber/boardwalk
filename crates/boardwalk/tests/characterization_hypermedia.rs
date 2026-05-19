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
use serde_json::{Value as Json, json};
use uuid::Uuid;

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

/// Same as `boot` but pins the LED device id, so absolute hrefs are
/// byte-stable across runs. Used by the survivor snapshot tests below.
async fn boot_pinned(device_id: Uuid) -> (SocketAddr, Arc<Core>, tokio::task::JoinHandle<()>) {
    let mut b = CoreBuilder::new("hub");
    b.add_device_with_id(device_id, Led::default());
    let core = b.build();
    let app = router(core.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, core, handle)
}

/// Replace the random TCP port in every string value with `PORT` so a
/// snapshot can match any boot. Topological structure — array order,
/// object keys, every other byte — is preserved.
fn normalize_port(value: &Json, port: u16) -> Json {
    let needles = [format!("127.0.0.1:{port}"), format!("127.0.0.1%3A{port}")];
    fn walk(v: &Json, needles: &[String]) -> Json {
        match v {
            Json::String(s) => {
                let mut out = s.clone();
                for n in needles {
                    out = out.replace(n, "127.0.0.1:PORT");
                }
                Json::String(out)
            }
            Json::Array(arr) => Json::Array(arr.iter().map(|x| walk(x, needles)).collect()),
            Json::Object(obj) => Json::Object(
                obj.iter()
                    .map(|(k, val)| (k.clone(), walk(val, needles)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }
    walk(value, &needles)
}

async fn fetch_json(addr: SocketAddr, path: &str) -> Json {
    reqwest::get(format!("http://{addr}{path}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Finds the first link whose `rel` array contains *all* of the given rels.
fn find_link_with_rels<'a>(entity: &'a Json, rels: &[&str]) -> Option<&'a Json> {
    entity.get("links")?.as_array()?.iter().find(|link| {
        let link_rels = link
            .get("rel")
            .and_then(|r| r.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
            .unwrap_or_default();
        rels.iter().all(|needle| link_rels.contains(needle))
    })
}

#[tokio::test]
async fn root_links_to_local_resources() {
    let (addr, _core, _h) = boot().await;
    let body: Json = reqwest::get(format!("http://{addr}/"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let resources_link = find_link_with_rels(&body, &["https://rels.boardwalk.to/resources"])
        .expect("resources link present");
    assert_eq!(
        resources_link["href"].as_str().unwrap(),
        format!("http://{addr}/resources")
    );
}

#[tokio::test]
async fn resources_collection_renders_resource_entities() {
    let (addr, _core, _h) = boot().await;
    let collection: Json = reqwest::get(format!("http://{addr}/resources"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let classes: Vec<&str> = collection["class"]
        .as_array()
        .expect("class array")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(
        classes.contains(&"resources"),
        "expected resources class, got {classes:?}"
    );

    let entities = collection["entities"]
        .as_array()
        .expect("resource entities present");
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0]["properties"]["node"], "hub");
    let resource_id = entities[0]["properties"]["id"].as_str().unwrap();
    assert_eq!(
        entities[0]["links"][0]["href"].as_str().unwrap(),
        format!("http://{addr}/resources/{resource_id}")
    );

    let resource: Json = reqwest::get(format!("http://{addr}/resources/{resource_id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resource["properties"]["node"], "hub");
}

#[tokio::test]
async fn old_device_routes_return_404_after_resource_switch() {
    let (addr, core, _h) = boot().await;
    let id = core.list_devices().await[0].id;

    let devices = reqwest::get(format!("http://{addr}/servers/hub/devices"))
        .await
        .unwrap();
    assert_eq!(devices.status(), 404);

    let device = reqwest::get(format!("http://{addr}/servers/hub/devices/{id}"))
        .await
        .unwrap();
    assert_eq!(device.status(), 404);
}

#[tokio::test]
async fn unknown_server_routes_do_not_fall_through_to_local() {
    let (addr, core, _h) = boot().await;
    let id = core.list_devices().await[0].id;
    let topic = urlencoding::encode(&format!("hub/led/{id}/state")).to_string();
    let paths = [
        "/servers/other".to_string(),
        "/servers/other/resources".to_string(),
        format!("/servers/other/resources/{id}"),
        "/servers/other/meta".to_string(),
        "/servers/other/meta/led".to_string(),
        format!("/servers/other/events?topic={topic}"),
    ];

    for path in paths {
        let resp = reqwest::get(format!("http://{addr}{path}")).await.unwrap();
        assert_eq!(
            resp.status(),
            404,
            "unknown server path {path:?} should not render local content"
        );
    }
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

    let _peer_management =
        find_link_with_rels(&body, &["https://rels.boardwalk.to/peer-management"])
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
async fn root_query_resources_action_has_ql_field() {
    let (addr, _core, _h) = boot().await;
    let body: Json = reqwest::get(format!("http://{addr}/"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let action = &body["actions"][0];
    assert_eq!(action["name"], "query-resources");
    assert_eq!(action["method"], "GET");
    assert_eq!(action["href"], format!("http://{addr}/resources"));
    assert_eq!(action["type"], "application/x-www-form-urlencoded");

    let field_names: Vec<&str> = action["fields"]
        .as_array()
        .expect("fields present")
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(field_names, vec!["ql"]);
}

#[tokio::test]
async fn server_renders_resource_actions_and_resource_entities() {
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
    assert!(
        classes.contains(&"server"),
        "expected `server` class, got {classes:?}"
    );
    assert_eq!(body["properties"]["name"], "hub");

    let actions = body["actions"].as_array().expect("actions present");
    let action_names: Vec<&str> = actions
        .iter()
        .map(|a| a["name"].as_str().unwrap())
        .collect();
    assert!(action_names.contains(&"query-resources"));
    assert!(action_names.contains(&"register-resource"));
    assert!(
        find_link_with_rels(&body, &["monitor"]).is_none(),
        "server view should not advertise an unfiltered monitor link"
    );

    let entities = body["entities"].as_array().expect("entities present");
    assert_eq!(entities.len(), 1, "expected one embedded resource entity");
    let resource_classes: Vec<&str> = entities[0]["class"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(resource_classes.contains(&"resource"));
    assert!(resource_classes.contains(&"led"));
}

#[tokio::test]
async fn resource_renders_state_gated_transition_action_and_stream_links() {
    let (addr, _core, _h) = boot().await;

    let resources: Json = reqwest::get(format!("http://{addr}/resources"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = resources["entities"][0]["properties"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let dev: Json = reqwest::get(format!("http://{addr}/resources/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let actions = dev["actions"].as_array().expect("actions present");
    assert_eq!(
        actions.len(),
        1,
        "expected exactly one allowed transition in 'off' state"
    );
    assert_eq!(actions[0]["name"], "turn-on");
    assert_eq!(actions[0]["type"], "application/json");
    assert_eq!(
        actions[0]["href"],
        format!("http://{addr}/resources/{id}/transitions/turn-on")
    );

    let stream_link = find_link_with_rels(
        &dev,
        &["monitor", "https://rels.boardwalk.to/object-stream"],
    )
    .expect("stream link present");
    let href = stream_link["href"].as_str().unwrap();
    let decoded = urlencoding::decode(href).expect("href url-decodes");
    let expected = format!("topic=hub/led/{id}/state");
    assert!(
        decoded.contains(&expected),
        "stream href {decoded:?} should contain {expected:?}"
    );
    assert!(
        decoded.starts_with(&format!("ws://{addr}/events")),
        "stream href {decoded:?} should use the local events route"
    );
}

#[tokio::test]
async fn meta_collection_renders_type_subentities_with_streams_and_transitions() {
    let (addr, _core, _h) = boot().await;
    let meta: Json = reqwest::get(format!("http://{addr}/meta"))
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

    assert_eq!(type_entity["properties"]["kind"], "led");
    assert!(type_entity["properties"].get("type").is_none());

    let streams: Vec<&str> = type_entity["properties"]["streams"]
        .as_array()
        .expect("streams array")
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect();
    assert!(
        streams.contains(&"state"),
        "expected stream `state`, got {streams:?}"
    );

    let transitions = type_entity["properties"]["transitions"]
        .as_array()
        .expect("transitions array");
    let names: Vec<&str> = transitions
        .iter()
        .map(|t| t["name"].as_str().expect("each transition has a name"))
        .collect();
    // Metadata is type-level — the full transition surface must be
    // visible regardless of the LED's current state.
    assert!(
        names.contains(&"turn-on"),
        "expected `turn-on` in meta transitions, got {names:?}"
    );
    assert!(
        names.contains(&"turn-off"),
        "expected `turn-off` in meta transitions, got {names:?}"
    );
}

#[tokio::test]
async fn current_resource_siren_crawl_is_byte_stable() {
    let device_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    let (addr, _core, _h) = boot_pinned(device_id).await;
    let port = addr.port();
    let id = device_id.to_string();

    let root = normalize_port(&fetch_json(addr, "/").await, port);
    let resources = normalize_port(&fetch_json(addr, "/resources").await, port);
    let resource = normalize_port(&fetch_json(addr, &format!("/resources/{id}")).await, port);
    let meta = normalize_port(&fetch_json(addr, "/meta").await, port);

    let expected_root = json!({
        "class": ["root"],
        "links": [
            {"rel": ["self"], "href": "http://127.0.0.1:PORT/"},
            {"rel": ["https://rels.boardwalk.to/resources"], "href": "http://127.0.0.1:PORT/resources"},
            {
                "rel": ["https://rels.boardwalk.to/server"],
                "href": "http://127.0.0.1:PORT/servers/hub",
                "title": "hub"
            },
            {
                "rel": ["https://rels.boardwalk.to/peer-management"],
                "href": "http://127.0.0.1:PORT/peer-management"
            },
            {
                "rel": ["https://rels.boardwalk.to/events"],
                "href": "ws://127.0.0.1:PORT/events"
            }
        ],
        "actions": [{
            "name": "query-resources",
            "method": "GET",
            "href": "http://127.0.0.1:PORT/resources",
            "type": "application/x-www-form-urlencoded",
            "fields": [{"name": "ql", "type": "text"}]
        }]
    });

    let expected_resource_sub_entity = json!({
        "class": ["resource", "led"],
        "rel": ["https://rels.boardwalk.to/resource"],
        "properties": {
            "id": "11111111-2222-3333-4444-555555555555",
            "kind": "led",
            "name": "LED",
            "node": "hub",
            "state": "off"
        },
        "links": [
            {"rel": ["self"], "href": "http://127.0.0.1:PORT/resources/11111111-2222-3333-4444-555555555555"},
            {"rel": ["up", "https://rels.boardwalk.to/resources"], "href": "http://127.0.0.1:PORT/resources"}
        ]
    });

    let expected_resources = json!({
        "class": ["resources"],
        "properties": {"node": "hub"},
        "entities": [expected_resource_sub_entity],
        "links": [{"rel": ["self"], "href": "http://127.0.0.1:PORT/resources"}],
        "actions": [
            {
                "name": "query-resources",
                "method": "GET",
                "href": "http://127.0.0.1:PORT/resources",
                "type": "application/x-www-form-urlencoded",
                "fields": [{"name": "ql", "type": "text"}]
            },
            {
                "name": "register-resource",
                "method": "POST",
                "href": "http://127.0.0.1:PORT/resources",
                "type": "application/x-www-form-urlencoded",
                "fields": [
                    {"name": "kind", "type": "text"},
                    {"name": "id", "type": "text"},
                    {"name": "name", "type": "text"}
                ]
            }
        ]
    });

    let expected_resource = json!({
        "class": ["resource", "led"],
        "properties": {
            "id": "11111111-2222-3333-4444-555555555555",
            "kind": "led",
            "name": "LED",
            "node": "hub",
            "state": "off"
        },
        "links": [
            {"rel": ["self", "edit"], "href": "http://127.0.0.1:PORT/resources/11111111-2222-3333-4444-555555555555"},
            {"rel": ["up", "https://rels.boardwalk.to/resources"], "href": "http://127.0.0.1:PORT/resources"},
            {
                "rel": ["https://rels.boardwalk.to/type", "describedby"],
                "href": "http://127.0.0.1:PORT/meta/led"
            },
            {
                "rel": ["monitor", "https://rels.boardwalk.to/object-stream"],
                "href": "ws://127.0.0.1:PORT/events?topic=hub%2Fled%2F11111111-2222-3333-4444-555555555555%2Fstate",
                "title": "state"
            }
        ],
        "actions": [{
            "name": "turn-on",
            "method": "POST",
            "href": "http://127.0.0.1:PORT/resources/11111111-2222-3333-4444-555555555555/transitions/turn-on",
            "type": "application/json",
            "class": ["transition"]
        }]
    });

    let expected_meta = json!({
        "class": ["metadata"],
        "properties": {"name": "hub"},
        "entities": [{
            "class": ["type"],
            "rel": ["https://rels.boardwalk.to/type", "item"],
            "properties": {
                "kind": "led",
                "name": "LED",
                "labels": {},
                "propertySchema": null,
                "properties": ["id", "kind", "node", "state"],
                "streams": [{"name": "state", "kind": "object"}],
                "transitions": [
                    {"name": "turn-off", "allowedStates": ["on"], "result": "sync", "idempotency": "none", "effect": "unsafe", "requiredScopes": []},
                    {"name": "turn-on", "allowedStates": ["off"], "result": "sync", "idempotency": "none", "effect": "unsafe", "requiredScopes": []}
                ]
            },
            "links": [
                {
                    "rel": ["self"],
                    "href": "http://127.0.0.1:PORT/meta/led"
                }
            ]
        }],
        "links": [
            {"rel": ["self"], "href": "http://127.0.0.1:PORT/meta"},
            {"rel": ["https://rels.boardwalk.to/resources"], "href": "http://127.0.0.1:PORT/resources"},
            {"rel": ["monitor"], "href": "ws://127.0.0.1:PORT/events?topic=meta"}
        ]
    });

    assert_eq!(root, expected_root, "root snapshot");
    assert_eq!(
        resources, expected_resources,
        "resources collection snapshot"
    );
    assert_eq!(resource, expected_resource, "resource snapshot");
    assert_eq!(meta, expected_meta, "meta collection snapshot");
}

#[tokio::test]
async fn meta_type_endpoint_returns_full_kind_metadata() {
    let (addr, _core, _h) = boot().await;
    let body: Json = reqwest::get(format!("http://{addr}/meta/led"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["class"], serde_json::json!(["type"]));
    assert_eq!(body["properties"]["kind"], "led");
    assert!(body["properties"].get("type").is_none());
    assert_eq!(
        body["properties"]["streams"],
        serde_json::json!([{"name": "state", "kind": "object"}])
    );
    let transitions = body["properties"]["transitions"]
        .as_array()
        .expect("transitions array");
    let transition_names: Vec<&str> = transitions
        .iter()
        .map(|transition| transition["name"].as_str().unwrap())
        .collect();
    assert!(transition_names.contains(&"turn-on"));
    assert!(transition_names.contains(&"turn-off"));

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
