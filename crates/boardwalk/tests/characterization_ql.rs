//! Characterization tests for the current `?ql=` and `ServerHandle::query`
//! behaviors.
//!
//! Three of these tests carry the `__pending_replacement` suffix: they
//! lock in behaviors that are explicitly slated for replacement (the
//! HTTP swallow-on-error, the app-side swallow-on-error, and the
//! query target's narrow projection). Replacing them will require
//! intentional updates to these tests *plus* a paired source change
//! — that is the point.

use std::net::SocketAddr;
use std::sync::Arc;

use boardwalk::http::{Core, CoreBuilder, router};
use boardwalk::{Device, DeviceConfig, DeviceError, ServerHandle, TransitionInput};
use serde_json::{Map, Value as Json};

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

/// LED variant that publishes an extra `color` property. Used to prove
/// that the current query target does NOT see `properties`.
struct ColoredLed {
    color: &'static str,
}

impl Device for ColoredLed {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led").name("LED").state("off");
    }
    fn state(&self) -> &str {
        "off"
    }
    fn properties(&self) -> Map<String, Json> {
        let mut m = Map::new();
        m.insert("color".into(), Json::String(self.color.into()));
        m
    }
    fn transition<'a>(
        &'a mut self,
        _name: &'a str,
        _input: TransitionInput,
    ) -> futures::future::BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async move { Err(DeviceError::Invalid("no transitions".into())) })
    }
}

async fn boot_with<D: Device + 'static>(device: D) -> (SocketAddr, Arc<Core>) {
    let mut b = CoreBuilder::new("hub");
    b.add_device(device);
    let core = b.build();
    let app = router(core.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, core)
}

#[tokio::test]
async fn ql_with_matching_predicate_returns_search_results_with_entities() {
    let (addr, _core) = boot_with(Led::default()).await;
    let url = format!(
        "http://{addr}/servers/hub?ql={}",
        urlencoding::encode(r#"where type = "led""#)
    );
    let body: Json = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let classes: Vec<&str> = body["class"]
        .as_array()
        .expect("class array")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(classes.contains(&"server"));
    assert!(classes.contains(&"search-results"));
    let entities = body["entities"]
        .as_array()
        .expect("matching ql should populate entities");
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0]["properties"]["type"], "led");
}

#[tokio::test]
async fn ql_with_non_matching_predicate_returns_search_results_with_no_entities() {
    let (addr, _core) = boot_with(Led::default()).await;
    let url = format!(
        "http://{addr}/servers/hub?ql={}",
        urlencoding::encode(r#"where type = "motion""#)
    );
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Json = resp.json().await.unwrap();
    let classes: Vec<&str> = body["class"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(classes.contains(&"search-results"));
    // Either an absent `entities` field or an empty array is allowed —
    // both are the same "no matches" rendering today.
    let entities = body.get("entities").and_then(|v| v.as_array());
    assert!(
        entities.map(|a| a.is_empty()).unwrap_or(true),
        "expected no entities, got {entities:?}"
    );
}

#[tokio::test]
async fn invalid_ql_returns_400_with_structured_body() {
    let (addr, _core) = boot_with(Led::default()).await;
    let url = format!(
        "http://{addr}/servers/hub?ql={}",
        urlencoding::encode("where type =")
    );
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 400);
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["error"], "query-parse-error");
    assert!(body["message"].is_string(), "expected message string");
    assert!(body["ql"].is_string(), "expected ql echoed in body");
}

#[tokio::test]
async fn app_query_returns_err_for_invalid_ql() {
    let (_addr, core) = boot_with(Led::default()).await;
    let server = ServerHandle::new_internal(core);
    let result = server.query("where ===bogus===").await;
    assert!(
        result.is_err(),
        "invalid ql should surface as Err, not be silently swallowed"
    );
}

#[tokio::test]
async fn app_query_uses_resource_snapshot_target() {
    let (_addr, core) = boot_with(ColoredLed { color: "red" }).await;
    let server = ServerHandle::new_internal(core);
    let matches = server
        .query(r#"where properties.color = "red""#)
        .await
        .expect("query parses");
    assert_eq!(matches.len(), 1);
}

#[tokio::test]
async fn query_can_match_extra_device_properties() {
    let (addr, _core) = boot_with(ColoredLed { color: "red" }).await;

    // The new query target exposes device properties under a
    // `properties` subobject, so `properties.color` resolves.
    let url = format!(
        "http://{addr}/servers/hub?ql={}",
        urlencoding::encode(r#"where properties.color = "red""#)
    );
    let body: Json = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let entities = body["entities"]
        .as_array()
        .expect("matching property query should populate entities");
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0]["properties"]["color"], "red");
}

/// Pins the `?ql=` parameter as the only way the server consumes a
/// CaQL predicate. Later resource-route work will swap `/servers/...`
/// for `/resources`, but the `ql` parameter name must survive.
#[tokio::test]
async fn current_server_query_uses_ql_parameter() {
    let (addr, _core) = boot_with(Led::default()).await;
    let url = format!(
        "http://{addr}/servers/hub?ql={}",
        urlencoding::encode(r#"where state = "off""#)
    );
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Json = resp.json().await.unwrap();
    let classes: Vec<&str> = body["class"]
        .as_array()
        .expect("class array")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(
        classes.contains(&"server") && classes.contains(&"search-results"),
        "expected server+search-results, got {classes:?}"
    );
    assert_eq!(body["properties"]["ql"], r#"where state = "off""#);
    let entities = body["entities"]
        .as_array()
        .expect("entities array for matching query");
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0]["properties"]["state"], "off");
}

/// Pins the unused `server` field on the root `query-devices` action —
/// the field exists in the form today but the route handler ignores it.
/// Removing the field is a route surface change that needs an explicit
/// test update.
#[tokio::test]
async fn current_root_query_action_carries_unused_server_field() {
    let (addr, _core) = boot_with(Led::default()).await;
    let body: Json = reqwest::get(format!("http://{addr}/"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let action = body["actions"]
        .as_array()
        .and_then(|arr| arr.iter().find(|a| a["name"] == "query-devices"))
        .expect("query-devices action on root");
    let field_names: Vec<&str> = action["fields"]
        .as_array()
        .expect("fields array")
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        field_names,
        vec!["server", "ql"],
        "root action still advertises both `server` (unused) and `ql`"
    );
}

/// Pins the dead `query` stream link that search-results currently
/// advertises. The href points at the server-wide events socket with a
/// `topic=query/{ql}` parameter, but nothing publishes on that topic.
/// Phase 5 will remove the link and the corresponding renderer code.
#[tokio::test]
async fn current_search_results_advertise_dead_query_stream_link() {
    let (addr, _core) = boot_with(Led::default()).await;
    let ql = r#"where kind = "led""#;
    let url = format!("http://{addr}/servers/hub?ql={}", urlencoding::encode(ql));
    let body: Json = reqwest::get(&url).await.unwrap().json().await.unwrap();

    let links = body["links"].as_array().expect("links array");
    let query_link = links
        .iter()
        .find(|l| {
            l["rel"]
                .as_array()
                .map(|rs| rs.iter().any(|r| r == "https://rels.boardwalk.to/query"))
                .unwrap_or(false)
        })
        .expect("dead query stream link present on search results");
    let href = query_link["href"].as_str().unwrap();
    assert!(
        href.starts_with("ws://") || href.starts_with("wss://"),
        "query stream link uses ws scheme; got {href}"
    );
    // The href is form-encoded (spaces become `+`); compare the
    // query/<ql> prefix on the form-decoded form.
    let after_topic = href
        .split_once("?topic=")
        .map(|(_, q)| q)
        .expect("?topic= query param present");
    let topic_form_decoded = after_topic.replace('+', " ");
    let topic_url_decoded = urlencoding::decode(&topic_form_decoded).expect("topic url-decodes");
    let expected = format!("query/{ql}");
    assert_eq!(
        topic_url_decoded, expected,
        "query stream topic should be query/<ql>"
    );
}

#[tokio::test]
async fn query_with_kind_alias_still_uses_type_keyword() {
    // `where type = "led"` continues to work via the `type → kind`
    // alias in the evaluator's path lookup.
    let (addr, _core) = boot_with(Led::default()).await;
    let url = format!(
        "http://{addr}/servers/hub?ql={}",
        urlencoding::encode(r#"where type = "led""#)
    );
    let body: Json = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let entities = body["entities"]
        .as_array()
        .expect("kind alias should resolve `type = led`");
    assert_eq!(entities.len(), 1);
}
