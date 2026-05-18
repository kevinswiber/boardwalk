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
#[allow(non_snake_case)]
async fn current_invalid_ql_returns_empty_search_results__pending_replacement() {
    // CONTRACT TO BREAK: the HTTP `?ql=` endpoint currently swallows
    // CaQL parse errors and renders an empty search-results entity
    // with status 200. The structured-error refactor will replace
    // this with a 400 response carrying a parse-error body — at that
    // point this assertion should be updated together with the
    // source change.
    let (addr, _core) = boot_with(Led::default()).await;
    let url = format!(
        "http://{addr}/servers/hub?ql={}",
        urlencoding::encode("where type =")
    );
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 200, "swallow-on-error keeps status at 200");
    let body: Json = resp.json().await.unwrap();
    let entities = body.get("entities").and_then(|v| v.as_array());
    assert!(
        entities.map(|a| a.is_empty()).unwrap_or(true),
        "invalid ql is currently rendered as an empty result, got {entities:?}"
    );
}

#[tokio::test]
#[allow(non_snake_case)]
async fn current_app_query_returns_empty_for_invalid_ql__pending_replacement() {
    // CONTRACT TO BREAK: `ServerHandle::query` currently logs a
    // warning and returns an empty `Vec<DeviceProxy>` on parse error.
    // The refactor will flip this to a `Result<Vec<DeviceProxy>>` so
    // apps can react explicitly.
    let (_addr, core) = boot_with(Led::default()).await;
    let server = ServerHandle::new_internal(core);
    let results = server.query("where ===bogus===").await;
    assert!(
        results.is_empty(),
        "today the app-side query swallows parse errors and returns empty"
    );
}

#[tokio::test]
#[allow(non_snake_case)]
async fn current_query_projection_is_limited_to_id_type_name_state__pending_replacement() {
    // CONTRACT TO BREAK: the current query target is a four-field
    // JSON object built inline in `filter_by_ql` — extra `properties`
    // exposed by the device do not participate in the query. After
    // the projection moves to ResourceSnapshot, querying `where
    // color = "red"` against a device that advertises `color: "red"`
    // should match.
    let (addr, _core) = boot_with(ColoredLed { color: "red" }).await;

    // Sanity check: the device DOES render `color` as a Siren property.
    let server: Json = reqwest::get(format!("http://{addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(server["entities"][0]["properties"]["color"], "red");

    // But it does not currently match a query against `color`.
    let url = format!(
        "http://{addr}/servers/hub?ql={}",
        urlencoding::encode(r#"where color = "red""#)
    );
    let body: Json = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let entities = body.get("entities").and_then(|v| v.as_array());
    assert!(
        entities.map(|a| a.is_empty()).unwrap_or(true),
        "current projection ignores `properties`; expected no matches, got {entities:?}"
    );
}
