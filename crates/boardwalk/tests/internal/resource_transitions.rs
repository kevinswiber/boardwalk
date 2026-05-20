//! Contract tests for state-gated Resource transition affordances.
//!
//! Pins three rules that the renderer + transition runner enforce:
//!   * only currently-allowed transitions render as Siren actions,
//!   * POSTing a transition that isn't allowed in the current state
//!     returns HTTP 409,
//!   * POSTing an unknown transition name *also* returns 409 because
//!     the `allowed_in` gate fires before the `Device::transition`
//!     trait method (see `http/core.rs::run_transition`).

use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value as Json;

use crate::core::{Device, DeviceConfig, DeviceError};
use crate::events::{SubscribeOpts, TopicPattern};
use crate::http::{Core, CoreBuilder, router};
use crate::runtime::TransitionInput;

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

async fn device_id(addr: SocketAddr) -> String {
    let resources: Json = reqwest::get(format!("http://{addr}/resources"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    resources["entities"][0]["properties"]["id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn only_currently_allowed_transitions_render_as_actions() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;

    let dev: Json = reqwest::get(format!("http://{addr}/resources/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = dev["actions"]
        .as_array()
        .expect("actions array")
        .iter()
        .map(|a| a["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["turn-on"],
        "in `off` state the only allowed transition is turn-on"
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources/{id}/transitions/turn-on"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let dev: Json = reqwest::get(format!("http://{addr}/resources/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = dev["actions"]
        .as_array()
        .expect("actions array")
        .iter()
        .map(|a| a["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["turn-off"],
        "in `on` state the only allowed transition is turn-off"
    );
}

#[tokio::test]
async fn post_disallowed_transition_returns_409() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;

    let client = reqwest::Client::new();
    // Flip to `on` first.
    let resp = client
        .post(format!("http://{addr}/resources/{id}/transitions/turn-on"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // turn-on is now disallowed; expect 409.
    let resp = client
        .post(format!("http://{addr}/resources/{id}/transitions/turn-on"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
}

#[tokio::test]
async fn json_transition_completed_returns_output_and_snapshot() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources/{id}/transitions/turn-on"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["output"], Json::Null);
    assert_eq!(body["snapshot"]["id"], id);
    assert_eq!(body["snapshot"]["state"], "on");
    assert_eq!(body["snapshot"]["kind"], "led");
}

#[tokio::test]
async fn malformed_json_transition_returns_problem_400() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources/{id}/transitions/turn-on"))
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("application/problem+json"),
        "expected problem+json content type, got {content_type}"
    );
    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid-json");
    assert_eq!(body["field"], "body");
}

#[tokio::test]
async fn form_urlencoded_transition_returns_415() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources/{id}/transitions/turn-on"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 415);
}

/// Pins the JSON resource transition path and the envelope it mints.
///
/// Pins three rules together:
///   * `POST /resources/{id}/transitions/{transition}` with JSON
///     returns 200 and an outcome body carrying the new snapshot,
///   * the bus emits one envelope with `payloadKind ==
///     "resource.state.changed"`, `stream == "state"`, and topic
///     `hub/led/{id}/state`,
///   * `causationId` is always populated (a fresh `CommandId` is
///     minted per call) while `correlationId` and `traceContext`
///     remain absent when no request headers were sent.
#[tokio::test]
async fn json_transition_returns_outcome_and_state_event() {
    let (addr, core, _h) = boot().await;
    let id = device_id(addr).await;
    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/resources/{id}/transitions/turn-on"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Json = resp.json().await.unwrap();
    assert_eq!(body["output"], Json::Null);
    assert_eq!(body["snapshot"]["state"], "on");
    assert_eq!(body["snapshot"]["kind"], "led");

    let env = sub.rx.recv().await.expect("state-change envelope");
    assert_eq!(env.payload_kind, "resource.state.changed");
    assert_eq!(env.stream, "state");
    assert_eq!(env.topic(), topic);
    assert!(
        env.correlation_id.is_none(),
        "correlationId should be absent when no x-request-id was sent"
    );
    let causation = env
        .causation_id
        .as_ref()
        .expect("causationId is minted per transition call");
    assert!(
        !causation.0.is_empty(),
        "causation_id carries the minted CommandId"
    );
    assert!(
        env.trace_context.is_none(),
        "traceContext should be absent when no traceparent was sent"
    );
}

#[tokio::test]
async fn post_unknown_transition_returns_409() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://{addr}/resources/{id}/transitions/does-not-exist"
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        409,
        "unknown transition names are gated by `allowed_in` before reaching the Device trait, so they return 409 (not 400/404)"
    );
}
