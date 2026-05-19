//! Characterization tests for state-gated transition affordances.
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

use boardwalk::events::{SubscribeOpts, TopicPattern};
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

async fn device_id(addr: SocketAddr) -> String {
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

#[tokio::test]
async fn only_currently_allowed_transitions_render_as_actions() {
    let (addr, _core, _h) = boot().await;
    let id = device_id(addr).await;

    let dev: Json = reqwest::get(format!("http://{addr}/servers/hub/devices/{id}"))
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
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let dev: Json = resp.json().await.unwrap();
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
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // turn-on is now disallowed; expect 409.
    let resp = client
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
}

/// Survivor characterization for the form-url-encoded transition
/// path and the envelope it mints.
///
/// Pins three rules together so each is renamed deliberately later:
///   * `POST /servers/hub/devices/{id}` with
///     `application/x-www-form-urlencoded; action=...` returns 200
///     and a Siren device entity carrying the new state,
///   * the bus emits one envelope with `payloadKind ==
///     "resource.state.changed"`, `stream == "state"`, and the legacy
///     topic `hub/led/{id}/state`,
///   * `causationId` is always populated (a fresh `CommandId` is
///     minted per call) while `correlationId` and `traceContext`
///     remain absent when no request headers were sent.
#[tokio::test]
async fn current_form_transition_returns_device_siren_and_state_event() {
    let (addr, core, _h) = boot().await;
    let id = device_id(addr).await;
    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=turn-on")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let body: Json = resp.json().await.unwrap();
    let class: Vec<&str> = body["class"]
        .as_array()
        .expect("class array")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(
        class.contains(&"device") && class.contains(&"led"),
        "expected device+led class, got {class:?}"
    );
    assert_eq!(body["properties"]["state"], "on");
    assert_eq!(body["properties"]["type"], "led");

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
        .expect("causationId is minted per form-route call");
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
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("action=does-not-exist")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        409,
        "unknown action names are gated by `allowed_in` before reaching the Device trait, so they return 409 (not 400/404)"
    );
}
