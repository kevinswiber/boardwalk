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
