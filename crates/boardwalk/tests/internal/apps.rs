//! Verify .use_app() runs the app on boot and that it can query and
//! call into devices.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use serde_json::{Map, Value as Json};

use crate::Boardwalk;
use crate::core::{Device, DeviceConfig, DeviceError};
use crate::http::{App, AppError, CoreBuilder, ServerHandle};
use crate::runtime::TransitionInput;

#[derive(Default)]
struct Led {
    on: bool,
}

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .name("LED")
            .state(self.state())
            .when("off", &["turn-on"])
            .when("on", &["turn-off"]);
    }
    fn state(&self) -> &str {
        if self.on { "on" } else { "off" }
    }
    fn transition<'a>(
        &'a mut self,
        name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
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
                _ => Err(DeviceError::Invalid("?".into())),
            }
        })
    }
}

struct TurnAllOn {
    fired: Arc<AtomicBool>,
}

#[async_trait]
impl App for TurnAllOn {
    async fn run(self: Arc<Self>, server: ServerHandle) -> Result<(), AppError> {
        let leds = server.query("where kind = \"led\"").await?;
        for led in &leds {
            if led.available("turn-on").await {
                led.call_simple("turn-on").await?;
            }
        }
        self.fired.store(true, Ordering::SeqCst);
        Ok(())
    }
}

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
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async move { Err(DeviceError::Invalid("no transitions".into())) })
    }
}

/// Pins the current app-side query contract: `ServerHandle::query`
/// returns one `DeviceProxy` per resource snapshot whose projection
/// matches, and invalid CaQL surfaces as `Err` (not silently empty).
/// The runtime-handle migration will move this surface off
/// `ServerHandle`; this snapshot must update when that happens.
#[tokio::test]
async fn server_handle_query_returns_device_proxy_for_resource_snapshot_match() {
    let mut b = CoreBuilder::new("hub");
    b.add_device(ColoredLed { color: "red" });
    let core = b.build();
    let server = ServerHandle::new_internal(core);

    let matches = server
        .query(r#"where exists properties.color"#)
        .await
        .expect("query parses against projection with properties.color");
    assert_eq!(
        matches.len(),
        1,
        "exactly one device exposes properties.color in this fixture"
    );

    let result = server.query("where ===nonsense===").await;
    assert!(
        result.is_err(),
        "invalid ql must surface as Err, not be silently swallowed"
    );
}

#[tokio::test]
async fn app_runs_and_transitions_device() {
    let fired = Arc::new(AtomicBool::new(false));
    let built = Boardwalk::new()
        .name("hub")
        .use_actor(Led::default())
        .use_app(TurnAllOn {
            fired: fired.clone(),
        })
        .build()
        .unwrap();

    // Wait briefly for the app to run.
    for _ in 0..50 {
        if fired.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(fired.load(Ordering::SeqCst), "app should have run");

    // The LED should now be in the `on` state.
    let snap = built.core.list_devices().await.into_iter().next().unwrap();
    assert_eq!(snap.state, "on");
}
