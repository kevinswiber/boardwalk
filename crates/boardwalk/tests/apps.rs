//! Verify .use_app() runs the app on boot and that it can query and
//! call into devices.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use boardwalk::{
    App, AppError, Boardwalk, Device, DeviceConfig, DeviceError, ServerHandle, TransitionInput,
};
use futures::future::BoxFuture;

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
        let leds = server.query("where type = \"led\"").await;
        for led in &leds {
            if led.available("turn-on").await {
                led.call_simple("turn-on").await?;
            }
        }
        self.fired.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn app_runs_and_transitions_device() {
    let fired = Arc::new(AtomicBool::new(false));
    let built = Boardwalk::new()
        .name("hub")
        .use_device(Led::default())
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
