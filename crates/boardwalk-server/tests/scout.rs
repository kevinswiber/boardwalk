//! Verify scouts discover devices at runtime.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use boardwalk_core::{Device, DeviceConfig, DeviceError, TransitionInput};
use boardwalk_http::{Scout, ScoutCtx};
use boardwalk_server::Boardwalk;
use futures::future::BoxFuture;

#[derive(Default)]
struct Sensor {
    name: String,
}

impl Device for Sensor {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("sensor").name(self.name.clone()).state("ready");
    }
    fn state(&self) -> &str {
        "ready"
    }
    fn transition<'a>(
        &'a mut self,
        _name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A scout that "discovers" two sensors over a short delay.
struct DelayedScout;

#[async_trait]
impl Scout for DelayedScout {
    async fn run(self: Arc<Self>, ctx: ScoutCtx) -> Result<(), DeviceError> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        ctx.discover(Sensor {
            name: "front-door".into(),
        })
        .await;
        ctx.discover(Sensor {
            name: "back-door".into(),
        })
        .await;
        Ok(())
    }
}

#[tokio::test]
async fn scout_discovers_devices_at_runtime() {
    let built = Boardwalk::new()
        .name("hub")
        .use_scout(DelayedScout)
        .build()
        .unwrap();

    // Before the scout runs, no devices.
    let before = built.core.list_devices().await;
    assert!(before.is_empty());

    // Wait briefly for the scout to discover.
    let mut tries = 0;
    loop {
        let now = built.core.list_devices().await;
        if now.len() == 2 {
            let mut names: Vec<String> = now.into_iter().filter_map(|d| d.name).collect();
            names.sort();
            assert_eq!(
                names,
                vec!["back-door".to_string(), "front-door".to_string()]
            );
            return;
        }
        tries += 1;
        if tries > 50 {
            panic!("scout never discovered devices");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
