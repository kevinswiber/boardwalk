//! Verify scouts discover devices at runtime.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::Boardwalk;
use crate::core::{Device, DeviceConfig, DeviceError, TransitionInput};
use crate::http::{Scout, ScoutCtx};

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

/// Pins the current `ScoutCtx::discover` contract: when persistence is
/// off, each discovery mints a fresh random `DeviceId`, and the device
/// becomes visible in `Core::list_devices`. The runtime-handle
/// migration will rebrand this to `ActorCtx::register`; this snapshot
/// must update when that happens.
#[tokio::test]
async fn current_scout_discover_mints_random_device_id() {
    let built = Boardwalk::new()
        .name("hub")
        .use_scout(DelayedScout)
        .build()
        .unwrap();

    let mut tries = 0;
    let devices = loop {
        let now = built.core.list_devices().await;
        if now.len() == 2 {
            break now;
        }
        tries += 1;
        if tries > 50 {
            panic!("scout never discovered devices");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    // Both ids must be parseable UUIDs and distinct from each other
    // (no persist => fresh v4).
    let id0 = devices[0].id;
    let id1 = devices[1].id;
    assert_ne!(id0, id1, "scout-minted ids must be distinct");
    assert_eq!(id0.get_version_num(), 4, "expected UUID v4 from scout");
    assert_eq!(id1.get_version_num(), 4, "expected UUID v4 from scout");
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
