//! Verify ServerHandle::observe fires when all queries are satisfied.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use zetta_core::{Device, DeviceConfig, DeviceError, TransitionInput};
use zetta_http::{App, AppError, Scout, ScoutCtx, ServerHandle};
use zetta_server::Zetta;

#[derive(Default)]
struct Led;

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .state("off")
            .when("off", &["turn-on"])
            .when("on", &["turn-off"]);
    }
    fn state(&self) -> &str {
        "off"
    }
    fn transition<'a>(
        &'a mut self,
        _name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
struct Photocell;

impl Device for Photocell {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("photocell").state("ready");
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

/// Scout that registers a Photocell 100ms after boot, simulating
/// late-discovered hardware.
struct LatePhotocell;

#[async_trait]
impl Scout for LatePhotocell {
    async fn run(self: Arc<Self>, ctx: ScoutCtx) -> Result<(), DeviceError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        ctx.discover(Photocell).await;
        Ok(())
    }
}

struct DuskDawn {
    fired: Arc<AtomicBool>,
}

#[async_trait]
impl App for DuskDawn {
    async fn run(self: Arc<Self>, server: ServerHandle) -> Result<(), AppError> {
        server
            .observe(
                vec![r#"where type = "led""#, r#"where type = "photocell""#],
                |devs| async move {
                    assert_eq!(devs.len(), 2);
                    self.fired.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
    }
}

#[tokio::test]
async fn observe_fires_when_all_queries_satisfied() {
    let fired = Arc::new(AtomicBool::new(false));
    let _built = Zetta::new()
        .name("hub")
        .use_device(Led)
        .use_scout(LatePhotocell)
        .use_app(DuskDawn {
            fired: fired.clone(),
        })
        .build()
        .unwrap();

    // The LED is present at boot, the photocell after 100ms. Observe
    // should fire shortly after that.
    for _ in 0..50 {
        if fired.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("observe never fired");
}
