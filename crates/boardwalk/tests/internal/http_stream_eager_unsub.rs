//! The HTTP NDJSON event stream eagerly unsubscribes from the bus
//! when its response body is dropped — the bus no longer waits for
//! the next publish to notice the closed receiver.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use serde_json::Value as Json;

use crate::core::{Device, DeviceConfig, DeviceError};
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
                _ => Err(DeviceError::Invalid("?".into())),
            }
        })
    }
}

async fn boot() -> (SocketAddr, Arc<Core>) {
    let mut b = CoreBuilder::new("hub");
    b.add_device(Led::default());
    let core = b.build();
    let app = router(core.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, core)
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
async fn http_ndjson_subscription_removed_on_body_drop() {
    let (addr, core) = boot().await;
    let id = device_id(addr).await;
    let topic = format!("hub/led/{id}/state");

    let url = format!("http://{addr}/servers/hub/events?topic={topic}");
    let resp = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("GET succeeds");
    assert_eq!(resp.status(), 200);

    // Give axum a moment to register the bus subscription.
    tokio::time::sleep(Duration::from_millis(75)).await;
    assert_eq!(
        core.bus.active_subscriptions(),
        1,
        "NDJSON GET should have registered exactly one bus subscription"
    );

    // Drop the response (and its body stream) and wait — *no* publish
    // happens. The drop guard should have run.
    let mut stream = resp.bytes_stream();
    // Touch the stream so it's polled at least once; this kicks the
    // async_stream into running its initial setup (subscribe is
    // already done synchronously, but the stream body is what holds
    // the guard).
    let _ = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
    drop(stream);

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        core.bus.active_subscriptions(),
        0,
        "subscription must be evicted on body drop — no publish required"
    );
}
