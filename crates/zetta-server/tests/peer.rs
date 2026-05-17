//! Integration test for the peer tunnel: hub links to cloud, cloud
//! confirms.

use std::time::Duration;

use futures::future::BoxFuture;
use zetta_core::{Device, DeviceConfig, DeviceError, TransitionInput};
use zetta_server::Zetta;

#[derive(Default)]
struct Led { on: bool }

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led").name("LED").state(self.state())
            .when("off", &["turn-on"]).when("on", &["turn-off"]);
    }
    fn state(&self) -> &str { if self.on { "on" } else { "off" } }
    fn transition<'a>(
        &'a mut self, _name: &'a str, _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn hub_links_to_cloud() {
    // Boot cloud.
    let cloud = Zetta::new().name("cloud").build();
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    tokio::spawn(async move {
        axum::serve(cloud_listener, cloud.router).await.unwrap();
    });

    // Boot hub, linking to cloud.
    let hub = Zetta::new()
        .name("hub")
        .use_device(Led::default())
        .link(format!("http://{cloud_addr}"))
        .build();
    let hub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(async move {
        axum::serve(hub_listener, hub.router).await.unwrap();
    });

    // Wait for cloud to see a confirmed peer.
    let confirmed = cloud_acceptors.wait_for_first(Duration::from_secs(5)).await;
    assert!(confirmed, "cloud should have received a confirmed peer within 5s");
    assert!(cloud_acceptors.confirmation_count() >= 1);
}
