//! Verify Boardwalk::listen_until shuts down cleanly when signaled.

use std::net::SocketAddr;
use std::time::Duration;

use boardwalk::Boardwalk;
use boardwalk::core::{Device, DeviceConfig, DeviceError, TransitionInput};
use futures::future::BoxFuture;
use tokio::sync::oneshot;

#[derive(Default)]
struct Led;

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led").state("off");
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

#[tokio::test]
async fn listen_until_returns_on_signal() {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    drop(listener); // give up the addr; listen_until will bind it again

    let server = tokio::spawn(async move {
        Boardwalk::new()
            .name("hub")
            .use_actor(Led)
            .listen_until(addr, async move {
                let _ = rx.await;
            })
            .await
    });

    // Give it a moment to start.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Signal shutdown.
    tx.send(()).unwrap();

    // listen_until should return within a couple seconds.
    let result = tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("listener did not return after shutdown signal")
        .unwrap();
    result.unwrap();
}
