//! Verify Boardwalk::listen_until shuts down cleanly when signaled.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::oneshot;

use super::actor_led_fixture::ActorLed;
use crate::Boardwalk;

#[tokio::test]
async fn listen_until_returns_on_signal() {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    drop(listener); // give up the addr; listen_until will bind it again

    let server = tokio::spawn(async move {
        Boardwalk::new()
            .name("hub")
            .use_actor(ActorLed::default())
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

#[tokio::test]
async fn listen_until_on_serves_prebound_listener_and_returns_on_signal() {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let addr = listener.local_addr().expect("listener has local addr");

    let server = tokio::spawn(async move {
        Boardwalk::new()
            .name("hub")
            .use_actor(ActorLed::default())
            .listen_until_on(listener, async move {
                let _ = rx.await;
            })
            .await
    });

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let response = loop {
        match client.get(&url).send().await {
            Ok(response) => break response,
            Err(err) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "listen_until_on did not accept requests on supplied listener: {err:?}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    };
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("listener did not return after shutdown signal")
        .unwrap();
    result.unwrap();
}
