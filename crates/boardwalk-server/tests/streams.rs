//! Device-declared streams: a driver publishes telemetry via
//! `DeviceCtx::publish`, and clients observe it over the multiplex WS.

use std::time::Duration;

use boardwalk_core::{Device, DeviceConfig, DeviceCtx, DeviceError, StreamKind, TransitionInput};
use boardwalk_server::Boardwalk;
use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use serde_json::Value as Json;
use tokio_tungstenite::tungstenite::Message;

#[derive(Default)]
struct Photocell;

impl Device for Photocell {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("photocell")
            .name("Cell")
            .state("ready")
            .stream("intensity", StreamKind::Object);
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
    fn on_start(&self, ctx: DeviceCtx) {
        tokio::spawn(async move {
            let mut counter = 0u32;
            loop {
                tokio::time::sleep(Duration::from_millis(30)).await;
                ctx.publish.publish("intensity", serde_json::json!(counter));
                counter += 1;
                if counter > 50 {
                    break;
                }
            }
        });
    }
}

#[tokio::test]
async fn device_publishes_to_declared_stream() {
    let built = Boardwalk::new()
        .name("hub")
        .use_device(Photocell)
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, built.router).await.unwrap();
    });

    let server: Json = reqwest::get(format!("http://{addr}/servers/hub"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = server["entities"][0]["properties"]["id"].as_str().unwrap();
    let topic = format!("hub/photocell/{id}/intensity");

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/events"))
        .await
        .unwrap();
    let sub = serde_json::json!({"type": "subscribe", "topic": topic});
    ws.send(Message::Text(sub.to_string().into()))
        .await
        .unwrap();
    let _ack = ws.next().await.unwrap().unwrap();

    // Read one event.
    let evt = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .expect("timeout")
        .unwrap()
        .unwrap();
    let evt: Json = match evt {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        _ => panic!(),
    };
    assert_eq!(evt["type"], "event");
    assert_eq!(evt["topic"], topic);
    assert!(evt["data"].is_number());
}
