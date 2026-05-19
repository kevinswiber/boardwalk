use std::net::SocketAddr;
use std::sync::Arc;

use boardwalk::core::{Effect, Idempotency, TransitionResultKind};
use boardwalk::http::{Core, CoreBuilder, router};
use boardwalk::{Device, DeviceConfig, DeviceError, TransitionInput};
use serde_json::{Value as Json, json};

struct SchemaLed;

impl Device for SchemaLed {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .name("LED")
            .state("off")
            .when("off", &["turn-on"])
            .monitor("state");
        let turn_on = cfg.transitions.get_mut("turn-on").unwrap();
        turn_on.title = Some("Turn on".into());
        turn_on.input_schema = Some(json!({
            "type": "object",
            "properties": {"brightness": {"type": "number"}}
        }));
        turn_on.output_schema = Some(json!({"type": "object"}));
        turn_on.result = TransitionResultKind::Sync;
        turn_on.idempotency = Idempotency::Supported;
        turn_on.effect = Effect::UnsafeIdempotent;
        turn_on.required_scopes = vec!["transition.invoke".into()];
    }

    fn state(&self) -> &str {
        "off"
    }

    fn transition<'a>(
        &'a mut self,
        _name: &'a str,
        _input: TransitionInput,
    ) -> futures::future::BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async { Ok(()) })
    }
}

async fn boot() -> (SocketAddr, Arc<Core>) {
    let mut b = CoreBuilder::new("hub");
    b.add_device(SchemaLed);
    let core = b.build();
    let app = router(core.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, core)
}

#[tokio::test]
async fn metadata_lists_kind_transitions_streams_and_schemas_from_resource_spec() {
    let (addr, _core) = boot().await;
    let meta: Json = reqwest::get(format!("http://{addr}/meta"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let type_entity = meta["entities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entity| entity["properties"]["kind"] == "led")
        .expect("led metadata entity");
    assert!(type_entity["properties"].get("type").is_none());
    assert_eq!(type_entity["properties"]["propertySchema"], Json::Null);

    let transition = &type_entity["properties"]["transitions"][0];
    assert_eq!(transition["name"], "turn-on");
    assert_eq!(transition["title"], "Turn on");
    assert_eq!(transition["allowedStates"], json!(["off"]));
    assert_eq!(
        transition["inputSchema"]["properties"]["brightness"]["type"],
        "number"
    );
    assert_eq!(transition["outputSchema"], json!({"type": "object"}));
    assert_eq!(transition["result"], "sync");
    assert_eq!(transition["idempotency"], "supported");
    assert_eq!(transition["effect"], "unsafe-idempotent");
    assert_eq!(transition["requiredScopes"], json!(["transition.invoke"]));

    assert_eq!(
        type_entity["properties"]["streams"],
        json!([{"name": "state", "kind": "object"}])
    );
}
