//! `BusSink` mints envelopes through the single shared
//! `StreamRegistry`. Subscribe-before-register exercises the
//! lifecycle, because `CoreBuilder::build` invokes `on_start`
//! synchronously before returning the `Core`.

use std::sync::Arc;

use boardwalk::core::{DeviceCtx, StreamKind, StreamSink};
use boardwalk::events::{StreamRegistry, SubscribeOpts, TopicPattern};
use boardwalk::http::{Core, CoreBuilder};
use boardwalk::{Device, DeviceConfig, DeviceError, TransitionInput};
use serde_json::json;
use uuid::Uuid;

/// Test driver that publishes `n` events to one stream the moment
/// `on_start` is invoked.
struct TestPublishOnStart {
    type_: String,
    stream: String,
    data: serde_json::Value,
    n: usize,
}

impl TestPublishOnStart {
    fn new(type_: &str, stream: &str, data: serde_json::Value) -> Self {
        Self::new_n(type_, stream, data, 1)
    }
    fn new_n(type_: &str, stream: &str, data: serde_json::Value, n: usize) -> Self {
        Self {
            type_: type_.into(),
            stream: stream.into(),
            data,
            n,
        }
    }
}

impl Device for TestPublishOnStart {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_(self.type_.clone())
            .state("idle")
            .stream(self.stream.clone(), StreamKind::Object);
    }
    fn state(&self) -> &str {
        "idle"
    }
    fn transition<'a>(
        &'a mut self,
        _name: &'a str,
        _input: TransitionInput,
    ) -> futures::future::BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async move { Ok(()) })
    }
    fn on_start(&self, ctx: DeviceCtx) {
        let publish: Arc<dyn StreamSink> = ctx.publish;
        for _ in 0..self.n {
            publish.publish(&self.stream, self.data.clone());
        }
    }
}

fn empty_core() -> Arc<Core> {
    CoreBuilder::new("hub").build()
}

#[tokio::test]
async fn driver_published_event_carries_envelope_minted_at_source() {
    let core = empty_core();
    let mut sub = core.bus.subscribe(
        TopicPattern::parse("hub/test-device/*/telemetry").unwrap(),
        SubscribeOpts::default(),
    );

    let id = Uuid::new_v4();
    let mut cfg = DeviceConfig::default();
    cfg.type_("test-device")
        .state("idle")
        .stream("telemetry", StreamKind::Object);
    core.register_device(
        id,
        cfg,
        Box::new(TestPublishOnStart::new(
            "test-device",
            "telemetry",
            json!({"value": 1}),
        )),
    )
    .await;

    let env = sub.rx.recv().await.expect("first envelope");
    assert!(!env.event_id.as_str().is_empty());
    assert_eq!(env.sequence, 1);
    assert_eq!(env.node_id.as_str(), "hub");
    assert_eq!(env.resource_kind, "test-device");
    assert_eq!(env.stream, "telemetry");
    assert_eq!(env.payload_kind, "resource.stream.data");
    assert_eq!(env.payload_version, 1);
}

#[tokio::test]
async fn two_publishes_on_same_stream_have_strictly_increasing_sequences() {
    let core = empty_core();
    let mut sub = core.bus.subscribe(
        TopicPattern::parse("hub/test-device/*/telemetry").unwrap(),
        SubscribeOpts::default(),
    );

    let id = Uuid::new_v4();
    let mut cfg = DeviceConfig::default();
    cfg.type_("test-device")
        .state("idle")
        .stream("telemetry", StreamKind::Object);
    core.register_device(
        id,
        cfg,
        Box::new(TestPublishOnStart::new_n(
            "test-device",
            "telemetry",
            json!({"value": 1}),
            2,
        )),
    )
    .await;

    let one = sub.rx.recv().await.unwrap();
    let two = sub.rx.recv().await.unwrap();
    assert_eq!(one.sequence, 1);
    assert_eq!(two.sequence, 2);
    assert_ne!(one.event_id, two.event_id);
}

#[tokio::test]
async fn parallel_publishes_on_different_streams_have_independent_sequences() {
    let core = empty_core();
    let mut sub_a = core.bus.subscribe(
        TopicPattern::parse("hub/a-device/*/telemetry").unwrap(),
        SubscribeOpts::default(),
    );
    let mut sub_b = core.bus.subscribe(
        TopicPattern::parse("hub/b-device/*/telemetry").unwrap(),
        SubscribeOpts::default(),
    );

    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();
    let mut cfg_a = DeviceConfig::default();
    cfg_a
        .type_("a-device")
        .state("idle")
        .stream("telemetry", StreamKind::Object);
    let mut cfg_b = DeviceConfig::default();
    cfg_b
        .type_("b-device")
        .state("idle")
        .stream("telemetry", StreamKind::Object);

    core.register_device(
        id_a,
        cfg_a,
        Box::new(TestPublishOnStart::new(
            "a-device",
            "telemetry",
            json!({"v": "a"}),
        )),
    )
    .await;
    core.register_device(
        id_b,
        cfg_b,
        Box::new(TestPublishOnStart::new(
            "b-device",
            "telemetry",
            json!({"v": "b"}),
        )),
    )
    .await;

    let env_a = sub_a.rx.recv().await.unwrap();
    let env_b = sub_b.rx.recv().await.unwrap();
    assert_eq!(env_a.sequence, 1);
    assert_eq!(env_b.sequence, 1);
    assert_ne!(env_a.stream_id, env_b.stream_id);
    assert_ne!(env_a.event_id, env_b.event_id);
}

#[tokio::test]
async fn bus_and_core_expose_the_same_registry_instance() {
    let core = empty_core();
    let bus_reg: &StreamRegistry = core.bus.stream_registry();
    assert!(
        core.stream_registry.same_instance(bus_reg),
        "Core::stream_registry and bus.stream_registry() must reference the same Arc inner"
    );
}

#[tokio::test]
async fn register_device_uses_existing_bus_registry() {
    let core = empty_core();
    let mut sub = core.bus.subscribe(
        TopicPattern::parse("hub/test-device/*/telemetry").unwrap(),
        SubscribeOpts::default(),
    );

    let id = Uuid::new_v4();
    let mut cfg = DeviceConfig::default();
    cfg.type_("test-device")
        .state("idle")
        .stream("telemetry", StreamKind::Object);
    core.register_device(
        id,
        cfg,
        Box::new(TestPublishOnStart::new(
            "test-device",
            "telemetry",
            json!({"value": 1}),
        )),
    )
    .await;

    let env = sub.rx.recv().await.unwrap();
    let reverse = core.bus.stream_registry().stream_for(&env.event_id);
    assert_eq!(
        reverse,
        Some(env.stream_id.clone()),
        "register_device must mint into the bus's registry — found {reverse:?}"
    );
}
