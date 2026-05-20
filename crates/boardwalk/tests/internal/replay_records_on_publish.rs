//! The bus records each successful publish into the per-stream replay
//! cache, and ring eviction prunes the matching `event_id` from the
//! shared `StreamRegistry`.

use std::sync::Arc;

use serde_json::json;
use uuid::Uuid;

use crate::core::{Device, DeviceConfig, DeviceCtx, DeviceError, StreamSink};
use crate::events::{NodeId, StreamId, SubscribeOpts, TopicPattern};
use crate::http::CoreBuilder;
use crate::runtime::{StreamKind, TransitionInput};

struct TestPublishOnStart {
    stream: String,
    data: serde_json::Value,
    n: usize,
}

impl Device for TestPublishOnStart {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("test-device")
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

#[tokio::test]
async fn bus_records_published_envelope_in_replay_cache() {
    let core = CoreBuilder::new("hub").build();
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
        Box::new(TestPublishOnStart {
            stream: "telemetry".into(),
            data: json!({"v": 1}),
            n: 1,
        }),
    )
    .await;

    let env = sub.rx.recv().await.expect("envelope delivered");

    let stream_id = StreamId::for_resource(&NodeId::new("hub"), &id.to_string(), "telemetry");
    let replayed = core.bus.replay_cache().events_after(&stream_id, 0);
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].event_id, env.event_id);
    assert_eq!(replayed[0].sequence, env.sequence);
}

#[tokio::test]
async fn bus_eviction_prunes_registry_reverse_index_through_bussink_path() {
    let core = CoreBuilder::new("hub").build_with_replay_capacity(2);
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
        Box::new(TestPublishOnStart {
            stream: "telemetry".into(),
            data: json!({"v": 1}),
            n: 3,
        }),
    )
    .await;

    let one = sub.rx.recv().await.unwrap();
    let two = sub.rx.recv().await.unwrap();
    let three = sub.rx.recv().await.unwrap();

    // Replay ring capacity 2 → the first envelope was evicted, which
    // must have called `registry.evict(&one.event_id)`.
    assert!(
        core.bus
            .stream_registry()
            .stream_for(&one.event_id)
            .is_none(),
        "one.event_id should be evicted from the registry"
    );
    assert!(
        core.bus
            .stream_registry()
            .stream_for(&two.event_id)
            .is_some()
    );
    assert!(
        core.bus
            .stream_registry()
            .stream_for(&three.event_id)
            .is_some()
    );
}
