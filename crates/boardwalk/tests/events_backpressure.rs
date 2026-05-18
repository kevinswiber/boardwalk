//! Pins async publish backpressure: a `Lossy + Backpressure`
//! subscriber must cause `EventBus::publish` to await queue capacity
//! instead of dropping the envelope.

use std::time::Duration;

use boardwalk::events::{
    ENVELOPE_VERSION, EventBus, EventEnvelope, NodeId, OverflowPolicy, StreamId, StreamRegistry,
    StreamSafety, SubscribeOpts, TopicPattern,
};
use serde_json::Value;

fn envelope(seq: u64) -> EventEnvelope {
    EventEnvelope {
        envelope_version: ENVELOPE_VERSION,
        event_id: boardwalk::events::EventId::from_raw(format!("ev-{seq}")),
        node_id: NodeId::new("hub"),
        resource_id: "r1".into(),
        resource_kind: "led".into(),
        resource_version: 1,
        stream_id: StreamId::for_resource(&NodeId::new("hub"), "r1", "state"),
        stream: "state".into(),
        sequence: seq,
        timestamp: time::OffsetDateTime::now_utc(),
        payload_kind: "resource.state.changed".into(),
        payload_version: 1,
        payload_schema: None,
        correlation_id: None,
        causation_id: None,
        trace_context: None,
        data: Value::String("on".into()),
    }
}

#[tokio::test]
async fn lossy_backpressure_awaits_capacity_instead_of_dropping() {
    let bus = EventBus::with_registry(StreamRegistry::new());
    let pattern = TopicPattern::parse("hub/led/r1/state").unwrap();
    let mut sub = bus.subscribe(
        pattern,
        SubscribeOpts {
            outbound_capacity: Some(1),
            stream_safety: StreamSafety::Lossy,
            overflow_policy: OverflowPolicy::Backpressure,
            ..Default::default()
        },
    );

    // First publish fills the buffer.
    let r1 = bus.publish(envelope(1)).await.expect("publish 1 succeeds");
    assert_eq!(r1.delivered, 1);
    assert_eq!(r1.dropped, 0);

    // Second publish must wait for capacity rather than drop.
    let bus2 = bus.clone();
    let publish2 = tokio::spawn(async move { bus2.publish(envelope(2)).await });

    // Give the runtime a chance to schedule the publish; it must
    // remain pending because the subscriber hasn't read yet.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !publish2.is_finished(),
        "publish must remain pending while buffer is full"
    );

    // Reading from the subscriber frees the slot.
    let _ = sub.rx.recv().await.expect("first envelope");

    let r2 = publish2
        .await
        .expect("task join")
        .expect("publish 2 succeeds");
    assert_eq!(r2.delivered, 1);
    assert_eq!(
        r2.dropped, 0,
        "backpressure must not drop; it awaits capacity"
    );
}
