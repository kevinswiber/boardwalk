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

/// With `limit: Some(1)`, two concurrent publishers must not both
/// enqueue an envelope into the subscription's outbound channel. The
/// slot is claimed under the bus lock, so at most one publisher
/// wins; the other observes the exhausted quota and skips.
#[tokio::test]
async fn concurrent_publish_respects_subscription_limit() {
    let bus = EventBus::with_registry(StreamRegistry::new());
    let pattern = TopicPattern::parse("hub/led/r1/state").unwrap();
    let sub = bus.subscribe(
        pattern,
        SubscribeOpts {
            outbound_capacity: Some(16),
            limit: Some(1),
            ..Default::default()
        },
    );

    let bus_a = bus.clone();
    let bus_b = bus.clone();
    let (a, b) = tokio::join!(
        async move { bus_a.publish(envelope(1)).await },
        async move { bus_b.publish(envelope(2)).await },
    );

    let total_delivered = a.unwrap().delivered + b.unwrap().delivered;
    assert_eq!(
        total_delivered, 1,
        "limit=1 must permit exactly one delivery across concurrent publishers"
    );

    // Drain the subscriber's mpsc channel; it should hold one
    // envelope and nothing else.
    let mut rx = sub.rx;
    let first = rx.try_recv().expect("one envelope was delivered");
    assert!(matches!(first.sequence, 1 | 2));
    assert!(
        rx.try_recv().is_err(),
        "no second envelope should have leaked past the limit"
    );
}

/// `Lossy + DropNewest` events that drop on a full outbound channel
/// must not consume the subscription's quota — matching sync
/// `try_publish` semantics. Scenario from the review: `limit=2`,
/// capacity 1, one event delivered, second dropped while buffer is
/// full, drain, third should still be delivered.
#[tokio::test]
async fn lossy_dropnewest_does_not_consume_subscription_quota() {
    let bus = EventBus::with_registry(StreamRegistry::new());
    let pattern = TopicPattern::parse("hub/led/r1/state").unwrap();
    let mut sub = bus.subscribe(
        pattern,
        SubscribeOpts {
            outbound_capacity: Some(1),
            limit: Some(2),
            stream_safety: StreamSafety::Lossy,
            overflow_policy: OverflowPolicy::DropNewest,
        },
    );

    // 1: delivered, buffer fills.
    let r1 = bus.publish(envelope(1)).await.unwrap();
    assert_eq!(r1.delivered, 1);
    assert_eq!(r1.dropped, 0);

    // 2: dropped because buffer is full. Quota stays at 1.
    let r2 = bus.publish(envelope(2)).await.unwrap();
    assert_eq!(r2.delivered, 0);
    assert_eq!(r2.dropped, 1);

    // Drain the buffer.
    let _ = sub.rx.recv().await.expect("first envelope");

    // 3: should still deliver — the dropped event must not have
    // burned the second slot.
    let r3 = bus.publish(envelope(3)).await.unwrap();
    assert_eq!(
        r3.delivered, 1,
        "the lossy drop must not consume quota; the third send must deliver"
    );
    assert_eq!(r3.dropped, 0);
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
