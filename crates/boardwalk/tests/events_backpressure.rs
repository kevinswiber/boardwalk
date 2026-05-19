//! Pins async publish backpressure: a `SlowConsumerPolicy::Backpressure`
//! subscriber must cause `EventBus::publish` to await queue capacity
//! instead of dropping the envelope.

use std::time::Duration;

use boardwalk::events::{
    ENVELOPE_VERSION, EventBus, EventEnvelope, NodeId, SlowConsumerPolicy, StreamId,
    StreamRegistry, SubscribeOpts, TopicPattern,
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

/// Regression for a publish-vs-publish race on quota removal: two
/// concurrent publishes against a `limit=2`, `DropNewest` subscriber
/// could end with the subscription removed after just one delivery if
/// the delivered path saw `remaining == 0` before the concurrent
/// dropped path applied its refund.
///
/// To force the interleaving we add a second `Backpressure` subscriber
/// whose outbound buffer is *already full* when both
/// publishes start. Each publish reaches the backpressure `send.await`
/// after running its claim phase, so both publishes have claimed a
/// slot on the limited subscription before either applies its
/// outcome. Draining the backpressure subscriber one envelope at a
/// time then serializes the apply phases.
#[tokio::test]
async fn concurrent_publish_does_not_remove_subscription_before_refund() {
    let bus = EventBus::with_registry(StreamRegistry::new());
    let pattern = TopicPattern::parse("hub/led/r1/state").unwrap();

    // Limited DropNewest subscriber. Capacity 1 so the second
    // outcome will be Dropped while the first is Delivered.
    // limit=3 because the prime consumes one slot; the two
    // concurrent publishes both claim a slot before either applies,
    // taking `remaining` to 0 across them. Without the in-flight
    // counter, the delivered path would remove the subscription
    // before the dropped path's refund applied.
    let mut sub_limited = bus.subscribe(
        pattern.clone(),
        SubscribeOpts {
            outbound_capacity: Some(1),
            limit: Some(3),
            slow_consumer_policy: SlowConsumerPolicy::DropNewest,
        },
    );

    // Backpressure subscriber whose buffer we pre-fill so every
    // concurrent publish must yield at `send.await`.
    let mut sub_back = bus.subscribe(
        pattern,
        SubscribeOpts {
            outbound_capacity: Some(1),
            slow_consumer_policy: SlowConsumerPolicy::Backpressure,
            ..Default::default()
        },
    );
    // Prime each subscriber once. The prime fills back's outbound
    // buffer (capacity 1) so every subsequent publish must yield at
    // `back.send.await`, giving the runtime an interleaving point.
    // Draining limited restores its slot so the two concurrent
    // publishes below can each claim one without first hitting the
    // capacity wall on the limited buffer.
    let prime = bus.publish(envelope(0)).await.unwrap();
    assert_eq!(prime.delivered, 2);
    let _ = sub_limited
        .rx
        .recv()
        .await
        .expect("primed envelope on limited sub");

    // Cooperative interleaving: both publishes and the drain are
    // polled in the same task. The drain advances each publish's
    // `send.await` one slot at a time, so by the time either
    // publish reaches its apply phase, the *other* publish has
    // already claimed its slot on the limited subscription.
    let drain = async {
        let mut out = Vec::new();
        for _ in 0..3 {
            out.push(sub_back.rx.recv().await.expect("back recv"));
        }
        out
    };
    let bus_a = bus.clone();
    let bus_b = bus.clone();
    let (r1, r2, _drained) = tokio::join!(
        bus_a.publish(envelope(1)),
        bus_b.publish(envelope(2)),
        drain,
    );
    let r1 = r1.unwrap();
    let r2 = r2.unwrap();
    let total_delivered = r1.delivered + r2.delivered;
    let total_dropped = r1.dropped + r2.dropped;

    // The limited subscriber accepts at most one envelope (capacity
    // 1); the other publish drops on it. Both publishes deliver to
    // the backpressure subscriber. Overall:
    //   delivered = 1 (limited) + 2 (backpressure) = 3
    //   dropped = 1 (limited)
    assert_eq!(total_delivered, 3, "expected 3 deliveries across both subs");
    assert_eq!(total_dropped, 1, "expected exactly one DropNewest drop");

    // Drain the limited sub's surviving envelope.
    let _surviving = sub_limited
        .rx
        .recv()
        .await
        .expect("limited sub received one envelope");

    // Now publish a third envelope. With the broken code, the
    // delivered path removed the limited subscription before the
    // concurrent refund applied; the third publish would deliver
    // only to the backpressure sub. With the in-flight counter, the
    // refund has restored quota and the limited sub is still alive.
    let r3 = bus.publish(envelope(3)).await.unwrap();
    assert_eq!(
        r3.delivered, 2,
        "limited subscription must still exist after the race; r3={r3:?}"
    );

    // The limited sub really receives the third envelope.
    let third = tokio::time::timeout(Duration::from_millis(200), sub_limited.rx.recv())
        .await
        .expect("third envelope arrives in time")
        .expect("envelope present");
    assert_eq!(third.sequence, 3);
    let _back_3 = sub_back.rx.recv().await.expect("back receives envelope 3");
}

/// Cancellation safety: if a `publish` future is dropped while
/// awaiting a `Backpressure` subscriber's `send`, the claimed
/// quota slot must be refunded so future publishes can still proceed.
/// Without an RAII guard, the claim would leak: `remaining` stays
/// decremented and `in_flight` stays incremented forever.
#[tokio::test]
async fn dropped_publish_future_refunds_claim_on_cancellation() {
    use std::time::Duration;

    let bus = EventBus::with_registry(StreamRegistry::new());
    let pattern = TopicPattern::parse("hub/led/r1/state").unwrap();
    // limit=2 so the prime delivery leaves the subscription alive
    // (one slot remaining) for the cancelled publish to claim.
    let mut sub = bus.subscribe(
        pattern,
        SubscribeOpts {
            outbound_capacity: Some(1),
            limit: Some(2),
            slow_consumer_policy: SlowConsumerPolicy::Backpressure,
        },
    );

    // Fill the buffer so the next publish must await capacity.
    let _ = bus.publish(envelope(0)).await.unwrap();

    // Spawn a publish that will claim the (now exhausted) quota slot
    // and then park on `send.await`. Aborting the task and awaiting
    // the JoinHandle ensures the task's stack — including our
    // `ClaimGuard` — has fully unwound.
    let bus_clone = bus.clone();
    let pending = tokio::spawn(async move { bus_clone.publish(envelope(1)).await });

    // Give the spawned task a chance to reach `send.await`.
    tokio::time::sleep(Duration::from_millis(50)).await;
    pending.abort();
    let _ = pending.await;

    // Drain the buffer so the next publish can deliver synchronously.
    let _ = sub.rx.recv().await.expect("primed envelope");

    // Without the cancellation refund, this publish would skip the
    // subscription (it would see `remaining == 0` left behind by the
    // cancelled claim) and report `delivered = 0`.
    let r = bus.publish(envelope(2)).await.unwrap();
    assert_eq!(
        r.delivered, 1,
        "cancelled publish must refund its claim; r={r:?}"
    );
    let env2 = tokio::time::timeout(Duration::from_millis(200), sub.rx.recv())
        .await
        .expect("envelope 2 arrives")
        .expect("envelope present");
    assert_eq!(env2.sequence, 2);
}

/// `DropNewest` events that drop on a full outbound channel
/// must not consume the subscription's quota — matching sync
/// `try_publish` semantics. Scenario from the review: `limit=2`,
/// capacity 1, one event delivered, second dropped while buffer is
/// full, drain, third should still be delivered.
#[tokio::test]
async fn dropnewest_does_not_consume_subscription_quota() {
    let bus = EventBus::with_registry(StreamRegistry::new());
    let pattern = TopicPattern::parse("hub/led/r1/state").unwrap();
    let mut sub = bus.subscribe(
        pattern,
        SubscribeOpts {
            outbound_capacity: Some(1),
            limit: Some(2),
            slow_consumer_policy: SlowConsumerPolicy::DropNewest,
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
        "DropNewest must not consume quota; the third send must deliver"
    );
    assert_eq!(r3.dropped, 0);
}

#[tokio::test]
async fn backpressure_awaits_capacity_instead_of_dropping() {
    let bus = EventBus::with_registry(StreamRegistry::new());
    let pattern = TopicPattern::parse("hub/led/r1/state").unwrap();
    let mut sub = bus.subscribe(
        pattern,
        SubscribeOpts {
            outbound_capacity: Some(1),
            slow_consumer_policy: SlowConsumerPolicy::Backpressure,
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
