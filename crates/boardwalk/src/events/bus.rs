use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use super::envelope::{EventEnvelope, StreamId};
use super::policy::{
    DEFAULT_MAX_EVENT_SIZE_BYTES, OverflowPolicy, PublishError, PublishResult, StreamSafety,
    SubscribeOpts,
};
use super::replay::{DEFAULT_REPLAY_CAPACITY, StreamReplayCache};
use super::sequencer::StreamRegistry;
use super::topic::TopicPattern;

pub const REASON_SLOW_CONSUMER: &str = "slow_consumer";

/// Notice delivered out-of-band when a `Lossless` subscriber is
/// disconnected because its bounded queue filled up. WS / NDJSON
/// forwarders use it to emit a final `stream-gap` to the client over
/// their own out-of-band channel (the regular `rx` is full and cannot
/// carry the gap frame).
#[derive(Debug, Clone)]
pub struct SlowConsumerNotice {
    pub stream_id: Option<StreamId>,
    pub last_delivered_sequence: Option<u64>,
    pub reason: &'static str,
}

pub type SubscriptionId = u64;

pub struct Subscription {
    pub id: SubscriptionId,
    pub topic: TopicPattern,
    pub rx: mpsc::Receiver<EventEnvelope>,
    /// Resolves once when a `Lossless` subscription's queue overflows;
    /// fires *before* the bus removes the entry. WS (5.1) and HTTP
    /// NDJSON (5.4) forwarders `select!` on this alongside `rx.recv()`.
    pub slow_consumer_rx: tokio::sync::oneshot::Receiver<SlowConsumerNotice>,
}

struct SubscriptionInner {
    topic: TopicPattern,
    tx: mpsc::Sender<EventEnvelope>,
    remaining: Option<u64>,
    stream_safety: StreamSafety,
    overflow_policy: OverflowPolicy,
    /// Last successfully delivered `(stream_id, sequence)`. Used to
    /// build the `SlowConsumerNotice` when a subsequent publish finds
    /// the queue full and decides to disconnect.
    last_delivered: Option<(StreamId, u64)>,
    /// One-shot sender used to signal a slow-consumer disconnect.
    /// `take()` ensures we only fire once.
    slow_consumer_notify: Option<tokio::sync::oneshot::Sender<SlowConsumerNotice>>,
}

#[derive(Clone)]
pub struct EventBus {
    inner: Arc<Inner>,
}

struct Inner {
    next_id: AtomicU64,
    subs: Mutex<HashMap<SubscriptionId, SubscriptionInner>>,
    registry: StreamRegistry,
    replay_cache: StreamReplayCache,
    max_event_size: AtomicUsize,
}

impl EventBus {
    /// Construct a bus that shares the given `StreamRegistry`. The
    /// `Core` that owns this bus, the `BusSink`s that mint envelopes,
    /// and the replay cache must all carry clones of the same
    /// registry — otherwise reverse-index pruning prunes a different
    /// map than minting populated.
    pub fn with_registry(registry: StreamRegistry) -> Self {
        Self::with_registry_and_replay_capacity(registry, DEFAULT_REPLAY_CAPACITY)
    }

    /// Test-only constructor: lets fixtures vary the replay capacity
    /// without nudging it to a different `StreamRegistry`.
    pub fn with_registry_and_replay_capacity(
        registry: StreamRegistry,
        replay_capacity: usize,
    ) -> Self {
        let replay_cache = StreamReplayCache::new(replay_capacity, registry.clone());
        Self {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(1),
                subs: Mutex::new(HashMap::new()),
                registry,
                replay_cache,
                max_event_size: AtomicUsize::new(DEFAULT_MAX_EVENT_SIZE_BYTES),
            }),
        }
    }

    /// Convenience for tests/unit paths that don't share a registry
    /// with a `Core`. Construct via `with_registry` everywhere else.
    pub fn new() -> Self {
        Self::with_registry(StreamRegistry::new())
    }

    pub fn stream_registry(&self) -> &StreamRegistry {
        &self.inner.registry
    }

    pub fn replay_cache(&self) -> &StreamReplayCache {
        &self.inner.replay_cache
    }

    /// Override the max serialized event size. Returns `self` for
    /// chaining: `EventBus::new().with_max_event_size(1024)`. Works
    /// on shared clones because the limit lives in an `AtomicUsize`.
    pub fn with_max_event_size(self, limit: usize) -> Self {
        self.inner.max_event_size.store(limit, Ordering::Relaxed);
        self
    }

    pub fn max_event_size(&self) -> usize {
        self.inner.max_event_size.load(Ordering::Relaxed)
    }

    pub fn subscribe(&self, topic: TopicPattern, opts: SubscribeOpts) -> Subscription {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let capacity = opts.resolved_outbound_capacity();
        let (tx, rx) = mpsc::channel::<EventEnvelope>(capacity);
        let (notify_tx, notify_rx) = tokio::sync::oneshot::channel::<SlowConsumerNotice>();
        let mut subs = self.inner.subs.lock().unwrap();
        subs.insert(
            id,
            SubscriptionInner {
                topic: topic.clone(),
                tx,
                remaining: opts.limit,
                stream_safety: opts.stream_safety,
                overflow_policy: opts.overflow_policy,
                last_delivered: None,
                slow_consumer_notify: Some(notify_tx),
            },
        );
        Subscription {
            id,
            topic,
            rx,
            slow_consumer_rx: notify_rx,
        }
    }

    pub fn unsubscribe(&self, id: SubscriptionId) -> bool {
        let mut subs = self.inner.subs.lock().unwrap();
        subs.remove(&id).is_some()
    }

    /// Publish an envelope. Fans out to all matching subscriptions.
    /// Honors `limit` by auto-unsubscribing once a subscription's quota
    /// runs out. Drops events for subscribers whose channel has closed.
    pub fn try_publish(&self, envelope: EventEnvelope) -> Result<PublishResult, PublishError> {
        // TODO: serializing the whole envelope just to measure size is
        // O(payload). High-rate streams may want a cheaper estimate or
        // a transport-layer enforcement instead.
        let limit = self.inner.max_event_size.load(Ordering::Relaxed);
        let size = serde_json::to_vec(&envelope).map(|v| v.len()).unwrap_or(0);
        if size > limit {
            tracing::warn!(
                limit,
                size,
                stream = %envelope.stream_id.as_str(),
                "event exceeds max size; rejected"
            );
            // The BusSink that called `registry.allocate(...)` already
            // populated the reverse-index entry for this event_id.
            // Since we are now rejecting the publish, the entry would
            // otherwise linger forever (replay-cache eviction won't
            // see it). Evict it here so the contract that
            // "reverse-index lifetime is bounded by replay retention"
            // also covers oversized rejects.
            self.inner.registry.evict(&envelope.event_id);
            return Err(PublishError::TooLarge { limit });
        }

        // Record before fan-out so replay queries can rebuild missed
        // events for late subscribers.
        self.inner.replay_cache.record(&envelope);

        let mut to_remove: Vec<SubscriptionId> = Vec::new();
        let mut result = PublishResult::default();
        let topic = envelope.topic();
        {
            let mut subs = self.inner.subs.lock().unwrap();
            for (id, sub) in subs.iter_mut() {
                if !sub.topic.matches_event(&topic, &envelope.data) {
                    continue;
                }
                match sub.tx.try_send(envelope.clone()) {
                    Ok(()) => {
                        result.delivered += 1;
                        sub.last_delivered = Some((envelope.stream_id.clone(), envelope.sequence));
                        if let Some(rem) = sub.remaining.as_mut() {
                            *rem = rem.saturating_sub(1);
                            if *rem == 0 {
                                to_remove.push(*id);
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => match sub.stream_safety {
                        StreamSafety::Lossless => {
                            // Lossless slow-consumer disconnect:
                            // remove the subscription, fire the
                            // out-of-band oneshot so the forwarder
                            // can emit a final `stream-gap`, and
                            // record the id in the publish result.
                            let last = sub.last_delivered.clone();
                            if let Some(notify) = sub.slow_consumer_notify.take() {
                                let _ = notify.send(SlowConsumerNotice {
                                    stream_id: last.as_ref().map(|(s, _)| s.clone()),
                                    last_delivered_sequence: last.as_ref().map(|(_, n)| *n),
                                    reason: REASON_SLOW_CONSUMER,
                                });
                            }
                            to_remove.push(*id);
                            result.disconnected_lossless.push(*id);
                        }
                        StreamSafety::Lossy => match sub.overflow_policy {
                            // `Backpressure` becomes real awaiting
                            // backpressure once the publish path is
                            // async. Today it cannot await, so it
                            // behaves identically to `DropNewest`.
                            OverflowPolicy::Backpressure | OverflowPolicy::DropNewest => {
                                result.dropped += 1;
                            }
                        },
                    },
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        to_remove.push(*id);
                    }
                }
            }
            for id in &to_remove {
                subs.remove(id);
            }
        }
        Ok(result)
    }

    pub fn active_subscriptions(&self) -> usize {
        self.inner.subs.lock().unwrap().len()
    }

    /// Async publish that respects `OverflowPolicy::Backpressure` by
    /// awaiting subscriber queue capacity instead of dropping. For
    /// every other policy/safety combination the behavior matches
    /// `try_publish` exactly.
    pub async fn publish(&self, envelope: EventEnvelope) -> Result<PublishResult, PublishError> {
        let limit = self.inner.max_event_size.load(Ordering::Relaxed);
        let size = serde_json::to_vec(&envelope).map(|v| v.len()).unwrap_or(0);
        if size > limit {
            tracing::warn!(
                limit,
                size,
                stream = %envelope.stream_id.as_str(),
                "event exceeds max size; rejected"
            );
            self.inner.registry.evict(&envelope.event_id);
            return Err(PublishError::TooLarge { limit });
        }

        self.inner.replay_cache.record(&envelope);

        // Snapshot matching subscriptions so we can await sends
        // without holding the std::sync::Mutex across `.await`. Do
        // NOT move the slow-consumer oneshot out at snapshot time:
        // concurrent publishers must each be able to observe (and at
        // most one of them claim) a notify on lossless overflow.
        struct Match {
            id: SubscriptionId,
            tx: mpsc::Sender<EventEnvelope>,
            stream_safety: StreamSafety,
            overflow_policy: OverflowPolicy,
        }

        let topic = envelope.topic();
        let mut matches: Vec<Match> = Vec::new();
        {
            let subs = self.inner.subs.lock().unwrap();
            for (id, sub) in subs.iter() {
                if !sub.topic.matches_event(&topic, &envelope.data) {
                    continue;
                }
                matches.push(Match {
                    id: *id,
                    tx: sub.tx.clone(),
                    stream_safety: sub.stream_safety,
                    overflow_policy: sub.overflow_policy,
                });
            }
        }

        enum SendResult {
            Delivered,
            Dropped,
            DisconnectLossless,
            Closed,
        }

        let mut outcomes: Vec<(SubscriptionId, SendResult)> = Vec::with_capacity(matches.len());
        for m in matches {
            // Fast path: try_send first to avoid yielding when the
            // buffer has room.
            let outcome = match m.tx.try_send(envelope.clone()) {
                Ok(()) => SendResult::Delivered,
                Err(mpsc::error::TrySendError::Closed(_)) => SendResult::Closed,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    match (m.stream_safety, m.overflow_policy) {
                        (StreamSafety::Lossy, OverflowPolicy::Backpressure) => {
                            if m.tx.send(envelope.clone()).await.is_ok() {
                                SendResult::Delivered
                            } else {
                                SendResult::Closed
                            }
                        }
                        (StreamSafety::Lossy, OverflowPolicy::DropNewest) => SendResult::Dropped,
                        (StreamSafety::Lossless, _) => SendResult::DisconnectLossless,
                    }
                }
            };
            outcomes.push((m.id, outcome));
        }

        // Apply outcomes under the lock so the counter decrement, the
        // slow-consumer notify, and the removal are atomic with
        // respect to other publishers.
        let mut result = PublishResult::default();
        {
            let mut subs = self.inner.subs.lock().unwrap();
            for (id, outcome) in outcomes {
                match outcome {
                    SendResult::Delivered => {
                        result.delivered += 1;
                        if let Some(sub) = subs.get_mut(&id) {
                            sub.last_delivered =
                                Some((envelope.stream_id.clone(), envelope.sequence));
                            let should_remove = if let Some(rem) = sub.remaining.as_mut() {
                                *rem = rem.saturating_sub(1);
                                *rem == 0
                            } else {
                                false
                            };
                            if should_remove {
                                subs.remove(&id);
                            }
                        }
                    }
                    SendResult::Dropped => {
                        result.dropped += 1;
                    }
                    SendResult::DisconnectLossless => {
                        // Fire the slow-consumer notify (if still
                        // present) and remove. Taking the notify
                        // under the lock prevents two concurrent
                        // publishers from both firing it.
                        if let Some(sub) = subs.get_mut(&id) {
                            let last = sub.last_delivered.clone();
                            if let Some(notify) = sub.slow_consumer_notify.take() {
                                let _ = notify.send(SlowConsumerNotice {
                                    stream_id: last.as_ref().map(|(s, _)| s.clone()),
                                    last_delivered_sequence: last.as_ref().map(|(_, n)| *n),
                                    reason: REASON_SLOW_CONSUMER,
                                });
                            }
                        }
                        subs.remove(&id);
                        result.disconnected_lossless.push(id);
                    }
                    SendResult::Closed => {
                        subs.remove(&id);
                    }
                }
            }
        }

        Ok(result)
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::events::envelope::{ENVELOPE_VERSION, EventId, NodeId, StreamId};

    fn test_envelope(
        topic_parts: (&str, &str, &str, &str),
        data: serde_json::Value,
    ) -> EventEnvelope {
        let (node, kind, id, stream) = topic_parts;
        let node_id = NodeId::new(node);
        let stream_id = StreamId::for_resource(&node_id, id, stream);
        EventEnvelope {
            envelope_version: ENVELOPE_VERSION,
            event_id: EventId::from_raw("test-1"),
            node_id,
            resource_id: id.into(),
            resource_kind: kind.into(),
            resource_version: 1,
            stream_id,
            stream: stream.into(),
            sequence: 1,
            timestamp: time::OffsetDateTime::UNIX_EPOCH,
            payload_kind: "resource.state.changed".into(),
            payload_version: 1,
            payload_schema: None,
            correlation_id: None,
            causation_id: None,
            trace_context: None,
            data,
        }
    }

    #[tokio::test]
    async fn try_publish_to_matching_subscriber_returns_delivered_one() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/*/state").unwrap();
        let mut sub = bus.subscribe(pattern, SubscribeOpts::default());
        let env = test_envelope(("hub", "led", "abc", "state"), json!("on"));
        let res = bus.try_publish(env).expect("publish ok");
        assert_eq!(res.delivered, 1);
        let got = sub.rx.recv().await.unwrap();
        assert_eq!(got.topic(), "hub/led/abc/state");
    }

    #[tokio::test]
    async fn try_publish_returns_delivered_zero_on_topic_mismatch() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/*/state").unwrap();
        let _sub = bus.subscribe(pattern, SubscribeOpts::default());
        let env = test_envelope(("hub", "led", "abc", "temperature"), json!(1));
        let res = bus.try_publish(env).expect("publish ok");
        assert_eq!(res.delivered, 0);
    }

    #[tokio::test]
    async fn try_publish_respects_subscription_limit_and_auto_unsubscribes() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                limit: Some(2),
                ..Default::default()
            },
        );
        let one = test_envelope(("hub", "led", "abc", "state"), json!("on"));
        let two = test_envelope(("hub", "led", "abc", "state"), json!("off"));
        let three = test_envelope(("hub", "led", "abc", "state"), json!("on"));
        assert_eq!(bus.try_publish(one).unwrap().delivered, 1);
        assert_eq!(bus.try_publish(two).unwrap().delivered, 1);
        assert_eq!(bus.try_publish(three).unwrap().delivered, 0);
        assert_eq!(bus.active_subscriptions(), 0);
    }

    #[tokio::test]
    async fn try_publish_passes_envelope_through_intact() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/*/state").unwrap();
        let mut sub = bus.subscribe(pattern, SubscribeOpts::default());
        let env = test_envelope(("hub", "led", "abc", "state"), json!("on"));
        let expected_event_id = env.event_id.clone();
        let expected_sequence = env.sequence;
        let expected_stream_id = env.stream_id.clone();
        bus.try_publish(env).unwrap();
        let got = sub.rx.recv().await.unwrap();
        assert_eq!(got.event_id, expected_event_id);
        assert_eq!(got.sequence, expected_sequence);
        assert_eq!(got.stream_id, expected_stream_id);
    }

    #[tokio::test]
    async fn topic_pattern_still_matches_via_topic_derivation() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/*/state").unwrap();
        let mut sub = bus.subscribe(pattern, SubscribeOpts::default());
        let env = test_envelope(("hub", "led", "abc", "state"), json!("on"));
        let res = bus.try_publish(env).unwrap();
        assert_eq!(res.delivered, 1);
        let got = sub.rx.recv().await.unwrap();
        assert_eq!(got.node_id.as_str(), "hub");
        assert_eq!(got.resource_kind, "led");
        assert_eq!(got.resource_id, "abc");
        assert_eq!(got.stream, "state");
    }

    #[tokio::test]
    async fn caql_filter_on_envelope_data() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/sensor/*/temp?ql=where data > 85").unwrap();
        let mut sub = bus.subscribe(pattern, SubscribeOpts::default());
        let lo = test_envelope(("hub", "sensor", "abc", "temp"), json!({"data": 50}));
        let hi = test_envelope(("hub", "sensor", "abc", "temp"), json!({"data": 90}));
        assert_eq!(bus.try_publish(lo).unwrap().delivered, 0);
        assert_eq!(bus.try_publish(hi).unwrap().delivered, 1);
        let got = sub.rx.recv().await.unwrap();
        assert_eq!(got.data["data"], 90);
    }

    #[tokio::test]
    async fn bounded_subscriber_default_capacity_is_64() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        // Lossy so a full queue drops rather than disconnects — the
        // shape under test is the capacity bound, not the safety
        // policy (covered separately by the lossless tests).
        let _sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                stream_safety: StreamSafety::Lossy,
                ..Default::default()
            },
        );

        for i in 0..64 {
            let env = test_envelope(("hub", "led", "abc", "state"), json!({"i": i}));
            let res = bus.try_publish(env).unwrap();
            assert_eq!(
                res.delivered, 1,
                "publish {i} should succeed (default cap 64)"
            );
            assert_eq!(res.dropped, 0);
        }

        // Queue is full now; the 65th publish drops.
        let res = bus
            .try_publish(test_envelope(
                ("hub", "led", "abc", "state"),
                json!("overflow"),
            ))
            .unwrap();
        assert_eq!(res.delivered, 0);
        assert_eq!(res.dropped, 1);
    }

    #[tokio::test]
    async fn bounded_subscriber_custom_capacity_honored() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(4),
                stream_safety: StreamSafety::Lossy,
                ..Default::default()
            },
        );

        for i in 0..4 {
            let res = bus
                .try_publish(test_envelope(
                    ("hub", "led", "abc", "state"),
                    json!({"i": i}),
                ))
                .unwrap();
            assert_eq!(res.delivered, 1);
        }
        let res = bus
            .try_publish(test_envelope(
                ("hub", "led", "abc", "state"),
                json!("overflow"),
            ))
            .unwrap();
        assert_eq!(res.delivered, 0);
        assert_eq!(res.dropped, 1);
    }

    #[tokio::test]
    async fn bounded_subscriber_drops_count_in_publish_result_for_lossy() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(1),
                stream_safety: StreamSafety::Lossy,
                ..Default::default()
            },
        );

        // First publish lands. Subsequent publishes find the queue
        // already full → dropped=1 each (Lossy keeps the subscription).
        let res = bus
            .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("on")))
            .unwrap();
        assert_eq!(res.delivered, 1);

        for _ in 0..3 {
            let res = bus
                .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("more")))
                .unwrap();
            assert_eq!(res.delivered, 0);
            assert_eq!(res.dropped, 1);
        }
    }

    #[tokio::test]
    async fn lossless_full_queue_disconnects_subscriber_in_publish_result() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(2),
                stream_safety: StreamSafety::Lossless,
                ..Default::default()
            },
        );
        let id = sub.id;
        let _hold = sub; // do not read

        // First two publishes fill the queue.
        for _ in 0..2 {
            let res = bus
                .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("x")))
                .unwrap();
            assert_eq!(res.delivered, 1);
        }
        // Third publish triggers Lossless disconnect.
        let res = bus
            .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("y")))
            .unwrap();
        assert_eq!(res.delivered, 0);
        assert_eq!(res.dropped, 0);
        assert_eq!(res.disconnected_lossless, vec![id]);
        assert_eq!(bus.active_subscriptions(), 0);
    }

    #[tokio::test]
    async fn lossy_full_queue_does_not_disconnect() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(2),
                stream_safety: StreamSafety::Lossy,
                ..Default::default()
            },
        );

        for _ in 0..2 {
            let res = bus
                .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("x")))
                .unwrap();
            assert_eq!(res.delivered, 1);
        }
        let mut total_dropped = 0usize;
        for _ in 0..3 {
            let res = bus
                .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("y")))
                .unwrap();
            total_dropped += res.dropped;
            assert!(res.disconnected_lossless.is_empty());
        }
        assert_eq!(total_dropped, 3);
        assert_eq!(bus.active_subscriptions(), 1);
    }

    #[tokio::test]
    async fn try_publish_rejects_oversized_envelope() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(pattern, SubscribeOpts::default());

        let env = test_envelope(("hub", "led", "abc", "state"), json!("x".repeat(300_000)));
        match bus.try_publish(env) {
            Err(PublishError::TooLarge { limit }) => {
                assert_eq!(limit, 256 * 1024)
            }
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn try_publish_accepts_envelope_just_under_limit() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(pattern, SubscribeOpts::default());

        let env = test_envelope(("hub", "led", "abc", "state"), json!("x".repeat(100)));
        let res = bus.try_publish(env).expect("under-cap publish ok");
        assert_eq!(res.delivered, 1);
    }

    #[tokio::test]
    async fn try_publish_too_large_evicts_reverse_index_entry() {
        // The BusSink path always allocates from the registry before
        // calling try_publish. Rejecting the publish must not leave
        // the reverse-index entry orphaned (would violate the "reverse
        // index lifetime bounded by replay retention" contract).
        let bus = EventBus::new().with_max_event_size(64);
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(pattern, SubscribeOpts::default());

        let alloc = bus.stream_registry().allocate(&StreamId::for_resource(
            &NodeId::new("hub"),
            "abc",
            "state",
        ));
        let event_id = alloc.event_id.clone();
        let stream_id = StreamId::for_resource(&NodeId::new("hub"), "abc", "state");
        assert_eq!(
            bus.stream_registry().stream_for(&event_id),
            Some(stream_id.clone())
        );

        let env = EventEnvelope {
            envelope_version: ENVELOPE_VERSION,
            event_id: event_id.clone(),
            node_id: NodeId::new("hub"),
            resource_id: "abc".into(),
            resource_kind: "led".into(),
            resource_version: 1,
            stream_id,
            stream: "state".into(),
            sequence: alloc.sequence,
            timestamp: time::OffsetDateTime::UNIX_EPOCH,
            payload_kind: "resource.state.changed".into(),
            payload_version: 1,
            payload_schema: None,
            correlation_id: None,
            causation_id: None,
            trace_context: None,
            data: json!("x".repeat(200)),
        };
        assert!(matches!(
            bus.try_publish(env),
            Err(PublishError::TooLarge { .. })
        ));
        assert!(
            bus.stream_registry().stream_for(&event_id).is_none(),
            "reverse-index entry must be evicted when try_publish rejects on size"
        );
    }

    #[tokio::test]
    async fn event_bus_with_custom_max_event_size() {
        let bus = EventBus::new().with_max_event_size(1024);
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(pattern, SubscribeOpts::default());

        let env = test_envelope(("hub", "led", "abc", "state"), json!("x".repeat(2_000)));
        match bus.try_publish(env) {
            Err(PublishError::TooLarge { limit }) => assert_eq!(limit, 1024),
            other => panic!("expected TooLarge {{ limit: 1024 }}, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lossy_drop_newest_counts_dropped_events() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(1),
                stream_safety: StreamSafety::Lossy,
                overflow_policy: OverflowPolicy::DropNewest,
                ..Default::default()
            },
        );

        let mut total_dropped = 0usize;
        for i in 0..5 {
            let res = bus
                .try_publish(test_envelope(
                    ("hub", "led", "abc", "state"),
                    json!({"i": i}),
                ))
                .unwrap();
            total_dropped += res.dropped;
        }
        assert_eq!(total_dropped, 4);
    }

    #[tokio::test]
    async fn lossy_drop_newest_subscription_stays_alive_after_drop() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(1),
                stream_safety: StreamSafety::Lossy,
                overflow_policy: OverflowPolicy::DropNewest,
                ..Default::default()
            },
        );

        for _ in 0..5 {
            let res = bus
                .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("x")))
                .unwrap();
            assert!(res.disconnected_lossless.is_empty());
        }
        assert_eq!(bus.active_subscriptions(), 1);
    }

    #[tokio::test]
    async fn lossy_backpressure_currently_behaves_like_drop_newest() {
        // `Backpressure` becomes real awaiting backpressure once the
        // publish path is async; today it cannot await, so it behaves
        // identically to `DropNewest`. This test pins that asymmetry
        // deliberately so a future async path has a clear contract to
        // change.
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(1),
                stream_safety: StreamSafety::Lossy,
                overflow_policy: OverflowPolicy::Backpressure,
                ..Default::default()
            },
        );

        let mut total_dropped = 0usize;
        for _ in 0..5 {
            let res = bus
                .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("x")))
                .unwrap();
            total_dropped += res.dropped;
        }
        assert_eq!(total_dropped, 4);
    }

    #[tokio::test]
    async fn lossless_overrides_overflow_policy_to_disconnect() {
        // Type system allows a Lossless + DropNewest combination, but
        // safety wins. Disconnect fires regardless of overflow policy.
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(1),
                stream_safety: StreamSafety::Lossless,
                overflow_policy: OverflowPolicy::DropNewest,
                ..Default::default()
            },
        );
        let id = sub.id;
        let _hold = sub;

        bus.try_publish(test_envelope(("hub", "led", "abc", "state"), json!("on")))
            .unwrap();
        let res = bus
            .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("off")))
            .unwrap();
        assert_eq!(res.disconnected_lossless, vec![id]);
        assert_eq!(res.dropped, 0);
        assert_eq!(bus.active_subscriptions(), 0);
    }

    #[tokio::test]
    async fn lossless_disconnect_fires_slow_consumer_notice() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let mut sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(1),
                stream_safety: StreamSafety::Lossless,
                ..Default::default()
            },
        );

        // First publish fills the queue with sequence=1.
        let env1 = test_envelope(("hub", "led", "abc", "state"), json!("on"));
        let stream_id = env1.stream_id.clone();
        bus.try_publish(env1).unwrap();

        // Second publish triggers Lossless disconnect + notice.
        bus.try_publish(test_envelope(("hub", "led", "abc", "state"), json!("off")))
            .unwrap();

        let notice = (&mut sub.slow_consumer_rx)
            .await
            .expect("slow_consumer_rx resolves");
        assert_eq!(notice.reason, REASON_SLOW_CONSUMER);
        assert_eq!(notice.stream_id.as_ref(), Some(&stream_id));
        assert_eq!(notice.last_delivered_sequence, Some(1));
    }

    #[tokio::test]
    async fn bounded_subscriber_recovers_after_consumer_drains() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let mut sub = bus.subscribe(
            pattern,
            SubscribeOpts {
                outbound_capacity: Some(2),
                stream_safety: StreamSafety::Lossy,
                ..Default::default()
            },
        );

        for _ in 0..2 {
            assert_eq!(
                bus.try_publish(test_envelope(("hub", "led", "abc", "state"), json!("x")))
                    .unwrap()
                    .delivered,
                1
            );
        }
        // Queue full now.
        let res = bus
            .try_publish(test_envelope(("hub", "led", "abc", "state"), json!("y")))
            .unwrap();
        assert_eq!(res.dropped, 1);

        // Drain and try again.
        sub.rx.recv().await.unwrap();
        sub.rx.recv().await.unwrap();
        let res = bus
            .try_publish(test_envelope(
                ("hub", "led", "abc", "state"),
                json!("recovered"),
            ))
            .unwrap();
        assert_eq!(res.delivered, 1);
        assert_eq!(res.dropped, 0);
    }
}
