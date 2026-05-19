//! `OverflowPolicy::Coalesce` performs iterable replace-by-key in a
//! per-subscription sidecar queue. It is a real policy, not a synonym
//! for `DropNewest`: when the queue contains an entry whose
//! `key_path`-extracted key matches the incoming envelope, the old
//! entry is replaced in place and `PublishResult.coalesced` increments.
//! Lossless safety overrides Coalesce (still disconnects on overflow).
//!
//! These tests are the failing pins for the task that introduces
//! `OverflowPolicy::Coalesce`. Until that variant exists they fail to
//! compile, which is the intended red signal.

use boardwalk::events::{
    ENVELOPE_VERSION, EventBus, EventEnvelope, EventId, NodeId, OverflowPolicy, PublishResult,
    StreamId, StreamSafety, SubscribeOpts, TopicPattern,
};
use boardwalk::query::FieldPath;
use serde_json::json;

fn progress_envelope(job_id: &str, attempt: u32, percent: u32, step: u32) -> EventEnvelope {
    let node_id = NodeId::new("hub");
    let stream_id = StreamId::for_resource(&node_id, "queue-1", "progress");
    EventEnvelope {
        envelope_version: ENVELOPE_VERSION,
        event_id: EventId::from_raw(format!("progress-{job_id}-{attempt}-{percent}")),
        node_id,
        resource_id: "queue-1".into(),
        resource_kind: "job".into(),
        resource_version: 1,
        stream_id,
        stream: "progress".into(),
        sequence: u64::from(percent),
        timestamp: time::OffsetDateTime::UNIX_EPOCH,
        payload_kind: "job.progress".into(),
        payload_version: 1,
        payload_schema: None,
        correlation_id: None,
        causation_id: None,
        trace_context: None,
        data: json!({
            "jobId": job_id,
            "attempt": attempt,
            "percent": percent,
            "step": step,
            "totalSteps": 5,
            "message": "compile",
        }),
    }
}

fn coalesce_opts(capacity: usize, key_path: FieldPath) -> SubscribeOpts {
    SubscribeOpts {
        outbound_capacity: Some(capacity),
        stream_safety: StreamSafety::Lossy,
        overflow_policy: OverflowPolicy::Coalesce { key_path },
        ..Default::default()
    }
}

#[tokio::test]
async fn coalesce_replaces_queued_event_with_same_key() {
    let bus = EventBus::new();
    let pattern = TopicPattern::parse("hub/job/queue-1/progress").unwrap();
    let mut sub = bus.subscribe(pattern, coalesce_opts(1, FieldPath::parse("data.jobId")));

    bus.try_publish(progress_envelope("job-1", 1, 10, 1))
        .expect("publish 10 ok");
    bus.try_publish(progress_envelope("job-1", 1, 20, 2))
        .expect("publish 20 ok");
    bus.try_publish(progress_envelope("job-1", 1, 30, 3))
        .expect("publish 30 ok");

    let env = sub.rx.recv().await.expect("queue has one survivor");
    assert_eq!(env.data["percent"], 30);

    let extra = tokio::time::timeout(std::time::Duration::from_millis(50), sub.rx.recv()).await;
    assert!(
        extra.is_err(),
        "coalesce must collapse same-key entries; got extra {extra:?}"
    );
}

#[tokio::test]
async fn coalesce_keeps_distinct_keys() {
    let bus = EventBus::new();
    let pattern = TopicPattern::parse("hub/job/queue-1/progress").unwrap();
    let mut sub = bus.subscribe(pattern, coalesce_opts(4, FieldPath::parse("data.jobId")));

    bus.try_publish(progress_envelope("job-1", 1, 10, 1))
        .expect("publish job-1 10 ok");
    bus.try_publish(progress_envelope("job-2", 1, 50, 1))
        .expect("publish job-2 50 ok");

    let first = sub.rx.recv().await.unwrap();
    let second = sub.rx.recv().await.unwrap();
    let ids: Vec<&str> = [&first, &second]
        .iter()
        .map(|e| e.data["jobId"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"job-1"));
    assert!(ids.contains(&"job-2"));
}

#[tokio::test]
async fn coalesce_reports_coalesced_count() {
    let bus = EventBus::new();
    let pattern = TopicPattern::parse("hub/job/queue-1/progress").unwrap();
    let _sub = bus.subscribe(pattern, coalesce_opts(1, FieldPath::parse("data.jobId")));

    let first = bus
        .try_publish(progress_envelope("job-1", 1, 10, 1))
        .expect("publish 10 ok");
    assert_eq!(first.delivered, 1);
    assert_eq!(first.coalesced, 0);
    assert_eq!(first.dropped, 0);

    let second = bus
        .try_publish(progress_envelope("job-1", 1, 20, 2))
        .expect("publish 20 ok");
    assert_eq!(second.delivered, 0);
    assert_eq!(second.coalesced, 1);
    assert_eq!(second.dropped, 0);

    let third = bus
        .try_publish(progress_envelope("job-1", 1, 30, 3))
        .expect("publish 30 ok");
    assert_eq!(third.coalesced, 1);
    assert_eq!(third.dropped, 0);
}

#[tokio::test]
async fn coalesce_requires_lossy_stream_safety() {
    let bus = EventBus::new();
    let pattern = TopicPattern::parse("hub/job/queue-1/progress").unwrap();
    let sub = bus.subscribe(
        pattern,
        SubscribeOpts {
            outbound_capacity: Some(1),
            stream_safety: StreamSafety::Lossless,
            overflow_policy: OverflowPolicy::Coalesce {
                key_path: FieldPath::parse("data.jobId"),
            },
            ..Default::default()
        },
    );
    let id = sub.id;
    let _hold = sub;

    bus.try_publish(progress_envelope("job-1", 1, 10, 1))
        .expect("first publish lands");
    let res = bus
        .try_publish(progress_envelope("job-1", 1, 20, 2))
        .expect("second publish ok");
    assert_eq!(
        res.coalesced, 0,
        "lossless must not collapse events under Coalesce"
    );
    assert_eq!(res.disconnected_lossless, vec![id]);
    assert_eq!(bus.active_subscriptions(), 0);
}

#[tokio::test]
async fn coalesce_key_path_matches_job_progress_payload_shape() {
    let env = progress_envelope("job-1", 2, 40, 2);
    let payload = serde_json::to_value(&env).expect("envelope serializes");

    let job_id_path = FieldPath::parse("data.jobId");
    let attempt_path = FieldPath::parse("data.attempt");

    assert_eq!(
        job_id_path.extract(&payload),
        Some(&json!("job-1")),
        "data.jobId must extract the canonical job id used by job progress"
    );
    assert_eq!(
        attempt_path.extract(&payload),
        Some(&json!(2)),
        "data.attempt must extract the attempt number used by job progress"
    );
}

/// Coalesce only sees a `PublishResult.coalesced` counter once the
/// publish result carries one. Keep this trivial pin so a change that
/// renames the field surfaces here too.
#[test]
fn publish_result_carries_coalesced_field() {
    let r = PublishResult::default();
    assert_eq!(r.coalesced, 0);
}

/// When the configured `key_path` does not resolve in an envelope,
/// that envelope is non-coalescible: it must not match other
/// key-missing envelopes (which would otherwise all collapse into a
/// single slot because `None == None`). A capacity-2 queue receiving
/// two distinct-but-key-missing envelopes must drain both.
#[tokio::test]
async fn coalesce_does_not_collapse_envelopes_missing_the_key() {
    let bus = EventBus::new();
    let pattern = TopicPattern::parse("hub/job/queue-1/progress").unwrap();
    let mut sub = bus.subscribe(pattern, coalesce_opts(2, FieldPath::parse("data.absent")));

    bus.try_publish(progress_envelope("job-1", 1, 10, 1))
        .expect("publish first ok");
    bus.try_publish(progress_envelope("job-2", 1, 20, 1))
        .expect("publish second ok");

    let first = sub.rx.recv().await.expect("first envelope arrives");
    let second = sub.rx.recv().await.expect("second envelope arrives");
    let ids: Vec<&str> = [&first, &second]
        .iter()
        .map(|e| e.data["jobId"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"job-1"));
    assert!(ids.contains(&"job-2"));
}

/// Dropping the receiver side of a Coalesce subscription must cause
/// the bus to drop the subscription on the next publish, mirroring the
/// mpsc path's lazy cleanup via `TrySendError::Closed`.
#[tokio::test]
async fn coalesce_drops_subscription_after_receiver_is_dropped() {
    let bus = EventBus::new();
    let pattern = TopicPattern::parse("hub/job/queue-1/progress").unwrap();
    let sub = bus.subscribe(pattern, coalesce_opts(4, FieldPath::parse("data.jobId")));
    assert_eq!(bus.active_subscriptions(), 1);

    drop(sub);

    let res = bus
        .try_publish(progress_envelope("job-1", 1, 10, 1))
        .expect("publish ok");
    assert_eq!(res.delivered, 0);
    assert_eq!(res.coalesced, 0);
    assert_eq!(
        bus.active_subscriptions(),
        0,
        "bus must reap a Coalesce subscription whose receiver was dropped"
    );
}
