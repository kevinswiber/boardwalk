//! Per-subscription sidecar queue backing `SlowConsumerPolicy::Coalesce`.
//!
//! Coalesce subscriptions hold a bounded `VecDeque<EventEnvelope>`
//! protected by a mutex plus a `Notify`. The bus replaces any queued
//! envelope whose payload key matches the incoming envelope; if no
//! match exists the queue tail accepts the envelope until capacity,
//! and overflow without a replacement target falls back to drop-newest.
//! The receiving side awaits the notify and pops from the front, so a
//! consumer that drains slowly still observes the *latest* same-key
//! envelope rather than a backlog.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::Value as Json;
use tokio::sync::Notify;

use super::envelope::EventEnvelope;
use crate::query::FieldPath;

/// Outcome of a coalesce push. The bus translates this into the
/// matching counter on `PublishResult` (or removes the subscription
/// when the receiver side has been dropped).
pub(crate) enum CoalescePushOutcome {
    /// Envelope appended to the queue (no same-key match was found and
    /// the queue had room).
    Pushed,
    /// A queued envelope with the same key was overwritten with the
    /// incoming envelope.
    Replaced,
    /// The queue was full and no same-key match existed; the incoming
    /// envelope was dropped (drop-newest fallback).
    Dropped,
    /// The consumer-side `SubscriptionRx` has been dropped. The bus
    /// treats this like an `mpsc::TrySendError::Closed`: the
    /// subscription is removed lazily on the next publish.
    ReceiverGone,
}

pub(crate) struct CoalesceState {
    queue: Mutex<VecDeque<EventEnvelope>>,
    capacity: usize,
    key_path: FieldPath,
    notify: Notify,
    /// Sender-side close (`unsubscribe` or quota-exhausted removal).
    /// Setting this drains the queue and steers waiters to `None`.
    closed: AtomicBool,
    /// Receiver-side drop. The `SubscriptionRx` flips this on `Drop`
    /// so the bus can reap the subscription lazily.
    receiver_dropped: AtomicBool,
}

impl CoalesceState {
    pub(crate) fn new(capacity: usize, key_path: FieldPath) -> Self {
        let cap = capacity.max(1);
        Self {
            queue: Mutex::new(VecDeque::with_capacity(cap)),
            capacity: cap,
            key_path,
            notify: Notify::new(),
            closed: AtomicBool::new(false),
            receiver_dropped: AtomicBool::new(false),
        }
    }

    /// Push an envelope. Envelopes whose `key_path` does not resolve
    /// (extracted key is `None`) are non-coalescible: they never
    /// replace a queued entry and are not replaced by later
    /// key-missing envelopes. They take a fresh slot if one is
    /// available; otherwise the push falls back to drop-newest.
    pub(crate) fn push(&self, env: EventEnvelope) -> CoalescePushOutcome {
        if self.receiver_dropped.load(Ordering::Acquire) {
            return CoalescePushOutcome::ReceiverGone;
        }
        let incoming_key = extract_key(&env, &self.key_path);
        let mut q = self.queue.lock().unwrap();
        if let Some(incoming) = &incoming_key {
            for slot in q.iter_mut() {
                if let Some(slot_key) = extract_key(slot, &self.key_path)
                    && &slot_key == incoming
                {
                    *slot = env;
                    drop(q);
                    self.notify.notify_one();
                    return CoalescePushOutcome::Replaced;
                }
            }
        }
        if q.len() < self.capacity {
            q.push_back(env);
            drop(q);
            self.notify.notify_one();
            CoalescePushOutcome::Pushed
        } else {
            CoalescePushOutcome::Dropped
        }
    }

    /// Block until the queue has an item to deliver or the sender side
    /// signals close. Uses `Notified::enable` to register intent
    /// before the queue-empty / closed-flag check so a concurrent
    /// `close()` lands at most one cycle later: the registered future
    /// captures the wake and re-checks `closed` on the next loop.
    pub(crate) async fn recv(&self) -> Option<EventEnvelope> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut q = self.queue.lock().unwrap();
                if let Some(env) = q.pop_front() {
                    return Some(env);
                }
                if self.closed.load(Ordering::Acquire) {
                    return None;
                }
            }
            notified.await;
        }
    }

    pub(crate) fn try_recv(&self) -> Result<EventEnvelope, super::bus::TryRecvError> {
        let mut q = self.queue.lock().unwrap();
        if let Some(env) = q.pop_front() {
            return Ok(env);
        }
        if self.closed.load(Ordering::Acquire) {
            Err(super::bus::TryRecvError::Disconnected)
        } else {
            Err(super::bus::TryRecvError::Empty)
        }
    }

    /// Sender-side close. Sets the `closed` flag and wakes any
    /// pending waiter so it observes the flag on its next loop. The
    /// `recv` future's enable-before-check pattern guarantees that
    /// even if `close` races with a waiter that just unlocked the
    /// queue, the next `notified.await` resolves immediately.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Receiver-side drop signal. Flips the `receiver_dropped` flag
    /// so the next `push` reports `ReceiverGone`. Also wakes any
    /// pending waiter (the receiver is gone by definition, but a
    /// stray clone of the recv future should not hang).
    pub(crate) fn mark_receiver_dropped(&self) {
        self.receiver_dropped.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

/// Extract a coalesce key from an envelope. Paths rooted at `data` are
/// the common shape (e.g. `data.jobId`); they walk `env.data` directly
/// to avoid serializing the full envelope. Any other root falls back
/// to a JSON view of the envelope.
fn extract_key(env: &EventEnvelope, path: &FieldPath) -> Option<Json> {
    let segs = path.segments();
    if let Some(first) = segs.first()
        && first == "data"
    {
        let mut cur = &env.data;
        for seg in &segs[1..] {
            match cur {
                Json::Object(m) => {
                    let v = m.get(seg)?;
                    cur = v
                }
                _ => return None,
            }
        }
        return Some(cur.clone());
    }
    let full = serde_json::to_value(env).ok()?;
    path.extract(&full).cloned()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::events::envelope::{ENVELOPE_VERSION, EventId, NodeId, StreamId};

    fn env(job_id: &str, percent: u32) -> EventEnvelope {
        let node_id = NodeId::new("hub");
        let stream_id = StreamId::for_resource(&node_id, "q", "progress");
        EventEnvelope {
            envelope_version: ENVELOPE_VERSION,
            event_id: EventId::from_raw(format!("{job_id}-{percent}")),
            node_id,
            resource_id: "q".into(),
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
            data: json!({"jobId": job_id, "percent": percent}),
        }
    }

    #[tokio::test]
    async fn replace_collapses_same_key_to_latest() {
        let state = CoalesceState::new(1, FieldPath::parse("data.jobId"));
        assert!(matches!(
            state.push(env("a", 10)),
            CoalescePushOutcome::Pushed
        ));
        assert!(matches!(
            state.push(env("a", 20)),
            CoalescePushOutcome::Replaced
        ));
        let got = state.recv().await.unwrap();
        assert_eq!(got.data["percent"], 20);
    }

    #[tokio::test]
    async fn distinct_keys_both_persist_up_to_capacity() {
        let state = CoalesceState::new(2, FieldPath::parse("data.jobId"));
        assert!(matches!(
            state.push(env("a", 10)),
            CoalescePushOutcome::Pushed
        ));
        assert!(matches!(
            state.push(env("b", 20)),
            CoalescePushOutcome::Pushed
        ));
        let first = state.recv().await.unwrap();
        let second = state.recv().await.unwrap();
        let mut ids: Vec<_> = [&first, &second]
            .iter()
            .map(|e| e.data["jobId"].as_str().unwrap().to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn overflow_without_replacement_target_drops() {
        let state = CoalesceState::new(1, FieldPath::parse("data.jobId"));
        assert!(matches!(
            state.push(env("a", 10)),
            CoalescePushOutcome::Pushed
        ));
        assert!(matches!(
            state.push(env("b", 20)),
            CoalescePushOutcome::Dropped
        ));
    }

    /// `recv` must surface `None` even when `close()` lands in the
    /// gap between the queue-empty check and the notify await. Before
    /// the fix, `notify_waiters()` would wake zero pending waiters
    /// (the future had not yet registered), and the receiver would
    /// hang forever on a closed subscription.
    #[tokio::test]
    async fn recv_returns_none_when_close_races_with_await() {
        use std::sync::Arc;
        use std::time::Duration;

        let state = Arc::new(CoalesceState::new(1, FieldPath::parse("data.jobId")));
        let receiver = {
            let state = state.clone();
            tokio::spawn(async move { state.recv().await })
        };
        // Give the receiver enough time to clear the queue check and
        // start (or be near) the notify await.
        tokio::time::sleep(Duration::from_millis(20)).await;
        state.close();
        let result = tokio::time::timeout(Duration::from_secs(2), receiver)
            .await
            .expect("recv must observe close within timeout")
            .expect("task did not panic");
        assert!(result.is_none(), "closed subscription must yield None");
    }

    /// Envelopes whose `key_path` does not resolve are non-coalescible:
    /// they must not collapse into one slot just because both extract
    /// to `None`.
    #[tokio::test]
    async fn missing_keys_are_not_collapsed() {
        let state = CoalesceState::new(2, FieldPath::parse("data.absent"));
        assert!(matches!(
            state.push(env("a", 10)),
            CoalescePushOutcome::Pushed
        ));
        assert!(matches!(
            state.push(env("b", 20)),
            CoalescePushOutcome::Pushed
        ));
    }

    /// Marking the receiver as dropped causes `push` to refuse further
    /// envelopes so the bus can reap the dead subscription.
    #[tokio::test]
    async fn push_reports_receiver_gone_after_drop_signal() {
        let state = CoalesceState::new(2, FieldPath::parse("data.jobId"));
        state.mark_receiver_dropped();
        assert!(matches!(
            state.push(env("a", 10)),
            CoalescePushOutcome::ReceiverGone
        ));
    }
}
