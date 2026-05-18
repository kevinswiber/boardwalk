//! In-memory per-stream replay ring.
//!
//! Each `(streamId)` keeps the last `capacity_per_stream` envelopes
//! (default 1000). On eviction the cache calls
//! [`StreamRegistry::evict`] so the reverse `EventId -> StreamId` map
//! does not outlive the ring that justified it. The replay cache and
//! the registry must share the same `Arc<Inner>`; wire them through
//! `EventBus::with_registry`.
//!
//! This is **not** a durable store. A future durable
//! `EventHistoryRepository` will subsume it; the eviction-driven
//! cleanup contract should be preserved there.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use super::envelope::{EventEnvelope, StreamId};
use super::sequencer::StreamRegistry;

pub const DEFAULT_REPLAY_CAPACITY: usize = 1000;

#[derive(Clone)]
pub struct StreamReplayCache {
    inner: Arc<Mutex<Inner>>,
    registry: StreamRegistry,
}

struct Inner {
    per_stream: HashMap<StreamId, VecDeque<EventEnvelope>>,
    capacity_per_stream: usize,
}

impl StreamReplayCache {
    pub fn new(capacity_per_stream: usize, registry: StreamRegistry) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                per_stream: HashMap::new(),
                capacity_per_stream,
            })),
            registry,
        }
    }

    /// Record an envelope. If the per-stream ring is at capacity, the
    /// oldest envelope is evicted and its `event_id` is removed from
    /// the shared `StreamRegistry`'s reverse index.
    pub fn record(&self, envelope: &EventEnvelope) {
        let evicted_event_id = {
            let mut g = self.inner.lock().unwrap();
            let cap = g.capacity_per_stream;
            let q = g.per_stream.entry(envelope.stream_id.clone()).or_default();
            let evicted = if q.len() >= cap {
                q.pop_front().map(|e| e.event_id)
            } else {
                None
            };
            q.push_back(envelope.clone());
            evicted
        };
        if let Some(id) = evicted_event_id {
            self.registry.evict(&id);
        }
    }

    /// Replay envelopes for `stream_id` whose `sequence` is strictly
    /// greater than `after_sequence`, in order.
    pub fn events_after(&self, stream_id: &StreamId, after_sequence: u64) -> Vec<EventEnvelope> {
        let g = self.inner.lock().unwrap();
        match g.per_stream.get(stream_id) {
            None => Vec::new(),
            Some(q) => q
                .iter()
                .filter(|e| e.sequence > after_sequence)
                .cloned()
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::events::envelope::{ENVELOPE_VERSION, EventEnvelope, EventId, NodeId, StreamId};

    fn sid(stream: &str) -> StreamId {
        StreamId::for_resource(&NodeId::new("hub"), "abc", stream)
    }

    fn envelope_at(stream_id: StreamId, sequence: u64) -> EventEnvelope {
        EventEnvelope {
            envelope_version: ENVELOPE_VERSION,
            event_id: EventId::from_raw(format!("evt-{sequence}")),
            node_id: NodeId::new("hub"),
            resource_id: "abc".into(),
            resource_kind: "led".into(),
            resource_version: 1,
            stream_id,
            stream: "state".into(),
            sequence,
            timestamp: time::OffsetDateTime::UNIX_EPOCH,
            payload_kind: "resource.stream.data".into(),
            payload_version: 1,
            payload_schema: None,
            correlation_id: None,
            causation_id: None,
            trace_context: None,
            data: json!({"i": sequence}),
        }
    }

    #[test]
    fn record_then_events_after_returns_in_order() {
        let cache = StreamReplayCache::new(10, StreamRegistry::new());
        let s = sid("state");
        for i in 1..=3 {
            cache.record(&envelope_at(s.clone(), i));
        }
        let got = cache.events_after(&s, 0);
        let seqs: Vec<u64> = got.iter().map(|e| e.sequence).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[test]
    fn events_after_filters_by_sequence() {
        let cache = StreamReplayCache::new(10, StreamRegistry::new());
        let s = sid("state");
        for i in 1..=3 {
            cache.record(&envelope_at(s.clone(), i));
        }
        let got = cache.events_after(&s, 1);
        let seqs: Vec<u64> = got.iter().map(|e| e.sequence).collect();
        assert_eq!(seqs, vec![2, 3]);
    }

    #[test]
    fn events_after_returns_empty_for_unknown_stream() {
        let cache = StreamReplayCache::new(10, StreamRegistry::new());
        let unknown = StreamId::for_resource(&NodeId::new("hub"), "abc", "unknown");
        assert!(cache.events_after(&unknown, 0).is_empty());
    }

    #[test]
    fn ring_caps_at_capacity_dropping_oldest() {
        let cache = StreamReplayCache::new(5, StreamRegistry::new());
        let s = sid("state");
        for i in 1..=8 {
            cache.record(&envelope_at(s.clone(), i));
        }
        let got = cache.events_after(&s, 0);
        let seqs: Vec<u64> = got.iter().map(|e| e.sequence).collect();
        assert_eq!(seqs, vec![4, 5, 6, 7, 8]);
    }

    #[test]
    fn records_for_different_streams_are_independent() {
        let cache = StreamReplayCache::new(10, StreamRegistry::new());
        let a = sid("a");
        let b = sid("b");
        cache.record(&envelope_at(a.clone(), 1));
        cache.record(&envelope_at(a.clone(), 2));
        cache.record(&envelope_at(b.clone(), 1));
        assert_eq!(cache.events_after(&a, 0).len(), 2);
        assert_eq!(cache.events_after(&b, 0).len(), 1);
    }

    #[test]
    fn eviction_calls_registry_evict_for_oldest_event_id() {
        let registry = StreamRegistry::new();
        let cache = StreamReplayCache::new(2, registry.clone());
        let s = sid("state");
        let mut event_ids = Vec::new();
        for _ in 0..3 {
            let alloc = registry.allocate(&s);
            let mut env = envelope_at(s.clone(), alloc.sequence);
            env.event_id = alloc.event_id.clone();
            cache.record(&env);
            event_ids.push(alloc.event_id);
        }
        assert!(
            registry.stream_for(&event_ids[0]).is_none(),
            "oldest event id should be evicted from registry"
        );
        assert_eq!(registry.stream_for(&event_ids[1]), Some(s.clone()));
        assert_eq!(registry.stream_for(&event_ids[2]), Some(s));
    }

    #[test]
    fn eviction_does_not_affect_unrelated_streams() {
        let registry = StreamRegistry::new();
        let cache = StreamReplayCache::new(1, registry.clone());
        let a = sid("a");
        let b = sid("b");

        let alloc_a1 = registry.allocate(&a);
        let mut env = envelope_at(a.clone(), alloc_a1.sequence);
        env.event_id = alloc_a1.event_id.clone();
        cache.record(&env);

        let alloc_b1 = registry.allocate(&b);
        let mut env = envelope_at(b.clone(), alloc_b1.sequence);
        env.event_id = alloc_b1.event_id.clone();
        cache.record(&env);

        let alloc_a2 = registry.allocate(&a);
        let mut env = envelope_at(a.clone(), alloc_a2.sequence);
        env.event_id = alloc_a2.event_id.clone();
        cache.record(&env);

        assert!(
            registry.stream_for(&alloc_a1.event_id).is_none(),
            "a1 should be evicted"
        );
        assert_eq!(registry.stream_for(&alloc_b1.event_id), Some(b));
    }
}
