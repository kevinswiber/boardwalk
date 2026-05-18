//! Node-scoped registry that hands out `(eventId, sequence)` pairs per
//! [`StreamId`] and owns the reverse `EventId -> StreamId` index.
//!
//! The reverse index is bounded by replay-cache retention: when the
//! per-stream ring in [`super::replay`] drops an envelope, it calls
//! [`StreamRegistry::evict`] so a recall on the dropped id returns
//! `None` rather than lingering forever. The per-stream sequence map
//! grows with distinct stream cardinality, which is itself bounded by
//! the resource registry.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::envelope::{EventId, StreamId};

#[derive(Clone, Default)]
pub struct StreamRegistry {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    per_stream: Mutex<HashMap<StreamId, Arc<StreamState>>>,
    by_event_id: Mutex<HashMap<EventId, StreamId>>,
}

struct StreamState {
    next_sequence: AtomicU64,
}

pub struct Allocated {
    pub event_id: EventId,
    pub sequence: u64,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pointer-identity check: do `self` and `other` share the same
    /// inner `Arc`? Used by tests to assert that the `Core`, `EventBus`,
    /// and replay cache all reference the same registry instance —
    /// otherwise eviction wouldn't prune what publishing minted.
    pub fn same_instance(&self, other: &StreamRegistry) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn allocate(&self, stream_id: &StreamId) -> Allocated {
        let state = {
            let mut map = self.inner.per_stream.lock().unwrap();
            map.entry(stream_id.clone())
                .or_insert_with(|| {
                    Arc::new(StreamState {
                        next_sequence: AtomicU64::new(1),
                    })
                })
                .clone()
        };
        let sequence = state.next_sequence.fetch_add(1, Ordering::SeqCst);
        let event_id = EventId::from_raw(format!("{}-{}", stream_id.as_str(), sequence));
        self.inner
            .by_event_id
            .lock()
            .unwrap()
            .insert(event_id.clone(), stream_id.clone());
        Allocated { event_id, sequence }
    }

    pub fn stream_for(&self, event_id: &EventId) -> Option<StreamId> {
        self.inner
            .by_event_id
            .lock()
            .unwrap()
            .get(event_id)
            .cloned()
    }

    /// Drop a reverse-index entry. Called by the replay cache (Task
    /// 6.1) when the corresponding envelope is evicted from its ring.
    pub fn evict(&self, event_id: &EventId) {
        self.inner.by_event_id.lock().unwrap().remove(event_id);
    }

    pub fn active_streams(&self) -> usize {
        self.inner.per_stream.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::envelope::NodeId;

    fn sid(stream: &str) -> StreamId {
        StreamId::for_resource(&NodeId::new("hub"), "abc", stream)
    }

    #[test]
    fn allocate_returns_monotonic_sequence_per_stream_starting_at_one() {
        let r = StreamRegistry::new();
        let s = sid("state");
        assert_eq!(r.allocate(&s).sequence, 1);
        assert_eq!(r.allocate(&s).sequence, 2);
        assert_eq!(r.allocate(&s).sequence, 3);
    }

    #[test]
    fn allocate_sequences_are_independent_per_stream() {
        let r = StreamRegistry::new();
        let a = sid("state");
        let b = sid("temperature");
        assert_eq!(r.allocate(&a).sequence, 1);
        assert_eq!(r.allocate(&b).sequence, 1);
        assert_eq!(r.allocate(&a).sequence, 2);
        assert_eq!(r.allocate(&b).sequence, 2);
        assert_eq!(r.allocate(&a).sequence, 3);
        assert_eq!(r.allocate(&b).sequence, 3);
    }

    #[test]
    fn event_id_is_unique_per_allocation() {
        let r = StreamRegistry::new();
        let s = sid("state");
        let mut ids = std::collections::HashSet::new();
        for _ in 0..100 {
            assert!(ids.insert(r.allocate(&s).event_id));
        }
    }

    #[test]
    fn stream_for_event_id_returns_stream_id_after_allocation() {
        let r = StreamRegistry::new();
        let s = sid("state");
        let alloc = r.allocate(&s);
        assert_eq!(r.stream_for(&alloc.event_id), Some(s));
    }

    #[test]
    fn stream_for_unknown_event_id_returns_none() {
        let r = StreamRegistry::new();
        assert!(r.stream_for(&EventId::from_raw("nope")).is_none());
    }

    #[tokio::test]
    async fn allocate_concurrent_sequence_is_strictly_monotonic() {
        let r = StreamRegistry::new();
        let s = sid("state");
        let mut handles = Vec::new();
        for _ in 0..8 {
            let r = r.clone();
            let s = s.clone();
            handles.push(tokio::spawn(async move {
                let mut seqs = Vec::with_capacity(100);
                for _ in 0..100 {
                    seqs.push(r.allocate(&s).sequence);
                }
                seqs
            }));
        }
        let mut all = Vec::with_capacity(800);
        for h in handles {
            all.extend(h.await.unwrap());
        }
        all.sort_unstable();
        let expected: Vec<u64> = (1..=800).collect();
        assert_eq!(all, expected);
    }

    #[test]
    fn active_streams_counts_distinct_stream_ids() {
        let r = StreamRegistry::new();
        r.allocate(&sid("a"));
        r.allocate(&sid("b"));
        r.allocate(&sid("c"));
        r.allocate(&sid("a"));
        assert_eq!(r.active_streams(), 3);
    }

    #[test]
    fn evict_removes_reverse_index_entry() {
        let r = StreamRegistry::new();
        let s = sid("state");
        let a = r.allocate(&s);
        assert_eq!(r.stream_for(&a.event_id), Some(s.clone()));
        r.evict(&a.event_id);
        assert!(r.stream_for(&a.event_id).is_none());
        // Per-stream sequence advances regardless of reverse-index eviction.
        assert_eq!(r.allocate(&s).sequence, 2);
    }

    #[test]
    fn evict_unknown_event_id_is_noop() {
        let r = StreamRegistry::new();
        r.allocate(&sid("state"));
        r.evict(&EventId::from_raw("nope"));
        assert_eq!(r.active_streams(), 1);
    }
}
