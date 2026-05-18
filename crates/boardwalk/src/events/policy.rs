//! Subscriber options and publish-result types.
//!
//! `OverflowPolicy::Coalesce` is intentionally absent: a truthful
//! coalesce policy needs a sidecar queue with iterable-replace, which
//! `mpsc` does not provide. `Backpressure` currently behaves as
//! `DropNewest` because the lift-point publish path is synchronous;
//! a future async publish path will make it real.

use crate::events::SubscriptionId;

pub const DEFAULT_OUTBOUND_CAPACITY: usize = 64;

/// Default cap on the serialized JSON size of a single event. Events
/// exceeding this are rejected at `try_publish` with
/// [`PublishError::TooLarge`].
pub const DEFAULT_MAX_EVENT_SIZE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamSafety {
    #[default]
    Lossless,
    Lossy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverflowPolicy {
    #[default]
    Backpressure,
    DropNewest,
}

#[derive(Debug, Clone, Default)]
pub struct SubscribeOpts {
    pub limit: Option<u64>,
    pub outbound_capacity: Option<usize>,
    pub overflow_policy: OverflowPolicy,
    pub stream_safety: StreamSafety,
}

impl SubscribeOpts {
    pub fn resolved_outbound_capacity(&self) -> usize {
        self.outbound_capacity.unwrap_or(DEFAULT_OUTBOUND_CAPACITY)
    }
}

#[derive(Debug, Default)]
pub struct PublishResult {
    pub delivered: usize,
    pub dropped: usize,
    pub disconnected_lossless: Vec<SubscriptionId>,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("event exceeds max serialized size of {limit} bytes")]
    TooLarge { limit: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_opts_default_is_lossless_backpressure_64_no_limit() {
        let o = SubscribeOpts::default();
        assert!(matches!(o.stream_safety, StreamSafety::Lossless));
        assert!(matches!(o.overflow_policy, OverflowPolicy::Backpressure));
        assert_eq!(o.outbound_capacity, None);
        assert_eq!(o.limit, None);
    }

    #[test]
    fn outbound_capacity_default_is_used_when_none() {
        assert_eq!(SubscribeOpts::default().resolved_outbound_capacity(), 64);
    }

    #[test]
    fn publish_result_default_is_empty() {
        let r = PublishResult::default();
        assert_eq!(r.delivered, 0);
        assert_eq!(r.dropped, 0);
        assert!(r.disconnected_lossless.is_empty());
    }

    #[test]
    fn publish_error_too_large_displays_limit() {
        let s = PublishError::TooLarge { limit: 1024 }.to_string();
        assert!(s.contains("1024"), "missing limit in display: {s}");
        assert!(s.contains("size"), "missing 'size' in display: {s}");
    }

    #[test]
    fn overflow_policy_is_only_two_variants() {
        // Exhaustive match: re-adding a third variant makes this fail
        // to compile — an intentional speed bump because any new
        // variant needs a real implementation, not a thinly-disguised
        // alias.
        fn _exhaust(o: OverflowPolicy) {
            match o {
                OverflowPolicy::Backpressure => {}
                OverflowPolicy::DropNewest => {}
            }
        }
    }
}
