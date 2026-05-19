//! Subscriber options and publish-result types.
//!
//! Slow-consumer policy is the bus's bounded-queue overflow contract.
//! `Disconnect` removes the subscriber and reports a terminal
//! slow-consumer notice. `Backpressure` awaits capacity on async
//! publish, while synchronous `try_publish` behaves like
//! `DropNewest` because it cannot await. `Coalesce` is backed by a
//! per-subscription sidecar queue with iterable replace-by-key.

use crate::events::SubscriptionId;
use crate::query::FieldPath;

pub const DEFAULT_OUTBOUND_CAPACITY: usize = 64;

/// Default cap on the serialized JSON size of a single event. Events
/// exceeding this are rejected at `try_publish` with
/// [`PublishError::TooLarge`].
pub const DEFAULT_MAX_EVENT_SIZE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SlowConsumerPolicy {
    #[default]
    Disconnect,
    Backpressure,
    DropNewest,
    /// Replace any queued envelope whose payload at `key_path`
    /// matches the incoming envelope's; falls back to drop-newest when
    /// the queue is full and no replacement target exists.
    Coalesce {
        key_path: FieldPath,
    },
}

#[derive(Debug, Clone, Default)]
pub struct SubscribeOpts {
    pub limit: Option<u64>,
    pub outbound_capacity: Option<usize>,
    pub slow_consumer_policy: SlowConsumerPolicy,
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
    /// Number of fan-outs that hit a same-key entry already queued on
    /// a `Coalesce` subscription and replaced it in place. `coalesced`
    /// is mutually exclusive with `delivered` on a per-subscription
    /// basis — a coalesced publish never lands in the consumer's queue
    /// as a new item.
    pub coalesced: usize,
    pub disconnected_slow_consumers: Vec<SubscriptionId>,
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
    fn subscribe_opts_default_disconnects_slow_consumers_with_capacity_64_no_limit() {
        let o = SubscribeOpts::default();
        assert!(matches!(
            o.slow_consumer_policy,
            SlowConsumerPolicy::Disconnect
        ));
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
        assert_eq!(r.coalesced, 0);
        assert!(r.disconnected_slow_consumers.is_empty());
    }

    #[test]
    fn publish_error_too_large_displays_limit() {
        let s = PublishError::TooLarge { limit: 1024 }.to_string();
        assert!(s.contains("1024"), "missing limit in display: {s}");
        assert!(s.contains("size"), "missing 'size' in display: {s}");
    }

    #[test]
    fn slow_consumer_policy_variants_are_exhaustively_covered() {
        // Exhaustive match: any new variant makes this fail to compile
        // — an intentional speed bump because a new variant needs a
        // real implementation in the bus, not a thinly-disguised alias.
        fn _exhaust(o: SlowConsumerPolicy) {
            match o {
                SlowConsumerPolicy::Disconnect => {}
                SlowConsumerPolicy::Backpressure => {}
                SlowConsumerPolicy::DropNewest => {}
                SlowConsumerPolicy::Coalesce { .. } => {}
            }
        }
    }
}
