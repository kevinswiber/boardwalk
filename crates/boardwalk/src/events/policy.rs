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

#[derive(Debug, thiserror::Error)]
pub enum SlowConsumerPolicyQueryError {
    #[error("coalesceKey requires slowConsumerPolicy=coalesce")]
    UnexpectedCoalesceKey,
    #[error("slowConsumerPolicy=coalesce requires coalesceKey")]
    MissingCoalesceKey,
    #[error("invalid coalesceKey `{key}`: {source}")]
    InvalidCoalesceKey {
        key: String,
        #[source]
        source: crate::query::QueryError,
    },
    #[error(
        "unknown slowConsumerPolicy `{value}`; expected disconnect, backpressure, drop-newest, or coalesce"
    )]
    Unknown { value: String },
}

impl SlowConsumerPolicy {
    pub fn query_value(&self) -> (&'static str, Option<String>) {
        match self {
            Self::Disconnect => ("disconnect", None),
            Self::Backpressure => ("backpressure", None),
            Self::DropNewest => ("drop-newest", None),
            Self::Coalesce { key_path } => ("coalesce", Some(key_path.segments().join("."))),
        }
    }

    pub fn from_query(
        value: &str,
        coalesce_key: Option<&str>,
    ) -> Result<Self, SlowConsumerPolicyQueryError> {
        let normalized = value.replace('_', "-").to_ascii_lowercase();
        match normalized.as_str() {
            "disconnect" => {
                if coalesce_key.is_some() {
                    return Err(SlowConsumerPolicyQueryError::UnexpectedCoalesceKey);
                }
                Ok(Self::Disconnect)
            }
            "backpressure" => {
                if coalesce_key.is_some() {
                    return Err(SlowConsumerPolicyQueryError::UnexpectedCoalesceKey);
                }
                Ok(Self::Backpressure)
            }
            "drop-newest" | "dropnewest" => {
                if coalesce_key.is_some() {
                    return Err(SlowConsumerPolicyQueryError::UnexpectedCoalesceKey);
                }
                Ok(Self::DropNewest)
            }
            "coalesce" => {
                let key = coalesce_key.ok_or(SlowConsumerPolicyQueryError::MissingCoalesceKey)?;
                let key_path = FieldPath::try_parse(key).map_err(|source| {
                    SlowConsumerPolicyQueryError::InvalidCoalesceKey {
                        key: key.to_string(),
                        source,
                    }
                })?;
                Ok(Self::Coalesce { key_path })
            }
            _ => Err(SlowConsumerPolicyQueryError::Unknown {
                value: value.to_string(),
            }),
        }
    }
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
    fn slow_consumer_policy_query_values_are_canonical() {
        assert_eq!(
            SlowConsumerPolicy::Disconnect.query_value(),
            ("disconnect", None)
        );
        assert_eq!(
            SlowConsumerPolicy::Backpressure.query_value(),
            ("backpressure", None)
        );
        assert_eq!(
            SlowConsumerPolicy::DropNewest.query_value(),
            ("drop-newest", None)
        );
        assert_eq!(
            SlowConsumerPolicy::Coalesce {
                key_path: FieldPath::parse("data.x")
            }
            .query_value(),
            ("coalesce", Some("data.x".to_string()))
        );
    }

    #[test]
    fn slow_consumer_policy_query_parser_round_trips_and_validates_keys() {
        for policy in [
            SlowConsumerPolicy::Disconnect,
            SlowConsumerPolicy::Backpressure,
            SlowConsumerPolicy::DropNewest,
            SlowConsumerPolicy::Coalesce {
                key_path: FieldPath::parse("data.coalesceKey"),
            },
        ] {
            let (value, key) = policy.query_value();
            assert_eq!(
                SlowConsumerPolicy::from_query(value, key.as_deref()).unwrap(),
                policy
            );
        }

        assert!(SlowConsumerPolicy::from_query("coalesce", None).is_err());
        assert!(SlowConsumerPolicy::from_query("disconnect", Some("data.x")).is_err());
        assert!(SlowConsumerPolicy::from_query("drop_newest", None).is_ok());
        assert!(SlowConsumerPolicy::from_query("dropnewest", None).is_ok());
        assert!(SlowConsumerPolicy::from_query("bogus", None).is_err());
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
