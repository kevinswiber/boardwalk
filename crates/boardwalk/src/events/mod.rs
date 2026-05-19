//! Event bus + multiplex WebSocket sub-protocol types.

#![forbid(unsafe_code)]

mod bus;
mod coalesce;
mod envelope;
mod policy;
mod replay;
mod sequencer;
mod topic;
mod wire;

pub use bus::{
    EventBus, REASON_SLOW_CONSUMER, SlowConsumerNotice, Subscription, SubscriptionId,
    SubscriptionRx, TryRecvError,
};
pub use envelope::{
    CausationId, CorrelationId, ENVELOPE_VERSION, EventEnvelope, EventId, NodeId, StreamId,
    TraceContext,
};
pub use policy::{
    DEFAULT_MAX_EVENT_SIZE_BYTES, DEFAULT_OUTBOUND_CAPACITY, PublishError, PublishResult,
    SlowConsumerPolicy, SubscribeOpts,
};
pub use replay::{DEFAULT_REPLAY_CAPACITY, StreamReplayCache};
pub use sequencer::{Allocated, StreamRegistry};
pub use topic::{Segment, TopicParseError, TopicPattern};
pub use wire::{InboundMessage, OutboundMessage, SubscriptionRef};
