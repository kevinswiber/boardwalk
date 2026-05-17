//! Event bus + multiplex WebSocket sub-protocol types.

#![forbid(unsafe_code)]

mod bus;
mod topic;
mod wire;

pub use bus::{EventBus, SubscribeOpts, Subscription, SubscriptionId};
pub use topic::{Segment, TopicParseError, TopicPattern};
pub use wire::{Event, InboundMessage, OutboundMessage, SubscriptionRef};
