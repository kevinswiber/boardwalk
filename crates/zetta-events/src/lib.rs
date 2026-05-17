//! Event bus + multiplex WebSocket sub-protocol types.

#![forbid(unsafe_code)]

mod bus;
mod topic;
mod wire;

pub use bus::{EventBus, Subscription, SubscriptionId, SubscribeOpts};
pub use topic::{Segment, TopicPattern, TopicParseError};
pub use wire::{Event, InboundMessage, OutboundMessage, SubscriptionRef};
