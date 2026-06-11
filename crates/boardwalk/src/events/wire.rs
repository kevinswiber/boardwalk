// missing_docs: this module predates the crate-wide gate; its public
// items still need a documentation sweep (tracked follow-up). New code
// here should be documented anyway.
#![allow(missing_docs)]
use serde::{Deserialize, Serialize};

use super::envelope::{EventId, NodeId, StreamId};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InboundMessage {
    #[serde(rename = "subscribe")]
    Subscribe {
        topic: String,
        #[serde(default)]
        limit: Option<u64>,
        #[serde(default, rename = "outboundCapacity")]
        outbound_capacity: Option<usize>,
    },
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        #[serde(rename = "subscriptionId")]
        subscription_id: u64,
    },
    #[serde(rename = "ping")]
    Ping {
        #[serde(default)]
        data: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)] // Event carries the optional envelope mirror; boxing would obscure the wire shape.
pub enum OutboundMessage {
    #[serde(rename = "subscribe-ack")]
    SubscribeAck {
        timestamp: i64,
        topic: String,
        #[serde(rename = "subscriptionId")]
        subscription_id: u64,
    },
    #[serde(rename = "unsubscribe-ack")]
    UnsubscribeAck {
        timestamp: i64,
        #[serde(rename = "subscriptionId")]
        subscription_id: u64,
    },
    #[serde(rename = "event")]
    Event {
        topic: String,
        #[serde(rename = "subscriptionId")]
        subscription_id: SubscriptionRef,
        timestamp: i64,
        data: serde_json::Value,
        #[serde(rename = "eventId", skip_serializing_if = "Option::is_none")]
        event_id: Option<EventId>,
        #[serde(rename = "streamId", skip_serializing_if = "Option::is_none")]
        stream_id: Option<StreamId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stream: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sequence: Option<u64>,
        #[serde(rename = "nodeId", skip_serializing_if = "Option::is_none")]
        node_id: Option<NodeId>,
        #[serde(rename = "resourceId", skip_serializing_if = "Option::is_none")]
        resource_id: Option<String>,
        #[serde(rename = "resourceKind", skip_serializing_if = "Option::is_none")]
        resource_kind: Option<String>,
        #[serde(rename = "payloadKind", skip_serializing_if = "Option::is_none")]
        payload_kind: Option<String>,
        #[serde(rename = "payloadVersion", skip_serializing_if = "Option::is_none")]
        payload_version: Option<u32>,
        #[serde(rename = "envelopeVersion", skip_serializing_if = "Option::is_none")]
        envelope_version: Option<u8>,
        #[serde(rename = "isoTimestamp", skip_serializing_if = "Option::is_none")]
        iso_timestamp: Option<String>,
    },
    #[serde(rename = "pong")]
    Pong {
        timestamp: i64,
        #[serde(default)]
        data: Option<String>,
    },
    #[serde(rename = "error")]
    Error {
        code: u16,
        timestamp: i64,
        topic: Option<String>,
        message: Option<String>,
        #[serde(rename = "subscriptionId", skip_serializing_if = "Option::is_none")]
        subscription_id: Option<u64>,
    },
    #[serde(rename = "stream-gap")]
    StreamGap {
        timestamp: i64,
        #[serde(rename = "subscriptionId")]
        subscription_id: u64,
        #[serde(rename = "streamId", skip_serializing_if = "Option::is_none")]
        stream_id: Option<StreamId>,
        #[serde(
            rename = "lastDeliveredSequence",
            skip_serializing_if = "Option::is_none"
        )]
        last_delivered_sequence: Option<u64>,
        reason: String,
        terminated: bool,
    },
}

/// Either a single id or an array (when `?filterMultiple=true`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubscriptionRef {
    Single(u64),
    Multiple(Vec<u64>),
}
