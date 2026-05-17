use serde::{Deserialize, Serialize};

/// A published event.
#[derive(Debug, Clone)]
pub struct Event {
    pub topic: String,
    pub timestamp_ms: i64,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum InboundMessage {
    #[serde(rename = "subscribe")]
    Subscribe {
        topic: String,
        #[serde(default)]
        limit: Option<u64>,
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
}

/// Either a single id or an array (when `?filterMultiple=true`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubscriptionRef {
    Single(u64),
    Multiple(Vec<u64>),
}
