//! Canonical runtime envelope for events fanned out by the event bus.

use serde::{Deserialize, Serialize};

pub const ENVELOPE_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEnvelope {
    pub envelope_version: u8,
    pub event_id: EventId,
    pub node_id: NodeId,
    pub resource_id: String,
    pub resource_kind: String,
    pub resource_version: u32,
    pub stream_id: StreamId,
    pub stream: String,
    pub sequence: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: time::OffsetDateTime,
    pub payload_kind: String,
    pub payload_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<CorrelationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<CausationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_context: Option<TraceContext>,
    pub data: serde_json::Value,
}

impl EventEnvelope {
    pub const ENVELOPE_VERSION: u8 = ENVELOPE_VERSION;

    pub fn topic(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.node_id.as_str(),
            self.resource_kind,
            self.resource_id,
            self.stream
        )
    }

    pub fn timestamp_ms(&self) -> i64 {
        (self.timestamp.unix_timestamp_nanos() / 1_000_000) as i64
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(String);

impl EventId {
    pub fn from_raw(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamId(String);

impl StreamId {
    pub fn for_resource(node: &NodeId, resource_id: &str, stream: &str) -> Self {
        Self(format!(
            "bw://{}/resources/{}/streams/{}",
            node.as_str(),
            resource_id,
            stream
        ))
    }
    pub fn from_raw(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CorrelationId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CausationId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceContext {
    pub traceparent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use time::Duration;

    use super::*;

    fn sample_envelope() -> EventEnvelope {
        EventEnvelope {
            envelope_version: ENVELOPE_VERSION,
            event_id: EventId::from_raw("evt-1"),
            node_id: NodeId::new("hub"),
            resource_id: "abc".to_string(),
            resource_kind: "led".to_string(),
            resource_version: 1,
            stream_id: StreamId::for_resource(&NodeId::new("hub"), "abc", "state"),
            stream: "state".to_string(),
            sequence: 1,
            timestamp: time::OffsetDateTime::UNIX_EPOCH + Duration::milliseconds(1234),
            payload_kind: "resource.state.changed".to_string(),
            payload_version: 1,
            payload_schema: None,
            correlation_id: None,
            causation_id: None,
            trace_context: None,
            data: json!("on"),
        }
    }

    #[test]
    fn envelope_default_envelope_version_is_one() {
        assert_eq!(EventEnvelope::ENVELOPE_VERSION, 1);
        assert_eq!(ENVELOPE_VERSION, 1);
    }

    #[test]
    fn event_id_is_opaque_string_with_only_as_str_accessor() {
        let id = EventId::from_raw("abc-1");
        assert_eq!(id.as_str(), "abc-1");
        // No Deref<str>: `&*id` would not compile if Deref existed.
        // We cannot assert "doesn't implement Deref" at runtime,
        // but the source above offers no such impl, so any future
        // addition would require a deliberate edit.
    }

    #[test]
    fn stream_id_for_resource_builds_bw_uri() {
        let s = StreamId::for_resource(
            &NodeId::new("hub"),
            "00000000-0000-0000-0000-000000000000",
            "state",
        );
        assert_eq!(
            s.as_str(),
            "bw://hub/resources/00000000-0000-0000-0000-000000000000/streams/state"
        );
    }

    #[test]
    fn envelope_topic_derives_node_kind_id_stream() {
        let env = sample_envelope();
        assert_eq!(env.topic(), "hub/led/abc/state");
    }

    #[test]
    fn envelope_timestamp_ms_renders_epoch_milliseconds() {
        let env = sample_envelope();
        assert_eq!(env.timestamp_ms(), 1234);
    }

    #[test]
    fn envelope_serde_round_trips_canonical_fields() {
        let env = sample_envelope();
        let v = serde_json::to_value(&env).expect("serialize");
        let back: EventEnvelope = serde_json::from_value(v).expect("deserialize");
        assert_eq!(back.event_id, env.event_id);
        assert_eq!(back.stream_id, env.stream_id);
        assert_eq!(back.sequence, env.sequence);
        assert_eq!(back.payload_kind, env.payload_kind);
        assert_eq!(back.payload_version, env.payload_version);
        assert_eq!(back.node_id, env.node_id);
        assert_eq!(back.resource_id, env.resource_id);
        assert_eq!(back.resource_kind, env.resource_kind);
    }

    #[test]
    fn trace_context_optional_field_omitted_when_none() {
        let env = sample_envelope();
        let v = serde_json::to_value(&env).expect("serialize");
        let obj = v.as_object().expect("envelope serializes to a JSON object");
        assert!(
            !obj.contains_key("traceContext"),
            "traceContext key must be omitted when None; got keys {:?}",
            obj.keys().collect::<Vec<_>>()
        );
    }
}
