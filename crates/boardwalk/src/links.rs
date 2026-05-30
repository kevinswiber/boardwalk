//! Typed link builders for Boardwalk HTTP affordances.

use crate::events::{DEFAULT_OUTBOUND_CAPACITY, SlowConsumerPolicy};

#[derive(Debug, Clone)]
pub struct SubscriptionUrl {
    node: String,
    topic: String,
    outbound_capacity: Option<usize>,
    replay: bool,
    slow_consumer_policy: SlowConsumerPolicy,
}

impl SubscriptionUrl {
    pub fn for_resource(
        node: impl Into<String>,
        kind: impl Into<String>,
        resource_id: impl Into<String>,
        stream: impl Into<String>,
    ) -> Self {
        let node = node.into();
        let topic = format!(
            "{}/{}/{}/{}",
            node,
            kind.into(),
            resource_id.into(),
            stream.into()
        );
        Self {
            node,
            topic,
            outbound_capacity: None,
            replay: false,
            slow_consumer_policy: SlowConsumerPolicy::Disconnect,
        }
    }

    pub fn outbound_capacity(mut self, capacity: usize) -> Self {
        self.outbound_capacity = Some(capacity);
        self
    }

    pub fn replay(mut self, replay: bool) -> Self {
        self.replay = replay;
        self
    }

    pub fn slow_consumer(mut self, policy: SlowConsumerPolicy) -> Self {
        self.slow_consumer_policy = policy;
        self
    }

    pub fn relative_href(&self) -> String {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("topic", &self.topic);
        if let Some(capacity) = self.outbound_capacity
            && capacity != DEFAULT_OUTBOUND_CAPACITY
        {
            query.append_pair("outboundCapacity", &capacity.to_string());
        }
        if self.replay {
            query.append_pair("replay", "true");
        }
        if self.slow_consumer_policy != SlowConsumerPolicy::Disconnect {
            let (value, coalesce_key) = self.slow_consumer_policy.query_value();
            query.append_pair("slowConsumerPolicy", value);
            if let Some(key) = coalesce_key {
                query.append_pair("coalesceKey", &key);
            }
        }
        format!("/servers/{}/events?{}", self.node, query.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::SlowConsumerPolicy;
    use crate::query::FieldPath;

    #[test]
    fn progress_url_matches_expected_contract() {
        let href = SubscriptionUrl::for_resource("runner", "job", "job-1", "progress")
            .outbound_capacity(16)
            .replay(true)
            .slow_consumer(SlowConsumerPolicy::Coalesce {
                key_path: FieldPath::parse("data.coalesceKey"),
            })
            .relative_href();

        assert!(href.starts_with("/servers/runner/events?"));
        assert!(href.contains("topic=runner%2Fjob%2Fjob-1%2Fprogress"));
        assert!(href.contains("outboundCapacity=16"));
        assert!(href.contains("replay=true"));
        assert!(href.contains("slowConsumerPolicy=coalesce"));
        assert!(href.contains("coalesceKey=data.coalesceKey"));
    }

    #[test]
    fn url_encodes_topic_special_chars() {
        let href =
            SubscriptionUrl::for_resource("runner", "job", "id&weird", "progress").relative_href();

        assert!(href.contains("topic=runner%2Fjob%2Fid%26weird%2Fprogress"));
        assert!(
            !href.contains("id&weird"),
            "topic query value should be URL-encoded: {href}"
        );
    }
}
