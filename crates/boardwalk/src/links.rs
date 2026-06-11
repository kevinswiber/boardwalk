//! Typed link builders for Boardwalk HTTP affordances.

// missing_docs: this module predates the crate-wide gate; its public
// items still need a documentation sweep (tracked follow-up). New code
// here should be documented anyway.
#![allow(missing_docs)]
use crate::events::{DEFAULT_OUTBOUND_CAPACITY, SlowConsumerPolicy};
use crate::runtime::{AcceptedJob, ResourceKind};

/// Percent-encode a single URL path segment, matching the absolute-href
/// encoding the HTTP renderer applies to resource ids and transition names.
fn encode_path_segment(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

/// Owned resource address: node, kind, and resource id. Lends a borrowed
/// [`ResourceRef`] so resource, transition, and subscription link builders
/// can share one identity without re-passing the component strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceAddress {
    node: String,
    kind: ResourceKind,
    resource_id: String,
}

impl ResourceAddress {
    pub fn new(
        node: impl Into<String>,
        kind: impl Into<ResourceKind>,
        resource_id: impl Into<String>,
    ) -> Self {
        Self {
            node: node.into(),
            kind: kind.into(),
            resource_id: resource_id.into(),
        }
    }

    /// Borrow this address as a `Copy` [`ResourceRef`]. Returns the ref by
    /// value (it just holds borrows), so this is not an `AsRef` impl; the
    /// explicit name avoids implying one.
    pub fn as_resource_ref(&self) -> ResourceRef<'_> {
        ResourceRef::new(&self.node, &self.kind, &self.resource_id)
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn resource_id(&self) -> &str {
        &self.resource_id
    }
}

/// Borrowed resource address. `Copy`, so the same ref can be handed to all
/// of the resource/transition/subscription href builders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceRef<'a> {
    node: &'a str,
    kind: &'a str,
    resource_id: &'a str,
}

impl<'a> ResourceRef<'a> {
    pub fn new(node: &'a str, kind: &'a str, resource_id: &'a str) -> Self {
        Self {
            node,
            kind,
            resource_id,
        }
    }

    pub fn node(self) -> &'a str {
        self.node
    }

    pub fn kind(self) -> &'a str {
        self.kind
    }

    pub fn resource_id(self) -> &'a str {
        self.resource_id
    }

    /// Relative href for the resource itself: `/resources/{id}`.
    pub fn resource_href(self) -> String {
        format!("/resources/{}", encode_path_segment(self.resource_id))
    }

    /// Relative href for a transition on the resource:
    /// `/resources/{id}/transitions/{transition}`.
    pub fn transition_href(self, transition: &str) -> String {
        format!(
            "/resources/{}/transitions/{}",
            encode_path_segment(self.resource_id),
            encode_path_segment(transition)
        )
    }

    /// Event subscription topic string: `{node}/{kind}/{id}/{stream}`.
    /// Kept internal so public subscription topics stay distinct from
    /// internal `StreamId` values. The segments are left un-encoded here;
    /// the query serializer percent-encodes the whole topic as a value.
    fn topic(self, stream: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            self.node, self.kind, self.resource_id, stream
        )
    }
}

impl AcceptedJob {
    /// Build an accepted-job handle from a resource ref. Keeps `id` and
    /// `kind` as the raw wire values and derives `location` from the same
    /// encoded resource href builder used elsewhere.
    pub fn for_resource(resource: ResourceRef<'_>, created: bool) -> Self {
        Self {
            id: resource.resource_id().to_owned(),
            kind: resource.kind().to_owned(),
            location: resource.resource_href(),
            created,
        }
    }
}

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
        let kind = kind.into();
        let resource_id = resource_id.into();
        Self::for_resource_ref(ResourceRef::new(&node, &kind, &resource_id), stream)
    }

    pub fn for_resource_ref(resource: ResourceRef<'_>, stream: impl Into<String>) -> Self {
        let stream = stream.into();
        Self {
            node: resource.node().to_owned(),
            topic: resource.topic(&stream),
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
        format!(
            "/servers/{}/events?{}",
            encode_path_segment(&self.node),
            query.finish()
        )
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

    #[test]
    fn resource_ref_builds_encoded_relative_hrefs() {
        let resource = ResourceRef::new("runner", "job", "job 1/2");

        assert_eq!(resource.resource_href(), "/resources/job%201%2F2");
        assert_eq!(
            resource.transition_href("cancel now"),
            "/resources/job%201%2F2/transitions/cancel%20now"
        );
    }

    #[test]
    fn resource_address_owns_and_borrows_identity() {
        let resource = ResourceAddress::new("runner", "job", "job-1");
        let borrowed = resource.as_resource_ref();

        assert_eq!(resource.node(), "runner");
        assert_eq!(resource.kind(), "job");
        assert_eq!(resource.resource_id(), "job-1");
        assert_eq!(borrowed.node(), "runner");
        assert_eq!(borrowed.kind(), "job");
        assert_eq!(borrowed.resource_id(), "job-1");
        assert_eq!(borrowed.resource_href(), "/resources/job-1");
    }

    #[test]
    fn subscription_url_from_resource_ref_matches_legacy_constructor() {
        let resource = ResourceRef::new("runner", "job", "job-1");

        let typed = SubscriptionUrl::for_resource_ref(resource, "progress")
            .outbound_capacity(16)
            .replay(true)
            .slow_consumer(SlowConsumerPolicy::Coalesce {
                key_path: FieldPath::parse("data.coalesceKey"),
            })
            .relative_href();
        let legacy = SubscriptionUrl::for_resource("runner", "job", "job-1", "progress")
            .outbound_capacity(16)
            .replay(true)
            .slow_consumer(SlowConsumerPolicy::Coalesce {
                key_path: FieldPath::parse("data.coalesceKey"),
            })
            .relative_href();

        assert_eq!(typed, legacy);
        assert!(typed.contains("topic=runner%2Fjob%2Fjob-1%2Fprogress"));
    }

    #[test]
    fn subscription_url_encodes_node_path_segment() {
        let href = SubscriptionUrl::for_resource_ref(
            ResourceRef::new("runner node", "job", "job-1"),
            "progress",
        )
        .relative_href();

        assert!(href.starts_with("/servers/runner%20node/events?"));
    }

    #[test]
    fn accepted_job_from_resource_ref_preserves_identity_and_encodes_location() {
        let job = AcceptedJob::for_resource(ResourceRef::new("runner", "job", "job 1/2"), true);

        assert_eq!(job.id, "job 1/2");
        assert_eq!(job.kind, "job");
        assert_eq!(job.location, "/resources/job%201%2F2");
        assert!(job.created);
    }
}
