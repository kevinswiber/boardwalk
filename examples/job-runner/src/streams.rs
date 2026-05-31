//! Single source of truth for the job-runner stream names, kinds, versions, policies, and hrefs.

use boardwalk::SlowConsumerPolicy;
use boardwalk::links::SubscriptionUrl;
use boardwalk::prelude::{StreamKind, StreamSpec};

use crate::{NODE_NAME, STREAM_OUTBOUND_CAPACITY};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobStream {
    Lifecycle,
    Progress,
    Logs,
}

impl JobStream {
    /// All job streams in spec/declaration order.
    pub(crate) const ALL: [JobStream; 3] = [Self::Lifecycle, Self::Progress, Self::Logs];

    /// Stream name on the wire (snapshot spec, publish target, href segment).
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Lifecycle => "lifecycle",
            Self::Progress => "progress",
            Self::Logs => "logs",
        }
    }

    /// Event payload kind published on this stream.
    pub(crate) const fn event_kind(self) -> &'static str {
        match self {
            Self::Lifecycle => "job.lifecycle",
            Self::Progress => "job.progress",
            Self::Logs => "job.log",
        }
    }

    /// Payload schema version published on this stream.
    pub(crate) const fn payload_version(self) -> u32 {
        1
    }

    /// The `StreamSpec` entry to register for this stream.
    pub(crate) fn spec(self) -> StreamSpec {
        StreamSpec {
            name: self.name().into(),
            kind: StreamKind::Object,
        }
    }

    /// Slow-consumer policy applied when subscribing to this stream.
    pub(crate) fn slow_consumer_policy(self) -> SlowConsumerPolicy {
        match self {
            Self::Progress => {
                // Mirrors ProgressEvent.coalesceKey; unifying the field/path is issue #32.
                SlowConsumerPolicy::from_query("coalesce", Some("data.coalesceKey"))
                    .expect("static coalesce key path is valid")
            }
            Self::Lifecycle | Self::Logs => SlowConsumerPolicy::Backpressure,
        }
    }

    /// Relative subscription href for this stream of `job_id`.
    pub(crate) fn href(self, job_id: &str) -> String {
        SubscriptionUrl::for_resource(NODE_NAME, "job", job_id, self.name())
            .outbound_capacity(STREAM_OUTBOUND_CAPACITY)
            .replay(true)
            .slow_consumer(self.slow_consumer_policy())
            .relative_href()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_streams_in_declaration_order() {
        assert_eq!(
            JobStream::ALL,
            [JobStream::Lifecycle, JobStream::Progress, JobStream::Logs]
        );
    }

    #[test]
    fn names_match_current_wire_strings() {
        assert_eq!(JobStream::Lifecycle.name(), "lifecycle");
        assert_eq!(JobStream::Progress.name(), "progress");
        assert_eq!(JobStream::Logs.name(), "logs");
    }

    #[test]
    fn event_kinds_match_current_wire_strings() {
        assert_eq!(JobStream::Lifecycle.event_kind(), "job.lifecycle");
        assert_eq!(JobStream::Progress.event_kind(), "job.progress");
        assert_eq!(JobStream::Logs.event_kind(), "job.log");
    }

    #[test]
    fn payload_version_is_one_for_all_streams() {
        for stream in JobStream::ALL {
            assert_eq!(stream.payload_version(), 1u32);
        }
    }

    #[test]
    fn spec_uses_name_and_object_kind() {
        let spec = JobStream::Progress.spec();
        assert_eq!(spec.name, "progress");
        assert!(matches!(spec.kind, StreamKind::Object));
    }

    #[test]
    fn progress_coalesces_on_the_data_coalesce_key() {
        let expected = SlowConsumerPolicy::from_query("coalesce", Some("data.coalesceKey"))
            .expect("static coalesce key path is valid");
        assert_eq!(JobStream::Progress.slow_consumer_policy(), expected);
    }

    #[test]
    fn logs_and_lifecycle_use_backpressure() {
        assert_eq!(
            JobStream::Logs.slow_consumer_policy(),
            SlowConsumerPolicy::Backpressure
        );
        assert_eq!(
            JobStream::Lifecycle.slow_consumer_policy(),
            SlowConsumerPolicy::Backpressure
        );
    }

    #[test]
    fn href_encodes_topic_capacity_replay_and_policy() {
        let progress = JobStream::Progress.href("job-1");
        assert!(progress.contains("slowConsumerPolicy=coalesce"));
        assert!(progress.contains("coalesceKey=data.coalesceKey"));
        assert!(progress.contains("outboundCapacity=16"));
        assert!(progress.contains("replay=true"));
        assert!(
            JobStream::Logs
                .href("job-1")
                .contains("slowConsumerPolicy=backpressure")
        );
    }
}
