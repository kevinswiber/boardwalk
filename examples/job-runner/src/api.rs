use std::collections::BTreeMap;

use boardwalk::AcceptedJob;
use serde::{Deserialize, Serialize};

use crate::address::job_resource;
use crate::streams::JobStream;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub(crate) enum FakeCommand {
    SuccessAfterTicks { ticks: u32 },
    FailAtStep { step: u32 },
}

impl FakeCommand {
    pub(crate) fn total_steps(&self) -> u32 {
        match self {
            Self::SuccessAfterTicks { ticks } => (*ticks).max(1),
            Self::FailAtStep { step } => (*step).max(1),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SubmitJob {
    pub(crate) command: FakeCommand,
    #[serde(default)]
    pub(crate) labels: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) owner: Option<String>,
    #[serde(default)]
    pub(crate) priority: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JobHandle {
    job_id: String,
    href: String,
    streams: JobStreams,
}

impl JobHandle {
    pub(crate) fn for_job(job_id: String) -> Self {
        let resource = job_resource(&job_id);
        Self {
            href: resource.resource_href(),
            streams: JobStreams::for_job(&job_id),
            job_id,
        }
    }

    pub(crate) fn to_outcome_job(&self, created: bool) -> AcceptedJob {
        AcceptedJob::for_resource(job_resource(&self.job_id), created)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobStreams {
    lifecycle: String,
    progress: String,
    logs: String,
}

impl JobStreams {
    fn for_job(job_id: &str) -> Self {
        let resource = job_resource(job_id);
        Self {
            lifecycle: JobStream::Lifecycle.href(resource),
            progress: JobStream::Progress.href(resource),
            logs: JobStream::Logs.href(resource),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RetryJob {
    #[allow(dead_code)]
    #[serde(default)]
    reset_logs: bool,
}

#[cfg(test)]
mod href_tests {
    use super::*;

    const EXPECTED_LIFECYCLE: &str = "/servers/runner/events?topic=runner%2Fjob%2Fjob-1%2Flifecycle&outboundCapacity=16&replay=true&slowConsumerPolicy=backpressure";
    const EXPECTED_PROGRESS: &str = "/servers/runner/events?topic=runner%2Fjob%2Fjob-1%2Fprogress&outboundCapacity=16&replay=true&slowConsumerPolicy=coalesce&coalesceKey=data.coalesceKey";
    const EXPECTED_LOGS: &str = "/servers/runner/events?topic=runner%2Fjob%2Fjob-1%2Flogs&outboundCapacity=16&replay=true&slowConsumerPolicy=backpressure";

    #[test]
    fn for_job_hrefs_are_unchanged() {
        let s = JobStreams::for_job("job-1");
        assert_eq!(s.lifecycle, EXPECTED_LIFECYCLE);
        assert_eq!(s.progress, EXPECTED_PROGRESS);
        assert_eq!(s.logs, EXPECTED_LOGS);
    }

    #[test]
    fn progress_href_coalesces_and_others_backpressure() {
        let s = JobStreams::for_job("job-1");
        assert!(s.progress.contains("slowConsumerPolicy=coalesce"));
        assert!(s.progress.contains("coalesceKey=data.coalesceKey"));
        assert!(s.logs.contains("slowConsumerPolicy=backpressure"));
        assert!(s.lifecycle.contains("slowConsumerPolicy=backpressure"));
    }

    #[test]
    fn stream_href_helper_is_removed() {
        let production = include_str!("api.rs").split("#[cfg(test)]").next().unwrap();
        assert!(
            !production.contains("fn stream_href"),
            "api.rs should delegate hrefs to JobStream::href, not a local stream_href"
        );
    }

    #[test]
    fn job_handle_and_accepted_job_shape_are_unchanged() {
        let handle = JobHandle::for_job("job-1".into());
        let job = handle.to_outcome_job(true);
        let json = serde_json::to_value(&handle).expect("JobHandle serializes");

        assert_eq!(json["jobId"], "job-1");
        assert_eq!(json["href"], "/resources/job-1");
        assert_eq!(job.id, "job-1");
        assert_eq!(job.kind, "job");
        assert_eq!(job.location, "/resources/job-1");
        assert!(job.created);
    }

    #[test]
    fn job_handle_uses_typed_resource_address_for_href_and_accepted_job() {
        let production = include_str!("api.rs").split("#[cfg(test)]").next().unwrap();

        assert!(
            !production.contains("format!(\"/resources/{job_id}\")"),
            "JobHandle::for_job should use ResourceRef::resource_href"
        );
        // Positive guard: the return type `-> AcceptedJob {` makes a bare
        // `AcceptedJob {` substring check ambiguous, so assert the constructor
        // is used rather than asserting the struct literal is absent.
        assert!(
            production.contains("AcceptedJob::for_resource("),
            "JobHandle::to_outcome_job should use AcceptedJob::for_resource"
        );
        assert!(
            !production.contains("kind: \"job\""),
            "accepted-job construction should not repeat the job kind literal"
        );
    }
}
