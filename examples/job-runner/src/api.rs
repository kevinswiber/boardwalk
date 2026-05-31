use std::collections::BTreeMap;

use boardwalk::AcceptedJob;
use serde::{Deserialize, Serialize};

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
        Self {
            href: format!("/resources/{job_id}"),
            streams: JobStreams::for_job(&job_id),
            job_id,
        }
    }

    pub(crate) fn to_outcome_job(&self, created: bool) -> AcceptedJob {
        AcceptedJob {
            id: self.job_id.clone(),
            kind: "job".into(),
            location: self.href.clone(),
            created,
        }
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
        Self {
            lifecycle: JobStream::Lifecycle.href(job_id),
            progress: JobStream::Progress.href(job_id),
            logs: JobStream::Logs.href(job_id),
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
}
