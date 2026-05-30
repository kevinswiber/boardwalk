use std::collections::BTreeMap;

use boardwalk::links::SubscriptionUrl;
use boardwalk::{AcceptedJob, SlowConsumerPolicy};
use serde::{Deserialize, Serialize};

use crate::{NODE_NAME, STREAM_OUTBOUND_CAPACITY};

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
            lifecycle: stream_href(job_id, "lifecycle"),
            progress: stream_href(job_id, "progress"),
            logs: stream_href(job_id, "logs"),
        }
    }
}

fn stream_href(job_id: &str, stream: &str) -> String {
    let url = SubscriptionUrl::for_resource(NODE_NAME, "job", job_id, stream)
        .outbound_capacity(STREAM_OUTBOUND_CAPACITY)
        .replay(true);
    let url = match stream {
        "progress" => url.slow_consumer(
            SlowConsumerPolicy::from_query("coalesce", Some("data.coalesceKey"))
                .expect("static coalesce key path is valid"),
        ),
        "logs" | "lifecycle" => url.slow_consumer(SlowConsumerPolicy::Backpressure),
        _ => url,
    };
    url.relative_href()
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RetryJob {
    #[allow(dead_code)]
    #[serde(default)]
    reset_logs: bool,
}
