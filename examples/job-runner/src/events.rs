use boardwalk::prelude::{ActorCtx, ActorError, TransitionCtx, TransitionError};
use serde::{Deserialize, Serialize};

use crate::job::JobData;
use crate::streams::JobStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProgressEvent {
    job_id: String,
    attempt: u32,
    coalesce_key: String,
    percent: u8,
    step: u32,
    total_steps: u32,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogEvent {
    job_id: String,
    attempt: u32,
    level: String,
    line: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LifecycleEvent {
    job_id: String,
    attempt: u32,
    state: String,
    reason: Option<String>,
}

pub(crate) async fn publish_lifecycle(
    ctx: &TransitionCtx,
    data: &JobData,
    reason: Option<&str>,
) -> Result<(), TransitionError> {
    ctx.publish(
        JobStream::Lifecycle.name(),
        JobStream::Lifecycle.event_kind(),
        JobStream::Lifecycle.payload_version(),
        serde_json::to_value(lifecycle_event(
            ctx.resource_id().unwrap_or_default(),
            data,
            reason,
        ))
        .map_err(TransitionError::internal)?,
    )
    .await
}

pub(crate) async fn publish_lifecycle_from_actor(
    ctx: &ActorCtx,
    data: &JobData,
    reason: Option<&str>,
) -> Result<(), ActorError> {
    ctx.publish(
        JobStream::Lifecycle.name(),
        JobStream::Lifecycle.event_kind(),
        JobStream::Lifecycle.payload_version(),
        serde_json::to_value(lifecycle_event(ctx.resource_id(), data, reason))
            .map_err(ActorError::internal)?,
    )
    .await
}

pub(crate) async fn publish_progress_from_actor(
    ctx: &ActorCtx,
    data: &JobData,
    message: &str,
) -> Result<(), ActorError> {
    ctx.publish(
        JobStream::Progress.name(),
        JobStream::Progress.event_kind(),
        JobStream::Progress.payload_version(),
        serde_json::to_value(ProgressEvent {
            job_id: ctx.resource_id().into(),
            attempt: data.attempt(),
            coalesce_key: format!("{}:{}", ctx.resource_id(), data.attempt()),
            percent: data.progress(),
            step: data.step(),
            total_steps: data.total_steps(),
            message: message.into(),
        })
        .map_err(ActorError::internal)?,
    )
    .await
}

pub(crate) async fn publish_log_from_actor(
    ctx: &ActorCtx,
    data: &JobData,
    level: &str,
    line: &str,
) -> Result<(), ActorError> {
    ctx.publish(
        JobStream::Logs.name(),
        JobStream::Logs.event_kind(),
        JobStream::Logs.payload_version(),
        serde_json::to_value(LogEvent {
            job_id: ctx.resource_id().into(),
            attempt: data.attempt(),
            level: level.into(),
            line: line.into(),
        })
        .map_err(ActorError::internal)?,
    )
    .await
}

fn lifecycle_event(job_id: &str, data: &JobData, reason: Option<&str>) -> LifecycleEvent {
    LifecycleEvent {
        job_id: job_id.into(),
        attempt: data.attempt(),
        state: data.state_name().into(),
        reason: reason.map(str::to_string),
    }
}

#[cfg(test)]
mod publish_source_guard {
    #[test]
    fn publish_calls_use_no_raw_event_kind_literals() {
        let production = include_str!("events.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        for literal in ["\"job.lifecycle\"", "\"job.progress\"", "\"job.log\""] {
            assert!(
                !production.contains(literal),
                "events.rs should pass JobStream::*.event_kind(), not `{literal}`, to ctx.publish"
            );
        }
    }
}
