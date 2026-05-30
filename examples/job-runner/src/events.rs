use boardwalk::prelude::{ActorCtx, ActorError, TransitionCtx, TransitionError};
use serde::{Deserialize, Serialize};

use crate::job::JobData;

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
        "lifecycle",
        "job.lifecycle",
        1,
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
        "lifecycle",
        "job.lifecycle",
        1,
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
        "progress",
        "job.progress",
        1,
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
        "logs",
        "job.log",
        1,
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
