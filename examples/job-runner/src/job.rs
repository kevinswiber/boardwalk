use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use boardwalk::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::api::{FakeCommand, JobHandle, RetryJob, SubmitJob};
use crate::events::{
    publish_lifecycle, publish_lifecycle_from_actor, publish_log_from_actor,
    publish_progress_from_actor,
};
use crate::{FIXED_FINISHED_AT, FIXED_STARTED_AT, FIXED_SUBMITTED_AT};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelling,
    Cancelled,
}

impl JobState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
        }
    }

    fn can_cancel(&self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }

    fn can_retry(&self) -> bool {
        matches!(self, Self::Failed)
    }
}

#[derive(Debug)]
pub(crate) struct Job {
    shared: Arc<Mutex<JobData>>,
    runner: Option<JoinHandle<()>>,
}

impl Job {
    pub(crate) fn from_submit(queue: String, input: SubmitJob) -> Self {
        let mut labels = example_labels();
        labels.extend(input.labels);
        Self {
            shared: Arc::new(Mutex::new(JobData {
                queue,
                command: input.command,
                labels,
                state: JobState::Queued,
                owner: input.owner,
                priority: input.priority,
                attempt: 1,
                step: 0,
                progress: 0,
                submitted_at: FIXED_SUBMITTED_AT.into(),
                started_at: None,
                finished_at: None,
                result: None,
                error: None,
            })),
            runner: None,
        }
    }
}

impl Resource for Job {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "job".into(),
            name: None,
            labels: example_labels(),
            property_schema: None,
            streams: vec![
                StreamSpec {
                    name: "lifecycle".into(),
                    kind: StreamKind::Object,
                },
                StreamSpec {
                    name: "progress".into(),
                    kind: StreamKind::Object,
                },
                StreamSpec {
                    name: "logs".into(),
                    kind: StreamKind::Object,
                },
            ],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move {
            let data = self.shared.lock().await;
            Ok(data.snapshot())
        })
    }
}

#[boardwalk::actor]
impl Job {
    #[boardwalk::transition]
    async fn cancel(
        &mut self,
        ctx: TransitionCtx,
        _input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        let mut data = self.shared.lock().await;
        data.cancel()?;
        publish_lifecycle(&ctx, &data, Some("user_cancelled")).await?;
        ctx.completed(
            Some(json!({
                "accepted": true,
                "state": data.state.as_str(),
            })),
            data.snapshot(),
        )
    }

    #[boardwalk::transition]
    async fn retry(
        &mut self,
        ctx: TransitionCtx,
        input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        let _input = input.deserialize::<RetryJob>()?;
        let mut data = self.shared.lock().await;
        if !data.state.can_retry() {
            return Err(TransitionError::Conflict(format!(
                "retry is not available for {} jobs",
                data.state.as_str()
            )));
        }
        data.attempt += 1;
        data.state = JobState::Queued;
        data.step = 0;
        data.progress = 0;
        data.started_at = None;
        data.finished_at = None;
        data.result = None;
        data.error = None;
        publish_lifecycle(&ctx, &data, Some("retried")).await?;
        let id = ctx.resource_id_required()?;
        let handle = JobHandle::for_job(id.to_string());
        TransitionOutcome::accepted(handle.to_outcome_job(false), &handle)
    }

    #[boardwalk::on_start]
    async fn boot(&mut self, ctx: ActorCtx) -> Result<(), ActorError> {
        let shared = self.shared.clone();
        self.runner = Some(tokio::spawn(async move {
            run_job(shared, ctx).await;
        }));
        Ok(())
    }

    #[boardwalk::on_stop]
    async fn teardown(&mut self, _ctx: ActorCtx) -> Result<(), ActorError> {
        if let Some(runner) = self.runner.take() {
            runner.abort();
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct JobData {
    queue: String,
    command: FakeCommand,
    labels: BTreeMap<String, String>,
    state: JobState,
    owner: Option<String>,
    priority: u8,
    attempt: u32,
    step: u32,
    progress: u8,
    submitted_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    result: Option<Value>,
    error: Option<Value>,
}

impl JobData {
    pub(crate) fn attempt(&self) -> u32 {
        self.attempt
    }

    pub(crate) fn state_name(&self) -> &'static str {
        self.state.as_str()
    }

    pub(crate) fn progress(&self) -> u8 {
        self.progress
    }

    pub(crate) fn step(&self) -> u32 {
        self.step
    }

    pub(crate) fn total_steps(&self) -> u32 {
        self.command.total_steps()
    }

    fn cancel(&mut self) -> Result<(), TransitionError> {
        match self.state {
            JobState::Queued => {
                self.state = JobState::Cancelled;
                self.finished_at = Some(FIXED_FINISHED_AT.into());
                Ok(())
            }
            JobState::Running => {
                self.state = JobState::Cancelling;
                Ok(())
            }
            JobState::Succeeded | JobState::Failed | JobState::Cancelling | JobState::Cancelled => {
                Err(TransitionError::Conflict(format!(
                    "cannot cancel {} job",
                    self.state.as_str()
                )))
            }
        }
    }

    fn snapshot(&self) -> ResourceSnapshot {
        ResourceSnapshot::builder("job")
            .state(self.state.as_str())
            .properties(self.properties())
            .labels(self.labels.clone())
            .transitions(self.transitions())
            .build()
    }

    fn properties(&self) -> Map<String, Value> {
        let mut properties = Map::new();
        properties.insert("queue".into(), json!(self.queue));
        properties.insert("owner".into(), option_json(self.owner.as_deref()));
        properties.insert("priority".into(), json!(self.priority));
        properties.insert("attempt".into(), json!(self.attempt));
        properties.insert("step".into(), json!(self.step));
        properties.insert("total_steps".into(), json!(self.command.total_steps()));
        properties.insert("progress".into(), json!(self.progress));
        properties.insert("submitted_at".into(), json!(self.submitted_at));
        properties.insert("started_at".into(), option_json(self.started_at.as_deref()));
        properties.insert(
            "finished_at".into(),
            option_json(self.finished_at.as_deref()),
        );
        properties.insert("result".into(), self.result.clone().unwrap_or(Value::Null));
        properties.insert("error".into(), self.error.clone().unwrap_or(Value::Null));
        properties
    }

    fn transitions(&self) -> Vec<TransitionAffordance> {
        let can_cancel = self.state.can_cancel();
        let can_retry = self.state.can_retry();
        vec![
            if can_cancel {
                TransitionAffordance::available(cancel_spec())
            } else {
                TransitionAffordance::unavailable(
                    cancel_spec(),
                    "cancel is only available for queued or running jobs",
                )
            },
            if can_retry {
                TransitionAffordance::available(retry_spec())
            } else {
                TransitionAffordance::unavailable(
                    retry_spec(),
                    "retry is only available for failed jobs",
                )
            },
        ]
    }
}

async fn run_job(shared: Arc<Mutex<JobData>>, ctx: ActorCtx) {
    tokio::time::sleep(Duration::from_millis(75)).await;

    {
        let mut data = shared.lock().await;
        if !matches!(data.state, JobState::Queued) {
            return;
        }
        data.state = JobState::Running;
        data.started_at = Some(FIXED_STARTED_AT.into());
        let _ = publish_lifecycle_from_actor(&ctx, &data, None).await;
        let _ = publish_log_from_actor(&ctx, &data, "info", "job started").await;
    }

    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let mut data = shared.lock().await;
        match data.state {
            JobState::Running => match data.command.clone() {
                FakeCommand::SuccessAfterTicks { ticks } => {
                    let total = ticks.max(1);
                    data.step += 1;
                    if data.step >= total {
                        data.progress = 100;
                        data.state = JobState::Succeeded;
                        data.finished_at = Some(FIXED_FINISHED_AT.into());
                        data.result = Some(json!({ "status": "ok" }));
                        let _ = publish_lifecycle_from_actor(&ctx, &data, None).await;
                        return;
                    }
                    data.progress = progress_percent(data.step, total);
                    let _ = publish_progress_from_actor(&ctx, &data, "job progress").await;
                    let _ = publish_log_from_actor(&ctx, &data, "info", "job progress").await;
                }
                FakeCommand::FailAtStep { step } => {
                    let fail_at = step.max(1);
                    data.step += 1;
                    if data.step >= fail_at {
                        data.state = JobState::Failed;
                        data.finished_at = Some(FIXED_FINISHED_AT.into());
                        data.error = Some(json!({
                            "code": "command_failed",
                            "message": "fake command failed",
                        }));
                        let _ = publish_lifecycle_from_actor(&ctx, &data, None).await;
                        let _ = publish_log_from_actor(&ctx, &data, "error", "fake command failed")
                            .await;
                        return;
                    }
                    data.progress = progress_percent(data.step, fail_at);
                    let _ = publish_progress_from_actor(&ctx, &data, "job progress").await;
                    let _ = publish_log_from_actor(&ctx, &data, "info", "job progress").await;
                }
            },
            JobState::Cancelling => {
                data.state = JobState::Cancelled;
                data.finished_at = Some(FIXED_FINISHED_AT.into());
                let _ = publish_lifecycle_from_actor(&ctx, &data, Some("user_cancelled")).await;
                return;
            }
            JobState::Queued => {}
            JobState::Succeeded | JobState::Failed | JobState::Cancelled => return,
        }
    }
}

pub(crate) fn example_labels() -> BTreeMap<String, String> {
    BTreeMap::from([("example".into(), "jobs".into())])
}

fn option_json(value: Option<&str>) -> Value {
    value.map(Value::from).unwrap_or(Value::Null)
}

fn progress_percent(step: u32, total: u32) -> u8 {
    ((step.saturating_mul(100)) / total.max(1)).min(99) as u8
}

fn cancel_spec() -> TransitionSpec {
    TransitionSpec::sync("cancel")
        .title("Cancel job")
        .allowed_states(["queued", "running"])
        .idempotency(Idempotency::Supported)
        .effect(Effect::UnsafeIdempotent)
}

fn retry_spec() -> TransitionSpec {
    TransitionSpec::async_job("retry")
        .title("Retry job")
        .allowed_states(["failed"])
        .idempotency(Idempotency::None)
        .effect(Effect::Unsafe)
}
