//! Resource/actor exemplar for a deterministic job runner.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::core::{
    ActorSpec, Effect, Idempotency, JobHandle as OutcomeJobHandle, ResourceSpec, StreamKind,
    StreamSpec, TransitionInput, TransitionOutcome, TransitionResultKind, TransitionSpec,
};
use crate::events::{SlowConsumerPolicy, SubscribeOpts};
use crate::http::{ResourceSnapshot, TransitionAffordance};
use crate::query::FieldPath;
use crate::runtime::{
    Actor, DynFuture, Resource, ResourceCtx, ResourceError, TransitionCtx, TransitionError,
};

const EXAMPLE_LABEL_KEY: &str = "example";
const EXAMPLE_LABEL_VALUE: &str = "jobs";
const FIXED_SUBMITTED_AT: &str = "2026-01-01T00:00:00Z";
const FIXED_STARTED_AT: &str = "2026-01-01T00:00:01Z";
const FIXED_FINISHED_AT: &str = "2026-01-01T00:00:02Z";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum FakeCommand {
    SuccessAfterTicks { ticks: u32 },
    FailAtStep { step: u32 },
}

impl FakeCommand {
    fn total_steps(&self) -> u32 {
        match self {
            FakeCommand::SuccessAfterTicks { ticks } => (*ticks).max(1),
            FakeCommand::FailAtStep { step } => (*step).max(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelling,
    Cancelled,
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelling => "cancelling",
            JobState::Cancelled => "cancelled",
        }
    }

    fn can_cancel(&self) -> bool {
        matches!(self, JobState::Queued | JobState::Running)
    }

    fn can_retry(&self) -> bool {
        matches!(self, JobState::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitJob {
    pub command: FakeCommand,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub priority: u8,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
}

impl SubmitJob {
    pub fn new(command: FakeCommand) -> Self {
        Self {
            command,
            labels: BTreeMap::new(),
            owner: None,
            priority: 0,
            max_attempts: default_max_attempts(),
        }
    }

    fn from_input(input: TransitionInput) -> Result<Self, TransitionError> {
        let object = input.fields.into_iter().collect();
        serde_json::from_value(Value::Object(object))
            .map_err(|err| TransitionError::InvalidInput(err.to_string()))
    }
}

fn default_max_attempts() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobHandle {
    pub job_id: String,
    pub href: String,
    pub state: String,
    pub streams: JobStreams,
}

impl JobHandle {
    fn for_job(job_id: String, state: &str) -> Self {
        Self {
            href: format!("/resources/{job_id}"),
            streams: JobStreams::for_job(&job_id),
            job_id,
            state: state.into(),
        }
    }

    fn to_outcome_handle(&self, created: bool) -> OutcomeJobHandle {
        OutcomeJobHandle {
            id: self.job_id.clone(),
            kind: "job".into(),
            location: self.href.clone(),
            created,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobStreams {
    pub lifecycle: String,
    pub progress: String,
    pub logs: String,
}

impl JobStreams {
    fn for_job(job_id: &str) -> Self {
        let base = format!("/resources/{job_id}/streams");
        Self {
            lifecycle: format!("{base}/lifecycle"),
            progress: format!("{base}/progress"),
            logs: format!("{base}/logs"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelOutput {
    pub accepted: bool,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryJob {
    #[serde(default)]
    pub reset_logs: bool,
}

impl RetryJob {
    fn from_input(input: TransitionInput) -> Result<Self, TransitionError> {
        let object = input.fields.into_iter().collect();
        serde_json::from_value(Value::Object(object))
            .map_err(|err| TransitionError::InvalidInput(err.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgressEvent {
    pub job_id: String,
    pub attempt: u32,
    pub coalesce_key: String,
    pub percent: u8,
    pub step: u32,
    pub total_steps: u32,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEvent {
    pub job_id: String,
    pub attempt: u32,
    pub level: String,
    pub line: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleEvent {
    pub job_id: String,
    pub attempt: u32,
    pub state: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JobQueue {
    name: String,
    labels: BTreeMap<String, String>,
    submitted: u64,
}

impl JobQueue {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            labels: example_labels(),
            submitted: 0,
        }
    }

    pub fn actor_spec(&self) -> ActorSpec {
        ActorSpec {
            resource: self.spec(),
            transitions: vec![submit_spec()],
        }
    }
}

impl Resource for JobQueue {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "job.queue".into(),
            name: Some(self.name.clone()),
            labels: self.labels.clone(),
            property_schema: None,
            streams: vec![],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let snapshot = ResourceSnapshot {
            id: format!("queue/{}", self.name),
            kind: "job.queue".into(),
            name: Some(self.name.clone()),
            state: Some("open".into()),
            node: "local".into(),
            properties: queue_properties(self.submitted),
            labels: self.labels.clone(),
            transitions: vec![TransitionAffordance {
                spec: submit_spec(),
                available: true,
                unavailable_reason: None,
            }],
            streams: vec![],
            revision: None,
            metadata: Map::new(),
        };
        Box::pin(async move { Ok(snapshot) })
    }
}

impl Actor for JobQueue {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            match name {
                "submit" => {
                    let input = SubmitJob::from_input(input)?;
                    let job = Job::from_submit(self.name.clone(), input);
                    let id = ctx.register_actor(job).await?;
                    self.submitted += 1;

                    let handle = JobHandle::for_job(id, "queued");
                    let submitted = LifecycleEvent {
                        job_id: handle.job_id.clone(),
                        attempt: 1,
                        state: "queued".into(),
                        reason: Some("submitted".into()),
                    };
                    publish_json_for_resource(
                        &ctx,
                        &handle.job_id,
                        "job",
                        "lifecycle",
                        "job.lifecycle",
                        submitted,
                    )
                    .await?;
                    let output = serde_json::to_value(&handle)
                        .map_err(|err| TransitionError::Internal(err.to_string()))?;
                    Ok(TransitionOutcome::Accepted {
                        job: handle.to_outcome_handle(true),
                        output: Some(output),
                    })
                }
                other => Err(TransitionError::NotAllowed(format!(
                    "transition `{other}` not supported by job queue"
                ))),
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    queue: String,
    command: FakeCommand,
    labels: BTreeMap<String, String>,
    state: JobState,
    owner: Option<String>,
    priority: u8,
    attempt: u32,
    max_attempts: u32,
    step: u32,
    progress: u8,
    submitted_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    result: Option<Value>,
    error: Option<JobErrorInfo>,
    logs: Vec<LogEvent>,
}

impl Job {
    pub fn new(queue: impl Into<String>, command: FakeCommand) -> Self {
        Self {
            queue: queue.into(),
            command,
            labels: example_labels(),
            state: JobState::Queued,
            owner: None,
            priority: 0,
            attempt: 1,
            max_attempts: 1,
            step: 0,
            progress: 0,
            submitted_at: FIXED_SUBMITTED_AT.into(),
            started_at: None,
            finished_at: None,
            result: None,
            error: None,
            logs: Vec::new(),
        }
    }

    pub fn from_submit(queue: impl Into<String>, input: SubmitJob) -> Self {
        let mut labels = example_labels();
        labels.extend(input.labels);
        Self {
            queue: queue.into(),
            command: input.command,
            labels,
            state: JobState::Queued,
            owner: input.owner,
            priority: input.priority,
            attempt: 1,
            max_attempts: input.max_attempts.max(1),
            step: 0,
            progress: 0,
            submitted_at: FIXED_SUBMITTED_AT.into(),
            started_at: None,
            finished_at: None,
            result: None,
            error: None,
            logs: Vec::new(),
        }
    }

    pub fn with_state(mut self, state: JobState) -> Self {
        self.state = state;
        match self.state {
            JobState::Queued => {
                self.step = 0;
                self.progress = 0;
                self.started_at = None;
                self.finished_at = None;
                self.result = None;
                self.error = None;
                self.logs.clear();
            }
            JobState::Running | JobState::Cancelling => {
                self.step = 0;
                self.progress = 0;
                self.started_at = Some(FIXED_STARTED_AT.into());
                self.finished_at = None;
                self.result = None;
                self.error = None;
            }
            JobState::Succeeded => {
                self.step = self.command.total_steps();
                self.progress = 100;
                self.started_at = Some(FIXED_STARTED_AT.into());
                self.finished_at = Some(FIXED_FINISHED_AT.into());
                self.result = Some(json!({ "status": "ok" }));
                self.error = None;
            }
            JobState::Failed => {
                self.step = self.command.total_steps();
                self.started_at = Some(FIXED_STARTED_AT.into());
                self.finished_at = Some(FIXED_FINISHED_AT.into());
                self.result = None;
                self.error = Some(JobErrorInfo {
                    code: "command_failed".into(),
                    message: "fake command failed".into(),
                });
            }
            JobState::Cancelled => {
                self.step = 0;
                self.progress = 0;
                self.started_at = Some(FIXED_STARTED_AT.into());
                self.finished_at = Some(FIXED_FINISHED_AT.into());
                self.result = None;
                self.error = None;
            }
        }
        self
    }

    pub fn state(&self) -> &JobState {
        &self.state
    }

    pub fn stream_subscribe_opts(stream: &str, outbound_capacity: usize) -> SubscribeOpts {
        let slow_consumer_policy = match stream {
            "progress" => SlowConsumerPolicy::Coalesce {
                key_path: FieldPath::parse("data.coalesceKey"),
            },
            "logs" | "lifecycle" => SlowConsumerPolicy::Backpressure,
            other => panic!("unknown job stream `{other}`"),
        };
        SubscribeOpts {
            limit: None,
            outbound_capacity: Some(outbound_capacity.max(1)),
            slow_consumer_policy,
        }
    }

    #[cfg(test)]
    fn advance(&mut self) -> Result<(), TransitionError> {
        match self.state {
            JobState::Queued => {
                self.state = JobState::Running;
                self.started_at = Some(FIXED_STARTED_AT.into());
                self.progress = 0;
                Ok(())
            }
            JobState::Running => {
                self.step += 1;
                match &self.command {
                    FakeCommand::SuccessAfterTicks { ticks } => {
                        let total = (*ticks).max(1);
                        if self.step >= total {
                            self.finish_success();
                        } else {
                            self.progress = progress_percent(self.step, total);
                        }
                        Ok(())
                    }
                    FakeCommand::FailAtStep { step } => {
                        let fail_at = (*step).max(1);
                        if self.step >= fail_at {
                            self.fail("command_failed", "fake command failed");
                        } else {
                            self.progress = progress_percent(self.step, fail_at);
                        }
                        Ok(())
                    }
                }
            }
            JobState::Cancelling => {
                self.state = JobState::Cancelled;
                self.finished_at = Some(FIXED_FINISHED_AT.into());
                Ok(())
            }
            JobState::Succeeded | JobState::Failed | JobState::Cancelled => Err(
                TransitionError::NotAllowed(format!("cannot advance {} job", self.state.as_str())),
            ),
        }
    }

    #[cfg(test)]
    async fn advance_with_ctx(&mut self, ctx: &TransitionCtx) -> Result<(), TransitionError> {
        let job_id = ctx_resource_id(ctx)?;
        match self.state {
            JobState::Queued => {
                self.state = JobState::Running;
                self.started_at = Some(FIXED_STARTED_AT.into());
                self.progress = 0;
                self.publish_lifecycle(ctx, &job_id, None).await?;
                self.publish_log(ctx, &job_id, "info", "job started")
                    .await?;
                Ok(())
            }
            JobState::Running => {
                self.step += 1;
                match &self.command {
                    FakeCommand::SuccessAfterTicks { ticks } => {
                        let total = (*ticks).max(1);
                        if self.step >= total {
                            self.finish_success();
                            self.publish_lifecycle(ctx, &job_id, None).await?;
                        } else {
                            self.progress = progress_percent(self.step, total);
                            self.publish_progress(ctx, &job_id, "job progress").await?;
                            self.publish_log(ctx, &job_id, "info", "job progress")
                                .await?;
                        }
                        Ok(())
                    }
                    FakeCommand::FailAtStep { step } => {
                        let fail_at = (*step).max(1);
                        if self.step >= fail_at {
                            self.fail("command_failed", "fake command failed");
                            self.publish_lifecycle(ctx, &job_id, None).await?;
                            self.publish_log(ctx, &job_id, "error", "fake command failed")
                                .await?;
                        } else {
                            self.progress = progress_percent(self.step, fail_at);
                            self.publish_progress(ctx, &job_id, "job progress").await?;
                            self.publish_log(ctx, &job_id, "info", "job progress")
                                .await?;
                        }
                        Ok(())
                    }
                }
            }
            JobState::Cancelling => {
                self.state = JobState::Cancelled;
                self.finished_at = Some(FIXED_FINISHED_AT.into());
                self.publish_lifecycle(ctx, &job_id, Some("user_cancelled"))
                    .await?;
                Ok(())
            }
            JobState::Succeeded | JobState::Failed | JobState::Cancelled => Err(
                TransitionError::NotAllowed(format!("cannot advance {} job", self.state.as_str())),
            ),
        }
    }

    pub fn cancel(&mut self) -> Result<CancelOutput, TransitionError> {
        match self.state {
            JobState::Queued => {
                self.state = JobState::Cancelled;
                self.finished_at = Some(FIXED_FINISHED_AT.into());
                Ok(CancelOutput {
                    accepted: true,
                    state: self.state.as_str().into(),
                })
            }
            JobState::Running => {
                self.state = JobState::Cancelling;
                Ok(CancelOutput {
                    accepted: true,
                    state: self.state.as_str().into(),
                })
            }
            JobState::Succeeded | JobState::Failed | JobState::Cancelling | JobState::Cancelled => {
                Err(TransitionError::Conflict(format!(
                    "cannot cancel {} job",
                    self.state.as_str()
                )))
            }
        }
    }

    async fn cancel_with_ctx(
        &mut self,
        ctx: &TransitionCtx,
    ) -> Result<CancelOutput, TransitionError> {
        let output = self.cancel()?;
        let job_id = ctx_resource_id(ctx)?;
        self.publish_lifecycle(ctx, &job_id, Some("user_cancelled"))
            .await?;
        Ok(output)
    }

    pub fn retry(
        &mut self,
        input: RetryJob,
        job_id: impl Into<String>,
    ) -> Result<JobHandle, TransitionError> {
        if !self.state.can_retry() {
            return Err(TransitionError::Conflict(format!(
                "retry is not available for {} jobs",
                self.state.as_str()
            )));
        }
        if input.reset_logs {
            self.logs.clear();
        }
        self.attempt += 1;
        self.step = 0;
        self.progress = 0;
        self.state = JobState::Queued;
        self.started_at = None;
        self.finished_at = None;
        self.result = None;
        self.error = None;
        Ok(JobHandle::for_job(job_id.into(), self.state.as_str()))
    }

    async fn retry_with_ctx(
        &mut self,
        ctx: &TransitionCtx,
        input: RetryJob,
    ) -> Result<JobHandle, TransitionError> {
        let job_id = ctx_resource_id(ctx)?;
        let handle = self.retry(input, job_id.clone())?;
        self.publish_lifecycle(ctx, &job_id, Some("retried"))
            .await?;
        Ok(handle)
    }

    pub fn actor_spec(&self) -> ActorSpec {
        ActorSpec {
            resource: self.spec(),
            transitions: vec![cancel_spec(), retry_spec()],
        }
    }

    fn snapshot_value(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            id: String::new(),
            kind: "job".into(),
            name: None,
            state: Some(self.state.as_str().into()),
            node: "local".into(),
            properties: self.properties(),
            labels: self.labels.clone(),
            transitions: self.transition_affordances(),
            streams: job_streams(),
            revision: None,
            metadata: Map::new(),
        }
    }

    fn snapshot_for_transition(&self, ctx: &TransitionCtx) -> ResourceSnapshot {
        let mut snapshot = self.snapshot_value();
        if let Some(resource_id) = ctx.resource_id() {
            snapshot.id = resource_id.to_string();
            snapshot.node = ctx.node().to_string();
        }
        snapshot
    }

    fn properties(&self) -> Map<String, Value> {
        let mut props = Map::new();
        props.insert("queue".into(), json!(self.queue));
        props.insert("owner".into(), option_json(self.owner.as_deref()));
        props.insert("priority".into(), json!(self.priority));
        props.insert("attempt".into(), json!(self.attempt));
        props.insert("max_attempts".into(), json!(self.max_attempts));
        props.insert("step".into(), json!(self.step));
        props.insert("total_steps".into(), json!(self.command.total_steps()));
        props.insert("progress".into(), json!(self.progress));
        props.insert("submitted_at".into(), json!(self.submitted_at));
        props.insert("started_at".into(), option_json(self.started_at.as_deref()));
        props.insert(
            "finished_at".into(),
            option_json(self.finished_at.as_deref()),
        );
        props.insert("result".into(), self.result.clone().unwrap_or(Value::Null));
        props.insert("log_count".into(), json!(self.logs.len()));
        props.insert(
            "error".into(),
            self.error
                .as_ref()
                .map(|error| {
                    json!({
                        "code": error.code,
                        "message": error.message,
                    })
                })
                .unwrap_or(Value::Null),
        );
        props.insert(
            "command".into(),
            serde_json::to_value(&self.command).unwrap(),
        );
        props
    }

    fn transition_affordances(&self) -> Vec<TransitionAffordance> {
        vec![
            TransitionAffordance {
                spec: cancel_spec(),
                available: self.state.can_cancel(),
                unavailable_reason: (!self.state.can_cancel())
                    .then(|| "cancel is only available for queued or running jobs".into()),
            },
            TransitionAffordance {
                spec: retry_spec(),
                available: self.state.can_retry(),
                unavailable_reason: (!self.state.can_retry())
                    .then(|| "retry is only available for failed jobs".into()),
            },
        ]
    }

    #[cfg(test)]
    fn finish_success(&mut self) {
        self.state = JobState::Succeeded;
        self.progress = 100;
        self.finished_at = Some(FIXED_FINISHED_AT.into());
        self.result = Some(json!({ "status": "ok" }));
        self.error = None;
    }

    #[cfg(test)]
    fn fail(&mut self, code: &str, message: &str) {
        self.state = JobState::Failed;
        self.finished_at = Some(FIXED_FINISHED_AT.into());
        self.result = None;
        self.error = Some(JobErrorInfo {
            code: code.into(),
            message: message.into(),
        });
    }

    async fn publish_lifecycle(
        &self,
        ctx: &TransitionCtx,
        job_id: &str,
        reason: Option<&str>,
    ) -> Result<(), TransitionError> {
        let event = LifecycleEvent {
            job_id: job_id.into(),
            attempt: self.attempt,
            state: self.state.as_str().into(),
            reason: reason.map(str::to_string),
        };
        publish_json(ctx, "lifecycle", "job.lifecycle", event).await
    }

    #[cfg(test)]
    async fn publish_progress(
        &self,
        ctx: &TransitionCtx,
        job_id: &str,
        message: &str,
    ) -> Result<(), TransitionError> {
        let event = ProgressEvent {
            job_id: job_id.into(),
            attempt: self.attempt,
            coalesce_key: format!("{job_id}:{}", self.attempt),
            percent: self.progress,
            step: self.step,
            total_steps: self.command.total_steps(),
            message: message.into(),
        };
        publish_json(ctx, "progress", "job.progress", event).await
    }

    #[cfg(test)]
    async fn publish_log(
        &mut self,
        ctx: &TransitionCtx,
        job_id: &str,
        level: &str,
        line: &str,
    ) -> Result<(), TransitionError> {
        let event = LogEvent {
            job_id: job_id.into(),
            attempt: self.attempt,
            level: level.into(),
            line: line.into(),
        };
        self.logs.push(event.clone());
        publish_json(ctx, "logs", "job.log", event).await
    }
}

impl Resource for Job {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "job".into(),
            name: None,
            labels: self.labels.clone(),
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
        let snapshot = self.snapshot_value();
        Box::pin(async move { Ok(snapshot) })
    }
}

impl Actor for Job {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            match name {
                "cancel" => {
                    let output = self.cancel_with_ctx(&ctx).await?;
                    let output = serde_json::to_value(output)
                        .map_err(|err| TransitionError::Internal(err.to_string()))?;
                    Ok(TransitionOutcome::Completed {
                        output: Some(output),
                        snapshot: self.snapshot_for_transition(&ctx),
                    })
                }
                "retry" => {
                    let input = RetryJob::from_input(input)?;
                    let handle = self.retry_with_ctx(&ctx, input).await?;
                    let output = serde_json::to_value(&handle)
                        .map_err(|err| TransitionError::Internal(err.to_string()))?;
                    Ok(TransitionOutcome::Accepted {
                        job: handle.to_outcome_handle(false),
                        output: Some(output),
                    })
                }
                other => Err(TransitionError::NotAllowed(format!(
                    "transition `{other}` not supported by job"
                ))),
            }
        })
    }
}

#[derive(Debug, Clone)]
struct JobErrorInfo {
    code: String,
    message: String,
}

fn example_labels() -> BTreeMap<String, String> {
    BTreeMap::from([(EXAMPLE_LABEL_KEY.into(), EXAMPLE_LABEL_VALUE.into())])
}

fn queue_properties(submitted: u64) -> Map<String, Value> {
    let mut props = Map::new();
    props.insert("submitted_count".into(), json!(submitted));
    props.insert("queued_count".into(), json!(0));
    props.insert("running_count".into(), json!(0));
    props.insert("succeeded_count".into(), json!(0));
    props.insert("failed_count".into(), json!(0));
    props.insert("cancelled_count".into(), json!(0));
    props
}

fn option_json(value: Option<&str>) -> Value {
    value.map(Value::from).unwrap_or(Value::Null)
}

#[cfg(test)]
fn progress_percent(step: u32, total: u32) -> u8 {
    ((step.saturating_mul(100)) / total.max(1)).min(99) as u8
}

fn ctx_resource_id(ctx: &TransitionCtx) -> Result<String, TransitionError> {
    ctx.resource_id()
        .map(str::to_string)
        .ok_or_else(|| TransitionError::Internal("TransitionCtx has no actor identity".into()))
}

async fn publish_json<T: Serialize>(
    ctx: &TransitionCtx,
    stream: &str,
    payload_kind: &str,
    event: T,
) -> Result<(), TransitionError> {
    let data =
        serde_json::to_value(event).map_err(|err| TransitionError::Internal(err.to_string()))?;
    ctx.publish(stream, payload_kind, 1, data).await
}

async fn publish_json_for_resource<T: Serialize>(
    ctx: &TransitionCtx,
    resource_id: &str,
    resource_kind: &str,
    stream: &str,
    payload_kind: &str,
    event: T,
) -> Result<(), TransitionError> {
    let data =
        serde_json::to_value(event).map_err(|err| TransitionError::Internal(err.to_string()))?;
    ctx.publish_for_resource(resource_id, resource_kind, stream, payload_kind, 1, data)
        .await
}

fn job_streams() -> Vec<crate::http::StreamSpec> {
    ["lifecycle", "progress", "logs"]
        .into_iter()
        .map(|name| crate::http::StreamSpec {
            name: name.into(),
            kind: "object".into(),
        })
        .collect()
}

fn submit_spec() -> TransitionSpec {
    TransitionSpec {
        name: "submit".into(),
        title: Some("Submit job".into()),
        allowed_states: vec!["open".into()],
        input_schema: None,
        output_schema: None,
        result: TransitionResultKind::AsyncJob,
        idempotency: Idempotency::Supported,
        effect: Effect::Unsafe,
        required_scopes: vec![],
        fields: vec![],
    }
}

fn cancel_spec() -> TransitionSpec {
    TransitionSpec {
        name: "cancel".into(),
        title: Some("Cancel job".into()),
        allowed_states: vec!["queued".into(), "running".into()],
        input_schema: None,
        output_schema: None,
        result: TransitionResultKind::Sync,
        idempotency: Idempotency::Supported,
        effect: Effect::UnsafeIdempotent,
        required_scopes: vec![],
        fields: vec![],
    }
}

fn retry_spec() -> TransitionSpec {
    TransitionSpec {
        name: "retry".into(),
        title: Some("Retry job".into()),
        allowed_states: vec!["failed".into()],
        input_schema: None,
        output_schema: None,
        result: TransitionResultKind::AsyncJob,
        idempotency: Idempotency::None,
        effect: Effect::Unsafe,
        required_scopes: vec![],
        fields: vec![],
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::events::{EventEnvelope, TopicPattern};
    use crate::runtime::{Node, NodeBuilder, NodeHandle, RequestCtx, ResourceProxy};

    async fn snapshot_of<R: Resource>(resource: &R) -> ResourceSnapshot {
        resource
            .snapshot(ResourceCtx::new_test())
            .await
            .expect("snapshot should render")
    }

    fn transition_input<T: serde::Serialize>(input: T) -> TransitionInput {
        let fields = serde_json::to_value(input)
            .expect("serialize transition input")
            .as_object()
            .expect("transition input should serialize as an object")
            .clone()
            .into_iter()
            .collect();
        TransitionInput { fields }
    }

    fn empty_input() -> TransitionInput {
        TransitionInput::default()
    }

    fn transition<'a>(snapshot: &'a ResourceSnapshot, name: &str) -> &'a TransitionAffordance {
        snapshot
            .transitions
            .iter()
            .find(|transition| transition.name() == name)
            .unwrap_or_else(|| panic!("missing transition {name}"))
    }

    fn test_ctx(node: &Node, id: &str) -> TransitionCtx {
        TransitionCtx::new(RequestCtx::default(), node.id()).with_test_actor(
            node,
            id,
            "job",
            example_labels(),
        )
    }

    async fn recv(sub: &mut crate::events::Subscription) -> EventEnvelope {
        tokio::time::timeout(Duration::from_secs(1), sub.rx.recv())
            .await
            .expect("event should arrive")
            .expect("subscription should remain open")
    }

    async fn job_proxy(handle: &NodeHandle, id: &str) -> ResourceProxy {
        handle
            .query("where kind = \"job\"")
            .await
            .expect("query parses")
            .into_iter()
            .find(|resource| resource.id() == id)
            .expect("job proxy")
    }

    #[tokio::test]
    async fn job_queue_and_job_snapshots_are_resources_not_devices() {
        let queue = JobQueue::new("default");
        let queue_snapshot = snapshot_of(&queue).await;

        assert_eq!(queue_snapshot.kind, "job.queue");
        assert_eq!(queue_snapshot.name.as_deref(), Some("default"));
        assert_eq!(queue_snapshot.state.as_deref(), Some("open"));
        assert_eq!(
            queue_snapshot.labels.get("example").map(String::as_str),
            Some("jobs")
        );
        assert_eq!(queue.actor_spec().transitions.len(), 1);

        let submit = transition(&queue_snapshot, "submit");
        assert!(submit.available);
        assert!(submit.unavailable_reason.is_none());
        assert_eq!(submit.spec.result, TransitionResultKind::AsyncJob);

        let states = [
            JobState::Queued,
            JobState::Running,
            JobState::Succeeded,
            JobState::Failed,
            JobState::Cancelling,
            JobState::Cancelled,
        ];

        for state in states {
            let job = Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 3 })
                .with_state(state.clone());
            let snapshot = snapshot_of(&job).await;

            assert_eq!(snapshot.kind, "job");
            assert_eq!(snapshot.state.as_deref(), Some(state.as_str()));
            assert_eq!(
                snapshot.labels.get("example").map(String::as_str),
                Some("jobs")
            );
            assert_eq!(
                snapshot.properties.get("queue"),
                Some(&serde_json::json!("default"))
            );
            assert_eq!(snapshot.properties.get("attempt"), Some(&json!(1)));
            assert_eq!(job.actor_spec().transitions.len(), 2);

            let stream_names = snapshot
                .streams
                .iter()
                .map(|stream| stream.name.as_str())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                stream_names,
                BTreeSet::from(["lifecycle", "logs", "progress"])
            );

            let cancel = transition(&snapshot, "cancel");
            let retry = transition(&snapshot, "retry");
            match state {
                JobState::Queued | JobState::Running => {
                    assert!(cancel.available, "cancel should be available in {state:?}");
                    assert!(cancel.unavailable_reason.is_none());
                    assert!(
                        !retry.available,
                        "retry should not be available in {state:?}"
                    );
                    assert!(retry.unavailable_reason.is_some());
                }
                JobState::Failed => {
                    assert!(!cancel.available);
                    assert!(cancel.unavailable_reason.is_some());
                    assert!(retry.available);
                    assert!(retry.unavailable_reason.is_none());
                }
                JobState::Succeeded | JobState::Cancelling | JobState::Cancelled => {
                    assert!(!cancel.available);
                    assert!(cancel.unavailable_reason.is_some());
                    assert!(!retry.available);
                    assert!(retry.unavailable_reason.is_some());
                }
            }
        }
    }

    #[tokio::test]
    async fn submit_returns_typed_job_handle_and_creates_discoverable_job_resource() {
        let node = Arc::new(NodeBuilder::new("runner").build());
        node.register_actor(JobQueue::new("default"))
            .await
            .expect("queue registers");

        let handle = NodeHandle::new(node.clone());
        let queue = handle
            .query("where kind = \"job.queue\"")
            .await
            .expect("query parses")
            .into_iter()
            .next()
            .expect("queue is discoverable");

        let mut submit = SubmitJob::new(FakeCommand::SuccessAfterTicks { ticks: 3 });
        submit.owner = Some("kevin".into());
        submit.priority = 7;
        submit.labels.insert("team".into(), "ops".into());

        let outcome = queue
            .transition("submit", transition_input(submit))
            .await
            .expect("submit should be accepted");
        let TransitionOutcome::Accepted { job, output } = outcome else {
            panic!("submit should return Accepted");
        };

        assert!(job.created);
        assert_eq!(job.kind, "job");
        assert_eq!(job.location, format!("/resources/{}", job.id));

        let output = output.expect("submit returns typed output");
        assert_eq!(output["jobId"], job.id);
        assert_eq!(output["href"], job.location);
        assert_eq!(output["state"], "queued");

        let jobs = handle
            .query("where kind = \"job\"")
            .await
            .expect("query parses");
        let job_resource = jobs
            .into_iter()
            .find(|resource| resource.id() == job.id)
            .expect("created job is discoverable");
        let snapshot = job_resource.snapshot().await.expect("job snapshot");

        assert_eq!(snapshot.state.as_deref(), Some("queued"));
        assert_eq!(snapshot.properties.get("owner"), Some(&json!("kevin")));
        assert_eq!(snapshot.properties.get("priority"), Some(&json!(7)));
        assert_eq!(snapshot.labels.get("team").map(String::as_str), Some("ops"));

        let owner_matches = handle
            .query("where properties.owner = \"kevin\"")
            .await
            .expect("owner query parses");
        assert_eq!(owner_matches.len(), 1);
        assert_eq!(owner_matches[0].id(), job.id);

        let label_matches = handle
            .query("where labels.team = \"ops\"")
            .await
            .expect("label query parses");
        assert_eq!(label_matches.len(), 1);
        assert_eq!(label_matches[0].id(), job.id);
    }

    #[tokio::test]
    async fn cancel_queued_job_moves_directly_to_cancelled() {
        let mut job = Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 3 });

        let output = job.cancel().expect("queued cancel succeeds");
        assert!(output.accepted);
        assert_eq!(output.state, JobState::Cancelled.as_str());
        assert_eq!(job.state(), &JobState::Cancelled);

        let snapshot = snapshot_of(&job).await;
        assert_eq!(
            snapshot.state.as_deref(),
            Some(JobState::Cancelled.as_str())
        );
        assert!(!transition(&snapshot, "cancel").available);
    }

    #[tokio::test]
    async fn cancel_unavailable_after_succeeded() {
        let node = Arc::new(NodeBuilder::new("runner").build());
        let id = node
            .register_actor(
                Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 1 })
                    .with_state(JobState::Succeeded),
            )
            .await
            .expect("job registers");

        let handle = NodeHandle::new(node);
        let job = handle
            .query("where kind = \"job\"")
            .await
            .expect("query parses")
            .into_iter()
            .find(|resource| resource.id() == id)
            .expect("job is discoverable");

        let snapshot = job.snapshot().await.expect("job snapshot");
        let cancel = transition(&snapshot, "cancel");
        assert!(!cancel.available);
        assert!(cancel.unavailable_reason.is_some());

        let err = job
            .transition("cancel", empty_input())
            .await
            .expect_err("cancel after success should conflict");
        match err {
            TransitionError::Conflict(message) => assert!(message.contains("succeeded")),
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn completed_transition_snapshot_uses_assigned_resource_identity() {
        let node = Arc::new(NodeBuilder::new("runner").build());
        let id = node
            .register_actor(Job::new(
                "default",
                FakeCommand::SuccessAfterTicks { ticks: 1 },
            ))
            .await
            .expect("job registers");

        let handle = NodeHandle::new(node);
        let job = handle
            .query("where kind = \"job\"")
            .await
            .expect("query parses")
            .into_iter()
            .find(|resource| resource.id() == id)
            .expect("job is discoverable");

        let outcome = job
            .transition("cancel", empty_input())
            .await
            .expect("queued cancel succeeds");
        let TransitionOutcome::Completed { snapshot, .. } = outcome else {
            panic!("cancel should complete synchronously");
        };
        assert_eq!(snapshot.id, id);
        assert_eq!(snapshot.kind, "job");
        assert_eq!(snapshot.node, "runner");
        assert_eq!(
            snapshot.state.as_deref(),
            Some(JobState::Cancelled.as_str())
        );
    }

    #[tokio::test]
    async fn advance_is_not_a_runtime_transition() {
        let node = Arc::new(NodeBuilder::new("runner").build());
        let id = node
            .register_actor(Job::new(
                "default",
                FakeCommand::SuccessAfterTicks { ticks: 1 },
            ))
            .await
            .expect("job registers");

        let handle = NodeHandle::new(node);
        let job = handle
            .query("where kind = \"job\"")
            .await
            .expect("query parses")
            .into_iter()
            .find(|resource| resource.id() == id)
            .expect("job is discoverable");

        let err = job
            .transition("advance", empty_input())
            .await
            .expect_err("advance is only a direct test driver");
        match err {
            TransitionError::NotAllowed(message) => assert!(message.contains("advance")),
            other => panic!("expected NotAllowed, got {other:?}"),
        }

        let snapshot = job.snapshot().await.expect("job snapshot");
        assert_eq!(snapshot.state.as_deref(), Some(JobState::Queued.as_str()));
        assert!(
            snapshot
                .transitions
                .iter()
                .all(|transition| transition.name() != "advance")
        );
    }

    #[tokio::test]
    async fn retry_failed_job_reuses_id_increments_attempt_and_restarts() {
        let node = Arc::new(NodeBuilder::new("runner").build());
        let id = node
            .register_actor(
                Job::new("default", FakeCommand::FailAtStep { step: 1 })
                    .with_state(JobState::Failed),
            )
            .await
            .expect("job registers");

        let handle = NodeHandle::new(node);
        let job = job_proxy(&handle, &id).await;

        let outcome = job
            .transition("retry", transition_input(RetryJob { reset_logs: true }))
            .await
            .expect("failed job can retry");
        let TransitionOutcome::Accepted {
            job: retried,
            output,
        } = outcome
        else {
            panic!("retry should return Accepted");
        };
        assert!(!retried.created);
        assert_eq!(retried.id, id);
        assert_eq!(retried.location, format!("/resources/{id}"));

        let output = output.expect("retry returns typed output");
        assert_eq!(output["jobId"], id);
        assert_eq!(output["state"], "queued");

        let snapshot = job.snapshot().await.expect("job snapshot");
        assert_eq!(snapshot.state.as_deref(), Some(JobState::Queued.as_str()));
        assert_eq!(snapshot.properties.get("attempt"), Some(&json!(2)));
        assert_eq!(snapshot.properties.get("progress"), Some(&json!(0)));
        assert_eq!(snapshot.properties.get("error"), Some(&Value::Null));
        assert_eq!(snapshot.properties.get("log_count"), Some(&json!(0)));
    }

    #[tokio::test]
    async fn retry_is_unavailable_until_failed() {
        let node = Arc::new(NodeBuilder::new("runner").build());
        let queued_id = node
            .register_actor(Job::new(
                "default",
                FakeCommand::SuccessAfterTicks { ticks: 1 },
            ))
            .await
            .expect("queued job registers");
        let cancelled_id = node
            .register_actor(
                Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 1 })
                    .with_state(JobState::Cancelled),
            )
            .await
            .expect("cancelled job registers");

        let handle = NodeHandle::new(node);
        for id in [queued_id, cancelled_id] {
            let job = job_proxy(&handle, &id).await;

            let snapshot = job.snapshot().await.expect("job snapshot");
            let retry = transition(&snapshot, "retry");
            assert!(!retry.available);
            assert!(retry.unavailable_reason.is_some());

            let err = job
                .transition("retry", transition_input(RetryJob { reset_logs: false }))
                .await
                .expect_err("retry should be rejected outside failed state");
            match err {
                TransitionError::Conflict(message) => assert!(message.contains("retry")),
                other => panic!("expected Conflict, got {other:?}"),
            }
        }
    }

    #[test]
    fn logs_stream_uses_backpressure_without_silent_drop() {
        let logs = Job::stream_subscribe_opts("logs", 1);
        assert!(matches!(
            logs.slow_consumer_policy,
            SlowConsumerPolicy::Backpressure
        ));

        let lifecycle = Job::stream_subscribe_opts("lifecycle", 1);
        assert!(matches!(
            lifecycle.slow_consumer_policy,
            SlowConsumerPolicy::Backpressure
        ));
    }

    #[test]
    #[should_panic(expected = "unknown job stream")]
    fn unknown_job_stream_subscription_options_are_rejected() {
        let _ = Job::stream_subscribe_opts("metrics", 1);
    }

    #[tokio::test]
    async fn submit_emits_submitted_lifecycle_for_preexisting_subscriber() {
        let node = Arc::new(NodeBuilder::new("runner").build());
        node.register_actor(JobQueue::new("default"))
            .await
            .expect("queue registers");

        let mut lifecycle = node.events().subscribe(
            TopicPattern::parse("runner/job/*/lifecycle").unwrap(),
            Job::stream_subscribe_opts("lifecycle", 8),
        );

        let handle = NodeHandle::new(node.clone());
        let queue = handle
            .query("where kind = \"job.queue\"")
            .await
            .expect("query parses")
            .into_iter()
            .next()
            .expect("queue proxy");

        let outcome = queue
            .transition(
                "submit",
                transition_input(SubmitJob::new(FakeCommand::SuccessAfterTicks { ticks: 1 })),
            )
            .await
            .expect("submit succeeds");
        let TransitionOutcome::Accepted { job, .. } = outcome else {
            panic!("submit should return Accepted");
        };

        let submitted = recv(&mut lifecycle).await;
        assert_eq!(submitted.resource_id, job.id);
        assert_eq!(submitted.resource_kind, "job");
        assert_eq!(submitted.stream, "lifecycle");
        assert_eq!(submitted.payload_kind, "job.lifecycle");
        assert_eq!(submitted.data["state"], "queued");
        assert_eq!(submitted.data["reason"], "submitted");
        assert!(submitted.causation_id.is_some());
    }

    #[tokio::test]
    async fn retry_emits_retried_lifecycle_event() {
        let node = Arc::new(NodeBuilder::new("runner").build());
        let id = node
            .register_actor(
                Job::new("default", FakeCommand::FailAtStep { step: 1 })
                    .with_state(JobState::Failed),
            )
            .await
            .expect("job registers");
        let handle = NodeHandle::new(node.clone());
        let job = job_proxy(&handle, &id).await;

        let mut lifecycle = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/lifecycle")).unwrap(),
            Job::stream_subscribe_opts("lifecycle", 8),
        );

        job.transition("retry", transition_input(RetryJob { reset_logs: false }))
            .await
            .expect("retry succeeds");

        let retried = recv(&mut lifecycle).await;
        assert_eq!(retried.payload_kind, "job.lifecycle");
        assert_eq!(retried.data["state"], "queued");
        assert_eq!(retried.data["reason"], "retried");
        assert!(retried.causation_id.is_some());
    }

    #[test]
    fn success_after_ticks_reaches_succeeded_with_progress_100() {
        let mut job = Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 3 });

        assert_eq!(
            job.snapshot_value().state.as_deref(),
            Some(JobState::Queued.as_str())
        );

        job.advance().expect("first tick starts the job");
        assert_eq!(
            job.snapshot_value().state.as_deref(),
            Some(JobState::Running.as_str())
        );

        for _ in 0..3 {
            job.advance().expect("tick succeeds");
        }

        let snapshot = job.snapshot_value();
        assert_eq!(
            snapshot.state.as_deref(),
            Some(JobState::Succeeded.as_str())
        );
        assert_eq!(
            snapshot.properties.get("progress"),
            Some(&serde_json::json!(100))
        );
        assert_eq!(
            snapshot.properties.get("result"),
            Some(&serde_json::json!({ "status": "ok" }))
        );
    }

    #[test]
    fn fail_at_step_reaches_failed_with_error() {
        let mut job = Job::new("default", FakeCommand::FailAtStep { step: 2 });

        job.advance().expect("first tick starts the job");
        job.advance().expect("first work tick runs");
        job.advance().expect("second work tick fails");

        let snapshot = job.snapshot_value();
        assert_eq!(snapshot.state.as_deref(), Some(JobState::Failed.as_str()));
        assert_eq!(
            snapshot
                .properties
                .get("error")
                .and_then(|error| error.get("code")),
            Some(&serde_json::json!("command_failed"))
        );
        assert_ne!(
            snapshot.properties.get("finished_at"),
            Some(&serde_json::Value::Null)
        );

        let retry = snapshot
            .transitions
            .iter()
            .find(|transition| transition.name() == "retry")
            .expect("retry transition");
        assert!(retry.available);
        assert!(retry.unavailable_reason.is_none());
    }

    #[test]
    fn cancel_running_job_moves_through_cancelling_then_cancelled() {
        let mut job = Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 3 });
        job.advance().expect("first tick starts running");

        let output = job.cancel().expect("running cancel is accepted");
        assert!(output.accepted);
        assert_eq!(output.state, JobState::Cancelling.as_str());
        assert_eq!(
            job.snapshot_value().state.as_deref(),
            Some(JobState::Cancelling.as_str())
        );

        job.advance().expect("next tick settles cancellation");
        assert_eq!(
            job.snapshot_value().state.as_deref(),
            Some(JobState::Cancelled.as_str())
        );
    }

    #[tokio::test]
    async fn job_progress_and_logs_are_streamed_with_envelopes() {
        let node = NodeBuilder::new("runner").build();
        let id = "job-1";
        let mut job = Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 3 });

        let mut lifecycle = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/lifecycle")).unwrap(),
            Job::stream_subscribe_opts("lifecycle", 4),
        );
        let mut progress = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/progress")).unwrap(),
            Job::stream_subscribe_opts("progress", 4),
        );
        let mut logs = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/logs")).unwrap(),
            Job::stream_subscribe_opts("logs", 4),
        );

        job.advance_with_ctx(&test_ctx(&node, id))
            .await
            .expect("start tick succeeds");
        job.advance_with_ctx(&test_ctx(&node, id))
            .await
            .expect("progress tick succeeds");

        let lifecycle = recv(&mut lifecycle).await;
        assert_eq!(lifecycle.resource_id, id);
        assert_eq!(lifecycle.resource_kind, "job");
        assert_eq!(lifecycle.stream, "lifecycle");
        assert_eq!(lifecycle.payload_kind, "job.lifecycle");
        assert!(lifecycle.causation_id.is_some());

        let progress = recv(&mut progress).await;
        assert_eq!(progress.resource_id, id);
        assert_eq!(progress.payload_kind, "job.progress");
        assert!(!progress.event_id.as_str().is_empty());
        assert!(progress.sequence >= 1);
        assert!(progress.causation_id.is_some());

        let logs = recv(&mut logs).await;
        assert_eq!(logs.resource_id, id);
        assert_eq!(logs.payload_kind, "job.log");
        assert!(logs.causation_id.is_some());
    }

    #[tokio::test]
    async fn progress_stream_coalesces_latest_progress_per_job() {
        let node = NodeBuilder::new("runner").build();
        let id = "job-1";
        let mut job = Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 5 });

        let mut sub = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/progress")).unwrap(),
            Job::stream_subscribe_opts("progress", 1),
        );

        job.advance_with_ctx(&test_ctx(&node, id)).await.unwrap();
        for _ in 0..3 {
            job.advance_with_ctx(&test_ctx(&node, id)).await.unwrap();
        }

        let event = recv(&mut sub).await;
        assert_eq!(event.payload_kind, "job.progress");
        assert_eq!(event.data["jobId"], id);
        assert_eq!(event.data["attempt"], 1);
        assert_eq!(event.data["step"], 3);
    }

    #[tokio::test]
    async fn success_job_emits_lifecycle_progress_logs_and_reaches_succeeded() {
        let node = NodeBuilder::new("runner").build();
        let id = "job-1";
        let mut job = Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 2 });

        let mut lifecycle = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/lifecycle")).unwrap(),
            Job::stream_subscribe_opts("lifecycle", 8),
        );
        let mut progress = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/progress")).unwrap(),
            Job::stream_subscribe_opts("progress", 8),
        );
        let mut logs = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/logs")).unwrap(),
            Job::stream_subscribe_opts("logs", 8),
        );

        job.advance_with_ctx(&test_ctx(&node, id)).await.unwrap();
        job.advance_with_ctx(&test_ctx(&node, id)).await.unwrap();
        job.advance_with_ctx(&test_ctx(&node, id)).await.unwrap();

        let started = recv(&mut lifecycle).await;
        let succeeded = recv(&mut lifecycle).await;
        assert_eq!(started.data["state"], "running");
        assert_eq!(succeeded.data["state"], "succeeded");
        assert!(started.causation_id.is_some());
        assert!(succeeded.causation_id.is_some());

        let progress = recv(&mut progress).await;
        assert_eq!(progress.data["percent"], 50);
        assert!(progress.causation_id.is_some());

        let log = recv(&mut logs).await;
        assert_eq!(log.payload_kind, "job.log");
        assert!(log.causation_id.is_some());

        let snapshot = job.snapshot_value();
        assert_eq!(snapshot.state.as_deref(), Some("succeeded"));
        assert_eq!(
            snapshot.properties.get("progress"),
            Some(&serde_json::json!(100))
        );
    }

    #[tokio::test]
    async fn failed_job_records_one_failure_log_with_real_job_id() {
        let node = NodeBuilder::new("runner").build();
        let id = "job-1";
        let mut job = Job::new("default", FakeCommand::FailAtStep { step: 1 });
        let mut logs = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/logs")).unwrap(),
            Job::stream_subscribe_opts("logs", 8),
        );

        job.advance_with_ctx(&test_ctx(&node, id)).await.unwrap();
        job.advance_with_ctx(&test_ctx(&node, id)).await.unwrap();

        let started = recv(&mut logs).await;
        let failed = recv(&mut logs).await;
        assert_eq!(started.data["jobId"], id);
        assert_eq!(started.data["level"], "info");
        assert_eq!(failed.data["jobId"], id);
        assert_eq!(failed.data["level"], "error");
        assert_eq!(job.logs.len(), 2);
    }

    #[tokio::test]
    async fn running_cancel_uses_consistent_lifecycle_reason() {
        let node = NodeBuilder::new("runner").build();
        let id = "job-1";
        let mut job = Job::new("default", FakeCommand::SuccessAfterTicks { ticks: 3 });
        let mut lifecycle = node.events().subscribe(
            TopicPattern::parse(&format!("runner/job/{id}/lifecycle")).unwrap(),
            Job::stream_subscribe_opts("lifecycle", 8),
        );

        job.advance_with_ctx(&test_ctx(&node, id)).await.unwrap();
        job.cancel_with_ctx(&test_ctx(&node, id)).await.unwrap();
        job.advance_with_ctx(&test_ctx(&node, id)).await.unwrap();

        let _running = recv(&mut lifecycle).await;
        let cancelling = recv(&mut lifecycle).await;
        let cancelled = recv(&mut lifecycle).await;
        assert_eq!(cancelling.data["state"], "cancelling");
        assert_eq!(cancelling.data["reason"], "user_cancelled");
        assert_eq!(cancelled.data["state"], "cancelled");
        assert_eq!(cancelled.data["reason"], "user_cancelled");
    }
}
