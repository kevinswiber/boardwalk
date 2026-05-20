//! Runnable job-runner example built on Boardwalk's resource and actor runtime.
//!
//! The example owns a tiny HTTP adapter so the flow can be exercised end to end.
//! Jobs are advanced by a spawned task and short `tokio::time::sleep` intervals;
//! production schedulers should use an explicit queue, tick, and shutdown boundary.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use boardwalk::events::{
    EventEnvelope, NodeId, SlowConsumerPolicy, StreamId, SubscribeOpts, TopicPattern,
};
use boardwalk::query::FieldPath;
use boardwalk::runtime::{
    Actor, ActorCtx, ActorError, DynFuture, Node, NodeBuilder, NodeHandle, Resource, ResourceCtx,
    ResourceError, ResourceProxy, TransitionCtx, TransitionError,
};
use boardwalk::{
    Effect, Idempotency, JobHandle as OutcomeJobHandle, ResourceSnapshot, ResourceSpec,
    SnapshotStreamSpec, StreamKind, StreamSpec, TransitionAffordance, TransitionInput,
    TransitionOutcome, TransitionResultKind, TransitionSpec,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

const QUEUE_ID: &str = "queue-default";
const QUEUE_NAME: &str = "default";
const NODE_NAME: &str = "runner";
const FIXED_SUBMITTED_AT: &str = "2026-01-01T00:00:00Z";
const FIXED_STARTED_AT: &str = "2026-01-01T00:00:01Z";
const FIXED_FINISHED_AT: &str = "2026-01-01T00:00:02Z";
const STREAM_OUTBOUND_CAPACITY: usize = 16;

pub async fn serve(addr: SocketAddr) -> anyhow::Result<()> {
    let node = build_node().await?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(node)).await?;
    Ok(())
}

#[doc(hidden)]
pub async fn spawn_test_server() -> anyhow::Result<RunningExample> {
    let node = build_node().await?;
    let app = router(node.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok(RunningExample { addr, server })
}

#[doc(hidden)]
pub struct RunningExample {
    addr: SocketAddr,
    server: JoinHandle<()>,
}

impl RunningExample {
    #[doc(hidden)]
    pub fn url(&self, path: &str) -> String {
        if path.starts_with('/') {
            format!("http://{}{}", self.addr, path)
        } else {
            format!("http://{}/{}", self.addr, path)
        }
    }

    #[doc(hidden)]
    pub fn queue_id(&self) -> &'static str {
        QUEUE_ID
    }
}

impl Drop for RunningExample {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn build_node() -> anyhow::Result<Arc<Node>> {
    let node = Arc::new(NodeBuilder::new(NODE_NAME).build());
    node.register_with_id(QUEUE_ID.into(), JobQueue::new(QUEUE_NAME))
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
    Ok(node)
}

fn router(node: Arc<Node>) -> Router {
    Router::new()
        .route("/resources", get(resources_get))
        .route("/resources/{id}", get(resource_get))
        .route(
            "/resources/{id}/transitions/{transition}",
            post(resource_transition_post),
        )
        .route("/resources/{id}/streams/{stream}", get(resource_stream_get))
        .with_state(AppState { node })
}

#[derive(Clone)]
struct AppState {
    node: Arc<Node>,
}

async fn resources_get(State(state): State<AppState>) -> Response {
    Json(
        state
            .node
            .resources()
            .await
            .into_iter()
            .map(|snapshot| snapshot.to_query_value())
            .collect::<Vec<_>>(),
    )
    .into_response()
}

async fn resource_get(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match snapshot(&state.node, &id).await {
        Ok(snapshot) => Json(snapshot.to_query_value()).into_response(),
        Err(status) => status.into_response(),
    }
}

async fn resource_transition_post(
    State(state): State<AppState>,
    Path((id, transition)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Response {
    let fields = match body {
        Value::Object(fields) => fields.into_iter().collect(),
        Value::Null => BTreeMap::new(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "transition input must be a JSON object",
            )
                .into_response();
        }
    };

    let proxy = match proxy(&state.node, &id).await {
        Ok(proxy) => proxy,
        Err(status) => return status.into_response(),
    };
    match proxy
        .transition(&transition, TransitionInput { fields })
        .await
    {
        Ok(outcome) => transition_response(outcome),
        Err(TransitionError::InvalidInput(message)) => {
            (StatusCode::BAD_REQUEST, message).into_response()
        }
        Err(TransitionError::NotAllowed(message) | TransitionError::Conflict(message)) => {
            (StatusCode::CONFLICT, message).into_response()
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:?}")).into_response(),
    }
}

async fn resource_stream_get(
    State(state): State<AppState>,
    Path((id, stream)): Path<(String, String)>,
) -> Response {
    let Some(kind) = resource_kind(&state.node, &id).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !["lifecycle", "logs", "progress"].contains(&stream.as_str()) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let stream_id = StreamId::for_resource(&NodeId::new(state.node.id()), &id, &stream);
    let replay = state
        .node
        .events()
        .replay_cache()
        .events_after(&stream_id, 0);
    let topic = format!("{}/{}/{}/{}", state.node.id(), kind, id, stream);
    let sub = match TopicPattern::parse(&topic) {
        Ok(topic) => state
            .node
            .events()
            .subscribe(topic, stream_subscribe_opts(&stream)),
        Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    };

    let lines = futures::stream::unfold((replay.into_iter(), sub), |(mut replay, mut sub)| async {
        let event = match replay.next() {
            Some(event) => event,
            None => sub.rx.recv().await?,
        };
        let line = event_line(&event);
        Some((Ok::<Bytes, Infallible>(Bytes::from(line)), (replay, sub)))
    });

    let mut response = Response::new(Body::from_stream(lines));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    response
}

fn stream_subscribe_opts(stream: &str) -> SubscribeOpts {
    let slow_consumer_policy = match stream {
        "progress" => SlowConsumerPolicy::Coalesce {
            key_path: FieldPath::parse("data.coalesceKey"),
        },
        "logs" | "lifecycle" => SlowConsumerPolicy::Backpressure,
        _ => SlowConsumerPolicy::Disconnect,
    };
    SubscribeOpts {
        limit: None,
        outbound_capacity: Some(STREAM_OUTBOUND_CAPACITY),
        slow_consumer_policy,
    }
}

async fn snapshot(node: &Arc<Node>, id: &str) -> Result<ResourceSnapshot, StatusCode> {
    let proxy = proxy(node, id).await?;
    proxy
        .snapshot()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn proxy(node: &Arc<Node>, id: &str) -> Result<ResourceProxy, StatusCode> {
    let handle = NodeHandle::new(node.clone());
    for query in ["where kind = \"job.queue\"", "where kind = \"job\""] {
        let resources = handle
            .query(query)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if let Some(resource) = resources.into_iter().find(|resource| resource.id() == id) {
            return Ok(resource);
        }
    }
    Err(StatusCode::NOT_FOUND)
}

async fn resource_kind(node: &Arc<Node>, id: &str) -> Option<String> {
    node.resources()
        .await
        .into_iter()
        .find(|snapshot| snapshot.id == id)
        .map(|snapshot| snapshot.kind)
}

fn transition_response(outcome: TransitionOutcome) -> Response {
    match outcome {
        TransitionOutcome::Completed { output, snapshot } => Json(json!({
            "output": output,
            "snapshot": snapshot.to_query_value(),
        }))
        .into_response(),
        TransitionOutcome::Accepted { job, output } => {
            let status = if job.created {
                StatusCode::CREATED
            } else {
                StatusCode::ACCEPTED
            };
            let location = job.location.clone();
            let mut response = (
                status,
                Json(json!({
                    "output": output,
                    "job": {
                        "id": job.id,
                        "kind": job.kind,
                        "location": location,
                    },
                })),
            )
                .into_response();
            if status == StatusCode::CREATED {
                response.headers_mut().insert(
                    header::LOCATION,
                    HeaderValue::from_str(&job.location)
                        .unwrap_or_else(|_| HeaderValue::from_static("/resources")),
                );
            }
            response
        }
    }
}

fn event_line(event: &EventEnvelope) -> String {
    serde_json::to_string(&json!({
        "topic": event.topic(),
        "timestamp": event.timestamp_ms(),
        "data": event.data,
        "eventId": event.event_id.as_str(),
        "streamId": event.stream_id.as_str(),
        "sequence": event.sequence,
        "nodeId": event.node_id.as_str(),
        "resourceId": event.resource_id,
        "resourceKind": event.resource_kind,
        "payloadKind": event.payload_kind,
        "payloadVersion": event.payload_version,
        "envelopeVersion": event.envelope_version,
        "stream": event.stream,
    }))
    .unwrap_or_default()
        + "\n"
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum FakeCommand {
    SuccessAfterTicks { ticks: u32 },
    FailAtStep { step: u32 },
}

impl FakeCommand {
    fn total_steps(&self) -> u32 {
        match self {
            Self::SuccessAfterTicks { ticks } => (*ticks).max(1),
            Self::FailAtStep { step } => (*step).max(1),
        }
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SubmitJob {
    command: FakeCommand,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    priority: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobHandle {
    job_id: String,
    href: String,
    state: String,
    streams: JobStreams,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobStreams {
    lifecycle: String,
    progress: String,
    logs: String,
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

#[derive(Debug)]
struct JobQueue {
    name: String,
    submitted: u64,
}

impl JobQueue {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            submitted: 0,
        }
    }
}

impl Resource for JobQueue {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "job.queue".into(),
            name: Some(self.name.clone()),
            labels: example_labels(),
            property_schema: None,
            streams: vec![],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let mut properties = Map::new();
        properties.insert("submitted_count".into(), json!(self.submitted));
        properties.insert("queued_count".into(), json!(0));
        properties.insert("running_count".into(), json!(0));
        properties.insert("succeeded_count".into(), json!(0));
        properties.insert("failed_count".into(), json!(0));
        properties.insert("cancelled_count".into(), json!(0));
        let snapshot = ResourceSnapshot {
            id: String::new(),
            kind: "job.queue".into(),
            name: Some(self.name.clone()),
            state: Some("open".into()),
            node: String::new(),
            properties,
            labels: example_labels(),
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
            if name != "submit" {
                return Err(TransitionError::NotAllowed(format!(
                    "transition `{name}` not supported by job queue"
                )));
            }
            let input = input_from_fields::<SubmitJob>(input)?;
            let id = ctx
                .register_actor(Job::from_submit(self.name.clone(), input))
                .await?;
            self.submitted += 1;
            let handle = JobHandle::for_job(id, "queued");
            let output = serde_json::to_value(&handle)
                .map_err(|err| TransitionError::Internal(err.to_string()))?;
            Ok(TransitionOutcome::Accepted {
                job: handle.to_outcome_handle(true),
                output: Some(output),
            })
        })
    }
}

#[derive(Debug)]
struct Job {
    shared: Arc<Mutex<JobData>>,
    runner: Option<JoinHandle<()>>,
}

impl Job {
    fn from_submit(queue: String, input: SubmitJob) -> Self {
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
                    let mut data = self.shared.lock().await;
                    data.cancel()?;
                    publish_lifecycle(&ctx, &data, Some("user_cancelled")).await?;
                    Ok(TransitionOutcome::Completed {
                        output: Some(json!({
                            "accepted": true,
                            "state": data.state.as_str(),
                        })),
                        snapshot: data.snapshot_for_ctx(&ctx)?,
                    })
                }
                "retry" => {
                    let _input = input_from_fields::<RetryJob>(input)?;
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
                    let id = resource_id(&ctx)?;
                    let handle = JobHandle::for_job(id, data.state.as_str());
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

    fn on_start<'a>(&'a mut self, ctx: ActorCtx) -> DynFuture<'a, Result<(), ActorError>> {
        let shared = self.shared.clone();
        self.runner = Some(tokio::spawn(async move {
            run_job(shared, ctx).await;
        }));
        Box::pin(async { Ok(()) })
    }

    fn on_stop<'a>(&'a mut self, _ctx: ActorCtx) -> DynFuture<'a, Result<(), ActorError>> {
        if let Some(runner) = self.runner.take() {
            runner.abort();
        }
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct JobData {
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
        ResourceSnapshot {
            id: String::new(),
            kind: "job".into(),
            name: None,
            state: Some(self.state.as_str().into()),
            node: String::new(),
            properties: self.properties(),
            labels: self.labels.clone(),
            transitions: self.transitions(),
            streams: vec![
                SnapshotStreamSpec {
                    name: "lifecycle".into(),
                    kind: "object".into(),
                },
                SnapshotStreamSpec {
                    name: "progress".into(),
                    kind: "object".into(),
                },
                SnapshotStreamSpec {
                    name: "logs".into(),
                    kind: "object".into(),
                },
            ],
            revision: None,
            metadata: Map::new(),
        }
    }

    fn snapshot_for_ctx(&self, ctx: &TransitionCtx) -> Result<ResourceSnapshot, TransitionError> {
        let mut snapshot = self.snapshot();
        snapshot.id = resource_id(ctx)?;
        snapshot.node = ctx.node().to_string();
        Ok(snapshot)
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

fn input_from_fields<T>(input: TransitionInput) -> Result<T, TransitionError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(Value::Object(input.fields.into_iter().collect()))
        .map_err(|err| TransitionError::InvalidInput(err.to_string()))
}

fn example_labels() -> BTreeMap<String, String> {
    BTreeMap::from([("example".into(), "jobs".into())])
}

fn option_json(value: Option<&str>) -> Value {
    value.map(Value::from).unwrap_or(Value::Null)
}

fn progress_percent(step: u32, total: u32) -> u8 {
    ((step.saturating_mul(100)) / total.max(1)).min(99) as u8
}

fn resource_id(ctx: &TransitionCtx) -> Result<String, TransitionError> {
    ctx.resource_id()
        .map(str::to_string)
        .ok_or_else(|| TransitionError::Internal("TransitionCtx has no actor identity".into()))
}

async fn publish_lifecycle(
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
        .map_err(|err| TransitionError::Internal(err.to_string()))?,
    )
    .await
}

async fn publish_lifecycle_from_actor(
    ctx: &ActorCtx,
    data: &JobData,
    reason: Option<&str>,
) -> Result<(), ActorError> {
    ctx.publish(
        "lifecycle",
        "job.lifecycle",
        1,
        serde_json::to_value(lifecycle_event(ctx.resource_id(), data, reason))
            .map_err(|err| ActorError::Internal(err.to_string()))?,
    )
    .await
}

async fn publish_progress_from_actor(
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
            attempt: data.attempt,
            coalesce_key: format!("{}:{}", ctx.resource_id(), data.attempt),
            percent: data.progress,
            step: data.step,
            total_steps: data.command.total_steps(),
            message: message.into(),
        })
        .map_err(|err| ActorError::Internal(err.to_string()))?,
    )
    .await
}

async fn publish_log_from_actor(
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
            attempt: data.attempt,
            level: level.into(),
            line: line.into(),
        })
        .map_err(|err| ActorError::Internal(err.to_string()))?,
    )
    .await
}

fn lifecycle_event(job_id: &str, data: &JobData, reason: Option<&str>) -> LifecycleEvent {
    LifecycleEvent {
        job_id: job_id.into(),
        attempt: data.attempt,
        state: data.state.as_str().into(),
        reason: reason.map(str::to_string),
    }
}

fn submit_spec() -> TransitionSpec {
    TransitionSpec {
        name: "submit".into(),
        title: Some("Submit job".into()),
        allowed_states: vec!["open".into()],
        result: TransitionResultKind::AsyncJob,
        idempotency: Idempotency::Supported,
        effect: Effect::Unsafe,
        ..Default::default()
    }
}

fn cancel_spec() -> TransitionSpec {
    TransitionSpec {
        name: "cancel".into(),
        title: Some("Cancel job".into()),
        allowed_states: vec!["queued".into(), "running".into()],
        result: TransitionResultKind::Sync,
        idempotency: Idempotency::Supported,
        effect: Effect::UnsafeIdempotent,
        ..Default::default()
    }
}

fn retry_spec() -> TransitionSpec {
    TransitionSpec {
        name: "retry".into(),
        title: Some("Retry job".into()),
        allowed_states: vec!["failed".into()],
        result: TransitionResultKind::AsyncJob,
        idempotency: Idempotency::None,
        effect: Effect::Unsafe,
        ..Default::default()
    }
}

#[derive(Debug, Default, Deserialize)]
struct RetryJob {
    #[allow(dead_code)]
    #[serde(default)]
    reset_logs: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_starts() {
        let runner = spawn_test_server()
            .await
            .expect("test server should bind and build node");
        assert_eq!(runner.queue_id(), QUEUE_ID);
        assert!(runner.url("/resources").starts_with("http://127.0.0.1:"));
    }

    #[test]
    fn stream_subscribe_opts_pin_slow_consumer_policy() {
        let progress = stream_subscribe_opts("progress");
        assert_eq!(progress.limit, None);
        assert_eq!(progress.outbound_capacity, Some(STREAM_OUTBOUND_CAPACITY));
        assert!(matches!(
            progress.slow_consumer_policy,
            SlowConsumerPolicy::Coalesce { ref key_path }
                if key_path == &FieldPath::parse("data.coalesceKey")
        ));

        let logs = stream_subscribe_opts("logs");
        assert_eq!(logs.outbound_capacity, Some(STREAM_OUTBOUND_CAPACITY));
        assert!(matches!(
            logs.slow_consumer_policy,
            SlowConsumerPolicy::Backpressure
        ));

        let lifecycle = stream_subscribe_opts("lifecycle");
        assert_eq!(lifecycle.outbound_capacity, Some(STREAM_OUTBOUND_CAPACITY));
        assert!(matches!(
            lifecycle.slow_consumer_policy,
            SlowConsumerPolicy::Backpressure
        ));
    }
}
