//! Job runner exemplar contract tests.

use std::collections::BTreeSet;
use std::sync::Arc;

use boardwalk::core::{TransitionInput, TransitionOutcome};
use boardwalk::job_runner::{FakeCommand, Job, JobQueue, JobState, RetryJob, SubmitJob};
use boardwalk::runtime::{NodeBuilder, NodeHandle, Resource, ResourceCtx, TransitionError};

async fn snapshot_of<R: Resource>(resource: &R) -> boardwalk::http::ResourceSnapshot {
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

fn transition<'a>(
    snapshot: &'a boardwalk::http::ResourceSnapshot,
    name: &str,
) -> &'a boardwalk::http::TransitionAffordance {
    snapshot
        .transitions
        .iter()
        .find(|transition| transition.name() == name)
        .unwrap_or_else(|| panic!("missing transition {name}"))
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

    let submit = transition(&queue_snapshot, "submit");
    assert!(submit.available);
    assert!(submit.unavailable_reason.is_none());
    assert_eq!(
        submit.spec.result,
        boardwalk::core::TransitionResultKind::AsyncJob
    );

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
        assert_eq!(
            snapshot.properties.get("attempt"),
            Some(&serde_json::json!(1))
        );

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
    assert_eq!(
        snapshot.properties.get("owner"),
        Some(&serde_json::json!("kevin"))
    );
    assert_eq!(
        snapshot.properties.get("priority"),
        Some(&serde_json::json!(7))
    );
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
            Job::new("default", FakeCommand::FailAtStep { step: 1 }).with_state(JobState::Failed),
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
    assert_eq!(
        snapshot.properties.get("attempt"),
        Some(&serde_json::json!(2))
    );
    assert_eq!(
        snapshot.properties.get("progress"),
        Some(&serde_json::json!(0))
    );
    assert_eq!(
        snapshot.properties.get("error"),
        Some(&serde_json::Value::Null)
    );
    assert_eq!(
        snapshot.properties.get("log_count"),
        Some(&serde_json::json!(0))
    );
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
        let job = handle
            .query("where kind = \"job\"")
            .await
            .expect("query parses")
            .into_iter()
            .find(|resource| resource.id() == id)
            .expect("job is discoverable");

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
