//! Job runner stream contract tests.

use std::sync::Arc;
use std::time::Duration;

use boardwalk::core::{TransitionInput, TransitionOutcome};
use boardwalk::events::{SlowConsumerPolicy, TopicPattern};
use boardwalk::job_runner::{FakeCommand, Job, JobQueue, RetryJob, SubmitJob};
use boardwalk::runtime::{NodeBuilder, NodeHandle, ResourceProxy};

fn empty_input() -> TransitionInput {
    TransitionInput::default()
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

async fn recv(sub: &mut boardwalk::events::Subscription) -> boardwalk::events::EventEnvelope {
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
async fn job_progress_and_logs_are_streamed_with_envelopes() {
    let node = Arc::new(NodeBuilder::new("runner").build());
    let id = node
        .register_actor(Job::new(
            "default",
            FakeCommand::SuccessAfterTicks { ticks: 3 },
        ))
        .await
        .expect("job registers");

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

    let handle = NodeHandle::new(node);
    let job = job_proxy(&handle, &id).await;
    job.transition("advance", empty_input())
        .await
        .expect("start tick succeeds");
    job.transition("advance", empty_input())
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
    let node = Arc::new(NodeBuilder::new("runner").build());
    let id = node
        .register_actor(Job::new(
            "default",
            FakeCommand::SuccessAfterTicks { ticks: 5 },
        ))
        .await
        .expect("job registers");

    let mut sub = node.events().subscribe(
        TopicPattern::parse(&format!("runner/job/{id}/progress")).unwrap(),
        Job::stream_subscribe_opts("progress", 1),
    );
    let handle = NodeHandle::new(node);
    let job = job_proxy(&handle, &id).await;

    job.transition("advance", empty_input()).await.unwrap();
    for _ in 0..3 {
        job.transition("advance", empty_input()).await.unwrap();
    }

    let event = recv(&mut sub).await;
    assert_eq!(event.payload_kind, "job.progress");
    assert_eq!(event.data["jobId"], id);
    assert_eq!(event.data["attempt"], 1);
    assert_eq!(event.data["step"], 3);
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

#[tokio::test]
async fn submit_success_job_emits_lifecycle_progress_logs_and_reaches_succeeded() {
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
        .expect("queue proxy");

    let outcome = queue
        .transition(
            "submit",
            transition_input(SubmitJob::new(FakeCommand::SuccessAfterTicks { ticks: 2 })),
        )
        .await
        .expect("submit succeeds");
    let TransitionOutcome::Accepted { job, .. } = outcome else {
        panic!("submit should return Accepted");
    };

    let mut lifecycle = node.events().subscribe(
        TopicPattern::parse(&format!("runner/job/{}/lifecycle", job.id)).unwrap(),
        Job::stream_subscribe_opts("lifecycle", 8),
    );
    let mut progress = node.events().subscribe(
        TopicPattern::parse(&format!("runner/job/{}/progress", job.id)).unwrap(),
        Job::stream_subscribe_opts("progress", 8),
    );
    let mut logs = node.events().subscribe(
        TopicPattern::parse(&format!("runner/job/{}/logs", job.id)).unwrap(),
        Job::stream_subscribe_opts("logs", 8),
    );

    let job_proxy = job_proxy(&handle, &job.id).await;
    job_proxy
        .transition("advance", empty_input())
        .await
        .unwrap();
    job_proxy
        .transition("advance", empty_input())
        .await
        .unwrap();
    job_proxy
        .transition("advance", empty_input())
        .await
        .unwrap();

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

    let snapshot = job_proxy.snapshot().await.expect("job snapshot");
    assert_eq!(snapshot.state.as_deref(), Some("succeeded"));
    assert_eq!(
        snapshot.properties.get("progress"),
        Some(&serde_json::json!(100))
    );
}

#[tokio::test]
async fn retry_emits_retried_lifecycle_event() {
    let node = Arc::new(NodeBuilder::new("runner").build());
    let id = node
        .register_actor(Job::new("default", FakeCommand::FailAtStep { step: 1 }))
        .await
        .expect("job registers");
    let handle = NodeHandle::new(node.clone());
    let job = job_proxy(&handle, &id).await;
    job.transition("advance", empty_input()).await.unwrap();
    job.transition("advance", empty_input()).await.unwrap();

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
