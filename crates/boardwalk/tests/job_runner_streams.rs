//! Job runner stream contract tests.

use std::sync::Arc;
use std::time::Duration;

use boardwalk::core::{TransitionInput, TransitionOutcome};
use boardwalk::events::{SlowConsumerPolicy, TopicPattern};
use boardwalk::job_runner::{FakeCommand, Job, JobQueue, JobState, RetryJob, SubmitJob};
use boardwalk::runtime::{NodeBuilder, NodeHandle, ResourceProxy};

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
            Job::new("default", FakeCommand::FailAtStep { step: 1 }).with_state(JobState::Failed),
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
