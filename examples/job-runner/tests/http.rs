use std::time::Duration;

use boardwalk_job_runner_example as job_runner_example;
use futures::StreamExt;
use reqwest::StatusCode;
use serde_json::{Value as Json, json};

#[test]
fn job_runner_example_uses_reusable_boardwalk_runtime() {
    let example_source = include_str!("../src/lib.rs");

    assert!(
        example_source.contains("Boardwalk::new()"),
        "job-runner should construct HTTP through the reusable Boardwalk runtime"
    );
    for forbidden in [
        "Router::new",
        ".route(\"/resources\"",
        "resource_transition_post",
        "resource_stream_get",
        "transition_response",
        "event_line",
        "JobHandle as OutcomeJobHandle",
        "OutcomeJobHandle",
    ] {
        assert!(
            !example_source.contains(forbidden),
            "job-runner should use the reusable route stack instead of local route/render/stream code: `{forbidden}`"
        );
    }
}

#[tokio::test]
async fn job_runner_example_submits_and_streams_success_without_shelling_out() {
    let runner = job_runner_example::spawn_test_server()
        .await
        .expect("job runner example starts");
    let client = reqwest::Client::new();

    let submitted = submit_job(
        &client,
        &runner,
        json!({
            "command": {
                "type": "success-after-ticks",
                "ticks": 2
            },
            "owner": "ci",
            "priority": 3
        }),
    )
    .await;
    let output = &submitted["output"];
    assert!(
        output.get("state").is_none(),
        "JobHandle should not duplicate state; resource snapshots expose state under properties"
    );
    let href = output["href"].as_str().expect("JobHandle.href");
    let progress_href = output["streams"]["progress"]
        .as_str()
        .expect("progress stream href")
        .to_owned();
    let logs_href = output["streams"]["logs"]
        .as_str()
        .expect("logs stream href")
        .to_owned();
    assert!(progress_href.contains("slowConsumerPolicy=coalesce"));
    assert!(progress_href.contains("coalesceKey=data.coalesceKey"));
    assert!(logs_href.contains("slowConsumerPolicy=backpressure"));

    let progress = client
        .get(runner.url(&progress_href))
        .send()
        .await
        .expect("progress stream opens");
    assert_eq!(progress.status(), StatusCode::OK);
    let mut progress_lines = progress.bytes_stream();

    let logs = client
        .get(runner.url(&logs_href))
        .send()
        .await
        .expect("logs stream opens");
    assert_eq!(logs.status(), StatusCode::OK);
    let mut log_lines = logs.bytes_stream();

    let progress_event = next_ndjson(&mut progress_lines).await;
    assert_eq!(progress_event["stream"], "progress");
    assert_eq!(progress_event["resourceKind"], "job");
    assert_eq!(progress_event["data"]["percent"], 50);

    let log_event = next_ndjson(&mut log_lines).await;
    assert_eq!(log_event["stream"], "logs");
    assert_eq!(log_event["resourceKind"], "job");
    assert_eq!(log_event["data"]["line"], "job started");

    let job = wait_for_state(&client, &runner, href, "succeeded").await;
    assert_eq!(job["properties"]["state"], "succeeded");
    assert_eq!(job["properties"]["progress"], 100);
    assert_eq!(job["properties"]["result"], json!({ "status": "ok" }));

    let example_source = include_str!("../src/lib.rs");
    for forbidden in ["std::process", "tokio::process", "Command::new", "/bin/sh"] {
        assert!(
            !example_source.contains(forbidden),
            "job runner example should not shell out through `{forbidden}`"
        );
    }
}

#[tokio::test]
async fn job_runner_example_handles_failure_cancel_retry_and_bad_input() {
    let runner = job_runner_example::spawn_test_server()
        .await
        .expect("job runner example starts");
    let client = reqwest::Client::new();

    let bad_input = client
        .post(submit_url(&runner))
        .json(&json!(["not", "an", "object"]))
        .send()
        .await
        .expect("bad submit request completes");
    assert_eq!(bad_input.status(), StatusCode::BAD_REQUEST);

    let failed = submit_job(
        &client,
        &runner,
        json!({
            "command": {
                "type": "fail-at-step",
                "step": 1
            }
        }),
    )
    .await;
    let failed_href = failed["output"]["href"].as_str().expect("JobHandle.href");
    let failed_job = wait_for_state(&client, &runner, failed_href, "failed").await;
    assert_eq!(
        failed_job["properties"]["error"]["code"],
        json!("command_failed")
    );

    let retry = post_transition(&client, &runner, failed_href, "retry", json!({})).await;
    assert_eq!(retry.status(), StatusCode::ACCEPTED);
    let retried: Json = retry.json().await.expect("retry response is JSON");
    assert_eq!(retried["output"]["jobId"], job_id_from_href(failed_href));
    assert_eq!(retried["job"]["location"], failed_href);
    let retried_job = fetch_resource(&client, &runner, failed_href).await;
    assert_eq!(retried_job["properties"]["state"], "queued");
    assert_eq!(retried_job["properties"]["attempt"], 2);

    let cancellable = submit_job(
        &client,
        &runner,
        json!({
            "command": {
                "type": "success-after-ticks",
                "ticks": 10
            }
        }),
    )
    .await;
    let cancellable_href = cancellable["output"]["href"]
        .as_str()
        .expect("JobHandle.href");
    let cancel = post_transition(&client, &runner, cancellable_href, "cancel", json!({})).await;
    assert_eq!(cancel.status(), StatusCode::OK);
    let cancelled: Json = cancel.json().await.expect("cancel response is JSON");
    assert!(
        matches!(
            cancelled["snapshot"]["state"].as_str(),
            Some("cancelled" | "cancelling")
        ),
        "cancel should move the job toward cancellation: {cancelled:?}"
    );
    wait_for_state(&client, &runner, cancellable_href, "cancelled").await;

    let cancel_again =
        post_transition(&client, &runner, cancellable_href, "cancel", json!({})).await;
    assert_eq!(cancel_again.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn job_runner_example_replays_progress_to_late_subscribers() {
    let runner = job_runner_example::spawn_test_server()
        .await
        .expect("job runner example starts");
    let client = reqwest::Client::new();

    let submitted = submit_job(
        &client,
        &runner,
        json!({
            "command": {
                "type": "success-after-ticks",
                "ticks": 4
            }
        }),
    )
    .await;
    let href = submitted["output"]["href"]
        .as_str()
        .expect("JobHandle.href");
    let progress_href = submitted["output"]["streams"]["progress"]
        .as_str()
        .expect("progress stream href");
    wait_for_progress_at_least(&client, &runner, href, 25).await;

    let progress = client
        .get(runner.url(progress_href))
        .send()
        .await
        .expect("late progress stream opens");
    assert_eq!(progress.status(), StatusCode::OK);
    let mut progress_lines = progress.bytes_stream();
    let progress_event = next_ndjson(&mut progress_lines).await;

    assert_eq!(progress_event["stream"], "progress");
    assert_eq!(progress_event["resourceKind"], "job");
    assert!(
        progress_event["data"]["percent"]
            .as_u64()
            .unwrap_or_default()
            >= 25,
        "late subscriber should receive a replayed progress event: {progress_event:?}"
    );
}

async fn submit_job(
    client: &reqwest::Client,
    runner: &job_runner_example::RunningExample,
    body: Json,
) -> Json {
    let submit = client
        .post(submit_url(runner))
        .json(&body)
        .send()
        .await
        .expect("submit request succeeds");
    assert_eq!(submit.status(), StatusCode::CREATED);
    submit.json().await.expect("submit response is JSON")
}

fn submit_url(runner: &job_runner_example::RunningExample) -> String {
    runner.url(&format!(
        "/resources/{}/transitions/submit",
        runner.queue_id()
    ))
}

async fn post_transition(
    client: &reqwest::Client,
    runner: &job_runner_example::RunningExample,
    href: &str,
    transition: &str,
    body: Json,
) -> reqwest::Response {
    client
        .post(runner.url(&format!("{href}/transitions/{transition}")))
        .json(&body)
        .send()
        .await
        .expect("transition request completes")
}

async fn next_ndjson(
    stream: &mut (impl StreamExt<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin),
) -> Json {
    let bytes = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("stream event should arrive")
        .expect("stream should remain open")
        .expect("stream chunk should be OK");
    let line = std::str::from_utf8(&bytes)
        .expect("stream chunk is utf8")
        .trim();
    let line = line.lines().next().expect("stream chunk contains a line");
    serde_json::from_str(line).expect("stream chunk is JSON")
}

async fn fetch_resource(
    client: &reqwest::Client,
    runner: &job_runner_example::RunningExample,
    href: &str,
) -> Json {
    let job = client
        .get(runner.url(href))
        .send()
        .await
        .expect("job resource fetch succeeds");
    assert_eq!(job.status(), StatusCode::OK);
    job.json().await.expect("job resource is JSON")
}

async fn wait_for_state(
    client: &reqwest::Client,
    runner: &job_runner_example::RunningExample,
    href: &str,
    state: &str,
) -> Json {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let job = fetch_resource(client, runner, href).await;
        if job["properties"]["state"] == state {
            return job;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job did not reach state `{state}` before timeout; latest snapshot: {job:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_progress_at_least(
    client: &reqwest::Client,
    runner: &job_runner_example::RunningExample,
    href: &str,
    progress: u64,
) -> Json {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let job = fetch_resource(client, runner, href).await;
        if job["properties"]["progress"].as_u64().unwrap_or_default() >= progress {
            return job;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job did not reach progress `{progress}` before timeout; latest snapshot: {job:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn job_id_from_href(href: &str) -> &str {
    href.strip_prefix("/resources/")
        .expect("job href should be resource-relative")
}
