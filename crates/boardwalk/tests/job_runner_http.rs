#[path = "../../../examples/job-runner/src/lib.rs"]
#[allow(dead_code)]
mod job_runner_example;

use std::time::Duration;

use futures::StreamExt;
use serde_json::{Value as Json, json};

#[tokio::test]
async fn job_runner_example_submits_and_streams_success_without_shelling_out() {
    let runner = job_runner_example::spawn_test_server()
        .await
        .expect("job runner example starts");
    let client = reqwest::Client::new();

    let submit = client
        .post(runner.url(&format!(
            "/resources/{}/transitions/submit",
            runner.encoded_queue_id()
        )))
        .json(&json!({
            "command": {
                "type": "success-after-ticks",
                "ticks": 2
            },
            "owner": "ci",
            "priority": 3
        }))
        .send()
        .await
        .expect("submit request succeeds");
    assert_eq!(submit.status(), reqwest::StatusCode::CREATED);
    let submitted: Json = submit.json().await.expect("submit response is JSON");
    let output = &submitted["output"];
    let href = output["href"].as_str().expect("JobHandle.href");
    let progress_href = output["streams"]["progress"]
        .as_str()
        .expect("progress stream href")
        .to_owned();
    let logs_href = output["streams"]["logs"]
        .as_str()
        .expect("logs stream href")
        .to_owned();

    let progress = client
        .get(runner.url(&progress_href))
        .send()
        .await
        .expect("progress stream opens");
    assert_eq!(progress.status(), reqwest::StatusCode::OK);
    let mut progress_lines = progress.bytes_stream();

    let logs = client
        .get(runner.url(&logs_href))
        .send()
        .await
        .expect("logs stream opens");
    assert_eq!(logs.status(), reqwest::StatusCode::OK);
    let mut log_lines = logs.bytes_stream();

    let progress_event = next_ndjson(&mut progress_lines).await;
    assert_eq!(progress_event["stream"], "progress");
    assert_eq!(progress_event["resourceKind"], "job");
    assert_eq!(progress_event["data"]["percent"], 50);

    let log_event = next_ndjson(&mut log_lines).await;
    assert_eq!(log_event["stream"], "logs");
    assert_eq!(log_event["resourceKind"], "job");
    assert_eq!(log_event["data"]["line"], "job started");

    let job = wait_for_succeeded(&client, &runner, href).await;
    assert_eq!(job["state"], "succeeded");
    assert_eq!(job["properties"]["progress"], 100);
    assert_eq!(job["properties"]["result"], json!({ "status": "ok" }));

    assert_eq!(
        job_runner_example::shell_command_execution_count(),
        0,
        "fake commands must not execute through a shell"
    );
    let example_source = include_str!("../../../examples/job-runner/src/lib.rs");
    for forbidden in ["std::process", "tokio::process", "Command::new", "/bin/sh"] {
        assert!(
            !example_source.contains(forbidden),
            "job runner example should not shell out through `{forbidden}`"
        );
    }
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
    serde_json::from_str(line).expect("stream chunk is JSON")
}

async fn wait_for_succeeded(
    client: &reqwest::Client,
    runner: &job_runner_example::RunningExample,
    href: &str,
) -> Json {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let job = client
            .get(runner.url(href))
            .send()
            .await
            .expect("job resource fetch succeeds");
        assert_eq!(job.status(), reqwest::StatusCode::OK);
        let job: Json = job.json().await.expect("job resource is JSON");
        if job["state"] == "succeeded" {
            return job;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job did not succeed before timeout; latest snapshot: {job:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
