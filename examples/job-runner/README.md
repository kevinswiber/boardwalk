# job-runner

Runnable Boardwalk HTTP server built on the reusable Resource/Actor
runtime.

```sh
cargo run -p boardwalk-job-runner-example
```

By default the server listens on `127.0.0.1:4000`. Override that with
`BOARDWALK_JOB_RUNNER_ADDR`.

The example constructs:

```rust,ignore
Boardwalk::new()
    .name("runner")
    .use_actor_with_id("queue-default", JobQueue::new("default"))
```

That builder registers the `JobQueue` actor into a `Node` and serves it
through Boardwalk's reusable HTTP, WebSocket, and peer route stack. The
queue accepts `submit` transitions, creates `Job` resources, returns a
typed job handle, and publishes lifecycle/progress/log streams.

Useful entry points:

```sh
GET  /resources
GET  /resources/queue-default
POST /resources/queue-default/transitions/submit
GET  /servers/runner/events?topic=runner/job/<id>/progress&replay=true
```

The commands are deterministic fixtures implemented in Rust. The
example does not spawn a shell or execute host commands.
