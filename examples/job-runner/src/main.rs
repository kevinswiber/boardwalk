use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr = std::env::var("BOARDWALK_JOB_RUNNER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:4000".into())
        .parse::<SocketAddr>()?;

    tracing::info!(%addr, "job runner example listening");
    boardwalk_job_runner_example::serve(addr).await
}
