//! Runnable job-runner example built on Boardwalk's resource and actor runtime.
//!
//! The example serves through Boardwalk's reusable HTTP runtime.
//! Jobs are advanced by a spawned task and short `tokio::time::sleep` intervals;
//! production schedulers should use an explicit queue, tick, and shutdown boundary.

mod api;
mod events;
mod job;
mod queue;
mod streams;

use std::net::SocketAddr;

use boardwalk::Boardwalk;
use queue::JobQueue;
use tokio::task::JoinHandle;

pub(crate) const QUEUE_ID: &str = "queue-default";
pub(crate) const QUEUE_NAME: &str = "default";
pub(crate) const NODE_NAME: &str = "runner";
pub(crate) const FIXED_SUBMITTED_AT: &str = "2026-01-01T00:00:00Z";
pub(crate) const FIXED_STARTED_AT: &str = "2026-01-01T00:00:01Z";
pub(crate) const FIXED_FINISHED_AT: &str = "2026-01-01T00:00:02Z";
pub(crate) const STREAM_OUTBOUND_CAPACITY: usize = 16;

pub async fn serve(addr: SocketAddr) -> anyhow::Result<()> {
    boardwalk().listen(addr).await
}

#[doc(hidden)]
pub async fn spawn_test_server() -> anyhow::Result<RunningExample> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = boardwalk().listen_on(listener).await;
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

fn boardwalk() -> Boardwalk {
    Boardwalk::new()
        .name(NODE_NAME)
        .use_actor_with_id(QUEUE_ID, JobQueue::new(QUEUE_NAME))
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
}
