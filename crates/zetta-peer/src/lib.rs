//! Peer client (outbound) and peer socket (inbound).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::TokioExecutor;
use thiserror::Error;
use url::Url;
use uuid::Uuid;
use zetta_http::{Core, PeerInitState};

#[derive(Debug, Error)]
pub enum PeerError {
    #[error("tunnel: {0}")]
    Tunnel(#[from] zetta_tunnel::TunnelError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("hyper: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("hyper legacy: {0}")]
    HyperLegacy(String),
}

/// Outbound peer client. Establishes the tunnel, hosts a local H2
/// server on top of the upgraded stream, and routes inbound H2 requests
/// through the supplied axum Router.
pub struct PeerClient {
    pub remote_url: Url,
    pub local_name: String,
    pub router: Router,
    pub peer_init: PeerInitState,
    pub _core: Arc<Core>,
}

impl PeerClient {
    pub fn new(
        remote_url: Url,
        local_name: String,
        router: Router,
        peer_init: PeerInitState,
        core: Arc<Core>,
    ) -> Self {
        Self { remote_url, local_name, router, peer_init, _core: core }
    }

    /// Run the client with infinite reconnect. Drop the returned
    /// `JoinHandle` to cancel.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut attempt = 0u32;
            loop {
                let connection_id = Uuid::new_v4();
                match self.attempt_once(connection_id).await {
                    Ok(_) => {
                        tracing::info!(peer = %self.remote_url, "peer link closed");
                        attempt = 0;
                    }
                    Err(e) => {
                        tracing::warn!(peer = %self.remote_url, error = %e, attempt, "peer link failed");
                    }
                }
                attempt = attempt.saturating_add(1);
                let backoff = backoff_ms(attempt);
                tokio::time::sleep(Duration::from_millis(backoff)).await;
            }
        })
    }

    async fn attempt_once(&self, connection_id: Uuid) -> Result<(), PeerError> {
        // Register so the acceptor's `/_initiate_peer/{id}` succeeds.
        self.peer_init.register(connection_id);
        let ready = zetta_tunnel::dial_initiator(
            self.remote_url.as_str(),
            &self.local_name,
            connection_id,
        ).await?;
        let service = self.router.clone()
            .into_service::<hyper::body::Incoming>();
        let svc = hyper_util::service::TowerToHyperService::new(service);
        let result = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(ready.upgraded, svc)
            .await
            .map_err(|e| zetta_tunnel::TunnelError::Upgrade(format!("h2 serve: {e}")));
        // Clean up the in-flight state if the acceptor never confirmed.
        self.peer_init.consume(&connection_id);
        result?;
        Ok(())
    }
}

/// Acceptor-side state used to track in-flight peer upgrades.
#[derive(Clone, Default)]
pub struct PeerAcceptors {
    inner: Arc<tokio::sync::Mutex<std::collections::HashMap<String, tokio::task::JoinHandle<()>>>>,
    confirmations: Arc<std::sync::atomic::AtomicU64>,
    notify: Arc<tokio::sync::Notify>,
}

impl PeerAcceptors {
    pub fn confirmation_count(&self) -> u64 {
        self.confirmations.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Wait until at least one peer confirmation has happened, or
    /// timeout. Returns true on success.
    pub async fn wait_for_first(&self, timeout: std::time::Duration) -> bool {
        if self.confirmation_count() > 0 {
            return true;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        match tokio::time::timeout(timeout, notified.as_mut()).await {
            Ok(_) => self.confirmation_count() > 0,
            Err(_) => false,
        }
    }
}

impl PeerAcceptors {
    pub fn new() -> Self { Self::default() }

    /// Called by the HTTP layer once a peer's WS upgrade has produced
    /// an `Upgraded` stream. We become the HTTP/2 client driving the
    /// initiator and send a confirmation `GET /_initiate_peer/{id}`.
    pub async fn on_upgraded(
        &self,
        peer_name: String,
        connection_id: Uuid,
        upgraded: hyper::upgrade::Upgraded,
    ) {
        let acceptors = self.clone();
        let peer_name_for_task = peer_name.clone();
        let task = tokio::spawn(async move {
            match drive_acceptor(peer_name_for_task.clone(), connection_id, upgraded).await {
                Ok(()) => {
                    acceptors
                        .confirmations
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    acceptors.notify.notify_waiters();
                }
                Err(e) => {
                    tracing::warn!(peer = %peer_name_for_task, error = %e, "peer acceptor failed");
                }
            }
            let mut inner = acceptors.inner.lock().await;
            inner.remove(&peer_name_for_task);
        });
        let mut inner = self.inner.lock().await;
        inner.insert(peer_name, task);
    }

    pub async fn active(&self) -> Vec<String> {
        self.inner.lock().await.keys().cloned().collect()
    }
}

async fn drive_acceptor(
    peer_name: String,
    connection_id: Uuid,
    upgraded: hyper::upgrade::Upgraded,
) -> Result<(), PeerError> {
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, Empty<Bytes>>(upgraded)
        .await?;
    let conn_task = tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::debug!(%e, "acceptor h2 connection ended");
        }
    });

    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "http://{}.unreachable.zettajs.io/_initiate_peer/{}",
            urlencoding::encode(&peer_name),
            connection_id
        ))
        .body(Empty::<Bytes>::new())
        .expect("request");
    let resp = sender.send_request(req).await?;
    if resp.status() != http::StatusCode::OK {
        return Err(PeerError::Tunnel(zetta_tunnel::TunnelError::Response(
            format!("confirm status {}", resp.status()),
        )));
    }
    let _body = resp.collect().await?.to_bytes();
    tracing::info!(peer = %peer_name, %connection_id, "peer confirmed");

    // Return promptly so the caller can record the confirmation; the
    // connection task continues in the background until the connection
    // drops.
    drop(sender);
    drop(conn_task);
    Ok(())
}

fn backoff_ms(attempt: u32) -> u64 {
    let base: u64 = 100;
    let max: u64 = 30_000;
    let computed = base.saturating_mul(1u64 << attempt.min(10));
    let jitter = (xorshift() % 1000) as u64;
    computed.min(max).saturating_add(jitter)
}

fn xorshift() -> u64 {
    use std::cell::Cell;
    use std::time::{SystemTime, UNIX_EPOCH};
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0xdead_beef_dead_beef) };
    }
    STATE.with(|s| {
        let mut x = s.get();
        if x == 0 {
            x = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1)
                .max(1);
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x
    })
}
