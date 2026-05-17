//! Peer client (outbound) and peer socket (inbound).

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use http::Request;
use http_body_util::BodyExt;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::TokioExecutor;
use thiserror::Error;
use url::Url;
use uuid::Uuid;
use zetta_http::{Core, PeerInitState, PeerSenders};
use zetta_registry::{PeerDirection, PeerRecord, PeerStatus, Registry};

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
    pub shutdown: Arc<tokio::sync::Notify>,
}

impl PeerClient {
    pub fn new(
        remote_url: Url,
        local_name: String,
        router: Router,
        peer_init: PeerInitState,
        core: Arc<Core>,
    ) -> Self {
        Self {
            remote_url,
            local_name,
            router,
            peer_init,
            _core: core,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Get a shutdown handle that can be signaled to stop the reconnect loop.
    pub fn shutdown_handle(&self) -> Arc<tokio::sync::Notify> {
        self.shutdown.clone()
    }

    /// Run the client with infinite reconnect. Returns when shutdown
    /// is signaled (via the shutdown handle).
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut attempt = 0u32;
            loop {
                let connection_id = Uuid::new_v4();
                let shutdown = self.shutdown.clone();
                tokio::select! {
                    biased;
                    _ = shutdown.notified() => {
                        tracing::info!(peer = %self.remote_url, "peer client shutting down");
                        return;
                    }
                    res = self.attempt_once(connection_id) => {
                        match res {
                            Ok(()) => {
                                tracing::info!(peer = %self.remote_url, "peer link closed");
                                attempt = 0;
                            }
                            Err(e) => {
                                tracing::warn!(peer = %self.remote_url, error = %e, attempt, "peer link failed");
                            }
                        }
                    }
                }
                attempt = attempt.saturating_add(1);
                let backoff = backoff_ms(attempt);
                tokio::select! {
                    biased;
                    _ = self.shutdown.notified() => return,
                    _ = tokio::time::sleep(Duration::from_millis(backoff)) => {}
                }
            }
        })
    }

    async fn attempt_once(&self, connection_id: Uuid) -> Result<(), PeerError> {
        self.peer_init.register(connection_id);
        let ready =
            zetta_tunnel::dial_initiator(self.remote_url.as_str(), &self.local_name, connection_id)
                .await?;
        let service = self.router.clone().into_service::<hyper::body::Incoming>();
        let svc = hyper_util::service::TowerToHyperService::new(service);
        let result = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(ready.upgraded, svc)
            .await
            .map_err(|e| zetta_tunnel::TunnelError::Upgrade(format!("h2 serve: {e}")));
        self.peer_init.consume(&connection_id);
        result?;
        Ok(())
    }
}

/// Acceptor-side state used to track in-flight peer upgrades and to
/// hold the live HTTP/2 `SendRequest` for forwarding queries from the
/// cloud's HTTP router to the hub.
#[derive(Clone, Default)]
pub struct PeerAcceptors {
    inner: Arc<tokio::sync::Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    senders: Arc<tokio::sync::Mutex<HashMap<String, SendRequest<axum::body::Body>>>>,
    confirmations: Arc<std::sync::atomic::AtomicU64>,
    notify: Arc<tokio::sync::Notify>,
    registry: Arc<std::sync::Mutex<Option<Arc<Registry>>>>,
}

impl PeerAcceptors {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a registry. Subsequent `on_upgraded` calls will persist
    /// PeerRecords on confirmation and update status on disconnect.
    pub fn with_registry(&self, registry: Arc<Registry>) {
        *self.registry.lock().unwrap() = Some(registry);
    }

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

    /// Called by the HTTP layer once a peer's WS upgrade has produced
    /// an `Upgraded` stream.
    pub async fn on_upgraded(
        &self,
        peer_name: String,
        connection_id: Uuid,
        upgraded: hyper::upgrade::Upgraded,
    ) {
        let acceptors = self.clone();
        let peer_name_for_task = peer_name.clone();
        let task = tokio::spawn(async move {
            let registry_snapshot = acceptors.registry.lock().unwrap().clone();
            match drive_acceptor(
                peer_name_for_task.clone(),
                connection_id,
                upgraded,
                acceptors.senders.clone(),
            )
            .await
            {
                Ok(()) => {
                    if let Some(reg) = &registry_snapshot {
                        write_peer(
                            reg,
                            &peer_name_for_task,
                            connection_id,
                            PeerStatus::Connected,
                        );
                    }
                    acceptors
                        .confirmations
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    acceptors.notify.notify_waiters();
                }
                Err(e) => {
                    if let Some(reg) = &registry_snapshot {
                        write_peer(reg, &peer_name_for_task, connection_id, PeerStatus::Failed);
                    }
                    tracing::warn!(peer = %peer_name_for_task, error = %e, "peer acceptor failed");
                }
            }
            let mut inner = acceptors.inner.lock().await;
            inner.remove(&peer_name_for_task);
            // The senders entry is cleaned up by the conn-cleanup task
            // inside drive_acceptor; once that fires, transition to
            // disconnected.
            if let Some(reg) = &registry_snapshot {
                // Best-effort: if the sender is still there, leave the
                // status alone; otherwise mark disconnected.
                let senders = acceptors.senders.lock().await;
                if !senders.contains_key(&peer_name_for_task) {
                    write_peer(
                        reg,
                        &peer_name_for_task,
                        connection_id,
                        PeerStatus::Disconnected,
                    );
                }
            }
        });
        let mut inner = self.inner.lock().await;
        inner.insert(peer_name, task);
    }

    pub async fn active(&self) -> Vec<String> {
        self.inner.lock().await.keys().cloned().collect()
    }
}

/// Cloud-side query forwarder. Implements `zetta_http::PeerSenders` so
/// the router can forward requests for `/servers/{peer-name}/...`
/// through the established H2 tunnel.
#[async_trait::async_trait]
impl PeerSenders for PeerAcceptors {
    async fn sender(&self, name: &str) -> Option<SendRequest<axum::body::Body>> {
        let map = self.senders.lock().await;
        map.get(name).cloned()
    }

    async fn names(&self) -> Vec<String> {
        let map = self.senders.lock().await;
        map.keys().cloned().collect()
    }

    async fn has_active_peer(&self, name: &str) -> bool {
        if self.senders.lock().await.contains_key(name) {
            return true;
        }
        self.inner.lock().await.contains_key(name)
    }
}

async fn drive_acceptor(
    peer_name: String,
    connection_id: Uuid,
    upgraded: hyper::upgrade::Upgraded,
    senders: Arc<tokio::sync::Mutex<HashMap<String, SendRequest<axum::body::Body>>>>,
) -> Result<(), PeerError> {
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, axum::body::Body>(upgraded)
        .await?;
    let peer_name_for_cleanup = peer_name.clone();
    let senders_for_cleanup = senders.clone();
    let _conn_task = tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::debug!(%e, "acceptor h2 connection ended");
        }
        let mut s = senders_for_cleanup.lock().await;
        s.remove(&peer_name_for_cleanup);
    });

    // Send confirmation.
    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "http://{}.unreachable.zettajs.io/_initiate_peer/{}",
            urlencoding::encode(&peer_name),
            connection_id
        ))
        .body(axum::body::Body::empty())
        .expect("request");
    let resp = sender.send_request(req).await?;
    if resp.status() != http::StatusCode::OK {
        return Err(PeerError::Tunnel(zetta_tunnel::TunnelError::Response(
            format!("confirm status {}", resp.status()),
        )));
    }
    let _body = resp.collect().await?.to_bytes();
    tracing::info!(peer = %peer_name, %connection_id, "peer confirmed");

    // Register the sender so the cloud's router can forward through it.
    // The clone keeps the connection alive; when the entry is removed
    // (by the conn cleanup task or by a reconnect overwriting) and the
    // last clone is dropped, the connection closes.
    {
        let mut s = senders.lock().await;
        s.insert(peer_name.clone(), sender);
    }
    Ok(())
}

fn write_peer(registry: &Registry, name: &str, connection_id: Uuid, status: PeerStatus) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let url: url::Url = format!("peer://{name}/").parse().unwrap();
    let record = PeerRecord {
        id: connection_id,
        name: name.to_string(),
        url,
        direction: PeerDirection::Acceptor,
        status,
        updated_ms: now_ms,
    };
    if let Err(e) = registry.put_peer(&record) {
        tracing::warn!(error = %e, "failed to persist peer record");
    }
}

fn backoff_ms(attempt: u32) -> u64 {
    let base: u64 = 100;
    let max: u64 = 30_000;
    let computed = base.saturating_mul(1u64 << attempt.min(10));
    let jitter = xorshift() % 1000;
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
