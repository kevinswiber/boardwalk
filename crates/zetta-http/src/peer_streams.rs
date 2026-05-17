//! Cloud-side subscription deduplication for peer events.
//!
//! Many WS clients on the cloud may subscribe to the same `hub/...`
//! topic. Without dedup we'd open one HTTP/2 stream to the hub per
//! client; with this hub, we open exactly one stream per
//! (peer_name, topic) and fan-out via `tokio::sync::broadcast`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BytesMut;
use http::Request;
use http_body_util::BodyExt;
use serde_json::Value as Json;
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinHandle;

use crate::routes::PeerSenders;

const BROADCAST_BUFFER: usize = 256;

type EntryMap = HashMap<(String, String), Arc<PeerStreamEntry>>;

#[derive(Clone, Default)]
pub struct PeerStreamHub {
    inner: Arc<Mutex<EntryMap>>,
}

struct PeerStreamEntry {
    sender: broadcast::Sender<Arc<Json>>,
    task: std::sync::Mutex<Option<JoinHandle<()>>>,
    refcount: AtomicUsize,
}

impl PeerStreamHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe to (peer, topic) on the shared stream. Spawns the
    /// per-(peer, topic) driver task if it doesn't already exist.
    pub async fn subscribe(
        &self,
        peer: &str,
        topic: &str,
        senders: Arc<dyn PeerSenders>,
    ) -> Option<broadcast::Receiver<Arc<Json>>> {
        let key = (peer.to_string(), topic.to_string());
        let mut map = self.inner.lock().await;
        if let Some(entry) = map.get(&key) {
            entry.refcount.fetch_add(1, Ordering::SeqCst);
            return Some(entry.sender.subscribe());
        }
        let sender = senders.sender(peer).await?;
        let (tx, rx) = broadcast::channel::<Arc<Json>>(BROADCAST_BUFFER);
        let tx_for_task = tx.clone();
        let peer_owned = peer.to_string();
        let topic_owned = topic.to_string();
        let task = tokio::spawn(async move {
            run_stream(peer_owned, topic_owned, sender, tx_for_task).await;
        });
        let entry = Arc::new(PeerStreamEntry {
            sender: tx,
            task: std::sync::Mutex::new(Some(task)),
            refcount: AtomicUsize::new(1),
        });
        map.insert(key, entry);
        Some(rx)
    }

    /// Decrement the refcount for (peer, topic). When it hits zero,
    /// abort the driver task and forget the stream.
    pub async fn unsubscribe(&self, peer: &str, topic: &str) {
        let key = (peer.to_string(), topic.to_string());
        let mut map = self.inner.lock().await;
        let should_remove = if let Some(entry) = map.get(&key) {
            let before = entry.refcount.fetch_sub(1, Ordering::SeqCst);
            before == 1
        } else {
            false
        };
        if should_remove
            && let Some(entry) = map.remove(&key)
            && let Some(handle) = entry.task.lock().unwrap().take()
        {
            handle.abort();
        }
    }

    /// Count of distinct (peer, topic) entries. Useful for tests.
    pub async fn active_streams(&self) -> usize {
        self.inner.lock().await.len()
    }
}

async fn run_stream(
    peer: String,
    topic: String,
    mut sender: hyper::client::conn::http2::SendRequest<axum::body::Body>,
    tx: broadcast::Sender<Arc<Json>>,
) {
    let target = format!(
        "http://{}.unreachable.zettajs.io/servers/{}/events?topic={}",
        urlencoding::encode(&peer),
        urlencoding::encode(&peer),
        urlencoding::encode(&topic),
    );
    let req = Request::builder()
        .method("GET")
        .uri(target)
        .body(axum::body::Body::empty())
        .expect("request");
    let resp = match sender.send_request(req).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(%peer, %topic, error = %e, "peer stream subscribe failed");
            return;
        }
    };
    if !resp.status().is_success() {
        tracing::warn!(%peer, %topic, status = %resp.status(), "peer stream non-200");
        return;
    }
    let mut body = resp.into_body();
    let mut buf = BytesMut::new();
    while let Some(chunk) = body.frame().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(%peer, %topic, error = %e, "peer stream body error");
                return;
            }
        };
        let Ok(data) = chunk.into_data() else {
            continue;
        };
        buf.extend_from_slice(&data);
        while let Some(nl) = buf.iter().position(|b| *b == b'\n') {
            let line = buf.split_to(nl + 1);
            let trimmed = &line[..line.len() - 1];
            let Ok(v) = serde_json::from_slice::<Json>(trimmed) else {
                continue;
            };
            // If no live receivers, this returns an Err — that's fine.
            let _ = tx.send(Arc::new(v));
        }
    }
}
