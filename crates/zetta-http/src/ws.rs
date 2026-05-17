use std::collections::HashMap;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use http::Request;

use zetta_events::{
    InboundMessage, OutboundMessage, SubscribeOpts, SubscriptionId, SubscriptionRef, TopicPattern,
};

use crate::core::now_ms;
use crate::routes::AppState;

/// Per-connection state for the multiplex WS endpoint.
struct ConnState {
    /// Local subscriptions: app-id → bus subscription id.
    local_subs: HashMap<u64, SubscriptionId>,
    /// Forwarded subscriptions: app-id → the abort handle for the
    /// background task that streams events from the peer.
    fwd_subs: HashMap<u64, tokio::task::AbortHandle>,
    next_app_id: u64,
}

pub(crate) async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<OutboundMessage>();

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let json = match serde_json::to_string(&msg) {
                Ok(j) => j,
                Err(_) => continue,
            };
            if sender.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
        let _ = sender.close().await;
    });

    let mut conn = ConnState {
        local_subs: HashMap::new(),
        fwd_subs: HashMap::new(),
        next_app_id: 1,
    };

    while let Some(msg) = receiver.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(_) => break,
        };
        match msg {
            Message::Text(text) => {
                let parsed: Result<InboundMessage, _> = serde_json::from_str(&text);
                match parsed {
                    Ok(InboundMessage::Subscribe { topic, limit }) => {
                        // Decide local vs peer-forward by first segment.
                        let first = topic.split('/').next().unwrap_or("");
                        let peer_sender = if first != state.core.name && !first.is_empty() {
                            match &state.peer_senders {
                                Some(p) => p.sender(first).await,
                                None => None,
                            }
                        } else {
                            None
                        };

                        let pattern = match TopicPattern::parse(&topic) {
                            Ok(p) => p,
                            Err(e) => {
                                let _ = out_tx.send(OutboundMessage::Error {
                                    code: 400,
                                    timestamp: now_ms(),
                                    topic: Some(topic.clone()),
                                    message: Some(format!("{e}")),
                                    subscription_id: None,
                                });
                                continue;
                            }
                        };

                        let app_id = conn.next_app_id;
                        conn.next_app_id += 1;

                        let _ = out_tx.send(OutboundMessage::SubscribeAck {
                            timestamp: now_ms(),
                            topic: topic.clone(),
                            subscription_id: app_id,
                        });

                        if let Some(mut sender) = peer_sender {
                            // Forward: GET /servers/{peer}/events?topic={topic}
                            let peer = first.to_string();
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
                            let out_tx = out_tx.clone();
                            let topic_for_task = topic.clone();
                            let task = tokio::spawn(async move {
                                let resp = match sender.send_request(req).await {
                                    Ok(r) => r,
                                    Err(_) => return,
                                };
                                if !resp.status().is_success() {
                                    return;
                                }
                                let mut body = resp.into_body();
                                let mut buf = Vec::new();
                                use http_body_util::BodyExt;
                                while let Some(chunk) = body.frame().await {
                                    let chunk = match chunk {
                                        Ok(c) => c,
                                        Err(_) => return,
                                    };
                                    let Ok(data) = chunk.into_data() else { continue };
                                    buf.extend_from_slice(&data);
                                    // Parse newline-delimited.
                                    loop {
                                        let Some(nl) = buf.iter().position(|b| *b == b'\n') else { break };
                                        let line = buf.drain(..=nl).collect::<Vec<u8>>();
                                        let trimmed = &line[..line.len() - 1];
                                        let Ok(v) = serde_json::from_slice::<serde_json::Value>(trimmed) else { continue };
                                        let _ = out_tx.send(OutboundMessage::Event {
                                            topic: v.get("topic").and_then(|t| t.as_str()).unwrap_or(&topic_for_task).to_string(),
                                            subscription_id: SubscriptionRef::Single(app_id),
                                            timestamp: v.get("timestamp").and_then(|t| t.as_i64()).unwrap_or_else(now_ms),
                                            data: v.get("data").cloned().unwrap_or(serde_json::Value::Null),
                                        });
                                    }
                                }
                            });
                            conn.fwd_subs.insert(app_id, task.abort_handle());
                            let _ = pattern;
                        } else {
                            // Local subscription.
                            let bus_sub = state
                                .core
                                .bus
                                .subscribe(pattern, SubscribeOpts { limit });
                            conn.local_subs.insert(app_id, bus_sub.id);
                            let out_tx_clone = out_tx.clone();
                            tokio::spawn(async move {
                                let mut rx = bus_sub.rx;
                                while let Some(ev) = rx.recv().await {
                                    let _ = out_tx_clone.send(OutboundMessage::Event {
                                        topic: ev.topic.clone(),
                                        subscription_id: SubscriptionRef::Single(app_id),
                                        timestamp: ev.timestamp_ms,
                                        data: ev.data,
                                    });
                                }
                            });
                        }
                    }
                    Ok(InboundMessage::Unsubscribe { subscription_id }) => {
                        if let Some(bus_id) = conn.local_subs.remove(&subscription_id) {
                            state.core.bus.unsubscribe(bus_id);
                        }
                        if let Some(abort) = conn.fwd_subs.remove(&subscription_id) {
                            abort.abort();
                        }
                        let _ = out_tx.send(OutboundMessage::UnsubscribeAck {
                            timestamp: now_ms(),
                            subscription_id,
                        });
                    }
                    Ok(InboundMessage::Ping { data }) => {
                        let _ = out_tx.send(OutboundMessage::Pong {
                            timestamp: now_ms(),
                            data,
                        });
                    }
                    Err(e) => {
                        let _ = out_tx.send(OutboundMessage::Error {
                            code: 400,
                            timestamp: now_ms(),
                            topic: None,
                            message: Some(format!("invalid json: {e}")),
                            subscription_id: None,
                        });
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    drop(out_tx);
    let _ = writer.await;

    // Clean up.
    for (_, bus_id) in conn.local_subs.drain() {
        state.core.bus.unsubscribe(bus_id);
    }
    for (_, abort) in conn.fwd_subs.drain() {
        abort.abort();
    }
}
