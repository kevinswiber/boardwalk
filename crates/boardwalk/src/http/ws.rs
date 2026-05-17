use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use serde_json::Value as Json;

use super::core::now_ms;
use super::routes::AppState;
use crate::events::{
    InboundMessage, OutboundMessage, SubscribeOpts, SubscriptionId, SubscriptionRef, TopicPattern,
};

/// Per-connection state for the multiplex WS endpoint.
struct ConnState {
    /// Local subscriptions: app-id → bus subscription id.
    local_subs: HashMap<u64, SubscriptionId>,
    /// Forwarded subscriptions: app-id → (peer name, topic) + abort handle.
    fwd_subs: HashMap<u64, FwdSub>,
    next_app_id: u64,
}

struct FwdSub {
    peer: String,
    topic: String,
    abort: tokio::task::AbortHandle,
}

pub(crate) async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<OutboundMessage>();

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            let Ok(json) = serde_json::to_string(&msg) else {
                continue;
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
        let Ok(msg) = msg else {
            break;
        };
        match msg {
            Message::Text(text) => {
                let parsed: Result<InboundMessage, _> = serde_json::from_str(&text);
                match parsed {
                    Ok(InboundMessage::Subscribe { topic, limit }) => {
                        let first = topic.split('/').next().unwrap_or("").to_string();
                        let is_peer_topic = first != state.core.name
                            && !first.is_empty()
                            && state.peer_senders.is_some();

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

                        tracing::debug!(
                            %topic,
                            subscription_id = app_id,
                            forwarded = is_peer_topic,
                            "events ws subscribe"
                        );

                        let _ = out_tx.send(OutboundMessage::SubscribeAck {
                            timestamp: now_ms(),
                            topic: topic.clone(),
                            subscription_id: app_id,
                        });

                        if is_peer_topic {
                            let senders = state.peer_senders.clone().unwrap();
                            let rx = state.peer_streams.subscribe(&first, &topic, senders).await;
                            let Some(mut rx) = rx else {
                                // Senders had no entry for this peer; ack already sent,
                                // so emit an error and skip.
                                let _ = out_tx.send(OutboundMessage::Error {
                                    code: 400,
                                    timestamp: now_ms(),
                                    topic: Some(topic.clone()),
                                    message: Some(format!("unknown peer `{first}`")),
                                    subscription_id: Some(app_id),
                                });
                                continue;
                            };
                            let out_tx_clone = out_tx.clone();
                            let topic_for_task = topic.clone();
                            let task = tokio::spawn(async move {
                                loop {
                                    match rx.recv().await {
                                        Ok(v) => emit_forwarded(
                                            &out_tx_clone,
                                            app_id,
                                            &topic_for_task,
                                            &v,
                                        ),
                                        Err(tokio::sync::broadcast::error::RecvError::Lagged(
                                            _,
                                        )) => continue,
                                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                            // Peer stream ended unexpectedly. Tell the WS
                                            // client so it can re-subscribe or clean up.
                                            let _ = out_tx_clone.send(OutboundMessage::Error {
                                                code: 502,
                                                timestamp: now_ms(),
                                                topic: Some(topic_for_task.clone()),
                                                message: Some("peer stream closed".to_string()),
                                                subscription_id: Some(app_id),
                                            });
                                            break;
                                        }
                                    }
                                }
                            });
                            conn.fwd_subs.insert(
                                app_id,
                                FwdSub {
                                    peer: first.clone(),
                                    topic: topic.clone(),
                                    abort: task.abort_handle(),
                                },
                            );
                        } else {
                            // Local subscription.
                            let bus_sub =
                                state.core.bus.subscribe(pattern, SubscribeOpts { limit });
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
                        tracing::debug!(subscription_id, "events ws unsubscribe");
                        if let Some(bus_id) = conn.local_subs.remove(&subscription_id) {
                            state.core.bus.unsubscribe(bus_id);
                        }
                        if let Some(fwd) = conn.fwd_subs.remove(&subscription_id) {
                            fwd.abort.abort();
                            state.peer_streams.unsubscribe(&fwd.peer, &fwd.topic).await;
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

    for (_, bus_id) in conn.local_subs.drain() {
        state.core.bus.unsubscribe(bus_id);
    }
    for (_, fwd) in conn.fwd_subs.drain() {
        fwd.abort.abort();
        state.peer_streams.unsubscribe(&fwd.peer, &fwd.topic).await;
    }
}

fn emit_forwarded(
    out_tx: &tokio::sync::mpsc::UnboundedSender<OutboundMessage>,
    app_id: u64,
    fallback_topic: &str,
    v: &Arc<Json>,
) {
    let _ = out_tx.send(OutboundMessage::Event {
        topic: v
            .get("topic")
            .and_then(|t| t.as_str())
            .unwrap_or(fallback_topic)
            .to_string(),
        subscription_id: SubscriptionRef::Single(app_id),
        timestamp: v
            .get("timestamp")
            .and_then(|t| t.as_i64())
            .unwrap_or_else(now_ms),
        data: v.get("data").cloned().unwrap_or(Json::Null),
    });
}
