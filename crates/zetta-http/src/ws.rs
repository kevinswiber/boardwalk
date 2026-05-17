use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};

use zetta_events::{InboundMessage, OutboundMessage, SubscribeOpts, SubscriptionId, SubscriptionRef, TopicPattern};

use crate::core::{now_ms, Core};

/// Per-connection state for the multiplex WS endpoint.
struct ConnState {
    /// Map app-level subscriptionId (per-connection) to the bus subscription.
    subs: HashMap<u64, SubscriptionId>,
    next_app_id: u64,
}

pub(crate) async fn handle_socket(socket: WebSocket, core: Arc<Core>) {
    let (mut sender, mut receiver) = socket.split();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<OutboundMessage>();

    // Outbound task — serializes OutboundMessages and writes to the WS.
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

    let mut state = ConnState { subs: HashMap::new(), next_app_id: 1 };

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
                        let bus_sub = core
                            .bus
                            .subscribe(pattern.clone(), SubscribeOpts { limit });
                        let app_id = state.next_app_id;
                        state.next_app_id += 1;
                        state.subs.insert(app_id, bus_sub.id);

                        let _ = out_tx.send(OutboundMessage::SubscribeAck {
                            timestamp: now_ms(),
                            topic: topic.clone(),
                            subscription_id: app_id,
                        });

                        // Pump events from the bus into the outbound writer.
                        let out_tx_clone = out_tx.clone();
                        let topic_for_event = topic.clone();
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
                            let _ = topic_for_event;
                        });
                    }
                    Ok(InboundMessage::Unsubscribe { subscription_id }) => {
                        if let Some(bus_id) = state.subs.remove(&subscription_id) {
                            core.bus.unsubscribe(bus_id);
                            let _ = out_tx.send(OutboundMessage::UnsubscribeAck {
                                timestamp: now_ms(),
                                subscription_id,
                            });
                        }
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

    // Drop senders so the writer task exits cleanly.
    drop(out_tx);
    let _ = writer.await;

    // Clean up any remaining subscriptions.
    for (_, bus_id) in state.subs.drain() {
        core.bus.unsubscribe(bus_id);
    }
}
