use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::future::{FusedFuture, FutureExt};
use futures::{SinkExt, StreamExt};
use serde_json::Value as Json;
use tokio::sync::mpsc;

use super::core::now_ms;
use super::routes::AppState;
use crate::events::{
    EventEnvelope, EventId, InboundMessage, NodeId, OutboundMessage, StreamId, SubscribeOpts,
    SubscriptionId, SubscriptionRef, TopicPattern,
};

/// Default capacity of the WS connection's outbound channel.
pub(crate) const WS_OUTBOUND_CAPACITY: usize = 64;

/// Maximum simultaneously-active subscriptions per WS connection.
pub(crate) const WS_MAX_SUBSCRIPTIONS_PER_CONN: usize = 64;

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

/// Back-channel from per-subscription forwarder tasks to the WS
/// dispatcher.
///
/// - `LagTerminated`: a peer-broadcast forwarder hit `Lagged` (or the
///   shared outbound was full when it tried to push) and emitted a
///   terminal `stream-gap`. The dispatcher removes the matching
///   `fwd_subs` entry and decrements the `PeerStreamHub` refcount
///   immediately.
/// - `LocalTerminated`: a local-bus forwarder exited (slow-consumer
///   disconnect, bus-side limit, explicit unsubscribe-by-bus, or the
///   bus side simply closing). The dispatcher removes the matching
///   `local_subs` entry so the WS-connection's subscription count
///   reflects reality.
#[derive(Debug)]
pub(crate) enum ForwarderEvent {
    LagTerminated { app_id: u64 },
    LocalTerminated { app_id: u64 },
}

pub(crate) async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();

    // Normal outbound queue — bounded so a slow reader propagates
    // backpressure into the publisher path.
    let (out_tx, mut out_rx) = mpsc::channel::<OutboundMessage>(WS_OUTBOUND_CAPACITY);
    // Out-of-band terminal queue — always-empty until a terminal frame
    // is enqueued. Capacity 1 so a slow-consumer / gap notice can
    // always reach the wire even when `out_tx` is full.
    let (terminal_tx, mut terminal_rx) = mpsc::channel::<OutboundMessage>(1);
    // Back-channel for forwarder tasks to nudge the dispatcher.
    let (fwd_event_tx, mut fwd_event_rx) = mpsc::unbounded_channel::<ForwarderEvent>();

    // Writer task: biased select prioritizes the terminal channel.
    // PRIORITY: when both queues are ready in the same iteration,
    // terminal wins. This prevents a backlogged out_rx from starving
    // a terminal frame; it does not retroactively preempt a normal
    // frame already selected on a prior iteration.
    //
    // The terminal channel does not auto-close the connection: a
    // peer-broadcast lag terminates exactly one subscription, not the
    // whole socket. Connection-level shutdown still happens via the
    // normal end-of-handler path (sender clones get dropped → both
    // recv()s return None → writer exits).
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                Some(msg) = terminal_rx.recv() => {
                    if send_text(&mut sender, &msg).await.is_err() {
                        break;
                    }
                }
                msg = out_rx.recv() => {
                    match msg {
                        Some(m) => {
                            if send_text(&mut sender, &m).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        let _ = sender.close().await;
    });

    let mut conn = ConnState {
        local_subs: HashMap::new(),
        fwd_subs: HashMap::new(),
        next_app_id: 1,
    };

    loop {
        let msg = tokio::select! {
            msg = receiver.next() => {
                match msg {
                    Some(Ok(m)) => m,
                    Some(Err(_)) | None => break,
                }
            }
            Some(evt) = fwd_event_rx.recv() => {
                match evt {
                    ForwarderEvent::LagTerminated { app_id } => {
                        if let Some(fwd) = conn.fwd_subs.remove(&app_id) {
                            fwd.abort.abort();
                            state.peer_streams.unsubscribe(&fwd.peer, &fwd.topic).await;
                        }
                    }
                    ForwarderEvent::LocalTerminated { app_id } => {
                        if let Some(bus_id) = conn.local_subs.remove(&app_id) {
                            state.core.bus.unsubscribe(bus_id);
                        }
                    }
                }
                continue;
            }
        };
        match msg {
            Message::Text(text) => {
                let parsed: Result<InboundMessage, _> = serde_json::from_str(&text);
                match parsed {
                    Ok(InboundMessage::Subscribe {
                        topic,
                        limit,
                        outbound_capacity,
                    }) => {
                        if conn.local_subs.len() + conn.fwd_subs.len()
                            >= WS_MAX_SUBSCRIPTIONS_PER_CONN
                        {
                            send_or_terminate(
                                &out_tx,
                                &terminal_tx,
                                OutboundMessage::Error {
                                    code: 429,
                                    timestamp: now_ms(),
                                    topic: Some(topic.clone()),
                                    message: Some(format!(
                                        "subscription cap reached ({} max per connection)",
                                        WS_MAX_SUBSCRIPTIONS_PER_CONN
                                    )),
                                    subscription_id: None,
                                },
                            );
                            continue;
                        }
                        let first = topic.split('/').next().unwrap_or("").to_string();
                        let is_peer_topic = first != state.core.name
                            && !first.is_empty()
                            && state.peer_senders.is_some();

                        let pattern = match TopicPattern::parse(&topic) {
                            Ok(p) => p,
                            Err(e) => {
                                send_or_terminate(
                                    &out_tx,
                                    &terminal_tx,
                                    OutboundMessage::Error {
                                        code: 400,
                                        timestamp: now_ms(),
                                        topic: Some(topic.clone()),
                                        message: Some(format!("{e}")),
                                        subscription_id: None,
                                    },
                                );
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

                        send_or_terminate(
                            &out_tx,
                            &terminal_tx,
                            OutboundMessage::SubscribeAck {
                                timestamp: now_ms(),
                                topic: topic.clone(),
                                subscription_id: app_id,
                            },
                        );

                        if is_peer_topic {
                            let senders = state.peer_senders.clone().unwrap();
                            let rx = state.peer_streams.subscribe(&first, &topic, senders).await;
                            let Some(rx) = rx else {
                                send_or_terminate(
                                    &out_tx,
                                    &terminal_tx,
                                    OutboundMessage::Error {
                                        code: 400,
                                        timestamp: now_ms(),
                                        topic: Some(topic.clone()),
                                        message: Some(format!("unknown peer `{first}`")),
                                        subscription_id: Some(app_id),
                                    },
                                );
                                continue;
                            };
                            let out_tx_clone = out_tx.clone();
                            let terminal_tx_clone = terminal_tx.clone();
                            let fwd_event_tx_clone = fwd_event_tx.clone();
                            let topic_for_task = topic.clone();
                            let task = tokio::spawn(forward_peer_subscription(
                                rx,
                                out_tx_clone,
                                terminal_tx_clone,
                                fwd_event_tx_clone,
                                app_id,
                                topic_for_task,
                            ));
                            conn.fwd_subs.insert(
                                app_id,
                                FwdSub {
                                    peer: first.clone(),
                                    topic: topic.clone(),
                                    abort: task.abort_handle(),
                                },
                            );
                        } else {
                            let bus_sub = state.core.bus.subscribe(
                                pattern,
                                SubscribeOpts {
                                    limit,
                                    outbound_capacity,
                                    ..Default::default()
                                },
                            );
                            conn.local_subs.insert(app_id, bus_sub.id);
                            let out_tx_clone = out_tx.clone();
                            let terminal_tx_clone = terminal_tx.clone();
                            let fwd_event_tx_clone = fwd_event_tx.clone();
                            tokio::spawn(local_forwarder(
                                app_id,
                                bus_sub.rx,
                                bus_sub.slow_consumer_rx,
                                out_tx_clone,
                                terminal_tx_clone,
                                fwd_event_tx_clone,
                            ));
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
                        send_or_terminate(
                            &out_tx,
                            &terminal_tx,
                            OutboundMessage::UnsubscribeAck {
                                timestamp: now_ms(),
                                subscription_id,
                            },
                        );
                    }
                    Ok(InboundMessage::Ping { data }) => {
                        send_or_terminate(
                            &out_tx,
                            &terminal_tx,
                            OutboundMessage::Pong {
                                timestamp: now_ms(),
                                data,
                            },
                        );
                    }
                    Err(e) => {
                        send_or_terminate(
                            &out_tx,
                            &terminal_tx,
                            OutboundMessage::Error {
                                code: 400,
                                timestamp: now_ms(),
                                topic: None,
                                message: Some(format!("invalid json: {e}")),
                                subscription_id: None,
                            },
                        );
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    drop(out_tx);
    drop(terminal_tx);
    let _ = writer.await;

    for (_, bus_id) in conn.local_subs.drain() {
        state.core.bus.unsubscribe(bus_id);
    }
    for (_, fwd) in conn.fwd_subs.drain() {
        fwd.abort.abort();
        state.peer_streams.unsubscribe(&fwd.peer, &fwd.topic).await;
    }
}

/// Forwards envelopes from a local bus subscription to the WS
/// outbound channel; on `slow_consumer` disconnect, emits a final
/// `stream-gap` via the terminal channel.
///
/// The slow-consumer oneshot resolves with `Err` if the bus drops the
/// `SubscriptionInner` for a non-overflow reason (e.g. `limit` auto-
/// removal). In that case there is no gap to report — just drain
/// queued envelopes until the bus-side sender closes, then exit.
async fn local_forwarder(
    app_id: u64,
    mut rx: crate::events::SubscriptionRx,
    slow_consumer_rx: tokio::sync::oneshot::Receiver<crate::events::SlowConsumerNotice>,
    out_tx: mpsc::Sender<OutboundMessage>,
    terminal_tx: mpsc::Sender<OutboundMessage>,
    fwd_event_tx: mpsc::UnboundedSender<ForwarderEvent>,
) {
    let mut slow_fut = slow_consumer_rx.fuse();
    'outer: loop {
        tokio::select! {
            biased;
            notice = &mut slow_fut, if !slow_fut.is_terminated() => {
                if let Ok(n) = notice {
                    let _ = terminal_tx.try_send(OutboundMessage::StreamGap {
                        timestamp: now_ms(),
                        subscription_id: app_id,
                        stream_id: n.stream_id,
                        last_delivered_sequence: n.last_delivered_sequence,
                        reason: n.reason.to_string(),
                        terminated: true,
                    });
                    break;
                }
                // Err: oneshot sender dropped for a non-overflow reason
                // (limit-auto-remove, explicit unsubscribe). Fall through
                // and keep draining `rx` until it closes.
            }
            env = rx.recv() => {
                let Some(env) = env else { break };
                let msg = render_event_for_ws(app_id, &env);
                // Block on `send` so a slow WS reader propagates
                // backpressure into the bus, where the next publish
                // surfaces it as a `slow_consumer` notice (Lossless)
                // or a `dropped` count (Lossy). Race the send against
                // `slow_fut` so a notice that fires while we are
                // blocked on `out_tx.send(...)` can still preempt the
                // send and reach the wire via `terminal_tx`.
                tokio::select! {
                    biased;
                    notice = &mut slow_fut, if !slow_fut.is_terminated() => {
                        if let Ok(n) = notice {
                            let _ = terminal_tx.try_send(OutboundMessage::StreamGap {
                                timestamp: now_ms(),
                                subscription_id: app_id,
                                stream_id: n.stream_id,
                                last_delivered_sequence: n.last_delivered_sequence,
                                reason: n.reason.to_string(),
                                terminated: true,
                            });
                            break 'outer;
                        }
                        // Err: bus dropped its sender side. Drop the
                        // in-flight `msg` (we're about to exit via the
                        // closed-`rx` branch on the next iteration).
                    }
                    send_result = out_tx.send(msg) => {
                        if send_result.is_err() {
                            break 'outer;
                        }
                    }
                }
            }
        }
    }
    // Notify the dispatcher to prune `conn.local_subs[app_id]` so the
    // per-connection subscription cap reflects what's actually live.
    let _ = fwd_event_tx.send(ForwarderEvent::LocalTerminated { app_id });
}

fn render_event_for_ws(app_id: u64, env: &EventEnvelope) -> OutboundMessage {
    let iso = env
        .timestamp
        .format(&time::format_description::well_known::Rfc3339)
        .ok();
    OutboundMessage::Event {
        topic: env.topic(),
        subscription_id: SubscriptionRef::Single(app_id),
        timestamp: env.timestamp_ms(),
        data: env.data.clone(),
        event_id: Some(env.event_id.clone()),
        stream_id: Some(env.stream_id.clone()),
        sequence: Some(env.sequence),
        node_id: Some(env.node_id.clone()),
        resource_id: Some(env.resource_id.clone()),
        resource_kind: Some(env.resource_kind.clone()),
        payload_kind: Some(env.payload_kind.clone()),
        payload_version: Some(env.payload_version),
        envelope_version: Some(env.envelope_version),
        iso_timestamp: iso,
    }
}

/// Sends a control / event message on the normal outbound channel; if
/// it's full, falls back to the terminal channel with a `control_full`
/// reason. The contract is that the message reaches the wire — either
/// way.
fn send_or_terminate(
    out_tx: &mpsc::Sender<OutboundMessage>,
    terminal_tx: &mpsc::Sender<OutboundMessage>,
    msg: OutboundMessage,
) {
    match out_tx.try_send(msg) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(returned)) => {
            let _ = terminal_tx.try_send(returned);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

async fn send_text<S>(sender: &mut S, msg: &OutboundMessage) -> Result<(), ()>
where
    S: SinkExt<Message> + Unpin,
{
    let Ok(json) = serde_json::to_string(msg) else {
        return Ok(());
    };
    sender
        .send(Message::Text(json.into()))
        .await
        .map_err(|_| ())
}

/// Drives one peer-forwarded subscription. Reads from a
/// `broadcast::Receiver<Arc<Json>>` and forwards each line as an
/// `OutboundMessage::Event`. On `RecvError::Lagged` emits a final
/// `stream-gap` via the terminal channel, signals
/// `ForwarderEvent::LagTerminated` to the dispatcher (so it eagerly
/// removes the fwd-subs entry and decrements the PeerStreamHub
/// refcount), and exits.
pub(crate) async fn forward_peer_subscription(
    mut rx: tokio::sync::broadcast::Receiver<Arc<Json>>,
    out_tx: mpsc::Sender<OutboundMessage>,
    terminal_tx: mpsc::Sender<OutboundMessage>,
    fwd_event_tx: mpsc::UnboundedSender<ForwarderEvent>,
    app_id: u64,
    fallback_topic: String,
) {
    loop {
        match rx.recv().await {
            Ok(v) => {
                let msg = render_peer_event(app_id, &fallback_topic, &v);
                // `try_send` (not `send().await`) so the next
                // `rx.recv()` is always reachable — a `broadcast::Lagged`
                // that materializes while we would otherwise be blocked
                // on a full `out_tx` is still detectable. On `Full` we
                // give up and route a terminal gap, which is the
                // honest signal that this connection is too slow.
                match out_tx.try_send(msg) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        let _ = terminal_tx.try_send(OutboundMessage::StreamGap {
                            timestamp: now_ms(),
                            subscription_id: app_id,
                            stream_id: None,
                            last_delivered_sequence: None,
                            reason: "ws_outbound_full".to_string(),
                            terminated: true,
                        });
                        let _ = fwd_event_tx.send(ForwarderEvent::LagTerminated { app_id });
                        break;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::debug!(app_id, skipped, "peer forwarder lagged");
                let _ = terminal_tx.try_send(OutboundMessage::StreamGap {
                    timestamp: now_ms(),
                    subscription_id: app_id,
                    stream_id: None,
                    last_delivered_sequence: None,
                    reason: format!("broadcast_lag({skipped})"),
                    terminated: true,
                });
                let _ = fwd_event_tx.send(ForwarderEvent::LagTerminated { app_id });
                break;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                let _ = out_tx.try_send(OutboundMessage::Error {
                    code: 502,
                    timestamp: now_ms(),
                    topic: Some(fallback_topic.clone()),
                    message: Some("peer stream closed".to_string()),
                    subscription_id: Some(app_id),
                });
                let _ = fwd_event_tx.send(ForwarderEvent::LagTerminated { app_id });
                break;
            }
        }
    }
}

fn render_peer_event(app_id: u64, fallback_topic: &str, v: &Arc<Json>) -> OutboundMessage {
    OutboundMessage::Event {
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
        event_id: v
            .get("eventId")
            .and_then(|x| x.as_str())
            .map(EventId::from_raw),
        stream_id: v
            .get("streamId")
            .and_then(|x| x.as_str())
            .map(StreamId::from_raw),
        sequence: v.get("sequence").and_then(|x| x.as_u64()),
        node_id: v.get("nodeId").and_then(|x| x.as_str()).map(NodeId::new),
        resource_id: v
            .get("resourceId")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        resource_kind: v
            .get("resourceKind")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        payload_kind: v
            .get("payloadKind")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        payload_version: v
            .get("payloadVersion")
            .and_then(|x| x.as_u64())
            .map(|n| n as u32),
        envelope_version: v
            .get("envelopeVersion")
            .and_then(|x| x.as_u64())
            .map(|n| n as u8),
        iso_timestamp: v
            .get("isoTimestamp")
            .and_then(|x| x.as_str())
            .map(str::to_string),
    }
}
