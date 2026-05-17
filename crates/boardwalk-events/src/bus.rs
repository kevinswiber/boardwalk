use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use super::topic::TopicPattern;
use super::wire::Event;

pub type SubscriptionId = u64;

#[derive(Debug, Default, Clone, Copy)]
pub struct SubscribeOpts {
    pub limit: Option<u64>,
}

pub struct Subscription {
    pub id: SubscriptionId,
    pub topic: TopicPattern,
    pub rx: mpsc::UnboundedReceiver<Event>,
}

struct SubscriptionInner {
    topic: TopicPattern,
    tx: mpsc::UnboundedSender<Event>,
    remaining: Option<u64>,
}

#[derive(Clone)]
pub struct EventBus {
    inner: Arc<Inner>,
}

struct Inner {
    next_id: AtomicU64,
    subs: Mutex<HashMap<SubscriptionId, SubscriptionInner>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(1),
                subs: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Register a new subscription. Returns the subscription with an
    /// `mpsc::Receiver` that yields matching events.
    pub fn subscribe(&self, topic: TopicPattern, opts: SubscribeOpts) -> Subscription {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        let mut subs = self.inner.subs.lock().unwrap();
        subs.insert(
            id,
            SubscriptionInner {
                topic: topic.clone(),
                tx,
                remaining: opts.limit,
            },
        );
        Subscription { id, topic, rx }
    }

    pub fn unsubscribe(&self, id: SubscriptionId) -> bool {
        let mut subs = self.inner.subs.lock().unwrap();
        subs.remove(&id).is_some()
    }

    /// Publish an event. Fans out to all matching subscriptions. Honors
    /// `limit` by auto-unsubscribing once a subscription's quota runs
    /// out. Drops events for subscribers whose channel has closed.
    pub fn publish(&self, event: Event) -> usize {
        let mut to_remove: Vec<SubscriptionId> = Vec::new();
        let mut delivered = 0usize;
        {
            let mut subs = self.inner.subs.lock().unwrap();
            for (id, sub) in subs.iter_mut() {
                if !sub.topic.matches_event(&event.topic, &event.data) {
                    continue;
                }
                if sub.tx.send(event.clone()).is_err() {
                    to_remove.push(*id);
                    continue;
                }
                delivered += 1;
                if let Some(rem) = sub.remaining.as_mut() {
                    *rem = rem.saturating_sub(1);
                    if *rem == 0 {
                        to_remove.push(*id);
                    }
                }
            }
            for id in &to_remove {
                subs.remove(id);
            }
        }
        delivered
    }

    pub fn active_subscriptions(&self) -> usize {
        self.inner.subs.lock().unwrap().len()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn event(topic: &str, data: serde_json::Value) -> Event {
        Event {
            topic: topic.into(),
            timestamp_ms: 0,
            data,
        }
    }

    #[tokio::test]
    async fn publish_to_matching_subscriber() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/*/state").unwrap();
        let mut sub = bus.subscribe(pattern, SubscribeOpts::default());
        assert_eq!(bus.publish(event("hub/led/abc/state", json!("on"))), 1);
        let got = sub.rx.recv().await.unwrap();
        assert_eq!(got.topic, "hub/led/abc/state");
    }

    #[tokio::test]
    async fn no_delivery_on_mismatch() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/*/state").unwrap();
        let _sub = bus.subscribe(pattern, SubscribeOpts::default());
        assert_eq!(bus.publish(event("hub/led/abc/temperature", json!(1))), 0);
    }

    #[tokio::test]
    async fn limit_auto_unsubscribes() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/led/abc/state").unwrap();
        let _sub = bus.subscribe(pattern, SubscribeOpts { limit: Some(2) });
        assert_eq!(bus.publish(event("hub/led/abc/state", json!("on"))), 1);
        assert_eq!(bus.publish(event("hub/led/abc/state", json!("off"))), 1);
        // After the limit is reached, future publishes deliver nothing.
        assert_eq!(bus.publish(event("hub/led/abc/state", json!("on"))), 0);
        assert_eq!(bus.active_subscriptions(), 0);
    }

    #[tokio::test]
    async fn caql_filter_on_event() {
        let bus = EventBus::new();
        let pattern = TopicPattern::parse("hub/sensor/*/temp?where data > 85").unwrap();
        let mut sub = bus.subscribe(pattern, SubscribeOpts::default());
        assert_eq!(
            bus.publish(event("hub/sensor/abc/temp", json!({"data": 50}))),
            0
        );
        assert_eq!(
            bus.publish(event("hub/sensor/abc/temp", json!({"data": 90}))),
            1
        );
        let got = sub.rx.recv().await.unwrap();
        assert_eq!(got.data["data"], 90);
    }
}
