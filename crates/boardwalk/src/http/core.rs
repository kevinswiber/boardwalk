use std::sync::Arc;

use serde_json::Value as JsonValue;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::core::{
    Device, DeviceConfig, DeviceCtx, DeviceError, DeviceId, StreamSink, TransitionInput,
};
use crate::events::{Event, EventBus};

/// Runtime owned by the HTTP layer (and reused by the peer tunnel
/// handler). Holds the registered devices and the event bus.
pub struct Core {
    pub name: String,
    pub bus: EventBus,
    devices: RwLock<Vec<DeviceHandle>>,
    /// Fires once per `register_device`. Subscribers see one tick per
    /// new device. Used by `ServerHandle::observe`.
    pub(crate) device_changes: tokio::sync::broadcast::Sender<()>,
}

/// One registered device. The runtime owns the device behind a lock so
/// transitions can mutate state safely.
pub struct DeviceHandle {
    pub id: DeviceId,
    pub config: DeviceConfig,
    pub device: tokio::sync::Mutex<Box<dyn Device>>,
}

impl DeviceHandle {
    pub fn type_(&self) -> &str {
        self.config.type_.as_deref().unwrap_or("unknown")
    }
}

/// Builder used by `boardwalk-server`. Devices are held un-Mutex'd until
/// `build()` so `on_start` can be called with `&self`.
pub struct CoreBuilder {
    name: String,
    pending: Vec<PendingDevice>,
}

struct PendingDevice {
    id: DeviceId,
    config: DeviceConfig,
    device: Box<dyn Device>,
}

impl CoreBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pending: Vec::new(),
        }
    }

    pub fn add_device<D: Device + 'static>(&mut self, device: D) -> DeviceId {
        let id = Uuid::new_v4();
        self.add_device_with_id(id, device);
        id
    }

    /// Add a device with a caller-supplied id. Used when persistence is
    /// enabled and a stable id was retrieved from the registry.
    pub fn add_device_with_id<D: Device + 'static>(&mut self, id: DeviceId, device: D) {
        let mut cfg = DeviceConfig::default();
        device.config(&mut cfg);
        self.add_device_full(id, cfg, Box::new(device));
    }

    /// Add a device when both the id and the config have already been
    /// resolved (e.g. via a registry lookup).
    pub fn add_device_full(&mut self, id: DeviceId, config: DeviceConfig, device: Box<dyn Device>) {
        self.pending.push(PendingDevice { id, config, device });
    }

    pub fn build(self) -> Arc<Core> {
        let (device_changes, _) = tokio::sync::broadcast::channel(64);
        let bus = EventBus::new();
        let mut handles = Vec::with_capacity(self.pending.len());
        for p in self.pending {
            let type_ = p.config.type_.clone().unwrap_or_else(|| "unknown".into());
            let sink: Arc<dyn StreamSink> = Arc::new(BusSink {
                bus: bus.clone(),
                server: self.name.clone(),
                type_: type_.clone(),
                id: p.id,
            });
            let ctx = DeviceCtx {
                id: p.id,
                type_,
                publish: sink,
            };
            p.device.on_start(ctx);
            handles.push(DeviceHandle {
                id: p.id,
                config: p.config,
                device: tokio::sync::Mutex::new(p.device),
            });
        }
        Arc::new(Core {
            name: self.name,
            bus,
            devices: RwLock::new(handles),
            device_changes,
        })
    }
}

/// `StreamSink` impl backed by the event bus.
struct BusSink {
    bus: EventBus,
    server: String,
    type_: String,
    id: DeviceId,
}

impl StreamSink for BusSink {
    fn publish(&self, stream: &str, data: serde_json::Value) {
        let topic = format!("{}/{}/{}/{}", self.server, self.type_, self.id, stream);
        self.bus.publish(Event {
            topic,
            timestamp_ms: now_ms(),
            data,
        });
    }
}

impl Core {
    /// Register a device at runtime (not via the static builder).
    /// Used by `POST /servers/{name}/devices` factories and by scouts.
    pub async fn register_device(
        &self,
        id: DeviceId,
        config: DeviceConfig,
        device: Box<dyn Device>,
    ) {
        let type_ = config.type_.clone().unwrap_or_else(|| "unknown".into());
        let sink: Arc<dyn StreamSink> = Arc::new(BusSink {
            bus: self.bus.clone(),
            server: self.name.clone(),
            type_: type_.clone(),
            id,
        });
        let ctx = DeviceCtx {
            id,
            type_,
            publish: sink,
        };
        device.on_start(ctx);
        let mut guard = self.devices.write().await;
        guard.push(DeviceHandle {
            id,
            config,
            device: tokio::sync::Mutex::new(device),
        });
        drop(guard);
        let _ = self.device_changes.send(());
    }

    pub async fn list_devices(&self) -> Vec<DeviceSnapshot> {
        let guard = self.devices.read().await;
        let mut out = Vec::with_capacity(guard.len());
        for h in guard.iter() {
            let dev = h.device.lock().await;
            out.push(DeviceSnapshot {
                id: h.id,
                type_: h.type_().to_string(),
                name: h.config.name.clone(),
                state: dev.state().to_string(),
                properties: dev.properties(),
                config: h.config.clone(),
            });
        }
        out
    }

    pub async fn get_device(&self, id: &DeviceId) -> Option<DeviceSnapshot> {
        let guard = self.devices.read().await;
        for h in guard.iter() {
            if h.id == *id {
                let dev = h.device.lock().await;
                return Some(DeviceSnapshot {
                    id: h.id,
                    type_: h.type_().to_string(),
                    name: h.config.name.clone(),
                    state: dev.state().to_string(),
                    properties: dev.properties(),
                    config: h.config.clone(),
                });
            }
        }
        None
    }

    /// Run a transition. Validates that the transition is allowed in the
    /// current state, dispatches, and publishes a state event if the
    /// state changed (and the device monitors `state`).
    pub async fn run_transition(
        &self,
        id: &DeviceId,
        name: &str,
        input: TransitionInput,
    ) -> Result<DeviceSnapshot, DeviceError> {
        let guard = self.devices.read().await;
        let handle = guard
            .iter()
            .find(|h| h.id == *id)
            .ok_or_else(|| DeviceError::Invalid(format!("unknown device {id}")))?;

        let mut dev = handle.device.lock().await;
        let prior_state = dev.state().to_string();

        if !handle
            .config
            .allowed_in(&prior_state)
            .iter()
            .any(|t| t == name)
        {
            tracing::debug!(
                device = %handle.id,
                transition = %name,
                state = %prior_state,
                "transition not allowed in current state"
            );
            return Err(DeviceError::NotAllowed(format!(
                "transition `{name}` not allowed in state `{prior_state}`"
            )));
        }

        if let Err(e) = dev.transition(name, input).await {
            tracing::warn!(
                device = %handle.id,
                transition = %name,
                error = %e,
                "device transition failed"
            );
            return Err(e);
        }

        let new_state = dev.state().to_string();
        tracing::debug!(
            device = %handle.id,
            transition = %name,
            from = %prior_state,
            to = %new_state,
            "device transition ok"
        );
        let extra = dev.properties();
        let snapshot = DeviceSnapshot {
            id: handle.id,
            type_: handle.type_().to_string(),
            name: handle.config.name.clone(),
            state: new_state.clone(),
            properties: extra,
            config: handle.config.clone(),
        };

        if prior_state != new_state && handle.config.monitored.iter().any(|m| m == "state") {
            let topic = format!("{}/{}/{}/state", self.name, handle.type_(), handle.id);
            self.bus.publish(Event {
                topic,
                timestamp_ms: now_ms(),
                data: JsonValue::String(new_state),
            });
        }

        Ok(snapshot)
    }
}

/// A frozen view of a device, safe to render into Siren responses.
#[derive(Debug, Clone)]
pub struct DeviceSnapshot {
    pub id: DeviceId,
    pub type_: String,
    pub name: Option<String>,
    pub state: String,
    pub properties: serde_json::Map<String, JsonValue>,
    pub config: DeviceConfig,
}

impl DeviceSnapshot {
    /// Bridges a `DeviceSnapshot` to the canonical `ResourceSnapshot`.
    /// `node` is the local server's name (the `Resource` lives on
    /// some node). Reserved field names are stripped from
    /// device-supplied properties; `type` maps to `kind`.
    pub fn to_resource_snapshot(&self, node: &str) -> ResourceSnapshot {
        let properties = sanitize_properties(self.properties.clone());
        let available_transitions: Vec<String> =
            self.config.allowed_in(&self.state).to_vec();
        let available_streams: Vec<String> = self
            .config
            .streams
            .iter()
            .map(|s| s.name.clone())
            .collect();
        ResourceSnapshot {
            id: self.id.to_string(),
            kind: self.type_.clone(),
            name: self.name.clone(),
            state: Some(self.state.clone()),
            node: node.to_string(),
            properties,
            labels: Vec::new(),
            affordances: Affordances {
                transitions: TransitionAffordances {
                    available: available_transitions,
                },
                streams: StreamAffordances {
                    available: available_streams,
                },
            },
            metadata: serde_json::Map::new(),
        }
    }
}

/// Canonical projection used by the renderer, query evaluator, and
/// future event/schema layers. Fields are deliberately reserved at
/// the top level: extra device-specific data lives under
/// `properties` and never collides with these names.
#[derive(Debug, Clone)]
pub struct ResourceSnapshot {
    pub id: String,
    pub kind: String,
    pub name: Option<String>,
    pub state: Option<String>,
    pub node: String,
    pub properties: serde_json::Map<String, JsonValue>,
    pub labels: Vec<String>,
    pub affordances: Affordances,
    pub metadata: serde_json::Map<String, JsonValue>,
}

#[derive(Debug, Clone, Default)]
pub struct Affordances {
    pub transitions: TransitionAffordances,
    pub streams: StreamAffordances,
}

#[derive(Debug, Clone, Default)]
pub struct TransitionAffordances {
    pub available: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct StreamAffordances {
    pub available: Vec<String>,
}

/// Top-level field names that `ResourceSnapshot` owns directly.
/// User-supplied properties carrying any of these names are stripped
/// by `sanitize_properties` to prevent them from masking the
/// canonical fields. Note: `"type"` is intentionally absent — it is
/// only a query-time alias for `kind` and may appear in user
/// properties.
pub const RESERVED_FIELDS: &[&str] = &[
    "id",
    "kind",
    "name",
    "state",
    "node",
    "properties",
    "labels",
    "affordances",
    "metadata",
];

/// Strips reserved top-level field names from a properties map. Emits
/// a `tracing::debug!` line listing the offenders if any were
/// removed. Adapters that build a `ResourceSnapshot` from
/// device-supplied properties should call this before assigning.
pub fn sanitize_properties(
    mut props: serde_json::Map<String, JsonValue>,
) -> serde_json::Map<String, JsonValue> {
    let offenders: Vec<&str> = RESERVED_FIELDS
        .iter()
        .filter(|k| props.contains_key(**k))
        .copied()
        .collect();
    if !offenders.is_empty() {
        tracing::debug!(
            ?offenders,
            "reserved field collision in resource properties; stripped"
        );
        for k in &offenders {
            props.remove(*k);
        }
    }
    props
}

impl ResourceSnapshot {
    /// Produces the JSON shape the query evaluator targets. `None`
    /// fields serialize as `Null` so `Exists(path)` semantics remain
    /// truthful (the key is always present).
    pub fn to_query_value(&self) -> JsonValue {
        use serde_json::Map;
        let mut o = Map::new();
        o.insert("id".into(), JsonValue::String(self.id.clone()));
        o.insert("kind".into(), JsonValue::String(self.kind.clone()));
        o.insert(
            "name".into(),
            self.name
                .clone()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
        );
        o.insert(
            "state".into(),
            self.state
                .clone()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
        );
        o.insert("node".into(), JsonValue::String(self.node.clone()));
        o.insert("properties".into(), JsonValue::Object(self.properties.clone()));
        o.insert(
            "labels".into(),
            JsonValue::Array(
                self.labels
                    .iter()
                    .cloned()
                    .map(JsonValue::String)
                    .collect(),
            ),
        );
        let mut affordances = Map::new();
        let mut transitions = Map::new();
        transitions.insert(
            "available".into(),
            JsonValue::Array(
                self.affordances
                    .transitions
                    .available
                    .iter()
                    .cloned()
                    .map(JsonValue::String)
                    .collect(),
            ),
        );
        affordances.insert("transitions".into(), JsonValue::Object(transitions));
        let mut streams = Map::new();
        streams.insert(
            "available".into(),
            JsonValue::Array(
                self.affordances
                    .streams
                    .available
                    .iter()
                    .cloned()
                    .map(JsonValue::String)
                    .collect(),
            ),
        );
        affordances.insert("streams".into(), JsonValue::Object(streams));
        o.insert("affordances".into(), JsonValue::Object(affordances));
        o.insert("metadata".into(), JsonValue::Object(self.metadata.clone()));
        JsonValue::Object(o)
    }
}

pub(crate) fn now_ms() -> i64 {
    use time::OffsetDateTime;
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}
