use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value as JsonValue;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::core::{
    Device, DeviceConfig, DeviceCtx, DeviceError, DeviceId, StreamSink, TransitionInput,
};
use crate::events::{
    ENVELOPE_VERSION, EventBus, EventEnvelope, NodeId, StreamId, StreamRegistry, TraceContext,
};
use crate::runtime::{CommandId, EmissionContext, EnvelopePlan, RequestCtx, publish_envelope};

/// Runtime owned by the HTTP layer (and reused by the peer tunnel
/// handler). Holds the registered devices and the event bus.
pub struct Core {
    pub name: String,
    pub bus: EventBus,
    /// The single shared `StreamRegistry`. `bus.stream_registry()` and
    /// every `BusSink` reference the same `Arc` inner — without that
    /// sharing, the replay cache's `evict` hook would prune a
    /// different map than minting populated.
    pub stream_registry: StreamRegistry,
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
        self.build_with_replay_capacity(crate::events::DEFAULT_REPLAY_CAPACITY)
    }

    /// Test-only: build a `Core` with a custom per-stream replay
    /// capacity. The shared `StreamRegistry` is constructed the same
    /// way as in `build()`; only the replay cache differs.
    pub fn build_with_replay_capacity(self, replay_capacity: usize) -> Arc<Core> {
        let (device_changes, _) = tokio::sync::broadcast::channel(64);
        let stream_registry = StreamRegistry::new();
        let bus =
            EventBus::with_registry_and_replay_capacity(stream_registry.clone(), replay_capacity);
        let node_id = NodeId::new(self.name.clone());
        let mut handles = Vec::with_capacity(self.pending.len());
        for p in self.pending {
            let resource_kind = p.config.type_.clone().unwrap_or_else(|| "unknown".into());
            let sink: Arc<dyn StreamSink> = Arc::new(BusSink {
                bus: bus.clone(),
                registry: stream_registry.clone(),
                node_id: node_id.clone(),
                resource_kind: resource_kind.clone(),
                resource_id: p.id.to_string(),
            });
            let ctx = DeviceCtx {
                id: p.id,
                type_: resource_kind,
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
            stream_registry,
            devices: RwLock::new(handles),
            device_changes,
        })
    }
}

/// `StreamSink` impl backed by the event bus. Mints an [`EventEnvelope`]
/// per publish via the shared [`StreamRegistry`].
struct BusSink {
    bus: EventBus,
    registry: StreamRegistry,
    node_id: NodeId,
    resource_kind: String,
    resource_id: String,
}

impl StreamSink for BusSink {
    fn publish(&self, stream: &str, data: serde_json::Value) {
        let stream_id = StreamId::for_resource(&self.node_id, &self.resource_id, stream);
        let allocated = self.registry.allocate(&stream_id);
        let env = EventEnvelope {
            envelope_version: ENVELOPE_VERSION,
            event_id: allocated.event_id,
            node_id: self.node_id.clone(),
            resource_id: self.resource_id.clone(),
            resource_kind: self.resource_kind.clone(),
            // Resource versioning is not yet wired in; emitted as 1 for now.
            resource_version: 1,
            stream_id,
            stream: stream.to_string(),
            sequence: allocated.sequence,
            timestamp: time::OffsetDateTime::from_unix_timestamp_nanos(
                (now_ms() as i128) * 1_000_000,
            )
            .unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
            payload_kind: "resource.stream.data".to_string(),
            payload_version: 1,
            payload_schema: None,
            correlation_id: None,
            causation_id: None,
            trace_context: None,
            data,
        };
        let _ = self.bus.try_publish(env);
    }
}

impl Core {
    /// Register a device at runtime (not via the static builder).
    /// Used by `POST /resources` factories and by scouts.
    pub async fn register_device(
        &self,
        id: DeviceId,
        config: DeviceConfig,
        device: Box<dyn Device>,
    ) {
        let resource_kind = config.type_.clone().unwrap_or_else(|| "unknown".into());
        let sink: Arc<dyn StreamSink> = Arc::new(BusSink {
            bus: self.bus.clone(),
            registry: self.stream_registry.clone(),
            node_id: NodeId::new(self.name.clone()),
            resource_kind: resource_kind.clone(),
            resource_id: id.to_string(),
        });
        let ctx = DeviceCtx {
            id,
            type_: resource_kind,
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
    /// state changed (and the device monitors `state`). `request`
    /// carries the request's W3C trace context and `x-request-id` so
    /// the resulting envelope can populate `correlationId`,
    /// `causationId` (from a fresh `CommandId`), and `traceContext`.
    pub async fn run_transition(
        &self,
        id: &DeviceId,
        name: &str,
        input: TransitionInput,
        request: RequestCtx,
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

        let command_id = CommandId::new();
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
            let node_id = NodeId::new(self.name.clone());
            let resource_id = handle.id.to_string();
            let resource_kind = handle.type_().to_string();
            let trace = request.traceparent().map(|tp| TraceContext {
                traceparent: tp.to_string(),
                tracestate: request.tracestate().map(String::from),
            });
            let _ = publish_envelope(
                &self.bus,
                &self.stream_registry,
                EnvelopePlan {
                    node_id: &node_id,
                    resource_id: &resource_id,
                    resource_kind: &resource_kind,
                    stream: "state",
                    payload_kind: "resource.state.changed",
                    payload_version: 1,
                    data: JsonValue::String(new_state),
                },
                EmissionContext {
                    correlation: request.request_id(),
                    causation: Some(command_id.as_str()),
                    trace,
                },
            )
            .await;
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
        let allowed: std::collections::BTreeSet<&str> = self
            .config
            .allowed_in(&self.state)
            .iter()
            .map(String::as_str)
            .collect();
        let transitions: Vec<TransitionAffordance> = self
            .config
            .transitions
            .values()
            .map(|spec| {
                let available = allowed.contains(spec.name.as_str());
                TransitionAffordance {
                    spec: spec.clone(),
                    available,
                    unavailable_reason: None,
                }
            })
            .collect();
        let streams: Vec<StreamSpec> = self
            .config
            .streams
            .iter()
            .map(|s| StreamSpec {
                name: s.name.clone(),
                kind: match s.kind {
                    crate::core::StreamKind::Object => "object".to_string(),
                    crate::core::StreamKind::Binary => "binary".to_string(),
                },
            })
            .collect();
        ResourceSnapshot {
            id: self.id.to_string(),
            kind: self.type_.clone(),
            name: self.name.clone(),
            state: Some(self.state.clone()),
            node: node.to_string(),
            properties,
            labels: BTreeMap::new(),
            transitions,
            streams,
            revision: None,
            metadata: serde_json::Map::new(),
        }
    }

    pub fn to_actor_spec(&self) -> crate::core::ActorSpec {
        crate::core::ActorSpec {
            resource: crate::core::ResourceSpec {
                kind: self.type_.clone(),
                name: self.name.clone(),
                labels: BTreeMap::new(),
                property_schema: None,
                streams: self.config.streams.clone(),
            },
            transitions: self.config.transitions.values().cloned().collect(),
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
    pub labels: BTreeMap<String, String>,
    pub transitions: Vec<TransitionAffordance>,
    pub streams: Vec<StreamSpec>,
    pub revision: Option<String>,
    pub metadata: serde_json::Map<String, JsonValue>,
}

/// One transition affordance on a resource. Carries the full
/// declared `TransitionSpec` so metadata renderers can read schema,
/// effect, idempotency, and required scopes directly from a snapshot.
/// `available` reflects whether the transition can fire in the
/// resource's current state; `unavailable_reason` carries an optional,
/// human-readable hint when `available` is false.
#[derive(Debug, Clone, Default)]
pub struct TransitionAffordance {
    pub spec: crate::core::TransitionSpec,
    pub available: bool,
    pub unavailable_reason: Option<String>,
}

impl TransitionAffordance {
    /// Convenience accessor since the most common use site needs only
    /// the name.
    pub fn name(&self) -> &str {
        &self.spec.name
    }
}

/// One stream a resource publishes. `kind` is the wire kind hint
/// (`"object"` or `"binary"`), serialized lowercase into the query
/// value and metadata renders.
#[derive(Debug, Clone, Default)]
pub struct StreamSpec {
    pub name: String,
    pub kind: String,
}

/// Top-level field names that `ResourceSnapshot` owns directly.
/// User-supplied properties carrying any of these names are stripped
/// by `sanitize_properties` so that user data cannot shadow
/// Boardwalk-owned fields or render aliases. `"type"` is reserved
/// alongside `"kind"`: it is a derived alias for `kind` exposed in
/// query values and Siren renders.
pub const RESERVED_FIELDS: &[&str] = &[
    "id",
    "kind",
    "type",
    "name",
    "state",
    "node",
    "properties",
    "labels",
    "transitions",
    "streams",
    "revision",
    "affordances",
    "metadata",
];

/// Strips reserved top-level field names from a properties map. Adapters
/// that build a `ResourceSnapshot` from device-supplied properties
/// should call this before assigning.
pub fn sanitize_properties(
    mut props: serde_json::Map<String, JsonValue>,
) -> serde_json::Map<String, JsonValue> {
    let offenders: Vec<&str> = RESERVED_FIELDS
        .iter()
        .filter(|k| props.contains_key(**k))
        .copied()
        .collect();
    if !offenders.is_empty() {
        tracing::trace!(
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
    /// truthful (the key is always present). `type` is exposed as a
    /// render/query alias for `kind`; do not store it separately.
    pub fn to_query_value(&self) -> JsonValue {
        use serde_json::Map;
        let mut o = Map::new();
        o.insert("id".into(), JsonValue::String(self.id.clone()));
        o.insert("kind".into(), JsonValue::String(self.kind.clone()));
        o.insert("type".into(), JsonValue::String(self.kind.clone()));
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
        o.insert(
            "properties".into(),
            JsonValue::Object(self.properties.clone()),
        );
        let labels_obj: Map<String, JsonValue> = self
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), JsonValue::String(v.clone())))
            .collect();
        o.insert("labels".into(), JsonValue::Object(labels_obj));
        let transitions: Vec<JsonValue> = self
            .transitions
            .iter()
            .map(transition_affordance_to_query_json)
            .collect();
        o.insert("transitions".into(), JsonValue::Array(transitions));
        let streams: Vec<JsonValue> = self
            .streams
            .iter()
            .map(|s| {
                let mut m = Map::new();
                m.insert("name".into(), JsonValue::String(s.name.clone()));
                m.insert("kind".into(), JsonValue::String(s.kind.clone()));
                JsonValue::Object(m)
            })
            .collect();
        o.insert("streams".into(), JsonValue::Array(streams));
        o.insert(
            "revision".into(),
            self.revision
                .clone()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
        );
        o.insert("metadata".into(), JsonValue::Object(self.metadata.clone()));
        JsonValue::Object(o)
    }
}

/// Serialize a `TransitionAffordance` for the query projection. The
/// shape inlines the `TransitionSpec` fields at the top level so
/// existing CaQL paths like `transitions[*].name` keep resolving, and
/// `available` / `unavailableReason` sit alongside them. Optional spec
/// fields are emitted only when populated; `requiredScopes` and
/// `allowedStates` are always arrays (possibly empty).
fn transition_affordance_to_query_json(t: &TransitionAffordance) -> JsonValue {
    use serde_json::Map;
    let spec = &t.spec;
    let mut m = Map::new();
    m.insert("name".into(), JsonValue::String(spec.name.clone()));
    if let Some(title) = &spec.title {
        m.insert("title".into(), JsonValue::String(title.clone()));
    }
    m.insert(
        "allowedStates".into(),
        JsonValue::Array(
            spec.allowed_states
                .iter()
                .cloned()
                .map(JsonValue::String)
                .collect(),
        ),
    );
    if let Some(s) = &spec.input_schema {
        m.insert("inputSchema".into(), s.clone());
    }
    if let Some(s) = &spec.output_schema {
        m.insert("outputSchema".into(), s.clone());
    }
    m.insert(
        "result".into(),
        JsonValue::String(
            match spec.result {
                crate::core::TransitionResultKind::Sync => "sync",
                crate::core::TransitionResultKind::AsyncJob => "async-job",
            }
            .into(),
        ),
    );
    m.insert(
        "idempotency".into(),
        JsonValue::String(
            match spec.idempotency {
                crate::core::Idempotency::None => "none",
                crate::core::Idempotency::Supported => "supported",
                crate::core::Idempotency::Required => "required",
            }
            .into(),
        ),
    );
    m.insert(
        "effect".into(),
        JsonValue::String(
            match spec.effect {
                crate::core::Effect::Safe => "safe",
                crate::core::Effect::UnsafeIdempotent => "unsafe-idempotent",
                crate::core::Effect::Unsafe => "unsafe",
            }
            .into(),
        ),
    );
    m.insert(
        "requiredScopes".into(),
        JsonValue::Array(
            spec.required_scopes
                .iter()
                .cloned()
                .map(JsonValue::String)
                .collect(),
        ),
    );
    m.insert("available".into(), JsonValue::Bool(t.available));
    m.insert(
        "unavailableReason".into(),
        t.unavailable_reason
            .clone()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
    );
    JsonValue::Object(m)
}

pub(crate) fn now_ms() -> i64 {
    use time::OffsetDateTime;
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}
