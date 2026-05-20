use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value as JsonValue;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::core::{Device, DeviceConfig, DeviceCtx, DeviceError, DeviceId, StreamSink};
use crate::events::{
    ENVELOPE_VERSION, EventBus, EventEnvelope, NodeId, StreamId, StreamRegistry, TraceContext,
};
use crate::runtime::{
    ActorSpec, CommandId, EmissionContext, EnvelopePlan, Node, NodeHandle, RequestCtx,
    ResourceError, ResourceSnapshot, ResourceSpec, SnapshotStreamSpec, StreamKind,
    TransitionAffordance, TransitionCtx, TransitionError, TransitionInput, TransitionOutcome,
    publish_envelope, sanitize_properties,
};

/// Runtime owned by the HTTP layer (and reused by the peer tunnel
/// handler). Holds private server-adapter resources and projects them
/// into the final Resource wire contract.
pub struct Core {
    pub name: String,
    pub bus: EventBus,
    /// The single shared `StreamRegistry`. `bus.stream_registry()` and
    /// every `BusSink` reference the same `Arc` inner — without that
    /// sharing, the replay cache's `evict` hook would prune a
    /// different map than minting populated.
    pub stream_registry: StreamRegistry,
    devices: RwLock<Vec<DeviceHandle>>,
    actor_node: Option<Arc<Node>>,
    /// Fires once per `register_device`. Subscribers see one tick per
    /// new adapter resource. Used by `ServerHandle::observe`.
    pub(crate) device_changes: tokio::sync::broadcast::Sender<()>,
}

/// One registered private adapter resource. The `Device*` name remains
/// internal compatibility vocabulary until this adapter is rebuilt
/// directly around `Actor`.
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

#[derive(Debug)]
pub(crate) enum ResourceReadError {
    InvalidId,
    NotFound,
    Unavailable(String),
    Internal(String),
}

#[derive(Debug)]
pub(crate) enum ResourceTransitionError {
    InvalidId,
    NotFound,
    InvalidInput(String),
    NotAllowed(String),
    Conflict(String),
    Busy,
    BackpressureRequired,
    Timeout,
    Unavailable(String),
    Internal(String),
}

/// Builder used by `boardwalk-server`. Adapter resources are held
/// un-Mutex'd until `build()` so `on_start` can be called with `&self`.
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

    #[allow(dead_code)]
    pub fn add_device<D: Device + 'static>(&mut self, device: D) -> DeviceId {
        let id = Uuid::new_v4();
        self.add_device_with_id(id, device);
        id
    }

    /// Add an adapter resource with a caller-supplied id. Used when persistence is
    /// enabled and a stable id was retrieved from the registry.
    #[allow(dead_code)]
    pub fn add_device_with_id<D: Device + 'static>(&mut self, id: DeviceId, device: D) {
        let mut cfg = DeviceConfig::default();
        device.config(&mut cfg);
        self.add_device_full(id, cfg, Box::new(device));
    }

    /// Add an adapter resource when both the id and the config have already been
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
            actor_node: None,
            device_changes,
        })
    }
}

/// `StreamSink` impl backed by the event bus. Mints an [`EventEnvelope`]
/// per publish via the shared [`StreamRegistry`].
#[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn from_node(node: Arc<Node>) -> Arc<Self> {
        let (device_changes, _) = tokio::sync::broadcast::channel(64);
        Arc::new(Self {
            name: node.id().to_string(),
            bus: node.events().clone(),
            stream_registry: node.stream_registry().clone(),
            devices: RwLock::new(Vec::new()),
            actor_node: Some(node),
            device_changes,
        })
    }

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

    pub async fn list_resources(&self) -> Vec<ResourceSnapshot> {
        if let Some(node) = &self.actor_node {
            return node.resources().await;
        }
        self.list_devices()
            .await
            .iter()
            .map(|device| device.to_resource_snapshot(&self.name))
            .collect()
    }

    pub async fn query_resources(
        &self,
        ql: &str,
    ) -> Result<Vec<ResourceSnapshot>, crate::query::QueryError> {
        let query = crate::caql::parse(ql)?;
        let mut matches = Vec::new();
        for snapshot in self.list_resources().await {
            if crate::query::matches(&query, &snapshot.to_query_value())? {
                matches.push(snapshot);
            }
        }
        Ok(matches)
    }

    pub async fn get_resource(
        &self,
        id: &str,
    ) -> Result<Option<ResourceSnapshot>, ResourceReadError> {
        if let Some(node) = &self.actor_node {
            let handle = NodeHandle::new(node.clone());
            let Some(proxy) = handle.resource(id).await else {
                return Ok(None);
            };
            return proxy
                .snapshot()
                .await
                .map(Some)
                .map_err(resource_read_error);
        }

        let id = uuid::Uuid::parse_str(id).map_err(|_| ResourceReadError::InvalidId)?;
        Ok(self
            .get_device(&id)
            .await
            .map(|device| device.to_resource_snapshot(&self.name)))
    }

    pub async fn actor_specs(&self) -> Vec<ActorSpec> {
        if let Some(node) = &self.actor_node {
            return node.actor_specs().await;
        }
        self.list_devices()
            .await
            .iter()
            .map(DeviceSnapshot::to_actor_spec)
            .collect()
    }

    pub async fn run_resource_transition(
        &self,
        id: &str,
        name: &str,
        input: TransitionInput,
        request: RequestCtx,
    ) -> Result<TransitionOutcome, ResourceTransitionError> {
        if let Some(node) = &self.actor_node {
            let handle = NodeHandle::new(node.clone());
            let Some(proxy) = handle.resource(id).await else {
                return Err(ResourceTransitionError::NotFound);
            };
            let ctx = TransitionCtx::with_node(request, node.clone());
            let outcome = proxy
                .transition_with_ctx(ctx, name, input)
                .await
                .map_err(resource_transition_error)?;
            return match outcome {
                TransitionOutcome::Completed { output, .. } => {
                    let snapshot = proxy
                        .snapshot()
                        .await
                        .map_err(resource_error_to_transition)?;
                    Ok(TransitionOutcome::Completed { output, snapshot })
                }
                TransitionOutcome::Accepted { .. } => Ok(outcome),
            };
        }

        let id = uuid::Uuid::parse_str(id).map_err(|_| ResourceTransitionError::InvalidId)?;
        if self.get_device(&id).await.is_none() {
            return Err(ResourceTransitionError::NotFound);
        }
        self.run_transition(&id, name, input, request)
            .await
            .map(|snapshot| TransitionOutcome::Completed {
                output: None,
                snapshot: snapshot.to_resource_snapshot(&self.name),
            })
            .map_err(device_transition_error)
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

fn resource_read_error(err: ResourceError) -> ResourceReadError {
    match err {
        ResourceError::NotFound(_) => ResourceReadError::NotFound,
        ResourceError::Unavailable(msg) => ResourceReadError::Unavailable(msg),
        ResourceError::Internal(msg) => ResourceReadError::Internal(msg),
    }
}

fn resource_error_to_transition(err: ResourceError) -> ResourceTransitionError {
    match err {
        ResourceError::NotFound(_) => ResourceTransitionError::NotFound,
        ResourceError::Unavailable(msg) => ResourceTransitionError::Unavailable(msg),
        ResourceError::Internal(msg) => ResourceTransitionError::Internal(msg),
    }
}

fn resource_transition_error(err: TransitionError) -> ResourceTransitionError {
    match err {
        TransitionError::InvalidInput(msg) => ResourceTransitionError::InvalidInput(msg),
        TransitionError::NotAllowed(msg) => ResourceTransitionError::NotAllowed(msg),
        TransitionError::Conflict(msg) => ResourceTransitionError::Conflict(msg),
        TransitionError::Busy => ResourceTransitionError::Busy,
        TransitionError::BackpressureRequired => ResourceTransitionError::BackpressureRequired,
        TransitionError::Timeout => ResourceTransitionError::Timeout,
        TransitionError::ResourceNotFound(_) => ResourceTransitionError::NotFound,
        TransitionError::Internal(msg) => ResourceTransitionError::Internal(msg),
    }
}

fn device_transition_error(err: DeviceError) -> ResourceTransitionError {
    match err {
        DeviceError::Invalid(msg) => ResourceTransitionError::InvalidInput(msg),
        DeviceError::NotAllowed(msg) => ResourceTransitionError::NotAllowed(msg),
        DeviceError::Conflict(msg) => ResourceTransitionError::Conflict(msg),
        DeviceError::Internal(msg) => ResourceTransitionError::Internal(msg),
    }
}

/// A frozen view of a private adapter resource, safe to render into
/// Siren responses.
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
    /// Bridges the private adapter snapshot to the canonical
    /// `ResourceSnapshot`.
    /// `node` is the local server's name (the `Resource` lives on
    /// some node). Reserved field names are stripped from
    /// adapter-supplied properties.
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
        let streams: Vec<SnapshotStreamSpec> = self
            .config
            .streams
            .iter()
            .map(|s| SnapshotStreamSpec {
                name: s.name.clone(),
                kind: match s.kind {
                    StreamKind::Object => "object".to_string(),
                    StreamKind::Binary => "binary".to_string(),
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

    pub fn to_actor_spec(&self) -> ActorSpec {
        ActorSpec {
            resource: ResourceSpec {
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

pub(crate) fn now_ms() -> i64 {
    use time::OffsetDateTime;
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}
