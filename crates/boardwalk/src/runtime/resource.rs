//! The `Resource` trait and supporting types.
//!
//! A `Resource` is the addressable, read-side projection of state on
//! a node. It does not have to be executable; metadata, peer
//! references, and other read-only entities implement only this
//! trait. The executable variant lives in `Actor`.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};

use super::transition::{
    Effect, Idempotency, ResourceKind, ResourceSpec, TransitionResultKind, TransitionSpec,
};

/// Pinned, boxed `Future` alias used by the trait methods so the
/// signatures stay readable while still being object-safe.
pub type DynFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Per-request context passed to `Resource::snapshot`. Carries the
/// node id and any forwarded request metadata. Kept opaque so future
/// task work can attach correlation IDs without touching the trait.
#[derive(Clone, Debug, Default)]
pub struct ResourceCtx {
    // Kept opaque so future request metadata can be added without
    // touching the trait method signature.
    _placeholder: (),
}

impl ResourceCtx {
    /// Test-only constructor used by trait-shape compile tests. Real
    /// callers build a `ResourceCtx` through the `Node` runtime.
    pub fn new_test() -> Self {
        Self::default()
    }
}

/// Read-only failure modes for `Resource::snapshot`. HTTP renderers can
/// map these onto 404 / 503 / 500 responses.
#[derive(Debug)]
pub enum ResourceError {
    NotFound(String),
    Unavailable(String),
    Internal(String),
}

/// Canonical projection used by the renderer, query evaluator, and
/// event/schema layers. Fields are deliberately reserved at the top
/// level: extra resource-specific data lives under `properties` and
/// never collides with these names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub id: String,
    pub kind: String,
    pub name: Option<String>,
    pub state: Option<String>,
    pub node: String,
    pub properties: Map<String, JsonValue>,
    pub labels: BTreeMap<String, String>,
    pub transitions: Vec<TransitionAffordance>,
    pub streams: Vec<SnapshotStreamSpec>,
    pub revision: Option<String>,
    pub metadata: Map<String, JsonValue>,
}

/// One transition affordance on a resource. Carries the full
/// declared `TransitionSpec` so metadata renderers can read schema,
/// effect, idempotency, and required scopes directly from a snapshot.
/// `available` reflects whether the transition can fire in the
/// resource's current state; `unavailable_reason` carries an optional,
/// human-readable hint when `available` is false.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransitionAffordance {
    pub spec: TransitionSpec,
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotStreamSpec {
    pub name: String,
    pub kind: String,
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
                TransitionResultKind::Sync => "sync",
                TransitionResultKind::AsyncJob => "async-job",
            }
            .into(),
        ),
    );
    m.insert(
        "idempotency".into(),
        JsonValue::String(
            match spec.idempotency {
                Idempotency::None => "none",
                Idempotency::Supported => "supported",
                Idempotency::Required => "required",
            }
            .into(),
        ),
    );
    m.insert(
        "effect".into(),
        JsonValue::String(
            match spec.effect {
                Effect::Safe => "safe",
                Effect::UnsafeIdempotent => "unsafe-idempotent",
                Effect::Unsafe => "unsafe",
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

/// Typed handle for an async transition's downstream job resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobHandle {
    pub id: String,
    pub kind: ResourceKind,
    pub location: String,
    pub created: bool,
}

/// Typed return type for invoking a transition. `Sync` transitions
/// produce `Completed`; async ones produce `Accepted` with a typed
/// `JobHandle`.
#[derive(Debug, Clone)]
pub enum TransitionOutcome {
    Completed {
        output: Option<JsonValue>,
        snapshot: ResourceSnapshot,
    },
    Accepted {
        job: JobHandle,
        output: Option<JsonValue>,
    },
}

/// Addressable read-only projection on a node.
pub trait Resource: Send + Sync + 'static {
    /// Declarative description of the resource kind: properties
    /// schema, labels, declared streams.
    fn spec(&self) -> ResourceSpec;

    /// Current snapshot. Reads are async because the resource may
    /// live behind the per-actor command queue.
    fn snapshot<'a>(
        &'a self,
        ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>>;
}
