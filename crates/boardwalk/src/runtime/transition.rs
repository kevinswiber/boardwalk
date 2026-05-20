//! Runtime-owned transition, resource, and actor contract types.
//!
//! These are model contracts shared by actors, the node runtime, and
//! HTTP renderers. The runtime owns them so in-process nodes and the
//! reusable Boardwalk route stack use the same transition model.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Wire-level identity of a transition (kebab-case in Siren responses).
pub type TransitionName = String;

/// Wire-level identity of a state.
pub type StateName = String;

/// Resource kind name (e.g. `"led"`, `"job"`). Currently a string;
/// a future revision may swap this for a richer type without
/// renaming usage.
pub type ResourceKind = String;

/// Inputs to a transition, parsed at the HTTP boundary.
#[derive(Debug, Default, Clone)]
pub struct TransitionInput {
    pub fields: BTreeMap<String, Value>,
}

impl TransitionInput {
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }

    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.fields.get(name).and_then(Value::as_str)
    }
}

/// Stream kind hint, surfaced in metadata for clients.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamKind {
    /// JSON-serializable structured data.
    #[default]
    Object,
    /// Opaque binary frames.
    Binary,
}

#[derive(Debug, Default, Clone)]
pub struct StreamSpec {
    pub name: String,
    pub kind: StreamKind,
}

/// Field descriptor for a transition input (becomes a Siren field).
#[derive(Debug, Clone)]
pub struct FieldSpec {
    pub name: String,
    pub type_: String,
    pub title: Option<String>,
    pub value: Option<Value>,
}

/// How a transition's effect is delivered. `Sync` transitions return
/// the updated `ResourceSnapshot` directly; `AsyncJob` transitions
/// hand back a typed `JobHandle` that the caller follows on a job
/// resource.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum TransitionResultKind {
    #[default]
    Sync,
    AsyncJob,
}

/// Re-invocation contract for a transition.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Idempotency {
    #[default]
    None,
    Supported,
    Required,
}

/// Effect classification for the transition. Combines HTTP's safety and
/// idempotency axes into one ordinal: `Safe` is read-only (implies idempotent),
/// `UnsafeIdempotent` mutates but repeats cleanly (PUT/DELETE-like), and
/// `Unsafe` makes no idempotency claim (POST-like). For Idempotency-Key
/// participation, see [`Idempotency`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    Safe,
    UnsafeIdempotent,
    #[default]
    Unsafe,
}

#[derive(Debug, Default, Clone)]
pub struct TransitionSpec {
    pub name: TransitionName,
    pub title: Option<String>,
    pub allowed_states: Vec<StateName>,
    pub input_schema: Option<Value>,
    pub output_schema: Option<Value>,
    pub result: TransitionResultKind,
    pub idempotency: Idempotency,
    pub effect: Effect,
    pub required_scopes: Vec<String>,
    /// Renderer-only projection for the current Siren `fields` surface.
    /// Will eventually be derived from `input_schema`; the field stays
    /// for now so existing form-based renders keep working.
    pub fields: Vec<FieldSpec>,
}

/// Declarative shape of a resource kind: stable identity, optional
/// property schema, and the streams it publishes.
#[derive(Debug, Default, Clone)]
pub struct ResourceSpec {
    pub kind: ResourceKind,
    pub name: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub property_schema: Option<Value>,
    pub streams: Vec<StreamSpec>,
}

/// Declarative shape of an actor: a resource plus the transitions it
/// accepts.
#[derive(Debug, Default, Clone)]
pub struct ActorSpec {
    pub resource: ResourceSpec,
    pub transitions: Vec<TransitionSpec>,
}
