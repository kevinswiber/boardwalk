//! Runtime-owned transition, resource, and actor contract types.
//!
//! These are model contracts shared by actors, the node runtime, and
//! HTTP renderers. The runtime owns them so in-process nodes and the
//! reusable Boardwalk route stack use the same transition model.

// missing_docs: this module predates the crate-wide gate; its public
// items still need a documentation sweep (tracked follow-up). New code
// here should be documented anyway.
#![allow(missing_docs)]
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

// Intentional sibling use: actor.rs imports TransitionInput, while decode helpers
// here need the transition error type. The types do not recursively contain each other.
use super::actor::TransitionError;

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

    pub fn deserialize<T: serde::de::DeserializeOwned>(self) -> Result<T, TransitionError> {
        serde_json::from_value(self.into_value())
            .map_err(|e| TransitionError::InvalidInput(e.to_string()))
    }

    pub fn as_deserialized<T: serde::de::DeserializeOwned>(&self) -> Result<T, TransitionError> {
        serde_json::from_value(Value::Object(self.fields.clone().into_iter().collect()))
            .map_err(|e| TransitionError::InvalidInput(e.to_string()))
    }

    pub fn into_value(self) -> Value {
        Value::Object(self.fields.into_iter().collect())
    }
}

/// Stream kind hint, surfaced in metadata for clients.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSpec {
    pub name: String,
    pub type_: String,
    pub title: Option<String>,
    pub value: Option<Value>,
}

/// How a transition's effect is delivered. `Sync` transitions return
/// the updated `ResourceSnapshot` directly; `AsyncJob` transitions
/// hand back a typed `AcceptedJob` that the caller follows on a job
/// resource.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransitionResultKind {
    #[default]
    Sync,
    AsyncJob,
}

/// Re-invocation contract for a transition.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    Safe,
    UnsafeIdempotent,
    #[default]
    Unsafe,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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

impl TransitionSpec {
    pub fn sync(name: impl Into<String>) -> Self {
        TransitionSpec {
            name: name.into(),
            result: TransitionResultKind::Sync,
            ..Default::default()
        }
    }

    pub fn async_job(name: impl Into<String>) -> Self {
        TransitionSpec {
            name: name.into(),
            result: TransitionResultKind::AsyncJob,
            ..Default::default()
        }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn allowed_states<I, S>(mut self, states: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_states = states.into_iter().map(Into::into).collect();
        self
    }

    pub fn effect(mut self, effect: Effect) -> Self {
        self.effect = effect;
        self
    }

    pub fn idempotency(mut self, idempotency: Idempotency) -> Self {
        self.idempotency = idempotency;
        self
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Deserialize;
    use serde_json::json;

    use super::super::actor::TransitionError;
    use super::*;

    #[derive(Deserialize, PartialEq, Debug)]
    struct Demo {
        a: u32,
        b: String,
    }

    fn input(pairs: &[(&str, serde_json::Value)]) -> TransitionInput {
        TransitionInput {
            fields: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[test]
    fn deserialize_valid_object_into_struct() {
        let got = input(&[("a", json!(1)), ("b", json!("x"))]).deserialize::<Demo>();
        assert_eq!(
            got.unwrap(),
            Demo {
                a: 1,
                b: "x".into()
            }
        );
    }

    #[test]
    fn deserialize_bad_input_is_invalid_input() {
        let err = input(&[("a", json!("not-a-number"))])
            .deserialize::<Demo>()
            .unwrap_err();
        assert!(matches!(err, TransitionError::InvalidInput(_)));
    }

    #[test]
    fn as_deserialized_does_not_consume() {
        let inp = input(&[("a", json!(2)), ("b", json!("y"))]);
        let got: Demo = inp.as_deserialized().unwrap();
        assert_eq!(
            got,
            Demo {
                a: 2,
                b: "y".into()
            }
        );
        assert!(inp.fields.contains_key("a"));
    }

    #[test]
    fn into_value_is_json_object() {
        let v = input(&[("a", json!(3))]).into_value();
        assert_eq!(v, json!({ "a": 3 }));
    }

    #[test]
    fn sync_builder_matches_literal() {
        let spec = TransitionSpec::sync("cancel")
            .title("Cancel job")
            .allowed_states(["queued", "running"])
            .effect(Effect::UnsafeIdempotent)
            .idempotency(Idempotency::Supported);
        assert_eq!(spec.name, "cancel");
        assert_eq!(spec.title.as_deref(), Some("Cancel job"));
        assert_eq!(
            spec.allowed_states,
            vec!["queued".to_string(), "running".to_string()]
        );
        assert_eq!(spec.result, TransitionResultKind::Sync);
        assert_eq!(spec.effect, Effect::UnsafeIdempotent);
        assert_eq!(spec.idempotency, Idempotency::Supported);
    }

    #[test]
    fn async_job_builder_sets_result_kind() {
        let spec = TransitionSpec::async_job("submit")
            .title("Submit job")
            .allowed_states(["open"]);
        assert_eq!(spec.result, TransitionResultKind::AsyncJob);
        assert_eq!(spec.name, "submit");
    }
}
