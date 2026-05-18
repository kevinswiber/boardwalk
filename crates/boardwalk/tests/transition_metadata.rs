//! Canonical shape tests for the widened `TransitionSpec`,
//! `ResourceSpec`/`ActorSpec` envelopes, and the `TransitionOutcome`
//! enum.
//!
//! These pins drive the spec-aware metadata rendering work that lands
//! in Phase 5 and the typed transition return type that the job runner
//! depends on. Field names and serialized keys are part of the
//! contract — changing them must update these snapshots.

use std::collections::BTreeMap;

use boardwalk::core::{
    ActorSpec, FieldSpec, Idempotency, JobHandle, ResourceSpec, Safety, StreamKind, StreamSpec,
    TransitionOutcome, TransitionResultKind, TransitionSpec,
};
use serde_json::json;

#[test]
fn transition_spec_serializes_schema_safety_idempotency_and_scopes() {
    let spec = TransitionSpec {
        name: "submit".into(),
        title: Some("Submit job".into()),
        allowed_states: vec!["open".into()],
        input_schema: Some(json!({"type": "object"})),
        output_schema: Some(json!({"$ref": "#/defs/JobHandle"})),
        result: TransitionResultKind::AsyncJob,
        idempotency: Idempotency::Supported,
        safety: Safety::Unsafe,
        required_scopes: vec!["transition.invoke".into()],
        fields: vec![],
    };
    assert_eq!(spec.name, "submit");
    assert_eq!(spec.title.as_deref(), Some("Submit job"));
    assert_eq!(spec.allowed_states, vec!["open".to_string()]);
    assert_eq!(spec.input_schema, Some(json!({"type": "object"})));
    assert_eq!(
        spec.output_schema,
        Some(json!({"$ref": "#/defs/JobHandle"}))
    );
    assert_eq!(spec.result, TransitionResultKind::AsyncJob);
    assert_eq!(spec.idempotency, Idempotency::Supported);
    assert_eq!(spec.safety, Safety::Unsafe);
    assert_eq!(spec.required_scopes, vec!["transition.invoke".to_string()]);
}

#[test]
fn resource_spec_carries_kind_labels_property_schema_and_streams() {
    let mut labels = BTreeMap::new();
    labels.insert("owner".into(), "platform".into());
    let spec = ResourceSpec {
        kind: "job".into(),
        name: Some("default".into()),
        labels: labels.clone(),
        property_schema: Some(json!({"type": "object"})),
        streams: vec![StreamSpec {
            name: "logs".into(),
            kind: StreamKind::Object,
        }],
    };
    assert_eq!(spec.kind, "job");
    assert_eq!(spec.name.as_deref(), Some("default"));
    assert_eq!(spec.labels, labels);
    assert_eq!(spec.property_schema, Some(json!({"type": "object"})));
    assert_eq!(spec.streams.len(), 1);
    assert_eq!(spec.streams[0].name, "logs");
}

#[test]
fn actor_spec_carries_transition_specs() {
    let resource = ResourceSpec {
        kind: "job".into(),
        name: None,
        labels: BTreeMap::new(),
        property_schema: None,
        streams: vec![],
    };
    let cancel = TransitionSpec {
        name: "cancel".into(),
        title: None,
        allowed_states: vec!["running".into()],
        input_schema: None,
        output_schema: None,
        result: TransitionResultKind::Sync,
        idempotency: Idempotency::Required,
        safety: Safety::Idempotent,
        required_scopes: vec![],
        fields: vec![FieldSpec {
            name: "reason".into(),
            type_: "text".into(),
            title: None,
            value: None,
        }],
    };
    let actor = ActorSpec {
        resource,
        transitions: vec![cancel],
    };
    assert_eq!(actor.resource.kind, "job");
    assert_eq!(actor.transitions.len(), 1);
    assert_eq!(actor.transitions[0].name, "cancel");
    assert_eq!(actor.transitions[0].fields.len(), 1);
}

#[test]
fn transition_outcome_accepted_carries_typed_job_handle() {
    let handle = JobHandle {
        id: "job-1".into(),
        kind: "job".into(),
        location: "/resources/job-1".into(),
    };
    let outcome = TransitionOutcome::Accepted {
        job: handle.clone(),
        output: Some(json!({"queue": "default"})),
    };
    match outcome {
        TransitionOutcome::Accepted { job, output } => {
            assert_eq!(job.id, handle.id);
            assert_eq!(job.kind, handle.kind);
            assert_eq!(job.location, handle.location);
            assert_eq!(output, Some(json!({"queue": "default"})));
        }
        TransitionOutcome::Completed { .. } => panic!("expected Accepted variant"),
    }
}
