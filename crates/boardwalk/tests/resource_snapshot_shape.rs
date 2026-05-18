//! Pins the canonical JSON shape `ResourceSnapshot::to_query_value()`
//! produces. The shape is the contract the query evaluator sees, so
//! any reordering or omission must be a deliberate, observable change.

use std::collections::BTreeMap;
use std::sync::Arc;

use boardwalk::http::{CoreBuilder, ResourceSnapshot, StreamSpec, TransitionAffordance};
use boardwalk::query::{self, ComparisonOp, FieldPath, Literal, Predicate, Projection, Query};
use boardwalk::{Device, DeviceConfig, DeviceError, TransitionInput};
use futures::future::BoxFuture;
use serde_json::{Map, Value as Json};

fn sample() -> ResourceSnapshot {
    let mut properties = Map::new();
    properties.insert("color".into(), Json::String("red".into()));
    let mut metadata = Map::new();
    metadata.insert("introduced_in".into(), Json::String("v0.1".into()));
    let mut labels = BTreeMap::new();
    labels.insert("zone".into(), "kitchen".into());
    ResourceSnapshot {
        id: "device-1".into(),
        kind: "led".into(),
        name: Some("LED".into()),
        state: Some("off".into()),
        node: "hub".into(),
        properties,
        labels,
        transitions: vec![TransitionAffordance {
            name: "turn-on".into(),
            available: true,
            unavailable_reason: None,
        }],
        streams: vec![StreamSpec {
            name: "state".into(),
            kind: "object".into(),
        }],
        revision: None,
        metadata,
    }
}

/// Canonical contract test for the widened `ResourceSnapshot`:
/// `labels` is a string-string map, `transitions` and `streams` are
/// structured arrays, `revision` is optional, and `type` is a derived
/// alias for `kind` so existing query/render expectations keep working.
#[test]
fn resource_snapshot_query_value_exposes_widened_contract() {
    use std::collections::BTreeMap;
    let mut labels = BTreeMap::new();
    labels.insert("owner".to_string(), "platform".to_string());
    labels.insert("queue".to_string(), "default".to_string());

    let snap = ResourceSnapshot {
        id: "job-1".into(),
        kind: "job".into(),
        name: Some("default".into()),
        state: Some("running".into()),
        node: "hub".into(),
        properties: Map::new(),
        labels,
        transitions: vec![boardwalk::http::TransitionAffordance {
            name: "cancel".into(),
            available: true,
            unavailable_reason: None,
        }],
        streams: vec![boardwalk::http::StreamSpec {
            name: "logs".into(),
            kind: "object".into(),
        }],
        revision: Some("rev-1".into()),
        metadata: Map::new(),
    };

    let v = snap.to_query_value();
    assert_eq!(v["kind"], "job");
    assert_eq!(v["type"], "job", "type alias must mirror kind");

    let labels_obj = v["labels"]
        .as_object()
        .expect("labels is an object, not an array");
    assert_eq!(
        labels_obj.get("owner"),
        Some(&Json::String("platform".into()))
    );
    assert_eq!(
        labels_obj.get("queue"),
        Some(&Json::String("default".into()))
    );

    let transitions = v["transitions"]
        .as_array()
        .expect("transitions is a structured array");
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0]["name"], "cancel");
    assert_eq!(transitions[0]["available"], true);
    assert_eq!(transitions[0]["unavailableReason"], Json::Null);

    let streams = v["streams"]
        .as_array()
        .expect("streams is a structured array");
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0]["name"], "logs");
    assert_eq!(streams[0]["kind"], "object");

    assert_eq!(v["revision"], "rev-1");
}

/// Canonical contract test for `sanitize_properties`: every reserved
/// name — including `type`, the render-time alias — is stripped from
/// user-supplied properties so user data cannot shadow Boardwalk-owned
/// fields or render aliases.
#[test]
fn sanitize_properties_strips_all_reserved_resource_fields() {
    let mut hostile = Map::new();
    for k in [
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
    ] {
        hostile.insert(k.into(), Json::String("attacker".into()));
    }
    hostile.insert("color".into(), Json::String("red".into()));

    let cleaned = boardwalk::http::sanitize_properties(hostile);
    assert_eq!(
        cleaned.len(),
        1,
        "expected every reserved field to be stripped; survivors: {cleaned:?}"
    );
    assert_eq!(cleaned.get("color"), Some(&Json::String("red".into())));
}

#[test]
fn to_query_value_includes_all_reserved_fields() {
    let v = sample().to_query_value();
    let obj = v.as_object().expect("to_query_value returns an object");
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort();
    let mut expected = [
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
        "metadata",
    ];
    expected.sort();
    assert_eq!(keys, expected);
}

#[test]
fn to_query_value_omits_state_when_none_serialized_as_null() {
    let mut snap = sample();
    snap.state = None;
    let v = snap.to_query_value();
    assert_eq!(v["state"], Json::Null);
    // The key is still present so `Exists("state")` returns true; the
    // alternative (omitting the key) would change Exists semantics.
    assert!(
        v.as_object().unwrap().contains_key("state"),
        "state key must be present even when None"
    );
}

#[test]
fn to_query_value_transitions_and_streams_shape() {
    let v = sample().to_query_value();
    let transitions = v["transitions"]
        .as_array()
        .expect("transitions is a structured array");
    assert_eq!(transitions.len(), 1);
    assert_eq!(transitions[0]["name"], "turn-on");
    assert_eq!(transitions[0]["available"], true);
    assert_eq!(transitions[0]["unavailableReason"], Json::Null);

    let streams = v["streams"]
        .as_array()
        .expect("streams is a structured array");
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0]["name"], "state");
    assert_eq!(streams[0]["kind"], "object");
}

#[test]
fn to_query_value_labels_is_object_even_if_empty() {
    let mut snap = sample();
    snap.labels = BTreeMap::new();
    let v = snap.to_query_value();
    assert_eq!(v["labels"], Json::Object(Map::new()));
}

#[test]
fn to_query_value_properties_round_trip_extra_keys() {
    let v = sample().to_query_value();
    assert_eq!(v["properties"]["color"], "red");
}

#[test]
fn to_query_value_metadata_is_object_even_if_empty() {
    let mut snap = sample();
    snap.metadata = Map::new();
    let v = snap.to_query_value();
    assert_eq!(v["metadata"], Json::Object(Map::new()));
}

#[test]
fn reserved_fields_are_stripped_from_properties() {
    let mut hostile = Map::new();
    hostile.insert("id".into(), Json::String("X".into()));
    hostile.insert("kind".into(), Json::String("hacker".into()));
    hostile.insert("color".into(), Json::String("red".into()));
    hostile.insert("affordances".into(), Json::Object(Map::new()));
    hostile.insert("labels".into(), Json::Object(Map::new()));
    hostile.insert("node".into(), Json::String("evil".into()));
    hostile.insert("metadata".into(), Json::Object(Map::new()));
    hostile.insert("properties".into(), Json::Object(Map::new()));

    let cleaned = boardwalk::http::sanitize_properties(hostile);
    assert_eq!(cleaned.len(), 1);
    assert_eq!(cleaned.get("color"), Some(&Json::String("red".into())));
}

#[test]
fn type_is_stripped_at_snapshot_level_as_render_compat_alias() {
    let mut props = Map::new();
    props.insert("type".into(), Json::String("shadow-led".into()));
    props.insert("color".into(), Json::String("red".into()));
    let cleaned = boardwalk::http::sanitize_properties(props);
    assert_eq!(
        cleaned.get("type"),
        None,
        "`type` is a render/query alias for `kind`; user properties must not shadow it"
    );
    assert_eq!(cleaned.get("color"), Some(&Json::String("red".into())));
}

// ---------- Adapter tests (Task 4.3) ----------

#[derive(Default)]
struct AdapterLed {
    on: bool,
    extra: Map<String, Json>,
}

impl Device for AdapterLed {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .name("LED")
            .state(if self.on { "on" } else { "off" })
            .when("off", &["turn-on"])
            .when("on", &["turn-off"])
            .monitor("state");
    }
    fn state(&self) -> &str {
        if self.on { "on" } else { "off" }
    }
    fn properties(&self) -> Map<String, Json> {
        self.extra.clone()
    }
    fn transition<'a>(
        &'a mut self,
        _name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async { Ok(()) })
    }
}

async fn one_device(led: AdapterLed) -> Arc<boardwalk::http::Core> {
    let mut b = CoreBuilder::new("hub");
    b.add_device(led);
    b.build()
}

#[tokio::test]
async fn adapter_maps_basic_fields() {
    let core = one_device(AdapterLed::default()).await;
    let devices = core.list_devices().await;
    let d = devices.into_iter().next().unwrap();

    let allowed: std::collections::BTreeSet<&str> = d
        .config
        .allowed_in(&d.state)
        .iter()
        .map(String::as_str)
        .collect();
    let expected_stream_names: Vec<String> =
        d.config.streams.iter().map(|s| s.name.clone()).collect();

    let snap = d.to_resource_snapshot("hub");
    assert_eq!(snap.id, d.id.to_string());
    assert_eq!(snap.kind, d.type_);
    assert_eq!(snap.name, d.name);
    assert_eq!(snap.state.as_deref(), Some(d.state.as_str()));
    assert_eq!(snap.node, "hub");
    assert!(snap.labels.is_empty());
    assert!(snap.revision.is_none());
    assert!(snap.metadata.is_empty());

    let snap_transition_names: std::collections::BTreeSet<&str> =
        snap.transitions.iter().map(|t| t.name.as_str()).collect();
    let cfg_transition_names: std::collections::BTreeSet<&str> =
        d.config.transitions.keys().map(String::as_str).collect();
    assert_eq!(
        snap_transition_names, cfg_transition_names,
        "every declared transition must be visible in the snapshot"
    );
    for t in &snap.transitions {
        assert_eq!(t.available, allowed.contains(t.name.as_str()));
        assert!(t.unavailable_reason.is_none());
    }

    let snap_stream_names: Vec<String> = snap.streams.iter().map(|s| s.name.clone()).collect();
    assert_eq!(snap_stream_names, expected_stream_names);
    for s in &snap.streams {
        assert_eq!(s.kind, "object");
    }
}

#[tokio::test]
async fn adapter_preserves_non_reserved_properties() {
    let mut extra = Map::new();
    extra.insert("color".into(), Json::String("red".into()));
    extra.insert("brightness".into(), Json::Number(42.into()));
    let core = one_device(AdapterLed {
        on: false,
        extra: extra.clone(),
    })
    .await;
    let d = core.list_devices().await.into_iter().next().unwrap();
    let snap = d.to_resource_snapshot("hub");
    assert_eq!(snap.properties, extra);
}

#[tokio::test]
async fn adapter_strips_reserved_property_keys() {
    let mut hostile = Map::new();
    hostile.insert("color".into(), Json::String("red".into()));
    hostile.insert("kind".into(), Json::String("shadow".into()));
    hostile.insert("affordances".into(), Json::Object(Map::new()));
    let core = one_device(AdapterLed {
        on: false,
        extra: hostile,
    })
    .await;
    let d = core.list_devices().await.into_iter().next().unwrap();
    let snap = d.to_resource_snapshot("hub");
    assert_eq!(snap.kind, d.type_);
    assert_eq!(snap.properties.len(), 1);
    assert_eq!(
        snap.properties.get("color"),
        Some(&Json::String("red".into()))
    );
}

#[tokio::test]
async fn adapter_to_query_value_used_by_query_eval() {
    let core = one_device(AdapterLed::default()).await;
    let d = core.list_devices().await.into_iter().next().unwrap();
    let snap = d.to_resource_snapshot("hub");

    let q = boardwalk::caql::parse(r#"where kind = "led""#).unwrap();
    assert!(query::matches(&q, &snap.to_query_value()).unwrap());
}

#[tokio::test]
async fn adapter_to_query_value_contains_works_on_transitions() {
    let core = one_device(AdapterLed::default()).await;
    let d = core.list_devices().await.into_iter().next().unwrap();
    let snap = d.to_resource_snapshot("hub");

    // Structured transition arrays use `name` keys; query `contains`
    // sees the array of names through `transitions[*].name`.
    let names: Vec<String> = snap.transitions.iter().map(|t| t.name.clone()).collect();
    assert!(names.iter().any(|n| n == "turn-on"));

    // The query evaluator can still spot the kind alias.
    let q = Query {
        projection: Projection::All,
        predicate: Predicate::Compare {
            left: FieldPath::parse("kind"),
            op: ComparisonOp::Eq,
            right: Literal::String("led".into()),
        },
    };
    assert!(query::matches(&q, &snap.to_query_value()).unwrap());
}

#[tokio::test]
async fn adapter_to_query_value_exists_works_on_optional_state() {
    let core = one_device(AdapterLed::default()).await;
    let d = core.list_devices().await.into_iter().next().unwrap();
    let snap = d.to_resource_snapshot("hub");

    let q = Query {
        projection: Projection::All,
        predicate: Predicate::exists(FieldPath::parse("state")),
    };
    assert!(query::matches(&q, &snap.to_query_value()).unwrap());
}
