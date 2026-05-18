//! Pins the canonical JSON shape `ResourceSnapshot::to_query_value()`
//! produces. The shape is the contract the query evaluator sees, so
//! any reordering or omission must be a deliberate, observable change.

use std::sync::Arc;

use boardwalk::http::{Affordances, CoreBuilder, ResourceSnapshot};
use boardwalk::query::{self, FieldPath, Literal, Predicate, Projection, Query};
use boardwalk::{Device, DeviceConfig, DeviceError, TransitionInput};
use futures::future::BoxFuture;
use serde_json::{Map, Value as Json};

fn sample() -> ResourceSnapshot {
    let mut properties = Map::new();
    properties.insert("color".into(), Json::String("red".into()));
    let mut metadata = Map::new();
    metadata.insert("introduced_in".into(), Json::String("v0.1".into()));
    ResourceSnapshot {
        id: "device-1".into(),
        kind: "led".into(),
        name: Some("LED".into()),
        state: Some("off".into()),
        node: "hub".into(),
        properties,
        labels: vec!["kitchen".into()],
        affordances: Affordances {
            transitions: boardwalk::http::TransitionAffordances {
                available: vec!["turn-on".into()],
            },
            streams: boardwalk::http::StreamAffordances {
                available: vec!["state".into()],
            },
        },
        metadata,
    }
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
        "name",
        "state",
        "node",
        "properties",
        "labels",
        "affordances",
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
fn to_query_value_affordances_shape() {
    let v = sample().to_query_value();
    let transitions = v["affordances"]["transitions"]["available"]
        .as_array()
        .expect("transitions.available is array");
    assert!(transitions.iter().all(|x| x.is_string()));
    assert_eq!(transitions, &vec![Json::String("turn-on".into())]);

    let streams = v["affordances"]["streams"]["available"]
        .as_array()
        .expect("streams.available is array");
    assert!(streams.iter().all(|x| x.is_string()));
    assert_eq!(streams, &vec![Json::String("state".into())]);
}

#[test]
fn to_query_value_labels_is_array_even_if_empty() {
    let mut snap = sample();
    snap.labels = vec![];
    let v = snap.to_query_value();
    assert_eq!(v["labels"], Json::Array(vec![]));
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
    hostile.insert("labels".into(), Json::Array(vec![Json::String("a".into())]));
    hostile.insert("node".into(), Json::String("evil".into()));
    hostile.insert("metadata".into(), Json::Object(Map::new()));
    hostile.insert("properties".into(), Json::Object(Map::new()));

    let cleaned = boardwalk::http::sanitize_properties(hostile);
    assert_eq!(cleaned.len(), 1);
    assert_eq!(cleaned.get("color"), Some(&Json::String("red".into())));
}

#[test]
fn type_is_not_reserved_at_snapshot_level() {
    let mut props = Map::new();
    props.insert("type".into(), Json::String("shadow-led".into()));
    let cleaned = boardwalk::http::sanitize_properties(props);
    assert_eq!(
        cleaned.get("type"),
        Some(&Json::String("shadow-led".into())),
        "`type` is only a query-time alias for `kind`; it must not be stripped at the snapshot layer"
    );
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

    let expected_transitions: Vec<String> = d.config.allowed_in(&d.state).to_vec();
    let expected_streams: Vec<String> = d.config.streams.iter().map(|s| s.name.clone()).collect();

    let snap = d.to_resource_snapshot("hub");
    assert_eq!(snap.id, d.id.to_string());
    assert_eq!(snap.kind, d.type_);
    assert_eq!(snap.name, d.name);
    assert_eq!(snap.state.as_deref(), Some(d.state.as_str()));
    assert_eq!(snap.node, "hub");
    assert!(snap.labels.is_empty());
    assert_eq!(snap.affordances.transitions.available, expected_transitions);
    assert_eq!(snap.affordances.streams.available, expected_streams);
    assert!(snap.metadata.is_empty());
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

    let q = Query {
        projection: Projection::All,
        predicate: Predicate::contains(
            FieldPath::parse("affordances.transitions.available"),
            Literal::String("turn-on".into()),
        ),
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
