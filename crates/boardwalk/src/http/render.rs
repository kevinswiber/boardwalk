use std::sync::Arc;

use serde_json::Value;
use url::Url;

use super::core::{Core, ResourceSnapshot};
use crate::core::{ActorSpec, Effect, Idempotency, StreamKind, TransitionResultKind};
use crate::siren::{Action, EmbeddedEntity, Entity, Field, Link, SubEntity, rels};

/// Url root for absolute hrefs. Computed per-request from request scheme + host.
#[derive(Clone)]
pub(crate) struct Hrefs {
    pub http: Url,
    pub ws: Url,
    pub server: String,
}

impl Hrefs {
    pub fn root(&self) -> Url {
        self.http.clone()
    }
    pub fn server_url(&self) -> Url {
        self.http
            .join(&format!("servers/{}", urlencoding::encode(&self.server)))
            .unwrap()
    }
    pub fn resources_url(&self) -> Url {
        self.http.join("resources").unwrap()
    }
    pub fn resource_url(&self, id: &str) -> Url {
        self.http
            .join(&format!("resources/{}", urlencoding::encode(id)))
            .unwrap()
    }
    pub fn resource_transition_url(&self, id: &str, transition: &str) -> Url {
        self.http
            .join(&format!(
                "resources/{}/transitions/{}",
                urlencoding::encode(id),
                urlencoding::encode(transition)
            ))
            .unwrap()
    }
    pub fn meta_url(&self) -> Url {
        self.http.join("meta").unwrap()
    }
    pub fn meta_type_url(&self, ty: &str) -> Url {
        self.http
            .join(&format!("meta/{}", urlencoding::encode(ty)))
            .unwrap()
    }
    pub fn events_url(&self) -> Url {
        self.ws.join("events").unwrap()
    }
    pub fn peer_management_url(&self) -> Url {
        self.http.join("peer-management").unwrap()
    }
    pub fn stream_url(&self, ty: &str, id: &str, stream: &str) -> Url {
        let topic = format!("{}/{}/{}/{}", self.server, ty, id, stream);
        let mut u = self.ws.join("events").unwrap();
        u.query_pairs_mut().append_pair("topic", &topic);
        u
    }
}

pub(crate) fn render_root(_core: &Arc<Core>, h: &Hrefs, peers: &[String]) -> Entity {
    let mut e = Entity::new()
        .with_class("root")
        .with_link(Link::new(rels::SELF, h.root()))
        .with_link(Link::new(rels::RESOURCES, h.resources_url()))
        .with_link(Link::new(rels::SERVER, h.server_url()).with_title(h.server.clone()));
    for peer in peers {
        let url = h
            .http
            .join(&format!("servers/{}", urlencoding::encode(peer)))
            .unwrap();
        e = e.with_link(Link::rels([rels::PEER, rels::SERVER], url).with_title(peer.clone()));
    }
    e.with_link(Link::new(rels::PEER_MANAGEMENT, h.peer_management_url()))
        .with_link(Link::new(rels::EVENTS, h.events_url()))
        .with_action(
            Action::new("query-resources", "GET", h.resources_url())
                .form_urlencoded()
                .with_field(Field::typed("ql", "text")),
        )
}

pub(crate) fn render_resources(h: &Hrefs, snaps: &[ResourceSnapshot]) -> Entity {
    let mut e = Entity::new()
        .with_class("resources")
        .with_property("node", Value::String(h.server.clone()))
        .with_link(Link::new(rels::SELF, h.resources_url()))
        .with_action(
            Action::new("query-resources", "GET", h.resources_url())
                .form_urlencoded()
                .with_field(Field::typed("ql", "text")),
        )
        .with_action(
            Action::new("register-resource", "POST", h.resources_url())
                .form_urlencoded()
                .with_field(Field::typed("type", "text"))
                .with_field(Field::typed("id", "text"))
                .with_field(Field::typed("name", "text")),
        );
    for snap in snaps {
        e = e.with_sub_entity(SubEntity::Embedded(resource_sub_entity(h, snap)));
    }
    e
}

pub(crate) fn render_server(h: &Hrefs, snaps: &[ResourceSnapshot]) -> Entity {
    let mut e = Entity::new()
        .with_class("server")
        .with_property("name", Value::String(h.server.clone()))
        .with_link(Link::new(rels::SELF, h.server_url()))
        .with_link(Link::new(rels::RESOURCES, h.resources_url()))
        .with_link(Link::new(rels::MONITOR, h.events_url()))
        .with_action(
            Action::new("query-resources", "GET", h.resources_url())
                .form_urlencoded()
                .with_field(Field::typed("ql", "text")),
        )
        .with_action(
            Action::new("register-resource", "POST", h.resources_url())
                .form_urlencoded()
                .with_field(Field::typed("type", "text"))
                .with_field(Field::typed("id", "text"))
                .with_field(Field::typed("name", "text")),
        );
    for snap in snaps {
        e = e.with_sub_entity(SubEntity::Embedded(resource_sub_entity(h, snap)));
    }
    e
}

pub(crate) fn resource_sub_entity(h: &Hrefs, snap: &ResourceSnapshot) -> EmbeddedEntity {
    let mut e = EmbeddedEntity::new([rels::RESOURCE])
        .with_class("resource")
        .with_class(snap.kind.clone());

    e = apply_resource_properties(e, snap);
    e.with_link(Link::new(rels::SELF, h.resource_url(&snap.id)))
        .with_link(Link::rels([rels::UP, rels::RESOURCES], h.resources_url()))
}

pub(crate) fn render_device(h: &Hrefs, snap: &ResourceSnapshot) -> Entity {
    let mut e = Entity::new()
        .with_class("resource")
        .with_class(snap.kind.clone());

    e = apply_resource_properties(e, snap)
        .with_link(Link::rels(
            [rels::SELF, rels::EDIT],
            h.resource_url(&snap.id),
        ))
        .with_link(Link::rels([rels::UP, rels::RESOURCES], h.resources_url()))
        .with_link(Link::rels(
            [rels::TYPE, "describedby"],
            h.meta_type_url(&snap.kind),
        ));

    // Actions for currently-allowed transitions. Field shapes come
    // from the snapshot's `TransitionAffordance`, not from runtime
    // internals, so rendering stays a pure projection step.
    for t_name in snap.transitions.iter().filter(|t| t.available) {
        let spec = &t_name.spec;
        let mut action = Action::new(
            spec.name.clone(),
            "POST",
            h.resource_transition_url(&snap.id, &spec.name),
        )
        .with_class("transition")
        .json();
        for f in &spec.fields {
            let mut field = Field::typed(f.name.clone(), f.type_.clone());
            field.title = f.title.clone();
            field.value = f.value.clone();
            action = action.with_field(field);
        }
        e = e.with_action(action);
    }

    // Stream links for every declared stream on the snapshot.
    for stream in &snap.streams {
        let link = Link::rels(
            [rels::MONITOR, rels::OBJECT_STREAM],
            h.stream_url(&snap.kind, &snap.id, &stream.name),
        )
        .with_title(stream.name.clone());
        e = e.with_link(link);
    }

    e
}

pub(crate) fn render_search_results(h: &Hrefs, ql: &str, snaps: &[ResourceSnapshot]) -> Entity {
    let mut self_url = h.resources_url();
    self_url.query_pairs_mut().append_pair("ql", ql);

    let mut e = Entity::new()
        .with_class("resources")
        .with_class("search-results")
        .with_property("name", Value::String(h.server.clone()))
        .with_property("ql", Value::String(ql.to_string()))
        .with_link(Link::new(rels::SELF, self_url))
        .with_action(
            Action::new("register-resource", "POST", h.resources_url())
                .form_urlencoded()
                .with_field(Field::typed("type", "text"))
                .with_field(Field::typed("id", "text"))
                .with_field(Field::typed("name", "text")),
        )
        .with_action(
            Action::new("query-resources", "GET", h.resources_url())
                .form_urlencoded()
                .with_field(Field::typed("ql", "text")),
        );
    for snap in snaps {
        e = e.with_sub_entity(SubEntity::Embedded(resource_sub_entity(h, snap)));
    }
    e
}

trait WithResourceProperty: Sized {
    fn with_resource_property(self, key: impl Into<String>, val: impl Into<Value>) -> Self;
}

impl WithResourceProperty for Entity {
    fn with_resource_property(self, key: impl Into<String>, val: impl Into<Value>) -> Self {
        self.with_property(key, val)
    }
}

impl WithResourceProperty for EmbeddedEntity {
    fn with_resource_property(self, key: impl Into<String>, val: impl Into<Value>) -> Self {
        self.with_property(key, val)
    }
}

fn apply_resource_properties<T: WithResourceProperty>(mut entity: T, snap: &ResourceSnapshot) -> T {
    for (k, v) in snap.properties.iter() {
        if k == "type" {
            continue;
        }
        entity = entity.with_resource_property(k.clone(), v.clone());
    }

    entity = entity
        .with_resource_property("id", Value::String(snap.id.clone()))
        .with_resource_property("kind", Value::String(snap.kind.clone()))
        .with_resource_property("type", Value::String(snap.kind.clone()))
        .with_resource_property("node", Value::String(snap.node.clone()))
        .with_resource_property(
            "name",
            snap.name.clone().map(Value::String).unwrap_or(Value::Null),
        )
        .with_resource_property(
            "state",
            snap.state.clone().map(Value::String).unwrap_or(Value::Null),
        );
    if !snap.labels.is_empty() {
        let labels = snap
            .labels
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect();
        entity = entity.with_resource_property("labels", Value::Object(labels));
    }
    if let Some(revision) = &snap.revision {
        entity = entity.with_resource_property("revision", Value::String(revision.clone()));
    }
    entity
}

pub(crate) struct KindMeta {
    pub spec: ActorSpec,
}

pub(crate) fn render_meta(h: &Hrefs, types: &[KindMeta]) -> Entity {
    let mut e = Entity::new()
        .with_class("metadata")
        .with_property("name", Value::String(h.server.clone()))
        .with_link(Link::new(rels::SELF, h.meta_url()))
        .with_link(Link::new(rels::RESOURCES, h.resources_url()))
        .with_link(Link::new(rels::MONITOR, {
            let mut u = h.events_url();
            u.query_pairs_mut().append_pair("topic", "meta");
            u
        }));

    let mut seen = std::collections::BTreeSet::new();
    for ty in types {
        if !seen.insert(ty.spec.resource.kind.clone()) {
            continue;
        }
        e = e.with_sub_entity(SubEntity::Embedded(meta_type_sub_entity(h, ty)));
    }
    e
}

pub(crate) fn meta_type_sub_entity(h: &Hrefs, ty: &KindMeta) -> EmbeddedEntity {
    let resource = &ty.spec.resource;
    let transitions: Vec<Value> = ty
        .spec
        .transitions
        .iter()
        .map(transition_spec_json)
        .collect();
    let streams: Vec<Value> = resource.streams.iter().map(stream_spec_json).collect();
    let labels = resource
        .labels
        .iter()
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect();

    EmbeddedEntity::new([rels::TYPE, "item"])
        .with_class("type")
        .with_property("kind", Value::String(resource.kind.clone()))
        .with_property("type", Value::String(resource.kind.clone()))
        .with_property(
            "name",
            resource
                .name
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        )
        .with_property("labels", Value::Object(labels))
        .with_property(
            "propertySchema",
            resource.property_schema.clone().unwrap_or(Value::Null),
        )
        .with_property(
            "properties",
            Value::Array(
                ["id", "kind", "type", "node", "state"]
                    .iter()
                    .map(|s| Value::String(s.to_string()))
                    .collect(),
            ),
        )
        .with_property("streams", Value::Array(streams))
        .with_property("transitions", Value::Array(transitions))
        .with_link(Link::new(rels::SELF, h.meta_type_url(&resource.kind)))
}

pub(crate) fn render_meta_type(h: &Hrefs, ty: &KindMeta) -> Entity {
    let sub = meta_type_sub_entity(h, ty);
    Entity {
        class: sub.class,
        title: sub.title,
        properties: sub.properties,
        entities: vec![],
        actions: sub.actions,
        links: sub.links,
    }
}

fn stream_spec_json(spec: &crate::core::StreamSpec) -> Value {
    serde_json::json!({
        "name": spec.name,
        "kind": match spec.kind {
            StreamKind::Object => "object",
            StreamKind::Binary => "binary",
        },
    })
}

fn transition_spec_json(spec: &crate::core::TransitionSpec) -> Value {
    let mut value = serde_json::json!({
        "name": spec.name,
        "allowedStates": spec.allowed_states,
        "result": match spec.result {
            TransitionResultKind::Sync => "sync",
            TransitionResultKind::AsyncJob => "async-job",
        },
        "idempotency": match spec.idempotency {
            Idempotency::None => "none",
            Idempotency::Supported => "supported",
            Idempotency::Required => "required",
        },
        "effect": match spec.effect {
            Effect::Safe => "safe",
            Effect::UnsafeIdempotent => "unsafe-idempotent",
            Effect::Unsafe => "unsafe",
        },
        "requiredScopes": spec.required_scopes,
    });
    let obj = value.as_object_mut().unwrap();
    if let Some(title) = &spec.title {
        obj.insert("title".into(), Value::String(title.clone()));
    }
    if let Some(schema) = &spec.input_schema {
        obj.insert("inputSchema".into(), schema.clone());
    }
    if let Some(schema) = &spec.output_schema {
        obj.insert("outputSchema".into(), schema.clone());
    }
    value
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{Map, Value as Json};

    use super::*;
    use crate::http::core::{StreamSpec, TransitionAffordance};

    fn hrefs() -> Hrefs {
        Hrefs {
            http: Url::parse("http://example/").unwrap(),
            ws: Url::parse("ws://example/").unwrap(),
            server: "hub".into(),
        }
    }

    fn led_snapshot() -> ResourceSnapshot {
        ResourceSnapshot {
            id: "abc".into(),
            kind: "led".into(),
            name: Some("LED".into()),
            state: Some("off".into()),
            node: "hub".into(),
            properties: Map::new(),
            labels: BTreeMap::new(),
            transitions: vec![
                TransitionAffordance {
                    spec: crate::core::TransitionSpec {
                        name: "turn-on".into(),
                        ..Default::default()
                    },
                    available: true,
                    unavailable_reason: None,
                },
                TransitionAffordance {
                    spec: crate::core::TransitionSpec {
                        name: "turn-off".into(),
                        ..Default::default()
                    },
                    available: false,
                    unavailable_reason: None,
                },
            ],
            streams: vec![StreamSpec {
                name: "state".into(),
                kind: "object".into(),
            }],
            revision: None,
            metadata: Map::new(),
        }
    }

    fn led_kind_meta() -> KindMeta {
        KindMeta {
            spec: ActorSpec {
                resource: crate::core::ResourceSpec {
                    kind: "led".into(),
                    name: Some("LED".into()),
                    labels: BTreeMap::new(),
                    property_schema: None,
                    streams: vec![crate::core::StreamSpec {
                        name: "state".into(),
                        kind: StreamKind::Object,
                    }],
                },
                transitions: vec![
                    crate::core::TransitionSpec {
                        name: "turn-on".into(),
                        allowed_states: vec!["off".into()],
                        ..Default::default()
                    },
                    crate::core::TransitionSpec {
                        name: "turn-off".into(),
                        allowed_states: vec!["on".into()],
                        ..Default::default()
                    },
                ],
            },
        }
    }

    #[test]
    fn render_resource_sub_entity_from_resource_snapshot_includes_kind_and_type_for_compat() {
        let h = hrefs();
        let snap = led_snapshot();
        let sub = resource_sub_entity(&h, &snap);
        let v = serde_json::to_value(&sub).unwrap();
        // Both the canonical `kind` (via class) and the compat
        // property `type` are present.
        let classes: Vec<&str> = v["class"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap())
            .collect();
        assert!(classes.contains(&"led"));
        assert_eq!(v["properties"]["type"], "led");
    }

    #[test]
    fn render_meta_type_sub_entity_uses_kind_from_snapshot() {
        let h = hrefs();
        let ty = led_kind_meta();
        let sub = meta_type_sub_entity(&h, &ty);
        let v = serde_json::to_value(&sub).unwrap();
        assert_eq!(v["properties"]["kind"], "led");
        assert_eq!(v["properties"]["type"], "led");
    }

    #[test]
    fn render_meta_type_sub_entity_lists_all_transitions_not_state_dependent_set() {
        // The snapshot only advertises currently-allowed transitions
        // (here: turn-on, since state == "off"). Metadata describes
        // the *kind*, not the instance, so the meta sub-entity must
        // list every transition the kind can ever perform.
        let h = hrefs();
        let snap = led_snapshot();
        let available_names: Vec<&str> = snap
            .transitions
            .iter()
            .filter(|t| t.available)
            .map(|t| t.spec.name.as_str())
            .collect();
        assert_eq!(
            available_names,
            vec!["turn-on"],
            "fixture sanity: the snapshot only sees turn-on in `off`"
        );
        let ty = led_kind_meta();
        let sub = meta_type_sub_entity(&h, &ty);
        let v = serde_json::to_value(&sub).unwrap();
        let names: Vec<&str> = v["properties"]["transitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"turn-on"));
        assert!(names.contains(&"turn-off"));
    }

    #[test]
    fn render_user_property_named_type_does_not_overwrite_compat_alias() {
        let h = hrefs();
        let mut snap = led_snapshot();
        snap.properties
            .insert("type".into(), Json::String("shadow-led".into()));
        snap.properties
            .insert("color".into(), Json::String("red".into()));

        let sub = resource_sub_entity(&h, &snap);
        let v = serde_json::to_value(&sub).unwrap();
        // Canonical alias wins.
        assert_eq!(v["properties"]["type"], "led");
        // Other extras still present.
        assert_eq!(v["properties"]["color"], "red");

        // Same guarantee on the full device render.
        let dev = render_device(&h, &snap);
        let v = serde_json::to_value(&dev).unwrap();
        assert_eq!(v["properties"]["type"], "led");
        assert_eq!(v["properties"]["color"], "red");
    }

    #[test]
    fn resource_renderer_uses_snapshot_transitions_streams_labels_and_revision() {
        let h = hrefs();
        let mut snap = led_snapshot();
        snap.labels.insert("room".into(), "kitchen".into());
        snap.revision = Some("rev-7".into());
        snap.transitions[0]
            .spec
            .fields
            .push(crate::core::FieldSpec {
                name: "brightness".into(),
                type_: "number".into(),
                title: Some("Brightness".into()),
                value: Some(Json::from(42)),
            });

        let dev = render_device(&h, &snap);
        let v = serde_json::to_value(&dev).unwrap();

        assert_eq!(v["properties"]["kind"], "led");
        assert_eq!(v["properties"]["type"], "led");
        assert_eq!(v["properties"]["labels"]["room"], "kitchen");
        assert_eq!(v["properties"]["revision"], "rev-7");

        let action = v["actions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|action| action["name"] == "turn-on")
            .expect("turn-on action");
        assert_eq!(action["type"], "application/json");
        let field_names: Vec<&str> = action["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field["name"].as_str().unwrap())
            .collect();
        assert_eq!(field_names, vec!["brightness"]);

        let stream_href = v["links"]
            .as_array()
            .unwrap()
            .iter()
            .find(|link| link["title"] == "state")
            .and_then(|link| link["href"].as_str())
            .expect("state stream href");
        assert!(
            stream_href.contains("hub%2Fled%2Fabc%2Fstate"),
            "stream href should use snapshot stream metadata, got {stream_href}"
        );
    }
}
