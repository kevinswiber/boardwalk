use std::sync::Arc;

use serde_json::Value;
use url::Url;

use super::core::{Core, ResourceSnapshot};
use crate::core::DeviceConfig;
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
    pub fn devices_url(&self) -> Url {
        self.http
            .join(&format!(
                "servers/{}/devices",
                urlencoding::encode(&self.server)
            ))
            .unwrap()
    }
    pub fn device_url(&self, id: &str) -> Url {
        self.http
            .join(&format!(
                "servers/{}/devices/{}",
                urlencoding::encode(&self.server),
                id
            ))
            .unwrap()
    }
    pub fn meta_url(&self) -> Url {
        self.http
            .join(&format!(
                "servers/{}/meta",
                urlencoding::encode(&self.server)
            ))
            .unwrap()
    }
    pub fn meta_type_url(&self, ty: &str) -> Url {
        self.http
            .join(&format!(
                "servers/{}/meta/{}",
                urlencoding::encode(&self.server),
                urlencoding::encode(ty)
            ))
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
        let mut u = self
            .ws
            .join(&format!(
                "servers/{}/events",
                urlencoding::encode(&self.server)
            ))
            .unwrap();
        u.query_pairs_mut().append_pair("topic", &topic);
        u
    }
}

pub(crate) fn render_root(_core: &Arc<Core>, h: &Hrefs, peers: &[String]) -> Entity {
    let mut e = Entity::new()
        .with_class("root")
        .with_link(Link::new(rels::SELF, h.root()))
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
            Action::new("query-devices", "GET", h.root())
                .form_urlencoded()
                .with_field(Field::typed("server", "text"))
                .with_field(Field::typed("ql", "text")),
        )
}

pub(crate) fn render_server(h: &Hrefs, snaps: &[ResourceSnapshot]) -> Entity {
    let mut e = Entity::new()
        .with_class("server")
        .with_property("name", Value::String(h.server.clone()))
        .with_link(Link::new(rels::SELF, h.server_url()))
        .with_link(Link::new(rels::MONITOR, h.events_url()))
        .with_action(
            Action::new("query-devices", "GET", h.server_url())
                .form_urlencoded()
                .with_field(Field::typed("ql", "text")),
        )
        .with_action(
            Action::new("register-device", "POST", h.devices_url())
                .form_urlencoded()
                .with_field(Field::typed("type", "text"))
                .with_field(Field::typed("id", "text"))
                .with_field(Field::typed("name", "text")),
        );
    for snap in snaps {
        e = e.with_sub_entity(SubEntity::Embedded(device_sub_entity(h, snap)));
    }
    e
}

pub(crate) fn device_sub_entity(h: &Hrefs, snap: &ResourceSnapshot) -> EmbeddedEntity {
    let mut e = EmbeddedEntity::new([rels::DEVICE])
        .with_class("device")
        .with_class(snap.kind.clone());

    // Write user-supplied extras first, skipping any literal `type`
    // key — the canonical `type` (= snap.kind) is the next write and
    // must not be shadowed by user input. `sanitize_properties`
    // already strips `type` from the snapshot's property map; the
    // explicit filter here is a defense-in-depth guard for callers
    // that construct a `ResourceSnapshot` without going through the
    // sanitizer.
    for (k, v) in snap.properties.iter() {
        if k == "type" {
            continue;
        }
        e = e.with_property(k.clone(), v.clone());
    }

    e.with_property("id", Value::String(snap.id.clone()))
        .with_property("type", Value::String(snap.kind.clone()))
        .with_property(
            "name",
            snap.name.clone().map(Value::String).unwrap_or(Value::Null),
        )
        .with_property(
            "state",
            snap.state.clone().map(Value::String).unwrap_or(Value::Null),
        )
        .with_link(Link::new(rels::SELF, h.device_url(&snap.id)))
        .with_link(
            Link::rels([rels::UP, rels::SERVER], h.server_url()).with_title(h.server.clone()),
        )
}

pub(crate) fn render_device(h: &Hrefs, snap: &ResourceSnapshot, cfg: &DeviceConfig) -> Entity {
    let mut e = Entity::new()
        .with_class("device")
        .with_class(snap.kind.clone());

    // See `device_sub_entity` for the property-write ordering rule.
    for (k, v) in snap.properties.iter() {
        if k == "type" {
            continue;
        }
        e = e.with_property(k.clone(), v.clone());
    }

    e = e
        .with_property("id", Value::String(snap.id.clone()))
        .with_property("type", Value::String(snap.kind.clone()))
        .with_property(
            "name",
            snap.name.clone().map(Value::String).unwrap_or(Value::Null),
        )
        .with_property(
            "state",
            snap.state.clone().map(Value::String).unwrap_or(Value::Null),
        )
        .with_link(Link::rels([rels::SELF, rels::EDIT], h.device_url(&snap.id)))
        .with_link(
            Link::rels([rels::UP, rels::SERVER], h.server_url()).with_title(h.server.clone()),
        )
        .with_link(Link::rels(
            [rels::TYPE, "describedby"],
            h.meta_type_url(&snap.kind),
        ));

    // Actions for currently-allowed transitions. Field shapes come
    // from `DeviceConfig` because the snapshot does not yet carry
    // `FieldSpec`.
    for t_name in snap
        .transitions
        .iter()
        .filter(|t| t.available)
        .map(|t| &t.spec.name)
    {
        let t_name: &String = t_name;
        let spec = cfg.transitions.get(t_name);
        let mut action = Action::new(t_name.clone(), "POST", h.device_url(&snap.id))
            .with_class("transition")
            .form_urlencoded()
            .with_field(Field::hidden("action", Value::String(t_name.clone())));
        if let Some(spec) = spec {
            for f in &spec.fields {
                let mut field = Field::typed(f.name.clone(), f.type_.clone());
                field.title = f.title.clone();
                field.value = f.value.clone();
                action = action.with_field(field);
            }
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
    let mut self_url = h.server_url();
    self_url.query_pairs_mut().append_pair("ql", ql);
    let mut query_ws = h.events_url();
    query_ws
        .query_pairs_mut()
        .append_pair("topic", &format!("query/{ql}"));

    let mut e = Entity::new()
        .with_class("server")
        .with_class("search-results")
        .with_property("name", Value::String(h.server.clone()))
        .with_property("ql", Value::String(ql.to_string()))
        .with_link(Link::new(rels::SELF, self_url))
        .with_link(Link::new(rels::QUERY, query_ws))
        .with_action(
            Action::new("register-device", "POST", h.devices_url())
                .form_urlencoded()
                .with_field(Field::typed("type", "text"))
                .with_field(Field::typed("id", "text"))
                .with_field(Field::typed("name", "text")),
        )
        .with_action(
            Action::new("query-devices", "GET", h.server_url())
                .form_urlencoded()
                .with_field(Field::typed("ql", "text")),
        );
    for snap in snaps {
        e = e.with_sub_entity(SubEntity::Embedded(device_sub_entity(h, snap)));
    }
    e
}

/// Metadata about a resource *kind* (not a specific instance).
/// `transitions` and `streams` describe the full surface declared by
/// `DeviceConfig`, regardless of any one instance's current state.
pub(crate) struct TypeMeta<'a> {
    pub snap: &'a ResourceSnapshot,
    pub all_transitions: Vec<String>,
    pub all_streams: Vec<String>,
}

pub(crate) fn render_meta(h: &Hrefs, types: &[TypeMeta<'_>]) -> Entity {
    let mut e = Entity::new()
        .with_class("metadata")
        .with_property("name", Value::String(h.server.clone()))
        .with_link(Link::new(rels::SELF, h.meta_url()))
        .with_link(Link::new(rels::SERVER, h.server_url()))
        .with_link(Link::new(rels::MONITOR, {
            let mut u = h.events_url();
            u.query_pairs_mut().append_pair("topic", "meta");
            u
        }));

    let mut seen = std::collections::BTreeSet::new();
    for ty in types {
        if !seen.insert(ty.snap.kind.clone()) {
            continue;
        }
        e = e.with_sub_entity(SubEntity::Embedded(meta_type_sub_entity(h, ty)));
    }
    e
}

pub(crate) fn meta_type_sub_entity(h: &Hrefs, ty: &TypeMeta<'_>) -> EmbeddedEntity {
    let transitions: Vec<Value> = ty
        .all_transitions
        .iter()
        .map(|n| serde_json::json!({"name": n}))
        .collect();
    let streams: Vec<Value> = ty
        .all_streams
        .iter()
        .map(|s| Value::String(s.clone()))
        .collect();

    EmbeddedEntity::new([rels::TYPE, "item"])
        .with_class("type")
        .with_property("type", Value::String(ty.snap.kind.clone()))
        .with_property(
            "properties",
            Value::Array(
                ["id", "type", "state"]
                    .iter()
                    .map(|s| Value::String(s.to_string()))
                    .collect(),
            ),
        )
        .with_property("streams", Value::Array(streams))
        .with_property("transitions", Value::Array(transitions))
        .with_link(Link::new(rels::SELF, h.meta_type_url(&ty.snap.kind)))
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

    #[test]
    fn render_device_sub_entity_from_resource_snapshot_includes_kind_and_type_for_compat() {
        let h = hrefs();
        let snap = led_snapshot();
        let sub = device_sub_entity(&h, &snap);
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
        let snap = led_snapshot();
        let ty = TypeMeta {
            snap: &snap,
            all_transitions: vec!["turn-on".into(), "turn-off".into()],
            all_streams: vec!["state".into()],
        };
        let sub = meta_type_sub_entity(&h, &ty);
        let v = serde_json::to_value(&sub).unwrap();
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
        let ty = TypeMeta {
            snap: &snap,
            all_transitions: vec!["turn-on".into(), "turn-off".into()],
            all_streams: vec!["state".into()],
        };
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

        let sub = device_sub_entity(&h, &snap);
        let v = serde_json::to_value(&sub).unwrap();
        // Canonical alias wins.
        assert_eq!(v["properties"]["type"], "led");
        // Other extras still present.
        assert_eq!(v["properties"]["color"], "red");

        // Same guarantee on the full device render.
        let cfg = DeviceConfig::default();
        let dev = render_device(&h, &snap, &cfg);
        let v = serde_json::to_value(&dev).unwrap();
        assert_eq!(v["properties"]["type"], "led");
        assert_eq!(v["properties"]["color"], "red");
    }
}
