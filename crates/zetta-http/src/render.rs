use std::sync::Arc;

use serde_json::Value;
use url::Url;
use zetta_siren::{rels, Action, EmbeddedEntity, Entity, Field, Link, SubEntity};

use crate::core::{Core, DeviceSnapshot};

/// Url root for absolute hrefs. Computed per-request from request scheme + host.
#[derive(Clone)]
pub(crate) struct Hrefs {
    pub http: Url,
    pub ws: Url,
    pub server: String,
}

impl Hrefs {
    pub fn root(&self) -> Url { self.http.clone() }
    pub fn server_url(&self) -> Url {
        self.http.join(&format!("servers/{}", urlencoding::encode(&self.server))).unwrap()
    }
    pub fn devices_url(&self) -> Url {
        self.http.join(&format!("servers/{}/devices", urlencoding::encode(&self.server))).unwrap()
    }
    pub fn device_url(&self, id: &uuid::Uuid) -> Url {
        self.http.join(&format!("servers/{}/devices/{}", urlencoding::encode(&self.server), id)).unwrap()
    }
    pub fn meta_url(&self) -> Url {
        self.http.join(&format!("servers/{}/meta", urlencoding::encode(&self.server))).unwrap()
    }
    pub fn meta_type_url(&self, ty: &str) -> Url {
        self.http.join(&format!("servers/{}/meta/{}", urlencoding::encode(&self.server), urlencoding::encode(ty))).unwrap()
    }
    pub fn events_url(&self) -> Url {
        self.ws.join("events").unwrap()
    }
    pub fn peer_management_url(&self) -> Url {
        self.http.join("peer-management").unwrap()
    }
    pub fn stream_url(&self, ty: &str, id: &uuid::Uuid, stream: &str) -> Url {
        let topic = format!("{}/{}/{}/{}", self.server, ty, id, stream);
        let mut u = self.ws.join(&format!("servers/{}/events", urlencoding::encode(&self.server))).unwrap();
        u.query_pairs_mut().append_pair("topic", &topic);
        u
    }
}

pub(crate) fn render_root(_core: &Arc<Core>, h: &Hrefs) -> Entity {
    Entity::new()
        .with_class("root")
        .with_link(Link::new(rels::SELF, h.root()))
        .with_link(
            Link::new(rels::SERVER, h.server_url())
                .with_title(h.server.clone()),
        )
        .with_link(Link::new(rels::PEER_MANAGEMENT, h.peer_management_url()))
        .with_link(Link::new(rels::EVENTS, h.events_url()))
        .with_action(
            Action::new("query-devices", "GET", h.root())
                .form_urlencoded()
                .with_field(Field::typed("server", "text"))
                .with_field(Field::typed("ql", "text")),
        )
}

pub(crate) fn render_server(
    h: &Hrefs,
    devices: &[DeviceSnapshot],
) -> Entity {
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
    for d in devices {
        e = e.with_sub_entity(SubEntity::Embedded(device_sub_entity(h, d)));
    }
    e
}

pub(crate) fn device_sub_entity(h: &Hrefs, d: &DeviceSnapshot) -> EmbeddedEntity {
    let mut e = EmbeddedEntity::new([rels::DEVICE])
        .with_class("device")
        .with_class(d.type_.clone())
        .with_property("id", Value::String(d.id.to_string()))
        .with_property("type", Value::String(d.type_.clone()))
        .with_property("name", d.name.clone().map(Value::String).unwrap_or(Value::Null))
        .with_property("state", Value::String(d.state.clone()))
        .with_link(Link::new(rels::SELF, h.device_url(&d.id)))
        .with_link(
            Link::rels([rels::UP, rels::SERVER], h.server_url())
                .with_title(h.server.clone()),
        );
    // Pass through extra properties.
    for (k, v) in d.properties.iter() {
        e = e.with_property(k.clone(), v.clone());
    }
    e
}

pub(crate) fn render_device(h: &Hrefs, d: &DeviceSnapshot) -> Entity {
    let mut e = Entity::new()
        .with_class("device")
        .with_class(d.type_.clone())
        .with_property("id", Value::String(d.id.to_string()))
        .with_property("type", Value::String(d.type_.clone()))
        .with_property("name", d.name.clone().map(Value::String).unwrap_or(Value::Null))
        .with_property("state", Value::String(d.state.clone()))
        .with_link(Link::rels([rels::SELF, rels::EDIT], h.device_url(&d.id)))
        .with_link(
            Link::rels([rels::UP, rels::SERVER], h.server_url())
                .with_title(h.server.clone()),
        )
        .with_link(Link::rels([rels::TYPE, "describedby"], h.meta_type_url(&d.type_)));

    for (k, v) in d.properties.iter() {
        e = e.with_property(k.clone(), v.clone());
    }

    // Actions for currently-allowed transitions.
    for t_name in d.config.allowed_in(&d.state) {
        let spec = d.config.transitions.get(t_name);
        let mut action = Action::new(t_name.clone(), "POST", h.device_url(&d.id))
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

    // Stream links for declared streams.
    for s in &d.config.streams {
        let link = Link::rels(
            [rels::MONITOR, rels::OBJECT_STREAM],
            h.stream_url(&d.type_, &d.id, &s.name),
        )
        .with_title(s.name.clone());
        e = e.with_link(link);
    }

    e
}

pub(crate) fn render_search_results(
    h: &Hrefs,
    ql: &str,
    devices: &[DeviceSnapshot],
) -> Entity {
    let mut self_url = h.server_url();
    self_url.query_pairs_mut().append_pair("ql", ql);
    let mut query_ws = h.events_url();
    query_ws.query_pairs_mut().append_pair("topic", &format!("query/{ql}"));

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
    for d in devices {
        e = e.with_sub_entity(SubEntity::Embedded(device_sub_entity(h, d)));
    }
    e
}

pub(crate) fn render_meta(h: &Hrefs, devices: &[DeviceSnapshot]) -> Entity {
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
    for d in devices {
        if !seen.insert(d.type_.clone()) {
            continue;
        }
        e = e.with_sub_entity(SubEntity::Embedded(meta_type_sub_entity(h, d)));
    }
    e
}

pub(crate) fn meta_type_sub_entity(h: &Hrefs, d: &DeviceSnapshot) -> EmbeddedEntity {
    let transitions: Vec<Value> = d
        .config
        .transitions
        .keys()
        .map(|n| serde_json::json!({"name": n}))
        .collect();
    let streams: Vec<Value> = d
        .config
        .streams
        .iter()
        .map(|s| Value::String(s.name.clone()))
        .collect();

    EmbeddedEntity::new([rels::TYPE, "item"])
        .with_class("type")
        .with_property("type", Value::String(d.type_.clone()))
        .with_property("properties", Value::Array(
            ["id", "type", "state"]
                .iter()
                .map(|s| Value::String(s.to_string()))
                .collect(),
        ))
        .with_property("streams", Value::Array(streams))
        .with_property("transitions", Value::Array(transitions))
        .with_link(Link::new(rels::SELF, h.meta_type_url(&d.type_)))
}
