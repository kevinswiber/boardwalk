//! Siren hypermedia types.
//!
//! Spec: <https://github.com/kevinswiber/siren>

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use url::Url;

/// Standard rels used throughout Boardwalk. Strings are kept verbatim with
/// the rel URIs the original Node implementation emits, so existing
/// clients keep working.
pub mod rels {
    pub const SELF: &str = "self";
    pub const UP: &str = "up";
    pub const MONITOR: &str = "monitor";
    pub const EDIT: &str = "edit";
    pub const SERVER: &str = "https://rels.boardwalk.dev/server";
    pub const DEVICE: &str = "https://rels.boardwalk.dev/device";
    pub const PEER: &str = "https://rels.boardwalk.dev/peer";
    pub const PEER_MANAGEMENT: &str = "https://rels.boardwalk.dev/peer-management";
    pub const EVENTS: &str = "https://rels.boardwalk.dev/events";
    pub const TYPE: &str = "https://rels.boardwalk.dev/type";
    pub const OBJECT_STREAM: &str = "https://rels.boardwalk.dev/object-stream";
    pub const QUERY: &str = "https://rels.boardwalk.dev/query";
    pub const ROOT: &str = "https://rels.boardwalk.dev/root";
    pub const INSTANCES: &str = "https://rels.boardwalk.dev/instances";
    pub const METADATA: &str = "https://rels.boardwalk.dev/metadata";
}

pub const SIREN_CONTENT_TYPE: &str = "application/vnd.siren+json";

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Entity {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub class: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub properties: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entities: Vec<SubEntity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<Action>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<Link>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SubEntity {
    Embedded(EmbeddedEntity),
    Link(EmbeddedLink),
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct EmbeddedEntity {
    pub rel: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub class: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub properties: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<Link>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<Action>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EmbeddedLink {
    pub rel: Vec<String>,
    pub href: Url,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub class: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub type_: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Link {
    pub rel: Vec<String>,
    pub href: Url,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub class: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub type_: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Action {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub class: Vec<String>,
    pub method: String, // keep as String — we want lower/upper exactly as written
    pub href: Url,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub type_: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<Field>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub class: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

impl Entity {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_class(mut self, c: impl Into<String>) -> Self {
        self.class.push(c.into());
        self
    }

    pub fn with_title(mut self, t: impl Into<String>) -> Self {
        self.title = Some(t.into());
        self
    }

    pub fn with_property(mut self, key: impl Into<String>, val: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), val.into());
        self
    }

    pub fn with_link(mut self, l: Link) -> Self {
        self.links.push(l);
        self
    }

    pub fn with_action(mut self, a: Action) -> Self {
        self.actions.push(a);
        self
    }

    pub fn with_sub_entity(mut self, e: SubEntity) -> Self {
        self.entities.push(e);
        self
    }

    pub fn with_self_link(mut self, href: Url) -> Self {
        self.links.push(Link::new(rels::SELF, href));
        self
    }
}

impl Link {
    pub fn new(rel: impl Into<String>, href: Url) -> Self {
        Self {
            rel: vec![rel.into()],
            href,
            class: vec![],
            title: None,
            type_: None,
        }
    }

    pub fn rels(rels: impl IntoIterator<Item = impl Into<String>>, href: Url) -> Self {
        Self {
            rel: rels.into_iter().map(Into::into).collect(),
            href,
            class: vec![],
            title: None,
            type_: None,
        }
    }

    pub fn with_title(mut self, t: impl Into<String>) -> Self {
        self.title = Some(t.into());
        self
    }
}

impl Action {
    pub fn new(name: impl Into<String>, method: impl Into<String>, href: Url) -> Self {
        Self {
            name: name.into(),
            class: vec![],
            method: method.into(),
            href,
            title: None,
            type_: None,
            fields: vec![],
        }
    }

    pub fn with_class(mut self, c: impl Into<String>) -> Self {
        self.class.push(c.into());
        self
    }

    pub fn with_field(mut self, f: Field) -> Self {
        self.fields.push(f);
        self
    }

    pub fn form_urlencoded(mut self) -> Self {
        self.type_ = Some("application/x-www-form-urlencoded".into());
        self
    }
}

impl Field {
    pub fn hidden(name: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            name: name.into(),
            type_: "hidden".into(),
            class: vec![],
            value: Some(value.into()),
            title: None,
        }
    }

    pub fn typed(name: impl Into<String>, ty: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_: ty.into(),
            class: vec![],
            value: None,
            title: None,
        }
    }
}

impl EmbeddedEntity {
    pub fn new(rel: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            rel: rel.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }

    pub fn with_class(mut self, c: impl Into<String>) -> Self {
        self.class.push(c.into());
        self
    }

    pub fn with_property(mut self, k: impl Into<String>, v: impl Into<Value>) -> Self {
        self.properties.insert(k.into(), v.into());
        self
    }

    pub fn with_link(mut self, l: Link) -> Self {
        self.links.push(l);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_serializes_with_self_link() {
        let href: Url = "http://127.0.0.1:1337/".parse().unwrap();
        let e = Entity::new().with_class("root").with_self_link(href);
        let v: serde_json::Value = serde_json::to_value(&e).unwrap();
        assert_eq!(v["class"], serde_json::json!(["root"]));
        assert_eq!(v["links"][0]["rel"], serde_json::json!(["self"]));
        assert_eq!(v["links"][0]["href"], "http://127.0.0.1:1337/");
    }

    #[test]
    fn empty_entity_drops_empty_fields() {
        let v = serde_json::to_value(Entity::new()).unwrap();
        assert!(v.as_object().unwrap().is_empty());
    }
}
