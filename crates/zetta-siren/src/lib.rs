//! Siren hypermedia types.
//!
//! Spec: <https://github.com/kevinswiber/siren>

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use url::Url;

/// Standard rels used throughout Zetta. Strings are kept verbatim with
/// the rel URIs the original Node implementation emits, so existing
/// clients keep working.
pub mod rels {
    pub const SELF: &str = "self";
    pub const UP: &str = "up";
    pub const MONITOR: &str = "monitor";
    pub const EDIT: &str = "edit";
    pub const SERVER: &str = "http://rels.zettajs.io/server";
    pub const DEVICE: &str = "http://rels.zettajs.io/device";
    pub const PEER: &str = "http://rels.zettajs.io/peer";
    pub const PEER_MANAGEMENT: &str = "http://rels.zettajs.io/peer-management";
    pub const EVENTS: &str = "http://rels.zettajs.io/events";
    pub const TYPE: &str = "http://rels.zettajs.io/type";
    pub const OBJECT_STREAM: &str = "http://rels.zettajs.io/object-stream";
    pub const QUERY: &str = "http://rels.zettajs.io/query";
    pub const ROOT: &str = "http://rels.zettajs.io/root";
    pub const INSTANCES: &str = "http://rels.zettajs.io/instances";
    pub const METADATA: &str = "http://rels.zettajs.io/metadata";
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
    pub fn new() -> Self { Self::default() }

    pub fn with_class(mut self, c: impl Into<String>) -> Self {
        self.class.push(c.into());
        self
    }

    pub fn with_self_link(mut self, href: Url) -> Self {
        self.links.push(Link {
            rel: vec![rels::SELF.into()],
            href,
            class: vec![],
            title: None,
            type_: None,
        });
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
