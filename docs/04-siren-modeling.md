# Siren Modeling

We are not depending on a third-party Siren crate (none is maintained
in 2026). `boardwalk-siren` ships a small in-house representation.

## Types

```rust
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Entity {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub class: Vec<String>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub properties: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entities: Vec<SubEntity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<Action>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<Link>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SubEntity {
    Embedded(EmbeddedEntity),
    Link(EmbeddedLink),
}

pub struct Link { pub rel: Vec<String>, pub href: Url, pub class: Vec<String>,
                  pub title: Option<String>, pub type_: Option<String> }

pub struct Action { pub name: String, pub method: Method,
                    pub href: Url, pub title: Option<String>,
                    pub class: Vec<String>, pub type_: Option<String>,
                    pub fields: Vec<Field> }

pub struct Field { pub name: String, pub class: Vec<String>, pub type_: String,
                   pub value: Option<Value>, pub title: Option<String> }
```

Content type: `application/vnd.siren+json`. The HTTP layer will fall
back to `application/json` for clients that don't ask for Siren.

## Standard rels (preserved from original)

| Rel                                       | Used at                          |
|-------------------------------------------|----------------------------------|
| `self`                                    | every resource                   |
| `up`                                      | child → parent                   |
| `monitor`                                 | resource → its WS feed           |
| `edit`                                    | resource that supports PUT/DELETE|
| `https://rels.boardwalk.to/server`           | root → server, device → server   |
| `https://rels.boardwalk.to/device`           | server → device sub-entity       |
| `https://rels.boardwalk.to/peer`             | server → peer server             |
| `https://rels.boardwalk.to/peer-management`  | root → peer-management           |
| `https://rels.boardwalk.to/events`           | root → multiplex WS              |
| `https://rels.boardwalk.to/type`             | device → its metadata            |
| `https://rels.boardwalk.to/object-stream`    | device → its monitored stream    |
| `https://rels.boardwalk.to/query`            | search results → live WS         |
| `https://rels.boardwalk.to/root`             | peer entry → its root            |
| `https://rels.boardwalk.to/instances`        | meta/type → instances query      |
| `https://rels.boardwalk.to/metadata`         | type → metadata collection       |

A constants module `boardwalk_http::rels` exposes these as `&'static str`s.

## Resource map

| Path                                          | GET                                                          | Other            |
|-----------------------------------------------|--------------------------------------------------------------|------------------|
| `/`                                           | Root: links to local server, peer servers, peer-management   | —                |
| `/servers/{name}`                             | Server: properties + device sub-entities                     | —                |
| `/servers/{name}/devices`                     | Devices collection                                           | POST: register   |
| `/servers/{name}/devices/{id}`                | Device: state, actions, monitor links                        | POST: transition, PUT: update, DELETE: destroy |
| `/servers/{name}/meta`                        | Metadata collection                                          | —                |
| `/servers/{name}/meta/{type}`                 | Type metadata                                                | —                |
| `/peer-management`                            | Peer list                                                    | POST: link       |
| `/peer-management/{id}`                       | Single peer                                                  | —                |
| `/events`                                     | (101) Multiplex WS upgrade                                   | —                |
| `/servers/{name}/events?topic=…`              | (101) Legacy single-stream WS upgrade *or* HTTP/2 long body  | —                |
| `/_initiate_peer/{connection_id}`             | Peer-tunnel internal handshake (HTTP/2 only)                 | —                |

The query-devices action lives on `/` and `/servers/{name}`:

```
GET /?ql=where%20type%3D%22led%22
```

The response is a search-results entity (`class: ["server",
"search-results"]`) with matching devices as sub-entities and a
`https://rels.boardwalk.to/query` link pointing at a live WS subscription
for the same query.

## Device transitions → Siren actions

Given the builder declaration:

```rust
config
    .state("off")
    .when("off", &["turn-on"])
    .when("on", &["turn-off"])
    .map_async("turn-on", Led::turn_on)
    .map_async("turn-off", Led::turn_off);
```

…the device resource at state `off` emits exactly one action,
`turn-on`, with `method=POST`, `href=/servers/{name}/devices/{id}`, and
a single hidden field `{name: "action", type: "hidden", value: "turn-on"}`
as in the original. Additional named inputs become additional Siren
fields, typed from the Rust type system:

| Rust type                  | Siren field `type`  |
|----------------------------|---------------------|
| `String`, `&str`           | `"text"`            |
| integers + floats          | `"number"`          |
| `bool`                     | `"checkbox"`        |
| `chrono::DateTime`         | `"datetime-local"`  |
| `url::Url`                 | `"url"`             |
| `Vec<T>`                   | repeated field      |

Mapping is via a `FieldType` trait, with manual overrides on the
builder.

## Stream-to-link rendering

A device with a `state` stream (from `config.monitor("state")`) emits:

```json
{
  "title": "state",
  "rel": ["monitor", "https://rels.boardwalk.to/object-stream"],
  "href": "ws://host/servers/{server}/events?topic={type}%2F{id}%2Fstate"
}
```

This is computed once per device-resource render — the href format is
fixed.
