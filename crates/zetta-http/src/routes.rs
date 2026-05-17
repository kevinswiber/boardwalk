use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, Request, State, WebSocketUpgrade},
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get},
    Json, Router,
};
use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use uuid::Uuid;
use zetta_core::TransitionInput;
use zetta_siren::SIREN_CONTENT_TYPE;

use crate::core::Core;
use crate::render::{self, Hrefs};

/// Callback invoked after a successful peer WS upgrade. The runtime
/// supplies this when peering is enabled.
pub type PeerHandler = Arc<
    dyn Fn(String, Uuid, hyper::upgrade::Upgraded) -> BoxFuture<'static, ()> + Send + Sync,
>;

/// Per-server in-flight peer-confirmation state, keyed by connection id.
/// Populated when a `PeerClient` (initiator) is mid-handshake and
/// drained when the acceptor's `GET /_initiate_peer/{id}` request lands.
#[derive(Clone, Default)]
pub struct PeerInitState {
    inner: Arc<std::sync::Mutex<std::collections::HashMap<Uuid, ()>>>,
}

impl PeerInitState {
    pub fn register(&self, id: Uuid) {
        self.inner.lock().unwrap().insert(id, ());
    }
    pub fn consume(&self, id: &Uuid) -> bool {
        self.inner.lock().unwrap().remove(id).is_some()
    }
}

#[derive(Clone)]
pub struct AppState {
    pub core: Arc<Core>,
    pub peer_handler: Option<PeerHandler>,
    pub peer_init: PeerInitState,
}

pub fn router(core: Arc<Core>) -> Router {
    router_with(AppState {
        core,
        peer_handler: None,
        peer_init: PeerInitState::default(),
    })
}

pub fn router_with(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/servers/{name}", get(server_get))
        .route("/servers/{name}/devices", get(devices_get))
        .route(
            "/servers/{name}/devices/{id}",
            get(device_get).post(device_post),
        )
        .route("/servers/{name}/meta", get(meta_get))
        .route("/servers/{name}/meta/{type}", get(meta_type_get))
        .route("/peer-management", get(peer_management_get))
        .route("/events", get(events_ws))
        .route("/peers/{name}", any(peers_upgrade))
        .route("/_initiate_peer/{id}", get(initiate_peer))
        .with_state(state)
}

fn build_hrefs(headers: &HeaderMap, uri: &Uri, server: &str) -> Hrefs {
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.scheme_str())
        .unwrap_or("http");
    let http_base: url::Url = format!("{scheme}://{host}/").parse().unwrap();
    let ws_scheme = if scheme == "https" { "wss" } else { "ws" };
    let ws_base: url::Url = format!("{ws_scheme}://{host}/").parse().unwrap();
    Hrefs { http: http_base, ws: ws_base, server: server.to_string() }
}

fn siren_response(entity: zetta_siren::Entity) -> Response {
    let body = serde_json::to_vec(&entity).unwrap();
    let mut resp = Response::new(axum::body::Body::from(body));
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(SIREN_CONTENT_TYPE),
    );
    resp
}

#[derive(Debug, Deserialize)]
struct QueryParams {
    ql: Option<String>,
    server: Option<String>,
}

async fn root(
    State(AppState { core, .. }): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    Query(params): Query<QueryParams>,
) -> Response {
    let h = build_hrefs(&headers, &uri, &core.name);
    if let Some(ql) = params.ql {
        let devices = core.list_devices().await;
        let filtered = filter_by_ql(&devices, &ql);
        return siren_response(render::render_search_results(&h, &ql, &filtered));
    }
    let _ = params.server;
    siren_response(render::render_root(&core, &h))
}

async fn server_get(
    State(AppState { core, .. }): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    Query(params): Query<QueryParams>,
) -> Response {
    if name != core.name {
        return (StatusCode::NOT_FOUND, "unknown server").into_response();
    }
    let h = build_hrefs(&headers, &uri, &core.name);
    let devices = core.list_devices().await;
    if let Some(ql) = params.ql {
        let filtered = filter_by_ql(&devices, &ql);
        return siren_response(render::render_search_results(&h, &ql, &filtered));
    }
    siren_response(render::render_server(&h, &devices))
}

async fn devices_get(
    State(AppState { core, .. }): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if name != core.name {
        return (StatusCode::NOT_FOUND, "unknown server").into_response();
    }
    let h = build_hrefs(&headers, &uri, &core.name);
    let devices = core.list_devices().await;
    siren_response(render::render_server(&h, &devices))
}

async fn device_get(
    State(AppState { core, .. }): State<AppState>,
    Path((name, id)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if name != core.name {
        return (StatusCode::NOT_FOUND, "unknown server").into_response();
    }
    let h = build_hrefs(&headers, &uri, &core.name);
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid device id").into_response(),
    };
    match core.get_device(&id).await {
        Some(d) => siren_response(render::render_device(&h, &d)),
        None => (StatusCode::NOT_FOUND, "unknown device").into_response(),
    }
}

async fn device_post(
    State(AppState { core, .. }): State<AppState>,
    Path((name, id)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
    body: String,
) -> Response {
    if name != core.name {
        return (StatusCode::NOT_FOUND, "unknown server").into_response();
    }
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid device id").into_response(),
    };
    let h = build_hrefs(&headers, &uri, &core.name);

    let pairs: Vec<(String, String)> = match serde_urlencoded::from_str(&body) {
        Ok(v) => v,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("bad form body: {e}")).into_response(),
    };
    let mut map: BTreeMap<String, JsonValue> = BTreeMap::new();
    let mut action_name = None;
    for (k, v) in pairs {
        if k == "action" {
            action_name = Some(v);
        } else {
            map.insert(k, JsonValue::String(v));
        }
    }
    let action_name = match action_name {
        Some(n) => n,
        None => return (StatusCode::BAD_REQUEST, "missing `action` field").into_response(),
    };
    let input = TransitionInput { fields: map };
    match core.run_transition(&id, &action_name, input).await {
        Ok(snap) => siren_response(render::render_device(&h, &snap)),
        Err(zetta_core::DeviceError::NotAllowed(_)) => {
            (StatusCode::CONFLICT, "transition not allowed in current state").into_response()
        }
        Err(zetta_core::DeviceError::Invalid(msg)) => {
            (StatusCode::BAD_REQUEST, msg).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}

async fn meta_get(
    State(AppState { core, .. }): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if name != core.name {
        return (StatusCode::NOT_FOUND, "unknown server").into_response();
    }
    let h = build_hrefs(&headers, &uri, &core.name);
    let devices = core.list_devices().await;
    siren_response(render::render_meta(&h, &devices))
}

async fn meta_type_get(
    State(AppState { core, .. }): State<AppState>,
    Path((name, ty)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if name != core.name {
        return (StatusCode::NOT_FOUND, "unknown server").into_response();
    }
    let h = build_hrefs(&headers, &uri, &core.name);
    let devices = core.list_devices().await;
    let dev = devices.iter().find(|d| d.type_ == ty);
    match dev {
        Some(d) => siren_response(zetta_siren::Entity::new()
            .with_class("type")
            .with_property("type", JsonValue::String(d.type_.clone()))
            .with_link(zetta_siren::Link::new(
                zetta_siren::rels::SELF,
                h.meta_type_url(&d.type_),
            ))),
        None => (StatusCode::NOT_FOUND, "unknown type").into_response(),
    }
}

async fn peer_management_get() -> Response {
    Json(serde_json::json!({
        "class": ["peer-management"],
        "actions": [],
        "entities": [],
        "links": [],
    })).into_response()
}

async fn events_ws(
    State(AppState { core, .. }): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| crate::ws::handle_socket(socket, core))
}

#[derive(Debug, Deserialize)]
struct PeerQuery { #[serde(rename = "connectionId")] connection_id: Option<Uuid> }

/// `POST /peers/{name}` — WebSocket upgrade then HTTP/2 (acceptor role).
async fn peers_upgrade(
    State(state): State<AppState>,
    Path(peer_name): Path<String>,
    Query(query): Query<PeerQuery>,
    mut req: Request<Body>,
) -> Response {
    let connection_id = match query.connection_id {
        Some(id) => id,
        None => return (StatusCode::BAD_REQUEST, "missing connectionId").into_response(),
    };

    let handler = match state.peer_handler.clone() {
        Some(h) => h,
        None => return (StatusCode::SERVICE_UNAVAILABLE, "peering disabled").into_response(),
    };

    // Build 101 from request headers.
    let upgrade_response = match zetta_tunnel_upgrade_response(req.headers()) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("upgrade: {e}")).into_response(),
    };

    let on_upgrade = hyper::upgrade::on(&mut req);
    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => handler(peer_name, connection_id, upgraded).await,
            Err(e) => tracing::warn!(%e, "peer upgrade failed"),
        }
    });

    upgrade_response
}

/// Reuse `zetta_tunnel::build_upgrade_response`.
fn zetta_tunnel_upgrade_response(headers: &HeaderMap) -> Result<Response, String> {
    let resp = zetta_tunnel::build_upgrade_response(headers).map_err(|e| format!("{e}"))?;
    let (parts, _) = resp.into_parts();
    let mut r = Response::builder().status(parts.status);
    for (name, value) in parts.headers.iter() {
        r = r.header(name.clone(), value.clone());
    }
    r.body(Body::empty()).map_err(|e| format!("{e}"))
}

/// `GET /_initiate_peer/{id}` — initiator-side handshake completion.
async fn initiate_peer(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    let id = match Uuid::parse_str(&id) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid id").into_response(),
    };
    if state.peer_init.consume(&id) {
        (StatusCode::OK, "ok").into_response()
    } else {
        (StatusCode::NOT_FOUND, "unknown connection id").into_response()
    }
}

fn filter_by_ql(devices: &[crate::core::DeviceSnapshot], ql: &str) -> Vec<crate::core::DeviceSnapshot> {
    let q = match zetta_caql::parse(ql) {
        Ok(q) => q,
        Err(_) => return Vec::new(),
    };
    devices
        .iter()
        .filter(|d| {
            let target = serde_json::json!({
                "id": d.id.to_string(),
                "type": d.type_,
                "name": d.name,
                "state": d.state,
            });
            zetta_caql::matches(&q, &target).unwrap_or(false)
        })
        .cloned()
        .collect()
}
