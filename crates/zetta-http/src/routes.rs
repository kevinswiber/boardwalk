use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, Request, State, WebSocketUpgrade},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get},
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
pub type PeerHandler =
    Arc<dyn Fn(String, Uuid, hyper::upgrade::Upgraded) -> BoxFuture<'static, ()> + Send + Sync>;

/// Cloud-side handle into the live HTTP/2 sender for each connected
/// peer. The runtime (zetta-peer) implements this; the HTTP router
/// uses it to forward `/servers/{peer-name}/...` requests through the
/// established tunnel.
#[async_trait::async_trait]
pub trait PeerSenders: Send + Sync + 'static {
    async fn sender(
        &self,
        name: &str,
    ) -> Option<hyper::client::conn::http2::SendRequest<axum::body::Body>>;
    async fn names(&self) -> Vec<String>;
}

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
    pub peer_senders: Option<Arc<dyn PeerSenders>>,
    pub peer_streams: crate::peer_streams::PeerStreamHub,
}

pub fn router(core: Arc<Core>) -> Router {
    router_with(AppState {
        core,
        peer_handler: None,
        peer_init: PeerInitState::default(),
        peer_senders: None,
        peer_streams: crate::peer_streams::PeerStreamHub::new(),
    })
}

pub fn router_with(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/servers/{name}", get(server_get))
        .route(
            "/servers/{name}/devices",
            get(devices_get).post(devices_post_stub),
        )
        .route(
            "/servers/{name}/devices/{id}",
            get(device_get).post(device_post),
        )
        .route("/servers/{name}/meta", get(meta_get))
        .route("/servers/{name}/meta/{type}", get(meta_type_get))
        .route("/servers/{name}/events", get(server_events_stream))
        .route("/peer-management", get(peer_management_get))
        .route("/events", get(events_ws))
        .route("/peers/{name}", any(peers_upgrade))
        .route("/_initiate_peer/{id}", get(initiate_peer))
        .with_state(state)
}

/// Either dispatch locally or forward to a connected peer by name.
/// Returns `None` if the name matches the local server (caller serves);
/// returns `Some(resp)` if forwarding (or 404'ing).
async fn maybe_forward_or_404(
    state: &AppState,
    target_name: &str,
    method: Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Body,
) -> Option<Response> {
    if target_name == state.core.name {
        return None;
    }
    let senders = state.peer_senders.as_ref()?;
    let mut sender = senders.sender(target_name).await?;
    let path_and_query = uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or(uri.path());
    let target_uri = format!(
        "http://{}.unreachable.zettajs.io{}",
        urlencoding::encode(target_name),
        path_and_query
    );
    let mut builder = http::Request::builder().method(method).uri(target_uri);
    for (name, value) in headers.iter() {
        if name == http::header::HOST {
            continue;
        }
        builder = builder.header(name.clone(), value.clone());
    }
    let req = match builder.body(body) {
        Ok(r) => r,
        Err(e) => {
            return Some(
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("forward build: {e}"),
                )
                    .into_response(),
            );
        }
    };
    let resp = match sender.send_request(req).await {
        Ok(r) => r,
        Err(e) => {
            return Some((StatusCode::BAD_GATEWAY, format!("peer forward: {e}")).into_response());
        }
    };
    let (parts, incoming) = resp.into_parts();
    let mut out = Response::builder().status(parts.status);
    for (name, value) in parts.headers.iter() {
        if name == http::header::TRANSFER_ENCODING {
            continue;
        }
        out = out.header(name.clone(), value.clone());
    }
    match out.body(Body::new(incoming)) {
        Ok(r) => Some(r),
        Err(e) => Some((StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response()),
    }
}

/// As above but without a request body — used for routes whose handler
/// has already consumed everything except path params.
async fn maybe_forward_get_or_404(
    state: &AppState,
    target_name: &str,
    uri: &Uri,
    headers: &HeaderMap,
) -> Option<Response> {
    maybe_forward_or_404(state, target_name, Method::GET, uri, headers, Body::empty()).await
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
    Hrefs {
        http: http_base,
        ws: ws_base,
        server: server.to_string(),
    }
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
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    Query(params): Query<QueryParams>,
) -> Response {
    let core = state.core.clone();
    let h = build_hrefs(&headers, &uri, &core.name);
    if let Some(ql) = params.ql {
        let devices = core.list_devices().await;
        let filtered = filter_by_ql(&devices, &ql);
        return siren_response(render::render_search_results(&h, &ql, &filtered));
    }
    let _ = params.server;
    let peers = match &state.peer_senders {
        Some(p) => p.names().await,
        None => Vec::new(),
    };
    siren_response(render::render_root(&core, &h, &peers))
}

async fn server_get(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    Query(params): Query<QueryParams>,
) -> Response {
    if let Some(r) = maybe_forward_get_or_404(&state, &name, &uri, &headers).await {
        return r;
    }
    let core = state.core.clone();
    let h = build_hrefs(&headers, &uri, &core.name);
    let devices = core.list_devices().await;
    if let Some(ql) = params.ql {
        let filtered = filter_by_ql(&devices, &ql);
        return siren_response(render::render_search_results(&h, &ql, &filtered));
    }
    siren_response(render::render_server(&h, &devices))
}

async fn devices_get(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Some(r) = maybe_forward_get_or_404(&state, &name, &uri, &headers).await {
        return r;
    }
    let core = state.core.clone();
    let h = build_hrefs(&headers, &uri, &core.name);
    let devices = core.list_devices().await;
    siren_response(render::render_server(&h, &devices))
}

async fn devices_post_stub(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    body: Body,
) -> Response {
    if let Some(r) = maybe_forward_or_404(&state, &name, Method::POST, &uri, &headers, body).await {
        return r;
    }
    (
        StatusCode::NOT_IMPLEMENTED,
        "POST /servers/:name/devices (hubless device registration) is not implemented in v0",
    )
        .into_response()
}

async fn device_get(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Some(r) = maybe_forward_get_or_404(&state, &name, &uri, &headers).await {
        return r;
    }
    let core = state.core.clone();
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
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
    body: String,
) -> Response {
    if name != state.core.name {
        if state.peer_senders.is_some() {
            return maybe_forward_or_404(
                &state,
                &name,
                Method::POST,
                &uri,
                &headers,
                Body::from(body),
            )
            .await
            .unwrap_or_else(|| (StatusCode::NOT_FOUND, "unknown server").into_response());
        }
        return (StatusCode::NOT_FOUND, "unknown server").into_response();
    }
    let core = state.core.clone();
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
        Err(zetta_core::DeviceError::NotAllowed(_)) => (
            StatusCode::CONFLICT,
            "transition not allowed in current state",
        )
            .into_response(),
        Err(zetta_core::DeviceError::Invalid(msg)) => {
            (StatusCode::BAD_REQUEST, msg).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    topic: Option<String>,
}

async fn server_events_stream(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<EventsQuery>,
) -> Response {
    // Peer-forward this same path if the name isn't local.
    if let Some(r) = maybe_forward_get_or_404(&state, &name, &uri, &headers).await {
        return r;
    }
    let topic = match q.topic {
        Some(t) => t,
        None => return (StatusCode::BAD_REQUEST, "missing ?topic=").into_response(),
    };
    let pattern = match zetta_events::TopicPattern::parse(&topic) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("topic: {e}")).into_response(),
    };
    let sub = state
        .core
        .bus
        .subscribe(pattern, zetta_events::SubscribeOpts::default());
    let mut rx = sub.rx;
    let stream = async_stream::stream! {
        while let Some(ev) = rx.recv().await {
            let line = match serde_json::to_string(&serde_json::json!({
                "topic": ev.topic,
                "timestamp": ev.timestamp_ms,
                "data": ev.data,
            })) {
                Ok(s) => s,
                Err(_) => continue,
            };
            yield Ok::<_, std::convert::Infallible>(format!("{line}\n"));
        }
    };
    let body = Body::from_stream(stream);
    let mut resp = Response::new(body);
    resp.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    resp
}

async fn meta_get(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Some(r) = maybe_forward_get_or_404(&state, &name, &uri, &headers).await {
        return r;
    }
    let core = state.core.clone();
    let h = build_hrefs(&headers, &uri, &core.name);
    let devices = core.list_devices().await;
    siren_response(render::render_meta(&h, &devices))
}

async fn meta_type_get(
    State(state): State<AppState>,
    Path((name, ty)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Some(r) = maybe_forward_get_or_404(&state, &name, &uri, &headers).await {
        return r;
    }
    let core = state.core.clone();
    let h = build_hrefs(&headers, &uri, &core.name);
    let devices = core.list_devices().await;
    let dev = devices.iter().find(|d| d.type_ == ty);
    match dev {
        Some(d) => siren_response(
            zetta_siren::Entity::new()
                .with_class("type")
                .with_property("type", JsonValue::String(d.type_.clone()))
                .with_link(zetta_siren::Link::new(
                    zetta_siren::rels::SELF,
                    h.meta_type_url(&d.type_),
                )),
        ),
        None => (StatusCode::NOT_FOUND, "unknown type").into_response(),
    }
}

async fn peer_management_get() -> Response {
    Json(serde_json::json!({
        "class": ["peer-management"],
        "actions": [],
        "entities": [],
        "links": [],
    }))
    .into_response()
}

async fn events_ws(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| crate::ws::handle_socket(socket, state))
}

#[derive(Debug, Deserialize)]
struct PeerQuery {
    #[serde(rename = "connectionId")]
    connection_id: Option<Uuid>,
}

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
async fn initiate_peer(State(state): State<AppState>, Path(id): Path<String>) -> Response {
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

fn filter_by_ql(
    devices: &[crate::core::DeviceSnapshot],
    ql: &str,
) -> Vec<crate::core::DeviceSnapshot> {
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
