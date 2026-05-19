use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, Request, State, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use super::core::{Core, now_ms};
use super::render::{self, Hrefs};
use crate::core::TransitionInput;
use crate::runtime::RequestCtx;
use crate::siren::SIREN_CONTENT_TYPE;

/// Callback invoked after a successful peer WS upgrade. The runtime
/// supplies this when peering is enabled.
pub(crate) type PeerHandler =
    Arc<dyn Fn(String, Uuid, hyper::upgrade::Upgraded) -> BoxFuture<'static, ()> + Send + Sync>;

/// Cloud-side handle into the live HTTP/2 sender for each connected
/// peer. The runtime (boardwalk-peer) implements this; the HTTP router
/// uses it to forward `/servers/{peer-name}/...` requests through the
/// established tunnel.
#[async_trait::async_trait]
pub trait PeerSenders: Send + Sync + 'static {
    async fn sender(
        &self,
        name: &str,
    ) -> Option<hyper::client::conn::http2::SendRequest<axum::body::Body>>;
    async fn names(&self) -> Vec<String>;
    /// Check whether a peer is currently connected or mid-handshake.
    /// Default consults `names()`; impls (e.g. `PeerAcceptors`) can
    /// override to also see pending peers.
    async fn has_active_peer(&self, name: &str) -> bool {
        self.names().await.iter().any(|n| n == name)
    }
}

/// Inputs to the hubless device registration flow
/// (`POST /servers/{name}/devices`).
#[derive(Debug, Clone, Default)]
pub(crate) struct DeviceRegistration {
    pub type_: String,
    pub name: Option<String>,
    pub id: Option<Uuid>,
    pub fields: std::collections::HashMap<String, String>,
}

/// Callback supplied by `boardwalk-server` that consumes a registration,
/// runs the appropriate factory, registers the device with the Core
/// (and the persistent registry), and returns its ID.
pub(crate) type DeviceRegistrar = Arc<
    dyn Fn(DeviceRegistration) -> BoxFuture<'static, Result<Uuid, crate::core::DeviceError>>
        + Send
        + Sync,
>;

/// Per-server in-flight peer-confirmation state, keyed by connection id.
/// Populated when a `PeerClient` (initiator) is mid-handshake and
/// drained when the acceptor's `GET /_initiate_peer/{id}` request lands.
#[derive(Clone, Default)]
pub(crate) struct PeerInitState {
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
pub(crate) struct AppState {
    pub core: Arc<Core>,
    pub peer_handler: Option<PeerHandler>,
    pub peer_init: PeerInitState,
    pub peer_senders: Option<Arc<dyn PeerSenders>>,
    pub peer_streams: super::peer_streams::PeerStreamHub,
    pub device_registrar: Option<DeviceRegistrar>,
}

pub fn router(core: Arc<Core>) -> Router {
    router_with(AppState {
        core,
        peer_handler: None,
        peer_init: PeerInitState::default(),
        peer_senders: None,
        peer_streams: super::peer_streams::PeerStreamHub::new(),
        device_registrar: None,
    })
}

pub(crate) fn router_with(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/servers/{name}", get(server_get))
        .route(
            "/servers/{name}/devices",
            get(devices_get).post(devices_post),
        )
        .route(
            "/servers/{name}/devices/{id}",
            get(device_get).post(device_post),
        )
        .route("/servers/{name}/meta", get(meta_get))
        .route("/servers/{name}/meta/{type}", get(meta_type_get))
        .route("/servers/{name}/events", get(server_events_stream))
        .route(
            "/servers/{name}/events/unsubscribe",
            axum::routing::post(events_unsubscribe),
        )
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
        "http://{}.peer.boardwalk.invalid{}",
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
    tracing::debug!(
        peer = %target_name,
        method = %req.method(),
        path = %path_and_query,
        "forwarding request to peer"
    );
    let resp = match sender.send_request(req).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                peer = %target_name,
                error = %e,
                "peer forward send failed"
            );
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

fn siren_response(entity: crate::siren::Entity) -> Response {
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
        return match filter_by_ql(&devices, &ql, &core.name) {
            Ok(filtered) => {
                let snaps: Vec<_> = filtered
                    .iter()
                    .map(|d| d.to_resource_snapshot(&core.name))
                    .collect();
                siren_response(render::render_search_results(&h, &ql, &snaps))
            }
            Err(e) => query_error_response(&ql, &e),
        };
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
        return match filter_by_ql(&devices, &ql, &core.name) {
            Ok(filtered) => {
                let snaps: Vec<_> = filtered
                    .iter()
                    .map(|d| d.to_resource_snapshot(&core.name))
                    .collect();
                siren_response(render::render_search_results(&h, &ql, &snaps))
            }
            Err(e) => query_error_response(&ql, &e),
        };
    }
    let snaps: Vec<_> = devices
        .iter()
        .map(|d| d.to_resource_snapshot(&core.name))
        .collect();
    siren_response(render::render_server(&h, &snaps))
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
    let snaps: Vec<_> = devices
        .iter()
        .map(|d| d.to_resource_snapshot(&core.name))
        .collect();
    siren_response(render::render_server(&h, &snaps))
}

async fn devices_post(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    body_bytes: bytes::Bytes,
) -> Response {
    // If forwarding to a peer, do it now (we still have the body in bytes).
    if name != state.core.name {
        let body = Body::from(body_bytes);
        return maybe_forward_or_404(&state, &name, Method::POST, &uri, &headers, body)
            .await
            .unwrap_or_else(|| (StatusCode::NOT_FOUND, "unknown server").into_response());
    }
    let Some(registrar) = state.device_registrar.clone() else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            "no factories registered; call Boardwalk::register_factory(...)",
        )
            .into_response();
    };
    let pairs: Vec<(String, String)> = match serde_urlencoded::from_bytes(&body_bytes) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("bad form: {e}")).into_response(),
    };
    let mut reg = DeviceRegistration::default();
    for (k, v) in pairs {
        match k.as_str() {
            "type" => reg.type_ = v,
            "name" => reg.name = Some(v),
            "id" => reg.id = Uuid::parse_str(&v).ok(),
            _ => {
                reg.fields.insert(k, v);
            }
        }
    }
    if reg.type_.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing `type` field").into_response();
    }
    let new_id = match registrar(reg).await {
        Ok(id) => id,
        Err(crate::core::DeviceError::Invalid(msg)) => {
            return (StatusCode::BAD_REQUEST, msg).into_response();
        }
        Err(crate::core::DeviceError::Conflict(msg)) => {
            return (StatusCode::CONFLICT, msg).into_response();
        }
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    };
    let h = build_hrefs(&headers, &uri, &state.core.name);
    let Some(snap) = state.core.get_device(&new_id).await else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "device missing after register",
        )
            .into_response();
    };
    let rsnap = snap.to_resource_snapshot(&state.core.name);
    let mut resp = siren_response(render::render_device(&h, &rsnap, &snap.config));
    *resp.status_mut() = StatusCode::CREATED;
    resp
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
        Some(d) => {
            let rsnap = d.to_resource_snapshot(&core.name);
            siren_response(render::render_device(&h, &rsnap, &d.config))
        }
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
    let request_ctx = RequestCtx::from_headers(&headers);
    match core
        .run_transition(&id, &action_name, input, request_ctx)
        .await
    {
        Ok(snap) => {
            let rsnap = snap.to_resource_snapshot(&core.name);
            siren_response(render::render_device(&h, &rsnap, &snap.config))
        }
        Err(crate::core::DeviceError::NotAllowed(_)) => (
            StatusCode::CONFLICT,
            "transition not allowed in current state",
        )
            .into_response(),
        Err(crate::core::DeviceError::Invalid(msg)) => {
            (StatusCode::BAD_REQUEST, msg).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    topic: Option<String>,
    #[serde(rename = "outboundCapacity")]
    outbound_capacity: Option<usize>,
}

/// `POST /servers/{name}/events/unsubscribe` — protocol-parity route.
/// The original SPDY peer protocol used this to cancel a long-body
/// event stream. Under HTTP/2 the cleaner pattern is dropping the
/// streaming response (which triggers RST_STREAM), so this route is
/// retained for compatibility but is intentionally a stub. Returns
/// 202 Accepted and otherwise does nothing.
async fn events_unsubscribe(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    body: Body,
) -> Response {
    // Forward to a peer if the name is remote.
    if name != state.core.name {
        return maybe_forward_or_404(&state, &name, Method::POST, &uri, &headers, body)
            .await
            .unwrap_or_else(|| (StatusCode::NOT_FOUND, "unknown server").into_response());
    }
    // Local: accept and ignore. (Subscriptions cancel via WS unsubscribe
    // or by RST_STREAM on the streaming response body.)
    (StatusCode::ACCEPTED, "").into_response()
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
    let pattern = match crate::events::TopicPattern::parse(&topic) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("topic: {e}")).into_response(),
    };
    let sub = state.core.bus.subscribe(
        pattern,
        crate::events::SubscribeOpts {
            outbound_capacity: q.outbound_capacity,
            ..Default::default()
        },
    );
    let bus_for_guard = state.core.bus.clone();
    let sub_id = sub.id;
    let mut rx = sub.rx;
    let mut slow_consumer_rx = sub.slow_consumer_rx;
    // Drop guard: when the response body is dropped (client
    // disconnect, axum tear-down, etc.), `_guard.drop()` runs and
    // eagerly calls `bus.unsubscribe(id)`. Without this, the bus
    // only prunes the subscription on the next `try_publish` that
    // notices the closed receiver.
    struct UnsubOnDrop {
        bus: crate::events::EventBus,
        id: crate::events::SubscriptionId,
    }
    impl Drop for UnsubOnDrop {
        fn drop(&mut self) {
            self.bus.unsubscribe(self.id);
        }
    }
    let stream = async_stream::stream! {
        let _guard = UnsubOnDrop { bus: bus_for_guard, id: sub_id };
        loop {
            tokio::select! {
                biased;
                // A `Lossless` overflow on the bus side fires this
                // notice. Emit a final structured `stream-gap` line and
                // close the response so the client sees the contract
                // (a gap with the cause) instead of an unexplained EOF.
                notice = &mut slow_consumer_rx => {
                    if let Ok(n) = notice {
                        let line = serde_json::to_string(&serde_json::json!({
                            "type": "stream-gap",
                            "timestamp": now_ms(),
                            "streamId": n.stream_id.as_ref().map(|s| s.as_str()),
                            "lastDeliveredSequence": n.last_delivered_sequence,
                            "reason": n.reason,
                            "terminated": true,
                        }))
                        .unwrap_or_default();
                        if !line.is_empty() {
                            yield Ok::<_, std::convert::Infallible>(format!("{line}\n"));
                        }
                    }
                    break;
                }
                env = rx.recv() => {
                    let Some(ev) = env else { break };
                    let iso = ev
                        .timestamp
                        .format(&time::format_description::well_known::Rfc3339)
                        .ok();
                    let line = match serde_json::to_string(&serde_json::json!({
                        "topic": ev.topic(),
                        "timestamp": ev.timestamp_ms(),
                        "data": ev.data,
                        "eventId": ev.event_id.as_str(),
                        "streamId": ev.stream_id.as_str(),
                        "sequence": ev.sequence,
                        "nodeId": ev.node_id.as_str(),
                        "resourceId": ev.resource_id,
                        "resourceKind": ev.resource_kind,
                        "payloadKind": ev.payload_kind,
                        "payloadVersion": ev.payload_version,
                        "envelopeVersion": ev.envelope_version,
                        "isoTimestamp": iso,
                    })) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };
                    yield Ok::<_, std::convert::Infallible>(format!("{line}\n"));
                }
            }
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
    let snaps: Vec<_> = devices
        .iter()
        .map(|d| d.to_resource_snapshot(&core.name))
        .collect();
    // Metadata describes the kind, not the instance — gather the full
    // transition and stream surfaces from `DeviceConfig` instead of
    // the snapshot's state-dependent `affordances.available` lists.
    let types: Vec<render::TypeMeta> = devices
        .iter()
        .zip(snaps.iter())
        .map(|(d, snap)| render::TypeMeta {
            snap,
            all_transitions: d.config.transitions.keys().cloned().collect(),
            all_streams: d.config.streams.iter().map(|s| s.name.clone()).collect(),
        })
        .collect();
    siren_response(render::render_meta(&h, &types))
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
            crate::siren::Entity::new()
                .with_class("type")
                .with_property("type", JsonValue::String(d.type_.clone()))
                .with_link(crate::siren::Link::new(
                    crate::siren::rels::SELF,
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

/// Wire-level subprotocol token for the multiplex event WS.
pub const EVENTS_SUBPROTOCOL: &str = "boardwalk-events/1";

async fn events_ws(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    // Negotiate the protocol token. If the client doesn't offer it,
    // axum serves the upgrade without a Sec-WebSocket-Protocol header
    // (backward-compatible).
    ws.protocols([EVENTS_SUBPROTOCOL])
        .on_upgrade(move |socket| super::ws::handle_socket(socket, state))
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

    // Reject duplicate peer names. Two hubs with the same name landing
    // on the same cloud is a config error — fail fast with 409.
    if let Some(senders) = &state.peer_senders
        && senders.has_active_peer(&peer_name).await
    {
        return (
            StatusCode::CONFLICT,
            format!("peer `{peer_name}` is already connected"),
        )
            .into_response();
    }

    // Build 101 from request headers.
    let upgrade_response = match boardwalk_tunnel_upgrade_response(req.headers()) {
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

/// Reuse `crate::tunnel::build_upgrade_response`.
fn boardwalk_tunnel_upgrade_response(headers: &HeaderMap) -> Result<Response, String> {
    let resp = crate::tunnel::build_upgrade_response(headers).map_err(|e| format!("{e}"))?;
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
    devices: &[super::core::DeviceSnapshot],
    ql: &str,
    node_name: &str,
) -> Result<Vec<super::core::DeviceSnapshot>, crate::query::QueryError> {
    let q = crate::caql::parse(ql)?;
    let mut out = Vec::with_capacity(devices.len());
    for d in devices {
        let snap = d.to_resource_snapshot(node_name);
        if crate::query::matches(&q, &snap.to_query_value())? {
            out.push(d.clone());
        }
    }
    Ok(out)
}

fn query_error_response(ql: &str, e: &crate::query::QueryError) -> Response {
    let body = serde_json::json!({
        "error": "query-parse-error",
        "message": e.to_string(),
        "ql": ql,
    });
    let mut resp = (StatusCode::BAD_REQUEST, Json(body)).into_response();
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/problem+json"),
    );
    resp
}
