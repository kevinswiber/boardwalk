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
use crate::core::{TransitionInput, TransitionOutcome};
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

/// Inputs to the hubless resource registration flow (`POST /resources`).
#[derive(Debug, Clone, Default)]
pub(crate) struct ResourceRegistration {
    pub type_: String,
    pub name: Option<String>,
    pub id: Option<Uuid>,
    pub fields: std::collections::HashMap<String, String>,
}

/// Callback supplied by `boardwalk-server` that consumes a registration,
/// runs the appropriate private adapter factory, registers it with the Core
/// (and the persistent registry), and returns its resource ID.
pub(crate) type ResourceRegistrar = Arc<
    dyn Fn(ResourceRegistration) -> BoxFuture<'static, Result<Uuid, crate::core::DeviceError>>
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
    pub resource_registrar: Option<ResourceRegistrar>,
}

#[allow(dead_code)]
pub fn router(core: Arc<Core>) -> Router {
    router_with(AppState {
        core,
        peer_handler: None,
        peer_init: PeerInitState::default(),
        peer_senders: None,
        peer_streams: super::peer_streams::PeerStreamHub::new(),
        resource_registrar: None,
    })
}

pub(crate) fn router_with(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/resources", get(resources_get).post(resources_post))
        .route("/resources/{id}", get(resource_get))
        .route(
            "/resources/{id}/transitions/{transition}",
            axum::routing::post(resource_transition_post),
        )
        .route("/meta", get(local_meta_get))
        .route("/meta/{type}", get(local_meta_type_get))
        .route("/servers/{name}", get(server_get))
        .route("/servers/{name}/resources", get(server_resources_get))
        .route("/servers/{name}/resources/{id}", get(server_resource_get))
        .route(
            "/servers/{name}/resources/{id}/transitions/{transition}",
            axum::routing::post(server_resource_transition_post),
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

fn unknown_server_response() -> Response {
    (StatusCode::NOT_FOUND, "unknown server").into_response()
}

/// Either dispatch locally or forward to a connected peer by name.
/// Returns `None` only if the name matches the local server (caller
/// serves); returns a response for forwarded requests, unknown peers, or
/// forwarding failures.
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
    let Some(senders) = state.peer_senders.as_ref() else {
        return Some(unknown_server_response());
    };
    let Some(mut sender) = senders.sender(target_name).await else {
        return Some(unknown_server_response());
    };
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
    let bases = forwarded_external_bases(headers, uri, target_name);
    builder = builder
        .header("x-forwarded-host", bases.host)
        .header("x-forwarded-proto", bases.scheme)
        .header("x-boardwalk-external-base", bases.http)
        .header("x-boardwalk-external-ws-base", bases.ws);
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
    if let Some(http_base) = headers
        .get("x-boardwalk-external-base")
        .and_then(|v| v.to_str().ok())
        .and_then(|base| base.parse::<url::Url>().ok())
    {
        let ws_base = headers
            .get("x-boardwalk-external-ws-base")
            .and_then(|v| v.to_str().ok())
            .and_then(|base| base.parse::<url::Url>().ok())
            .unwrap_or_else(|| {
                let mut ws = http_base.clone();
                let _ = ws.set_scheme(if http_base.scheme() == "https" {
                    "wss"
                } else {
                    "ws"
                });
                ws
            });
        return Hrefs {
            http: http_base,
            ws: ws_base,
            server: server.to_string(),
        };
    }
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

struct ForwardedExternalBases {
    host: String,
    scheme: String,
    http: String,
    ws: String,
}

fn forwarded_external_bases(
    headers: &HeaderMap,
    uri: &Uri,
    target_name: &str,
) -> ForwardedExternalBases {
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost")
        .to_string();
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.scheme_str())
        .unwrap_or("http")
        .to_string();
    let ws_scheme = if scheme == "https" { "wss" } else { "ws" };
    let peer = urlencoding::encode(target_name);
    ForwardedExternalBases {
        host: host.clone(),
        scheme: scheme.clone(),
        http: format!("{scheme}://{host}/servers/{peer}/"),
        ws: format!("{ws_scheme}://{host}/servers/{peer}/"),
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

async fn server_resources_get(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    Query(params): Query<QueryParams>,
) -> Response {
    if let Some(r) = maybe_forward_get_or_404(&state, &name, &uri, &headers).await {
        return r;
    }
    resources_response(&state.core, &headers, &uri, params).await
}

async fn server_resource_get(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if let Some(r) = maybe_forward_get_or_404(&state, &name, &uri, &headers).await {
        return r;
    }
    resource_response(&state.core, &headers, &uri, &id).await
}

async fn server_resource_transition_post(
    State(state): State<AppState>,
    Path((name, id, transition)): Path<(String, String, String)>,
    headers: HeaderMap,
    uri: Uri,
    body_bytes: bytes::Bytes,
) -> Response {
    if name != state.core.name {
        return maybe_forward_or_404(
            &state,
            &name,
            Method::POST,
            &uri,
            &headers,
            Body::from(body_bytes),
        )
        .await
        .unwrap_or_else(|| (StatusCode::NOT_FOUND, "unknown server").into_response());
    }
    transition_response(&state.core, &headers, &id, &transition, body_bytes).await
}

async fn resources_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    Query(params): Query<QueryParams>,
) -> Response {
    resources_response(&state.core, &headers, &uri, params).await
}

async fn local_meta_get(State(state): State<AppState>, headers: HeaderMap, uri: Uri) -> Response {
    meta_response(&state.core, &headers, &uri).await
}

async fn local_meta_type_get(
    State(state): State<AppState>,
    Path(ty): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    meta_type_response(&state.core, &headers, &uri, &ty).await
}

async fn resources_response(
    core: &Arc<Core>,
    headers: &HeaderMap,
    uri: &Uri,
    params: QueryParams,
) -> Response {
    let h = build_hrefs(headers, uri, &core.name);
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
    siren_response(render::render_resources(&h, &snaps))
}

async fn resources_post(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
    body_bytes: bytes::Bytes,
) -> Response {
    let Some(registrar) = state.resource_registrar.clone() else {
        return (
            StatusCode::NOT_IMPLEMENTED,
            "no factories registered; call Boardwalk::register_factory(...)",
        )
            .into_response();
    };
    if !is_form_content_type(&headers) {
        return problem_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported-media-type",
            "resource registration must be application/x-www-form-urlencoded",
            Some("content-type"),
        );
    }
    let pairs: Vec<(String, String)> = match serde_urlencoded::from_bytes(&body_bytes) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("bad form: {e}")).into_response(),
    };
    let mut reg = ResourceRegistration::default();
    for (k, v) in pairs {
        match k.as_str() {
            "kind" => reg.type_ = v,
            "name" => reg.name = Some(v),
            "id" => reg.id = Uuid::parse_str(&v).ok(),
            _ => {
                reg.fields.insert(k, v);
            }
        }
    }
    if reg.type_.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing `kind` field").into_response();
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
            "resource missing after register",
        )
            .into_response();
    };
    let rsnap = snap.to_resource_snapshot(&state.core.name);
    let mut resp = siren_response(render::render_resource(&h, &rsnap));
    *resp.status_mut() = StatusCode::CREATED;
    resp.headers_mut().insert(
        http::header::LOCATION,
        HeaderValue::from_str(h.resource_url(&rsnap.id).as_str()).unwrap(),
    );
    resp
}

async fn resource_get(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    resource_response(&state.core, &headers, &uri, &id).await
}

async fn resource_response(core: &Arc<Core>, headers: &HeaderMap, uri: &Uri, id: &str) -> Response {
    let h = build_hrefs(headers, uri, &core.name);
    let id = match uuid::Uuid::parse_str(id) {
        Ok(id) => id,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid resource id").into_response(),
    };
    match core.get_device(&id).await {
        Some(d) => {
            let rsnap = d.to_resource_snapshot(&core.name);
            siren_response(render::render_resource(&h, &rsnap))
        }
        None => (StatusCode::NOT_FOUND, "unknown resource").into_response(),
    }
}

async fn resource_transition_post(
    State(state): State<AppState>,
    Path((id, transition)): Path<(String, String)>,
    headers: HeaderMap,
    body_bytes: bytes::Bytes,
) -> Response {
    transition_response(&state.core, &headers, &id, &transition, body_bytes).await
}

async fn transition_response(
    core: &Arc<Core>,
    headers: &HeaderMap,
    id: &str,
    transition: &str,
    body_bytes: bytes::Bytes,
) -> Response {
    if !is_json_content_type(headers) {
        return problem_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported-media-type",
            "transition input must be application/json",
            Some("content-type"),
        );
    }
    let body: JsonValue = match serde_json::from_slice(&body_bytes) {
        Ok(body) => body,
        Err(err) => {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "invalid-json",
                &err.to_string(),
                Some("body"),
            );
        }
    };
    let fields = match body {
        JsonValue::Object(fields) => fields.into_iter().collect::<BTreeMap<_, _>>(),
        JsonValue::Null => BTreeMap::new(),
        _ => {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "invalid-json",
                "transition input must be a JSON object",
                Some("body"),
            );
        }
    };

    let id = match uuid::Uuid::parse_str(id) {
        Ok(id) => id,
        Err(_) => {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "invalid-resource-id",
                "resource id must be a UUID",
                Some("id"),
            );
        }
    };
    if core.get_device(&id).await.is_none() {
        return problem_response(
            StatusCode::NOT_FOUND,
            "resource-not-found",
            "unknown resource",
            Some("id"),
        );
    }

    let request_ctx = RequestCtx::from_headers(headers);
    match core
        .run_transition(&id, transition, TransitionInput { fields }, request_ctx)
        .await
    {
        Ok(snap) => {
            let rsnap = snap.to_resource_snapshot(&core.name);
            transition_outcome_response(TransitionOutcome::Completed {
                output: None,
                snapshot: rsnap,
            })
        }
        Err(crate::core::DeviceError::NotAllowed(msg)) => problem_response(
            StatusCode::CONFLICT,
            "transition-not-allowed",
            &msg,
            Some("transition"),
        ),
        Err(crate::core::DeviceError::Invalid(msg)) => {
            problem_response(StatusCode::BAD_REQUEST, "invalid-input", &msg, None)
        }
        Err(crate::core::DeviceError::Conflict(msg)) => {
            problem_response(StatusCode::CONFLICT, "conflict", &msg, None)
        }
        Err(crate::core::DeviceError::Internal(msg)) => {
            problem_response(StatusCode::INTERNAL_SERVER_ERROR, "internal", &msg, None)
        }
    }
}

fn transition_outcome_response(outcome: TransitionOutcome) -> Response {
    match outcome {
        TransitionOutcome::Completed { output, snapshot } => Json(serde_json::json!({
            "output": output,
            "snapshot": snapshot.to_query_value(),
        }))
        .into_response(),
        TransitionOutcome::Accepted { job, output } => {
            let status = if job.created {
                StatusCode::CREATED
            } else {
                StatusCode::ACCEPTED
            };
            let location = job.location.clone();
            let location_header = if status == StatusCode::CREATED {
                match HeaderValue::from_str(&location) {
                    Ok(value) => Some(value),
                    Err(_) => {
                        return problem_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "invalid-job-location",
                            "accepted job returned an invalid Location header",
                            Some("job.location"),
                        );
                    }
                }
            } else {
                None
            };
            let body = serde_json::json!({
                "output": output,
                "job": {
                    "id": job.id,
                    "kind": job.kind,
                    "location": location.clone(),
                },
            });
            let mut resp = (status, Json(body)).into_response();
            if let Some(location_header) = location_header {
                resp.headers_mut()
                    .insert(http::header::LOCATION, location_header);
            }
            resp
        }
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .map(str::trim)
                .is_some_and(|mime| mime.eq_ignore_ascii_case("application/json"))
        })
        .unwrap_or(false)
}

fn is_form_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .map(str::trim)
                .is_some_and(|mime| mime.eq_ignore_ascii_case("application/x-www-form-urlencoded"))
        })
        .unwrap_or(false)
}

fn problem_response(
    status: StatusCode,
    error: &str,
    message: &str,
    field: Option<&str>,
) -> Response {
    let mut body = serde_json::json!({
        "error": error,
        "message": message,
    });
    if let Some(field) = field
        && let Some(obj) = body.as_object_mut()
    {
        obj.insert("field".into(), JsonValue::String(field.to_string()));
    }
    let mut resp = (status, Json(body)).into_response();
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/problem+json"),
    );
    resp
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
                // A `Disconnect` overflow on the bus side fires this
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
    meta_response(&state.core, &headers, &uri).await
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
    meta_type_response(&state.core, &headers, &uri, &ty).await
}

async fn meta_response(core: &Arc<Core>, headers: &HeaderMap, uri: &Uri) -> Response {
    let h = build_hrefs(headers, uri, &core.name);
    let devices = core.list_devices().await;
    let types: Vec<render::KindMeta> = devices
        .iter()
        .map(|device| render::KindMeta {
            spec: device.to_actor_spec(),
        })
        .collect();
    siren_response(render::render_meta(&h, &types))
}

async fn meta_type_response(
    core: &Arc<Core>,
    headers: &HeaderMap,
    uri: &Uri,
    ty: &str,
) -> Response {
    let h = build_hrefs(headers, uri, &core.name);
    let devices = core.list_devices().await;
    let Some(spec) = devices
        .iter()
        .find(|device| device.type_ == ty)
        .map(|device| render::KindMeta {
            spec: device.to_actor_spec(),
        })
    else {
        return (StatusCode::NOT_FOUND, "unknown type").into_response();
    };
    siren_response(render::render_meta_type(&h, &spec))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::JobHandle;

    #[tokio::test]
    async fn accepted_created_transition_sets_201_location_and_job_body() {
        let resp = transition_outcome_response(TransitionOutcome::Accepted {
            job: JobHandle {
                id: "job-1".into(),
                kind: "job".into(),
                location: "/resources/job-1".into(),
                created: true,
            },
            output: Some(serde_json::json!({"queued": true})),
        });

        assert_eq!(resp.status(), StatusCode::CREATED);
        assert_eq!(
            resp.headers()
                .get(http::header::LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some("/resources/job-1")
        );
        let body = response_json(resp).await;
        assert_eq!(body["job"]["id"], "job-1");
        assert_eq!(body["job"]["kind"], "job");
        assert_eq!(body["job"]["location"], "/resources/job-1");
        assert_eq!(body["output"], serde_json::json!({"queued": true}));
    }

    #[tokio::test]
    async fn accepted_existing_transition_sets_202_without_location() {
        let resp = transition_outcome_response(TransitionOutcome::Accepted {
            job: JobHandle {
                id: "job-1".into(),
                kind: "job".into(),
                location: "/resources/job-1".into(),
                created: false,
            },
            output: None,
        });

        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert!(!resp.headers().contains_key(http::header::LOCATION));
        let body = response_json(resp).await;
        assert_eq!(body["job"]["location"], "/resources/job-1");
        assert_eq!(body["output"], JsonValue::Null);
    }

    #[tokio::test]
    async fn accepted_created_transition_rejects_invalid_location_header() {
        let resp = transition_outcome_response(TransitionOutcome::Accepted {
            job: JobHandle {
                id: "job-1".into(),
                kind: "job".into(),
                location: "/resources/job-1\nx".into(),
                created: true,
            },
            output: None,
        });

        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = response_json(resp).await;
        assert_eq!(body["error"], "invalid-job-location");
        assert_eq!(body["field"], "job.location");
    }

    async fn response_json(resp: Response) -> JsonValue {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}
