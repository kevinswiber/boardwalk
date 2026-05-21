use std::collections::{BTreeMap, HashMap, HashSet};
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

use super::core::{Core, ResourceReadError, ResourceTransitionError, now_ms};
use super::render::{self, Hrefs};
use crate::events::{NodeId, Segment, SlowConsumerPolicy, StreamId, SubscribeOpts, TopicPattern};
use crate::peer::{AdmittedPeerConnection, PeerAdmissionConfig, PeerCapabilities};
use crate::query::{FieldPath, QueryScope};
use crate::runtime::{RequestCtx, TransitionInput, TransitionOutcome};
use crate::siren::SIREN_CONTENT_TYPE;

const RENDER_CAPABILITIES_HEADER: &str = "x-boardwalk-render-capabilities";

/// Callback invoked after a successful peer WS upgrade. The runtime
/// supplies this when peering is enabled.
pub(crate) type PeerHandler = Arc<
    dyn Fn(AdmittedPeerConnection, hyper::upgrade::Upgraded) -> BoxFuture<'static, ()>
        + Send
        + Sync,
>;

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
    async fn peer_context(&self, _name: &str) -> Option<AdmittedPeerConnection> {
        None
    }
    /// Check whether a peer is currently connected or mid-handshake.
    /// Default consults `names()`; impls (e.g. `PeerAcceptors`) can
    /// override to also see pending peers.
    async fn has_active_peer(&self, name: &str) -> bool {
        self.names().await.iter().any(|n| n == name)
    }
}

/// Parsed inputs to runtime resource registration (`POST /resources`).
#[derive(Debug, Clone, Default)]
pub(crate) struct ResourceRegistration {
    pub kind: String,
    pub name: Option<String>,
    pub id: Option<Uuid>,
    pub fields: HashMap<String, String>,
}

/// Failure modes for actor-native runtime resource registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResourceRegistrationError {
    Invalid(String),
    Conflict(String),
    Internal(String),
}

/// Callback supplied by the server builder that turns a registration
/// request into a live resource actor and returns its assigned
/// resource id.
pub(crate) type ResourceRegistrar = Arc<
    dyn Fn(ResourceRegistration) -> BoxFuture<'static, Result<String, ResourceRegistrationError>>
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
    pub peer_admission: Arc<Vec<PeerAdmissionConfig>>,
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
        peer_admission: Arc::new(Vec::new()),
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
    let Some(intent) = ProxyIntent::from_parts(method.clone(), uri.path(), uri.query()) else {
        return Some(unknown_server_response());
    };
    let Some(senders) = state.peer_senders.as_ref() else {
        return Some(unknown_server_response());
    };
    let Some(context) = senders.peer_context(target_name).await else {
        return Some(unknown_server_response());
    };
    if !context
        .negotiated_capabilities
        .contains(intent.required_capability())
    {
        return Some((StatusCode::FORBIDDEN, "peer capability denied").into_response());
    }
    let Some(mut sender) = senders.sender(target_name).await else {
        return Some(unknown_server_response());
    };
    let req = match build_peer_forward_request(
        &state.core.name,
        target_name,
        method,
        uri,
        headers,
        body,
        context.negotiated_capabilities,
    ) {
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
        uri = %req.uri(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProxyIntent {
    Read,
    List,
    Query,
    Subscribe,
    Invoke,
    Register,
    Metadata,
    Admin,
}

impl ProxyIntent {
    pub(crate) fn from_parts(method: Method, path: &str, query: Option<&str>) -> Option<Self> {
        let segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let ["servers", peer, rest @ ..] = segments.as_slice() else {
            return None;
        };
        if peer.is_empty() {
            return None;
        }

        match (method, rest) {
            (Method::GET, []) => Some(Self::read_or_query(query)),
            (Method::GET, ["resources"]) => Some(Self::list_or_query(query)),
            (Method::POST, ["resources"]) => Some(Self::Register),
            (Method::GET, ["resources", resource_id]) if !resource_id.is_empty() => {
                Some(Self::Read)
            }
            (Method::POST, ["resources", resource_id, "transitions", transition])
                if !resource_id.is_empty() && !transition.is_empty() =>
            {
                Some(Self::Invoke)
            }
            (Method::GET, ["meta"]) | (Method::GET, ["meta", _]) => Some(Self::Metadata),
            (Method::GET, ["events"]) if has_query_param(query, "topic") => Some(Self::Subscribe),
            (Method::POST, ["events", "unsubscribe"]) => Some(Self::Subscribe),
            (Method::GET, ["peer-management"]) => Some(Self::Admin),
            _ => None,
        }
    }

    fn read_or_query(query: Option<&str>) -> Self {
        if has_query_param(query, "ql") {
            Self::Query
        } else {
            Self::Read
        }
    }

    fn list_or_query(query: Option<&str>) -> Self {
        if has_query_param(query, "ql") {
            Self::Query
        } else {
            Self::List
        }
    }

    fn required_capability(self) -> PeerCapabilities {
        match self {
            Self::Read | Self::List | Self::Metadata => PeerCapabilities::resource_read(),
            Self::Query => PeerCapabilities::resource_query(),
            Self::Subscribe => PeerCapabilities::stream_subscribe_capability(),
            Self::Invoke => PeerCapabilities::transition_invoke(),
            Self::Register => PeerCapabilities::resource_register(),
            Self::Admin => PeerCapabilities::peer_admin(),
        }
    }
}

fn has_query_param(query: Option<&str>, key: &str) -> bool {
    query.is_some_and(|query| {
        url::form_urlencoded::parse(query.as_bytes()).any(|(name, _)| name == key)
    })
}

fn build_peer_forward_request(
    gateway_name: &str,
    target_name: &str,
    method: Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Body,
    render_capabilities: PeerCapabilities,
) -> Result<http::Request<Body>, http::Error> {
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
    let connection_headers = connection_header_tokens(headers);
    for (name, value) in sanitize_forward_headers(headers, &connection_headers) {
        builder = builder.header(name.clone(), value.clone());
    }
    let bases = forwarded_external_bases(headers, uri, target_name);
    builder = builder
        .header("x-forwarded-host", bases.host)
        .header("x-forwarded-proto", bases.scheme)
        .header("x-boardwalk-external-base", bases.http)
        .header("x-boardwalk-external-ws-base", bases.ws)
        .header("x-boardwalk-forwarded-by", gateway_name)
        .header(RENDER_CAPABILITIES_HEADER, render_capabilities.to_string())
        .header("x-boardwalk-correlation-id", Uuid::new_v4().to_string());
    builder.body(body)
}

fn sanitize_forward_headers<'a>(
    headers: &'a HeaderMap,
    connection_headers: &'a HashSet<String>,
) -> impl Iterator<Item = (&'a http::HeaderName, &'a HeaderValue)> {
    headers
        .iter()
        .filter(move |(name, _)| should_forward_header(name, connection_headers))
}

fn should_forward_header(name: &http::HeaderName, connection_headers: &HashSet<String>) -> bool {
    if name == http::header::HOST {
        return false;
    }
    let name = name.as_str().to_ascii_lowercase();
    if connection_headers.contains(&name) {
        return false;
    }
    if matches!(
        name.as_str(),
        "authorization"
            | "cookie"
            | "connection"
            | "forwarded"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    ) {
        return false;
    }
    if name.starts_with("proxy-")
        || name.starts_with("x-forwarded-")
        || name.starts_with("x-boardwalk-")
        || name.starts_with("sec-websocket-")
    {
        return false;
    }
    true
}

fn connection_header_tokens(headers: &HeaderMap) -> HashSet<String> {
    headers
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| !token.is_empty())
        .collect()
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

fn render_policy_from_headers(headers: &HeaderMap) -> render::RenderPolicy {
    let Some(value) = headers.get(RENDER_CAPABILITIES_HEADER) else {
        return render::RenderPolicy::local();
    };
    let Ok(value) = value.to_str() else {
        return render::RenderPolicy::from_capabilities(PeerCapabilities::empty());
    };
    let capabilities =
        PeerCapabilities::parse_list(value).unwrap_or_else(|_| PeerCapabilities::empty());
    render::RenderPolicy::from_capabilities(capabilities)
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
    let scheme = uri.scheme_str().unwrap_or("http").to_string();
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
    let policy = render_policy_from_headers(&headers);
    let scope = query_scope_from_server(params.server.as_deref());
    if let Some(ql) = params.ql {
        return match scope {
            QueryScope::Local => match core.query_resources(&ql).await {
                Ok(snaps) => siren_response(render::render_search_results(&h, &ql, &snaps, policy)),
                Err(e) => query_error_response(&ql, &e),
            },
            QueryScope::Peer(peer) => {
                let peer_uri = peer_server_query_uri(&peer, Some(&ql));
                if let Some(response) =
                    maybe_forward_get_or_404(&state, &peer, &peer_uri, &headers).await
                {
                    response
                } else {
                    let h = build_hrefs(&headers, &peer_uri, &core.name);
                    match core.query_resources(&ql).await {
                        Ok(snaps) => {
                            siren_response(render::render_search_results(&h, &ql, &snaps, policy))
                        }
                        Err(e) => query_error_response(&ql, &e),
                    }
                }
            }
            QueryScope::Federation { .. } => unsupported_federation_query_response(),
        };
    }
    match scope {
        QueryScope::Local => {}
        QueryScope::Peer(peer) => {
            let peer_uri = peer_server_query_uri(&peer, None);
            if let Some(response) =
                maybe_forward_get_or_404(&state, &peer, &peer_uri, &headers).await
            {
                return response;
            }
        }
        QueryScope::Federation { .. } => return unsupported_federation_query_response(),
    }
    let peers = match &state.peer_senders {
        Some(p) => peer_render_policies(p).await,
        None => Vec::new(),
    };
    siren_response(render::render_root(&core, &h, &peers, policy))
}

fn query_scope_from_server(server: Option<&str>) -> QueryScope {
    match server.map(str::trim).filter(|server| !server.is_empty()) {
        None => QueryScope::Local,
        Some("*") => QueryScope::Federation {
            peers: Vec::new(),
            include_local: true,
        },
        Some(peer) => QueryScope::Peer(peer.to_string()),
    }
}

fn peer_server_query_uri(peer: &str, ql: Option<&str>) -> Uri {
    let mut path = format!("/servers/{}", urlencoding::encode(peer));
    if let Some(ql) = ql {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("ql", ql);
        path.push('?');
        path.push_str(&query.finish());
    }
    path.parse().expect("peer server query URI")
}

fn unsupported_federation_query_response() -> Response {
    problem_response(
        StatusCode::BAD_REQUEST,
        "unsupported-federation-query",
        "federated query requires explicit policy and limits",
        Some("server"),
    )
}

async fn peer_render_policies(senders: &Arc<dyn PeerSenders>) -> Vec<render::PeerRenderPolicy> {
    let names = senders.names().await;
    let mut peers = Vec::with_capacity(names.len());
    for name in names {
        if let Some(context) = senders.peer_context(&name).await {
            peers.push(render::PeerRenderPolicy {
                route_name: name,
                capabilities: context.negotiated_capabilities,
            });
        }
    }
    peers
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
    let policy = render_policy_from_headers(&headers);
    if let Some(ql) = params.ql {
        return match core.query_resources(&ql).await {
            Ok(snaps) => siren_response(render::render_search_results(&h, &ql, &snaps, policy)),
            Err(e) => query_error_response(&ql, &e),
        };
    }
    let snaps = core.list_resources().await;
    siren_response(render::render_server(&h, &snaps, policy))
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
    let policy = render_policy_from_headers(headers);
    if let Some(ql) = params.ql {
        return match core.query_resources(&ql).await {
            Ok(snaps) => siren_response(render::render_search_results(&h, &ql, &snaps, policy)),
            Err(e) => query_error_response(&ql, &e),
        };
    }
    let snaps = core.list_resources().await;
    siren_response(render::render_resources(&h, &snaps, policy))
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
            "runtime resource registration is not enabled",
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
            "kind" => reg.kind = v,
            "name" => reg.name = Some(v),
            "id" => reg.id = Uuid::parse_str(&v).ok(),
            _ => {
                reg.fields.insert(k, v);
            }
        }
    }
    if reg.kind.is_empty() {
        return (StatusCode::BAD_REQUEST, "missing `kind` field").into_response();
    }
    let new_id = match registrar(reg).await {
        Ok(id) => id,
        Err(ResourceRegistrationError::Invalid(msg)) => {
            return (StatusCode::BAD_REQUEST, msg).into_response();
        }
        Err(ResourceRegistrationError::Conflict(msg)) => {
            return (StatusCode::CONFLICT, msg).into_response();
        }
        Err(ResourceRegistrationError::Internal(msg)) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
        }
    };
    let h = build_hrefs(&headers, &uri, &state.core.name);
    let snap = match state.core.get_resource(&new_id).await {
        Ok(Some(snap)) => snap,
        Ok(None) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "resource missing after register",
            )
                .into_response();
        }
        Err(ResourceReadError::NotFound) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "resource missing after register",
            )
                .into_response();
        }
        Err(ResourceReadError::Unavailable(msg)) => {
            return problem_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "resource-unavailable",
                &msg,
                Some("id"),
            );
        }
        Err(ResourceReadError::Internal(msg)) => {
            return problem_response(StatusCode::INTERNAL_SERVER_ERROR, "internal", &msg, None);
        }
    };
    let policy = render_policy_from_headers(&headers);
    let mut resp = siren_response(render::render_resource(&h, &snap, policy));
    *resp.status_mut() = StatusCode::CREATED;
    resp.headers_mut().insert(
        http::header::LOCATION,
        HeaderValue::from_str(h.resource_url(&snap.id).as_str()).unwrap(),
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
    let policy = render_policy_from_headers(headers);
    match core.get_resource(id).await {
        Ok(Some(snapshot)) => siren_response(render::render_resource(&h, &snapshot, policy)),
        Ok(None) => (StatusCode::NOT_FOUND, "unknown resource").into_response(),
        Err(ResourceReadError::NotFound) => {
            (StatusCode::NOT_FOUND, "unknown resource").into_response()
        }
        Err(ResourceReadError::Unavailable(msg)) => problem_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "resource-unavailable",
            &msg,
            None,
        ),
        Err(ResourceReadError::Internal(msg)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }
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

    let request_ctx = RequestCtx::from_headers(headers);
    match core
        .run_resource_transition(id, transition, TransitionInput { fields }, request_ctx)
        .await
    {
        Ok(outcome) => transition_outcome_response(outcome),
        Err(ResourceTransitionError::NotFound) => problem_response(
            StatusCode::NOT_FOUND,
            "resource-not-found",
            "unknown resource",
            Some("id"),
        ),
        Err(ResourceTransitionError::NotAllowed(msg)) => problem_response(
            StatusCode::CONFLICT,
            "transition-not-allowed",
            &msg,
            Some("transition"),
        ),
        Err(ResourceTransitionError::InvalidInput(msg)) => {
            problem_response(StatusCode::BAD_REQUEST, "invalid-input", &msg, None)
        }
        Err(ResourceTransitionError::Conflict(msg)) => {
            problem_response(StatusCode::CONFLICT, "conflict", &msg, None)
        }
        Err(ResourceTransitionError::Busy) => problem_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "busy",
            "resource actor is busy",
            Some("transition"),
        ),
        Err(ResourceTransitionError::BackpressureRequired) => problem_response(
            StatusCode::TOO_MANY_REQUESTS,
            "backpressure-required",
            "resource actor requires caller backpressure",
            Some("transition"),
        ),
        Err(ResourceTransitionError::Timeout) => problem_response(
            StatusCode::GATEWAY_TIMEOUT,
            "timeout",
            "resource actor transition timed out",
            Some("transition"),
        ),
        Err(ResourceTransitionError::Unavailable(msg)) => problem_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "resource-unavailable",
            &msg,
            Some("id"),
        ),
        Err(ResourceTransitionError::Internal(msg)) => {
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
    #[serde(default)]
    replay: bool,
    #[serde(rename = "slowConsumerPolicy")]
    slow_consumer_policy: Option<String>,
    #[serde(rename = "coalesceKey")]
    coalesce_key: Option<String>,
}

fn stream_id_from_concrete_pattern(pattern: &TopicPattern) -> Option<StreamId> {
    let [node, kind, resource, stream] = pattern.segments.as_slice() else {
        return None;
    };
    let (
        Segment::Literal(node),
        Segment::Literal(kind),
        Segment::Literal(resource),
        Segment::Literal(stream),
    ) = (node, kind, resource, stream)
    else {
        return None;
    };
    if node.is_empty() || kind.is_empty() || resource.is_empty() || stream.is_empty() {
        return None;
    }
    Some(StreamId::for_resource(&NodeId::new(node), resource, stream))
}

fn subscribe_opts_from_query(query: &EventsQuery) -> Result<SubscribeOpts, String> {
    let policy_name = query
        .slow_consumer_policy
        .as_deref()
        .unwrap_or("disconnect")
        .replace('_', "-")
        .to_ascii_lowercase();
    let slow_consumer_policy = match policy_name.as_str() {
        "disconnect" => {
            if query.coalesce_key.is_some() {
                return Err("coalesceKey requires slowConsumerPolicy=coalesce".into());
            }
            SlowConsumerPolicy::Disconnect
        }
        "backpressure" => {
            if query.coalesce_key.is_some() {
                return Err("coalesceKey requires slowConsumerPolicy=coalesce".into());
            }
            SlowConsumerPolicy::Backpressure
        }
        "drop-newest" | "dropnewest" => {
            if query.coalesce_key.is_some() {
                return Err("coalesceKey requires slowConsumerPolicy=coalesce".into());
            }
            SlowConsumerPolicy::DropNewest
        }
        "coalesce" => {
            let key = query
                .coalesce_key
                .as_deref()
                .ok_or_else(|| "slowConsumerPolicy=coalesce requires coalesceKey".to_string())?;
            let key_path = FieldPath::try_parse(key)
                .map_err(|err| format!("invalid coalesceKey `{key}`: {err}"))?;
            SlowConsumerPolicy::Coalesce { key_path }
        }
        _ => {
            return Err(format!(
                "unknown slowConsumerPolicy `{}`; expected disconnect, backpressure, drop-newest, or coalesce",
                query.slow_consumer_policy.as_deref().unwrap_or_default()
            ));
        }
    };
    Ok(SubscribeOpts {
        outbound_capacity: query.outbound_capacity,
        slow_consumer_policy,
        ..Default::default()
    })
}

#[cfg(test)]
mod event_route_query_tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    use super::*;
    use crate::runtime::NodeBuilder;

    fn empty_core(name: &str) -> Arc<Core> {
        Core::from_node(Arc::new(NodeBuilder::new(name).build()))
    }

    fn events_query(topic: &str) -> EventsQuery {
        EventsQuery {
            topic: Some(topic.to_string()),
            outbound_capacity: None,
            replay: false,
            slow_consumer_policy: None,
            coalesce_key: None,
        }
    }

    #[test]
    fn replay_stream_id_requires_exact_concrete_resource_stream_topic() {
        let pattern = TopicPattern::parse("hub/job/job-1/progress").expect("concrete topic parses");
        let stream_id = stream_id_from_concrete_pattern(&pattern)
            .expect("four-segment resource stream topic resolves");
        assert_eq!(
            stream_id.as_str(),
            "bw://hub/resources/job-1/streams/progress"
        );

        for topic in [
            "hub/job/job-1/progress/extra",
            "hub/job/job-1",
            "hub/job//progress",
            "hub/job/*/progress",
            "hub/**/progress",
        ] {
            let pattern = TopicPattern::parse(topic).expect("topic parses");
            assert!(
                stream_id_from_concrete_pattern(&pattern).is_none(),
                "`{topic}` should not resolve as a replayable concrete topic"
            );
        }
    }

    #[test]
    fn ndjson_subscribe_policy_defaults_to_disconnect() {
        let query = events_query("hub/job/job-1/progress");
        let opts = subscribe_opts_from_query(&query).expect("default opts");

        assert!(matches!(
            opts.slow_consumer_policy,
            SlowConsumerPolicy::Disconnect
        ));
    }

    #[test]
    fn ndjson_subscribe_policy_is_explicit_not_topic_derived() {
        let mut progress = events_query("hub/job/job-1/progress");
        progress.outbound_capacity = Some(4);
        let opts = subscribe_opts_from_query(&progress).expect("default progress opts");
        assert_eq!(opts.outbound_capacity, Some(4));
        assert!(matches!(
            opts.slow_consumer_policy,
            SlowConsumerPolicy::Disconnect
        ));

        let mut logs = events_query("hub/job/job-1/logs");
        logs.slow_consumer_policy = Some("backpressure".to_string());
        let opts = subscribe_opts_from_query(&logs).expect("backpressure opts");
        assert!(matches!(
            opts.slow_consumer_policy,
            SlowConsumerPolicy::Backpressure
        ));

        let mut stray_key = events_query("hub/job/job-1/logs");
        stray_key.coalesce_key = Some("data.jobId".to_string());
        assert!(
            subscribe_opts_from_query(&stray_key)
                .unwrap_err()
                .contains("requires slowConsumerPolicy=coalesce")
        );
    }

    #[test]
    fn ndjson_coalesce_policy_requires_explicit_key_path() {
        let mut missing_key = events_query("hub/job/job-1/progress");
        missing_key.slow_consumer_policy = Some("coalesce".to_string());
        assert!(
            subscribe_opts_from_query(&missing_key)
                .unwrap_err()
                .contains("requires coalesceKey")
        );

        let mut coalesce = events_query("hub/job/job-1/progress");
        coalesce.slow_consumer_policy = Some("coalesce".to_string());
        coalesce.coalesce_key = Some("data.jobId".to_string());
        let opts = subscribe_opts_from_query(&coalesce).expect("coalesce opts");
        assert!(matches!(
            opts.slow_consumer_policy,
            SlowConsumerPolicy::Coalesce { .. }
        ));
    }

    #[tokio::test]
    async fn ndjson_replay_rejects_non_concrete_topics() {
        let core = empty_core("hub");
        let app = router(core);

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/servers/hub/events?topic=hub/job/*/progress&replay=true")
                    .body(Body::empty())
                    .expect("request builds"),
            )
            .await
            .expect("request completes");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("problem JSON");
        assert_eq!(body["error"], "invalid_replay_topic");
        assert_eq!(body["field"], "topic");
    }
}

fn ndjson_event_line(ev: &crate::events::EventEnvelope) -> Option<String> {
    let iso = ev
        .timestamp
        .format(&time::format_description::well_known::Rfc3339)
        .ok();
    serde_json::to_string(&serde_json::json!({
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
        "stream": ev.stream,
    }))
    .ok()
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
    let topic = match q.topic.as_deref() {
        Some(t) => t,
        None => return (StatusCode::BAD_REQUEST, "missing ?topic=").into_response(),
    };
    let pattern = match TopicPattern::parse(topic) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("topic: {e}")).into_response(),
    };
    let replay_stream_id = if q.replay {
        let Some(stream_id) = stream_id_from_concrete_pattern(&pattern) else {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "invalid_replay_topic",
                "replay=true requires a concrete topic=node/kind/resource/stream",
                Some("topic"),
            );
        };
        Some(stream_id)
    } else {
        None
    };
    let opts = match subscribe_opts_from_query(&q) {
        Ok(opts) => opts,
        Err(message) => {
            return problem_response(
                StatusCode::BAD_REQUEST,
                "invalid_slow_consumer_policy",
                &message,
                Some("slowConsumerPolicy"),
            );
        }
    };
    let sub = state.core.subscribe_events(pattern, opts);
    let replay = replay_stream_id
        .map(|stream_id| state.core.bus.replay_cache().events_after(&stream_id, 0))
        .unwrap_or_default();
    let core_for_guard = state.core.clone();
    let sub_id = sub.id;
    let mut rx = sub.rx;
    let mut slow_consumer_rx = sub.slow_consumer_rx;
    // Drop guard: when the response body is dropped (client
    // disconnect, axum tear-down, etc.), `_guard.drop()` runs and
    // eagerly calls `core.unsubscribe_events(id)`. Without this, the bus
    // only prunes the subscription on the next `try_publish` that
    // notices the closed receiver.
    struct UnsubOnDrop {
        core: Arc<Core>,
        id: crate::events::SubscriptionId,
    }
    impl Drop for UnsubOnDrop {
        fn drop(&mut self) {
            self.core.unsubscribe_events(self.id);
        }
    }
    let stream = async_stream::stream! {
        let _guard = UnsubOnDrop { core: core_for_guard, id: sub_id };
        let mut replayed_event_ids = HashSet::new();
        for ev in replay {
            replayed_event_ids.insert(ev.event_id.clone());
            if let Some(line) = ndjson_event_line(&ev) {
                yield Ok::<_, std::convert::Infallible>(format!("{line}\n"));
            }
        }
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
                    if replayed_event_ids.remove(&ev.event_id) {
                        continue;
                    }
                    let Some(line) = ndjson_event_line(&ev) else { continue };
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
    let specs = core.actor_specs().await;
    let types: Vec<render::KindMeta> = specs
        .iter()
        .map(|spec| render::KindMeta { spec: spec.clone() })
        .collect();
    let policy = render_policy_from_headers(headers);
    siren_response(render::render_meta(&h, &types, policy))
}

async fn meta_type_response(
    core: &Arc<Core>,
    headers: &HeaderMap,
    uri: &Uri,
    ty: &str,
) -> Response {
    let h = build_hrefs(headers, uri, &core.name);
    let specs = core.actor_specs().await;
    let Some(spec) = specs
        .iter()
        .find(|spec| spec.resource.kind == ty)
        .map(|spec| render::KindMeta { spec: spec.clone() })
    else {
        return (StatusCode::NOT_FOUND, "unknown type").into_response();
    };
    siren_response(render::render_meta_type(&h, &spec))
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

    let admitted = match admit_peer_connection(
        &peer_name,
        connection_id,
        req.headers(),
        &state.peer_admission,
    ) {
        Ok(admitted) => admitted,
        Err(response) => return response,
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
            Ok(upgraded) => handler(admitted, upgraded).await,
            Err(e) => tracing::warn!(%e, "peer upgrade failed"),
        }
    });

    upgrade_response
}

fn admit_peer_connection(
    peer_name: &str,
    connection_id: Uuid,
    headers: &HeaderMap,
    admissions: &[PeerAdmissionConfig],
) -> Result<AdmittedPeerConnection, Response> {
    if admissions.is_empty() {
        return Ok(AdmittedPeerConnection::local_development(
            peer_name.to_string(),
            connection_id,
        ));
    }

    let token_id = required_header(headers, crate::tunnel::PEER_TOKEN_ID_HEADER)
        .map_err(|_| (StatusCode::UNAUTHORIZED, "missing peer token id").into_response())?;
    let bearer = bearer_token(headers)
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "missing bearer token").into_response())?;

    let token_matches = admissions
        .iter()
        .filter(|config| config.token_id == token_id)
        .collect::<Vec<_>>();
    if token_matches.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "unknown peer token id").into_response());
    }

    let verified = token_matches
        .into_iter()
        .filter(|config| config.token_verifier.verify(bearer))
        .collect::<Vec<_>>();
    if verified.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "invalid bearer token").into_response());
    }

    let Some(config) = verified
        .into_iter()
        .find(|config| config.allowed_route_name.as_str() == peer_name)
    else {
        return Err((StatusCode::FORBIDDEN, "peer token is not valid for route").into_response());
    };

    let node_id = required_header(headers, crate::tunnel::PEER_NODE_ID_HEADER)
        .map_err(|message| (StatusCode::BAD_REQUEST, message).into_response())?;
    if let Some(expected) = &config.expected_node_id
        && expected.as_str() != node_id
    {
        return Err((StatusCode::FORBIDDEN, "peer node id mismatch").into_response());
    }

    let requested_capabilities = required_header(headers, crate::tunnel::PEER_CAPABILITIES_HEADER)
        .map_err(|message| (StatusCode::BAD_REQUEST, message).into_response())
        .and_then(|raw| {
            PeerCapabilities::parse_list(raw)
                .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()).into_response())
        })?;
    let negotiated_capabilities = requested_capabilities.intersection(config.allowed_capabilities);
    if negotiated_capabilities.is_empty() {
        return Err((StatusCode::FORBIDDEN, "peer capabilities are not allowed").into_response());
    }

    let display_name =
        optional_header(headers, crate::tunnel::PEER_NODE_NAME_HEADER).map(ToOwned::to_owned);
    Ok(AdmittedPeerConnection::token_bound(
        peer_name,
        token_id,
        connection_id,
        node_id,
        display_name,
        config.allowed_capabilities,
        negotiated_capabilities,
    ))
}

fn required_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

fn optional_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let raw = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    raw.strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
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
    use std::convert::Infallible;

    use bytes::Bytes;
    use http::HeaderName;
    use http_body_util::Full;
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use serde_json::Value as JsonValue;
    use tokio::sync::Mutex;

    use super::*;
    use crate::runtime::{JobHandle, NodeBuilder};

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

    #[tokio::test]
    async fn peer_gateway_policy_strips_sensitive_and_hop_by_hop_headers() {
        let (senders, seen, _server) = recording_peer_senders().await;
        let state = test_state(senders);
        let headers = sensitive_gateway_headers();

        let resp = maybe_forward_or_404(
            &state,
            "hub",
            Method::GET,
            &"/servers/hub/resources/resource-1".parse().unwrap(),
            &headers,
            Body::empty(),
        )
        .await
        .expect("remote peer response");

        assert_eq!(resp.status(), StatusCode::OK);
        let seen = seen.lock().await;
        let headers = &seen.first().expect("forwarded request").headers;
        for denied in [
            "authorization",
            "cookie",
            "proxy-authorization",
            "proxy-connection",
            "connection",
            "sec-websocket-key",
            "forwarded",
            "x-forwarded-for",
        ] {
            assert!(
                !headers.contains_key(denied),
                "gateway forwarded denied header {denied}: {headers:?}"
            );
        }
        assert_eq!(
            headers.get("x-boardwalk-external-base"),
            Some(&"http://external.example/servers/hub/".to_string())
        );
        assert_eq!(
            headers.get("x-forwarded-proto"),
            Some(&"http".to_string()),
            "gateway should not trust inbound x-forwarded-proto spoofing"
        );
        assert_eq!(
            headers.get("x-boardwalk-forwarded-by"),
            Some(&"cloud".to_string())
        );
        assert!(
            headers.contains_key("x-boardwalk-correlation-id"),
            "gateway should add a fresh correlation id: {headers:?}"
        );
    }

    #[test]
    fn peer_gateway_policy_strips_hop_by_hop_headers_before_h2_send() {
        let req = build_peer_forward_request(
            "cloud",
            "hub",
            Method::GET,
            &"/servers/hub/resources/resource-1".parse().unwrap(),
            &sensitive_gateway_headers(),
            Body::empty(),
            PeerCapabilities::resource_read(),
        )
        .expect("forward request");
        let headers = req.headers();

        for denied in [
            "authorization",
            "cookie",
            "proxy-authorization",
            "proxy-connection",
            "connection",
            "sec-websocket-key",
            "forwarded",
            "x-forwarded-for",
        ] {
            assert!(
                !headers.contains_key(denied),
                "forward request contains denied header {denied}: {headers:?}"
            );
        }
        assert_eq!(
            headers
                .get("x-boardwalk-external-base")
                .and_then(|value| value.to_str().ok()),
            Some("http://external.example/servers/hub/")
        );
        assert_eq!(
            headers
                .get("x-forwarded-proto")
                .and_then(|value| value.to_str().ok()),
            Some("http")
        );
        assert_eq!(
            headers
                .get("x-boardwalk-forwarded-by")
                .and_then(|value| value.to_str().ok()),
            Some("cloud")
        );
        assert_eq!(
            headers.get_all("x-boardwalk-forwarded-by").iter().count(),
            1
        );
        assert!(
            headers.contains_key("x-boardwalk-correlation-id"),
            "gateway should add a fresh correlation id: {headers:?}"
        );
        assert_eq!(
            headers.get_all("x-boardwalk-correlation-id").iter().count(),
            1
        );
        assert_eq!(
            headers
                .get(RENDER_CAPABILITIES_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("resource.read")
        );
        assert_eq!(
            headers.get_all(RENDER_CAPABILITIES_HEADER).iter().count(),
            1
        );
    }

    #[tokio::test]
    async fn peer_gateway_policy_rejects_unknown_peer_routes_before_forwarding() {
        let (senders, seen, _server) = recording_peer_senders().await;
        let state = test_state(senders);

        let resp = maybe_forward_or_404(
            &state,
            "hub",
            Method::GET,
            &"/servers/hub/admin/raw".parse().unwrap(),
            &HeaderMap::new(),
            Body::empty(),
        )
        .await
        .expect("gateway denial response");

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert!(
            seen.lock().await.is_empty(),
            "unknown peer route should not be forwarded"
        );
    }

    #[test]
    fn proxy_intent_classifies_known_boardwalk_routes() {
        assert_eq!(
            ProxyIntent::from_parts(
                Method::GET,
                "/servers/hub/resources",
                Some("ql=kind%3D%22led%22"),
            ),
            Some(ProxyIntent::Query)
        );
        assert_eq!(
            ProxyIntent::from_parts(
                Method::POST,
                "/servers/hub/resources/abc/transitions/turn-on",
                None,
            ),
            Some(ProxyIntent::Invoke)
        );
        assert_eq!(
            ProxyIntent::from_parts(
                Method::GET,
                "/servers/hub/events",
                Some("topic=hub/led/abc/state"),
            ),
            Some(ProxyIntent::Subscribe)
        );
    }

    #[test]
    fn proxy_intent_rejects_unknown_routes() {
        assert_eq!(
            ProxyIntent::from_parts(Method::GET, "/servers/hub/raw/admin", None),
            None
        );
    }

    #[derive(Debug, Clone)]
    struct RecordedForward {
        headers: std::collections::BTreeMap<String, String>,
    }

    #[derive(Clone)]
    struct RecordingPeerSenders {
        sender: hyper::client::conn::http2::SendRequest<Body>,
    }

    #[async_trait::async_trait]
    impl PeerSenders for RecordingPeerSenders {
        async fn sender(
            &self,
            name: &str,
        ) -> Option<hyper::client::conn::http2::SendRequest<Body>> {
            (name == "hub").then(|| self.sender.clone())
        }

        async fn names(&self) -> Vec<String> {
            vec!["hub".to_string()]
        }

        async fn peer_context(&self, name: &str) -> Option<AdmittedPeerConnection> {
            (name == "hub").then(|| AdmittedPeerConnection::local_development("hub", Uuid::nil()))
        }
    }

    async fn recording_peer_senders() -> (
        Arc<dyn PeerSenders>,
        Arc<Mutex<Vec<RecordedForward>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (sender, client_conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake::<_, Body>(TokioIo::new(client_io))
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = client_conn.await;
        });

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_service = seen.clone();
        let service = service_fn(move |req: http::Request<hyper::body::Incoming>| {
            let seen_for_request = seen_for_service.clone();
            async move {
                let headers = req
                    .headers()
                    .iter()
                    .filter_map(|(name, value)| {
                        value
                            .to_str()
                            .ok()
                            .map(|value| (name.as_str().to_string(), value.to_string()))
                    })
                    .collect();
                seen_for_request
                    .lock()
                    .await
                    .push(RecordedForward { headers });
                Ok::<_, Infallible>(http::Response::new(Full::new(Bytes::from_static(b"ok"))))
            }
        });
        let server = tokio::spawn(async move {
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(server_io), service)
                .await;
        });

        (Arc::new(RecordingPeerSenders { sender }), seen, server)
    }

    fn test_state(peer_senders: Arc<dyn PeerSenders>) -> AppState {
        let node = Arc::new(NodeBuilder::new("cloud").try_build().unwrap());
        AppState {
            core: Core::from_node(node),
            peer_handler: None,
            peer_init: PeerInitState::default(),
            peer_senders: Some(peer_senders),
            peer_streams: super::super::peer_streams::PeerStreamHub::new(),
            peer_admission: Arc::new(Vec::new()),
            resource_registrar: None,
        }
    }

    fn sensitive_gateway_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::HOST,
            HeaderValue::from_static("external.example"),
        );
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer public-client-token"),
        );
        headers.insert(
            http::header::COOKIE,
            HeaderValue::from_static("session=private"),
        );
        headers.insert(
            HeaderName::from_static("proxy-authorization"),
            HeaderValue::from_static("Basic secret"),
        );
        headers.insert(
            HeaderName::from_static("proxy-connection"),
            HeaderValue::from_static("keep-alive"),
        );
        headers.insert(
            http::header::CONNECTION,
            HeaderValue::from_static("upgrade"),
        );
        headers.insert(
            HeaderName::from_static("sec-websocket-key"),
            HeaderValue::from_static("not-for-h2-forward"),
        );
        headers.insert(
            HeaderName::from_static("forwarded"),
            HeaderValue::from_static("for=198.51.100.10"),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("198.51.100.10"),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-proto"),
            HeaderValue::from_static("https"),
        );
        headers.insert(
            HeaderName::from_static("x-boardwalk-external-base"),
            HeaderValue::from_static("https://spoofed.example/servers/hub/"),
        );
        headers.insert(
            HeaderName::from_static("x-boardwalk-forwarded-by"),
            HeaderValue::from_static("attacker"),
        );
        headers.insert(
            HeaderName::from_static("x-boardwalk-correlation-id"),
            HeaderValue::from_static("attacker-correlation"),
        );
        headers.insert(
            HeaderName::from_static(RENDER_CAPABILITIES_HEADER),
            HeaderValue::from_static("peer.admin"),
        );
        headers
    }

    async fn response_json(resp: Response) -> JsonValue {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}
