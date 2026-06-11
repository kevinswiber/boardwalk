use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, Request, State, WebSocketUpgrade};
use axum::http::{Extensions, HeaderMap, HeaderValue, Method, StatusCode, Uri};
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
use crate::peer::{
    AdmittedPeerConnection, PeerAdmissionConfig, PeerCapabilities, UnauthenticatedPeerPolicy,
};
use crate::query::QueryScope;
use crate::runtime::{
    AdmittedPeer, CallerProvenance, RequestCtx, TransitionInput, TransitionOutcome,
};
use crate::siren::SIREN_CONTENT_TYPE;

const RENDER_CAPABILITIES_HEADER: &str = "boardwalk-render-capabilities";

// Gateway-attested caller identity on the forwarding hop. Attached in
// `build_peer_forward_request` after sanitization strips all inbound
// `boardwalk-*` headers (and legacy `x-boardwalk-*`), so values are
// unforgeable across the hop and derive only from the gateway's own
// admission state. All boardwalk headers are unprefixed per RFC 6648.
// (Tunnel admission headers — the dialer's self-asserted identity —
// live in `tunnel.rs`.)
const CALLER_PEER_ID_HEADER: &str = "boardwalk-caller-peer-id";
const CALLER_ROUTE_HEADER: &str = "boardwalk-caller-route";
const CALLER_TOKEN_ID_HEADER: &str = "boardwalk-caller-token-id";
const CALLER_NODE_ID_HEADER: &str = "boardwalk-caller-node-id";
const CALLER_NODE_NAME_HEADER: &str = "boardwalk-caller-node-name";
const CALLER_CAPABILITIES_HEADER: &str = "boardwalk-caller-capabilities";
const CALLER_CONNECTION_ID_HEADER: &str = "boardwalk-caller-connection-id";

/// Request-extension marker present only on the router clone a node
/// serves over tunnels it dialed itself (`PeerClient`). Its presence is
/// the trust boundary for honoring forwarded/attested caller headers;
/// the public listener serves the unmarked router, so forged headers
/// there are ignored.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TunnelLeg;

/// Stable tracing target for every admission, capability, and caller
/// ingress deny decision. The target and its field names are a
/// documented contract for log scraping and alerting (`docs/peers.md`);
/// `kind` is `admission` | `capability` | `ingress`.
pub(super) const ADMISSION_TRACING_TARGET: &str = "boardwalk::admission";

/// Build caller provenance for a request: local/anonymous unless the
/// request arrived over this node's own authenticated tunnel leg, in
/// which case the gateway-owned forwarded/attested headers are honored.
/// An unparsable attested capability list fails closed: the caller is
/// treated as anonymous rather than guessed.
fn caller_provenance(extensions: &Extensions, headers: &HeaderMap) -> CallerProvenance {
    if extensions.get::<TunnelLeg>().is_none() {
        return CallerProvenance::default();
    }
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    let Some(gateway) = header("boardwalk-forwarded-by") else {
        return CallerProvenance::default();
    };
    let caller = header(CALLER_PEER_ID_HEADER).and_then(|peer_id| {
        let capabilities = match header(CALLER_CAPABILITIES_HEADER) {
            Some(raw) => match PeerCapabilities::parse_list(&raw) {
                Ok(caps) => caps.to_capabilities(),
                Err(_) => return None,
            },
            None => Vec::new(),
        };
        Some(AdmittedPeer::new(
            header(CALLER_ROUTE_HEADER).unwrap_or_default(),
            peer_id,
            header(CALLER_TOKEN_ID_HEADER),
            header(CALLER_NODE_ID_HEADER),
            header(CALLER_NODE_NAME_HEADER),
            capabilities,
            header(CALLER_CONNECTION_ID_HEADER).unwrap_or_default(),
        ))
    });
    CallerProvenance::forwarded(gateway, caller)
}

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
    pub unauthenticated_local_peers: Option<UnauthenticatedPeerPolicy>,
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
        unauthenticated_local_peers: None,
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
///
/// Gate order: local-name early-return → intent parse → resolve caller
/// (ingress credentials) → caller capability ceiling → target context →
/// target capability ceiling → forward.
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
    // Resolution happens after the local-name early-return, so
    // local-target requests never engage ingress; invalid credentials
    // are refused before any target-side information is computed.
    let caller =
        match resolve_caller_ingress(headers, &state.peer_admission, senders.as_ref()).await {
            Ok(caller) => caller,
            Err(response) => return Some(*response),
        };
    // Caller-side twin of the target-side gate below: the caller's own
    // negotiated ceiling gates the intent, before any target-side
    // information (404 vs 403) is computed. The effective ceiling for a
    // forwarded request is caller.negotiated ∩ target.negotiated,
    // enforced as two sequential gates so each principal's denial is
    // separately attributable in tracing.
    if let Some(caller) = &caller
        && !caller
            .negotiated_capabilities
            .contains(intent.required_capability())
    {
        tracing::warn!(
            target: ADMISSION_TRACING_TARGET,
            kind = "capability",
            route = %target_name,
            caller = %caller.peer_id,
            intent = %intent.required_capability(),
            negotiated = %caller.negotiated_capabilities,
            reason = "caller capability denied",
            status = StatusCode::FORBIDDEN.as_u16(),
            "caller capability denied"
        );
        return Some((StatusCode::FORBIDDEN, "caller capability denied").into_response());
    }
    let Some(context) = senders.peer_context(target_name).await else {
        return Some(unknown_server_response());
    };
    if !context
        .negotiated_capabilities
        .contains(intent.required_capability())
    {
        tracing::warn!(
            target: ADMISSION_TRACING_TARGET,
            kind = "capability",
            route = %target_name,
            intent = %intent.required_capability(),
            negotiated = %context.negotiated_capabilities,
            reason = "peer capability denied",
            status = StatusCode::FORBIDDEN.as_u16(),
            "peer capability denied"
        );
        return Some((StatusCode::FORBIDDEN, "peer capability denied").into_response());
    }
    let Some(mut sender) = senders.sender(target_name).await else {
        return Some(unknown_server_response());
    };
    let req = match build_peer_forward_request(
        ForwardAttestation {
            gateway_name: &state.core.name,
            render_capabilities: context.negotiated_capabilities,
            // Attested from the gateway's own admission state, resolved
            // at ingress from the caller's per-request credentials and
            // live tunnel.
            caller: caller.as_ref(),
        },
        target_name,
        method,
        uri,
        headers,
        body,
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

/// Gateway-owned values stamped onto a forwarded request after
/// sanitization: who is forwarding, the render-capability ceiling, and
/// the attested caller identity (if the gateway admitted one).
struct ForwardAttestation<'a> {
    gateway_name: &'a str,
    render_capabilities: PeerCapabilities,
    caller: Option<&'a AdmittedPeerConnection>,
}

fn build_peer_forward_request(
    attestation: ForwardAttestation<'_>,
    target_name: &str,
    method: Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Body,
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
        .header("boardwalk-external-base", bases.http)
        .header("boardwalk-external-ws-base", bases.ws)
        .header("boardwalk-forwarded-by", attestation.gateway_name)
        .header(
            RENDER_CAPABILITIES_HEADER,
            attestation.render_capabilities.to_string(),
        )
        .header("boardwalk-correlation-id", Uuid::new_v4().to_string());
    if let Some(caller) = attestation.caller {
        builder = builder
            .header(CALLER_PEER_ID_HEADER, caller.peer_id.as_str())
            .header(CALLER_ROUTE_HEADER, caller.route_name.as_str())
            .header(
                CALLER_CAPABILITIES_HEADER,
                caller.negotiated_capabilities.to_string(),
            )
            .header(
                CALLER_CONNECTION_ID_HEADER,
                caller.connection_id.to_string(),
            );
        if let Some(token_id) = caller.token_id.as_deref() {
            builder = builder.header(CALLER_TOKEN_ID_HEADER, token_id);
        }
        if let Some(node_id) = caller.node_id.as_deref() {
            builder = builder.header(CALLER_NODE_ID_HEADER, node_id);
        }
        if let Some(node_name) = caller.display_name.as_deref() {
            builder = builder.header(CALLER_NODE_NAME_HEADER, node_name);
        }
    }
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
        || name.starts_with("boardwalk-")
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
        .get("boardwalk-external-base")
        .and_then(|v| v.to_str().ok())
        .and_then(|base| base.parse::<url::Url>().ok())
    {
        let ws_base = headers
            .get("boardwalk-external-ws-base")
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
    extensions: Extensions,
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
    let provenance = caller_provenance(&extensions, &headers);
    transition_response(
        &state.core,
        &headers,
        provenance,
        &id,
        &transition,
        body_bytes,
    )
    .await
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
    extensions: Extensions,
    body_bytes: bytes::Bytes,
) -> Response {
    let provenance = caller_provenance(&extensions, &headers);
    transition_response(
        &state.core,
        &headers,
        provenance,
        &id,
        &transition,
        body_bytes,
    )
    .await
}

async fn transition_response(
    core: &Arc<Core>,
    headers: &HeaderMap,
    provenance: CallerProvenance,
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

    let request_ctx = RequestCtx::from_headers(headers).with_provenance(provenance);
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
        .unwrap_or("disconnect");
    let slow_consumer_policy =
        SlowConsumerPolicy::from_query(policy_name, query.coalesce_key.as_deref())
            .map_err(|err| err.to_string())?;
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
        state.unauthenticated_local_peers.as_ref(),
    ) {
        Ok(admitted) => admitted,
        Err(response) => return *response,
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
    unauthenticated: Option<&UnauthenticatedPeerPolicy>,
) -> Result<AdmittedPeerConnection, Box<Response>> {
    let deny = |status: StatusCode, reason: &str, token_id: Option<&str>, node_id: Option<&str>| {
        admission_denied(AdmissionDeny {
            kind: "admission",
            status,
            reason,
            route: Some(peer_name),
            token_id,
            node_id,
            requested: None,
            allowed: None,
            connection_id: Some(connection_id),
        })
    };
    if admissions.is_empty() {
        let Some(policy) = unauthenticated else {
            return Err(deny(
                StatusCode::FORBIDDEN,
                "peer admission is not configured",
                None,
                None,
            ));
        };
        return Ok(AdmittedPeerConnection::unauthenticated(
            peer_name.to_string(),
            connection_id,
            policy.allowed_capabilities,
        ));
    }

    let token_id = required_header(headers, crate::tunnel::PEER_TOKEN_ID_HEADER).map_err(|_| {
        deny(
            StatusCode::UNAUTHORIZED,
            "missing peer token id",
            None,
            None,
        )
    })?;
    let bearer = bearer_token(headers).ok_or_else(|| {
        deny(
            StatusCode::UNAUTHORIZED,
            "missing bearer token",
            Some(token_id),
            None,
        )
    })?;

    let verified = verified_admissions(admissions, token_id, bearer)
        .map_err(|reason| deny(StatusCode::UNAUTHORIZED, reason, Some(token_id), None))?;

    let Some(config) = verified
        .into_iter()
        .find(|config| config.allowed_route_name.as_str() == peer_name)
    else {
        return Err(deny(
            StatusCode::FORBIDDEN,
            "peer token is not valid for route",
            Some(token_id),
            None,
        ));
    };

    let node_id = required_header(headers, crate::tunnel::PEER_NODE_ID_HEADER)
        .map_err(|message| deny(StatusCode::BAD_REQUEST, &message, Some(token_id), None))?;
    if let Some(expected) = &config.expected_node_id
        && expected.as_str() != node_id
    {
        return Err(deny(
            StatusCode::FORBIDDEN,
            "peer node id mismatch",
            Some(token_id),
            Some(node_id),
        ));
    }

    let requested_capabilities = required_header(headers, crate::tunnel::PEER_CAPABILITIES_HEADER)
        .map_err(|message| deny(StatusCode::BAD_REQUEST, &message, Some(token_id), None))
        .and_then(|raw| {
            PeerCapabilities::parse_list(raw).map_err(|err| {
                deny(
                    StatusCode::BAD_REQUEST,
                    &err.to_string(),
                    Some(token_id),
                    None,
                )
            })
        })?;
    let negotiated_capabilities = requested_capabilities.intersection(config.allowed_capabilities);
    if negotiated_capabilities.is_empty() {
        let requested = requested_capabilities.to_string();
        let allowed = config.allowed_capabilities.to_string();
        return Err(admission_denied(AdmissionDeny {
            kind: "admission",
            status: StatusCode::FORBIDDEN,
            reason: "peer capabilities are not allowed",
            route: Some(peer_name),
            token_id: Some(token_id),
            node_id: None,
            requested: Some(&requested),
            allowed: Some(&allowed),
            connection_id: Some(connection_id),
        }));
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

/// Token-id filter plus constant-time secret verification, shared by
/// handshake admission (`admit_peer_connection`) and per-request caller
/// ingress (`resolve_caller_ingress`) so the two paths cannot drift.
/// Returns the verified configs or the denial reason; callers attach
/// their own status and deny machinery.
fn verified_admissions<'a>(
    admissions: &'a [PeerAdmissionConfig],
    token_id: &str,
    bearer: &str,
) -> Result<Vec<&'a PeerAdmissionConfig>, &'static str> {
    let token_matches = admissions
        .iter()
        .filter(|config| config.token_id == token_id)
        .collect::<Vec<_>>();
    if token_matches.is_empty() {
        return Err("unknown peer token id");
    }
    let verified = token_matches
        .into_iter()
        .filter(|config| config.token_verifier.verify(bearer))
        .collect::<Vec<_>>();
    if verified.is_empty() {
        return Err("invalid bearer token");
    }
    Ok(verified)
}

/// Resolve an admitted caller from per-request ingress credentials.
///
/// Engagement signal: the `boardwalk-peer-token-id` header. Absent →
/// `Ok(None)` (anonymous; unchanged behavior). Present → fail-closed:
/// the credentials must verify against a configured admission AND the
/// matching route must hold a live admitted tunnel under the same token
/// id. Negotiated capabilities and connection identity come from that
/// live handshake — nothing is synthesized per-request.
pub(crate) async fn resolve_caller_ingress(
    headers: &HeaderMap,
    admissions: &[PeerAdmissionConfig],
    senders: &dyn PeerSenders,
) -> Result<Option<AdmittedPeerConnection>, Box<Response>> {
    let Some(token_id) = optional_header(headers, crate::tunnel::PEER_TOKEN_ID_HEADER) else {
        return Ok(None);
    };
    // Ingress denials never enumerate capabilities; `token_id` is the
    // engagement signal so it is always present on the event.
    let refuse =
        |status: StatusCode, reason: &str, route: Option<&str>, connection_id: Option<Uuid>| {
            admission_denied(AdmissionDeny {
                kind: "ingress",
                status,
                reason,
                route,
                token_id: Some(token_id),
                node_id: None,
                requested: None,
                allowed: None,
                connection_id,
            })
        };
    if admissions.is_empty() {
        return Err(refuse(
            StatusCode::FORBIDDEN,
            "peer admission is not configured",
            None,
            None,
        ));
    }
    let Some(bearer) = bearer_token(headers) else {
        return Err(refuse(
            StatusCode::UNAUTHORIZED,
            "missing bearer token",
            None,
            None,
        ));
    };
    let verified = verified_admissions(admissions, token_id, bearer)
        .map_err(|reason| refuse(StatusCode::UNAUTHORIZED, reason, None, None))?;
    let mut live = Vec::new();
    let mut mismatched_connection = None;
    for config in &verified {
        if let Some(context) = senders
            .peer_context(config.allowed_route_name.as_str())
            .await
        {
            if context.token_id.as_deref() == Some(token_id) {
                live.push(context);
            } else {
                // The route is live, but under a different token id —
                // recorded for the deny event, never attested.
                mismatched_connection = Some(context.connection_id);
            }
        }
    }
    let verified_route = (verified.len() == 1).then(|| verified[0].allowed_route_name.as_str());
    match live.len() {
        0 => Err(refuse(
            StatusCode::FORBIDDEN,
            "caller peer is not connected",
            verified_route,
            mismatched_connection,
        )),
        1 => Ok(Some(live.remove(0))),
        _ => Err(refuse(
            StatusCode::FORBIDDEN,
            "ambiguous caller admission",
            None,
            None,
        )),
    }
}

/// One admission deny decision, traced and converted to a response in
/// one place so the event can never drift from what the dialer saw.
struct AdmissionDeny<'a> {
    /// Decision class: `admission` (handshake) or `ingress`
    /// (per-request caller credentials). The target-side capability
    /// denial logs `kind = "capability"` inline.
    kind: &'a str,
    status: StatusCode,
    reason: &'a str,
    route: Option<&'a str>,
    token_id: Option<&'a str>,
    node_id: Option<&'a str>,
    /// Requested-vs-allowed enumeration for the empty-intersection
    /// refusal: logged server-side only, never in the response body.
    requested: Option<&'a str>,
    allowed: Option<&'a str>,
    /// Handshake denials always carry the connection id; ingress
    /// denials only when a live context exists (token mismatch).
    connection_id: Option<Uuid>,
}

/// Emit the structured deny event at the stable `boardwalk::admission`
/// target and build the matching response. The target and field names
/// (`kind`, `route`, `reason`, `status`, `token_id`, `node_id`,
/// `connection_id`) are a documented contract for log scraping and
/// alerting; `token_id` is a public identifier — bearer secrets are
/// never logged.
fn admission_denied(deny: AdmissionDeny<'_>) -> Box<Response> {
    // Stable, quotable hint only on the enumerating refusal — the one
    // denial an operator fixes by widening the ceiling.
    let message = if deny.requested.is_some() {
        "peer admission denied; widen with PeerAdmission::allow(...) on the accepting node"
    } else {
        "peer admission denied"
    };
    let connection_id = deny.connection_id.map(|id| id.to_string());
    tracing::warn!(
        target: ADMISSION_TRACING_TARGET,
        kind = deny.kind,
        route = deny.route,
        reason = deny.reason,
        status = deny.status.as_u16(),
        token_id = deny.token_id,
        node_id = deny.node_id,
        requested = deny.requested,
        allowed = deny.allowed,
        connection_id = connection_id.as_deref(),
        "{message}"
    );
    Box::new((deny.status, deny.reason.to_string()).into_response())
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
    use crate::peer::AdmittedPeerConnection;
    use crate::runtime::{AcceptedJob, NodeBuilder};

    #[test]
    fn provenance_is_local_without_tunnel_marker_even_with_attested_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("boardwalk-forwarded-by", "cloud".parse().unwrap());
        headers.insert("boardwalk-caller-peer-id", "peer-fake".parse().unwrap());
        let provenance = caller_provenance(&Extensions::new(), &headers);
        assert!(provenance.is_local());
        assert!(provenance.peer().is_none());
    }

    #[test]
    fn provenance_is_forwarded_with_tunnel_marker() {
        let mut extensions = Extensions::new();
        extensions.insert(TunnelLeg);
        let mut headers = HeaderMap::new();
        headers.insert("boardwalk-forwarded-by", "cloud".parse().unwrap());
        let provenance = caller_provenance(&extensions, &headers);
        assert_eq!(provenance.forwarded_by(), Some("cloud"));
        assert!(provenance.peer().is_none()); // anonymous caller
    }

    #[test]
    fn provenance_carries_attested_caller_over_tunnel_leg() {
        let mut extensions = Extensions::new();
        extensions.insert(TunnelLeg);
        let mut headers = HeaderMap::new();
        headers.insert("boardwalk-forwarded-by", "cloud".parse().unwrap());
        headers.insert(
            "boardwalk-caller-peer-id",
            "peer-reviewer-respond-rs-1".parse().unwrap(),
        );
        headers.insert(
            "boardwalk-caller-route",
            "reviewer-respond".parse().unwrap(),
        );
        headers.insert(
            "boardwalk-caller-capabilities",
            "resource.read,transition.invoke".parse().unwrap(),
        );
        headers.insert(
            "boardwalk-caller-connection-id",
            "00000000-0000-0000-0000-000000000001".parse().unwrap(),
        );
        let provenance = caller_provenance(&extensions, &headers);
        let peer = provenance.peer().expect("attested caller");
        assert_eq!(peer.peer_id(), "peer-reviewer-respond-rs-1");
        assert!(peer.has_capability(crate::peer::PeerCapability::TransitionInvoke));
    }

    #[test]
    fn forward_request_attaches_caller_headers_from_admission_state() {
        let caller = AdmittedPeerConnection::token_bound(
            "reviewer-respond",
            "rs-1",
            Uuid::nil(),
            "node-reviewer-9",
            None,
            PeerCapabilities::all(),
            PeerCapabilities::resource_read(),
        );
        let uri: Uri = "/servers/hub/resources".parse().unwrap();
        let headers = HeaderMap::new();
        let req = build_peer_forward_request(
            ForwardAttestation {
                gateway_name: "cloud",
                render_capabilities: PeerCapabilities::resource_read(),
                caller: Some(&caller),
            },
            "hub",
            Method::POST,
            &uri,
            &headers,
            Body::empty(),
        )
        .unwrap();
        let h = req.headers();
        assert_eq!(
            h.get("boardwalk-caller-peer-id").unwrap(),
            "peer-reviewer-respond-rs-1"
        );
        assert_eq!(
            h.get("boardwalk-caller-node-id").unwrap(),
            "node-reviewer-9"
        );
        assert_eq!(h.get("boardwalk-caller-route").unwrap(), "reviewer-respond");
        assert_eq!(h.get("boardwalk-caller-token-id").unwrap(), "rs-1");
        assert_eq!(
            h.get("boardwalk-caller-capabilities").unwrap(),
            "resource.read"
        );
        assert!(h.get("boardwalk-caller-connection-id").is_some());
    }

    #[test]
    fn forward_request_attaches_no_caller_headers_for_anonymous_callers() {
        let uri: Uri = "/servers/hub/resources".parse().unwrap();
        let headers = HeaderMap::new();
        let req = build_peer_forward_request(
            ForwardAttestation {
                gateway_name: "cloud",
                render_capabilities: PeerCapabilities::resource_read(),
                caller: None,
            },
            "hub",
            Method::GET,
            &uri,
            &headers,
            Body::empty(),
        )
        .unwrap();
        assert!(req.headers().get("boardwalk-caller-peer-id").is_none());
    }

    #[test]
    fn inbound_caller_headers_are_stripped_before_attestation() {
        // A public caller trying to smuggle boardwalk-caller-* through
        // the gateway must be stripped by the existing sanitization, and
        // with an anonymous caller nothing is re-attached.
        let uri: Uri = "/servers/hub/resources".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("boardwalk-caller-peer-id", "peer-fake".parse().unwrap());
        let req = build_peer_forward_request(
            ForwardAttestation {
                gateway_name: "cloud",
                render_capabilities: PeerCapabilities::resource_read(),
                caller: None,
            },
            "hub",
            Method::GET,
            &uri,
            &headers,
            Body::empty(),
        )
        .unwrap();
        assert!(req.headers().get("boardwalk-caller-peer-id").is_none());
    }

    #[tokio::test]
    async fn forwarded_request_with_ingress_credentials_attests_the_caller() {
        let (senders, seen, _server) = recording_peer_senders().await;
        let state = test_state_with_admissions(senders, vec![reviewer_admission()]);

        let resp = maybe_forward_or_404(
            &state,
            "hub",
            Method::GET,
            &"/servers/hub/resources".parse().unwrap(),
            &ingress_headers("kid-2", "reviewer-secret"),
            Body::empty(),
        )
        .await
        .expect("forwarded response");

        assert_eq!(resp.status(), StatusCode::OK);
        let seen = seen.lock().await;
        let headers = &seen.first().expect("forwarded request").headers;
        assert_eq!(
            headers.get("boardwalk-caller-peer-id"),
            Some(&"peer-reviewer-kid-2".to_string())
        );
    }

    #[tokio::test]
    async fn caller_without_invoke_capability_cannot_invoke_through_the_gateway() {
        // The reviewer's live context negotiated resource.read only; the
        // target "hub" context allows everything. The caller's own
        // ceiling must gate the intent before anything is forwarded.
        let (senders, seen, _server) = recording_peer_senders().await;
        let state = test_state_with_admissions(senders, vec![reviewer_admission()]);

        let resp = maybe_forward_or_404(
            &state,
            "hub",
            Method::POST,
            &"/servers/hub/resources/r-1/transitions/report"
                .parse()
                .unwrap(),
            &ingress_headers("kid-2", "reviewer-secret"),
            Body::empty(),
        )
        .await
        .expect("gateway denial response");

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert!(
            seen.lock().await.is_empty(),
            "caller above its ceiling must not be forwarded"
        );
    }

    #[tokio::test]
    async fn caller_read_within_ceiling_still_forwards() {
        // Same read-only reviewer: reads stay within its negotiated
        // ceiling and forward with attestation.
        let (senders, seen, _server) = recording_peer_senders().await;
        let state = test_state_with_admissions(senders, vec![reviewer_admission()]);

        let resp = maybe_forward_or_404(
            &state,
            "hub",
            Method::GET,
            &"/servers/hub/resources".parse().unwrap(),
            &ingress_headers("kid-2", "reviewer-secret"),
            Body::empty(),
        )
        .await
        .expect("forwarded response");

        assert_eq!(resp.status(), StatusCode::OK);
        let seen = seen.lock().await;
        let headers = &seen.first().expect("forwarded request").headers;
        assert_eq!(
            headers.get("boardwalk-caller-peer-id"),
            Some(&"peer-reviewer-kid-2".to_string())
        );
    }

    #[tokio::test]
    async fn caller_credentials_never_cross_the_forwarding_hop() {
        let (senders, seen, _server) = recording_peer_senders().await;
        let state = test_state_with_admissions(senders, vec![reviewer_admission()]);

        let resp = maybe_forward_or_404(
            &state,
            "hub",
            Method::GET,
            &"/servers/hub/resources".parse().unwrap(),
            &ingress_headers("kid-2", "reviewer-secret"),
            Body::empty(),
        )
        .await
        .expect("forwarded response");

        assert_eq!(resp.status(), StatusCode::OK);
        let seen = seen.lock().await;
        let headers = &seen.first().expect("forwarded request").headers;
        for denied in ["authorization", crate::tunnel::PEER_TOKEN_ID_HEADER] {
            assert!(
                !headers.contains_key(denied),
                "caller credential header {denied} crossed the hop: {headers:?}"
            );
        }
    }

    #[tokio::test]
    async fn anonymous_forward_still_carries_no_caller_headers() {
        // Regression pin: no credentials → no boardwalk-caller-* headers.
        let (senders, seen, _server) = recording_peer_senders().await;
        let state = test_state_with_admissions(senders, vec![reviewer_admission()]);

        let resp = maybe_forward_or_404(
            &state,
            "hub",
            Method::GET,
            &"/servers/hub/resources".parse().unwrap(),
            &HeaderMap::new(),
            Body::empty(),
        )
        .await
        .expect("forwarded response");

        assert_eq!(resp.status(), StatusCode::OK);
        let seen = seen.lock().await;
        let headers = &seen.first().expect("forwarded request").headers;
        assert!(
            headers
                .keys()
                .all(|name| !name.starts_with("boardwalk-caller-")),
            "anonymous forward must carry no caller headers: {headers:?}"
        );
    }

    #[tokio::test]
    async fn accepted_created_transition_sets_201_location_and_job_body() {
        let resp = transition_outcome_response(TransitionOutcome::Accepted {
            job: AcceptedJob {
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
            job: AcceptedJob {
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
            job: AcceptedJob {
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
            "proxy-authenticate",
            "proxy-authorization",
            "proxy-debug",
            "proxy-connection",
            "connection",
            "sec-websocket-accept",
            "sec-websocket-extensions",
            "sec-websocket-key",
            "sec-websocket-protocol",
            "sec-websocket-version",
            "forwarded",
            "x-forwarded-for",
            "x-forwarded-port",
        ] {
            assert!(
                !headers.contains_key(denied),
                "gateway forwarded denied header {denied}: {headers:?}"
            );
        }
        assert_eq!(
            headers.get("boardwalk-external-base"),
            Some(&"http://external.example/servers/hub/".to_string())
        );
        assert_eq!(
            headers.get("x-forwarded-proto"),
            Some(&"http".to_string()),
            "gateway should not trust inbound x-forwarded-proto spoofing"
        );
        assert_eq!(
            headers.get("x-forwarded-host"),
            Some(&"external.example".to_string()),
            "gateway should not trust inbound x-forwarded-host spoofing"
        );
        assert_eq!(
            headers
                .iter()
                .filter(|(name, _)| name.as_str().starts_with("x-forwarded-"))
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("x-forwarded-host", "external.example"),
                ("x-forwarded-proto", "http")
            ],
            "gateway should only write fresh x-forwarded metadata: {headers:?}"
        );
        assert_eq!(
            headers.get("boardwalk-forwarded-by"),
            Some(&"cloud".to_string())
        );
        assert!(
            headers.contains_key("boardwalk-correlation-id"),
            "gateway should add a fresh correlation id: {headers:?}"
        );
    }

    #[test]
    fn peer_gateway_policy_strips_hop_by_hop_headers_before_h2_send() {
        let req = build_peer_forward_request(
            ForwardAttestation {
                gateway_name: "cloud",
                render_capabilities: PeerCapabilities::resource_read(),
                caller: None,
            },
            "hub",
            Method::GET,
            &"/servers/hub/resources/resource-1".parse().unwrap(),
            &sensitive_gateway_headers(),
            Body::empty(),
        )
        .expect("forward request");
        let headers = req.headers();

        for denied in [
            "authorization",
            "cookie",
            "proxy-authenticate",
            "proxy-authorization",
            "proxy-debug",
            "proxy-connection",
            "connection",
            "sec-websocket-accept",
            "sec-websocket-extensions",
            "sec-websocket-key",
            "sec-websocket-protocol",
            "sec-websocket-version",
            "forwarded",
            "x-forwarded-for",
            "x-forwarded-port",
        ] {
            assert!(
                !headers.contains_key(denied),
                "forward request contains denied header {denied}: {headers:?}"
            );
        }
        assert_eq!(
            headers
                .get("boardwalk-external-base")
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
                .get("x-forwarded-host")
                .and_then(|value| value.to_str().ok()),
            Some("external.example")
        );
        assert_eq!(
            headers
                .iter()
                .filter(|(name, _)| name.as_str().starts_with("x-forwarded-"))
                .map(|(name, value)| (name.as_str(), value.to_str().unwrap()))
                .collect::<std::collections::BTreeMap<_, _>>(),
            std::collections::BTreeMap::from([
                ("x-forwarded-host", "external.example"),
                ("x-forwarded-proto", "http")
            ])
        );
        assert_eq!(
            headers
                .get("boardwalk-forwarded-by")
                .and_then(|value| value.to_str().ok()),
            Some("cloud")
        );
        assert_eq!(headers.get_all("boardwalk-forwarded-by").iter().count(), 1);
        assert!(
            headers.contains_key("boardwalk-correlation-id"),
            "gateway should add a fresh correlation id: {headers:?}"
        );
        assert_eq!(
            headers.get_all("boardwalk-correlation-id").iter().count(),
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
            match name {
                "hub" => Some(AdmittedPeerConnection::unauthenticated(
                    "hub",
                    Uuid::nil(),
                    PeerCapabilities::all(),
                )),
                "reviewer" => Some(live_reviewer_context()),
                _ => None,
            }
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
        test_state_with_admissions(peer_senders, Vec::new())
    }

    fn test_state_with_admissions(
        peer_senders: Arc<dyn PeerSenders>,
        admissions: Vec<PeerAdmissionConfig>,
    ) -> AppState {
        let node = Arc::new(NodeBuilder::new("cloud").try_build().unwrap());
        AppState {
            core: Core::from_node(node),
            peer_handler: None,
            peer_init: PeerInitState::default(),
            peer_senders: Some(peer_senders),
            peer_streams: super::super::peer_streams::PeerStreamHub::new(),
            peer_admission: Arc::new(admissions),
            unauthenticated_local_peers: None,
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
            HeaderName::from_static("proxy-authenticate"),
            HeaderValue::from_static("Basic realm=secret"),
        );
        headers.insert(
            HeaderName::from_static("proxy-debug"),
            HeaderValue::from_static("leak"),
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
            HeaderName::from_static("sec-websocket-protocol"),
            HeaderValue::from_static("boardwalk-peer/3"),
        );
        headers.insert(
            HeaderName::from_static("sec-websocket-version"),
            HeaderValue::from_static("13"),
        );
        headers.insert(
            HeaderName::from_static("sec-websocket-extensions"),
            HeaderValue::from_static("permessage-deflate"),
        );
        headers.insert(
            HeaderName::from_static("sec-websocket-accept"),
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
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("spoofed.example"),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-proto"),
            HeaderValue::from_static("https"),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-port"),
            HeaderValue::from_static("443"),
        );
        headers.insert(
            HeaderName::from_static("boardwalk-external-base"),
            HeaderValue::from_static("https://spoofed.example/servers/hub/"),
        );
        headers.insert(
            HeaderName::from_static("boardwalk-forwarded-by"),
            HeaderValue::from_static("attacker"),
        );
        headers.insert(
            HeaderName::from_static("boardwalk-correlation-id"),
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

    /// `PeerSenders` stub serving a static route-name → live-context map
    /// for ingress resolution tests.
    struct StaticContexts(HashMap<String, AdmittedPeerConnection>);

    #[async_trait::async_trait]
    impl PeerSenders for StaticContexts {
        async fn sender(
            &self,
            _name: &str,
        ) -> Option<hyper::client::conn::http2::SendRequest<Body>> {
            None
        }

        async fn names(&self) -> Vec<String> {
            self.0.keys().cloned().collect()
        }

        async fn peer_context(&self, name: &str) -> Option<AdmittedPeerConnection> {
            self.0.get(name).cloned()
        }
    }

    fn ingress_headers(token_id: &str, bearer: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::tunnel::PEER_TOKEN_ID_HEADER,
            token_id.parse().unwrap(),
        );
        headers.insert(
            http::header::AUTHORIZATION,
            format!("Bearer {bearer}").parse().unwrap(),
        );
        headers
    }

    fn reviewer_admission() -> PeerAdmissionConfig {
        let mut config =
            PeerAdmissionConfig::shared_token("reviewer", "kid-2", "reviewer-secret").unwrap();
        config.allowed_capabilities =
            PeerCapabilities::parse_list("resource.read,transition.invoke").unwrap();
        config
    }

    fn live_reviewer_context() -> AdmittedPeerConnection {
        AdmittedPeerConnection::token_bound(
            "reviewer",
            "kid-2",
            Uuid::new_v4(),
            "node-reviewer-9",
            None,
            PeerCapabilities::all(),
            PeerCapabilities::resource_read(),
        )
    }

    #[tokio::test]
    async fn ingress_without_token_id_header_is_anonymous() {
        // Authorization alone never engages ingress.
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            "Bearer anything".parse().unwrap(),
        );
        let senders = StaticContexts(HashMap::new());
        let caller = resolve_caller_ingress(&headers, &[], &senders)
            .await
            .unwrap();
        assert!(caller.is_none());
    }

    #[tokio::test]
    async fn ingress_resolves_live_admitted_caller() {
        let senders = StaticContexts(HashMap::from([(
            "reviewer".into(),
            live_reviewer_context(),
        )]));
        let caller = resolve_caller_ingress(
            &ingress_headers("kid-2", "reviewer-secret"),
            &[reviewer_admission()],
            &senders,
        )
        .await
        .unwrap()
        .expect("admitted caller");
        assert_eq!(caller.peer_id, "peer-reviewer-kid-2");
        assert_eq!(caller.token_id.as_deref(), Some("kid-2"));
        assert_eq!(
            caller.negotiated_capabilities,
            PeerCapabilities::resource_read()
        );
    }

    #[tokio::test]
    async fn ingress_with_unknown_token_id_is_401() {
        let senders = StaticContexts(HashMap::new());
        let err = resolve_caller_ingress(
            &ingress_headers("kid-unknown", "reviewer-secret"),
            &[reviewer_admission()],
            &senders,
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingress_with_invalid_bearer_is_401() {
        let senders = StaticContexts(HashMap::from([(
            "reviewer".into(),
            live_reviewer_context(),
        )]));
        let err = resolve_caller_ingress(
            &ingress_headers("kid-2", "wrong-secret"),
            &[reviewer_admission()],
            &senders,
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingress_with_missing_bearer_is_401() {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::tunnel::PEER_TOKEN_ID_HEADER,
            "kid-2".parse().unwrap(),
        );
        let senders = StaticContexts(HashMap::new());
        let err = resolve_caller_ingress(&headers, &[reviewer_admission()], &senders)
            .await
            .unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingress_without_admission_config_is_403() {
        let senders = StaticContexts(HashMap::new());
        let err =
            resolve_caller_ingress(&ingress_headers("kid-2", "reviewer-secret"), &[], &senders)
                .await
                .unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn ingress_for_disconnected_peer_is_403() {
        // Valid credentials, no live context at the config's route.
        let senders = StaticContexts(HashMap::new());
        let err = resolve_caller_ingress(
            &ingress_headers("kid-2", "reviewer-secret"),
            &[reviewer_admission()],
            &senders,
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn ingress_token_mismatch_with_live_context_is_403() {
        // Route is connected, but under a different token id: a second
        // token valid for the same route must not be attested as the
        // live principal.
        let mut live = live_reviewer_context();
        live.token_id = Some("kid-other".into());
        live.peer_id = "peer-reviewer-kid-other".into();
        let senders = StaticContexts(HashMap::from([("reviewer".into(), live)]));
        let err = resolve_caller_ingress(
            &ingress_headers("kid-2", "reviewer-secret"),
            &[reviewer_admission()],
            &senders,
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn ingress_ambiguous_across_routes_is_403() {
        // One token id verified for two configs whose routes are both
        // live under that token id → refuse rather than guess.
        let second_admission = {
            let mut config =
                PeerAdmissionConfig::shared_token("reviewer-b", "kid-2", "reviewer-secret")
                    .unwrap();
            config.allowed_capabilities = PeerCapabilities::resource_read();
            config
        };
        let second_live = AdmittedPeerConnection::token_bound(
            "reviewer-b",
            "kid-2",
            Uuid::new_v4(),
            "node-reviewer-10",
            None,
            PeerCapabilities::all(),
            PeerCapabilities::resource_read(),
        );
        let senders = StaticContexts(HashMap::from([
            ("reviewer".into(), live_reviewer_context()),
            ("reviewer-b".into(), second_live),
        ]));
        let err = resolve_caller_ingress(
            &ingress_headers("kid-2", "reviewer-secret"),
            &[reviewer_admission(), second_admission],
            &senders,
        )
        .await
        .unwrap_err();
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }
}
