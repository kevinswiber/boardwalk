//! Peer client (outbound) and peer socket (inbound).

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use http::Request;
use http_body_util::BodyExt;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::TokioExecutor;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::http::{PeerInitState, PeerSenders};
use crate::persistence::{
    DefaultRepositories, PeerConfigRecord, PeerConnectionDirection, PeerConnectionStatusRecord,
    Repositories,
};

#[derive(Debug, Error)]
pub enum PeerError {
    #[error("tunnel: {0}")]
    Tunnel(#[from] crate::tunnel::TunnelError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("hyper: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("hyper legacy: {0}")]
    #[allow(dead_code)]
    HyperLegacy(String),
}

#[allow(dead_code)]
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum PeerModelError {
    #[error("node id cannot be empty")]
    EmptyNodeId,
    #[error("peer id cannot be empty")]
    EmptyPeerId,
    #[error("route name cannot be empty")]
    EmptyRouteName,
    #[error("route name `{0}` is not a URL path segment")]
    InvalidRouteName(String),
    #[error("unknown peer capability `{0}`")]
    UnknownCapability(String),
    #[error("invalid peer url: {0}")]
    InvalidUrl(String),
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct PeerId(String);

#[allow(dead_code)]
impl PeerId {
    pub(crate) fn new(id: impl Into<String>) -> Result<Self, PeerModelError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(PeerModelError::EmptyPeerId);
        }
        Ok(Self(id))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct RouteName(String);

#[allow(dead_code)]
impl RouteName {
    pub(crate) fn new(route_name: impl Into<String>) -> Result<Self, PeerModelError> {
        let route_name = route_name.into();
        if route_name.trim().is_empty() {
            return Err(PeerModelError::EmptyRouteName);
        }
        if !route_name.chars().all(is_route_name_char) {
            return Err(PeerModelError::InvalidRouteName(route_name));
        }
        Ok(Self(route_name))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

fn is_route_name_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '.' | '_' | '~')
}

impl fmt::Display for RouteName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NodeIdentity {
    pub node_id: crate::events::NodeId,
    pub display_name: String,
    pub route_name: RouteName,
}

#[allow(dead_code)]
impl NodeIdentity {
    pub(crate) fn new(
        node_id: impl Into<String>,
        display_name: impl Into<String>,
        route_name: impl Into<String>,
    ) -> Result<Self, PeerModelError> {
        let node_id = node_id.into();
        if node_id.trim().is_empty() {
            return Err(PeerModelError::EmptyNodeId);
        }
        Ok(Self {
            node_id: crate::events::NodeId::new(node_id),
            display_name: display_name.into(),
            route_name: RouteName::new(route_name)?,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct PeerCapabilities(u8);

/// Single source of truth for the capability axes: bit, typed variant,
/// canonical dotted wire name. Adding an axis is one new row here plus
/// the `PeerCapability` variant.
const CAPABILITY_AXES: [(u8, PeerCapability, &str); 6] = [
    (
        PeerCapabilities::RESOURCE_READ,
        PeerCapability::ResourceRead,
        "resource.read",
    ),
    (
        PeerCapabilities::RESOURCE_QUERY,
        PeerCapability::ResourceQuery,
        "resource.query",
    ),
    (
        PeerCapabilities::STREAM_SUBSCRIBE,
        PeerCapability::StreamSubscribe,
        "stream.subscribe",
    ),
    (
        PeerCapabilities::TRANSITION_INVOKE,
        PeerCapability::TransitionInvoke,
        "transition.invoke",
    ),
    (
        PeerCapabilities::RESOURCE_REGISTER,
        PeerCapability::ResourceRegister,
        "resource.register",
    ),
    (
        PeerCapabilities::PEER_ADMIN,
        PeerCapability::PeerAdmin,
        "peer.admin",
    ),
];

#[allow(dead_code)]
impl PeerCapabilities {
    const RESOURCE_READ: u8 = 1 << 0;
    const RESOURCE_QUERY: u8 = 1 << 1;
    const STREAM_SUBSCRIBE: u8 = 1 << 2;
    const TRANSITION_INVOKE: u8 = 1 << 3;
    const RESOURCE_REGISTER: u8 = 1 << 4;
    const PEER_ADMIN: u8 = 1 << 5;

    pub(crate) fn empty() -> Self {
        Self(0)
    }

    pub(crate) fn all() -> Self {
        Self(
            CAPABILITY_AXES
                .iter()
                .fold(0, |bits, (bit, _, _)| bits | bit),
        )
    }

    pub(crate) fn resource_read() -> Self {
        Self(Self::RESOURCE_READ)
    }

    pub(crate) fn resource_query() -> Self {
        Self(Self::RESOURCE_QUERY)
    }

    pub(crate) fn stream_subscribe_capability() -> Self {
        Self(Self::STREAM_SUBSCRIBE)
    }

    pub(crate) fn transition_invoke() -> Self {
        Self(Self::TRANSITION_INVOKE)
    }

    pub(crate) fn resource_register() -> Self {
        Self(Self::RESOURCE_REGISTER)
    }

    pub(crate) fn peer_admin() -> Self {
        Self(Self::PEER_ADMIN)
    }

    pub(crate) fn parse_list(input: &str) -> Result<Self, PeerModelError> {
        let mut caps = Self::empty();
        for raw in input.split(',') {
            let name = raw.trim();
            if name.is_empty() {
                continue;
            }
            caps.0 |= match CAPABILITY_AXES.iter().find(|(_, _, axis)| *axis == name) {
                Some((bit, _, _)) => *bit,
                None => return Err(PeerModelError::UnknownCapability(name.to_string())),
            };
        }
        Ok(caps)
    }

    pub(crate) fn from_capabilities(caps: impl IntoIterator<Item = PeerCapability>) -> Self {
        let mut out = Self::empty();
        for cap in caps {
            out.0 |= cap.bit();
        }
        out
    }

    pub(crate) fn to_capabilities(self) -> Vec<PeerCapability> {
        CAPABILITY_AXES
            .iter()
            .filter_map(|(bit, cap, _)| (self.0 & bit != 0).then_some(*cap))
            .collect()
    }

    pub(crate) fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    pub(crate) fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }

    pub(crate) fn stream_subscribe(self) -> bool {
        self.0 & Self::STREAM_SUBSCRIBE != 0
    }

    pub(crate) fn is_empty(self) -> bool {
        self.0 == 0
    }

    fn names(self) -> impl Iterator<Item = &'static str> {
        CAPABILITY_AXES
            .into_iter()
            .filter_map(move |(bit, _, name)| (self.0 & bit != 0).then_some(name))
    }
}

impl fmt::Display for PeerCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names = self.names();
        if let Some(first) = names.next() {
            f.write_str(first)?;
            for name in names {
                f.write_str(",")?;
                f.write_str(name)?;
            }
        }
        Ok(())
    }
}

/// One peer capability axis. Granting a capability on an admission
/// ceiling (or requesting it on an outbound link) uses these typed
/// values; the canonical dotted names (`resource.read`, ...) remain the
/// wire format and round-trip through `Display`/`FromStr`.
///
/// `ResourceRegister` and `PeerAdmin` exist as axes but their remote
/// semantics are reserved (not yet implemented end-to-end).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PeerCapability {
    ResourceRead,
    ResourceQuery,
    StreamSubscribe,
    TransitionInvoke,
    ResourceRegister,
    PeerAdmin,
}

impl PeerCapability {
    fn axis(self) -> (u8, PeerCapability, &'static str) {
        *CAPABILITY_AXES
            .iter()
            .find(|(_, cap, _)| *cap == self)
            .expect("every PeerCapability variant has a CAPABILITY_AXES row")
    }

    fn as_dotted_name(self) -> &'static str {
        self.axis().2
    }

    fn bit(self) -> u8 {
        self.axis().0
    }
}

impl fmt::Display for PeerCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_dotted_name())
    }
}

impl std::str::FromStr for PeerCapability {
    type Err = PeerConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CAPABILITY_AXES
            .iter()
            .find(|(_, _, name)| *name == s)
            .map(|(_, cap, _)| *cap)
            .ok_or_else(|| PeerConfigError::UnknownCapability(s.to_string()))
    }
}

/// Errors surfaced when constructing peer admission or link
/// configuration. Validation happens at config construction so a bad
/// value fails at the line that contains it — never logged and skipped.
#[non_exhaustive]
#[derive(Debug, Clone, Error)]
pub enum PeerConfigError {
    #[error("invalid peer route name: `{0}`")]
    InvalidRouteName(String),
    #[error("invalid gateway url: {0}")]
    InvalidUrl(String),
    #[error("unknown peer capability: `{0}`")]
    UnknownCapability(String),
}

impl From<PeerModelError> for PeerConfigError {
    fn from(err: PeerModelError) -> Self {
        match err {
            // Reachable from `PeerAdmission::shared_token` / `PeerLink::new`.
            PeerModelError::EmptyRouteName => Self::InvalidRouteName(String::new()),
            PeerModelError::InvalidRouteName(name) => Self::InvalidRouteName(name),
            PeerModelError::InvalidUrl(message) => Self::InvalidUrl(message),
            PeerModelError::UnknownCapability(name) => Self::UnknownCapability(name),
            // Node/peer ids never flow through the public config
            // constructors; totality keeps this conversion honest.
            other @ (PeerModelError::EmptyNodeId | PeerModelError::EmptyPeerId) => {
                Self::InvalidRouteName(other.to_string())
            }
        }
    }
}

/// Inbound peer admission: a shared token bound to one `/peers/{route}`
/// route, with an explicit capability ceiling.
///
/// The default ceiling is [`PeerCapability::ResourceRead`] only.
/// Widening is always a visible act: [`PeerAdmission::allow`]
/// **replaces** the ceiling with exactly the set you pass.
#[derive(Debug, Clone)]
pub struct PeerAdmission {
    inner: PeerAdmissionConfig,
}

impl PeerAdmission {
    /// Admit peers that present this shared token on `/peers/{route_name}`.
    ///
    /// Fails if `route_name` is not a valid URL path segment.
    pub fn shared_token(
        route_name: impl Into<String>,
        token_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> Result<Self, PeerConfigError> {
        let inner = PeerAdmissionConfig::shared_token(route_name, token_id, secret)
            .map_err(PeerConfigError::from)?;
        Ok(Self { inner })
    }

    /// Pin this token to one expected node id: an exact string match
    /// against the connecting peer's self-asserted node id header.
    /// This is a misconfiguration guard under token possession, not
    /// proof of node identity — a token holder can present any node id.
    #[must_use]
    pub fn expected_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.inner = self.inner.expected_node_id(node_id);
        self
    }

    /// Replace the capability ceiling with exactly this set.
    #[must_use]
    pub fn allow(mut self, capabilities: impl IntoIterator<Item = PeerCapability>) -> Self {
        self.inner.allowed_capabilities = PeerCapabilities::from_capabilities(capabilities);
        self
    }

    pub(crate) fn into_inner(self) -> PeerAdmissionConfig {
        self.inner
    }
}

/// Outbound peer link: dial a gateway's `/peers/{route}` with optional
/// token credentials, node identity, and a requested capability set
/// (default: [`PeerCapability::ResourceRead`] only). The acceptor
/// intersects the request with its configured ceiling; an empty
/// intersection is refused.
#[derive(Debug, Clone)]
pub struct PeerLink {
    inner: PeerLinkConfig,
}

impl PeerLink {
    /// Link to the gateway at `gateway_url` as `/peers/{route_name}`.
    ///
    /// Fails if `gateway_url` does not parse as a URL or `route_name`
    /// is not a valid URL path segment.
    pub fn new(
        gateway_url: impl AsRef<str>,
        route_name: impl Into<String>,
    ) -> Result<Self, PeerConfigError> {
        let inner = PeerLinkConfig::new(gateway_url, route_name).map_err(PeerConfigError::from)?;
        Ok(Self { inner })
    }

    /// Present this shared token when dialing.
    #[must_use]
    pub fn token(mut self, token_id: impl Into<String>, secret: impl Into<String>) -> Self {
        self.inner = self.inner.token(token_id, secret);
        self
    }

    /// Present this stable node id to the acceptor (self-asserted; the
    /// acceptor may pin it with [`PeerAdmission::expected_node_id`]).
    #[must_use]
    pub fn node_id(mut self, node_id: impl Into<String>) -> Self {
        self.inner = self.inner.node_id(node_id);
        self
    }

    /// Present this human-readable display name to the acceptor.
    #[must_use]
    pub fn node_name(mut self, node_name: impl Into<String>) -> Self {
        self.inner = self.inner.node_name(node_name);
        self
    }

    /// Replace the requested capability set with exactly this set.
    #[must_use]
    pub fn request_capabilities(
        mut self,
        capabilities: impl IntoIterator<Item = PeerCapability>,
    ) -> Self {
        self.inner.requested_capabilities = PeerCapabilities::from_capabilities(capabilities);
        self
    }

    pub(crate) fn into_inner(self) -> PeerLinkConfig {
        self.inner
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Peer {
    pub peer_id: PeerId,
    pub route_name: RouteName,
    pub expected_node_id: Option<crate::events::NodeId>,
    pub display_name: Option<String>,
    /// Negotiated capabilities are the ceiling for gateway forwarding
    /// and for rendering remote hypermedia affordances.
    pub allowed_capabilities: PeerCapabilities,
}

#[allow(dead_code)]
impl Peer {
    pub(crate) fn configured(
        peer_id: impl Into<String>,
        route_name: impl Into<String>,
        expected_node_id: Option<&str>,
    ) -> Result<Self, PeerModelError> {
        Ok(Self {
            peer_id: PeerId::new(peer_id)?,
            route_name: RouteName::new(route_name)?,
            expected_node_id: expected_node_id.map(crate::events::NodeId::new),
            display_name: None,
            allowed_capabilities: PeerCapabilities::all(),
        })
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct PeerTokenVerifier {
    secret: String,
}

impl PeerTokenVerifier {
    pub(crate) fn new(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn verify(&self, candidate: &str) -> bool {
        self.secret == candidate
    }
}

impl fmt::Debug for PeerTokenVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PeerTokenVerifier(<redacted>)")
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct PeerAdmissionConfig {
    pub allowed_route_name: RouteName,
    pub token_id: String,
    pub token_verifier: PeerTokenVerifier,
    pub expected_node_id: Option<crate::events::NodeId>,
    pub allowed_capabilities: PeerCapabilities,
}

#[allow(dead_code)]
impl PeerAdmissionConfig {
    pub(crate) fn shared_token(
        route_name: impl Into<String>,
        token_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> Result<Self, PeerModelError> {
        Ok(Self {
            allowed_route_name: RouteName::new(route_name)?,
            token_id: token_id.into(),
            token_verifier: PeerTokenVerifier::new(secret),
            expected_node_id: None,
            allowed_capabilities: PeerCapabilities::resource_read(),
        })
    }

    pub(crate) fn expected_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.expected_node_id = Some(crate::events::NodeId::new(node_id));
        self
    }
}

/// Explicit opt-in policy for admitting peers without token-bound
/// admission. Local development only; the capability ceiling is the
/// allowed and negotiated set for every unauthenticated peer.
#[derive(Debug, Clone)]
pub(crate) struct UnauthenticatedPeerPolicy {
    pub allowed_capabilities: PeerCapabilities,
}

impl UnauthenticatedPeerPolicy {
    pub(crate) fn local_development() -> Self {
        Self {
            allowed_capabilities: PeerCapabilities::all(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct PeerLinkConfig {
    pub gateway_url: Url,
    pub route_name: RouteName,
    pub token_id: Option<String>,
    pub token_secret: Option<String>,
    pub local_node_id: Option<crate::events::NodeId>,
    pub local_node_name: Option<String>,
    pub requested_capabilities: PeerCapabilities,
}

#[allow(dead_code)]
impl PeerLinkConfig {
    pub(crate) fn new(
        gateway_url: impl AsRef<str>,
        route_name: impl Into<String>,
    ) -> Result<Self, PeerModelError> {
        let gateway_url = Url::parse(gateway_url.as_ref())
            .map_err(|err| PeerModelError::InvalidUrl(err.to_string()))?;
        Ok(Self {
            gateway_url,
            route_name: RouteName::new(route_name)?,
            token_id: None,
            token_secret: None,
            local_node_id: None,
            local_node_name: None,
            requested_capabilities: PeerCapabilities::resource_read(),
        })
    }

    pub(crate) fn token(mut self, token_id: impl Into<String>, secret: impl Into<String>) -> Self {
        self.token_id = Some(token_id.into());
        self.token_secret = Some(secret.into());
        self
    }

    pub(crate) fn node_id(mut self, node_id: impl Into<String>) -> Self {
        self.local_node_id = Some(crate::events::NodeId::new(node_id));
        self
    }

    pub(crate) fn node_name(mut self, node_name: impl Into<String>) -> Self {
        self.local_node_name = Some(node_name.into());
        self
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdmittedPeerConnection {
    pub route_name: String,
    pub peer_id: String,
    pub token_id: Option<String>,
    pub connection_id: Uuid,
    pub node_id: Option<String>,
    pub display_name: Option<String>,
    pub allowed_capabilities: PeerCapabilities,
    pub negotiated_capabilities: PeerCapabilities,
}

#[allow(dead_code)]
impl AdmittedPeerConnection {
    pub(crate) fn unauthenticated(
        route_name: impl Into<String>,
        connection_id: Uuid,
        allowed_capabilities: PeerCapabilities,
    ) -> Self {
        let route_name = route_name.into();
        Self {
            peer_id: format!("peer-{route_name}"),
            route_name,
            token_id: None,
            connection_id,
            node_id: None,
            display_name: None,
            allowed_capabilities,
            negotiated_capabilities: allowed_capabilities,
        }
    }

    pub(crate) fn token_bound(
        route_name: impl Into<String>,
        token_id: impl Into<String>,
        connection_id: Uuid,
        node_id: impl Into<String>,
        display_name: Option<String>,
        allowed_capabilities: PeerCapabilities,
        negotiated_capabilities: PeerCapabilities,
    ) -> Self {
        let route_name = route_name.into();
        let token_id = token_id.into();
        Self {
            peer_id: format!("peer-{route_name}-{token_id}"),
            route_name,
            token_id: Some(token_id),
            connection_id,
            node_id: Some(node_id.into()),
            display_name,
            allowed_capabilities,
            negotiated_capabilities,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum PeerConnectionStatus {
    Opening,
    Connected,
    Disconnected,
    Failed,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PeerConnection {
    pub connection_id: Uuid,
    pub peer_id: PeerId,
    pub route_name: RouteName,
    pub status: PeerConnectionStatus,
    /// Negotiated capabilities are the ceiling for gateway forwarding
    /// and for rendering remote hypermedia affordances.
    pub negotiated_capabilities: PeerCapabilities,
}

#[allow(dead_code)]
impl PeerConnection {
    pub(crate) fn opening(
        peer_id: PeerId,
        route_name: impl Into<String>,
        connection_id: Uuid,
    ) -> Result<Self, PeerModelError> {
        Ok(Self {
            connection_id,
            peer_id,
            route_name: RouteName::new(route_name)?,
            status: PeerConnectionStatus::Opening,
            negotiated_capabilities: PeerCapabilities::empty(),
        })
    }
}

/// Outbound peer client. Establishes the tunnel, hosts a local H2
/// server on top of the upgraded stream, and routes inbound H2 requests
/// through the supplied axum Router.
pub(crate) struct PeerClient {
    pub remote_url: Url,
    pub local_name: String,
    pub router: Router,
    pub peer_init: PeerInitState,
    admission: Option<PeerLinkConfig>,
    pub shutdown: Arc<tokio::sync::Notify>,
}

impl PeerClient {
    pub(crate) fn new(
        remote_url: Url,
        local_name: String,
        router: Router,
        peer_init: PeerInitState,
    ) -> Self {
        Self {
            remote_url,
            local_name,
            router,
            peer_init,
            admission: None,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub(crate) fn from_link(
        link: PeerLinkConfig,
        router: Router,
        peer_init: PeerInitState,
    ) -> Self {
        Self {
            remote_url: link.gateway_url.clone(),
            local_name: link.route_name.as_str().to_string(),
            router,
            peer_init,
            admission: Some(link),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Run the client with infinite reconnect. Returns when shutdown
    /// is signaled (via the shutdown handle).
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut attempt = 0u32;
            loop {
                let connection_id = Uuid::new_v4();
                let shutdown = self.shutdown.clone();
                tokio::select! {
                    biased;
                    _ = shutdown.notified() => {
                        tracing::info!(peer = %self.remote_url, "peer client shutting down");
                        return;
                    }
                    res = self.attempt_once(connection_id) => {
                        match res {
                            Ok(()) => {
                                tracing::info!(peer = %self.remote_url, "peer link closed");
                                attempt = 0;
                            }
                            Err(e) => {
                                tracing::warn!(peer = %self.remote_url, error = %e, attempt, "peer link failed");
                            }
                        }
                    }
                }
                attempt = attempt.saturating_add(1);
                let backoff = backoff_ms(attempt);
                tokio::select! {
                    biased;
                    _ = self.shutdown.notified() => return,
                    _ = tokio::time::sleep(Duration::from_millis(backoff)) => {}
                }
            }
        })
    }

    async fn attempt_once(&self, connection_id: Uuid) -> Result<(), PeerError> {
        self.peer_init.register(connection_id);
        let result = self.attempt_registered(connection_id).await;
        self.peer_init.consume(&connection_id);
        result
    }

    async fn attempt_registered(&self, connection_id: Uuid) -> Result<(), PeerError> {
        let requested_capabilities = self
            .admission
            .as_ref()
            .map(|link| link.requested_capabilities.to_string());
        let admission = self.admission.as_ref().and_then(|link| {
            let token_id = link.token_id.as_deref()?;
            let token_secret = link.token_secret.as_deref()?;
            let node_id = link.local_node_id.as_ref()?;
            Some(crate::tunnel::InitiatorAdmission {
                token_id,
                token_secret,
                node_id: node_id.as_str(),
                node_name: link.local_node_name.as_deref(),
                requested_capabilities: requested_capabilities.as_deref().unwrap_or_default(),
            })
        });
        let ready = crate::tunnel::dial_initiator(
            self.remote_url.as_str(),
            &self.local_name,
            connection_id,
            admission,
        )
        .await?;
        let service = self.router.clone().into_service::<hyper::body::Incoming>();
        let svc = hyper_util::service::TowerToHyperService::new(service);
        let result = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(ready.upgraded, svc)
            .await
            .map_err(|e| crate::tunnel::TunnelError::Upgrade(format!("h2 serve: {e}")));
        result?;
        Ok(())
    }
}

/// Acceptor-side state used to track in-flight peer upgrades and to
/// hold the live HTTP/2 `SendRequest` for forwarding queries from the
/// cloud's HTTP router to the hub.
#[derive(Clone, Default)]
pub struct PeerAcceptors {
    inner: Arc<tokio::sync::Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    senders: Arc<tokio::sync::Mutex<HashMap<String, SendRequest<axum::body::Body>>>>,
    contexts: Arc<tokio::sync::Mutex<HashMap<String, AdmittedPeerConnection>>>,
    confirmations: Arc<std::sync::atomic::AtomicU64>,
    notify: Arc<tokio::sync::Notify>,
    repositories: Arc<std::sync::Mutex<Option<Arc<DefaultRepositories>>>>,
}

impl PeerAcceptors {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_repositories(&self, repositories: Arc<DefaultRepositories>) {
        *self.repositories.lock().unwrap() = Some(repositories);
    }

    #[allow(dead_code)]
    pub fn confirmation_count(&self) -> u64 {
        self.confirmations.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Wait until at least one peer confirmation has happened, or
    /// timeout. Returns true on success.
    #[allow(dead_code)]
    pub async fn wait_for_first(&self, timeout: std::time::Duration) -> bool {
        if self.confirmation_count() > 0 {
            return true;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        match tokio::time::timeout(timeout, notified.as_mut()).await {
            Ok(_) => self.confirmation_count() > 0,
            Err(_) => false,
        }
    }

    /// Called by the HTTP layer once a peer's WS upgrade has produced
    /// an `Upgraded` stream.
    pub async fn on_upgraded(
        &self,
        admitted: AdmittedPeerConnection,
        upgraded: hyper::upgrade::Upgraded,
    ) {
        let acceptors = self.clone();
        let peer_name = admitted.route_name.clone();
        let connection_id = admitted.connection_id;
        let peer_name_for_task = peer_name.clone();
        let admitted_for_task = admitted.clone();
        let task = tokio::spawn(async move {
            let repositories_snapshot = acceptors.repositories.lock().unwrap().clone();
            acceptors
                .contexts
                .lock()
                .await
                .insert(peer_name_for_task.clone(), admitted_for_task.clone());
            let drive_succeeded = match drive_acceptor(
                peer_name_for_task.clone(),
                connection_id,
                upgraded,
                acceptors.senders.clone(),
                acceptors.contexts.clone(),
            )
            .await
            {
                Ok(()) => {
                    write_peer(
                        repository_ref(repositories_snapshot.as_deref()),
                        &admitted_for_task,
                        PeerConnectionStatus::Connected,
                    );
                    acceptors
                        .confirmations
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    acceptors.notify.notify_waiters();
                    true
                }
                Err(e) => {
                    acceptors.contexts.lock().await.remove(&peer_name_for_task);
                    write_peer(
                        repository_ref(repositories_snapshot.as_deref()),
                        &admitted_for_task,
                        PeerConnectionStatus::Failed,
                    );
                    tracing::warn!(peer = %peer_name_for_task, error = %e, "peer acceptor failed");
                    false
                }
            };
            let mut inner = acceptors.inner.lock().await;
            inner.remove(&peer_name_for_task);
            // The senders entry is cleaned up by the conn-cleanup task
            // inside drive_acceptor; once that fires, transition to
            // disconnected.
            let senders = acceptors.senders.lock().await;
            if should_record_disconnected(
                drive_succeeded,
                senders.contains_key(&peer_name_for_task),
            ) {
                write_peer(
                    repository_ref(repositories_snapshot.as_deref()),
                    &admitted_for_task,
                    PeerConnectionStatus::Disconnected,
                );
            }
        });
        let mut inner = self.inner.lock().await;
        inner.insert(peer_name, task);
    }

    #[allow(dead_code)]
    pub async fn active(&self) -> Vec<String> {
        self.inner.lock().await.keys().cloned().collect()
    }

    #[allow(dead_code)]
    pub(crate) async fn peer_context(&self, route_name: &str) -> Option<AdmittedPeerConnection> {
        self.contexts.lock().await.get(route_name).cloned()
    }
}

/// Cloud-side query forwarder. Implements `crate::http::PeerSenders` so
/// the router can forward requests for `/servers/{peer-name}/...`
/// through the established H2 tunnel.
#[async_trait::async_trait]
impl PeerSenders for PeerAcceptors {
    async fn sender(&self, name: &str) -> Option<SendRequest<axum::body::Body>> {
        let map = self.senders.lock().await;
        map.get(name).cloned()
    }

    async fn names(&self) -> Vec<String> {
        let map = self.senders.lock().await;
        map.keys().cloned().collect()
    }

    async fn peer_context(&self, name: &str) -> Option<AdmittedPeerConnection> {
        self.contexts.lock().await.get(name).cloned()
    }

    async fn has_active_peer(&self, name: &str) -> bool {
        if self.senders.lock().await.contains_key(name) {
            return true;
        }
        self.inner.lock().await.contains_key(name)
    }
}

async fn drive_acceptor(
    peer_name: String,
    connection_id: Uuid,
    upgraded: hyper::upgrade::Upgraded,
    senders: Arc<tokio::sync::Mutex<HashMap<String, SendRequest<axum::body::Body>>>>,
    contexts: Arc<tokio::sync::Mutex<HashMap<String, AdmittedPeerConnection>>>,
) -> Result<(), PeerError> {
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, axum::body::Body>(upgraded)
        .await?;
    let peer_name_for_cleanup = peer_name.clone();
    let senders_for_cleanup = senders.clone();
    let _conn_task = tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::debug!(%e, "acceptor h2 connection ended");
        }
        let mut s = senders_for_cleanup.lock().await;
        s.remove(&peer_name_for_cleanup);
        contexts.lock().await.remove(&peer_name_for_cleanup);
    });

    // Send confirmation.
    let req = Request::builder()
        .method("GET")
        .uri(format!(
            "http://{}.peer.boardwalk.invalid/_initiate_peer/{}",
            urlencoding::encode(&peer_name),
            connection_id
        ))
        .body(axum::body::Body::empty())
        .expect("request");
    let resp = sender.send_request(req).await?;
    if resp.status() != http::StatusCode::OK {
        return Err(PeerError::Tunnel(crate::tunnel::TunnelError::Response(
            format!("confirm status {}", resp.status()),
        )));
    }
    let _body = resp.collect().await?.to_bytes();
    tracing::info!(peer = %peer_name, %connection_id, "peer confirmed");

    // Register the sender so the cloud's router can forward through it.
    // The clone keeps the connection alive; when the entry is removed
    // (by the conn cleanup task or by a reconnect overwriting) and the
    // last clone is dropped, the connection closes.
    {
        let mut s = senders.lock().await;
        s.insert(peer_name.clone(), sender);
    }
    Ok(())
}

fn should_record_disconnected(drive_succeeded: bool, sender_active: bool) -> bool {
    drive_succeeded && !sender_active
}

fn repository_ref(repositories: Option<&DefaultRepositories>) -> Option<&dyn Repositories> {
    repositories.map(|repositories| repositories as &dyn Repositories)
}

fn write_peer(
    repositories: Option<&dyn Repositories>,
    admitted: &AdmittedPeerConnection,
    status: PeerConnectionStatus,
) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let Some(repositories) = repositories else {
        return;
    };
    if let Err(e) = repositories.peer_configs().put(PeerConfigRecord {
        peer_id: admitted.peer_id.clone(),
        route_name: admitted.route_name.clone(),
        node_id: admitted.node_id.clone(),
        display_name: admitted.display_name.clone(),
        allowed_capabilities: admitted.allowed_capabilities,
        updated_ms: now_ms,
    }) {
        tracing::warn!(error = %e, "failed to persist peer config");
    }
    if let Err(e) = repositories
        .peer_connection_status()
        .put_latest(PeerConnectionStatusRecord {
            connection_id: admitted.connection_id.to_string(),
            peer_id: admitted.peer_id.clone(),
            route_name: admitted.route_name.clone(),
            direction: PeerConnectionDirection::Acceptor,
            status,
            negotiated_capabilities: admitted.negotiated_capabilities,
            updated_ms: now_ms,
        })
    {
        tracing::warn!(error = %e, "failed to persist peer connection status");
    }
}

fn backoff_ms(attempt: u32) -> u64 {
    let base: u64 = 100;
    let max: u64 = 30_000;
    let computed = base.saturating_mul(1u64 << attempt.min(10));
    let jitter = xorshift() % 1000;
    computed.min(max).saturating_add(jitter)
}

fn xorshift() -> u64 {
    use std::cell::Cell;
    use std::time::{SystemTime, UNIX_EPOCH};
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0xdead_beef_dead_beef) };
    }
    STATE.with(|s| {
        let mut x = s.get();
        if x == 0 {
            x = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1)
                .max(1);
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x
    })
}

#[cfg(test)]
mod peer_model_tests {
    use super::*;

    #[test]
    fn peer_model_node_identity_separates_stable_node_id_from_route_name() {
        let identity = NodeIdentity::new("node-hub-1", "Kitchen Hub", "hub").unwrap();

        assert_eq!(identity.node_id.as_str(), "node-hub-1");
        assert_eq!(identity.display_name, "Kitchen Hub");
        assert_eq!(identity.route_name.as_str(), "hub");
    }

    #[test]
    fn peer_model_route_name_rejects_unsafe_path_segment_chars() {
        for name in [
            "hub name", "hub/name", "hub?x", "hub#frag", "hub%2f", "hub\n",
        ] {
            assert!(
                RouteName::new(name).is_err(),
                "route name {name:?} should be rejected"
            );
        }
        for name in ["hub", "peer-hub", "peer.hub", "peer_hub", "peer~hub"] {
            assert!(
                RouteName::new(name).is_ok(),
                "route name {name:?} should be accepted"
            );
        }
    }

    #[test]
    fn unauthenticated_peer_policy_local_development_ceiling_is_explicit_full_set() {
        let policy = UnauthenticatedPeerPolicy::local_development();
        assert_eq!(policy.allowed_capabilities, PeerCapabilities::all());
    }

    #[test]
    fn peer_model_capabilities_parse_and_intersect_known_names() {
        let requested = PeerCapabilities::parse_list("resource.read,stream.subscribe").unwrap();
        let allowed = PeerCapabilities::parse_list("resource.read,transition.invoke").unwrap();

        assert_eq!(
            requested.intersection(allowed),
            PeerCapabilities::resource_read()
        );
    }

    #[test]
    fn peer_model_capabilities_reject_unknown_names() {
        assert!(PeerCapabilities::parse_list("resource.read,unknown").is_err());
    }

    #[test]
    fn peer_model_peer_connection_has_session_id_separate_from_peer_id() {
        let peer = Peer::configured("peer-hub", "hub", Some("node-hub-1")).unwrap();
        let conn = PeerConnection::opening(peer.peer_id.clone(), "hub", Uuid::new_v4()).unwrap();

        assert_ne!(peer.peer_id.to_string(), conn.connection_id.to_string());
    }

    #[test]
    fn failed_acceptor_setup_does_not_immediately_overwrite_failed_status() {
        assert!(!should_record_disconnected(false, false));
        assert!(should_record_disconnected(true, false));
        assert!(!should_record_disconnected(true, true));
    }

    #[test]
    fn peer_capability_display_round_trips_canonical_names() {
        let all = [
            (PeerCapability::ResourceRead, "resource.read"),
            (PeerCapability::ResourceQuery, "resource.query"),
            (PeerCapability::StreamSubscribe, "stream.subscribe"),
            (PeerCapability::TransitionInvoke, "transition.invoke"),
            (PeerCapability::ResourceRegister, "resource.register"),
            (PeerCapability::PeerAdmin, "peer.admin"),
        ];
        for (cap, name) in all {
            assert_eq!(cap.to_string(), name);
            assert_eq!(name.parse::<PeerCapability>().unwrap(), cap);
        }
        assert!("resource.write".parse::<PeerCapability>().is_err());
    }

    #[test]
    fn peer_capability_converts_to_internal_bitset() {
        let caps = PeerCapabilities::from_capabilities([
            PeerCapability::ResourceRead,
            PeerCapability::TransitionInvoke,
        ]);
        assert!(caps.contains(PeerCapabilities::resource_read()));
        assert!(caps.contains(PeerCapabilities::transition_invoke()));
        assert!(!caps.contains(PeerCapabilities::peer_admin()));
    }

    #[test]
    fn internal_bitset_enumerates_back_to_capability_variants() {
        let caps = PeerCapabilities::resource_read().intersection(PeerCapabilities::all());
        assert_eq!(caps.to_capabilities(), vec![PeerCapability::ResourceRead]);
    }

    #[test]
    fn peer_admission_defaults_to_resource_read_ceiling() {
        let admission = PeerAdmission::shared_token("hub", "kid-1", "secret").unwrap();
        let inner = admission.into_inner();
        assert_eq!(
            inner.allowed_capabilities,
            PeerCapabilities::resource_read()
        );
        assert!(inner.expected_node_id.is_none());
    }

    #[test]
    fn peer_admission_allow_replaces_the_ceiling() {
        let admission = PeerAdmission::shared_token("hub", "kid-1", "secret")
            .unwrap()
            .allow([
                PeerCapability::StreamSubscribe,
                PeerCapability::TransitionInvoke,
            ]);
        let inner = admission.into_inner();
        assert!(
            !inner
                .allowed_capabilities
                .contains(PeerCapabilities::resource_read())
        );
        assert!(
            inner
                .allowed_capabilities
                .contains(PeerCapabilities::transition_invoke())
        );
    }

    #[test]
    fn peer_admission_rejects_invalid_route_name_at_construction() {
        let err = PeerAdmission::shared_token("hub name", "kid-1", "secret").unwrap_err();
        assert!(matches!(err, PeerConfigError::InvalidRouteName(_)));
    }

    #[test]
    fn peer_admission_expected_node_id_pins_the_binding() {
        let admission = PeerAdmission::shared_token("hub", "kid-1", "secret")
            .unwrap()
            .expected_node_id("node-hub-7f3a");
        assert_eq!(
            admission
                .into_inner()
                .expected_node_id
                .as_ref()
                .map(|id| id.as_str()),
            Some("node-hub-7f3a")
        );
    }

    #[test]
    fn peer_link_defaults_request_to_resource_read() {
        let link = PeerLink::new("ws://127.0.0.1:4444", "hub").unwrap();
        let inner = link.into_inner();
        assert_eq!(
            inner.requested_capabilities,
            PeerCapabilities::resource_read()
        );
        assert!(inner.token_id.is_none());
    }

    #[test]
    fn peer_link_rejects_invalid_url_at_construction() {
        let err = PeerLink::new("not a url", "hub").unwrap_err();
        assert!(matches!(err, PeerConfigError::InvalidUrl(_)));
    }

    #[test]
    fn peer_link_carries_token_node_identity_and_request() {
        let link = PeerLink::new("ws://127.0.0.1:4444", "hub")
            .unwrap()
            .token("kid-1", "secret")
            .node_id("node-hub-7f3a")
            .node_name("author-hub")
            .request_capabilities([
                PeerCapability::ResourceRead,
                PeerCapability::TransitionInvoke,
            ]);
        let inner = link.into_inner();
        assert_eq!(inner.token_id.as_deref(), Some("kid-1"));
        assert_eq!(
            inner.local_node_id.as_ref().map(|id| id.as_str()),
            Some("node-hub-7f3a")
        );
        assert_eq!(inner.local_node_name.as_deref(), Some("author-hub"));
        assert!(
            inner
                .requested_capabilities
                .contains(PeerCapabilities::transition_invoke())
        );
    }

    #[test]
    fn peer_config_error_messages_name_the_bad_value() {
        let err = PeerConfigError::InvalidRouteName("hub name".to_string());
        assert!(err.to_string().contains("hub name"));
    }
}
