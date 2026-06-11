//! Characterization tests for the peer admission boundary.

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use bytes::Bytes;
use http::StatusCode;
use http::header::{CONNECTION, HOST, HeaderName, HeaderValue, UPGRADE};
use http_body_util::Empty;
use hyper::Request;
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use uuid::Uuid;

use crate::Boardwalk;
use crate::peer::{
    PeerAdmission, PeerCapabilities, PeerCapability, PeerConnectionStatus, PeerLink,
};
use crate::persistence::{PeerConfigRecord, PeerConnectionStatusRecord};
use crate::server::Built;
use crate::tunnel::SUBPROTOCOL;

#[tokio::test]
async fn admitted_peer_sends_identity_and_gets_negotiated_capabilities() {
    let dir = tempfile::tempdir().unwrap();
    let registry_path = dir.path().join("boardwalk.redb");
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .persist(&registry_path)
            .accept_peer(
                PeerAdmission::shared_token("hub", "kid-1", "secret")
                    .unwrap()
                    .allow([
                        PeerCapability::ResourceRead,
                        PeerCapability::StreamSubscribe,
                    ]),
            ),
    )
    .await;

    let _hub = Boardwalk::new()
        .name("hub")
        .node_id("node-hub-1")
        .link_peer(
            PeerLink::new(format!("http://{}", cloud.addr), "hub")
                .unwrap()
                .token("kid-1", "secret")
                .node_id("node-hub-1")
                .node_name("Kitchen Hub")
                .request_capabilities([
                    PeerCapability::ResourceRead,
                    PeerCapability::TransitionInvoke,
                ]),
        )
        .build()
        .unwrap();

    assert!(
        cloud
            .built
            .acceptors
            .wait_for_first(Duration::from_secs(5))
            .await,
        "cloud should confirm the admitted peer"
    );

    let record = wait_for_peer_record(&cloud.built, "hub").await;
    assert_eq!(record.node_id.as_deref(), Some("node-hub-1"));
    assert_eq!(record.display_name.as_deref(), Some("Kitchen Hub"));

    let connection = cloud
        .built
        .repositories()
        .unwrap()
        .peer_connection_status()
        .latest_by_route("hub")
        .unwrap()
        .expect("peer connection");
    assert_eq!(
        connection.negotiated_capabilities,
        PeerCapabilities::resource_read()
    );
}

#[tokio::test]
async fn peer_upgrade_without_admission_token_is_rejected_before_upgrade() {
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .accept_peer_token("hub", "kid-1", "secret"),
    )
    .await;

    let upgrade = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .node_id("node-hub-1")
            .request_capabilities(["resource.read"])
            .without_token(),
    )
    .await;

    assert_eq!(upgrade.status, StatusCode::UNAUTHORIZED);
    assert_denied_without_websocket_upgrade_headers(&upgrade);
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

#[tokio::test]
async fn builder_opt_in_carries_unauthenticated_policy_into_built_state() {
    let built = Boardwalk::new()
        .name("cloud")
        .allow_unauthenticated_local_peers()
        .build()
        .unwrap();

    let policy = built
        .unauthenticated_local_peers
        .as_ref()
        .expect("opt-in stores the unauthenticated peer policy");
    assert_eq!(policy.allowed_capabilities, PeerCapabilities::all());

    let default_built = Boardwalk::new().name("cloud").build().unwrap();
    assert!(default_built.unauthenticated_local_peers.is_none());
}

#[tokio::test]
async fn peer_upgrade_without_admission_config_is_refused_before_upgrade() {
    // Cloud with no admission config and no unauthenticated opt-in:
    // every peer upgrade must be refused before 101.
    let cloud = serve(Boardwalk::new().name("cloud")).await;

    let upgrade = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .node_id("node-hub-1")
            .request_capabilities(["resource.read"])
            .without_token(),
    )
    .await;

    assert_eq!(upgrade.status, StatusCode::FORBIDDEN);
    assert_denied_without_websocket_upgrade_headers(&upgrade);
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

#[tokio::test]
async fn linked_hub_is_not_confirmed_by_cloud_without_admission_config() {
    let cloud = serve(Boardwalk::new().name("cloud")).await;

    // End-to-end `.link()` flow: the hub dials, the cloud must refuse.
    let _hub = Boardwalk::new()
        .name("hub")
        .link(format!("http://{}", cloud.addr))
        .build()
        .unwrap();

    assert!(
        !cloud
            .built
            .acceptors
            .wait_for_first(Duration::from_millis(750))
            .await,
        "cloud must not confirm an unauthenticated peer without explicit opt-in"
    );
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

#[tokio::test]
async fn opted_in_cloud_admits_linked_hub_with_local_development_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let registry_path = dir.path().join("boardwalk.redb");
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .persist(&registry_path)
            .allow_unauthenticated_local_peers(),
    )
    .await;

    let _hub = Boardwalk::new()
        .name("hub")
        .link(format!("http://{}", cloud.addr))
        .build()
        .unwrap();

    assert!(
        cloud
            .built
            .acceptors
            .wait_for_first(Duration::from_secs(5))
            .await,
        "opted-in cloud should confirm the unauthenticated local peer"
    );

    // The grant comes from the policy's explicit ceiling, not an
    // implicit constant: allowed and negotiated are the full set.
    let connection = cloud
        .built
        .repositories()
        .unwrap()
        .peer_connection_status()
        .latest_by_route("hub")
        .unwrap()
        .expect("peer connection");
    assert_eq!(connection.negotiated_capabilities, PeerCapabilities::all());
}

#[tokio::test]
async fn opt_in_does_not_bypass_configured_token_admission() {
    // Both opt-in AND a token config: token admission must still be
    // required; the opt-in only applies when no admission is configured.
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .allow_unauthenticated_local_peers()
            .accept_peer_token("hub", "kid-1", "secret"),
    )
    .await;

    let upgrade = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .node_id("node-hub-1")
            .request_capabilities(["resource.read"])
            .without_token(),
    )
    .await;

    assert_eq!(upgrade.status, StatusCode::UNAUTHORIZED);
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

#[tokio::test]
async fn peer_upgrade_with_wrong_bearer_token_is_rejected_before_upgrade() {
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .accept_peer_token("hub", "kid-1", "secret"),
    )
    .await;

    let upgrade = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .node_id("node-hub-1")
            .request_capabilities(["resource.read"])
            .token("kid-1", "wrong-secret"),
    )
    .await;

    assert_eq!(upgrade.status, StatusCode::UNAUTHORIZED);
    assert_denied_without_websocket_upgrade_headers(&upgrade);
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

#[tokio::test]
async fn peer_upgrade_with_unknown_token_id_is_rejected_before_upgrade() {
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .accept_peer_token("hub", "kid-1", "secret"),
    )
    .await;

    let upgrade = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .node_id("node-hub-1")
            .request_capabilities(["resource.read"])
            .token("kid-2", "secret"),
    )
    .await;

    assert_eq!(upgrade.status, StatusCode::UNAUTHORIZED);
    assert_denied_without_websocket_upgrade_headers(&upgrade);
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

#[tokio::test]
async fn token_configured_for_one_route_cannot_claim_another_route_name() {
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .accept_peer_token("hub-a", "kid-1", "secret"),
    )
    .await;

    let upgrade = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub-b", Uuid::new_v4())
            .node_id("node-hub-1")
            .request_capabilities(["resource.read"])
            .token("kid-1", "secret"),
    )
    .await;

    assert_eq!(upgrade.status, StatusCode::FORBIDDEN);
    assert_denied_without_websocket_upgrade_headers(&upgrade);
    assert_no_peer_sender_registered(&cloud.built, "hub-b").await;
}

#[tokio::test]
async fn expected_node_id_mismatch_is_rejected_before_upgrade() {
    let cloud = serve(
        Boardwalk::new().name("cloud").accept_peer(
            PeerAdmission::shared_token("hub", "kid-1", "secret")
                .unwrap()
                .expected_node_id("node-hub-1")
                .allow([PeerCapability::ResourceRead]),
        ),
    )
    .await;

    let upgrade = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .node_id("node-hub-2")
            .request_capabilities(["resource.read"])
            .token("kid-1", "secret"),
    )
    .await;

    assert_eq!(upgrade.status, StatusCode::FORBIDDEN);
    assert_denied_without_websocket_upgrade_headers(&upgrade);
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

#[tokio::test]
async fn empty_capability_intersection_is_rejected_before_upgrade() {
    let cloud = serve(
        Boardwalk::new().name("cloud").accept_peer(
            PeerAdmission::shared_token("hub", "kid-1", "secret")
                .unwrap()
                .allow([PeerCapability::ResourceRead]),
        ),
    )
    .await;

    let upgrade = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .node_id("node-hub-1")
            .request_capabilities(["transition.invoke"])
            .token("kid-1", "secret"),
    )
    .await;

    assert_eq!(upgrade.status, StatusCode::FORBIDDEN);
    assert_denied_without_websocket_upgrade_headers(&upgrade);
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

#[tokio::test]
async fn admitted_peer_identity_survives_reconnects_without_using_connection_id_as_peer_id() {
    let dir = tempfile::tempdir().unwrap();
    let registry_path = dir.path().join("boardwalk.redb");
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .persist(&registry_path)
            .accept_peer(
                PeerAdmission::shared_token("hub", "kid-1", "secret")
                    .unwrap()
                    .expected_node_id("node-hub-1")
                    .allow([PeerCapability::ResourceRead]),
            ),
    )
    .await;

    let first_connection_id = Uuid::new_v4();
    let first_peer = connect_admitted_peer(
        &cloud,
        PeerUpgradeAttempt::new("hub", first_connection_id)
            .node_id("node-hub-1")
            .node_name("Kitchen Hub")
            .request_capabilities(["resource.read"])
            .token("kid-1", "secret"),
    )
    .await;
    let first_record = wait_for_peer_record(&cloud.built, "hub").await;
    assert_eq!(
        first_record.peer_id, "peer-hub-kid-1",
        "durable token-bound peer identity should include route name and token id"
    );
    assert_eq!(first_record.route_name, "hub");
    assert_eq!(first_record.node_id.as_deref(), Some("node-hub-1"));
    assert_eq!(first_record.display_name.as_deref(), Some("Kitchen Hub"));
    assert!(
        first_record
            .allowed_capabilities
            .contains(PeerCapabilities::resource_read())
    );

    let first_status = cloud
        .built
        .repositories()
        .unwrap()
        .peer_connection_status()
        .latest_by_route("hub")
        .unwrap()
        .expect("first peer connection");
    assert_eq!(first_status.connection_id, first_connection_id.to_string());
    assert_eq!(first_status.peer_id, first_record.peer_id);
    assert_ne!(
        first_record.peer_id,
        first_connection_id.to_string(),
        "durable peer identity must not be the transient connection id"
    );

    first_peer.close();
    wait_for_no_peer_sender(&cloud.built, "hub").await;

    let second_connection_id = Uuid::new_v4();
    let _second_peer = connect_admitted_peer(
        &cloud,
        PeerUpgradeAttempt::new("hub", second_connection_id)
            .node_id("node-hub-1")
            .node_name("Kitchen Hub")
            .request_capabilities(["resource.read"])
            .token("kid-1", "secret"),
    )
    .await;
    let second_record = wait_for_peer_record(&cloud.built, "hub").await;
    let second_status = cloud
        .built
        .repositories()
        .unwrap()
        .peer_connection_status()
        .latest_by_route("hub")
        .unwrap()
        .expect("second peer connection");

    assert_eq!(
        second_record.peer_id, first_record.peer_id,
        "durable peer identity should remain stable across reconnects"
    );
    assert_ne!(second_record.peer_id, second_status.connection_id);
    assert_eq!(
        second_status.connection_id,
        second_connection_id.to_string()
    );
    assert_eq!(second_status.peer_id, second_record.peer_id);
    assert_ne!(
        second_status.connection_id, first_status.connection_id,
        "reconnect must create a new connection without replacing peer identity"
    );
}

#[tokio::test]
async fn failed_peer_confirmation_persists_failed_connection_status() {
    let dir = tempfile::tempdir().unwrap();
    let registry_path = dir.path().join("boardwalk.redb");
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .persist(&registry_path)
            .accept_peer(
                PeerAdmission::shared_token("hub", "kid-1", "secret")
                    .unwrap()
                    .expected_node_id("node-hub-1")
                    .allow([PeerCapability::ResourceRead]),
            ),
    )
    .await;

    let connection_id = Uuid::new_v4();
    let upgrade = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", connection_id)
            .node_id("node-hub-1")
            .request_capabilities(["resource.read"])
            .token("kid-1", "secret"),
    )
    .await;
    assert_eq!(upgrade.status, StatusCode::SWITCHING_PROTOCOLS);
    let upgraded = upgrade.upgraded.expect("upgraded stream");
    let _h2_server = tokio::spawn(async move {
        let service = Router::new()
            .route(
                "/_initiate_peer/{id}",
                get(|| async { StatusCode::NOT_FOUND }),
            )
            .into_service::<Incoming>();
        let service = hyper_util::service::TowerToHyperService::new(service);
        let _ = hyper::server::conn::http2::Builder::new(crate::tunnel::H2Executor::new())
            .serve_connection(upgraded, service)
            .await;
    });

    let connection =
        wait_for_peer_connection_status(&cloud.built, "hub", PeerConnectionStatus::Failed).await;
    assert_eq!(connection.connection_id, connection_id.to_string());
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

struct RunningBoardwalk {
    built: Built,
    addr: SocketAddr,
    _server: tokio::task::JoinHandle<()>,
}

async fn serve(boardwalk: Boardwalk) -> RunningBoardwalk {
    let built = boardwalk.build().unwrap();
    let router = built.router.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    RunningBoardwalk {
        built,
        addr,
        _server: server,
    }
}

struct FakePeer {
    _h2_server: tokio::task::JoinHandle<()>,
}

impl FakePeer {
    fn close(self) {
        self._h2_server.abort();
    }
}

async fn connect_admitted_peer(cloud: &RunningBoardwalk, attempt: PeerUpgradeAttempt) -> FakePeer {
    let confirmations = cloud.built.acceptors.confirmation_count();
    let upgrade = raw_peer_upgrade(cloud.addr, attempt).await;
    assert_eq!(upgrade.status, StatusCode::SWITCHING_PROTOCOLS);
    let upgraded = upgrade.upgraded.expect("upgraded stream");
    let h2_server = tokio::spawn(async move {
        let service = Router::new()
            .route("/_initiate_peer/{id}", get(|| async { StatusCode::OK }))
            .into_service::<Incoming>();
        let service = hyper_util::service::TowerToHyperService::new(service);
        let _ = hyper::server::conn::http2::Builder::new(crate::tunnel::H2Executor::new())
            .serve_connection(upgraded, service)
            .await;
    });

    wait_for_confirmation(&cloud.built, confirmations).await;
    FakePeer {
        _h2_server: h2_server,
    }
}

async fn wait_for_confirmation(built: &Built, previous: u64) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if built.acceptors.confirmation_count() > previous {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peer confirmation");
}

async fn wait_for_peer_record(built: &Built, route_name: &str) -> PeerConfigRecord {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(record) = built
                .repositories()
                .unwrap()
                .peer_configs()
                .get_by_route(route_name)
                .unwrap()
            {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peer record")
}

async fn wait_for_peer_connection_status(
    built: &Built,
    route_name: &str,
    status: PeerConnectionStatus,
) -> PeerConnectionStatusRecord {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(connection) = built
                .repositories()
                .unwrap()
                .peer_connection_status()
                .latest_by_route(route_name)
                .unwrap()
                && connection.status == status
            {
                return connection;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peer connection status")
}

async fn assert_no_peer_sender_registered(built: &Built, route_name: &str) {
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !built
            .acceptors
            .active()
            .await
            .contains(&route_name.to_string()),
        "peer sender should not be registered for rejected route {route_name:?}"
    );
}

async fn wait_for_no_peer_sender(built: &Built, route_name: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if !built
                .acceptors
                .active()
                .await
                .contains(&route_name.to_string())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peer sender cleanup");
}

struct PeerUpgradeResult {
    status: StatusCode,
    headers: http::HeaderMap,
    upgraded: Option<hyper::upgrade::Upgraded>,
}

async fn raw_peer_upgrade(addr: SocketAddr, attempt: PeerUpgradeAttempt) -> PeerUpgradeResult {
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::Builder::new()
        .handshake::<_, Empty<Bytes>>(io)
        .await
        .unwrap();
    let conn = conn.with_upgrades();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let key = crate::tunnel::ws_new_key();
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!(
            "/peers/{}?connectionId={}",
            urlencoding::encode(&attempt.route_name),
            attempt.connection_id
        ))
        .header(HOST, addr.to_string())
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header(
            HeaderName::from_static("sec-websocket-key"),
            HeaderValue::from_str(&key).unwrap(),
        )
        .header(HeaderName::from_static("sec-websocket-version"), "13")
        .header(
            HeaderName::from_static("sec-websocket-protocol"),
            HeaderValue::from_static(SUBPROTOCOL),
        );

    if let Some(node_id) = attempt.node_id {
        builder = builder.header("x-boardwalk-node-id", node_id);
    }
    if let Some(node_name) = attempt.node_name {
        builder = builder.header("x-boardwalk-node-name", node_name);
    }
    if let Some(token_id) = attempt.token_id {
        builder = builder.header("x-boardwalk-peer-token-id", token_id);
    }
    if let Some(secret) = attempt.secret {
        builder = builder.header("authorization", format!("Bearer {secret}"));
    }
    if let Some(capabilities) = attempt.requested_capabilities {
        builder = builder.header("x-boardwalk-peer-capabilities", capabilities);
    }

    let response = sender
        .send_request(builder.body(Empty::<Bytes>::new()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let upgraded = if status == StatusCode::SWITCHING_PROTOCOLS {
        Some(hyper::upgrade::on(response).await.unwrap())
    } else {
        None
    };

    PeerUpgradeResult {
        status,
        headers,
        upgraded,
    }
}

fn assert_denied_without_websocket_upgrade_headers(upgrade: &PeerUpgradeResult) {
    assert!(!upgrade.headers.contains_key("sec-websocket-accept"));
    assert!(!upgrade.headers.contains_key("sec-websocket-protocol"));
}

struct PeerUpgradeAttempt {
    route_name: String,
    connection_id: Uuid,
    node_id: Option<&'static str>,
    node_name: Option<&'static str>,
    token_id: Option<&'static str>,
    secret: Option<&'static str>,
    requested_capabilities: Option<String>,
}

impl PeerUpgradeAttempt {
    fn new(route_name: impl Into<String>, connection_id: Uuid) -> Self {
        Self {
            route_name: route_name.into(),
            connection_id,
            node_id: None,
            node_name: None,
            token_id: None,
            secret: None,
            requested_capabilities: None,
        }
    }

    fn node_id(mut self, node_id: &'static str) -> Self {
        self.node_id = Some(node_id);
        self
    }

    fn node_name(mut self, node_name: &'static str) -> Self {
        self.node_name = Some(node_name);
        self
    }

    fn token(mut self, token_id: &'static str, secret: &'static str) -> Self {
        self.token_id = Some(token_id);
        self.secret = Some(secret);
        self
    }

    fn request_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.requested_capabilities = Some(
            capabilities
                .into_iter()
                .map(|capability| capability.as_ref().to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        self
    }

    fn without_token(mut self) -> Self {
        self.token_id = None;
        self.secret = None;
        self
    }
}

/// One structured tracing event captured during a test, with fields
/// flattened to display/debug strings.
#[derive(Clone, Debug, Default)]
struct CapturedEvent {
    target: String,
    fields: std::collections::HashMap<String, String>,
}

impl CapturedEvent {
    fn field(&self, name: &str) -> &str {
        self.fields
            .get(name)
            .map(String::as_str)
            .unwrap_or_else(|| panic!("event has no field `{name}`: {self:?}"))
    }

    fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }
}

struct CaptureLayer {
    events: std::sync::Arc<std::sync::Mutex<Vec<CapturedEvent>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Visitor<'a>(&'a mut std::collections::HashMap<String, String>);
        impl tracing::field::Visit for Visitor<'_> {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.insert(field.name().to_string(), value.to_string());
            }

            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0
                    .insert(field.name().to_string(), format!("{value:?}"));
            }
        }
        let mut captured = CapturedEvent {
            target: event.metadata().target().to_string(),
            fields: std::collections::HashMap::new(),
        };
        event.record(&mut Visitor(&mut captured.fields));
        self.events.lock().unwrap().push(captured);
    }
}

/// Install a thread-local capturing subscriber. Works because the test
/// runtime is current-thread: server tasks emit on this thread while
/// the guard is held.
fn capture_admission_events() -> (
    std::sync::Arc<std::sync::Mutex<Vec<CapturedEvent>>>,
    tracing::subscriber::DefaultGuard,
) {
    use tracing_subscriber::layer::SubscriberExt;
    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(CaptureLayer {
        events: events.clone(),
    });
    let guard = tracing::subscriber::set_default(subscriber);
    (events, guard)
}

fn admission_denials(events: &std::sync::Mutex<Vec<CapturedEvent>>) -> Vec<CapturedEvent> {
    events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.target == "boardwalk::admission")
        .cloned()
        .collect()
}

#[tokio::test]
async fn admission_refusal_default_emits_structured_event() {
    let cloud = serve(Boardwalk::new().name("cloud")).await;
    let (events, _guard) = capture_admission_events();

    let result = raw_peer_upgrade(cloud.addr, PeerUpgradeAttempt::new("hub", Uuid::new_v4())).await;
    assert_eq!(result.status, StatusCode::FORBIDDEN);

    let denials = admission_denials(&events);
    assert_eq!(denials.len(), 1, "expected one denial event: {denials:?}");
    let denial = &denials[0];
    assert_eq!(denial.field("kind"), "admission");
    assert_eq!(denial.field("route"), "hub");
    assert_eq!(denial.field("reason"), "peer admission is not configured");
    assert_eq!(denial.field("status"), "403");
    assert!(denial.has_field("connection_id"));
}

#[tokio::test]
async fn admission_denials_emit_structured_events_per_branch() {
    let cloud = serve(
        Boardwalk::new().name("cloud").accept_peer(
            PeerAdmission::shared_token("hub", "kid-1", "secret")
                .unwrap()
                .expected_node_id("node-hub-1")
                .allow([PeerCapability::ResourceRead]),
        ),
    )
    .await;

    struct Case {
        name: &'static str,
        attempt: PeerUpgradeAttempt,
        status: StatusCode,
        reason: String,
        token_id: Option<&'static str>,
        node_id: Option<&'static str>,
    }
    let mut missing_bearer = PeerUpgradeAttempt::new("hub", Uuid::new_v4());
    missing_bearer.token_id = Some("kid-1");
    let cases = vec![
        Case {
            name: "missing token id",
            attempt: PeerUpgradeAttempt::new("hub", Uuid::new_v4()),
            status: StatusCode::UNAUTHORIZED,
            reason: "missing peer token id".into(),
            token_id: None,
            node_id: None,
        },
        Case {
            name: "missing bearer",
            attempt: missing_bearer,
            status: StatusCode::UNAUTHORIZED,
            reason: "missing bearer token".into(),
            token_id: Some("kid-1"),
            node_id: None,
        },
        Case {
            name: "unknown token id",
            attempt: PeerUpgradeAttempt::new("hub", Uuid::new_v4()).token("nope", "secret"),
            status: StatusCode::UNAUTHORIZED,
            reason: "unknown peer token id".into(),
            token_id: Some("nope"),
            node_id: None,
        },
        Case {
            name: "invalid bearer",
            attempt: PeerUpgradeAttempt::new("hub", Uuid::new_v4()).token("kid-1", "wrong"),
            status: StatusCode::UNAUTHORIZED,
            reason: "invalid bearer token".into(),
            token_id: Some("kid-1"),
            node_id: None,
        },
        Case {
            name: "token not valid for route",
            attempt: PeerUpgradeAttempt::new("other", Uuid::new_v4()).token("kid-1", "secret"),
            status: StatusCode::FORBIDDEN,
            reason: "peer token is not valid for route".into(),
            token_id: Some("kid-1"),
            node_id: None,
        },
        Case {
            name: "missing node id",
            attempt: PeerUpgradeAttempt::new("hub", Uuid::new_v4()).token("kid-1", "secret"),
            status: StatusCode::BAD_REQUEST,
            reason: "missing x-boardwalk-node-id".into(),
            token_id: Some("kid-1"),
            node_id: None,
        },
        Case {
            name: "node id mismatch",
            attempt: PeerUpgradeAttempt::new("hub", Uuid::new_v4())
                .token("kid-1", "secret")
                .node_id("node-other"),
            status: StatusCode::FORBIDDEN,
            reason: "peer node id mismatch".into(),
            token_id: Some("kid-1"),
            node_id: Some("node-other"),
        },
        Case {
            name: "missing capabilities",
            attempt: PeerUpgradeAttempt::new("hub", Uuid::new_v4())
                .token("kid-1", "secret")
                .node_id("node-hub-1"),
            status: StatusCode::BAD_REQUEST,
            reason: "missing x-boardwalk-peer-capabilities".into(),
            token_id: Some("kid-1"),
            node_id: None,
        },
        Case {
            name: "unparsable capabilities",
            attempt: PeerUpgradeAttempt::new("hub", Uuid::new_v4())
                .token("kid-1", "secret")
                .node_id("node-hub-1")
                .request_capabilities(["bogus"]),
            status: StatusCode::BAD_REQUEST,
            reason: "unknown peer capability `bogus`".into(),
            token_id: Some("kid-1"),
            node_id: None,
        },
        Case {
            name: "empty negotiated set",
            attempt: PeerUpgradeAttempt::new("hub", Uuid::new_v4())
                .token("kid-1", "secret")
                .node_id("node-hub-1")
                .request_capabilities(["transition.invoke"]),
            status: StatusCode::FORBIDDEN,
            reason: "peer capabilities are not allowed".into(),
            token_id: Some("kid-1"),
            node_id: None,
        },
    ];

    for case in cases {
        let (events, guard) = capture_admission_events();
        let route = case.attempt.route_name.clone();
        let result = raw_peer_upgrade(cloud.addr, case.attempt).await;
        drop(guard);
        assert_eq!(result.status, case.status, "case `{}`", case.name);

        let denials = admission_denials(&events);
        assert_eq!(
            denials.len(),
            1,
            "case `{}` expected one denial event: {denials:?}",
            case.name
        );
        let denial = &denials[0];
        assert_eq!(denial.field("kind"), "admission", "case `{}`", case.name);
        assert_eq!(denial.field("route"), route, "case `{}`", case.name);
        assert_eq!(denial.field("reason"), case.reason, "case `{}`", case.name);
        assert_eq!(
            denial.field("status"),
            case.status.as_u16().to_string(),
            "case `{}`",
            case.name
        );
        match case.token_id {
            Some(expected) => {
                assert_eq!(denial.field("token_id"), expected, "case `{}`", case.name);
            }
            None => assert!(
                !denial.has_field("token_id"),
                "case `{}` should not log a token id: {denial:?}",
                case.name
            ),
        }
        if let Some(expected) = case.node_id {
            assert_eq!(denial.field("node_id"), expected, "case `{}`", case.name);
        }
        for value in denial.fields.values() {
            assert!(
                !value.contains("secret") && !value.contains("wrong"),
                "case `{}` leaked secret material: {denial:?}",
                case.name
            );
        }
    }
}

#[tokio::test]
async fn successful_admission_emits_no_denial_event() {
    let cloud = serve(
        Boardwalk::new().name("cloud").accept_peer(
            PeerAdmission::shared_token("hub", "kid-1", "secret")
                .unwrap()
                .allow([PeerCapability::ResourceRead]),
        ),
    )
    .await;
    let (events, _guard) = capture_admission_events();

    let result = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .token("kid-1", "secret")
            .node_id("node-hub-1")
            .request_capabilities(["resource.read"]),
    )
    .await;
    assert_eq!(result.status, StatusCode::SWITCHING_PROTOCOLS);

    let denials = admission_denials(&events);
    assert!(denials.is_empty(), "unexpected denial events: {denials:?}");
}
