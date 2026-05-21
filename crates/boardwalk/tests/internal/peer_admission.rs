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
use crate::registry::{PeerRecord, Registry};
use crate::server::Built;
use crate::tunnel::SUBPROTOCOL;

#[tokio::test]
async fn peer_upgrade_without_admission_token_is_rejected_before_upgrade() {
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .expect_peer_token("hub", "kid-1", "secret"),
    )
    .await;

    let status = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .node_id("node-hub-1")
            .without_token(),
    )
    .await
    .status;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_no_peer_sender_registered(&cloud.built, "hub").await;
}

#[tokio::test]
async fn token_configured_for_one_route_cannot_claim_another_route_name() {
    let cloud = serve(
        Boardwalk::new()
            .name("cloud")
            .expect_peer_token("hub-a", "kid-1", "secret"),
    )
    .await;

    let status = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub-b", Uuid::new_v4())
            .node_id("node-hub-1")
            .token("kid-1", "secret"),
    )
    .await
    .status;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_no_peer_sender_registered(&cloud.built, "hub-b").await;
}

#[tokio::test]
async fn expected_node_id_mismatch_is_rejected_before_upgrade() {
    let cloud = serve(Boardwalk::new().name("cloud").expect_peer_token_for_node(
        "hub",
        "kid-1",
        "secret",
        "node-hub-1",
    ))
    .await;

    let status = raw_peer_upgrade(
        cloud.addr,
        PeerUpgradeAttempt::new("hub", Uuid::new_v4())
            .node_id("node-hub-2")
            .token("kid-1", "secret"),
    )
    .await
    .status;

    assert_eq!(status, StatusCode::FORBIDDEN);
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
            .expect_peer_token_for_node("hub", "kid-1", "secret", "node-hub-1"),
    )
    .await;
    let registry = cloud.built.registry.as_ref().expect("registry");

    let first_connection_id = Uuid::new_v4();
    let first_peer = connect_admitted_peer(
        &cloud,
        PeerUpgradeAttempt::new("hub", first_connection_id)
            .node_id("node-hub-1")
            .token("kid-1", "secret"),
    )
    .await;
    let first_record = wait_for_peer_record(registry, "hub").await;

    assert_ne!(
        first_record.id, first_connection_id,
        "durable peer identity must not be the transient connection id"
    );

    first_peer.close();
    wait_for_no_peer_sender(&cloud.built, "hub").await;

    let second_connection_id = Uuid::new_v4();
    let _second_peer = connect_admitted_peer(
        &cloud,
        PeerUpgradeAttempt::new("hub", second_connection_id)
            .node_id("node-hub-1")
            .token("kid-1", "secret"),
    )
    .await;
    let second_record = wait_for_peer_record(registry, "hub").await;

    assert_eq!(
        second_record.id, first_record.id,
        "durable peer identity should remain stable across reconnects"
    );
    assert_ne!(
        second_record.id, second_connection_id,
        "reconnect must create a new connection without replacing peer identity"
    );
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

async fn wait_for_peer_record(registry: &Registry, route_name: &str) -> PeerRecord {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(record) = registry.get_peer(route_name).unwrap() {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("peer record")
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
    if let Some(token_id) = attempt.token_id {
        builder = builder.header("x-boardwalk-token-id", token_id);
    }
    if let Some(secret) = attempt.secret {
        builder = builder.header("authorization", format!("Bearer {secret}"));
    }

    let response = sender
        .send_request(builder.body(Empty::<Bytes>::new()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let upgraded = if status == StatusCode::SWITCHING_PROTOCOLS {
        Some(hyper::upgrade::on(response).await.unwrap())
    } else {
        None
    };

    PeerUpgradeResult { status, upgraded }
}

struct PeerUpgradeAttempt {
    route_name: String,
    connection_id: Uuid,
    node_id: Option<&'static str>,
    token_id: Option<&'static str>,
    secret: Option<&'static str>,
}

impl PeerUpgradeAttempt {
    fn new(route_name: impl Into<String>, connection_id: Uuid) -> Self {
        Self {
            route_name: route_name.into(),
            connection_id,
            node_id: None,
            token_id: None,
            secret: None,
        }
    }

    fn node_id(mut self, node_id: &'static str) -> Self {
        self.node_id = Some(node_id);
        self
    }

    fn token(mut self, token_id: &'static str, secret: &'static str) -> Self {
        self.token_id = Some(token_id);
        self.secret = Some(secret);
        self
    }

    fn without_token(mut self) -> Self {
        self.token_id = None;
        self.secret = None;
        self
    }
}

trait PeerAdmissionConfigExt {
    fn expect_peer_token(self, route_name: &str, token_id: &str, secret: &str) -> Self;

    fn expect_peer_token_for_node(
        self,
        route_name: &str,
        token_id: &str,
        secret: &str,
        expected_node_id: &str,
    ) -> Self;
}

impl PeerAdmissionConfigExt for Boardwalk {
    fn expect_peer_token(self, route_name: &str, token_id: &str, secret: &str) -> Self {
        let _ = (route_name, token_id, secret);
        self
    }

    fn expect_peer_token_for_node(
        self,
        route_name: &str,
        token_id: &str,
        secret: &str,
        expected_node_id: &str,
    ) -> Self {
        let _ = (route_name, token_id, secret, expected_node_id);
        self
    }
}
