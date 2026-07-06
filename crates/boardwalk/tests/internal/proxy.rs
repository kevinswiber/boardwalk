//! Peer dial through an HTTP CONNECT forward proxy.
//!
//! An in-process proxy fixture accepts `CONNECT host:port`, records
//! what it saw (authority, `Proxy-Authorization`), and splices bytes
//! between the caller and the target — the egress shape mandated by
//! sandboxed and corporate networks. The `wss` case additionally
//! layers TLS inside the tunnel and needs `--features
//! dangerous-test-tls` (self-signed test cert).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use http_body_util::Empty;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value as Json;

use super::actor_led_fixture::ActorLed;
use crate::{Boardwalk, PeerLink};

/// What the fixture saw for one CONNECT request.
#[derive(Debug, Clone)]
struct SeenConnect {
    authority: String,
    proxy_authorization: Option<String>,
}

#[derive(Clone)]
enum ProxyBehavior {
    /// Tunnel every CONNECT to its target.
    Forward,
    /// Tunnel only when this exact `Proxy-Authorization` value is
    /// presented; otherwise 407.
    RequireAuthorization(String),
    /// Refuse every CONNECT with this status.
    Refuse(StatusCode),
}

struct ConnectProxy {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<SeenConnect>>>,
}

impl ConnectProxy {
    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn seen(&self) -> Vec<SeenConnect> {
        self.seen.lock().unwrap().clone()
    }
}

async fn spawn_connect_proxy(behavior: ProxyBehavior) -> ConnectProxy {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen: Arc<Mutex<Vec<SeenConnect>>> = Arc::default();
    let accept_seen = seen.clone();
    tokio::spawn(async move {
        loop {
            let (tcp, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => return,
            };
            let seen = accept_seen.clone();
            let behavior = behavior.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| handle(req, seen.clone(), behavior.clone()));
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(tcp), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    ConnectProxy { addr, seen }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    seen: Arc<Mutex<Vec<SeenConnect>>>,
    behavior: ProxyBehavior,
) -> Result<Response<Empty<Bytes>>, std::convert::Infallible> {
    assert_eq!(
        req.method(),
        hyper::Method::CONNECT,
        "fixture only speaks CONNECT; the dialer must not send anything else"
    );
    let authority = req
        .uri()
        .authority()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let proxy_authorization = req
        .headers()
        .get(hyper::header::PROXY_AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    seen.lock().unwrap().push(SeenConnect {
        authority: authority.clone(),
        proxy_authorization: proxy_authorization.clone(),
    });

    fn respond(status: StatusCode) -> Result<Response<Empty<Bytes>>, std::convert::Infallible> {
        let mut res = Response::new(Empty::new());
        *res.status_mut() = status;
        Ok(res)
    }

    match behavior {
        ProxyBehavior::Refuse(status) => return respond(status),
        ProxyBehavior::RequireAuthorization(expected) => {
            if proxy_authorization.as_deref() != Some(expected.as_str()) {
                return respond(StatusCode::PROXY_AUTHENTICATION_REQUIRED);
            }
        }
        ProxyBehavior::Forward => {}
    }

    // Return 200 first; hyper flips this connection to a raw byte
    // tunnel once the response is written, and the spawned task
    // splices it to the target.
    tokio::spawn(async move {
        let Ok(upgraded) = hyper::upgrade::on(req).await else {
            return;
        };
        let Ok(mut target) = tokio::net::TcpStream::connect(&authority).await else {
            return;
        };
        let mut caller = TokioIo::new(upgraded);
        let _ = tokio::io::copy_bidirectional(&mut caller, &mut target).await;
    });
    respond(StatusCode::OK)
}

/// Boot a plain cloud that admits unauthenticated local peers; return
/// its address and acceptors.
async fn spawn_cloud() -> (SocketAddr, crate::peer::PeerAcceptors) {
    let cloud = Boardwalk::new()
        .name("cloud")
        .allow_unauthenticated_local_peers()
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptors = cloud.acceptors.clone();
    tokio::spawn(async move {
        axum::serve(listener, cloud.router).await.unwrap();
    });
    (addr, acceptors)
}

/// Boot a hub that dials out via `link`. Its peer tasks detach and
/// keep running; the hub's own HTTP listener is irrelevant here.
fn spawn_hub(link: PeerLink) {
    let _hub = Boardwalk::new()
        .name("hub")
        .use_actor(ActorLed::default())
        .link_peer(link)
        .build()
        .unwrap();
}

#[tokio::test]
async fn hub_links_to_cloud_through_a_connect_proxy() {
    let (cloud_addr, cloud_acceptors) = spawn_cloud().await;
    let proxy = spawn_connect_proxy(ProxyBehavior::Forward).await;

    let link = PeerLink::new(format!("ws://{cloud_addr}"), "hub")
        .unwrap()
        .proxy(proxy.url())
        .unwrap();
    spawn_hub(link);

    assert!(
        cloud_acceptors.wait_for_first(Duration::from_secs(5)).await,
        "cloud should confirm the peer dialed through the proxy within 5s"
    );

    let seen = proxy.seen();
    assert!(
        seen.iter().any(|c| c.authority == cloud_addr.to_string()),
        "the dial must traverse the proxy: {seen:?}"
    );
    assert!(
        seen.iter().all(|c| c.proxy_authorization.is_none()),
        "no credentials were configured: {seen:?}"
    );

    // End to end: the cloud's root advertises the hub over the
    // proxied tunnel.
    let root: Json = reqwest::get(format!("http://{cloud_addr}/"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let has_peer = root["links"]
        .as_array()
        .unwrap()
        .iter()
        .any(|l| l["title"] == "hub");
    assert!(has_peer, "cloud root should advertise hub as peer: {root}");
}

#[tokio::test]
async fn proxy_authorization_is_sent_when_credentials_are_configured() {
    let (cloud_addr, cloud_acceptors) = spawn_cloud().await;
    let expected = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("svc:hunter2")
    );
    let proxy = spawn_connect_proxy(ProxyBehavior::RequireAuthorization(expected.clone())).await;

    let link = PeerLink::new(format!("ws://{cloud_addr}"), "hub")
        .unwrap()
        .proxy(proxy.url())
        .unwrap()
        .proxy_auth("svc", "hunter2");
    spawn_hub(link);

    assert!(
        cloud_acceptors.wait_for_first(Duration::from_secs(5)).await,
        "cloud should confirm the peer once the proxy accepts the credentials"
    );
    let seen = proxy.seen();
    assert!(
        seen.iter()
            .any(|c| c.proxy_authorization.as_deref() == Some(expected.as_str())),
        "the CONNECT request must carry Proxy-Authorization: {seen:?}"
    );
}

#[tokio::test]
async fn refused_connect_surfaces_a_clear_proxy_error() {
    let proxy = spawn_connect_proxy(ProxyBehavior::Refuse(StatusCode::FORBIDDEN)).await;

    let selection = crate::tunnel::ProxySelection {
        explicit: Some(crate::tunnel::ProxyConfig::from_url_str(&proxy.url()).unwrap()),
        auth: None,
    };
    // TEST-NET-1 target: the proxy refuses before any connection to it.
    let Err(err) = crate::tunnel::dial_initiator(
        "ws://192.0.2.1:81/",
        "hub",
        uuid::Uuid::new_v4(),
        None,
        &selection,
    )
    .await
    else {
        panic!("dial through a refusing proxy must fail");
    };

    match err {
        crate::tunnel::TunnelError::Proxy(message) => {
            assert!(
                message.contains("403"),
                "the error should name the proxy's status: {message}"
            );
            assert!(
                message.contains("192.0.2.1:81"),
                "the error should name the CONNECT target: {message}"
            );
        }
        other => panic!("expected TunnelError::Proxy, got: {other}"),
    }
}

#[cfg(feature = "dangerous-test-tls")]
#[tokio::test]
async fn hub_links_over_tls_through_a_connect_proxy() {
    let cloud = Boardwalk::new()
        .name("cloud")
        .allow_unauthenticated_local_peers()
        .build()
        .unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    let tls_addr = super::tls::serve_tls(cloud.router).await;
    let proxy = spawn_connect_proxy(ProxyBehavior::Forward).await;

    let link = PeerLink::new(format!("wss://localhost:{}/", tls_addr.port()), "hub")
        .unwrap()
        .proxy(proxy.url())
        .unwrap();
    spawn_hub(link);

    assert!(
        cloud_acceptors
            .wait_for_first(Duration::from_secs(10))
            .await,
        "cloud should confirm the peer over TLS-inside-CONNECT within 10s"
    );
    let seen = proxy.seen();
    assert!(
        seen.iter()
            .any(|c| c.authority == format!("localhost:{}", tls_addr.port())),
        "the TLS dial must traverse the proxy: {seen:?}"
    );
}
