//! Peer link over TLS (`https://` / `wss://`).
//!
//! Self-signed cert via rcgen; client trusts it via the
//! `dangerous-test-tls` feature. Run with `--features dangerous-test-tls`.

#![cfg(feature = "dangerous-test-tls")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde_json::Value as Json;
use tokio_rustls::TlsAcceptor;

use super::actor_led_fixture::ActorLed;
use crate::Boardwalk;

/// Stand up an HTTPS listener wrapping `router` with a self-signed cert
/// for "localhost"/"127.0.0.1". Returns its address.
async fn serve_tls(router: axum::Router) -> SocketAddr {
    // Install the rustls crypto provider once for the test process.
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
            .unwrap();
    let cert_der: CertificateDer<'static> = cert.cert.der().clone();
    let key_der: PrivateKeyDer<'static> =
        PrivateKeyDer::try_from(cert.key_pair.serialize_der()).unwrap();

    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (tcp, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let acceptor = acceptor.clone();
            let router = router.clone();
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let io = hyper_util::rt::TokioIo::new(tls);
                let service = router.into_service::<hyper::body::Incoming>();
                let svc = hyper_util::service::TowerToHyperService::new(service);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .with_upgrades()
                    .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn hub_links_to_cloud_over_tls() {
    let cloud = Boardwalk::new().name("cloud").build().unwrap();
    let cloud_acceptors = cloud.acceptors.clone();
    let tls_addr = serve_tls(cloud.router).await;

    let hub = Boardwalk::new()
        .name("hub")
        .use_actor(ActorLed::default())
        .link(format!("https://localhost:{}/", tls_addr.port()))
        .build()
        .unwrap();
    let hub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    tokio::spawn(async move {
        axum::serve(hub_listener, hub.router).await.unwrap();
    });

    // The TLS handshake adds latency; allow a bit more.
    assert!(
        cloud_acceptors
            .wait_for_first(Duration::from_secs(10))
            .await,
        "cloud should have received a confirmed peer over TLS within 10s"
    );

    // Sanity: the cloud's root advertises the hub as a peer through TLS.
    let cloud_url = format!("https://localhost:{}/", tls_addr.port());
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let root: Json = client
        .get(&cloud_url)
        .send()
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
    assert!(has_peer);
}
