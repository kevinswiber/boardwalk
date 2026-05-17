//! WebSocket-upgrade-then-HTTP/2 tunnel primitive.
//!
//! After a 101 Switching Protocols handshake, both sides drop WebSocket
//! framing entirely and speak HTTP/2 over the raw stream. The side that
//! originally opened the WS (initiator) becomes the HTTP/2 server; the
//! side that accepted (acceptor) becomes the HTTP/2 client. See
//! `docs/02-protocol-peer.md`.

#![forbid(unsafe_code)]

use std::pin::Pin;
use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use http_body_util::Empty;
use hyper::header::{HeaderName, HeaderValue};
use hyper::Request;
use hyper_util::rt::TokioIo;
use sha1::{Digest, Sha1};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[allow(unused_imports)]
use uuid::Uuid;

pub const SUBPROTOCOL: &str = "zetta-peer/2";

/// RFC 6455 GUID used in Sec-WebSocket-Accept derivation.
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Debug, Error)]
pub enum TunnelError {
    #[error("websocket upgrade: {0}")]
    Upgrade(String),
    #[error("invalid url: {0}")]
    Url(String),
    #[error("h2: {0}")]
    H2(#[from] h2::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("hyper: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("invalid response: {0}")]
    Response(String),
}

/// Derive the `Sec-WebSocket-Accept` header value from the client's
/// `Sec-WebSocket-Key`.
pub fn ws_accept_key(client_key: &str) -> String {
    let mut h = Sha1::new();
    h.update(client_key.as_bytes());
    h.update(WS_GUID.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(h.finalize())
}

/// Generate a fresh `Sec-WebSocket-Key` value (16 random bytes, base64-encoded).
pub fn ws_new_key() -> String {
    use rand::TryRngCore;
    let mut buf = [0u8; 16];
    rand::rngs::OsRng.try_fill_bytes(&mut buf).expect("os rng");
    base64::engine::general_purpose::STANDARD.encode(buf)
}

/// What `dial_initiator` returns once the WS upgrade is complete.
pub struct InitiatorReady {
    pub upgraded: hyper::upgrade::Upgraded,
    pub remote_authority: String,
}

/// As the **initiator**: open a TCP connection to `remote_url` and send
/// a WebSocket Upgrade request to `/peers/{local_name}?connectionId={uuid}`.
/// On a 101 response, take the upgraded byte stream (no WS framing) and
/// return it. Caller then runs `h2::server::handshake` on it.
pub async fn dial_initiator(
    remote_url: &str,
    local_name: &str,
    connection_id: Uuid,
) -> Result<InitiatorReady, TunnelError> {
    let url = url::Url::parse(remote_url)
        .map_err(|e| TunnelError::Url(format!("{e}")))?;
    let scheme = url.scheme();
    let host = url.host_str().ok_or_else(|| TunnelError::Url("no host".into()))?;
    let port = url.port_or_known_default().unwrap_or(match scheme {
        "https" | "wss" => 443,
        _ => 80,
    });

    let tcp = TcpStream::connect((host, port)).await?;

    // Stream is boxed so we can hold either a plain TCP or a TLS
    // wrapper without spreading generics through the rest of the
    // function.
    let stream: Pin<Box<dyn AsyncReadWrite + Send>> = match scheme {
        "http" | "ws" => Box::pin(tcp),
        "https" | "wss" => Box::pin(tls_connect(host, tcp).await?),
        other => {
            return Err(TunnelError::Url(format!("scheme `{other}` not supported")));
        }
    };

    let path = format!(
        "/peers/{}?connectionId={}",
        urlencoding::encode(local_name),
        connection_id
    );
    let authority = if let Some(p) = url.port() {
        format!("{host}:{p}")
    } else {
        host.to_string()
    };

    let key = ws_new_key();

    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::Builder::new()
        .handshake::<_, Empty<Bytes>>(io)
        .await?;
    let conn = conn.with_upgrades();
    let conn_task = tokio::spawn(async move { let _ = conn.await; });

    let req = Request::builder()
        .method("POST")
        .uri(&path)
        .header(hyper::header::HOST, authority.clone())
        .header(hyper::header::CONNECTION, "Upgrade")
        .header(hyper::header::UPGRADE, "websocket")
        .header(HeaderName::from_static("sec-websocket-key"), key.clone())
        .header(HeaderName::from_static("sec-websocket-version"), "13")
        .header(
            HeaderName::from_static("sec-websocket-protocol"),
            HeaderValue::from_static(SUBPROTOCOL),
        )
        .body(Empty::<Bytes>::new())
        .map_err(|e| TunnelError::Upgrade(format!("build request: {e}")))?;

    let response = sender.send_request(req).await?;
    drop(sender); // Let the connection task own the path to completion.

    if response.status() != hyper::StatusCode::SWITCHING_PROTOCOLS {
        return Err(TunnelError::Response(format!(
            "expected 101, got {}",
            response.status()
        )));
    }

    let expected_accept = ws_accept_key(&key);
    if let Some(got) = response.headers().get("sec-websocket-accept") {
        if got.to_str().ok() != Some(expected_accept.as_str()) {
            return Err(TunnelError::Upgrade("invalid Sec-WebSocket-Accept".into()));
        }
    } else {
        return Err(TunnelError::Upgrade("missing Sec-WebSocket-Accept".into()));
    }

    let upgraded = hyper::upgrade::on(response).await?;
    // The connection task may now finish; that's OK — we own the upgraded stream.
    let _ = conn_task;

    Ok(InitiatorReady { upgraded, remote_authority: authority })
}

/// Build a 101 Switching Protocols response for a peer WS upgrade
/// request. Validates Sec-WebSocket-Key, Sec-WebSocket-Version, and
/// requires the `zetta-peer/2` subprotocol token.
pub fn build_upgrade_response(
    headers: &http::HeaderMap,
) -> Result<http::Response<()>, TunnelError> {
    let key = headers
        .get("sec-websocket-key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| TunnelError::Upgrade("missing Sec-WebSocket-Key".into()))?;
    let version = headers
        .get("sec-websocket-version")
        .and_then(|v| v.to_str().ok());
    if version != Some("13") {
        return Err(TunnelError::Upgrade("missing or wrong Sec-WebSocket-Version".into()));
    }

    let offered = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !offered.split(',').any(|tok| tok.trim() == SUBPROTOCOL) {
        return Err(TunnelError::Upgrade(format!(
            "client did not offer `{SUBPROTOCOL}` subprotocol; got `{offered}`"
        )));
    }

    let accept = ws_accept_key(key);

    let builder = http::Response::builder()
        .status(http::StatusCode::SWITCHING_PROTOCOLS)
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-accept", accept)
        .header("sec-websocket-protocol", SUBPROTOCOL);

    builder
        .body(())
        .map_err(|e| TunnelError::Upgrade(format!("build response: {e}")))
}

/// Helper re-export of the hyper-util executor used for serving H2.
pub use hyper_util::rt::TokioExecutor as H2Executor;

/// Helper trait alias for boxed I/O.
pub trait AsyncReadWrite: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncReadWrite for T {}

/// Establish a TLS connection over `tcp` for `host`. Uses
/// `rustls-platform-verifier` so certificates validate against the
/// OS-native trust store (Keychain on macOS, Schannel on Windows,
/// system CA on Linux) — same trust model the rest of the OS uses,
/// no baked-in root bundle to keep current.
async fn tls_connect(
    host: &str,
    tcp: TcpStream,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, TunnelError> {
    static PROVIDER_INSTALLED: std::sync::Once = std::sync::Once::new();
    PROVIDER_INSTALLED.call_once(|| {
        // Best-effort install. If something else installed first, fine.
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );
    });

    use rustls_platform_verifier::BuilderVerifierExt;
    let config = rustls::ClientConfig::builder()
        .with_platform_verifier()
        .map_err(|e| TunnelError::Upgrade(format!("rustls platform verifier: {e}")))?
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let server_name = rustls_pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| TunnelError::Url(format!("invalid TLS server name: {e}")))?;
    let tls = connector
        .connect(server_name, tcp)
        .await
        .map_err(TunnelError::Io)?;
    Ok(tls)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc6455_example_accept_key() {
        // The classic example from RFC 6455 §1.3.
        let got = ws_accept_key("dGhlIHNhbXBsZSBub25jZQ==");
        assert_eq!(got, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn ws_new_key_is_base64_16_bytes() {
        let k = ws_new_key();
        let decoded = base64::engine::general_purpose::STANDARD.decode(&k).unwrap();
        assert_eq!(decoded.len(), 16);
    }
}
