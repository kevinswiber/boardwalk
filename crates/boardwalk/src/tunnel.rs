//! WebSocket-upgrade-then-HTTP/2 tunnel primitive.
//!
//! After a 101 Switching Protocols handshake, both sides drop WebSocket
//! framing entirely and speak HTTP/2 over the raw stream. The side that
//! originally opened the WS (initiator) becomes the HTTP/2 server; the
//! side that accepted (acceptor) becomes the HTTP/2 client.

#![forbid(unsafe_code)]

use std::pin::Pin;
use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use http_body_util::Empty;
use hyper::Request;
use hyper::header::{HeaderName, HeaderValue};
use hyper_util::rt::TokioIo;
use sha1::{Digest, Sha1};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[allow(unused_imports)]
use uuid::Uuid;

use crate::secret::RedactedSecret;

pub const SUBPROTOCOL: &str = "boardwalk-peer/3";

/// Header naming the peer token id (`PeerAdmission::shared_token`'s
/// `token_id`) on boardwalk's wire. Presented in two places:
///
/// 1. The tunnel handshake (`/peers/{route}` upgrade) alongside
///    `Authorization: Bearer <secret>` — establishes the admitted link.
/// 2. Per-request caller ingress at a gateway: an admitted peer
///    attaches this header plus `Authorization: Bearer <secret>` to an
///    ordinary gateway request, and the gateway forwards the request
///    with the caller's attested admission context. Presenting this
///    header opts the request into authentication: invalid credentials
///    or a missing live admitted tunnel refuse the request (401/403) —
///    there is no anonymous fallback. See the caller ingress section of
///    `docs/peers.md` for the full contract.
pub const PEER_TOKEN_ID_HEADER: &str = "boardwalk-peer-token-id";
// The remaining admission headers are handshake-only vocabulary; they
// have no per-request role and stay crate-private.
pub(crate) const PEER_NODE_ID_HEADER: &str = "boardwalk-node-id";
pub(crate) const PEER_NODE_NAME_HEADER: &str = "boardwalk-node-name";
pub(crate) const PEER_CAPABILITIES_HEADER: &str = "boardwalk-peer-capabilities";

/// RFC 6455 GUID used in Sec-WebSocket-Accept derivation.
const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Debug, Error)]
pub enum TunnelError {
    #[error("websocket upgrade: {0}")]
    Upgrade(String),
    #[error("invalid url: {0}")]
    Url(String),
    #[error("proxy: {0}")]
    Proxy(String),
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
    #[allow(dead_code)]
    pub remote_authority: String,
}

pub(crate) struct InitiatorAdmission<'a> {
    pub token_id: &'a str,
    pub token_secret: &'a str,
    pub node_id: &'a str,
    pub node_name: Option<&'a str>,
    pub requested_capabilities: &'a str,
}

/// How the dial selects an HTTP CONNECT forward proxy: an explicit
/// target always wins; otherwise the conventional env vars
/// (`HTTPS_PROXY`/`HTTP_PROXY`, `NO_PROXY`) apply. `auth` overrides
/// credentials for whichever proxy is selected.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProxySelection {
    pub explicit: Option<ProxyConfig>,
    pub auth: Option<ProxyAuth>,
}

/// An HTTP CONNECT forward proxy target.
#[derive(Debug, Clone)]
pub(crate) struct ProxyConfig {
    pub host: String,
    pub port: u16,
    pub auth: Option<ProxyAuth>,
}

/// Credentials sent as `Proxy-Authorization: Basic ...` on the CONNECT
/// request. The password never appears in `Debug` output.
#[derive(Debug, Clone)]
pub(crate) struct ProxyAuth {
    pub username: String,
    pub password: RedactedSecret,
}

impl ProxyAuth {
    fn basic_header_value(&self) -> Result<HeaderValue, TunnelError> {
        let raw = format!("{}:{}", self.username, self.password.expose());
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
        let mut value = HeaderValue::from_str(&format!("Basic {encoded}"))
            .map_err(|e| TunnelError::Proxy(format!("credentials not header-safe: {e}")))?;
        value.set_sensitive(true);
        Ok(value)
    }
}

impl ProxyConfig {
    /// Parse a proxy URL. Only `http://` proxies are supported: the
    /// CONNECT leg is plaintext to the proxy, which is the standard
    /// shape for egress proxies inside a trusted network boundary.
    /// URL userinfo becomes [`ProxyAuth`] and is not retained as part
    /// of any stored URL, so credentialed env-var values never reach
    /// `Debug` output or logs.
    ///
    /// Returns a plain message so callers can wrap it in their own
    /// error type ([`TunnelError::Proxy`] or `PeerConfigError`).
    pub(crate) fn from_url_str(raw: &str) -> Result<Self, String> {
        let url = url::Url::parse(raw).map_err(|e| format!("invalid proxy url: {e}"))?;
        match url.scheme() {
            "http" => {}
            other => {
                return Err(format!(
                    "proxy scheme `{other}` not supported; use an `http://` CONNECT proxy"
                ));
            }
        }
        let host = url
            .host_str()
            .ok_or_else(|| "proxy url has no host".to_string())?
            .to_string();
        let port = url.port_or_known_default().unwrap_or(80);
        let auth = match (url.username(), url.password()) {
            ("", None) => None,
            (username, password) => Some(ProxyAuth {
                username: percent_decode(username)?,
                password: RedactedSecret::new(percent_decode(password.unwrap_or(""))?),
            }),
        };
        Ok(Self { host, port, auth })
    }
}

fn percent_decode(raw: &str) -> Result<String, String> {
    urlencoding::decode(raw)
        .map(|cow| cow.into_owned())
        .map_err(|e| format!("invalid percent-encoding in proxy credentials: {e}"))
}

/// Resolve a proxy for `scheme://host` from the process environment.
fn proxy_from_env(scheme: &str, host: &str) -> Result<Option<ProxyConfig>, TunnelError> {
    proxy_from_env_with(|key| std::env::var(key).ok(), scheme, host)
}

/// Env-var proxy resolution, parameterized over the lookup so tests
/// never touch process-global state. `wss`/`https` targets use
/// `HTTPS_PROXY`; `ws`/`http` targets use `HTTP_PROXY` (lowercase
/// variants accepted). `NO_PROXY` bypasses matching hosts. Loopback
/// targets never use an env-derived proxy, so a globally exported
/// proxy cannot capture local-development dials.
fn proxy_from_env_with(
    lookup: impl Fn(&str) -> Option<String>,
    scheme: &str,
    host: &str,
) -> Result<Option<ProxyConfig>, TunnelError> {
    if is_loopback_host(host) {
        return Ok(None);
    }
    let lookup_set = |key: &str| lookup(key).filter(|value| !value.trim().is_empty());
    if let Some(list) = lookup_set("NO_PROXY").or_else(|| lookup_set("no_proxy"))
        && no_proxy_matches(&list, host)
    {
        return Ok(None);
    }
    let keys = match scheme {
        "https" | "wss" => ["HTTPS_PROXY", "https_proxy"],
        _ => ["HTTP_PROXY", "http_proxy"],
    };
    let Some(raw) = keys.iter().find_map(|key| lookup_set(key)) else {
        return Ok(None);
    };
    ProxyConfig::from_url_str(raw.trim())
        .map(Some)
        .map_err(TunnelError::Proxy)
}

fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    bare.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

/// `NO_PROXY` matching: `*` bypasses everything; each comma-separated
/// entry matches its exact host and any subdomain (a leading dot is
/// accepted and ignored). Ports and CIDR ranges are not interpreted.
fn no_proxy_matches(list: &str, host: &str) -> bool {
    list.split(',')
        .map(|entry| entry.trim().trim_start_matches('.'))
        .filter(|entry| !entry.is_empty())
        .any(|entry| {
            entry == "*"
                || host.eq_ignore_ascii_case(entry)
                || (host.len() > entry.len()
                    && host.as_bytes()[host.len() - entry.len() - 1] == b'.'
                    && host[host.len() - entry.len()..].eq_ignore_ascii_case(entry))
        })
}

/// Open a TCP connection to the proxy and establish a CONNECT tunnel
/// to `host:port`. Returns the raw tunneled byte stream; the caller
/// layers TLS and the WS upgrade over it exactly as it would over a
/// direct TCP connection.
async fn connect_via_proxy(
    proxy: &ProxyConfig,
    host: &str,
    port: u16,
) -> Result<TokioIo<hyper::upgrade::Upgraded>, TunnelError> {
    let tcp = TcpStream::connect((proxy.host.as_str(), proxy.port)).await?;
    let (mut sender, conn) = hyper::client::conn::http1::Builder::new()
        .handshake::<_, Empty<Bytes>>(TokioIo::new(tcp))
        .await?;
    let conn = conn.with_upgrades();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let authority = format!("{host}:{port}");
    let mut builder = Request::builder()
        .method(hyper::Method::CONNECT)
        .uri(&authority)
        .header(hyper::header::HOST, authority.clone());
    if let Some(auth) = &proxy.auth {
        builder = builder.header(
            hyper::header::PROXY_AUTHORIZATION,
            auth.basic_header_value()?,
        );
    }
    let req = builder
        .body(Empty::<Bytes>::new())
        .map_err(|e| TunnelError::Proxy(format!("build CONNECT request: {e}")))?;

    let response = sender.send_request(req).await?;
    drop(sender);
    if !response.status().is_success() {
        return Err(TunnelError::Proxy(format!(
            "CONNECT {authority} via {}:{} refused: {}",
            proxy.host,
            proxy.port,
            response.status()
        )));
    }
    let upgraded = hyper::upgrade::on(response).await?;
    Ok(TokioIo::new(upgraded))
}

/// As the **initiator**: open a connection to `remote_url` — direct
/// TCP, or a CONNECT tunnel when a forward proxy is selected — and send
/// a WebSocket Upgrade request to `/peers/{local_name}?connectionId={uuid}`.
/// On a 101 response, take the upgraded byte stream (no WS framing) and
/// return it. Caller then runs `h2::server::handshake` on it.
pub async fn dial_initiator(
    remote_url: &str,
    local_name: &str,
    connection_id: Uuid,
    admission: Option<InitiatorAdmission<'_>>,
    proxy: &ProxySelection,
) -> Result<InitiatorReady, TunnelError> {
    let url = url::Url::parse(remote_url).map_err(|e| TunnelError::Url(format!("{e}")))?;
    let scheme = url.scheme();
    if !matches!(scheme, "http" | "ws" | "https" | "wss") {
        return Err(TunnelError::Url(format!("scheme `{scheme}` not supported")));
    }
    let host = url
        .host_str()
        .ok_or_else(|| TunnelError::Url("no host".into()))?;
    let port = url.port_or_known_default().unwrap_or(match scheme {
        "https" | "wss" => 443,
        _ => 80,
    });

    let mut selected_proxy = match &proxy.explicit {
        Some(explicit) => Some(explicit.clone()),
        None => proxy_from_env(scheme, host)?,
    };
    if let (Some(selected), Some(auth)) = (selected_proxy.as_mut(), &proxy.auth) {
        selected.auth = Some(auth.clone());
    }

    // Streams are boxed so we can hold any of TCP, a CONNECT tunnel,
    // or a TLS wrapper over either without spreading generics through
    // the rest of the function.
    let transport: Pin<Box<dyn AsyncReadWrite + Send>> = match &selected_proxy {
        Some(selected) => Box::pin(connect_via_proxy(selected, host, port).await?),
        None => Box::pin(TcpStream::connect((host, port)).await?),
    };
    let stream: Pin<Box<dyn AsyncReadWrite + Send>> = match scheme {
        "http" | "ws" => transport,
        _ => Box::pin(tls_connect(host, transport).await?),
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
    let conn_task = tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder()
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
        );
    if let Some(admission) = admission {
        builder = builder
            .header(PEER_TOKEN_ID_HEADER, admission.token_id)
            .header(
                hyper::header::AUTHORIZATION,
                format!("Bearer {}", admission.token_secret),
            )
            .header(PEER_NODE_ID_HEADER, admission.node_id)
            .header(PEER_CAPABILITIES_HEADER, admission.requested_capabilities);
        if let Some(node_name) = admission.node_name {
            builder = builder.header(PEER_NODE_NAME_HEADER, node_name);
        }
    }

    let req = builder
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
    drop(conn_task);

    Ok(InitiatorReady {
        upgraded,
        remote_authority: authority,
    })
}

/// Build a 101 Switching Protocols response for a peer WS upgrade
/// request. Validates Sec-WebSocket-Key, Sec-WebSocket-Version, and
/// requires the current Boardwalk peer subprotocol token.
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
        return Err(TunnelError::Upgrade(
            "missing or wrong Sec-WebSocket-Version".into(),
        ));
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
#[allow(unused_imports)]
pub use hyper_util::rt::TokioExecutor as H2Executor;

/// Helper trait alias for boxed I/O.
pub trait AsyncReadWrite: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncReadWrite for T {}

/// Establish a TLS connection over `transport` (direct TCP or a
/// CONNECT tunnel) for `host`. Uses `rustls-platform-verifier` so
/// certificates validate against the OS-native trust store (Keychain
/// on macOS, Schannel on Windows, system CA on Linux) — same trust
/// model the rest of the OS uses, no baked-in root bundle to keep
/// current.
async fn tls_connect<S>(
    host: &str,
    transport: S,
) -> Result<tokio_rustls::client::TlsStream<S>, TunnelError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    static PROVIDER_INSTALLED: std::sync::Once = std::sync::Once::new();
    PROVIDER_INSTALLED.call_once(|| {
        // Best-effort install. If something else installed first, fine.
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::aws_lc_rs::default_provider(),
        );
    });

    #[cfg(feature = "dangerous-test-tls")]
    let config = {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(dangerous_test_verifier())
            .with_no_client_auth()
    };
    #[cfg(not(feature = "dangerous-test-tls"))]
    let config = {
        use rustls_platform_verifier::BuilderVerifierExt;
        rustls::ClientConfig::builder()
            .with_platform_verifier()
            .map_err(|e| TunnelError::Upgrade(format!("rustls platform verifier: {e}")))?
            .with_no_client_auth()
    };
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let server_name = rustls_pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| TunnelError::Url(format!("invalid TLS server name: {e}")))?;
    let tls = connector
        .connect(server_name, transport)
        .await
        .map_err(TunnelError::Io)?;
    Ok(tls)
}

/// Test-only verifier that accepts any server cert. Enabled by the
/// `dangerous-test-tls` feature; never compiled into production builds.
#[cfg(feature = "dangerous-test-tls")]
fn dangerous_test_verifier() -> Arc<dyn rustls::client::danger::ServerCertVerifier> {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};

    #[derive(Debug)]
    struct Accept;
    impl ServerCertVerifier for Accept {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ED25519,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
            ]
        }
    }
    Arc::new(Accept)
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
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&k)
            .unwrap();
        assert_eq!(decoded.len(), 16);
    }

    fn env<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            vars.iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn proxy_url_userinfo_becomes_auth_and_is_percent_decoded() {
        let proxy = ProxyConfig::from_url_str("http://svc%40corp:p%40ss@proxy.internal:3128")
            .expect("valid proxy url");
        assert_eq!(proxy.host, "proxy.internal");
        assert_eq!(proxy.port, 3128);
        let auth = proxy.auth.expect("auth from userinfo");
        assert_eq!(auth.username, "svc@corp");
        assert_eq!(auth.password.expose(), "p@ss");
    }

    #[test]
    fn proxy_url_without_port_defaults_to_80() {
        let proxy = ProxyConfig::from_url_str("http://proxy.internal").unwrap();
        assert_eq!(proxy.port, 80);
        assert!(proxy.auth.is_none());
    }

    #[test]
    fn https_proxy_scheme_is_rejected() {
        let err = ProxyConfig::from_url_str("https://proxy.internal:3128").unwrap_err();
        assert!(err.contains("not supported"), "unexpected message: {err}");
    }

    #[test]
    fn proxy_auth_basic_header_value() {
        let auth = ProxyAuth {
            username: "user".into(),
            password: RedactedSecret::new("pass"),
        };
        let value = auth.basic_header_value().unwrap();
        assert_eq!(value.to_str().unwrap(), "Basic dXNlcjpwYXNz");
        assert!(value.is_sensitive());
    }

    #[test]
    fn proxy_auth_debug_redacts_password() {
        let auth = ProxyAuth {
            username: "user".into(),
            password: RedactedSecret::new("hunter2"),
        };
        let debug = format!("{auth:?}");
        assert!(!debug.contains("hunter2"), "leaked: {debug}");
    }

    #[test]
    fn env_resolution_picks_https_proxy_for_wss_targets() {
        let vars = [
            ("HTTPS_PROXY", "http://secure-proxy:3128"),
            ("HTTP_PROXY", "http://plain-proxy:3128"),
        ];
        let proxy = proxy_from_env_with(env(&vars), "wss", "cloud.example.com")
            .unwrap()
            .expect("proxy resolved");
        assert_eq!(proxy.host, "secure-proxy");
        let proxy = proxy_from_env_with(env(&vars), "ws", "cloud.example.com")
            .unwrap()
            .expect("proxy resolved");
        assert_eq!(proxy.host, "plain-proxy");
    }

    #[test]
    fn env_resolution_accepts_lowercase_variants() {
        let vars = [("https_proxy", "http://proxy:3128")];
        let proxy = proxy_from_env_with(env(&vars), "wss", "cloud.example.com").unwrap();
        assert!(proxy.is_some());
    }

    #[test]
    fn env_resolution_returns_none_without_vars() {
        let proxy = proxy_from_env_with(env(&[]), "wss", "cloud.example.com").unwrap();
        assert!(proxy.is_none());
    }

    #[test]
    fn env_resolution_errors_on_invalid_proxy_url() {
        let vars = [("HTTPS_PROXY", "socks5://proxy:1080")];
        let err = proxy_from_env_with(env(&vars), "wss", "cloud.example.com").unwrap_err();
        assert!(matches!(err, TunnelError::Proxy(_)), "got: {err}");
    }

    #[test]
    fn no_proxy_bypasses_exact_host_and_subdomains() {
        let vars = [
            ("HTTPS_PROXY", "http://proxy:3128"),
            ("NO_PROXY", "internal.test, example.com"),
        ];
        for host in ["example.com", "api.example.com", "internal.test"] {
            let proxy = proxy_from_env_with(env(&vars), "wss", host).unwrap();
            assert!(proxy.is_none(), "{host} should bypass the proxy");
        }
        // Suffix matches respect label boundaries.
        for host in ["notexample.com", "other.test"] {
            let proxy = proxy_from_env_with(env(&vars), "wss", host).unwrap();
            assert!(proxy.is_some(), "{host} should use the proxy");
        }
    }

    #[test]
    fn no_proxy_star_bypasses_everything() {
        let vars = [("HTTPS_PROXY", "http://proxy:3128"), ("NO_PROXY", "*")];
        let proxy = proxy_from_env_with(env(&vars), "wss", "cloud.example.com").unwrap();
        assert!(proxy.is_none());
    }

    #[test]
    fn no_proxy_accepts_leading_dot_entries() {
        let vars = [
            ("HTTPS_PROXY", "http://proxy:3128"),
            ("no_proxy", ".example.com"),
        ];
        let proxy = proxy_from_env_with(env(&vars), "wss", "api.example.com").unwrap();
        assert!(proxy.is_none());
    }

    #[test]
    fn loopback_targets_never_use_env_proxies() {
        let vars = [
            ("HTTPS_PROXY", "http://proxy:3128"),
            ("HTTP_PROXY", "http://proxy:3128"),
        ];
        for host in ["localhost", "127.0.0.1", "[::1]"] {
            let proxy = proxy_from_env_with(env(&vars), "ws", host).unwrap();
            assert!(proxy.is_none(), "{host} should never be proxied via env");
        }
    }
}
