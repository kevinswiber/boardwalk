//! Peer client (outbound) and peer socket (inbound).
//!
//! Full implementation in M7.

#![forbid(unsafe_code)]

use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum PeerError {
    #[error("tunnel: {0}")]
    Tunnel(#[from] zetta_tunnel::TunnelError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Configuration for a peer client (outbound link to a remote Zetta).
pub struct PeerClientConfig {
    pub url: Url,
    pub name: String,
}
