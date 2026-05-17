//! WebSocket-upgrade-then-HTTP/2 tunnel primitive.
//!
//! After a 101 Switching Protocols handshake, both sides drop WebSocket
//! framing entirely and speak HTTP/2 over the raw stream. The side that
//! originally opened the WS (initiator) becomes the HTTP/2 server; the
//! side that accepted (acceptor) becomes the HTTP/2 client. See
//! `docs/02-protocol-peer.md`.
//!
//! Full implementation lands in M0 (the PoC) and M7 (production wiring).

#![forbid(unsafe_code)]

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TunnelError {
    #[error("websocket upgrade failed: {0}")]
    Upgrade(String),
    #[error("h2 handshake failed: {0}")]
    H2(#[from] h2::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub const SUBPROTOCOL: &str = "zetta-peer/2";
