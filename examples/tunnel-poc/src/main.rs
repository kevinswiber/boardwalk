//! Role-reversed HTTP/2 proof of concept.
//!
//! Two tasks share a `tokio::io::duplex` pair. Task A — the "initiator"
//! that *would* have opened the outbound WebSocket — calls
//! `h2::server::handshake` and serves requests. Task B — the "acceptor"
//! that *would* have accepted the WebSocket — calls
//! `h2::client::handshake` and drives a GET at task A.
//!
//! If this passes, the peer protocol is feasible: in production we
//! just swap the duplex pair for a real TCP socket whose WebSocket
//! handshake has completed.

use std::convert::Infallible;

use anyhow::Result;
use bytes::Bytes;
use http::Request;
use tokio::io::duplex;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info,h2=warn")
        .init();

    // Bidirectional in-memory pipe; 64 KiB each direction.
    let (initiator_io, acceptor_io) = duplex(64 * 1024);

    let initiator = tokio::spawn(run_initiator(initiator_io));
    let acceptor = tokio::spawn(run_acceptor(acceptor_io));

    initiator.await??;
    acceptor.await??;

    info!("tunnel PoC OK — role-reversed HTTP/2 works over an arbitrary stream");
    Ok(())
}

/// "Initiator" side: hosts the HTTP/2 server.
async fn run_initiator(io: tokio::io::DuplexStream) -> Result<()> {
    let mut connection = h2::server::handshake(io).await?;
    info!("initiator: h2 server handshake done");

    while let Some(req_result) = connection.accept().await {
        let (request, mut respond) = req_result?;
        let path = request.uri().path().to_string();
        info!("initiator: received request {}", path);

        if path.starts_with("/_initiate_peer/") {
            let connection_id = path.trim_start_matches("/_initiate_peer/");
            info!("initiator: peer confirmed connection_id={}", connection_id);

            let response = http::Response::builder().status(200).body(())?;
            let mut send_stream = respond.send_response(response, false)?;
            send_stream.send_data(Bytes::from_static(b"ok"), true)?;
        } else {
            let response = http::Response::builder().status(404).body(())?;
            respond.send_response(response, true)?;
        }
    }

    info!("initiator: connection closed");
    Ok::<_, anyhow::Error>(())
}

/// "Acceptor" side: drives HTTP/2 requests at the initiator.
async fn run_acceptor(io: tokio::io::DuplexStream) -> Result<()> {
    let (mut send_request, connection) = h2::client::handshake(io).await?;
    info!("acceptor: h2 client handshake done");

    // The h2 client returns a future that drives the connection.
    // We have to poll it concurrently with our requests.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!("acceptor: h2 connection ended: {:?}", e);
        }
    });

    // Build a peer-confirmation request — the same shape the real
    // acceptor sends to the initiator across the tunnel.
    let request = Request::builder()
        .method("GET")
        .uri("http://alice.peer.boardwalk.invalid/_initiate_peer/635712d6-03e7-4147-b33d-f80e14e4f74d")
        .body(())?;

    let (response_fut, _send_stream) = send_request.send_request(request, true)?;
    let response = response_fut.await?;
    info!("acceptor: response status = {}", response.status());

    let mut body = response.into_body();
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        info!("acceptor: body chunk: {:?}", chunk);
    }

    // For type-inference housekeeping.
    let _: fn() -> Result<Infallible, Infallible> = || unreachable!();
    Ok(())
}
