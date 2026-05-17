# Peer Protocol (HTTP/2 over WebSocket-upgraded tunnel)

This replaces the original Z2Z (Zetta server-to-server) protocol that
used SPDY/3.1. The wire-visible handshake is unchanged; only the
post-upgrade multiplexing layer moves from SPDY to HTTP/2.

## Roles and direction

There are two roles, fixed for the lifetime of a connection.

| Role          | Opens the TCP socket | Becomes HTTP/2 _server_     | Becomes HTTP/2 _client_   |
|---------------|----------------------|------------------------------|----------------------------|
| **Initiator** | yes (outbound WS)    | **yes**                      | no                         |
| **Acceptor**  | no (accepts WS)      | no                           | **yes**                    |

In the original deployment the initiator is the "hub" (behind NAT) and
the acceptor is the cloud. The acceptor cannot dial into the initiator,
so the initiator dials out and the acceptor then drives HTTP/2 requests
the *other* way over that single socket.

## Phase 1 — WebSocket upgrade

The initiator sends an HTTP/1.1 `Upgrade: websocket` request. The path
is `/peers/{name}?connectionId={uuid}`. `{name}` is the initiator's
human-readable name; `{uuid}` is a v4/v7 UUID identifying this
particular connection attempt.

```
POST /peers/alice?connectionId=635712d6-03e7-4147-b33d-f80e14e4f74d HTTP/1.1
Host: bob.example.com
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Key: …
Sec-WebSocket-Version: 13
Sec-WebSocket-Protocol: boardwalk-peer/2
```

The acceptor responds:

```
HTTP/1.1 101 Switching Protocols
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Accept: …
Sec-WebSocket-Protocol: boardwalk-peer/2
```

> ⚠ **Crucial detail (preserved from original protocol):** after the 101
> response, **neither side speaks WebSocket framing on the connection
> any longer**. The WebSocket handshake is used solely as a transport
> negotiation that gets past HTTP-aware middleboxes. Both sides treat
> the underlying socket as a plain byte stream from here on.

The sub-protocol token `boardwalk-peer/2` distinguishes this from the
original SPDY-based Z2Z. The acceptor MAY also accept the legacy
`boardwalk-peer/1` token to interop with the Node implementation — see
[09-questions.md](09-questions.md) Q5.

## Phase 2 — HTTP/2 prior knowledge handshake

Both sides start an HTTP/2 connection on the raw upgraded socket using
*prior knowledge*: no `Upgrade:` headers, no ALPN, just the connection
preface and SETTINGS frames per RFC 9113.

- The **initiator** calls `h2::server::handshake(io)` and is ready to
  serve HTTP/2 requests.
- The **acceptor** calls `h2::client::handshake(io)` and obtains a
  `SendRequest` handle.

Both handshakes complete in parallel; there is no application-level
synchronization beyond the byte-level HTTP/2 preface.

## Phase 3 — Connection confirmation

The acceptor sends a single HTTP/2 request:

```
:method GET
:scheme http
:path   /_initiate_peer/{connectionId}
:authority {initiator-name}.peer.boardwalk.invalid
```

The initiator's router has a route for `/_initiate_peer/{id}` that:

1. Looks up the in-flight outbound peer record for `{id}`.
2. Marks it `connected`.
3. Returns `200 OK`.

Once the acceptor sees `200`, the connection is considered established.
A timeout (default 10 s, from the original) bounds this phase.

The synthetic authority `{name}.peer.boardwalk.invalid` is preserved
from the original protocol so existing tooling that inspects logs can
recognize peer traffic. We may also accept just `{name}.peers.boardwalk`
as a less-marketing-coded alternative.

## Phase 4 — Steady state

The acceptor can now issue HTTP requests against the initiator's router
as if they were normal HTTP/2:

```
GET /servers/{name}/devices                           # list devices
GET /servers/{name}/devices/{id}                       # device state
POST /servers/{name}/devices/{id}                      # transition
GET /servers/{name}/events?topic={…}                   # subscribe
POST /servers/{name}/events/unsubscribe                # unsubscribe
```

These are *the same routes* the local HTTP listener exposes — see
[01-architecture.md](01-architecture.md) "Runtime topology". The
initiator's `axum::Router` is mounted as a `tower::Service` and served
via `hyper::server::conn::http2::Builder::serve_connection(io, service)`
where `io` is the upgraded socket.

### Event streaming

The original Z2Z used SPDY server push: the response to
`GET /servers/{name}/events?topic=…` was a 200 OK, then the initiator
called `response.push(stream_url, headers)` for each event.

We use **long-lived response bodies** instead:

- The acceptor's `GET /servers/{name}/events?topic=…` returns
  `200 OK` with `Content-Type: application/json` and **does not close
  the body**. The body is framed: a stream of length-prefixed or
  newline-delimited JSON records, one per event.
- Unsubscribe is either a `POST /events/unsubscribe` (compat with old
  protocol) **or** simply RST_STREAM on the HTTP/2 stream (cleaner).
  We support both. See Q3 in questions.

This is simpler than h2 server push, doesn't depend on a deprecated
feature, and matches what every modern long-poll/reverse-tunnel system
(gRPC streaming, SSE-over-h2, etc.) does.

### Keepalive

HTTP/2 has PING frames built in. `h2::client::Connection` and
`h2::server::Connection` expose a `ping` method. We use HTTP/2 PING for
keepalive instead of the original's SPDY PING + 30-second timer, with
the same semantics: 10 s interval (matching the acceptor's
`_pingTimeout`), and a 30 s deadline before tearing down (matching the
initiator's `pingTimeout`).

### Reconnect / backoff

Initiator only. Exponential backoff identical to the original:

- min 100 ms
- max 30 s
- jitter up to 1 s
- `delay = min(max, min * 2^attempts) + rand(0..jitter_max)`

This matches `PeerClient.generateBackoff` exactly.

## Errors

| Phase | Failure                       | Initiator action     | Acceptor action     |
|-------|-------------------------------|----------------------|---------------------|
| 1     | WS upgrade rejected           | backoff + retry      | log, close          |
| 2     | h2 preface / SETTINGS bad     | backoff + retry      | log, close          |
| 3     | confirmation 4xx/5xx          | backoff + retry      | close               |
| 3     | confirmation timeout          | backoff + retry      | already closed      |
| 4     | h2 GOAWAY                     | backoff + retry      | close on next ping  |
| 4     | PING timeout                  | force-close + retry  | force-close         |

## Security

The original supported `http` and `https` for the initial connection
URL, with TLS terminated at the WebSocket layer. We preserve that: the
upgraded socket is whatever `tokio_rustls::TlsStream<TcpStream>` (for
`wss://`) or `TcpStream` (for `ws://`) wraps. `h2` accepts either.

Auth is layered on top — the acceptor MAY require an Authorization
header on the initial `POST /peers/...` request. This is out of scope
for v0; we leave a `peer_authenticator` hook.

## Differences from original Z2Z

| Aspect          | Original (SPDY)            | boardwalk-rs (HTTP/2)          |
|-----------------|----------------------------|----------------------------|
| Multiplex frame | SPDY/3.1                   | HTTP/2 (RFC 9113)          |
| Server push     | Yes (SPDY push)            | No — use long body         |
| Keepalive       | SPDY PING                  | HTTP/2 PING                |
| Handshake       | WS → SPDY                  | WS → HTTP/2 prior knowledge|
| Sub-protocol    | (none)                     | `boardwalk-peer/2`             |
| Auth            | None built in              | Hook reserved              |
