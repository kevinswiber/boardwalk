# Peers (reverse-tunnel federation)

A Boardwalk **hub** can dial a **cloud** and become reachable through
that cloud, even when the hub sits behind a NAT or firewall. The cloud
proxies HTTP requests and WebSocket subscriptions back through the
same outbound socket the hub opened.

## How the link is established

1. The hub does `Boardwalk::new().link("https://cloud.example.com")`.
2. The hub opens a WebSocket to `wss://cloud.example.com/peers/<hub-name>`
   with the `boardwalk-peer/1` subprotocol token.
3. Once the upgrade succeeds, both sides drop WebSocket framing and
   speak HTTP/2 prior-knowledge over the raw stream — **with reversed
   roles**: the cloud is the HTTP/2 client, the hub is the HTTP/2
   server.
4. The cloud sends a `GET /_initiate_peer/<connection-id>` over the
   tunnel; the hub confirms; the link is live.

After that, anything the cloud receives at `/servers/<hub-name>/...`
gets forwarded over the tunnel.

## Setting up a link

Federation today is configured through the `Boardwalk::new()` server adapter.
Actors built with `NodeBuilder` need an HTTP adapter, such as the job-runner
example's local router, before they are reachable over peer-forwarded requests.

**Hub side:**

```rust,ignore
Boardwalk::new()
    .name("hub")
    .link("wss://cloud.example.com")
    .listen("127.0.0.1:1338".parse()?)
    .await?
```

**Cloud side:**

```rust,ignore
Boardwalk::new()
    .name("cloud")
    .listen("0.0.0.0:443".parse()?)
    .await?
```

The cloud doesn't need to know in advance that a hub will link to it —
it just accepts whatever peers connect.

## Reaching a hub through the cloud

Once linked, the cloud proxies everything for the hub:

```
# Read a resource on the hub via the cloud
curl https://cloud.example.com/servers/hub/resources/<id>

# Drive a transition
curl -H 'content-type: application/json' \
  -d '{}' \
  https://cloud.example.com/servers/hub/resources/<id>/transitions/turn-on
```

The cloud's Siren root advertises connected peers, so a client crawling
links discovers them:

```
curl https://cloud.example.com/ | jq '.links[] | select(.rel[] | contains("peer"))'
```

## Forwarded event subscriptions

A WebSocket client connected to the cloud's `/events` can subscribe to
any hub topic. The cloud opens a long-lived NDJSON `GET` to the hub's
`/servers/<hub>/events?topic=...` and re-emits each event over the
multiplex WS:

```json
{ "type": "subscribe", "topic": "hub/led/<id>/state" }
```

Multiple cloud-side subscribers to the same `(hub, topic)` share a
single upstream stream — the cloud deduplicates.

## TLS

Peer dials use [rustls](https://docs.rs/rustls/) with the OS-native
trust store via
[rustls-platform-verifier](https://docs.rs/rustls-platform-verifier/).
The hub's TLS config matches the rest of your OS — there's no separate
cert bundle to keep current.

For tests with self-signed certs, the `boardwalk-tunnel` crate has a
`dangerous-test-tls` feature that disables verification. **Never** turn
this on in production.

## Reconnect behavior

The hub's `PeerClient` runs in a loop with backoff: connect, run until
the connection closes, wait, reconnect. The link state is propagated
to the cloud's peer registry via `409 Conflict` on the second
connection attempt for the same name (preventing accidental hijacks).

When a hub goes offline mid-request, the cloud returns `502 Bad Gateway`
on forwarded calls until the hub reconnects.

## Graceful shutdown

```rust,ignore
Boardwalk::new()
    .name("hub")
    .link("wss://cloud.example.com")
    .listen_until(addr, signal_future)
    .await?
```

`listen_until` accepts any `Future<Output = ()>` as the shutdown
signal. Peer tasks, app tasks, and scout tasks are aborted on return.
