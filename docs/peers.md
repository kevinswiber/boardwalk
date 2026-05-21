# Peers (reverse-tunnel federation)

A Boardwalk **hub** can dial a **cloud** and become reachable through
that cloud, even when the hub sits behind a NAT or firewall. The cloud
proxies allowed HTTP requests and WebSocket subscriptions back through
the same outbound socket the hub opened.

Boardwalk keeps three names separate:

- A **Resource** is addressable state rendered through `/resources`.
- an **Actor** owns resource behavior and transitions inside a runtime.
- a **Node** owns actors, the resource directory, and event streams.

Peer routing uses a stable **route name** such as `hub` in
`/servers/hub/...`. Admission can also carry a stable node id and, when
configured, an **expected node id** hard-locks that route so another node
cannot claim the same route name with a valid token for a different
identity.

## How the Link Is Established

1. For local development, the hub can do
   `Boardwalk::new().link("https://cloud.example.com")`.
2. The hub opens a WebSocket to `wss://cloud.example.com/peers/<route-name>`
   with the `boardwalk-peer/3` subprotocol token.
3. Token-bound peer links send `Authorization: Bearer <token>`,
   `X-Boardwalk-Peer-Token-Id`, `X-Boardwalk-Node-Id`,
   `X-Boardwalk-Node-Name`, and `X-Boardwalk-Peer-Capabilities` before
   the cloud returns `101 Switching Protocols`.
4. Once the upgrade succeeds, both sides drop WebSocket framing and speak
   HTTP/2 prior-knowledge over the raw stream, with reversed roles: the
   cloud is the HTTP/2 client, and the hub is the HTTP/2 server.
5. The cloud sends `GET /_initiate_peer/<connection-id>` over the tunnel;
   the hub confirms; the link is live.

After that, cloud requests at `/servers/<route-name>/...` are forwarded
over the tunnel only when the admitted peer's negotiated capabilities
allow the route.

## Admission Configuration

Federation is configured through the `Boardwalk::new()` builder. Actors
registered with `Boardwalk::new().use_actor(...)` are placed into the
same `Node` that serves local resource routes and peer-forwarded
requests. A `Node` built directly with `NodeBuilder` is the lower-level
in-process runtime; register actors through `Boardwalk` when they should
be reachable through the supplied HTTP and peer routes.

The cloud side can require a shared token for a route:

```rust,ignore
Boardwalk::new()
    .name("cloud")
    .accept_peer_token("hub", "token-id", std::env::var("BOARDWALK_HUB_TOKEN")?)
    .listen("0.0.0.0:443".parse()?)
    .await?
```

Token secrets belong in runtime configuration or environment variables.
They are used for admission checks and are not persisted into peer
records. A token is bound to a route name; an admission policy can also
require the expected node id presented during the upgrade.

The public `.link(...)` convenience remains a trusted local-development
peer initiator. At present, public outbound token-bound links are not available yet;
a cloud exposed beyond a trusted development environment should use token admission
and TLS, and should reject unknown peers before the WebSocket upgrade completes.

```rust,ignore
Boardwalk::new()
    .name("hub")
    .use_actor(Led::default())
    .link("wss://cloud.example.com")
    .listen("127.0.0.1:1338".parse()?)
    .await?
```

## Capabilities

Peer admission negotiates coarse capabilities. The negotiated set is the
ceiling for both gateway forwarding and rendered hypermedia:

- `resource.read` permits resource and metadata reads.
- `resource.query` permits directed peer queries with `?ql=<caql>`.
- `stream.subscribe` permits peer event streams and stream links.
- `transition.invoke` permits transition POST forwarding and transition
  actions in rendered resources.
- `resource.register` is reserved for resource registration forwarding.
- `peer.admin` is reserved for future peer administration surfaces.

If the requested and allowed capability sets have an empty intersection,
admission fails with `403`.

## Gateway Policy

The gateway recognizes Boardwalk peer routes before forwarding. Unknown
peer paths are rejected instead of being tunneled as raw HTTP. Forwarded
requests strip inbound `Forwarded`, `X-Forwarded-*`,
`X-Boardwalk-External-*`, `Proxy-*`, `Proxy-Connection`, credential,
hop-by-hop, and WebSocket negotiation headers, then write fresh
gateway-owned forwarding metadata for the peer.

Rendered responses are policy-aware. Root peer links require that
specific peer's `resource.read`; query actions require `resource.query`;
transition actions require `transition.invoke`; and stream links require
`stream.subscribe`.

## Queries

Use the same CaQL syntax for local and directed peer queries:

```text
GET /resources?ql=where%20kind%20%3D%20%22led%22
GET /servers/hub?ql=where%20kind%20%3D%20%22led%22
GET /servers/hub/resources?ql=where%20kind%20%3D%20%22led%22
```

Directed peer query requires `resource.query` and forwards to exactly
one peer. Wildcard fan-out is not enabled: `/?server=*&ql=...` returns
`400` with `unsupported-federation-query` until explicit fan-out policy
and limits exist.

## Reaching a Hub Through the Cloud

Once linked, allowed cloud routes proxy to the hub:

```text
# Read a resource on the hub via the cloud
curl https://cloud.example.com/servers/hub/resources/<id>

# Drive a transition
curl -H 'content-type: application/json' \
  -d '{}' \
  https://cloud.example.com/servers/hub/resources/<id>/transitions/turn-on
```

The cloud's Siren root advertises connected peers only when that peer
has `resource.read`, so a client crawling links discovers peers it can
actually read:

```text
curl https://cloud.example.com/ | jq '.links[] | select(.rel[] | contains("peer"))'
```

## Forwarded Event Subscriptions

A WebSocket client connected to the cloud's `/events` can subscribe to a
hub topic only when that peer has `stream.subscribe`. The cloud opens a
long-lived NDJSON `GET` to the hub's
`/servers/<hub>/events?topic=...` and re-emits each event over the
multiplex WS:

```json
{ "type": "subscribe", "topic": "hub/led/<id>/state" }
```

Multiple cloud-side subscribers to the same `(hub, topic)` share one
upstream stream.

## Peer Management

`/peer-management` is hidden by default and returns `404`. Boardwalk does
not expose public peer mutation or administration actions until there is
a real admin policy and resource contract.

## TLS

Peer dials use [rustls](https://docs.rs/rustls/) with the OS-native
trust store via
[rustls-platform-verifier](https://docs.rs/rustls-platform-verifier/).
The hub's TLS config matches the rest of your OS; there is no separate
certificate bundle to keep current.

For tests with self-signed certs, the `boardwalk-tunnel` crate has a
`dangerous-test-tls` feature that disables verification. Never turn this
on in production.

## Reconnect Behavior

The hub's peer client runs in a loop with backoff: connect, run until
the connection closes, wait, reconnect. Token-bound admission records
the latest connection separately from the durable peer route, so
reconnects do not change the peer identity.

When a hub goes offline mid-request, the cloud returns `502 Bad Gateway`
on forwarded calls until the hub reconnects.

## Graceful Shutdown

For local-development peer shutdown wiring:

```rust,ignore
Boardwalk::new()
    .name("hub")
    .link("ws://127.0.0.1:1337")
    .listen_until(addr, signal_future)
    .await?
```

`listen_until` accepts any `Future<Output = ()>` as the shutdown signal.
Use `listen_until_on` when the listener is already bound, such as in test
harnesses that need to reserve an ephemeral port. Peer tasks, app tasks,
and scout tasks are aborted on return.

## Non-Goals

This peer boundary does not add OAuth, mTLS-based peer identity, RBAC,
multi-hop federation, durable event-history repositories, wildcard
federation fan-out, reactive query streams, or a public Worker type.
