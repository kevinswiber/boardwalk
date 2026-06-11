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
   `Boardwalk::new().link("https://cloud.example.com")`, and the
   accepting cloud must opt in to unauthenticated peers:

   ```rust,ignore
   // accepting side (cloud), local development only
   Boardwalk::new()
       .name("cloud")
       .allow_unauthenticated_local_peers()
       .listen("127.0.0.1:1337".parse()?)
       .await?
   ```
2. The hub opens a WebSocket to `wss://cloud.example.com/peers/<route-name>`
   with the `boardwalk-peer/3` subprotocol token.
3. Token-bound peer links send `Authorization: Bearer <token>`,
   `Boardwalk-Peer-Token-Id`, `Boardwalk-Node-Id`,
   `Boardwalk-Node-Name`, and `Boardwalk-Peer-Capabilities` before
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

The cloud side can require a shared token for a route.
`accept_peer_token` admits its peer at the `resource.read` ceiling and
panics if the route name is invalid — security configuration is never
logged and skipped:

```rust,ignore
Boardwalk::new()
    .name("cloud")
    .accept_peer_token("hub", "token-id", std::env::var("BOARDWALK_HUB_TOKEN")?)
    .listen("0.0.0.0:443".parse()?)
    .await?
```

The full admission config widens the ceiling and pins identity.
`PeerAdmission::shared_token` validates at construction and returns
`Result`, so a bad value fails at the line that contains it; the
builder chain stays infallible:

```rust,ignore
use boardwalk::{Boardwalk, PeerAdmission, PeerCapability};

Boardwalk::new()
    .name("cloud")
    .accept_peer(
        PeerAdmission::shared_token("hub", "token-id", std::env::var("BOARDWALK_HUB_TOKEN")?)?
            .expected_node_id("node-hub-7f3a")
            .allow([
                PeerCapability::ResourceRead,
                PeerCapability::StreamSubscribe,
                PeerCapability::TransitionInvoke,
            ]),
    )
    .listen("0.0.0.0:443".parse()?)
    .await?
```

`.allow([...])` **replaces** the ceiling with exactly the set you pass —
the default ceiling is `resource.read` only, and widening is always a
visible act.

Token secrets belong in runtime configuration or environment variables.
They are used for admission checks (constant-time comparison) and are
not persisted into peer records. A token is bound to a route name.

`expected_node_id` is an exact string match against the node id the
connecting peer self-asserts during the upgrade, checked under token
possession. It guards against the wrong legitimate node using a token —
a misconfiguration guard, not identity proof: a token thief can present
any node id. Cryptographic node identity is out of scope for this
release.

A cloud with no admission configuration refuses every peer upgrade with
`403` and the body `peer admission is not configured`, before the
WebSocket upgrade completes. `allow_unauthenticated_local_peers()` is
the explicit local-development opt-in: every admitted unauthenticated
peer receives the local-development capability ceiling (currently the
full capability set) as both its allowed and negotiated capabilities.
The opt-in applies only while no token admission is configured; once any
`accept_peer_token` or `accept_peer` entry exists, token-bound admission
is required for all peers.

Known wrinkle: under `allow_unauthenticated_local_peers()`, a presented
token is ignored and the peer is admitted at the local-development
ceiling. Tokened links should target token-configured nodes.

## Outbound Links

Token-bound outbound links are configured with `link_peer`.
`PeerLink::new` validates the gateway URL and route name at
construction; the requested capability set defaults to `resource.read`
only, and `.request_capabilities([...])` replaces it with exactly the
set you pass. The acceptor intersects the request with its configured
ceiling; an empty intersection is refused:

```rust,ignore
use boardwalk::{Boardwalk, PeerCapability, PeerLink};

Boardwalk::new()
    .name("hub")
    .use_actor(Led::default())
    .link_peer(
        PeerLink::new("wss://cloud.example.com", "hub")?
            .token("token-id", std::env::var("BOARDWALK_HUB_TOKEN")?)
            .node_id("node-hub-7f3a")
            .request_capabilities([
                PeerCapability::ResourceRead,
                PeerCapability::StreamSubscribe,
            ]),
    )
    .listen("127.0.0.1:1338".parse()?)
    .await?
```

The public `.link(...)` convenience is a trusted local-development peer
initiator and pairs with an opted-in cloud; it panics on an invalid
URL. A cloud exposed beyond a trusted development environment should
use token admission and TLS, and should reject unknown peers before the
WebSocket upgrade completes.

```rust,ignore
Boardwalk::new()
    .name("hub")
    .use_actor(Led::default())
    .link("wss://cloud.example.com")
    .listen("127.0.0.1:1338".parse()?)
    .await?
```

## Capabilities

Peer admission negotiates coarse capabilities. Configuration uses the
typed `PeerCapability` enum; the canonical dotted names remain the wire
format and round-trip through `Display`/`FromStr`. The negotiated set
(requested ∩ allowed) is the ceiling for both gateway forwarding and
rendered hypermedia:

- `PeerCapability::ResourceRead` (`resource.read`) permits resource and
  metadata reads.
- `PeerCapability::ResourceQuery` (`resource.query`) permits directed
  peer queries with `?ql=<caql>`.
- `PeerCapability::StreamSubscribe` (`stream.subscribe`) permits peer
  event streams and stream links.
- `PeerCapability::TransitionInvoke` (`transition.invoke`) permits
  transition POST forwarding and transition actions in rendered
  resources.
- `PeerCapability::ResourceRegister` (`resource.register`) is reserved
  for resource registration forwarding.
- `PeerCapability::PeerAdmin` (`peer.admin`) is reserved for future
  peer administration surfaces.

`.allow` and `.request_capabilities` replace the respective set with
exactly what you pass — they never union with the default.

If the requested and allowed capability sets have an empty intersection,
admission fails with `403`. The wire response stays generic; the
server-side log enumerates the requested-vs-allowed names (see
Observability below).

## Caller Provenance

Inside a transition handler, `TransitionCtx::provenance()` reports
where the invocation came from, as far as the serving node can verify:

- `is_local()` — direct local invocations and anonymous public HTTP
  callers (the node saw no verifiable forwarding metadata).
- `forwarded_by()` — the gateway route that forwarded the request over
  the node's own authenticated tunnel leg.
- `peer()` — the gateway-attested admitted caller, when one exists.
  `None` means anonymous or local.

The trust rule: forwarded/attested caller headers are honored only when
the request arrived over a tunnel this node itself dialed. Forged
headers on a public listener are ignored. Public HTTP callers reaching
a gateway are anonymous — downstream nodes see `peer() == None` —
unless they present admission credentials per the Caller Ingress
section below. Resource reads (`ResourceCtx`) do not carry provenance
in this release.

## Caller Ingress

An admitted peer — one that dialed in under token-bound admission and
holds a live tunnel — can place ordinary HTTP requests at the gateway
that carry its admission context. The caller attaches the same
credential pair the handshake uses, per request:

```text
boardwalk-peer-token-id: <token-id>
Authorization: Bearer <secret>
```

The header name is exported as `boardwalk::PEER_TOKEN_ID_HEADER` so
consumers never hardcode the string. The gateway verifies the
credentials against its configured admissions, resolves the live
admitted context for the verified route, and forwards the request with
the attested caller identity; downstream transition handlers see it via
`TransitionCtx::provenance().peer()`.

Presenting `boardwalk-peer-token-id` opts the request into
authentication, fail-closed:

- Missing or unknown credentials → `401` (`missing bearer token`,
  `unknown peer token id`, `invalid bearer token`).
- Admission not configured → `403` (`peer admission is not configured`).
- Valid credentials but no live tunnel under that token at the
  configured route → `403` (`caller peer is not connected`).
- One token id valid for more than one live route → `403`
  (`ambiguous caller admission`) — refused rather than guessed.

There is no anonymous fallback once the header is present. Requests
without the header stay anonymous (`peer() == None` downstream) —
unchanged behavior.

The live-tunnel requirement is what keeps the attestation honest:
negotiated capabilities are a handshake product (requested ∩ allowed)
and the connection identity is real, so nothing is synthesized
per-request. A configured-but-disconnected peer is refused.

Capability composition: a forwarded request passes two ceilings — the
caller's negotiated set and the target link's negotiated set — enforced
as two sequential gates. A read-only caller cannot invoke transitions
through the gateway (`403` with `caller capability denied`), no matter
what the target link negotiated.

Caller credentials never cross the forwarding hop: sanitization strips
`Authorization` and all `boardwalk-*` headers before the gateway stamps
its own attestation.

Boundaries, stated plainly:

- Ingress applies to the gateway *forwarding* path only. A credentialed
  request whose target is the gateway node itself keeps local
  provenance.
- Streams and WebSocket subscriptions carry no caller attestation in
  this release.
- Unauthenticated opt-in peers (`allow_unauthenticated_local_peers`)
  have no credential to present and cannot use caller ingress; the
  mechanism requires token-bound admission.
- Which actor identities a caller may write *as* is gateway-consumer
  policy (for example a relay's peer-to-actor binding rules), not
  boardwalk's.

## Observability

Every admission, capability, and caller ingress deny decision emits one
structured `tracing` event at the stable target `boardwalk::admission`
(level `warn`). Stable fields:
`kind` (`admission` | `capability` | `ingress`), `route`, `reason`,
`status`, plus `token_id`, `node_id`, `connection_id`, `intent`, and
`negotiated` where known. The
empty-intersection refusal also logs `requested` and `allowed`
capability names — log-side only; the wire body stays generic.

Ingress credential denials (`kind = "ingress"`) always carry `token_id`
(the engagement signal) and carry `route` once a verified admission
names one; they never enumerate capabilities. Caller-side capability
denials reuse `kind = "capability"` with a `caller` field carrying the
attested `peer_id`, distinguishing them from target-side denials.

Alert on repeated `unknown peer token id` (credential guessing),
`peer node id mismatch` (token-theft signal), and denied
`transition.invoke` intents (probing the operative path). Repeated
`kind = "ingress"` with `invalid bearer token` is credential guessing
against a live route; `caller peer is not connected` churn flags a
flapping reviewer link.

## Node Identity

On first persisted startup (persistence enabled, no persisted record,
no explicit `.node_id()`), the node generates a UUID node id, persists
it, and logs it at `info` so operators can write acceptor-side
`expected_node_id` bindings. The id is sticky across renames as long as
persistence stays enabled. Non-persisted nodes default the node id to
the display name — renaming a non-persisted node changes its presented
identity and rewrites its stream ids.

## Gateway Policy

The gateway recognizes Boardwalk peer routes before forwarding. Unknown
peer paths are rejected instead of being tunneled as raw HTTP. Forwarded
requests strip inbound `Forwarded`, `X-Forwarded-*`,
`Boardwalk-External-*`, `Proxy-*`, `Proxy-Connection`, credential,
hop-by-hop, and WebSocket negotiation headers, then write fresh
gateway-owned forwarding metadata for the peer.

Rendered responses are policy-aware. Root peer links require that
specific peer's `resource.read`; query actions require `resource.query`;
transition actions require `transition.invoke`; and stream links require
`stream.subscribe`.

Persistent peer records keep peer config separate from latest connection
status. The peer config is the durable route identity and admission
metadata; latest connection status tracks the current or most recent
tunnel connection without changing that peer identity.

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

For local-development peer shutdown wiring (the cloud at
`127.0.0.1:1337` opts in with `allow_unauthenticated_local_peers()`):

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
