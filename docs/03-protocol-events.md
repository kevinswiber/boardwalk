# Multiplexed WebSocket Event Sub-Protocol

Preserved largely as-is from the original wiki page
"Multiplexed-WebSocket-Streams". This is the **client-facing** event
stream; the peer-to-peer event stream is described in
[02-protocol-peer.md](02-protocol-peer.md) and uses a different
mechanism (long-lived HTTP/2 response bodies).

## Entry point

Client API root advertises:

```json
{ "rel": ["https://rels.boardwalk.dev/events"], "href": "ws://example.com/events" }
```

Sub-protocol token: `boardwalk-events/1` (advertised via
`Sec-WebSocket-Protocol`).

`?filterMultiple=true` query parameter changes the event message shape:
`subscriptionId` becomes an array when set.

## Topic format

```
{server}/{device-type}/{device-id}/{stream}
```

For a CaQL query subscription:

```
{server}/query/{CaQL expression}
```

Wildcards:

- `*` matches a single path segment.
- `**` matches one or more path segments.

Regex: a topic segment wrapped in `{}` is treated as a regular
expression (`{^Det.+$}/thermostat/.../temperature`).

CaQL filter on stream data: `?` followed by a CaQL expression at the
end of the topic, e.g. `Detroit/thermostat/*/temperature?select * where data > 85`.

## Messages

All messages are JSON. We use **NDJSON** internally and at peer
boundaries, but over a WebSocket each frame contains exactly one JSON
document — matching the original.

### Subscribe (client → server)

```json
{ "type": "subscribe", "topic": "Detroit/arm/abc/state", "limit": 10 }
```

`limit` (optional, integer) — auto-unsubscribe after N events; default
is unbounded.

### Subscribe-ack (server → client)

```json
{
  "type": "subscribe-ack",
  "timestamp": 1730000000000,
  "topic": "Detroit/arm/abc/state",
  "subscriptionId": 7
}
```

### Event (server → client)

```json
{
  "type": "event",
  "topic": "Detroit/arm/abc/state",
  "subscriptionId": 7,
  "timestamp": 1730000000123,
  "data": "moving-claw"
}
```

If `filterMultiple=true`, `subscriptionId` is an array.

### Unsubscribe (client → server)

```json
{ "type": "unsubscribe", "subscriptionId": 7 }
```

### Unsubscribe-ack (server → client)

```json
{
  "type": "unsubscribe-ack",
  "timestamp": 1730000000456,
  "subscriptionId": 7
}
```

### Ping / pong

```json
{ "type": "ping", "data": "anything" }
{ "type": "pong", "timestamp": 1730000000789, "data": "anything" }
```

Either side MAY send `ping`. The peer SHOULD pong promptly. We also
emit WebSocket-level `Ping` control frames for low-level keepalive (the
JSON form is application-level).

### Error

```json
{
  "type": "error",
  "code": 400,
  "timestamp": 1730000000000,
  "topic": "...",
  "message": "..."
}
```

Codes from the original spec:
- `400` Bad Request — invalid JSON
- `405` Method Not Supported — unknown `type` value
- `500` Server Error

## Implementation notes (boardwalk-events)

```rust
pub struct EventBus {
    subscriptions: DashMap<SubscriptionId, Subscription>,
    topic_index: TopicTrie, // for matching publishes to subscribers
}

pub struct Subscription {
    topic: TopicPattern,
    sender: mpsc::Sender<Event>,
    remaining: Option<u64>, // for `limit`
    filter: Option<CaqlFilter>,
}
```

- Each WebSocket connection owns its `mpsc::Sender` for inbound events.
- The bus indexes subscriptions by parsed `TopicPattern` (literal +
  wildcard + regex segments). On `publish`, we walk the index;
  `O(segments × matching)` per publish.
- Deduplication for `filterMultiple=false`: a publish that matches
  multiple of a single connection's subscriptions yields multiple
  event messages, each with its own `subscriptionId`. The
  `filterMultiple=true` mode collapses these into one event message
  with `subscriptionId: [..]`.
- The bus is the single fanout point. Peer-side events arriving via
  the reverse tunnel (see peer protocol doc) are published into the
  same bus with `from_remote: true` marking, which lets subscribers
  scope to local vs. remote if needed.

## Compatibility

The original Zetta Node implementation used the same JSON shape; the
only divergence we introduce is the explicit `Sec-WebSocket-Protocol`
negotiation token. Clients that don't send the token still get
served — we just don't echo the token back.
