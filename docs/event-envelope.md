# Event envelope

Every event Boardwalk fans out — to local WS subscribers, peer NDJSON
streams, and the in-memory replay cache — carries the same canonical
envelope. Wire serializations are camelCase; this document uses the
runtime Rust field names alongside.

## Shape

| Wire key          | Rust field          | Type                    | Notes |
| ----------------- | ------------------- | ----------------------- | ----- |
| `envelopeVersion` | `envelope_version`  | `u8` (currently `1`)    | Bumped on breaking envelope changes. |
| `eventId`         | `event_id`          | opaque string           | Globally unique per node lifetime. Consumers must not parse. |
| `nodeId`          | `node_id`           | string                  | Local node's server name. |
| `resourceId`      | `resource_id`       | string                  | UUID today; opaque to consumers. |
| `resourceKind`    | `resource_kind`     | string                  | e.g. `led`, `sensor`, `job`. |
| `resourceVersion` | `resource_version`  | `u32`                   | Currently always `1`; real kind versioning is future work. |
| `streamId`        | `stream_id`         | `bw://...` URI          | `bw://{node}/resources/{resource}/streams/{stream}`. |
| `stream`          | `stream`            | string                  | Last segment of `streamId`. |
| `sequence`        | `sequence`          | `u64`                   | Monotonic per `streamId`, starting at `1`. |
| `timestamp`       | `timestamp`         | RFC3339 string (serde)  | Source-of-truth timestamp. |
| `isoTimestamp`    | (derived on wire)   | RFC3339 string          | Convenience for wire consumers. |
| `payloadKind`     | `payload_kind`      | string                  | e.g. `resource.state.changed`, `resource.stream.data`. |
| `payloadVersion`  | `payload_version`   | `u32`                   | Bumped on breaking payload changes for a kind. |
| `payloadSchema`   | `payload_schema`    | `Option<string>`        | Optional schema URI. Omitted when `None`. |
| `correlationId`   | `correlation_id`    | `Option<string>`        | Reserved; omitted when `None`. |
| `causationId`     | `causation_id`      | `Option<string>`        | Reserved; omitted when `None`. |
| `traceContext`    | `trace_context`     | `Option<TraceContext>`  | W3C `traceparent`/`tracestate`. Omitted when `None`. |
| `data`            | `data`              | JSON value              | The actual payload. |

## Topic derivation

`topic` (the addressable identifier on the multiplex WS and the peer
NDJSON path) is derived once at the bus and is **not** parsed by
consumers:

```
topic = "{node}/{kind}/{resource}/{stream}"
```

`streamId` is the canonical reference for replay; `topic` is the
publish/subscribe pattern surface.

## Stream safety

A subscription has a `StreamSafety` mode. The default is `Lossless`:

- **Lossless** — when the per-subscriber bounded queue fills, the bus
  emits a `stream-gap` (see below) over an out-of-band terminal
  channel and removes the subscription. The publisher is never told to
  slow down; the slow consumer is dropped.
- **Lossy** — when the queue fills, the bus increments
  `PublishResult.dropped` and the subscription stays alive. The
  consumer may observe gaps in `sequence`.

## Overflow policy

For `Lossy` subscriptions, `OverflowPolicy` selects the dropping
strategy:

- **Backpressure** — currently equivalent to `DropNewest`. The sync
  `BusSink::publish` lift point cannot await, so backpressure is not
  expressible end-to-end yet. A future async publish path will make
  this real.
- **DropNewest** — drop the incoming envelope when the queue is full.

`Coalesce` is **intentionally deferred** and not shipped. A truthful
implementation requires a sidecar queue + iterable replace that `mpsc`
doesn't provide, and shipping a dishonest `Coalesce` that behaves
identically to `DropNewest` would lie to API consumers. The variant
will land alongside real coalescing once a stream whose expected
emission rate makes `DropNewest` lose user-meaningful signal motivates
it.

## Slow-consumer disconnect protocol

When a `Lossless` subscription overflows, two things happen at the
bus:

1. The next `try_publish` returns `Result::Ok(PublishResult)` with the
   subscription id in `disconnected_lossless`.
2. A `SlowConsumerNotice` fires on the `Subscription::slow_consumer_rx`
   oneshot, carrying the last delivered `(streamId, sequence)`.

The transport (WS forwarder, HTTP NDJSON stream) reads
`slow_consumer_rx` alongside `Subscription::rx` and emits a final
`stream-gap` over an out-of-band `terminal_tx` channel. The terminal
channel has capacity 1 and is biased-selected by the writer task so
the gap reaches the wire even when the normal outbound queue is full.

## Peer broadcast lag

The cloud-side WS forwarder reads from a `tokio::sync::broadcast`
channel that fans the hub's NDJSON stream out to many WS clients.
When that broadcast lags, the forwarder emits

```
{type:"stream-gap", reason:"broadcast_lag(<skipped>)", terminated:true, ...}
```

over the same terminal channel and signals the WS dispatcher via a
back-channel (`ForwarderEvent::LagTerminated`). The dispatcher
removes the matching `fwd_subs` entry and decrements the
`PeerStreamHub` refcount eagerly, without waiting for the client to
send `unsubscribe` or for the socket to close.

## Reverse index lifetime

`StreamRegistry` keeps a reverse `EventId -> StreamId` map so consumers
don't have to parse opaque event ids. The map is bounded by replay-cache
retention: when `StreamReplayCache` evicts an envelope from its
per-stream ring, it calls `StreamRegistry::evict(&event_id)`. `Core`,
`EventBus`, every `BusSink`, and the replay cache all share the same
`Arc<Inner>` (asserted by
`tests/event_envelope_minting.rs::bus_and_core_expose_the_same_registry_instance`).

## Limits

| Limit                                  | Default     | Override |
| -------------------------------------- | ----------- | -------- |
| Per-subscriber outbound queue capacity | 64 envelopes | `SubscribeOpts.outbound_capacity` |
| WS connection outbound capacity        | 64 messages | (internal constant) |
| WS subscriptions per connection        | 64          | (internal constant) |
| Per-stream replay ring capacity        | 1000 envelopes | `CoreBuilder::build_with_replay_capacity` (test-only) |
| Max serialized event size              | 256 KiB     | `EventBus::with_max_event_size` |

Subscribing past the per-connection cap returns
`OutboundMessage::Error { code: 429, ... }`. Publishing an envelope
larger than the cap returns `PublishError::TooLarge { limit }` from
`try_publish`.

## What's not in this slice

- Durable event history (future work).
- `Last-Event-ID` resume (future work).
- AuthN/authZ on streams (future work).

See `docs/events.md` for the wire-protocol walkthrough and
`docs/caql.md` for the topic-filter grammar that runs alongside
subscription patterns.
