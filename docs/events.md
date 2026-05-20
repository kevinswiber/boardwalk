# Events wire protocol

Boardwalk multiplexes event subscriptions over a single WebSocket
connection at `/events`, and exposes the same stream over NDJSON at
`/servers/{name}/events?topic=...`. This document covers the wire
shapes. For envelope-field semantics see
[`event-envelope.md`](./event-envelope.md).

## WebSocket protocol (multiplex)

Each frame is a UTF-8 text frame containing JSON. The top-level
discriminator is the `type` field.

### Subscribe

```json
{"type":"subscribe","topic":"hub/led/<id>/state"}
```

Optional fields:

- `limit` (integer) — auto-unsubscribe after N events.
- `outboundCapacity` (integer) — per-subscriber bounded queue size on
  the bus; default 64.

### Subscribe ack

```json
{"type":"subscribe-ack","timestamp":1715990000000,"topic":"...","subscriptionId":1}
```

### Event

```json
{
  "type": "event",
  "topic": "hub/led/<id>/state",
  "subscriptionId": 1,
  "timestamp": 1715990000123,
  "data": "on",

  "eventId": "bw://hub/resources/<id>/streams/state-1",
  "streamId": "bw://hub/resources/<id>/streams/state",
  "sequence": 1,
  "nodeId": "hub",
  "resourceId": "<id>",
  "resourceKind": "led",
  "payloadKind": "resource.state.changed",
  "payloadVersion": 1,
  "envelopeVersion": 1,
  "isoTimestamp": "2026-05-18T17:00:00.123Z",
  "correlationId": "req-123",
  "causationId": "0f4c967c-9d36-4b38-8f97-cf5799df5c19"
}
```

The first five fields are the subscription frame. The bottom block is
the event envelope mirror, and optional envelope fields are omitted when
their values are absent.

Events emitted from an actor transition are populated from the
transition context. `correlationId` copies the inbound `x-request-id`
when one exists, and `causationId` is the command id minted for that
transition invocation. Lifecycle emissions published through
`ActorCtx::publish` do not have an inbound request, so they omit both
fields unless a future caller supplies that context explicitly.

### Stream gap

Emitted when a subscription is terminated by the runtime (slow
consumer disconnect, peer broadcast lag, oversized event, etc.):

```json
{
  "type": "stream-gap",
  "timestamp": 1715990000456,
  "subscriptionId": 1,
  "streamId": "bw://hub/resources/<id>/streams/state",
  "lastDeliveredSequence": 3,
  "reason": "slow_consumer",
  "terminated": true
}
```

Reason values today:

- `slow_consumer` — the per-subscriber bounded queue filled while the
  subscription's slow-consumer policy was `Disconnect`.
- `broadcast_lag(<n>)` — the cloud-side peer-broadcast channel
  evicted `n` events from this subscription's receiver.

`terminated: true` means the runtime has dropped this subscription.
Reconnecting (re-subscribing) is the only resume path in this slice;
a future slice will support `Last-Event-ID` resume.

### Unsubscribe / Unsubscribe-ack

```json
{"type":"unsubscribe","subscriptionId":1}
{"type":"unsubscribe-ack","timestamp":...,"subscriptionId":1}
```

### Ping / Pong

```json
{"type":"ping","data":"keepalive-7"}
{"type":"pong","timestamp":...,"data":"keepalive-7"}
```

### Error

```json
{
  "type": "error",
  "code": 400,
  "timestamp": 1715990000000,
  "topic": "...",
  "message": "..."
}
```

Codes:

- `400` — malformed input (bad JSON, invalid topic).
- `429` — connection has reached its per-connection subscription cap.
- `502` — upstream peer stream closed unexpectedly.

## NDJSON protocol (peer-forwarded)

`GET /servers/{name}/events?topic=...` returns
`application/x-ndjson` — each line is one event JSON object. The
line shape mirrors the WS `event` payload but without the
`type`/`subscriptionId` fields:

```
{"topic":"...","timestamp":...,"data":...,"eventId":"...","streamId":"...","sequence":1,"nodeId":"...","resourceId":"...","resourceKind":"...","payloadKind":"...","payloadVersion":1,"envelopeVersion":1,"isoTimestamp":"...","correlationId":"req-123","causationId":"..."}
```

The response body's lifetime is tied to the underlying subscription:
when the client disconnects, the runtime calls
`EventBus::unsubscribe` immediately (no longer deferred until next
publish).

## Envelope reference

See [`event-envelope.md`](./event-envelope.md) for the canonical
envelope shape and for the slow-consumer / overflow-policy semantics.
