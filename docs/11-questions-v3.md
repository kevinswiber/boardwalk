# Open Questions (after Q11/Q16/Q17/Q18/Q20 implementation)

This is the third question doc — the prior set is largely answered. The
v0 server now does end-to-end peer-mediated control of remote devices,
both HTTP and WebSocket events. What's left are the **deferred-to-v0.1**
items from the v2 doc plus new issues that surfaced during impl.

---

### Q21. TLS integration test

`zetta-tunnel::dial_initiator` now supports `https://` and `wss://`
schemes via rustls + webpki-roots + aws-lc-rs. The codepath compiles
clean. But there's no integration test exercising it because the test
harness would need to (a) generate a self-signed cert at runtime, (b)
stand up a rustls-fronted axum listener, (c) trust the cert from the
client side.

**Default:** add a TLS integration test in v0.1 once we have the
self-signed cert helper. Manual verification (linking to a real
https-fronted Zetta) is the v0 acceptance bar.

---

### Q22. Multiple peers with the same name

If two hubs both call themselves "hub" and link to the same cloud, the
second connection overwrites the first in `PeerAcceptors.senders`.
Symptom: queries route to the most-recently-connected peer; the first
peer's connection drifts.

**Default:** treat as user error; document that peer names must be
unique within a cloud. Add a duplicate-name check in v0.1 that rejects
the second upgrade with `409 Conflict`.

---

### Q23. Per-event-stream backpressure

The cloud's WS-event forwarder reads from the hub's NDJSON body and
pushes into the WS sender's mpsc. If the WS client is slow, the mpsc
grows unbounded.

**Default:** bound the mpsc (e.g. 256 events). Drop oldest on overflow
with a warning log. Add in v0.1 polish.

---

### Q24. Cloud-side state for forwarded subscriptions

Right now each `Subscribe` from a WS client opens a fresh HTTP/2 stream
to the hub. If 100 WS clients all subscribe to `hub/led/abc/state`, we
open 100 streams on the cloud→hub tunnel.

The original Zetta deduplicates: it tracks a refcount per topic per
peer and only opens one stream, fanning to all interested clients.

**Default:** add subscription deduplication on the cloud side in v0.1.
For v0 the n²-stream behavior is acceptable.

---

### Q25. Forward WS protocol negotiation

The cloud's WS upgrade for `/events` doesn't currently negotiate a
subprotocol. Both the original Zetta JS spec and modern clients prefer
explicit negotiation (e.g. `zetta-events/1`).

**Default:** add it in M10 polish. Not urgent — clients that don't send
the token still get served.

---

### Q26. Apps support (carries over from v2 Q14)

`zetta_core::App` trait still placeholder. The `dusk_to_dawn` example
in `docs/07-api-ergonomics.md` requires real `server.observe([q1,q2],
|d1,d2| ...)` plumbing. Worth a half-day of work.

**Default:** v0.1.

---

### Q27. Scouts support (carries over from v2 Q13)

Punt. Drivers register statically via `.use_device()`.

---

### Q28. Registry persistence (carries over from v2 Q15)

`zetta_registry` exists and is tested but not wired into the running
server. Persist devices on add, restore on boot.

**Default:** v0.1.

---

### Q29. Hubless device registration (carries over from v2 Q16)

`POST /servers/{name}/devices` returns 501. Wiring a real implementation
needs:
- A way to instantiate a typed `Device` from form input (type=led, id, name).
- A device factory registry keyed by type string.

**Default:** v0.1. A reasonable shape would be `Zetta::register_factory(type_name, |args| -> Box<dyn Device>)`.

---

### Q30. Macros for driver authoring (carries over from v2 Q12)

`#[device]` / `#[transition]` macros are still nice-to-have, not
must-have.

**Default:** v0.1+.

---

### Q31. Cancellation across the tunnel

When a cloud WS client unsubscribes, we abort the local forwarding task
(its `AbortHandle`). The forwarder's HTTP/2 stream to the hub probably
needs an explicit `RST_STREAM` or the hub's end will hang waiting for
events nobody reads. The connection ought to drop cleanly, but worth
verifying with a longer-running test.

**Default:** verify and tighten in v0.1.

---

### Q32. Restart-on-graceful-disconnect

If the hub goes offline cleanly, the cloud's `drive_acceptor` ends, the
`SendRequest` is dropped, and subsequent forwarded queries return
`502 Bad Gateway`. There's no automatic re-dial from the hub side —
the cloud just goes back to "no peer". The hub's `PeerClient` does
reconnect with backoff, so the hub will come back. But is that enough?

**Default:** the current behavior is correct; reconnect is the hub's
job. Will revisit if real deployments show issues.
