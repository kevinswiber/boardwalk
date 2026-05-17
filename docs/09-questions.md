# Open Questions (for review)

These are the points where the port has to take a position the original
Node implementation didn't make explicit, or where we considered two
viable approaches. None of them are blockers for getting started, but
each will lock in a meaningful default once chosen.

---

### Q1. `Device` as trait vs. value-only

The original Zetta has no static type system, so a "device" is just a
JS object with a `.init(config)` method. In Rust we can either:

- **(a) Trait-based** (recommended): user implements `impl Device for Led`
  with a `config(&self, &mut DeviceConfig)` method. Transitions are
  separate methods on the type, wired in via `cfg.map_async(...)` or via
  `#[transition]` proc macro. State stays on the struct; transitions are
  `&mut self` methods.
- **(b) Value-only**: `Device` is just a `DeviceConfig`; transitions are
  closures captured in the config. No trait. More flexible for tiny
  drivers, less ergonomic for stateful ones.

**Default:** (a). It maps cleanly to "I have a real device with real
state" and works well with proc macros. (b) is still possible — the
`map_async("turn-on", closure)` path stays as an escape hatch.

OK to lock in (a)?

---

### Q2. h2 role-reversal: bidirectional or one-way?

HTTP/2 multiplexes streams in one direction: the side that did
`client::handshake` is the only side that can *initiate* requests. The
other side can only respond. The original Zetta protocol exploits this:
the acceptor is the H2 client, so the acceptor drives queries at the
initiator.

But a peering relationship might want both directions: cloud queries
hub, hub queries cloud. The original handles this by having *both*
sides be initiators+acceptors of each other (i.e. two separate TCP
connections, two separate role-reversed tunnels).

**Options:**

- (a) **One direction per link** (matches original). If you want
  bidirectional querying, both peers `link()` to each other; you end
  up with two physical connections. Simple, predictable.
- (b) **Bidirectional over one connection** by running *both* an h2
  client and an h2 server on the same socket, multiplexed via some
  framing we invent. This is what HTTP/3 + WebTransport gives you
  natively, but in HTTP/2 there's no clean way.
- (c) **Use HTTP/2 streams "backwards" via push** — server push lets
  the H2 server pre-emptively send responses for requests it imagines
  the client *would* make. Could be coerced into being a poor-man's
  bidirectional channel, but it's fragile and h2 server push is
  deprecated in browsers (we control both sides here, but still).

**Default:** (a). It matches the original protocol semantics and is the
simplest mental model. (b) is interesting future work.

OK?

---

### Q3. Event streaming over the peer tunnel: long-body vs. server push

For the acceptor → initiator flow of streamed events
(`GET /servers/{name}/events?topic=…`), we have two implementation
choices:

- (a) **Long-lived response body** (recommended). Initiator's response
  starts with `200 OK`, keeps the body open, and writes one JSON record
  per event. Acceptor reads the body as a stream. Unsubscribe via
  `POST /events/unsubscribe` *or* RST_STREAM.
- (b) **HTTP/2 server push**, mimicking SPDY server push verbatim.

(a) is simpler, doesn't depend on a deprecated H2 feature, and matches
what most modern reverse-tunnel systems do. (b) is what the original
did. The wire format inside the body is the same either way.

**Default:** (a). Will the original Node Zetta talk to us if we do (a)?
No — but we have a separate `Sec-WebSocket-Protocol` token
(`zetta-peer/2`) to negotiate this. If interop with old Zetta is
required, we keep the door open for (b) too.

Is interop with the old Node implementation a goal? My assumption: no,
this is a clean break. Confirm?

---

### Q4. CaQL grammar boundary

The wiki references CaQL but doesn't fully spec it; the
`kevinswiber/caql` repo has the canonical grammar. My v0 subset
(see [05-caql.md](05-caql.md)) covers:

- `where` with `and`/`or`/`not`/grouping
- comparison ops: `=`, `!=`, `<`, `<=`, `>`, `>=`, `like`, `in`
- `select` projection (`*` or dotted path list)

Not covered in v0:
- aggregates
- joins / subqueries
- arithmetic
- functions (`upper()`, etc., if those exist)

Is this subset enough for actual Zetta usage you remember? Or do I need
to also ship arithmetic and functions in v0?

---

### Q5. Sub-protocol token: `zetta-peer/2` and interop

I propose negotiating `Sec-WebSocket-Protocol: zetta-peer/2` on the
peer handshake. This is purely an interop marker. Options:

- (a) Emit `zetta-peer/2`. Reject (or just ignore) `zetta-peer/1`
  (old SPDY-based). Clean break.
- (b) Accept both, dispatch SPDY for v1 and HTTP/2 for v2. Heavy.
- (c) Don't emit a token at all; assume both ends are v2.

**Default:** (a). Confirm cleaner break is wanted.

---

### Q6. State machine: per-state ALL-typed transitions, or string-keyed?

The original wires transitions by string name (`"turn-on"`). We
recommend keeping that as the wire-level identity (kebab-case in
Siren responses) but using a Rust enum for `state` so transitions
can match exhaustively against `LedState::On`/`LedState::Off`.

This is the recommendation already encoded in
[07-api-ergonomics.md](07-api-ergonomics.md). Confirm?

---

### Q7. Registry persistence default

The original ships LevelDB with a directory location of `./.devices`
and `./.peers`. We'll use `redb` (single file each). Default paths
under `./.zetta/devices.redb` and `./.zetta/peers.redb` (one parent
directory; easier to gitignore).

Acceptable?

---

### Q8. Should `zetta-server` start an embedded admin UI?

The original Node version had no built-in UI; it pointed at
`browser.zettajs.io` (hosted). We are not porting that browser.

- (a) Ship no UI in v0. Users use `curl` or a custom client.
- (b) Embed a tiny static UI (yew/leptos build, served at `/_ui`).
  More fun but a lot of work.

**Default:** (a). The terminal output + Siren JSON is the v0 surface.

---

### Q9. Crate name / publishing

- Top-level crate: `zetta` (façade).
- Sub-crates: `zetta-core`, `zetta-http`, etc.
- Existing crates.io name "zetta" — is it taken? Need to check before
  publishing. If taken, fallback names: `zetta-rs`, `zettajs`,
  `zetta-server`.

Will check before any publish, but worth noting.

---

### Q10. License

Original Zetta is Apache-2.0 (per `LICENSE`). I'll match — Apache-2.0
for everything in this workspace. Sound?

---

## Status of these questions

Nothing blocks getting started. M0 (tunnel PoC) and M1 (core types)
proceed under the **Default** answers listed above. We can revisit
each once there's running code to look at.
