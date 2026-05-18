# Devices

Boardwalk models any addressable thing — a sensor, an LED, a SaaS
account, a worker pool — as a `Device`: a typed state machine with
named transitions, optional input fields, and optional telemetry
streams.

> **Direction:** Boardwalk is migrating toward a canonical
> `ResourceSnapshot` projection used by render, query, and (later)
> events. The `Device` trait stays in place as the stable authoring
> API for v0.1. Internally, a `DeviceSnapshot` is adapted into a
> `ResourceSnapshot` before query evaluation and rendering.
>
> Query predicates target the snapshot shape: `kind`, `state`,
> `properties.*`, `labels`, `affordances.transitions.available`,
> `affordances.streams.available`. See [caql.md](caql.md) for the
> query language. The wire `type` keyword keeps working as a
> compatibility alias for `kind`.

## The trait

```rust,ignore
pub trait Device: Send + 'static {
    fn config(&self, cfg: &mut DeviceConfig);
    fn state(&self) -> &str;

    fn transition<'a>(
        &'a mut self,
        name: &'a str,
        input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>>;

    // Optional. Called once at startup with a publish handle.
    fn on_start(&self, _ctx: DeviceCtx) {}

    // Optional. Extra serializable fields rendered as Siren properties.
    fn properties(&self) -> serde_json::Map<String, serde_json::Value> {
        Default::default()
    }
}
```

## `DeviceConfig` builder

`config(&self, cfg)` is called by the runtime to learn about the
device. The builder is chainable:

```rust,ignore
cfg.type_("thermostat")           // wire type, required
   .name("hallway")               // optional human label
   .state(self.state())           // current state
   .when("idle",     &["heat", "cool"])
   .when("heating",  &["idle"])
   .when("cooling",  &["idle"])
   .monitor("state")              // auto-publish state changes
   .monitor("temperature");       // auto-publish a property
```

- **`type_(t)`** — wire type, lowercase kebab-case by convention.
- **`name(n)`** — human label, optional.
- **`state(s)`** — current state.
- **`when(state, transitions)`** — declares which transitions are
  allowed when the device is in `state`. Anything not listed is
  rejected with `409 Conflict` (state-not-allowed).
- **`monitor(name)`** — declares a property/stream that the framework
  should auto-publish on change.

## Transitions

```rust,ignore
fn transition(&mut self, name: &str, input: TransitionInput)
    -> BoxFuture<'_, Result<(), DeviceError>>;
```

The runtime calls this with the transition name and any input fields
that came in the request body (form-urlencoded or JSON). Inputs are
typed strings — convert with `input.fields.get("...")`.

Return values:

- `Ok(())` — transition succeeded; runtime updates device state and
  publishes events.
- `Err(DeviceError::NotAllowed(_))` — wrong state. The runtime
  generally precomputes this from `when(...)`, but you can return it
  yourself if you detect a deeper state conflict.
- `Err(DeviceError::Invalid(_))` — bad input or unknown transition →
  `400 Bad Request`.
- `Err(DeviceError::Internal(_))` — `500`.

## Properties beyond state

```rust,ignore
fn properties(&self) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    m.insert("temperature".into(), serde_json::json!(self.last_temp_c));
    m.insert("set-point".into(),   serde_json::json!(self.set_point));
    m
}
```

Anything returned here is rendered as Siren properties and is
inspectable via `GET /servers/{name}/devices/{id}`. Pair with
`.monitor("temperature")` to also publish changes on the event bus.

## Streams

For values that change frequently (sensor readings, log lines) use the
`DeviceCtx` handed to `on_start`:

```rust,ignore
fn on_start(&self, ctx: DeviceCtx) {
    let publish = ctx.publish.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tick.tick().await;
            let reading = read_sensor().await;
            publish.publish("intensity", serde_json::json!(reading));
        }
    });
}
```

The published topic is `{server}/{type}/{id}/{stream}` — in this case
`hub/photocell/<uuid>/intensity`.

## Subscribing to events

The multiplex WebSocket at `/events` accepts JSON messages:

```json
{ "type": "subscribe", "topic": "hub/led/abc-123/state" }
{ "type": "subscribe", "topic": "hub/*/*/state" }        // all state events
{ "type": "subscribe", "topic": "hub/**" }               // everything from hub
{ "type": "subscribe", "topic": "hub/photocell/*/intensity?ql=where data > 80" }
```

Patterns:

- `*` — one path segment.
- `**` — zero-or-more segments (must be the last component).
- `?ql=<caql>` — optional [CaQL](caql.md) filter applied to each
  event's `data` payload. The suffix follows URL query-string
  semantics; URL-encode the value when it contains `&` or
  unsupported characters.

Responses:

- `{"type":"subscribe-ack", "subscriptionId":N, ...}` once accepted.
- `{"type":"event", "subscriptionId":N, "topic": "...", "data": ..., "timestamp": ...}`
  for each matching event.
- `{"type":"unsubscribe", "subscriptionId":N}` cancels.

## Registering with a server

```rust,ignore
Boardwalk::new()
    .name("hub")
    .use_device(Led::default())
    .use_device(Thermostat::default())
    .listen("0.0.0.0:1337".parse()?)
    .await?
```

## Hubless registration via factories

If you want a server that accepts `POST /servers/{name}/devices` to
register devices at runtime (instead of compile-time), register a
factory:

```rust,ignore
Boardwalk::new()
    .name("hub")
    .register_factory("led", |_args| Ok(Box::new(Led::default())))
    .listen(addr).await?
```

Then:

```
curl -d 'type=led&name=garage' http://hub:1337/servers/hub/devices
```

## Scouts

A `Scout` is a long-running task that registers devices at runtime —
for example, a process that walks `/dev/tty*` and instantiates a
device per discovered serial port. Implement the trait, then:

```rust,ignore
.use_scout(SerialScout::new())
```

The scout receives a `ScoutCtx` from which it calls
`ctx.discover(...)` for each device it finds.

## Persistence

```rust,ignore
Boardwalk::new()
    .name("hub")
    .persist("/var/lib/boardwalk/state.redb")
    .use_device(Led::default())
    .listen(addr).await?
```

With `.persist(path)`, device IDs become stable across restarts
(looked up by `(type, name)` identity). Peer connection history is
also persisted.
