# Resources and actors

Boardwalk's current public model is `Resource` / `Actor` / `Node`.
A `Resource` is anything addressable on a node. An `Actor` is a
resource that owns state and accepts transitions. A `Node` is the
runtime boundary that registers resources, serializes actor commands,
and shares one event bus with the resource directory.

## HTTP routes

The current wire vocabulary uses resource routes:

```
GET  /resources
GET  /resources/{id}
POST /resources/{id}/transitions/{transition}
```

The reusable Boardwalk router serves those routes from a `Node` runtime.
Actors registered with `Boardwalk::new().use_actor(...)` are exposed
through the local resource routes. Code that builds a `Node` directly
with `NodeBuilder` keeps owning that node and can wrap it with custom
HTTP when needed; the `examples/job-runner` package uses the reusable
builder so the example exercises the same route stack.

Peer-forwarded routes mirror the same vocabulary under a server name:

```
GET  /servers/{name}/resources
GET  /servers/{name}/resources/{id}
POST /servers/{name}/resources/{id}/transitions/{transition}
```

`GET /resources?ql=<caql>` filters the collection with CaQL. The
parameter name is part of the wire contract; use `?ql=` and URL-encode
the value when it contains `&` or other query-string separators.
Clients should follow Siren link relations and action metadata where
available instead of treating path strings as the protocol boundary.

## Resource snapshots

`ResourceSnapshot` is the render and query target. Its top-level fields
are reserved for Boardwalk-owned data, and resource-specific data lives
under `properties`.

```json
{
  "id": "job-1",
  "kind": "job",
  "name": "import-photos",
  "state": "running",
  "node": "runner",
  "properties": { "progress": 42 },
  "labels": { "queue": "default" },
  "transitions": [],
  "streams": [{ "name": "progress", "kind": "object" }],
  "revision": null,
  "metadata": {}
}
```

CaQL predicates evaluate against this shape. Current field paths walk
JSON objects, so useful query paths are scalar/object paths such as
`kind`, `state`, `properties.progress`, `labels.queue`, and
`metadata.owner`. Arrays such as `transitions` and `streams` are
present in the snapshot, but the query engine does not yet traverse
arrays of objects.

## Authoring model

Implement `Resource` when something is read-only or when transition
execution is not part of its contract. Implement `Actor` when the
resource owns state and accepts transitions.

```rust,ignore
use boardwalk::{ResourceSnapshot, ResourceSpec, TransitionInput, TransitionOutcome};
use boardwalk::runtime::{
    Actor, DynFuture, Resource, ResourceCtx, ResourceError, TransitionCtx, TransitionError,
};

struct Led {
    on: bool,
}

impl Resource for Led {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            ..Default::default()
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move {
            // Build the current ResourceSnapshot here.
            todo!()
        })
    }
}

impl Actor for Led {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            // Mutate state, publish events with ctx.publish(...), and
            // return TransitionOutcome::Completed or ::Accepted.
            let _ = (ctx, name, input);
            todo!()
        })
    }
}
```

`TransitionOutcome::Completed` returns the updated snapshot for
synchronous transitions. `TransitionOutcome::Accepted` returns a typed
job handle when transition work continues through another resource.

## Node runtime

`NodeBuilder` builds a named node. The node owns the resource directory,
the event bus, the shared stream registry, and per-actor command queues.

```rust,ignore
use std::sync::Arc;

use boardwalk::runtime::{NodeBuilder, NodeHandle};

let node = Arc::new(NodeBuilder::new("hub").build());
let id = node.register_actor(Led { on: false }).await?;
let handle = NodeHandle::new(node.clone());
let leds = handle.query("where kind = \"led\"").await?;
let led = leds.into_iter().find(|resource| resource.id() == id).unwrap();
let snapshot = led.snapshot().await?;
```

## Events

Actors publish explicitly. `TransitionCtx::publish` attaches the
transition command id as `causationId`, copies request correlation from
`x-request-id` into `correlationId`, and carries W3C trace context when
present. `ActorCtx::publish` is available for lifecycle emissions; those
do not have an inbound request and therefore omit correlation and
causation fields.

Slow consumers are controlled with `SlowConsumerPolicy`: `Disconnect`,
`Backpressure`, `DropNewest`, or `Coalesce { key_path }`.

## Job runner

The `examples/job-runner` package is the current async-transition
example. It models a `JobQueue` actor and `Job` resources, returns
`TransitionOutcome::Accepted` from submit-style transitions, publishes
progress/log/lifecycle streams explicitly, and uses
`SlowConsumerPolicy::Coalesce` for progress updates.

That package serves through `Boardwalk::new().use_actor_with_id(...)`.
Its typed job handles point at the reusable NDJSON event route with
replay enabled so late subscribers can catch up on progress events.
