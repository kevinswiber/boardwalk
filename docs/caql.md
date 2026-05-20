# CaQL — Calypso Query Language

CaQL is the tiny query DSL Boardwalk uses for filtering and projecting
resources and event payloads. It looks like a SQL `where` clause and
keeps the same operators.

CaQL is a *syntax*. It compiles into a structured query model
(`boardwalk::query::Query`) that the runtime evaluates against JSON.

## Where it's used

1. **HTTP query filter** — `GET /resources?ql=where kind = "led"`.
   Returns a Siren search-results entity with the matching resources.
2. **Event-stream filter** — append `?ql=where data > 80` to a
   subscription topic to drop events whose payload doesn't match.
   The suffix uses URL query-string semantics: `ql=<caql>` is the
   parameter the parser reads.
3. **Runtime query** — code with a `NodeHandle` can run CaQL
   programmatically inside the process via `NodeHandle::query(ql)`.

## Surface syntax

```
where <predicate>
select <projection> where <predicate>
select <projection>
```

### Projection

```
*                       — all fields
foo, bar.baz, qux       — comma-separated field paths (dotted)
```

### Predicate grammar

```
predicate    = orExpr
orExpr       = andExpr ("or" andExpr)*
andExpr      = notExpr ("and" notExpr)*
notExpr      = ["not"] primary
primary      = "exists" path
             | path "contains" value
             | path op value
             | "(" predicate ")"
op           = "=" | "!=" | "<" | "<=" | ">" | ">=" | "like" | "in"
path         = ident ("." ident)*
value        = string | number | bool | "null" | "[" valueList "]"
```

- **Identifiers** are unquoted Unicode XID — start with a letter,
  continue with letters/digits/`_`.
- **Strings** are double-quoted with backslash escapes.
- **Numbers** are JSON numbers.
- **`like`** is glob — `*` matches zero-or-more, `?` matches one.
- **`in`** takes an array of values.
- **`contains`** tests array membership for a single literal RHS.
  `path contains x` is `true` when `path` resolves to an array
  containing a value equal to `x`. Array RHS (e.g. `contains [a, b]`)
  is rejected — use `or` to compose multiple checks for now.
- **`exists`** tests path resolution. `exists path` is `true` when
  every segment of `path` resolves, including when the final value
  is `null`.

## Resource Kinds

The canonical resource field is `kind`. CaQL does not define aliases
for resource fields.

## Resource query shape

CaQL evaluates against the canonical `ResourceSnapshot` projection:

```json
{
  "id":          "...",
  "kind":        "led",
  "name":        "...",
  "state":       "on",
  "node":        "hub",
  "properties":  { "color": "red", "brightness": 42 },
  "labels":      { "room": "kitchen" },
  "transitions": [
    {
      "name": "turn-off",
      "allowedStates": ["on"],
      "result": "sync",
      "idempotency": "none",
      "effect": "unsafe",
      "requiredScopes": [],
      "available": true,
      "unavailableReason": null
    }
  ],
  "streams": [
    { "name": "state", "kind": "object" }
  ],
  "revision":    null,
  "metadata":    { ... }
}
```

Current field paths walk JSON objects, not arrays of objects. Use CaQL
for scalar fields, nested `properties`, labels, and metadata; inspect
the `transitions` and `streams` arrays from the returned snapshot when
you need affordance details. Transition entries may also include optional
fields such as `title`, `inputSchema`, and `outputSchema` when the
resource exposes them.

```
where kind = "job"
where state = "running"
where labels.queue = "default"
where properties.color = "red"
where properties.tags contains "urgent"
where exists metadata.owner
```

For event-stream filters, the evaluator sees the event payload as-is
(`data`, `topic`, etc.) — the snapshot shape only applies to resource
queries.

## Examples

```
where kind = "led"
where kind = "thermostat" and state = "on"
where data > 85
select data.degreesC where data.degreesF > 85
where kind in ["led", "switch"]
where name like "kitchen-*"
where not (state = "off")
where properties.tags contains "urgent"
where labels.room = "kitchen"
where exists properties.owner
where not exists properties.deprecated_at
```

## Error handling

Invalid CaQL at HTTP `?ql=` returns `400 Bad Request` with an
`application/problem+json` body:

```json
{
  "error":   "query-parse-error",
  "message": "parse error at offset 13: expected literal value",
  "ql":      "where kind ="
}
```

`NodeHandle::query` returns `Result` and propagates parse and
evaluation errors as `NodeHandleError`.

## Not available yet

- Aggregations (`count`, `sum`, `avg`) — express in Rust for now.
- Joins across resources — express via multiple queries.
- Subqueries.
- `contains any` / `contains all` — use `or` / `and` over multiple
  `contains` checks.
