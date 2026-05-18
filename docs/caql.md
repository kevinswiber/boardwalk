# CaQL — Calypso Query Language

CaQL is the tiny query DSL Boardwalk uses for filtering and projecting
resources and event payloads. It looks like a SQL `where` clause and
keeps the same operators.

CaQL is a *syntax*. It compiles into a structured query model
(`boardwalk::query::Query`) that the runtime evaluates against JSON.

## Where it's used

1. **HTTP query filter** — `GET /servers/<name>?ql=where kind = "led"`.
   Returns a Siren search-results entity with the matching resources.
2. **Event-stream filter** — append `?ql=where data > 80` to a
   subscription topic to drop events whose payload doesn't match.
3. **`App::query` / `ScoutCtx`** — apps and scouts can run CaQL
   programmatically inside the process via `ServerHandle::query(ql)`.

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

## `kind` vs `type`

The canonical resource field is `kind`. The query evaluator accepts
`type` as a compatibility alias at the top level: `where type = "led"`
is equivalent to `where kind = "led"`. The alias applies only at the
root segment — `where properties.type = "X"` continues to look up the
literal `type` key inside `properties`, so resources that carry their
own user-defined `type` property are not aliased.

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
  "labels":      ["kitchen"],
  "affordances": {
    "transitions": { "available": ["turn-off"] },
    "streams":     { "available": ["state"] }
  },
  "metadata":    { ... }
}
```

So predicates can match into the affordances tree:

```
where affordances.transitions.available contains "cancel"
where exists properties.owner
where properties.color = "red"
```

For event-stream filters, the evaluator sees the event payload as-is
(`data`, `topic`, etc.) — the snapshot shape only applies to resource
queries.

## Examples

```
where kind = "led"
where type = "led"                          -- alias for kind = "led"
where kind = "thermostat" and state = "on"
where data > 85
select data.degreesC where data.degreesF > 85
where kind in ["led", "switch"]
where name like "kitchen-*"
where not (state = "off")
where labels contains "urgent"
where exists properties.owner
where not exists properties.deprecated_at
where affordances.transitions.available contains "cancel"
```

## Error handling

Invalid CaQL at HTTP `?ql=` returns `400 Bad Request` with an
`application/problem+json` body:

```json
{
  "error":   "query-parse-error",
  "message": "parse error at offset 13: expected literal value",
  "ql":      "where type ="
}
```

`ServerHandle::query`, `observe`, and `observe_loop` return `Result`
and propagate parse and evaluation errors as `AppError`.

## Not in v0.1

- Aggregations (`count`, `sum`, `avg`) — express in Rust for now.
- Joins across resources — express via multiple queries.
- Subqueries.
- `contains any` / `contains all` — use `or` / `and` over multiple
  `contains` checks.
