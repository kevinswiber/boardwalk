# CaQL — Calypso Query Language

CaQL is the tiny query DSL Boardwalk uses for filtering and projecting
devices and event payloads. It looks like a SQL `where` clause and
keeps the same operators.

## Where it's used

1. **HTTP query filter** — `GET /servers/<name>?ql=where type = "led"`.
   Returns a Siren search-results entity with the matching devices.
2. **Event-stream filter** — append `?ql=where data > 80` to a
   subscription topic to drop events whose `data` payload doesn't
   match.
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
notExpr      = ["not"] cmpExpr
cmpExpr      = path op value
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

## Examples

```
where type = "led"
where type = "thermostat" and state = "on"
where data > 85
select data.degreesC where data.degreesF > 85
where type in ["led", "switch"]
where name like "kitchen-*"
where not (state = "off")
```

## Two evaluators, one grammar

CaQL evaluates against `serde_json::Value`. Boardwalk uses two target
shapes:

- **Device target** — `{ "id": ..., "type": ..., "name": ..., "state": ..., ... }`,
  built from each device's snapshot.
- **Event target** — the event payload's `data` field, evaluated per
  event for `?ql=...` filters on subscriptions.

Both share the AST and the evaluator; only the projection differs
slightly (events return a reduced data shape).

## Not in v0.1

- Aggregations (`count`, `sum`, `avg`) — express in Rust for now.
- Joins across devices — express via multiple queries.
- Subqueries.
