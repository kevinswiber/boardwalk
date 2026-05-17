# CaQL (Calypso Query Language)

Source: https://github.com/kevinswiber/caql

No Rust port exists. We will implement a subset in `boardwalk-caql` using
`chumsky` 1.x.

## Surface syntax (subset supported in v0)

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

### Predicate grammar (informal)

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

Identifiers are unquoted Unicode XID (start with letter, continue with
letters/digits/`_`). Strings are double-quoted with backslash escapes.
Numbers are JSON numbers. `like` is glob (`*`, `?`); `in` takes an array.

This is the union of what we observe in Zetta's wiki examples plus the
caql README, narrowed to what we actually need for device filtering and
stream filtering. We can expand later without breaking grammar.

## Examples

```
where type = "led"
where type = "thermostat" and state = "on"
where data > 85
select data.degreesC where data.degreesF > 85
where type in ["led", "switch"]
where name like "kitchen-*"
```

## AST

```rust
pub enum Predicate {
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    Cmp(Path, Op, Value),
}

pub enum Op { Eq, Ne, Lt, Le, Gt, Ge, Like, In }
pub enum Value { String(String), Number(f64), Bool(bool), Null, List(Vec<Value>) }
pub struct Path(pub Vec<String>);

pub struct Query {
    pub select: Option<Vec<Path>>, // None means *
    pub predicate: Option<Predicate>,
}
```

## Evaluator

The evaluator takes a `&serde_json::Value` (or a generic
`AsJson` trait for cheap structural inspection of `Device` state) and
returns `bool` for predicates. `like` is compiled once to a `regex::Regex`
at parse time.

## Two evaluators, two contexts

CaQL is used in two places, with different "target" objects:

1. **Device query** — `where type = "led"`. The target is a device's
   serialized properties (id, type, name, state, plus extras).
2. **Stream data filter** — `?select data.degreesC where data.degreesF > 85`.
   The target is the event payload (a single stream record).

Both share the AST and evaluator; only the projector for `select`
differs slightly (returns a new event message with reduced data).

## Notes / not-yet

- Aggregations (`count`, `sum`): not in v0.
- Joins across devices: not in v0; an app can express this with
  multiple `where` clauses orchestrated in Rust.
- Subqueries: not in v0.

If we discover the wiki has examples requiring more grammar than is
listed above, we extend before v0 ships rather than after. See Q4 in
questions.
