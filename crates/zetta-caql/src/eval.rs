use serde_json::{Map, Value as JsonValue};

use super::{CaqlError, Op, Path, Predicate, Query, Value};

/// Test whether `target` satisfies the query's predicate. A query with
/// no predicate matches everything.
pub fn matches(q: &Query, target: &JsonValue) -> Result<bool, CaqlError> {
    match &q.predicate {
        None => Ok(true),
        Some(p) => eval_pred(p, target),
    }
}

/// Apply the query's projection. Returns a new JSON object containing
/// only the projected paths. If the query is `select *`, returns the
/// target unchanged.
pub fn project(q: &Query, target: &JsonValue) -> JsonValue {
    match &q.select {
        None => target.clone(),
        Some(paths) => {
            let mut out = Map::new();
            for p in paths {
                if let Some(v) = lookup(p, target) {
                    insert_path(&mut out, &p.0, v.clone());
                }
            }
            JsonValue::Object(out)
        }
    }
}

fn eval_pred(p: &Predicate, target: &JsonValue) -> Result<bool, CaqlError> {
    match p {
        Predicate::And(a, b) => Ok(eval_pred(a, target)? && eval_pred(b, target)?),
        Predicate::Or(a, b) => Ok(eval_pred(a, target)? || eval_pred(b, target)?),
        Predicate::Not(inner) => Ok(!eval_pred(inner, target)?),
        Predicate::Cmp(path, op, v) => {
            let got = lookup(path, target);
            Ok(compare(op, got, v))
        }
    }
}

fn lookup<'a>(path: &Path, target: &'a JsonValue) -> Option<&'a JsonValue> {
    let mut cur = target;
    for seg in &path.0 {
        match cur {
            JsonValue::Object(m) => match m.get(seg) {
                Some(v) => cur = v,
                None => return None,
            },
            _ => return None,
        }
    }
    Some(cur)
}

fn compare(op: &Op, got: Option<&JsonValue>, rhs: &Value) -> bool {
    match op {
        Op::Eq => match got {
            Some(v) => json_eq(v, rhs),
            None => matches!(rhs, Value::Null),
        },
        Op::Ne => match got {
            Some(v) => !json_eq(v, rhs),
            None => !matches!(rhs, Value::Null),
        },
        Op::Lt | Op::Le | Op::Gt | Op::Ge => match (got, rhs) {
            (Some(JsonValue::Number(n)), Value::Number(r)) => {
                let l = n.as_f64();
                match l {
                    Some(l) => match op {
                        Op::Lt => l < *r,
                        Op::Le => l <= *r,
                        Op::Gt => l > *r,
                        Op::Ge => l >= *r,
                        _ => unreachable!(),
                    },
                    None => false,
                }
            }
            (Some(JsonValue::String(s)), Value::String(r)) => match op {
                Op::Lt => s.as_str() < r.as_str(),
                Op::Le => s.as_str() <= r.as_str(),
                Op::Gt => s.as_str() > r.as_str(),
                Op::Ge => s.as_str() >= r.as_str(),
                _ => unreachable!(),
            },
            _ => false,
        },
        Op::Like => match (got, rhs) {
            (Some(JsonValue::String(s)), Value::String(pat)) => glob_match(pat, s),
            _ => false,
        },
        Op::In => match rhs {
            Value::List(items) => match got {
                Some(g) => items.iter().any(|i| json_eq(g, i)),
                None => false,
            },
            _ => false,
        },
    }
}

fn json_eq(j: &JsonValue, c: &Value) -> bool {
    match (j, c) {
        (JsonValue::String(a), Value::String(b)) => a == b,
        (JsonValue::Number(a), Value::Number(b)) => a.as_f64() == Some(*b),
        (JsonValue::Bool(a), Value::Bool(b)) => a == b,
        (JsonValue::Null, Value::Null) => true,
        _ => false,
    }
}

fn glob_match(pat: &str, s: &str) -> bool {
    // Minimal glob: `*` matches any run, `?` matches one char. No escaping.
    fn helper(p: &[char], s: &[char]) -> bool {
        match (p.first(), s.first()) {
            (None, None) => true,
            (Some('*'), _) => {
                if helper(&p[1..], s) {
                    return true;
                }
                if s.is_empty() {
                    return false;
                }
                helper(p, &s[1..])
            }
            (Some('?'), Some(_)) => helper(&p[1..], &s[1..]),
            (Some(pc), Some(sc)) if pc == sc => helper(&p[1..], &s[1..]),
            _ => false,
        }
    }
    let p: Vec<char> = pat.chars().collect();
    let s: Vec<char> = s.chars().collect();
    helper(&p, &s)
}

fn insert_path(out: &mut Map<String, JsonValue>, segs: &[String], value: JsonValue) {
    if segs.is_empty() {
        return;
    }
    if segs.len() == 1 {
        out.insert(segs[0].clone(), value);
        return;
    }
    let next = out
        .entry(segs[0].clone())
        .or_insert_with(|| JsonValue::Object(Map::new()));
    if let JsonValue::Object(m) = next {
        insert_path(m, &segs[1..], value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse as parse_caql;
    use serde_json::json;

    #[test]
    fn match_eq_and_or() {
        let q = parse_caql("where type = \"led\" or type = \"motion\"").unwrap();
        assert!(matches(&q, &json!({"type": "led"})).unwrap());
        assert!(matches(&q, &json!({"type": "motion"})).unwrap());
        assert!(!matches(&q, &json!({"type": "switch"})).unwrap());
    }

    #[test]
    fn match_comparison() {
        let q = parse_caql("where data > 85").unwrap();
        assert!(matches(&q, &json!({"data": 86})).unwrap());
        assert!(matches(&q, &json!({"data": 85.1})).unwrap());
        assert!(!matches(&q, &json!({"data": 85})).unwrap());
    }

    #[test]
    fn match_like() {
        let q = parse_caql("where name like \"kitchen-*\"").unwrap();
        assert!(matches(&q, &json!({"name": "kitchen-led"})).unwrap());
        assert!(matches(&q, &json!({"name": "kitchen-"})).unwrap());
        assert!(!matches(&q, &json!({"name": "living-led"})).unwrap());
    }

    #[test]
    fn match_in() {
        let q = parse_caql("where type in [\"led\", \"switch\"]").unwrap();
        assert!(matches(&q, &json!({"type": "led"})).unwrap());
        assert!(matches(&q, &json!({"type": "switch"})).unwrap());
        assert!(!matches(&q, &json!({"type": "motion"})).unwrap());
    }

    #[test]
    fn match_nested_path() {
        let q = parse_caql("where data.degreesF > 85").unwrap();
        assert!(matches(&q, &json!({"data": {"degreesF": 100}})).unwrap());
        assert!(!matches(&q, &json!({"data": {"degreesF": 70}})).unwrap());
        assert!(!matches(&q, &json!({"data": {"other": 99}})).unwrap());
    }

    #[test]
    fn project_single_path() {
        let q = parse_caql("select data.degreesC where data.degreesF > 85").unwrap();
        let p = project(&q, &json!({"data": {"degreesC": 30, "degreesF": 90}}));
        assert_eq!(p, json!({"data": {"degreesC": 30}}));
    }

    #[test]
    fn project_star() {
        let q = parse_caql("where state = \"on\"").unwrap();
        let p = project(&q, &json!({"state": "on", "other": 1}));
        assert_eq!(p, json!({"state": "on", "other": 1}));
    }

    #[test]
    fn missing_path_does_not_match() {
        let q = parse_caql("where missing.field = \"x\"").unwrap();
        assert!(!matches(&q, &json!({"other": 1})).unwrap());
    }
}
