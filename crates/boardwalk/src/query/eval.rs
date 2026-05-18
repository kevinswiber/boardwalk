use serde_json::{Map, Value as Json};

use super::ast::{ComparisonOp, FieldPath, Literal, Predicate, Projection, Query, QueryError};

pub fn matches(q: &Query, target: &Json) -> Result<bool, QueryError> {
    eval_pred(&q.predicate, target)
}

pub fn project(q: &Query, target: &Json) -> Json {
    match &q.projection {
        Projection::All => target.clone(),
        Projection::Fields(paths) => {
            let mut out = Map::new();
            for p in paths {
                if let Some(v) = lookup(p, target) {
                    insert_path(&mut out, p.segments(), v.clone());
                }
            }
            Json::Object(out)
        }
    }
}

fn eval_pred(p: &Predicate, t: &Json) -> Result<bool, QueryError> {
    match p {
        Predicate::True => Ok(true),
        Predicate::False => Ok(false),
        Predicate::And(items) => {
            for i in items {
                if !eval_pred(i, t)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Predicate::Or(items) => {
            for i in items {
                if eval_pred(i, t)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Predicate::Not(inner) => Ok(!eval_pred(inner, t)?),
        Predicate::Compare { left, op, right } => Ok(compare(op, lookup(left, t), right)),
        Predicate::Contains { path, value } => match lookup(path, t) {
            Some(Json::Array(items)) => Ok(items.iter().any(|i| json_eq(i, value))),
            _ => Ok(false),
        },
        Predicate::Exists(path) => Ok(lookup(path, t).is_some()),
    }
}

fn lookup<'a>(p: &FieldPath, t: &'a Json) -> Option<&'a Json> {
    let mut cur = t;
    for (i, seg) in p.segments().iter().enumerate() {
        // `type` is a compatibility alias for `kind` at the root segment
        // only. Nested paths like `properties.type` resolve normally.
        let key: &str = if i == 0 && seg == "type" { "kind" } else { seg };
        match cur {
            Json::Object(m) => match m.get(key) {
                Some(v) => cur = v,
                None => return None,
            },
            _ => return None,
        }
    }
    Some(cur)
}

fn compare(op: &ComparisonOp, got: Option<&Json>, rhs: &Literal) -> bool {
    use ComparisonOp::*;
    match op {
        Eq => match got {
            Some(v) => json_eq(v, rhs),
            None => matches!(rhs, Literal::Null),
        },
        Ne => match got {
            Some(v) => !json_eq(v, rhs),
            None => !matches!(rhs, Literal::Null),
        },
        Lt | Le | Gt | Ge => match (got, rhs) {
            (Some(Json::Number(n)), Literal::Number(r)) => match n.as_f64() {
                Some(l) => match op {
                    Lt => l < *r,
                    Le => l <= *r,
                    Gt => l > *r,
                    Ge => l >= *r,
                    _ => unreachable!(),
                },
                None => false,
            },
            (Some(Json::String(s)), Literal::String(r)) => match op {
                Lt => s.as_str() < r.as_str(),
                Le => s.as_str() <= r.as_str(),
                Gt => s.as_str() > r.as_str(),
                Ge => s.as_str() >= r.as_str(),
                _ => unreachable!(),
            },
            _ => false,
        },
        Like => match (got, rhs) {
            (Some(Json::String(s)), Literal::String(pat)) => glob_match(pat, s),
            _ => false,
        },
        InSet => match rhs {
            Literal::Array(items) => match got {
                Some(g) => items.iter().any(|i| json_eq(g, i)),
                None => false,
            },
            _ => false,
        },
    }
}

fn json_eq(j: &Json, c: &Literal) -> bool {
    match (j, c) {
        (Json::String(a), Literal::String(b)) => a == b,
        (Json::Number(a), Literal::Number(b)) => a.as_f64() == Some(*b),
        (Json::Bool(a), Literal::Bool(b)) => a == b,
        (Json::Null, Literal::Null) => true,
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

fn insert_path(out: &mut Map<String, Json>, segs: &[String], value: Json) {
    if segs.is_empty() {
        return;
    }
    if segs.len() == 1 {
        out.insert(segs[0].clone(), value);
        return;
    }
    let next = out
        .entry(segs[0].clone())
        .or_insert_with(|| Json::Object(Map::new()));
    if let Json::Object(m) = next {
        insert_path(m, &segs[1..], value);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ast::{ComparisonOp, FieldPath, Literal, Predicate, Projection, Query};
    use super::*;

    fn q_with(predicate: Predicate) -> Query {
        Query {
            projection: Projection::All,
            predicate,
        }
    }

    fn cmp(path: &str, op: ComparisonOp, right: Literal) -> Predicate {
        Predicate::Compare {
            left: FieldPath::parse(path),
            op,
            right,
        }
    }

    #[test]
    fn matches_compare_eq_on_top_level_field() {
        let q = q_with(cmp("kind", ComparisonOp::Eq, Literal::String("led".into())));
        assert!(matches(&q, &json!({"kind": "led"})).unwrap());
        assert!(!matches(&q, &json!({"kind": "motion"})).unwrap());
    }

    #[test]
    fn type_alias_matches_kind_field() {
        let q = q_with(cmp("type", ComparisonOp::Eq, Literal::String("led".into())));
        assert!(matches(&q, &json!({"kind": "led"})).unwrap());
    }

    #[test]
    fn nested_properties_type_is_not_aliased() {
        let q = q_with(cmp(
            "properties.type",
            ComparisonOp::Eq,
            Literal::String("shadow".into()),
        ));
        // Only the root segment aliases — `properties.type` must look
        // up `type` inside `properties`, not `kind`.
        assert!(
            matches(
                &q,
                &json!({"kind": "led", "properties": {"type": "shadow"}})
            )
            .unwrap()
        );
        assert!(
            !matches(
                &q,
                &json!({"kind": "shadow", "properties": {"type": "led"}})
            )
            .unwrap()
        );
    }

    #[test]
    fn matches_compare_ne_with_missing_field_is_true() {
        let q = q_with(cmp("kind", ComparisonOp::Ne, Literal::String("x".into())));
        assert!(matches(&q, &json!({})).unwrap());
    }

    #[test]
    fn matches_compare_lt_le_gt_ge_on_number() {
        let target = json!({"n": 50.0});
        assert!(
            matches(
                &q_with(cmp("n", ComparisonOp::Lt, Literal::Number(60.0))),
                &target
            )
            .unwrap()
        );
        assert!(
            matches(
                &q_with(cmp("n", ComparisonOp::Le, Literal::Number(50.0))),
                &target
            )
            .unwrap()
        );
        assert!(
            matches(
                &q_with(cmp("n", ComparisonOp::Gt, Literal::Number(40.0))),
                &target
            )
            .unwrap()
        );
        assert!(
            matches(
                &q_with(cmp("n", ComparisonOp::Ge, Literal::Number(50.0))),
                &target
            )
            .unwrap()
        );
        assert!(
            !matches(
                &q_with(cmp("n", ComparisonOp::Lt, Literal::Number(40.0))),
                &target
            )
            .unwrap()
        );
    }

    #[test]
    fn matches_compare_like_glob() {
        let q = q_with(cmp(
            "name",
            ComparisonOp::Like,
            Literal::String("kitchen-*".into()),
        ));
        assert!(matches(&q, &json!({"name": "kitchen-led"})).unwrap());
        assert!(matches(&q, &json!({"name": "kitchen-"})).unwrap());
        assert!(!matches(&q, &json!({"name": "living-led"})).unwrap());
    }

    #[test]
    fn matches_compare_in_set() {
        let q = q_with(cmp(
            "kind",
            ComparisonOp::InSet,
            Literal::Array(vec![
                Literal::String("led".into()),
                Literal::String("switch".into()),
            ]),
        ));
        assert!(matches(&q, &json!({"kind": "led"})).unwrap());
        assert!(matches(&q, &json!({"kind": "switch"})).unwrap());
        assert!(!matches(&q, &json!({"kind": "motion"})).unwrap());
    }

    #[test]
    fn matches_and_or_not_compose() {
        let on = cmp("state", ComparisonOp::Eq, Literal::String("on".into()));
        let led = cmp("kind", ComparisonOp::Eq, Literal::String("led".into()));

        let q_and = q_with(Predicate::and(vec![on.clone(), led.clone()]));
        assert!(matches(&q_and, &json!({"kind": "led", "state": "on"})).unwrap());
        assert!(!matches(&q_and, &json!({"kind": "led", "state": "off"})).unwrap());

        let q_or = q_with(Predicate::or(vec![on.clone(), led.clone()]));
        assert!(matches(&q_or, &json!({"kind": "switch", "state": "on"})).unwrap());
        assert!(matches(&q_or, &json!({"kind": "led", "state": "off"})).unwrap());
        assert!(!matches(&q_or, &json!({"kind": "switch", "state": "off"})).unwrap());

        let q_not = q_with(Predicate::not(on));
        assert!(!matches(&q_not, &json!({"state": "on"})).unwrap());
        assert!(matches(&q_not, &json!({"state": "off"})).unwrap());
    }

    #[test]
    fn matches_nested_path_exists_and_contains() {
        let target = json!({
            "affordances": { "transitions": { "available": ["turn-on"] } }
        });
        let q_exists = q_with(Predicate::exists(FieldPath::parse(
            "affordances.transitions.available",
        )));
        assert!(matches(&q_exists, &target).unwrap());

        let q_contains = q_with(Predicate::contains(
            FieldPath::parse("affordances.transitions.available"),
            Literal::String("turn-on".into()),
        ));
        assert!(matches(&q_contains, &target).unwrap());
    }

    #[test]
    fn matches_contains_on_array_returns_true_when_value_present() {
        let q = q_with(Predicate::contains(
            FieldPath::parse("labels"),
            Literal::String("urgent".into()),
        ));
        assert!(matches(&q, &json!({"labels": ["urgent", "cron"]})).unwrap());
    }

    #[test]
    fn matches_contains_on_array_returns_false_when_absent() {
        let q = q_with(Predicate::contains(
            FieldPath::parse("labels"),
            Literal::String("urgent".into()),
        ));
        assert!(!matches(&q, &json!({"labels": ["cron"]})).unwrap());
    }

    #[test]
    fn matches_contains_on_scalar_field_returns_false() {
        let q = q_with(Predicate::contains(
            FieldPath::parse("labels"),
            Literal::String("urgent".into()),
        ));
        assert!(!matches(&q, &json!({"labels": "urgent"})).unwrap());
    }

    #[test]
    fn matches_exists_returns_true_for_present_field_including_null() {
        let q = q_with(Predicate::exists(FieldPath::parse("name")));
        assert!(matches(&q, &json!({"name": null})).unwrap());
        assert!(matches(&q, &json!({"name": "led"})).unwrap());
    }

    #[test]
    fn matches_exists_returns_false_for_missing_field() {
        let q = q_with(Predicate::exists(FieldPath::parse("name")));
        assert!(!matches(&q, &json!({})).unwrap());
    }

    #[test]
    fn project_all_returns_target_unchanged() {
        let q = Query {
            projection: Projection::All,
            predicate: Predicate::True,
        };
        let t = json!({"a": 1, "b": 2});
        assert_eq!(project(&q, &t), t);
    }

    #[test]
    fn project_fields_returns_only_requested_paths() {
        let q = Query {
            projection: Projection::Fields(vec![FieldPath::parse("data.degreesC")]),
            predicate: Predicate::True,
        };
        let t = json!({"data": {"degreesC": 30, "degreesF": 90}, "other": 1});
        assert_eq!(project(&q, &t), json!({"data": {"degreesC": 30}}));
    }

    #[test]
    fn project_missing_field_is_omitted() {
        let q = Query {
            projection: Projection::Fields(vec![
                FieldPath::parse("present"),
                FieldPath::parse("missing"),
            ]),
            predicate: Predicate::True,
        };
        let t = json!({"present": 1});
        assert_eq!(project(&q, &t), json!({"present": 1}));
    }
}
