//! CaQL — Calypso Query Language.
//!
//! CaQL is a textual syntax that compiles into [`crate::query::Query`];
//! it does not own its own AST or evaluator. Use [`crate::query::matches`]
//! and [`crate::query::project`] to evaluate parsed queries.
//!
//! Grammar (informal):
//!
//! ```text
//! query     = ["select" projection] ["where" predicate]
//! projection= "*" | path ("," path)*
//! path      = ident ("." ident)*
//! predicate = orExpr
//! orExpr    = andExpr ("or" andExpr)*
//! andExpr   = notExpr ("and" notExpr)*
//! notExpr   = ["not"] primary
//! primary   = "exists" path
//!           | path "contains" value
//!           | path op value
//!           | "(" predicate ")"
//! op        = "=" | "!=" | "<" | "<=" | ">" | ">=" | "like" | "in"
//! value     = string | number | bool | "null" | "[" (value ("," value)*)? "]"
//! ```
//!
//! - `like` is glob (`*` and `?`).
//! - `in` requires an array value.
//! - `contains` tests array membership for a single scalar literal;
//!   `path contains v` is true when `path` resolves to an array and
//!   any element equals `v`. Array RHS is rejected.
//! - `exists` tests path resolution; `exists p` is true when every
//!   segment of `p` resolves, including when the final value is
//!   `null`.
//!
//! The evaluator treats `type` as a root-segment alias for `kind` so
//! `where type = "led"` keeps working under the canonical
//! `ResourceSnapshot` projection. See [`docs/caql.md`] in the repo
//! for the full user-facing reference.

#![forbid(unsafe_code)]

mod lex;
mod parse;

use crate::query::{Query, QueryError};

/// Backwards-compatible alias. New code should refer to
/// [`crate::query::QueryError`].
pub type CaqlError = QueryError;

pub fn parse(input: &str) -> Result<Query, QueryError> {
    let toks = lex::tokenize(input)?;
    parse::parse_query(&toks)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::query::{self, ComparisonOp, FieldPath, Literal, Predicate, Projection};

    #[test]
    fn caql_parse_returns_query_module_type() {
        let q: query::Query = parse(r#"where type = "led""#).unwrap();
        assert!(matches!(q.predicate, Predicate::Compare { .. }));
    }

    #[test]
    fn caql_select_star_becomes_projection_all() {
        let q = parse(r#"select * where state = "on""#).unwrap();
        assert!(matches!(q.projection, Projection::All));
    }

    #[test]
    fn caql_select_paths_becomes_projection_fields() {
        let q = parse("select a.b, c where state = \"on\"").unwrap();
        match q.projection {
            Projection::Fields(paths) => {
                assert_eq!(paths.len(), 2);
                assert_eq!(paths[0].segments(), &["a".to_string(), "b".to_string()]);
                assert_eq!(paths[1].segments(), &["c".to_string()]);
            }
            other => panic!("expected Fields, got {other:?}"),
        }
    }

    #[test]
    fn caql_in_becomes_comparison_in_set() {
        let q = parse("where x in [1, 2]").unwrap();
        match q.predicate {
            Predicate::Compare { op, right, .. } => {
                assert_eq!(op, ComparisonOp::InSet);
                match right {
                    Literal::Array(items) => assert_eq!(items.len(), 2),
                    other => panic!("expected Literal::Array, got {other:?}"),
                }
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    #[test]
    fn caql_existing_grammar_round_trips() {
        let q = parse(r#"where a = 1 and b > 2 or not (c like "x*")"#).unwrap();
        match q.predicate {
            Predicate::Or(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], Predicate::And(_)));
                assert!(matches!(items[1], Predicate::Not(_)));
            }
            other => panic!("expected Or at top, got {other:?}"),
        }
    }

    #[test]
    fn caql_parse_returns_query_error_not_caql_error() {
        let err = parse("where type =").unwrap_err();
        let _err: query::QueryError = err;
    }

    #[test]
    fn caql_error_alias_is_query_error() {
        // Backwards-compat: `caql::CaqlError` resolves to `query::QueryError`.
        let _: CaqlError = QueryError::InvalidPath("x".into());
    }

    // ---- Parse + evaluate round trips ----

    #[test]
    fn caql_parse_then_eval_eq_and_or() {
        let q = parse(r#"where type = "led" or type = "motion""#).unwrap();
        assert!(query::matches(&q, &json!({"kind": "led"})).unwrap());
        assert!(query::matches(&q, &json!({"kind": "motion"})).unwrap());
        assert!(!query::matches(&q, &json!({"kind": "switch"})).unwrap());
    }

    #[test]
    fn caql_parse_then_eval_like() {
        let q = parse(r#"where name like "kitchen-*""#).unwrap();
        assert!(query::matches(&q, &json!({"name": "kitchen-led"})).unwrap());
        assert!(query::matches(&q, &json!({"name": "kitchen-"})).unwrap());
        assert!(!query::matches(&q, &json!({"name": "living-led"})).unwrap());
    }

    #[test]
    fn caql_parse_then_eval_in() {
        let q = parse(r#"where kind in ["led", "switch"]"#).unwrap();
        assert!(query::matches(&q, &json!({"kind": "led"})).unwrap());
        assert!(query::matches(&q, &json!({"kind": "switch"})).unwrap());
        assert!(!query::matches(&q, &json!({"kind": "motion"})).unwrap());
    }

    #[test]
    fn caql_parse_then_eval_nested_path() {
        let q = parse("where data.degreesF > 85").unwrap();
        assert!(query::matches(&q, &json!({"data": {"degreesF": 100}})).unwrap());
        assert!(!query::matches(&q, &json!({"data": {"degreesF": 70}})).unwrap());
    }

    #[test]
    fn caql_parse_then_eval_missing_path() {
        let q = parse(r#"where missing.field = "x""#).unwrap();
        assert!(!query::matches(&q, &json!({"other": 1})).unwrap());
    }

    #[test]
    fn caql_parse_then_project_single_path() {
        let q = parse("select data.degreesC where data.degreesF > 85").unwrap();
        let v = query::project(&q, &json!({"data": {"degreesC": 30, "degreesF": 90}}));
        assert_eq!(v, json!({"data": {"degreesC": 30}}));
    }

    #[test]
    fn caql_parse_then_project_star() {
        let q = parse(r#"where state = "on""#).unwrap();
        let v = query::project(&q, &json!({"state": "on", "other": 1}));
        assert_eq!(v, json!({"state": "on", "other": 1}));
    }

    #[test]
    fn caql_like_and_number_still_parse() {
        let q = parse(r#"where name like "kitchen-*" and data > 12.5"#).unwrap();
        assert!(matches!(q.predicate, Predicate::And(_)));
    }

    #[test]
    fn caql_not_grouping_parses_to_predicate_not() {
        let q = parse(r#"where not (state = "off")"#).unwrap();
        assert!(matches!(q.predicate, Predicate::Not(_)));
    }

    #[test]
    fn caql_field_path_segments_match_dotted_input() {
        let q = parse("select data.degreesC where data.degreesF > 85").unwrap();
        match q.projection {
            Projection::Fields(paths) => {
                assert_eq!(
                    paths[0].segments(),
                    &["data".to_string(), "degreesC".to_string()]
                );
            }
            other => panic!("expected Fields, got {other:?}"),
        }
    }

    #[test]
    fn caql_null_literal_parses() {
        let q = parse("where state = null").unwrap();
        match q.predicate {
            Predicate::Compare {
                right: Literal::Null,
                ..
            } => {}
            other => panic!("expected Compare with Literal::Null, got {other:?}"),
        }
    }

    #[test]
    fn caql_bool_literals_parse() {
        let q_t = parse("where on = true").unwrap();
        assert!(matches!(
            q_t.predicate,
            Predicate::Compare {
                right: Literal::Bool(true),
                ..
            }
        ));
        let q_f = parse("where on = false").unwrap();
        assert!(matches!(
            q_f.predicate,
            Predicate::Compare {
                right: Literal::Bool(false),
                ..
            }
        ));
    }

    // ---- contains / exists grammar ----

    #[test]
    fn caql_contains_string_literal_against_array_field() {
        let q = parse(r#"where labels contains "urgent""#).unwrap();
        match q.predicate {
            Predicate::Contains { path, value } => {
                assert_eq!(path.segments(), &["labels".to_string()]);
                assert_eq!(value, Literal::String("urgent".into()));
            }
            other => panic!("expected Contains, got {other:?}"),
        }
    }

    #[test]
    fn caql_contains_number_against_array_field() {
        let q = parse("where seats contains 4").unwrap();
        match q.predicate {
            Predicate::Contains {
                value: Literal::Number(n),
                ..
            } => assert_eq!(n, 4.0),
            other => panic!("expected Contains with number, got {other:?}"),
        }
    }

    #[test]
    fn caql_exists_top_level_path() {
        let q = parse("where exists name").unwrap();
        match q.predicate {
            Predicate::Exists(path) => assert_eq!(path.segments(), &["name".to_string()]),
            other => panic!("expected Exists, got {other:?}"),
        }
    }

    #[test]
    fn caql_exists_nested_path() {
        let q = parse("where exists properties.owner").unwrap();
        match q.predicate {
            Predicate::Exists(path) => assert_eq!(
                path.segments(),
                &["properties".to_string(), "owner".to_string()]
            ),
            other => panic!("expected Exists, got {other:?}"),
        }
    }

    #[test]
    fn caql_exists_inside_boolean_tree() {
        let q = parse(r#"where exists labels and kind = "job""#).unwrap();
        match q.predicate {
            Predicate::And(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], Predicate::Exists(_)));
                assert!(matches!(items[1], Predicate::Compare { .. }));
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn caql_not_exists_via_not_keyword() {
        let q = parse("where not exists properties.owner").unwrap();
        match q.predicate {
            Predicate::Not(inner) => {
                assert!(matches!(*inner, Predicate::Exists(_)));
            }
            other => panic!("expected Not(Exists(_)), got {other:?}"),
        }
    }

    #[test]
    fn caql_contains_evaluates_correctly_via_query_eval() {
        let q = parse(r#"where labels contains "urgent""#).unwrap();
        assert!(query::matches(&q, &json!({"labels": ["a", "urgent"]})).unwrap());
        assert!(!query::matches(&q, &json!({"labels": ["a"]})).unwrap());
    }

    #[test]
    fn caql_exists_evaluates_against_present_field_including_null() {
        let q = parse("where exists name").unwrap();
        assert!(query::matches(&q, &json!({"name": null})).unwrap());
        assert!(query::matches(&q, &json!({"name": "led"})).unwrap());
        assert!(!query::matches(&q, &json!({})).unwrap());
    }

    #[test]
    fn caql_contains_with_array_rhs_is_an_error() {
        // `contains` rhs must be a single literal.
        assert!(parse(r#"where labels contains ["a", "b"]"#).is_err());
    }

    #[allow(dead_code)]
    fn _ensure_field_path_constructor() -> FieldPath {
        FieldPath::from_segments(vec!["a".into()])
    }
}
