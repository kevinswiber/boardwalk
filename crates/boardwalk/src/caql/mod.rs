//! CaQL — Calypso Query Language.
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
//! notExpr   = ["not"] cmpExpr
//! cmpExpr   = path op value | "(" predicate ")"
//! op        = "=" | "!=" | "<" | "<=" | ">" | ">=" | "like" | "in"
//! value     = string | number | bool | "null" | "[" (value ("," value)*)? "]"
//! ```
//!
//! `like` is glob ( `*` and `?` ). `in` requires a list value.

#![forbid(unsafe_code)]

mod eval;
mod lex;
mod parse;

pub use eval::{matches, project};

use crate::query::{Query, QueryError};

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
        // Top-level should be Or.
        match q.predicate {
            Predicate::Or(items) => {
                assert_eq!(items.len(), 2);
                // First arm is And.
                assert!(matches!(items[0], Predicate::And(_)));
                // Second arm is Not.
                assert!(matches!(items[1], Predicate::Not(_)));
            }
            other => panic!("expected Or at top, got {other:?}"),
        }
    }

    #[test]
    fn caql_parse_returns_query_error_not_caql_error() {
        let err = parse("where type =").unwrap_err();
        // The error must be a query::QueryError (the assignment below
        // would fail to compile if it were caql::CaqlError).
        let _err: query::QueryError = err;
    }

    #[test]
    fn caql_matches_shim_accepts_query_module_type() {
        let q = parse(r#"where type = "led""#).unwrap();
        let v = json!({"type": "led"});
        assert_eq!(matches(&q, &v).unwrap(), query::matches(&q, &v).unwrap());
    }

    #[test]
    fn caql_project_shim_accepts_query_module_type() {
        let q = parse(r#"select data.x where data.x = 1"#).unwrap();
        let v = json!({"data": {"x": 1, "y": 2}});
        assert_eq!(project(&q, &v), query::project(&q, &v));
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
        match q_t.predicate {
            Predicate::Compare {
                right: Literal::Bool(true),
                ..
            } => {}
            other => panic!("expected Compare with Literal::Bool(true), got {other:?}"),
        }
        let q_f = parse("where on = false").unwrap();
        match q_f.predicate {
            Predicate::Compare {
                right: Literal::Bool(false),
                ..
            } => {}
            other => panic!("expected Compare with Literal::Bool(false), got {other:?}"),
        }
    }

    // Compile-time check that field paths from FieldPath are usable here.
    #[allow(dead_code)]
    fn _ensure_field_path_constructor() -> FieldPath {
        FieldPath::from_segments(vec!["a".into()])
    }
}
