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
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CaqlError {
    #[error("parse error at offset {offset}: {message}")]
    Parse { offset: usize, message: String },
    #[error("evaluation error: {0}")]
    Eval(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Query {
    /// `None` means `select *`.
    pub select: Option<Vec<Path>>,
    pub predicate: Option<Predicate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Path(pub Vec<String>);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Predicate {
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    Cmp(Path, Op, Value),
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    In,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    List(Vec<Value>),
}

pub fn parse(input: &str) -> Result<Query, CaqlError> {
    let toks = lex::tokenize(input)?;
    parse::parse_query(&toks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_where_eq() {
        let q = parse("where type = \"led\"").unwrap();
        assert!(q.select.is_none());
        match q.predicate.unwrap() {
            Predicate::Cmp(p, Op::Eq, Value::String(s)) => {
                assert_eq!(p.0, vec!["type".to_string()]);
                assert_eq!(s, "led");
            }
            _ => panic!("wrong predicate"),
        }
    }

    #[test]
    fn parse_and_or() {
        let q = parse("where type = \"led\" and state = \"on\" or type = \"motion\"").unwrap();
        // Should parse as (type=led AND state=on) OR type=motion
        match q.predicate.unwrap() {
            Predicate::Or(_, _) => {}
            other => panic!("expected Or at top, got {:?}", other),
        }
    }

    #[test]
    fn parse_in_list() {
        let q = parse("where type in [\"led\", \"switch\"]").unwrap();
        match q.predicate.unwrap() {
            Predicate::Cmp(_, Op::In, Value::List(xs)) => assert_eq!(xs.len(), 2),
            _ => panic!(),
        }
    }

    #[test]
    fn parse_select_paths() {
        let q = parse("select data.degreesC where data.degreesF > 85").unwrap();
        let sel = q.select.unwrap();
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].0, vec!["data", "degreesC"]);
    }

    #[test]
    fn parse_select_star_implicit() {
        let q = parse("where state = \"on\"").unwrap();
        assert!(q.select.is_none());
    }

    #[test]
    fn parse_select_star_explicit() {
        let q = parse("select * where state = \"on\"").unwrap();
        assert!(q.select.is_none());
    }

    #[test]
    fn parse_not_grouping() {
        let q = parse("where not (state = \"off\")").unwrap();
        assert!(matches!(q.predicate.unwrap(), Predicate::Not(_)));
    }

    #[test]
    fn parse_like_and_number() {
        let q = parse("where name like \"kitchen-*\" and data > 12.5").unwrap();
        assert!(q.predicate.is_some());
    }
}
