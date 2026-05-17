//! CaQL — Calypso Query Language. v0 subset.
//!
//! Grammar in docs/05-caql.md. Parser and evaluator are stubs in this
//! milestone; tests and a real implementation land in M4.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CaqlError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("evaluation error: {0}")]
    Eval(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub select: Option<Vec<Path>>,
    pub predicate: Option<Predicate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Path(pub Vec<String>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Predicate {
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    Cmp(Path, Op, Value),
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum Op { Eq, Ne, Lt, Le, Gt, Ge, Like, In }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    List(Vec<Value>),
}

pub fn parse(_input: &str) -> Result<Query, CaqlError> {
    Err(CaqlError::Parse("not yet implemented".into()))
}

pub fn matches(_q: &Query, _target: &serde_json::Value) -> Result<bool, CaqlError> {
    Err(CaqlError::Eval("not yet implemented".into()))
}
