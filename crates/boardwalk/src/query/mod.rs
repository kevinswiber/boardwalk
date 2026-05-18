//! Runtime-owned query model. CaQL is one syntax adapter; this module
//! holds the canonical AST and evaluator.

mod ast;
mod eval;

pub use ast::{ComparisonOp, FieldPath, Literal, Predicate, Projection, Query, QueryError};
pub use eval::{matches, project};
