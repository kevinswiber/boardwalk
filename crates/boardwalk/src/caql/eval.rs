//! Compatibility shims that delegate to `crate::query::eval`. These
//! survive while callers under `http/` and `events/` are rewired to
//! call `query::matches` and `query::project` directly. Once those
//! callsites are rewired, the shims will be removed.

use serde_json::Value;

use crate::query::{self, Query, QueryError};

pub fn matches(q: &Query, v: &Value) -> Result<bool, QueryError> {
    query::matches(q, v)
}

pub fn project(q: &Query, v: &Value) -> Value {
    query::project(q, v)
}
