//! Pins the public API surface of the `boardwalk::query` module.
//!
//! If any of these names move or become private, this test fails to
//! compile loudly.

#[test]
fn query_types_are_reachable_via_boardwalk_query() {
    let _q = boardwalk::query::Query::default();
    let _p = boardwalk::query::Predicate::True;
    let _e = boardwalk::query::QueryError::InvalidPath("x".into());
    let _path = boardwalk::query::FieldPath::parse("a.b");
    let _lit = boardwalk::query::Literal::Bool(true);
    let _op = boardwalk::query::ComparisonOp::Eq;
    let _proj = boardwalk::query::Projection::All;
}
