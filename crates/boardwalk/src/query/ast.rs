use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Query {
    pub projection: Projection,
    pub predicate: Predicate,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum Projection {
    #[default]
    All,
    Fields(Vec<FieldPath>),
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum Predicate {
    #[default]
    True,
    False,
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    Compare {
        left: FieldPath,
        op: ComparisonOp,
        right: Literal,
    },
    Contains {
        path: FieldPath,
        value: Literal,
    },
    Exists(FieldPath),
}

impl Predicate {
    pub fn and(items: Vec<Predicate>) -> Self {
        Self::And(items)
    }
    pub fn or(items: Vec<Predicate>) -> Self {
        Self::Or(items)
    }
    // Method name mirrors the variant. `std::ops::Not` would conflict
    // with the borrow rules we'd want for the boxed inner, so this
    // stays as a free constructor.
    #[allow(clippy::should_implement_trait)]
    pub fn not(inner: Predicate) -> Self {
        Self::Not(Box::new(inner))
    }
    pub fn contains(path: FieldPath, value: Literal) -> Self {
        Self::Contains { path, value }
    }
    pub fn exists(path: FieldPath) -> Self {
        Self::Exists(path)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    InSet,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FieldPath(Vec<String>);

impl FieldPath {
    pub fn segments(&self) -> &[String] {
        &self.0
    }

    pub fn parse(s: &str) -> Self {
        Self::try_parse(s).expect("valid field path")
    }

    pub fn try_parse(s: &str) -> Result<Self, QueryError> {
        if s.is_empty() {
            return Err(QueryError::InvalidPath("empty path".into()));
        }
        let segs: Vec<String> = s.split('.').map(str::to_string).collect();
        if segs.iter().any(String::is_empty) {
            return Err(QueryError::InvalidPath(format!("empty segment in `{s}`")));
        }
        Ok(Self(segs))
    }

    pub fn from_segments(segs: Vec<String>) -> Self {
        Self(segs)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Array(Vec<Literal>),
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("parse error at offset {offset}: {message}")]
    Parse { offset: usize, message: String },
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("unknown field: {0}")]
    Unknown(String),
    #[error("evaluation error: {0}")]
    Eval(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_default_is_select_all_no_predicate() {
        let q = Query::default();
        assert!(matches!(q.projection, Projection::All));
        assert!(matches!(q.predicate, Predicate::True));
    }

    #[test]
    fn field_path_from_dotted_string_parses_into_segments() {
        let p = FieldPath::parse("affordances.transitions.available");
        assert_eq!(
            p.segments(),
            &[
                "affordances".to_string(),
                "transitions".to_string(),
                "available".to_string()
            ]
        );
    }

    #[test]
    fn field_path_rejects_empty_segments() {
        assert!(FieldPath::try_parse("a..b").is_err());
        assert!(FieldPath::try_parse("").is_err());
    }

    #[test]
    fn literal_string_roundtrip() {
        let _s = Literal::String("hi".into());
        let _n = Literal::Number(1.5);
        let _b = Literal::Bool(true);
        let _u = Literal::Null;
        let _a = Literal::Array(vec![Literal::Number(1.0), Literal::Number(2.0)]);
    }

    #[test]
    fn predicate_and_or_not_constructors() {
        let p1 = Predicate::True;
        let p2 = Predicate::False;
        let a = Predicate::and(vec![p1.clone(), p2.clone()]);
        let o = Predicate::or(vec![p1.clone(), p2.clone()]);
        let n = Predicate::not(p1.clone());
        assert!(matches!(a, Predicate::And(_)));
        assert!(matches!(o, Predicate::Or(_)));
        assert!(matches!(n, Predicate::Not(_)));
    }

    #[test]
    fn contains_and_exists_constructors() {
        let c = Predicate::contains(FieldPath::parse("labels"), Literal::String("urgent".into()));
        assert!(matches!(c, Predicate::Contains { .. }));
        let e = Predicate::exists(FieldPath::parse("properties.owner"));
        assert!(matches!(e, Predicate::Exists(_)));
    }

    #[test]
    fn query_error_variants_have_messages() {
        let parse = QueryError::Parse {
            offset: 0,
            message: "x".into(),
        }
        .to_string();
        assert!(parse.contains("offset"));
        let path = QueryError::InvalidPath("y".into()).to_string();
        assert!(path.contains("y"));
        let unknown = QueryError::Unknown("z".into()).to_string();
        assert!(unknown.contains("z"));
    }
}
