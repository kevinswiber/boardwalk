use super::lex::{Spanned, Tok};
use crate::query::{ComparisonOp, FieldPath, Literal, Predicate, Projection, Query, QueryError};

struct Parser<'a> {
    toks: &'a [Spanned],
    pos: usize,
}

pub(crate) fn parse_query(toks: &[Spanned]) -> Result<Query, QueryError> {
    let mut p = Parser { toks, pos: 0 };
    let projection = if p.peek_is(&Tok::Select) {
        p.bump();
        p.parse_projection()?
    } else {
        Projection::All
    };
    let predicate = if p.peek_is(&Tok::Where) {
        p.bump();
        p.parse_or()?
    } else {
        Predicate::True
    };
    if p.pos < p.toks.len() {
        return Err(p.err("unexpected trailing tokens"));
    }
    Ok(Query {
        projection,
        predicate,
    })
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|s| &s.tok)
    }

    fn peek_is(&self, t: &Tok) -> bool {
        match (self.peek(), t) {
            (Some(a), b) => std::mem::discriminant(a) == std::mem::discriminant(b),
            _ => false,
        }
    }

    fn bump(&mut self) -> Option<&Spanned> {
        let v = self.toks.get(self.pos)?;
        self.pos += 1;
        Some(v)
    }

    fn err(&self, msg: &str) -> QueryError {
        let offset = self.toks.get(self.pos).map(|s| s.offset).unwrap_or(0);
        QueryError::Parse {
            offset,
            message: msg.into(),
        }
    }

    fn parse_projection(&mut self) -> Result<Projection, QueryError> {
        if matches!(self.peek(), Some(Tok::Star)) {
            self.bump();
            return Ok(Projection::All);
        }
        let mut paths = Vec::new();
        paths.push(self.parse_path()?);
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.bump();
            paths.push(self.parse_path()?);
        }
        Ok(Projection::Fields(paths))
    }

    fn parse_path(&mut self) -> Result<FieldPath, QueryError> {
        let mut segs = Vec::new();
        let first = match self.bump() {
            Some(s) => match &s.tok {
                Tok::Ident(id) => id.clone(),
                _ => return Err(self.err("expected identifier")),
            },
            None => return Err(self.err("expected identifier, got end of input")),
        };
        segs.push(first);
        while matches!(self.peek(), Some(Tok::Dot)) {
            self.bump();
            let next = match self.bump() {
                Some(s) => match &s.tok {
                    Tok::Ident(id) => id.clone(),
                    _ => return Err(self.err("expected identifier after `.`")),
                },
                None => return Err(self.err("expected identifier after `.`")),
            };
            segs.push(next);
        }
        Ok(FieldPath::from_segments(segs))
    }

    fn parse_or(&mut self) -> Result<Predicate, QueryError> {
        let mut lhs = self.parse_and()?;
        while matches!(self.peek(), Some(Tok::Or)) {
            self.bump();
            let rhs = self.parse_and()?;
            lhs = Predicate::Or(vec![lhs, rhs]);
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Predicate, QueryError> {
        let mut lhs = self.parse_not()?;
        while matches!(self.peek(), Some(Tok::And)) {
            self.bump();
            let rhs = self.parse_not()?;
            lhs = Predicate::And(vec![lhs, rhs]);
        }
        Ok(lhs)
    }

    fn parse_not(&mut self) -> Result<Predicate, QueryError> {
        if matches!(self.peek(), Some(Tok::Not)) {
            self.bump();
            let inner = self.parse_not()?;
            return Ok(Predicate::not(inner));
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<Predicate, QueryError> {
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.bump();
            let inner = self.parse_or()?;
            match self.bump() {
                Some(Spanned {
                    tok: Tok::RParen, ..
                }) => {}
                _ => return Err(self.err("expected `)`")),
            }
            return Ok(inner);
        }
        let left = self.parse_path()?;
        let op = match self.bump() {
            Some(s) => match &s.tok {
                Tok::Eq => ComparisonOp::Eq,
                Tok::Ne => ComparisonOp::Ne,
                Tok::Lt => ComparisonOp::Lt,
                Tok::Le => ComparisonOp::Le,
                Tok::Gt => ComparisonOp::Gt,
                Tok::Ge => ComparisonOp::Ge,
                Tok::Like => ComparisonOp::Like,
                Tok::In => ComparisonOp::InSet,
                _ => return Err(self.err("expected comparison operator")),
            },
            None => return Err(self.err("expected comparison operator")),
        };
        let right = self.parse_value()?;
        Ok(Predicate::Compare { left, op, right })
    }

    fn parse_value(&mut self) -> Result<Literal, QueryError> {
        match self.bump() {
            Some(s) => match &s.tok {
                Tok::String(v) => Ok(Literal::String(v.clone())),
                Tok::Number(n) => Ok(Literal::Number(*n)),
                Tok::True => Ok(Literal::Bool(true)),
                Tok::False => Ok(Literal::Bool(false)),
                Tok::Null => Ok(Literal::Null),
                Tok::LBracket => {
                    let mut items = Vec::new();
                    if !matches!(self.peek(), Some(Tok::RBracket)) {
                        items.push(self.parse_value()?);
                        while matches!(self.peek(), Some(Tok::Comma)) {
                            self.bump();
                            items.push(self.parse_value()?);
                        }
                    }
                    match self.bump() {
                        Some(Spanned {
                            tok: Tok::RBracket, ..
                        }) => Ok(Literal::Array(items)),
                        _ => Err(self.err("expected `]`")),
                    }
                }
                _ => Err(self.err("expected literal value")),
            },
            None => Err(self.err("expected literal value")),
        }
    }
}
