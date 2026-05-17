use super::lex::{Spanned, Tok};
use super::{CaqlError, Op, Path, Predicate, Query, Value};

struct Parser<'a> {
    toks: &'a [Spanned],
    pos: usize,
}

pub(crate) fn parse_query(toks: &[Spanned]) -> Result<Query, CaqlError> {
    let mut p = Parser { toks, pos: 0 };
    let select = if p.peek_is(&Tok::Select) {
        p.bump();
        Some(p.parse_projection()?)
    } else {
        None
    };
    let select = match select {
        Some(None) => None, // `select *` is the same as no selection
        Some(Some(paths)) => Some(paths),
        None => None,
    };
    let predicate = if p.peek_is(&Tok::Where) {
        p.bump();
        Some(p.parse_or()?)
    } else {
        None
    };
    if p.pos < p.toks.len() {
        return Err(p.err("unexpected trailing tokens"));
    }
    Ok(Query { select, predicate })
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

    fn err(&self, msg: &str) -> CaqlError {
        let offset = self.toks.get(self.pos).map(|s| s.offset).unwrap_or(0);
        CaqlError::Parse {
            offset,
            message: msg.into(),
        }
    }

    /// Returns Some(None) for `*`, Some(Some(paths)) otherwise.
    fn parse_projection(&mut self) -> Result<Option<Vec<Path>>, CaqlError> {
        if matches!(self.peek(), Some(Tok::Star)) {
            self.bump();
            return Ok(None);
        }
        let mut paths = Vec::new();
        paths.push(self.parse_path()?);
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.bump();
            paths.push(self.parse_path()?);
        }
        Ok(Some(paths))
    }

    fn parse_path(&mut self) -> Result<Path, CaqlError> {
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
        Ok(Path(segs))
    }

    fn parse_or(&mut self) -> Result<Predicate, CaqlError> {
        let mut lhs = self.parse_and()?;
        while matches!(self.peek(), Some(Tok::Or)) {
            self.bump();
            let rhs = self.parse_and()?;
            lhs = Predicate::Or(Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Predicate, CaqlError> {
        let mut lhs = self.parse_not()?;
        while matches!(self.peek(), Some(Tok::And)) {
            self.bump();
            let rhs = self.parse_not()?;
            lhs = Predicate::And(Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_not(&mut self) -> Result<Predicate, CaqlError> {
        if matches!(self.peek(), Some(Tok::Not)) {
            self.bump();
            let inner = self.parse_not()?;
            return Ok(Predicate::Not(Box::new(inner)));
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<Predicate, CaqlError> {
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
        let path = self.parse_path()?;
        let op = match self.bump() {
            Some(s) => match &s.tok {
                Tok::Eq => Op::Eq,
                Tok::Ne => Op::Ne,
                Tok::Lt => Op::Lt,
                Tok::Le => Op::Le,
                Tok::Gt => Op::Gt,
                Tok::Ge => Op::Ge,
                Tok::Like => Op::Like,
                Tok::In => Op::In,
                _ => return Err(self.err("expected comparison operator")),
            },
            None => return Err(self.err("expected comparison operator")),
        };
        let value = self.parse_value()?;
        Ok(Predicate::Cmp(path, op, value))
    }

    fn parse_value(&mut self) -> Result<Value, CaqlError> {
        match self.bump() {
            Some(s) => match &s.tok {
                Tok::String(v) => Ok(Value::String(v.clone())),
                Tok::Number(n) => Ok(Value::Number(*n)),
                Tok::True => Ok(Value::Bool(true)),
                Tok::False => Ok(Value::Bool(false)),
                Tok::Null => Ok(Value::Null),
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
                        }) => Ok(Value::List(items)),
                        _ => Err(self.err("expected `]`")),
                    }
                }
                _ => Err(self.err("expected literal value")),
            },
            None => Err(self.err("expected literal value")),
        }
    }
}
