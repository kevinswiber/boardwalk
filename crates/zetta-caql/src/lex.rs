use super::CaqlError;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Tok {
    // keywords
    Select,
    Where,
    And,
    Or,
    Not,
    In,
    Like,
    True,
    False,
    Null,
    // literals
    Ident(String),
    Number(f64),
    String(String),
    // symbols
    Dot,
    Comma,
    Star,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone)]
pub(crate) struct Spanned {
    pub tok: Tok,
    pub offset: usize,
}

pub(crate) fn tokenize(src: &str) -> Result<Vec<Spanned>, CaqlError> {
    let bytes = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        match c {
            b'.' => { out.push(Spanned { tok: Tok::Dot, offset: start }); i += 1; }
            b',' => { out.push(Spanned { tok: Tok::Comma, offset: start }); i += 1; }
            b'*' => { out.push(Spanned { tok: Tok::Star, offset: start }); i += 1; }
            b'(' => { out.push(Spanned { tok: Tok::LParen, offset: start }); i += 1; }
            b')' => { out.push(Spanned { tok: Tok::RParen, offset: start }); i += 1; }
            b'[' => { out.push(Spanned { tok: Tok::LBracket, offset: start }); i += 1; }
            b']' => { out.push(Spanned { tok: Tok::RBracket, offset: start }); i += 1; }
            b'=' => { out.push(Spanned { tok: Tok::Eq, offset: start }); i += 1; }
            b'!' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    out.push(Spanned { tok: Tok::Ne, offset: start });
                    i += 2;
                } else {
                    return Err(CaqlError::Parse { offset: start, message: "expected `!=`".into() });
                }
            }
            b'<' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    out.push(Spanned { tok: Tok::Le, offset: start });
                    i += 2;
                } else {
                    out.push(Spanned { tok: Tok::Lt, offset: start });
                    i += 1;
                }
            }
            b'>' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    out.push(Spanned { tok: Tok::Ge, offset: start });
                    i += 2;
                } else {
                    out.push(Spanned { tok: Tok::Gt, offset: start });
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                let str_start = i;
                let mut s = String::new();
                while i < bytes.len() {
                    let b = bytes[i];
                    if b == b'\\' {
                        i += 1;
                        if i >= bytes.len() {
                            return Err(CaqlError::Parse {
                                offset: start,
                                message: "unterminated escape".into(),
                            });
                        }
                        match bytes[i] {
                            b'"' => s.push('"'),
                            b'\\' => s.push('\\'),
                            b'n' => s.push('\n'),
                            b't' => s.push('\t'),
                            b'r' => s.push('\r'),
                            other => s.push(other as char),
                        }
                        i += 1;
                    } else if b == b'"' {
                        i += 1;
                        out.push(Spanned { tok: Tok::String(s), offset: start });
                        break;
                    } else {
                        s.push(b as char);
                        i += 1;
                    }
                }
                if i == bytes.len() && !out.last().is_some_and(|t| matches!(t.tok, Tok::String(_) if t.offset == start)) {
                    let _ = str_start;
                    return Err(CaqlError::Parse {
                        offset: start,
                        message: "unterminated string literal".into(),
                    });
                }
            }
            c if c.is_ascii_digit() || c == b'-' || c == b'+' => {
                let mut j = i;
                if c == b'-' || c == b'+' { j += 1; }
                while j < bytes.len() && (bytes[j].is_ascii_digit() || bytes[j] == b'.') {
                    j += 1;
                }
                // Optional exponent
                if j < bytes.len() && (bytes[j] == b'e' || bytes[j] == b'E') {
                    j += 1;
                    if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') { j += 1; }
                    while j < bytes.len() && bytes[j].is_ascii_digit() { j += 1; }
                }
                let s = &src[i..j];
                let n: f64 = s.parse().map_err(|_| CaqlError::Parse {
                    offset: i,
                    message: format!("invalid number `{s}`"),
                })?;
                out.push(Spanned { tok: Tok::Number(n), offset: start });
                i = j;
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let mut j = i + 1;
                while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                    j += 1;
                }
                let ident = &src[i..j];
                let tok = match ident.to_ascii_lowercase().as_str() {
                    "select" => Tok::Select,
                    "where" => Tok::Where,
                    "and" => Tok::And,
                    "or" => Tok::Or,
                    "not" => Tok::Not,
                    "in" => Tok::In,
                    "like" => Tok::Like,
                    "true" => Tok::True,
                    "false" => Tok::False,
                    "null" => Tok::Null,
                    _ => Tok::Ident(ident.to_string()),
                };
                out.push(Spanned { tok, offset: start });
                i = j;
            }
            _ => {
                return Err(CaqlError::Parse {
                    offset: i,
                    message: format!("unexpected character `{}`", c as char),
                });
            }
        }
    }

    Ok(out)
}
