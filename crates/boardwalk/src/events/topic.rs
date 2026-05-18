use thiserror::Error;

use crate::query::{Query, QueryError};

#[derive(Debug, Error)]
pub enum TopicParseError {
    #[error("empty topic")]
    Empty,
    #[error("invalid regex segment at index {index}: {source}")]
    BadRegex {
        index: usize,
        #[source]
        source: regex::Error,
    },
    #[error("caql filter: {0}")]
    Caql(#[from] QueryError),
}

#[derive(Debug, Clone)]
pub enum Segment {
    Literal(String),
    /// `*` matches one segment.
    SingleWild,
    /// `**` matches one or more segments.
    MultiWild,
    /// `{regex}` matches one segment via regex.
    Regex(regex::Regex),
}

/// A parsed topic. May include a trailing `?ql=...` CaQL filter that
/// applies to event payloads.
#[derive(Debug, Clone)]
pub struct TopicPattern {
    pub segments: Vec<Segment>,
    pub filter: Option<Query>,
    /// The original string the pattern was parsed from. Useful for
    /// echoing back in subscribe-ack messages.
    pub raw: String,
}

impl TopicPattern {
    /// Parse a topic. Examples:
    /// - `hub/led/abc/state`
    /// - `hub/*/abc/state`
    /// - `hub/**/state`
    /// - `{^Det.+$}/thermostat/abc/temperature`
    /// - `hub/thermostat/*/temperature?where data > 85`
    pub fn parse(input: &str) -> Result<Self, TopicParseError> {
        if input.is_empty() {
            return Err(TopicParseError::Empty);
        }
        let raw = input.to_string();

        // Split off optional `?caql` suffix.
        let (path, filter) = match input.find('?') {
            Some(i) => {
                let q = crate::caql::parse(&input[i + 1..])?;
                (&input[..i], Some(q))
            }
            None => (input, None),
        };

        let mut segments = Vec::new();
        for (idx, seg) in path.split('/').enumerate() {
            if seg == "*" {
                segments.push(Segment::SingleWild);
            } else if seg == "**" {
                segments.push(Segment::MultiWild);
            } else if seg.starts_with('{') && seg.ends_with('}') && seg.len() >= 2 {
                let inner = &seg[1..seg.len() - 1];
                let re = regex::Regex::new(inner).map_err(|e| TopicParseError::BadRegex {
                    index: idx,
                    source: e,
                })?;
                segments.push(Segment::Regex(re));
            } else {
                segments.push(Segment::Literal(seg.to_string()));
            }
        }
        Ok(Self {
            segments,
            filter,
            raw,
        })
    }

    /// Test whether this pattern matches a concrete topic string.
    pub fn matches_topic(&self, topic: &str) -> bool {
        let parts: Vec<&str> = topic.split('/').collect();
        matches_at(&self.segments, &parts)
    }

    /// Test whether this pattern matches a topic AND its payload (if a
    /// CaQL filter is attached).
    pub fn matches_event(&self, topic: &str, payload: &serde_json::Value) -> bool {
        if !self.matches_topic(topic) {
            return false;
        }
        match &self.filter {
            None => true,
            Some(q) => crate::caql::matches(q, payload).unwrap_or(false),
        }
    }
}

fn matches_at(pat: &[Segment], parts: &[&str]) -> bool {
    match pat.first() {
        None => parts.is_empty(),
        Some(Segment::MultiWild) => {
            if pat.len() == 1 {
                // trailing `**` — must match >=1 segment.
                return !parts.is_empty();
            }
            // Try consuming 1..=len segments.
            for consume in 1..=parts.len() {
                if matches_at(&pat[1..], &parts[consume..]) {
                    return true;
                }
            }
            false
        }
        Some(_) if parts.is_empty() => false,
        Some(Segment::SingleWild) => matches_at(&pat[1..], &parts[1..]),
        Some(Segment::Literal(s)) => parts[0] == s && matches_at(&pat[1..], &parts[1..]),
        Some(Segment::Regex(re)) => re.is_match(parts[0]) && matches_at(&pat[1..], &parts[1..]),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn literal() {
        let p = TopicPattern::parse("hub/led/abc/state").unwrap();
        assert!(p.matches_topic("hub/led/abc/state"));
        assert!(!p.matches_topic("hub/led/xyz/state"));
    }

    #[test]
    fn single_wild() {
        let p = TopicPattern::parse("hub/led/*/state").unwrap();
        assert!(p.matches_topic("hub/led/abc/state"));
        assert!(p.matches_topic("hub/led/xyz/state"));
        assert!(!p.matches_topic("hub/led/abc/temperature"));
        assert!(!p.matches_topic("hub/led/abc/inner/state"));
    }

    #[test]
    fn multi_wild() {
        let p = TopicPattern::parse("hub/**/state").unwrap();
        assert!(p.matches_topic("hub/led/abc/state"));
        assert!(p.matches_topic("hub/zone1/zone2/led/abc/state"));
        assert!(!p.matches_topic("hub/state")); // ** requires at least 1
        assert!(!p.matches_topic("hub/led/abc/temperature"));
    }

    #[test]
    fn trailing_multi_wild() {
        let p = TopicPattern::parse("hub/**").unwrap();
        assert!(p.matches_topic("hub/led/abc/state"));
        assert!(p.matches_topic("hub/x"));
        assert!(!p.matches_topic("hub"));
    }

    #[test]
    fn regex_segment() {
        let p = TopicPattern::parse("{^hub.+$}/led/abc/state").unwrap();
        assert!(p.matches_topic("hubA/led/abc/state"));
        assert!(p.matches_topic("hub-cloud/led/abc/state"));
        assert!(!p.matches_topic("hub/led/abc/state"));
    }

    #[test]
    fn caql_filter() {
        let p = TopicPattern::parse("hub/sensor/*/temp?where data > 85").unwrap();
        assert!(p.matches_event("hub/sensor/abc/temp", &json!({"data": 90})));
        assert!(!p.matches_event("hub/sensor/abc/temp", &json!({"data": 50})));
        assert!(!p.matches_event("hub/sensor/abc/humidity", &json!({"data": 90})));
    }
}
