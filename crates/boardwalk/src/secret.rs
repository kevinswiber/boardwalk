//! Crate-internal wrapper for credential strings.

#![forbid(unsafe_code)]

use std::fmt;

/// A secret string whose `Debug` output is redacted.
///
/// Config structs that hold bearer secrets or proxy passwords derive
/// `Debug`; wrapping the credential in this type keeps every derived
/// `Debug` path (logs, error context, `dbg!`) from emitting the value.
/// The raw value escapes only through an explicit [`expose`] call.
///
/// [`expose`]: RedactedSecret::expose
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RedactedSecret(String);

impl RedactedSecret {
    pub(crate) fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    /// Expose the raw secret for wire use (auth headers). Keep call
    /// sites to the places the value actually goes on the wire.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RedactedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RedactedSecret(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_the_secret() {
        let secret = RedactedSecret::new("hunter2");
        let debug = format!("{secret:?}");
        assert!(!debug.contains("hunter2"), "leaked: {debug}");
        assert_eq!(debug, "RedactedSecret(<redacted>)");
    }

    #[test]
    fn expose_returns_the_raw_value() {
        assert_eq!(RedactedSecret::new("hunter2").expose(), "hunter2");
    }
}
