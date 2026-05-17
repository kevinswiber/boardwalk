//! HTTP layer for Zetta.
//!
//! Full router lands in M6. This file declares the public entry point.

#![forbid(unsafe_code)]

use std::sync::Arc;

use axum::Router;

#[derive(Clone)]
pub struct Core {
    // Wiring lives here once M1/M3/M5 are in.
    _placeholder: (),
}

impl Default for Core {
    fn default() -> Self { Self { _placeholder: () } }
}

/// Build the axum router used for both the public listener and the
/// reverse peer tunnel.
pub fn router(_core: Arc<Core>) -> Router {
    Router::new()
        .route("/", axum::routing::get(|| async { "zetta-rs" }))
}
