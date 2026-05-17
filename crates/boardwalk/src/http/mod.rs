//! HTTP layer for Boardwalk. Hosts the runtime (`Core`) and exposes it as
//! an axum Router that emits Siren over HTTP.

#![forbid(unsafe_code)]

mod app;
mod core;
mod peer_streams;
mod render;
mod routes;
mod ws;

pub use core::{Core, CoreBuilder, DeviceHandle};

pub use app::{App, AppError, DeviceProxy, Scout, ScoutCtx, ServerHandle};
pub use peer_streams::PeerStreamHub;
// Internal-only assembly types; surfaced to sibling modules
// (`crate::server`, `crate::peer`) but not re-exported.
pub(crate) use routes::{
    AppState, DeviceRegistrar, DeviceRegistration, PeerHandler, PeerInitState, router_with,
};
pub use routes::{PeerSenders, router};
