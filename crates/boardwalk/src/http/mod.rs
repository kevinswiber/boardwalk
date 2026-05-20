//! HTTP layer for Boardwalk. Hosts the runtime (`Core`) and exposes it as
//! an axum Router that emits Siren over HTTP.

#![forbid(unsafe_code)]

mod app;
mod core;
mod peer_streams;
mod render;
mod routes;
mod ws;

#[allow(unused_imports)]
pub(crate) use core::{Core, CoreBuilder, DeviceHandle, RESERVED_FIELDS, sanitize_properties};
pub use core::{ResourceSnapshot, StreamSpec, TransitionAffordance};

#[allow(unused_imports)]
pub(crate) use app::{App, AppError, DeviceProxy, Scout, ScoutCtx, ServerHandle};
pub(crate) use peer_streams::PeerStreamHub;
// Internal-only assembly types; surfaced to sibling modules
// (`crate::server`, `crate::peer`) but not re-exported.
pub(crate) use routes::{
    AppState, DeviceRegistrar, DeviceRegistration, PeerHandler, PeerInitState, router_with,
};
#[allow(unused_imports)]
pub(crate) use routes::{PeerSenders, router};
