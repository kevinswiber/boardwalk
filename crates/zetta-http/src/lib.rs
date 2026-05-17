//! HTTP layer for Zetta. Hosts the runtime (`Core`) and exposes it as
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
pub use routes::{
    AppState, DeviceRegistrar, DeviceRegistration, PeerHandler, PeerInitState, PeerSenders, router,
    router_with,
};
