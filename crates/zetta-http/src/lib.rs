//! HTTP layer for Zetta. Hosts the runtime (`Core`) and exposes it as
//! an axum Router that emits Siren over HTTP.

#![forbid(unsafe_code)]

mod core;
mod render;
mod routes;
mod ws;

pub use core::{Core, CoreBuilder, DeviceHandle};
pub use routes::{router, router_with, AppState, PeerHandler, PeerInitState};
