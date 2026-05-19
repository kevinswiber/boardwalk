//! Boardwalk — hypermedia server framework with reverse-tunnel federation.
//!
//! See [the README](https://github.com/kevinswiber/boardwalk) for an
//! introduction.

#![forbid(unsafe_code)]

pub mod caql;
pub mod core;
pub mod events;
pub mod http;
pub mod peer;
pub mod query;
pub mod registry;
pub mod runtime;
pub mod server;
pub mod siren;
pub mod tunnel;

pub use boardwalk_macros::{actor, device, transition};

// Curated re-exports — the surface most users want at the crate root.
pub use crate::core::{Device, DeviceConfig, DeviceError, TransitionInput};
pub use crate::http::{App, AppError, DeviceProxy, Scout, ScoutCtx, ServerHandle};
pub use crate::server::Boardwalk;
