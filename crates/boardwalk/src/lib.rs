//! `boardwalk` is the façade crate. Most users start here.

#![forbid(unsafe_code)]

pub use boardwalk_core::{Device, DeviceConfig, DeviceError, TransitionInput};
pub use boardwalk_http::{App, AppError, DeviceProxy, Scout, ScoutCtx, ServerHandle};
pub use boardwalk_macros::{device, transition};
pub use boardwalk_server::Boardwalk;
