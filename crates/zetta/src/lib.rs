//! `zetta` is the façade crate. Most users start here.

#![forbid(unsafe_code)]

pub use zetta_core::{Device, DeviceConfig, DeviceError, TransitionInput};
pub use zetta_http::{App, AppError, DeviceProxy, Scout, ScoutCtx, ServerHandle};
pub use zetta_macros::{device, transition};
pub use zetta_server::Zetta;
