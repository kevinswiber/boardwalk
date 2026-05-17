//! `zetta` is the façade crate. Most users start here.

#![forbid(unsafe_code)]

pub use zetta_core::{Device, DeviceConfig, DeviceError, Scout, TransitionInput};
pub use zetta_http::{App, AppError, DeviceProxy, ServerHandle};
pub use zetta_server::Zetta;
