//! `zetta` is the façade crate. Most users start here.

#![forbid(unsafe_code)]

pub use zetta_core::{App, Device, DeviceConfig, DeviceError, Scout};
pub use zetta_server::Zetta;
