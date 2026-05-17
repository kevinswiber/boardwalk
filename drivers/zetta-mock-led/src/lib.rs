//! A mock LED device. Two states (off/on), two transitions.

#![forbid(unsafe_code)]

use zetta_core::{Device, DeviceConfig};

#[derive(Default)]
pub struct Led {
    pub on: bool,
}

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
           .name("LED")
           .state(if self.on { "on" } else { "off" })
           .when("off", &["turn-on"])
           .when("on", &["turn-off"]);
    }
}
