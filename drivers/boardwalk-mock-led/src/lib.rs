//! A mock LED device. Two states (off/on), two transitions.

#![forbid(unsafe_code)]

use boardwalk_core::{Device, DeviceConfig, DeviceError};

#[derive(Default)]
pub struct Led {
    pub on: bool,
}

impl Led {
    async fn turn_on(&mut self) -> Result<(), DeviceError> {
        self.on = true;
        Ok(())
    }
    async fn turn_off(&mut self) -> Result<(), DeviceError> {
        self.on = false;
        Ok(())
    }
}

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .name("LED")
            .state(self.state())
            .when("off", &["turn-on"])
            .when("on", &["turn-off"])
            .monitor("state");
    }

    fn state(&self) -> &str {
        if self.on { "on" } else { "off" }
    }

    boardwalk_core::transitions! {
        "turn-on" => turn_on,
        "turn-off" => turn_off,
    }
}
