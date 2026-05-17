//! A mock LED device. Two states (off/on), two transitions.

#![forbid(unsafe_code)]

use futures::future::BoxFuture;
use zetta_core::{Device, DeviceConfig, DeviceError, TransitionInput};

#[derive(Default)]
pub struct Led {
    pub on: bool,
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

    fn transition<'a>(
        &'a mut self,
        name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async move {
            match name {
                "turn-on" => {
                    self.on = true;
                    Ok(())
                }
                "turn-off" => {
                    self.on = false;
                    Ok(())
                }
                other => Err(DeviceError::Invalid(format!("unknown transition {other}"))),
            }
        })
    }
}
