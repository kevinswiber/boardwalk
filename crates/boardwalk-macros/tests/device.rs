//! Smoke test for the #[device] macro.

use boardwalk_core::{Device, DeviceConfig, DeviceError, TransitionInput};

pub struct Led {
    pub on: bool,
}

#[boardwalk_macros::device]
impl Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .state(self.state())
            .when("off", &["turn-on"])
            .when("on", &["turn-off"]);
    }

    fn state(&self) -> &str {
        if self.on { "on" } else { "off" }
    }

    #[boardwalk_macros::transition]
    async fn turn_on(&mut self) -> Result<(), DeviceError> {
        self.on = true;
        Ok(())
    }

    #[boardwalk_macros::transition]
    async fn turn_off(&mut self) -> Result<(), DeviceError> {
        self.on = false;
        Ok(())
    }
}

#[tokio::test]
async fn macro_generates_device_impl() {
    let mut led = Led { on: false };
    let mut cfg = DeviceConfig::default();
    led.config(&mut cfg);
    assert_eq!(cfg.type_.as_deref(), Some("led"));
    assert_eq!(<Led as Device>::state(&led), "off");

    led.transition("turn-on", TransitionInput::default())
        .await
        .unwrap();
    assert_eq!(<Led as Device>::state(&led), "on");

    let err = led
        .transition("nope", TransitionInput::default())
        .await
        .unwrap_err();
    assert!(matches!(err, DeviceError::Invalid(_)));
}
