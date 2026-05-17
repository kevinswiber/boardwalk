//! Verify that .persist(path) gives devices stable IDs across restarts.

use boardwalk::{Boardwalk, Device, DeviceConfig, DeviceError, TransitionInput};
use futures::future::BoxFuture;

#[derive(Default)]
struct Led {
    on: bool,
}

impl Device for Led {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .name("LED")
            .state(self.state())
            .when("off", &["turn-on"])
            .when("on", &["turn-off"]);
    }
    fn state(&self) -> &str {
        if self.on { "on" } else { "off" }
    }
    fn transition<'a>(
        &'a mut self,
        _name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn device_id_is_stable_across_builds() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");

    let first = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_device(Led::default())
        .build()
        .unwrap();
    let first_devices = first.core.list_devices().await;
    assert_eq!(first_devices.len(), 1);
    let first_id = first_devices[0].id;

    // Drop everything, including the registry; the file persists.
    drop(first);

    let second = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_device(Led::default())
        .build()
        .unwrap();
    let second_devices = second.core.list_devices().await;
    assert_eq!(second_devices.len(), 1);
    assert_eq!(
        second_devices[0].id, first_id,
        "device id must be stable across runs when persistence is on"
    );
}

#[tokio::test]
async fn device_id_random_without_persist() {
    let a = Boardwalk::new()
        .name("hub")
        .use_device(Led::default())
        .build()
        .unwrap();
    let b = Boardwalk::new()
        .name("hub")
        .use_device(Led::default())
        .build()
        .unwrap();
    let a_id = a.core.list_devices().await[0].id;
    let b_id = b.core.list_devices().await[0].id;
    assert_ne!(a_id, b_id, "without .persist(), IDs should differ");
}
