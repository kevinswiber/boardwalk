//! Verify that .persist(path) gives devices stable IDs across restarts.

use futures::future::BoxFuture;

use crate::Boardwalk;
use crate::core::{Device, DeviceConfig, DeviceError, TransitionInput};

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
        .use_actor(Led::default())
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
        .use_actor(Led::default())
        .build()
        .unwrap();
    let second_devices = second.core.list_devices().await;
    assert_eq!(second_devices.len(), 1);
    assert_eq!(
        second_devices[0].id, first_id,
        "device id must be stable across runs when persistence is on"
    );
}

struct NamedLed {
    name: &'static str,
}

impl Device for NamedLed {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led").name(self.name.to_string()).state("off");
    }
    fn state(&self) -> &str {
        "off"
    }
    fn transition<'a>(
        &'a mut self,
        _name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Pins today's persistent identity key: `(type, name)`. Two devices
/// of the same type but different names get two distinct stable ids,
/// and each restart reuses the same id only for its matching name.
/// A future repository expansion may change the key beyond
/// `(type, name)`; this snapshot must update when that happens.
#[tokio::test]
async fn current_registry_identity_is_type_and_name() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");

    let first = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_actor(NamedLed { name: "kitchen" })
        .use_actor(NamedLed { name: "pantry" })
        .build()
        .unwrap();
    let mut first_devices = first.core.list_devices().await;
    first_devices.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(first_devices.len(), 2);
    assert_ne!(
        first_devices[0].id, first_devices[1].id,
        "two devices with different names must get distinct ids"
    );
    let kitchen_id = first_devices
        .iter()
        .find(|d| d.name.as_deref() == Some("kitchen"))
        .unwrap()
        .id;
    let pantry_id = first_devices
        .iter()
        .find(|d| d.name.as_deref() == Some("pantry"))
        .unwrap()
        .id;
    drop(first);

    // Restart with the same two (type, name) pairs — each must reuse
    // its matching id.
    let second = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_actor(NamedLed { name: "kitchen" })
        .use_actor(NamedLed { name: "pantry" })
        .build()
        .unwrap();
    let second_devices = second.core.list_devices().await;
    let kitchen_again = second_devices
        .iter()
        .find(|d| d.name.as_deref() == Some("kitchen"))
        .unwrap()
        .id;
    let pantry_again = second_devices
        .iter()
        .find(|d| d.name.as_deref() == Some("pantry"))
        .unwrap()
        .id;
    assert_eq!(kitchen_id, kitchen_again);
    assert_eq!(pantry_id, pantry_again);
}

#[tokio::test]
async fn device_id_random_without_persist() {
    let a = Boardwalk::new()
        .name("hub")
        .use_actor(Led::default())
        .build()
        .unwrap();
    let b = Boardwalk::new()
        .name("hub")
        .use_actor(Led::default())
        .build()
        .unwrap();
    let a_id = a.core.list_devices().await[0].id;
    let b_id = b.core.list_devices().await[0].id;
    assert_ne!(a_id, b_id, "without .persist(), IDs should differ");
}
