//! Resource registry persistence contract tests.

use super::actor_led_fixture::ActorLed;
use crate::Boardwalk;

#[tokio::test]
async fn resource_id_is_stable_across_builds() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");

    let first = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_actor(ActorLed::default())
        .build()
        .unwrap();
    let first_resources = first.core.list_resources().await;
    assert_eq!(first_resources.len(), 1);
    let first_id = first_resources[0].id.clone();
    drop(first);

    let second = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_actor(ActorLed::default())
        .build()
        .unwrap();
    let second_resources = second.core.list_resources().await;
    assert_eq!(second_resources.len(), 1);
    assert_eq!(
        second_resources[0].id, first_id,
        "resource id must be stable across runs when persistence is enabled"
    );
}

#[tokio::test]
async fn resource_id_is_random_without_persistence() {
    let first = Boardwalk::new()
        .name("hub")
        .use_actor(ActorLed::default())
        .build()
        .unwrap();
    let second = Boardwalk::new()
        .name("hub")
        .use_actor(ActorLed::default())
        .build()
        .unwrap();

    let first_id = first.core.list_resources().await[0].id.clone();
    let second_id = second.core.list_resources().await[0].id.clone();
    assert_ne!(
        first_id, second_id,
        "without persistence, IDs should differ across builds"
    );
}
