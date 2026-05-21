//! Resource registry persistence contract tests.

use std::collections::BTreeMap;

use super::actor_led_fixture::ActorLed;
use crate::Boardwalk;
use crate::persistence::{
    IdentityKey, Repositories, ResourceIdentityRecord, ResourceSnapshotRecord,
};
use crate::runtime::ResourceSnapshot;

#[test]
fn resource_identity_and_latest_snapshot_are_distinct_repository_records() {
    let (repos, _dir) = temp_repositories();
    let resource_id = uuid::Uuid::new_v4().to_string();

    repos
        .resource_identities()
        .put(identity_record(&resource_id))
        .unwrap();

    repos
        .resource_snapshots()
        .upsert_latest(latest_snapshot_record(&resource_id, "off"))
        .unwrap();

    assert_eq!(
        repos
            .resource_identities()
            .find_by_identity_key(&IdentityKey::static_name("led", "front"))
            .unwrap()
            .unwrap()
            .id,
        resource_id
    );
    assert_eq!(
        repos
            .resource_snapshots()
            .latest(&resource_id)
            .unwrap()
            .unwrap()
            .snapshot
            .state
            .as_deref(),
        Some("off")
    );
}

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

fn temp_repositories() -> (Repositories, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");
    let repos = Repositories::open(&db_path).unwrap();
    (repos, dir)
}

fn identity_record(resource_id: &str) -> ResourceIdentityRecord {
    ResourceIdentityRecord {
        id: resource_id.into(),
        kind: "led".into(),
        name: Some("front".into()),
        identity_keys: vec![IdentityKey::static_name("led", "front")],
        labels: BTreeMap::new(),
        created_ms: 1,
        updated_ms: 1,
    }
}

fn latest_snapshot_record(resource_id: &str, state: &str) -> ResourceSnapshotRecord {
    ResourceSnapshotRecord {
        resource_id: resource_id.into(),
        node_id: "hub".into(),
        snapshot: led_snapshot(state),
        revision: Some("rev-1".into()),
        updated_ms: 2,
        source_event_id: None,
    }
}

fn led_snapshot(state: &str) -> ResourceSnapshot {
    ResourceSnapshot {
        id: "resource-id".into(),
        kind: "led".into(),
        name: Some("front".into()),
        state: Some(state.into()),
        node: "hub".into(),
        properties: serde_json::Map::new(),
        labels: BTreeMap::new(),
        transitions: Vec::new(),
        streams: Vec::new(),
        revision: Some("rev-1".into()),
        metadata: serde_json::Map::new(),
    }
}
