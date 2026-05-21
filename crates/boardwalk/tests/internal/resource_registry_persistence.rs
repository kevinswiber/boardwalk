//! Resource registry persistence contract tests.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use super::actor_led_fixture::ActorLed;
use crate::Boardwalk;
use crate::http::{ResourceRegistration, ResourceRegistrationError};
use crate::persistence::{
    IdentityKey, Repositories, ResourceIdentityRecord, ResourceSnapshotRecord,
};
use crate::runtime::{
    Actor, DynFuture, Resource, ResourceCtx, ResourceError, ResourceSnapshot, ResourceSpec,
    TransitionCtx, TransitionError, TransitionInput, TransitionOutcome,
};

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
async fn persisted_latest_snapshot_is_used_when_actor_is_unavailable_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");

    let first = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_actor_with_id("front-panel", SnapshotActor::new("off"))
        .build()
        .unwrap();
    assert_eq!(
        first
            .core
            .get_resource("front-panel")
            .await
            .unwrap()
            .unwrap()
            .state
            .as_deref(),
        Some("off")
    );
    drop(first);

    let second = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_actor_with_id("front-panel", UnavailableSnapshotActor)
        .build()
        .unwrap();

    let snapshot = second
        .core
        .get_resource("front-panel")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state.as_deref(), Some("off"));
}

#[tokio::test]
async fn live_snapshot_wins_over_persisted_latest_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");

    let first = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_actor_with_id("front-panel", SnapshotActor::new("off"))
        .build()
        .unwrap();
    assert_eq!(
        first
            .core
            .get_resource("front-panel")
            .await
            .unwrap()
            .unwrap()
            .state
            .as_deref(),
        Some("off")
    );
    drop(first);

    let second = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_actor_with_id("front-panel", SnapshotActor::new("on"))
        .build()
        .unwrap();

    let snapshot = second
        .core
        .get_resource("front-panel")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state.as_deref(), Some("on"));
}

#[tokio::test]
async fn factory_registration_uses_same_identity_repository_as_static_resources() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");

    let first_id = register_factory_resource(&db_path, "job", "build-1", None).await;
    let second_id = register_factory_resource(&db_path, "job", "build-1", None).await;

    assert_eq!(first_id, second_id);
}

#[tokio::test]
async fn explicit_factory_id_conflict_is_reported_before_actor_starts() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");

    register_factory_resource(&db_path, "job", "build-1", Some(uuid::Uuid::nil())).await;
    let err = try_register_factory_resource(&db_path, "job", "build-2", Some(uuid::Uuid::nil()))
        .await
        .unwrap_err();

    assert_conflict(err);
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

async fn register_factory_resource(
    db_path: &Path,
    kind: &str,
    name: &str,
    explicit_id: Option<uuid::Uuid>,
) -> String {
    try_register_factory_resource(db_path, kind, name, explicit_id)
        .await
        .unwrap()
}

async fn try_register_factory_resource(
    db_path: &Path,
    kind: &str,
    name: &str,
    explicit_id: Option<uuid::Uuid>,
) -> Result<String, ResourceRegistrationError> {
    let built = Boardwalk::new()
        .name("hub")
        .persist(db_path)
        .register_actor_factory(kind, |registration| {
            Ok(FactorySnapshotActor::new(
                registration.kind,
                registration.name,
                "off",
            ))
        })
        .build()
        .unwrap();
    let registrar = built.resource_registrar.unwrap();
    registrar(ResourceRegistration {
        kind: kind.into(),
        name: Some(name.into()),
        id: explicit_id,
        fields: HashMap::new(),
    })
    .await
}

fn assert_conflict(err: ResourceRegistrationError) {
    match err {
        ResourceRegistrationError::Conflict(_) => {}
        other => panic!("expected conflict, got {other:?}"),
    }
}

struct FactorySnapshotActor {
    kind: String,
    name: Option<String>,
    state: String,
}

impl FactorySnapshotActor {
    fn new(kind: impl Into<String>, name: Option<String>, state: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            name,
            state: state.into(),
        }
    }
}

impl Resource for FactorySnapshotActor {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: self.kind.clone(),
            name: self.name.clone(),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: Vec::new(),
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move {
            let mut snapshot = led_snapshot(&self.state);
            snapshot.kind.clone_from(&self.kind);
            snapshot.name.clone_from(&self.name);
            Ok(snapshot)
        })
    }
}

impl Actor for FactorySnapshotActor {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async { Err(TransitionError::NotAllowed("not implemented".into())) })
    }
}

struct SnapshotActor {
    state: String,
}

impl SnapshotActor {
    fn new(state: impl Into<String>) -> Self {
        Self {
            state: state.into(),
        }
    }
}

impl Resource for SnapshotActor {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some("front".into()),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: Vec::new(),
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move { Ok(led_snapshot(&self.state)) })
    }
}

impl Actor for SnapshotActor {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async { Err(TransitionError::NotAllowed("not implemented".into())) })
    }
}

struct UnavailableSnapshotActor;

impl Resource for UnavailableSnapshotActor {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some("front".into()),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: Vec::new(),
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async { Err(ResourceError::Unavailable("offline".into())) })
    }
}

impl Actor for UnavailableSnapshotActor {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async { Err(TransitionError::NotAllowed("not implemented".into())) })
    }
}
