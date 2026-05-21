//! Resource registry persistence contract tests.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use super::actor_led_fixture::ActorLed;
use crate::Boardwalk;
use crate::http::{ResourceRegistration, ResourceRegistrationError};
use crate::peer::{PeerCapabilities, PeerConnectionStatus};
use crate::persistence::redb::RedbRepositories;
use crate::persistence::{
    EventHistoryRepository, IdentityKey, MemoryRepositories, NodeConfigRecord,
    NodeConfigRepository, PeerConfigRecord, PeerConfigRepository, PeerConnectionStatusRecord,
    PeerConnectionStatusRepository, Repositories, ResourceIdentityRecord,
    ResourceIdentityRepository, ResourceSnapshotRecord, ResourceSnapshotRepository, StorageError,
};
use crate::registry::{
    PeerConnectionDirection, PeerConnectionRecord, PeerRecord, Registry, ResourceRecord,
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

#[test]
fn repository_facade_exposes_domain_repositories() {
    let repos = MemoryRepositories::default();

    let _: &dyn ResourceIdentityRepository = repos.resource_identities();
    let _: &dyn ResourceSnapshotRepository = repos.resource_snapshots();
    let _: &dyn NodeConfigRepository = repos.node_config();
    let _: &dyn PeerConfigRepository = repos.peer_configs();
    let _: &dyn PeerConnectionStatusRepository = repos.peer_connection_status();
    let _: Option<&dyn EventHistoryRepository> = repos.event_history();

    let facade: &dyn Repositories = &repos;
    assert!(facade.event_history().is_none());
}

#[test]
fn redb_repositories_round_trip_domain_records() {
    let (repos, _dir) = temp_redb_repositories();

    repos
        .resource_identities()
        .put(identity_record("front"))
        .unwrap();
    repos
        .resource_snapshots()
        .upsert_latest(latest_snapshot_record("front", "off"))
        .unwrap();
    repos.node_config().put(node_config("hub")).unwrap();
    repos.peer_configs().put(peer_config("hub-peer")).unwrap();
    repos
        .peer_connection_status()
        .put_latest(connection_status("hub-peer"))
        .unwrap();

    assert!(
        repos
            .resource_snapshots()
            .latest("front")
            .unwrap()
            .is_some()
    );
    assert!(
        repos
            .peer_connection_status()
            .latest_by_route("hub")
            .unwrap()
            .is_some()
    );
}

#[test]
fn redb_repositories_adapt_existing_resource_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");
    let resource_id = uuid::Uuid::new_v4();
    let registry = Registry::open(&db_path).unwrap();
    registry
        .put_resource(&ResourceRecord {
            id: resource_id,
            type_: "led".into(),
            name: Some("front".into()),
            properties: serde_json::Map::new(),
        })
        .unwrap();
    drop(registry);

    let repos = RedbRepositories::open(&db_path).unwrap();

    let by_key = repos
        .resource_identities()
        .find_by_identity_key(&IdentityKey::static_name("led", "front"))
        .unwrap()
        .unwrap();
    assert_eq!(by_key.id, resource_id.to_string());
    assert_eq!(
        repos
            .resource_identities()
            .get(&resource_id.to_string())
            .unwrap()
            .unwrap()
            .kind,
        "led"
    );
}

#[test]
fn redb_repositories_adapt_existing_latest_snapshot_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");
    let registry = Registry::open(&db_path).unwrap();
    let mut snapshot = led_snapshot("off");
    snapshot.id = "front-panel".into();
    registry.put_latest_resource_snapshot(&snapshot).unwrap();
    drop(registry);

    let repos = RedbRepositories::open(&db_path).unwrap();

    let latest = repos
        .resource_snapshots()
        .latest("front-panel")
        .unwrap()
        .unwrap();
    assert_eq!(latest.resource_id, "front-panel");
    assert_eq!(latest.node_id, "hub");
    assert_eq!(latest.revision.as_deref(), Some("rev-1"));
    assert_eq!(latest.snapshot.state.as_deref(), Some("off"));

    repos
        .resource_snapshots()
        .upsert_latest(latest_snapshot_record("front-panel", "on"))
        .unwrap();
    drop(repos);
    let registry = Registry::open(&db_path).unwrap();
    assert_eq!(
        registry
            .latest_resource_snapshot("front-panel")
            .unwrap()
            .unwrap()
            .state
            .as_deref(),
        Some("on")
    );
}

#[test]
fn redb_repositories_adapt_existing_peer_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");
    let registry = Registry::open(&db_path).unwrap();
    registry.put_peer(&peer_record("hub-peer")).unwrap();
    let connection = peer_connection_record("hub-peer");
    registry.put_peer_connection(&connection).unwrap();
    drop(registry);

    let repos = RedbRepositories::open(&db_path).unwrap();

    assert_eq!(
        repos
            .peer_configs()
            .get_by_route("hub")
            .unwrap()
            .unwrap()
            .peer_id,
        "hub-peer"
    );
    let latest = repos
        .peer_connection_status()
        .latest_by_route("hub")
        .unwrap()
        .unwrap();
    assert_eq!(latest.connection_id, connection.connection_id.to_string());
    assert_eq!(latest.status, PeerConnectionStatus::Connected);

    repos
        .peer_configs()
        .put(peer_config("new-hub-peer"))
        .unwrap();
    repos
        .peer_connection_status()
        .put_latest(connection_status("new-hub-peer"))
        .unwrap();
    drop(repos);
    let registry = Registry::open(&db_path).unwrap();
    assert_eq!(
        registry.get_peer_by_route("hub").unwrap().unwrap().peer_id,
        "new-hub-peer"
    );
    assert_eq!(
        registry
            .latest_peer_connection("hub")
            .unwrap()
            .unwrap()
            .peer_id,
        "new-hub-peer"
    );
}

#[test]
fn redb_open_failure_maps_to_storage_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let not_a_directory = dir.path().join("not-a-directory");
    std::fs::write(&not_a_directory, b"file").unwrap();
    let db_path = not_a_directory.join("boardwalk.redb");

    let err = match RedbRepositories::open(&db_path) {
        Ok(_) => panic!("opening under a file should fail"),
        Err(err) => err,
    };

    assert!(matches!(err, StorageError::Unavailable(_)));
}

#[test]
fn builder_persistence_open_error_is_generic() {
    let dir = tempfile::tempdir().unwrap();
    let not_a_directory = dir.path().join("not-a-directory");
    std::fs::write(&not_a_directory, b"file").unwrap();
    let db_path = not_a_directory.join("boardwalk.redb");

    let err = match Boardwalk::new().name("hub").persist(db_path).build() {
        Ok(_) => panic!("opening persistence under a file should fail"),
        Err(err) => err,
    };
    let message = format!("{err:#}");

    assert_eq!(err.to_string(), "storage unavailable");
    assert_no_backend_error_details(&message);
}

#[test]
fn redb_decode_failure_maps_to_storage_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");
    let db = redb::Database::create(&db_path).unwrap();
    let legacy_resources: redb::TableDefinition<&str, &[u8]> =
        redb::TableDefinition::new("resources");
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(legacy_resources).unwrap();
        table.insert("bad", b"not-json".as_slice()).unwrap();
    }
    txn.commit().unwrap();
    drop(db);

    let repos = RedbRepositories::open(&db_path).unwrap();
    let err = repos.resource_identities().get("bad").unwrap_err();

    assert!(matches!(err, StorageError::Corrupt(_)));
}

#[test]
fn resource_identity_storage_error_is_generic() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");
    write_bad_resource_row(&db_path);

    let err = match Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .use_actor(ActorLed::default())
        .build()
    {
        Ok(_) => panic!("corrupt persisted resource row should fail resource identity lookup"),
        Err(err) => err,
    };
    let message = format!("{err:#}");

    assert_eq!(err.to_string(), "storage unavailable");
    assert_no_backend_error_details(&message);
}

#[tokio::test]
async fn factory_registration_storage_error_is_generic() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");
    write_bad_resource_row(&db_path);

    let built = Boardwalk::new()
        .name("hub")
        .persist(&db_path)
        .register_actor_factory("led", |registration| {
            Ok(FactorySnapshotActor::new(
                registration.kind,
                registration.name,
                "off",
            ))
        })
        .build()
        .unwrap();
    let registrar = built.resource_registrar.unwrap();
    let err = registrar(ResourceRegistration {
        kind: "led".into(),
        name: Some("front".into()),
        id: None,
        fields: HashMap::new(),
    })
    .await
    .unwrap_err();

    match err {
        ResourceRegistrationError::Internal(message) => {
            assert_eq!(message, "storage unavailable");
            assert_no_backend_error_details(&message);
        }
        other => panic!("expected internal storage error, got {other:?}"),
    }
}

#[test]
fn resource_identity_repository_rejects_key_reassignment() {
    let (repos, _dir) = temp_repositories();
    let first_id = uuid::Uuid::new_v4().to_string();
    let second_id = uuid::Uuid::new_v4().to_string();

    repos
        .resource_identities()
        .put(identity_record(&first_id))
        .unwrap();
    let err = repos
        .resource_identities()
        .put(identity_record(&second_id))
        .unwrap_err();

    assert!(
        err.to_string().contains("storage conflict"),
        "unexpected error: {err}"
    );
}

#[test]
fn resource_identity_repository_replaces_stale_keys_for_same_record() {
    let (repos, _dir) = temp_repositories();
    let resource_id = uuid::Uuid::new_v4().to_string();
    repos
        .resource_identities()
        .put(identity_record(&resource_id))
        .unwrap();

    let replacement = ResourceIdentityRecord {
        identity_keys: vec![IdentityKey::static_name("led", "back")],
        ..identity_record(&resource_id)
    };
    repos.resource_identities().put(replacement).unwrap();

    assert!(
        repos
            .resource_identities()
            .find_by_identity_key(&IdentityKey::static_name("led", "front"))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        repos
            .resource_identities()
            .find_by_identity_key(&IdentityKey::static_name("led", "back"))
            .unwrap()
            .unwrap()
            .id,
        resource_id
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

fn temp_repositories() -> (MemoryRepositories, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let repos = MemoryRepositories::default();
    (repos, dir)
}

fn temp_redb_repositories() -> (RedbRepositories, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("boardwalk.redb");
    let repos = RedbRepositories::open(&db_path).unwrap();
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

fn node_config(node_id: &str) -> NodeConfigRecord {
    NodeConfigRecord {
        node_id: node_id.into(),
        display_name: "Hub".into(),
        route_name: "hub".into(),
        updated_ms: 1,
    }
}

fn peer_config(peer_id: &str) -> PeerConfigRecord {
    PeerConfigRecord {
        peer_id: peer_id.into(),
        route_name: "hub".into(),
        node_id: Some("node-hub-1".into()),
        display_name: Some("Hub".into()),
        allowed_capabilities: PeerCapabilities::resource_read(),
        updated_ms: 1,
    }
}

fn peer_record(peer_id: &str) -> PeerRecord {
    PeerRecord {
        peer_id: peer_id.into(),
        route_name: "hub".into(),
        node_id: Some("node-hub-1".into()),
        display_name: Some("Hub".into()),
        allowed_capabilities: PeerCapabilities::resource_read(),
        updated_ms: 1,
    }
}

fn connection_status(peer_id: &str) -> PeerConnectionStatusRecord {
    PeerConnectionStatusRecord {
        connection_id: uuid::Uuid::new_v4().to_string(),
        peer_id: peer_id.into(),
        route_name: "hub".into(),
        direction: PeerConnectionDirection::Acceptor,
        status: PeerConnectionStatus::Connected,
        negotiated_capabilities: PeerCapabilities::resource_read(),
        updated_ms: 2,
    }
}

fn peer_connection_record(peer_id: &str) -> PeerConnectionRecord {
    PeerConnectionRecord {
        connection_id: uuid::Uuid::new_v4(),
        peer_id: peer_id.into(),
        route_name: "hub".into(),
        direction: PeerConnectionDirection::Acceptor,
        status: PeerConnectionStatus::Connected,
        negotiated_capabilities: PeerCapabilities::resource_read(),
        updated_ms: 2,
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

fn write_bad_resource_row(db_path: &Path) {
    let db = redb::Database::create(db_path).unwrap();
    let legacy_resources: redb::TableDefinition<&str, &[u8]> =
        redb::TableDefinition::new("resources");
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(legacy_resources).unwrap();
        table.insert("bad", b"not-json".as_slice()).unwrap();
    }
    txn.commit().unwrap();
}

fn assert_no_backend_error_details(message: &str) {
    for snippet in [
        "redb",
        "io:",
        "encode:",
        "decode",
        "registry",
        "not-a-directory",
    ] {
        assert!(
            !message.contains(snippet),
            "public storage error must not expose backend detail `{snippet}`: {message}"
        );
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
