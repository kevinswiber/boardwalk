//! Crate-private persistence repository boundaries.

// Repository contracts are introduced before the runtime wiring moves over to them.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::peer::{PeerCapabilities, PeerConnectionStatus};
use crate::registry::PeerConnectionDirection;
use crate::runtime::ResourceSnapshot;

pub(crate) mod redb;
pub(crate) use redb::RedbRepositories as DefaultRepositories;

#[derive(Debug, Error)]
pub(crate) enum StorageError {
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    #[error("storage conflict: {0}")]
    Conflict(String),
    #[error("storage corrupt: {0}")]
    Corrupt(String),
    #[error("storage internal: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct IdentityKey {
    pub(crate) namespace: String,
    pub(crate) kind: String,
    pub(crate) key: String,
}

impl IdentityKey {
    pub(crate) fn static_name(kind: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: "static".into(),
            kind: kind.into(),
            key: name.into(),
        }
    }

    pub(crate) fn static_unnamed(kind: impl Into<String>) -> Self {
        Self {
            namespace: "static".into(),
            kind: kind.into(),
            key: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ResourceIdentityRecord {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) name: Option<String>,
    pub(crate) identity_keys: Vec<IdentityKey>,
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) created_ms: i64,
    pub(crate) updated_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ResourceSnapshotRecord {
    pub(crate) resource_id: String,
    pub(crate) node_id: String,
    pub(crate) snapshot: ResourceSnapshot,
    pub(crate) revision: Option<String>,
    pub(crate) updated_ms: i64,
    pub(crate) source_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeConfigRecord {
    pub(crate) node_id: String,
    pub(crate) display_name: String,
    pub(crate) route_name: String,
    pub(crate) updated_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PeerConfigRecord {
    pub(crate) peer_id: String,
    pub(crate) route_name: String,
    pub(crate) node_id: Option<String>,
    pub(crate) display_name: Option<String>,
    pub(crate) allowed_capabilities: PeerCapabilities,
    pub(crate) updated_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PeerConnectionStatusRecord {
    pub(crate) connection_id: String,
    pub(crate) peer_id: String,
    pub(crate) route_name: String,
    pub(crate) direction: PeerConnectionDirection,
    pub(crate) status: PeerConnectionStatus,
    pub(crate) negotiated_capabilities: PeerCapabilities,
    pub(crate) updated_ms: i64,
}

pub(crate) trait ResourceIdentityRepository {
    fn get(&self, id: &str) -> Result<Option<ResourceIdentityRecord>, StorageError>;
    fn put(&self, record: ResourceIdentityRecord) -> Result<(), StorageError>;
    fn find_by_identity_key(
        &self,
        key: &IdentityKey,
    ) -> Result<Option<ResourceIdentityRecord>, StorageError>;
}

pub(crate) trait ResourceSnapshotRepository {
    fn upsert_latest(&self, record: ResourceSnapshotRecord) -> Result<(), StorageError>;
    fn latest(&self, resource_id: &str) -> Result<Option<ResourceSnapshotRecord>, StorageError>;
}

pub(crate) trait NodeConfigRepository {
    fn put(&self, record: NodeConfigRecord) -> Result<(), StorageError>;
    fn get(&self, node_id: &str) -> Result<Option<NodeConfigRecord>, StorageError>;
}

pub(crate) trait PeerConfigRepository {
    fn put(&self, record: PeerConfigRecord) -> Result<(), StorageError>;
    fn get_by_route(&self, route_name: &str) -> Result<Option<PeerConfigRecord>, StorageError>;
}

pub(crate) trait PeerConnectionStatusRepository {
    fn put_latest(&self, record: PeerConnectionStatusRecord) -> Result<(), StorageError>;
    fn latest_by_route(
        &self,
        route_name: &str,
    ) -> Result<Option<PeerConnectionStatusRecord>, StorageError>;
}

pub(crate) trait EventHistoryRepository {}

pub(crate) trait Repositories {
    fn resource_identities(&self) -> &dyn ResourceIdentityRepository;
    fn resource_snapshots(&self) -> &dyn ResourceSnapshotRepository;
    fn node_config(&self) -> &dyn NodeConfigRepository;
    fn peer_configs(&self) -> &dyn PeerConfigRepository;
    fn peer_connection_status(&self) -> &dyn PeerConnectionStatusRepository;
    fn event_history(&self) -> Option<&dyn EventHistoryRepository> {
        None
    }
}

#[derive(Clone)]
pub(crate) struct MemoryRepositories {
    resource_identities: ResourceIdentityStore,
    resource_snapshots: ResourceSnapshotStore,
    node_config: NodeConfigStore,
    peer_configs: PeerConfigStore,
    peer_connection_status: PeerConnectionStatusStore,
}

impl Default for MemoryRepositories {
    fn default() -> Self {
        let state = Arc::new(Mutex::new(RepositoryState::default()));
        Self {
            resource_identities: ResourceIdentityStore::new(Arc::clone(&state)),
            resource_snapshots: ResourceSnapshotStore::new(Arc::clone(&state)),
            node_config: NodeConfigStore::new(Arc::clone(&state)),
            peer_configs: PeerConfigStore::new(Arc::clone(&state)),
            peer_connection_status: PeerConnectionStatusStore::new(state),
        }
    }
}

impl MemoryRepositories {
    pub(crate) fn resource_identities(&self) -> &ResourceIdentityStore {
        &self.resource_identities
    }

    pub(crate) fn resource_snapshots(&self) -> &ResourceSnapshotStore {
        &self.resource_snapshots
    }

    pub(crate) fn node_config(&self) -> &NodeConfigStore {
        &self.node_config
    }

    pub(crate) fn peer_configs(&self) -> &PeerConfigStore {
        &self.peer_configs
    }

    pub(crate) fn peer_connection_status(&self) -> &PeerConnectionStatusStore {
        &self.peer_connection_status
    }
}

impl Repositories for MemoryRepositories {
    fn resource_identities(&self) -> &dyn ResourceIdentityRepository {
        &self.resource_identities
    }

    fn resource_snapshots(&self) -> &dyn ResourceSnapshotRepository {
        &self.resource_snapshots
    }

    fn node_config(&self) -> &dyn NodeConfigRepository {
        &self.node_config
    }

    fn peer_configs(&self) -> &dyn PeerConfigRepository {
        &self.peer_configs
    }

    fn peer_connection_status(&self) -> &dyn PeerConnectionStatusRepository {
        &self.peer_connection_status
    }
}

#[derive(Default)]
struct RepositoryState {
    identities: HashMap<String, ResourceIdentityRecord>,
    identity_keys: HashMap<IdentityKey, String>,
    latest_snapshots: HashMap<String, ResourceSnapshotRecord>,
    node_configs: HashMap<String, NodeConfigRecord>,
    peer_configs: HashMap<String, PeerConfigRecord>,
    peer_connection_statuses: HashMap<String, PeerConnectionStatusRecord>,
}

#[derive(Clone)]
pub(crate) struct ResourceIdentityStore {
    state: Arc<Mutex<RepositoryState>>,
}

impl ResourceIdentityStore {
    fn new(state: Arc<Mutex<RepositoryState>>) -> Self {
        Self { state }
    }

    pub(crate) fn put(&self, record: ResourceIdentityRecord) -> Result<(), StorageError> {
        <Self as ResourceIdentityRepository>::put(self, record)
    }

    pub(crate) fn find_by_identity_key(
        &self,
        key: &IdentityKey,
    ) -> Result<Option<ResourceIdentityRecord>, StorageError> {
        <Self as ResourceIdentityRepository>::find_by_identity_key(self, key)
    }
}

impl ResourceIdentityRepository for ResourceIdentityStore {
    fn get(&self, id: &str) -> Result<Option<ResourceIdentityRecord>, StorageError> {
        let state = lock_state(&self.state)?;
        Ok(state.identities.get(id).cloned())
    }

    fn put(&self, record: ResourceIdentityRecord) -> Result<(), StorageError> {
        let mut state = lock_state(&self.state)?;
        for key in &record.identity_keys {
            if let Some(existing_id) = state.identity_keys.get(key)
                && existing_id != &record.id
            {
                return Err(StorageError::Conflict(format!(
                    "identity key is already assigned to `{existing_id}`"
                )));
            }
        }
        if let Some(previous_keys) = state
            .identities
            .get(&record.id)
            .map(|previous| previous.identity_keys.clone())
        {
            for key in &previous_keys {
                state.identity_keys.remove(key);
            }
        }
        for key in &record.identity_keys {
            state.identity_keys.insert(key.clone(), record.id.clone());
        }
        state.identities.insert(record.id.clone(), record);
        Ok(())
    }

    fn find_by_identity_key(
        &self,
        key: &IdentityKey,
    ) -> Result<Option<ResourceIdentityRecord>, StorageError> {
        let state = lock_state(&self.state)?;
        let Some(id) = state.identity_keys.get(key) else {
            return Ok(None);
        };
        Ok(state.identities.get(id).cloned())
    }
}

#[derive(Clone)]
pub(crate) struct NodeConfigStore {
    state: Arc<Mutex<RepositoryState>>,
}

impl NodeConfigStore {
    fn new(state: Arc<Mutex<RepositoryState>>) -> Self {
        Self { state }
    }
}

impl NodeConfigRepository for NodeConfigStore {
    fn put(&self, record: NodeConfigRecord) -> Result<(), StorageError> {
        let mut state = lock_state(&self.state)?;
        state.node_configs.insert(record.node_id.clone(), record);
        Ok(())
    }

    fn get(&self, node_id: &str) -> Result<Option<NodeConfigRecord>, StorageError> {
        let state = lock_state(&self.state)?;
        Ok(state.node_configs.get(node_id).cloned())
    }
}

#[derive(Clone)]
pub(crate) struct PeerConfigStore {
    state: Arc<Mutex<RepositoryState>>,
}

impl PeerConfigStore {
    fn new(state: Arc<Mutex<RepositoryState>>) -> Self {
        Self { state }
    }
}

impl PeerConfigRepository for PeerConfigStore {
    fn put(&self, record: PeerConfigRecord) -> Result<(), StorageError> {
        let mut state = lock_state(&self.state)?;
        state.peer_configs.insert(record.route_name.clone(), record);
        Ok(())
    }

    fn get_by_route(&self, route_name: &str) -> Result<Option<PeerConfigRecord>, StorageError> {
        let state = lock_state(&self.state)?;
        Ok(state.peer_configs.get(route_name).cloned())
    }
}

#[derive(Clone)]
pub(crate) struct PeerConnectionStatusStore {
    state: Arc<Mutex<RepositoryState>>,
}

impl PeerConnectionStatusStore {
    fn new(state: Arc<Mutex<RepositoryState>>) -> Self {
        Self { state }
    }
}

impl PeerConnectionStatusRepository for PeerConnectionStatusStore {
    fn put_latest(&self, record: PeerConnectionStatusRecord) -> Result<(), StorageError> {
        let mut state = lock_state(&self.state)?;
        state
            .peer_connection_statuses
            .insert(record.route_name.clone(), record);
        Ok(())
    }

    fn latest_by_route(
        &self,
        route_name: &str,
    ) -> Result<Option<PeerConnectionStatusRecord>, StorageError> {
        let state = lock_state(&self.state)?;
        Ok(state.peer_connection_statuses.get(route_name).cloned())
    }
}

#[derive(Clone)]
pub(crate) struct ResourceSnapshotStore {
    state: Arc<Mutex<RepositoryState>>,
}

impl ResourceSnapshotStore {
    fn new(state: Arc<Mutex<RepositoryState>>) -> Self {
        Self { state }
    }

    pub(crate) fn upsert_latest(&self, record: ResourceSnapshotRecord) -> Result<(), StorageError> {
        <Self as ResourceSnapshotRepository>::upsert_latest(self, record)
    }

    pub(crate) fn latest(
        &self,
        resource_id: &str,
    ) -> Result<Option<ResourceSnapshotRecord>, StorageError> {
        <Self as ResourceSnapshotRepository>::latest(self, resource_id)
    }
}

impl ResourceSnapshotRepository for ResourceSnapshotStore {
    fn upsert_latest(&self, record: ResourceSnapshotRecord) -> Result<(), StorageError> {
        let mut state = lock_state(&self.state)?;
        state
            .latest_snapshots
            .insert(record.resource_id.clone(), record);
        Ok(())
    }

    fn latest(&self, resource_id: &str) -> Result<Option<ResourceSnapshotRecord>, StorageError> {
        let state = lock_state(&self.state)?;
        Ok(state.latest_snapshots.get(resource_id).cloned())
    }
}

fn lock_state(
    state: &Arc<Mutex<RepositoryState>>,
) -> Result<MutexGuard<'_, RepositoryState>, StorageError> {
    state
        .lock()
        .map_err(|_| StorageError::Internal("repository lock poisoned".into()))
}
