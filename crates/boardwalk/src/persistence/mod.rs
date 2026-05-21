//! Crate-private persistence repository boundaries.

// Repository contracts are introduced before the runtime wiring moves over to them.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use thiserror::Error;

use crate::runtime::ResourceSnapshot;

#[derive(Debug, Error)]
pub(crate) enum StorageError {
    #[error("repository lock poisoned")]
    LockPoisoned,
    #[error("identity conflict: {0}")]
    IdentityConflict(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct IdentityKey {
    scope: String,
    value: String,
}

impl IdentityKey {
    pub(crate) fn static_name(kind: &str, name: &str) -> Self {
        Self {
            scope: "static-name".into(),
            value: format!("{kind}:{name}"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResourceIdentityRecord {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) name: Option<String>,
    pub(crate) identity_keys: Vec<IdentityKey>,
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) created_ms: i64,
    pub(crate) updated_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ResourceSnapshotRecord {
    pub(crate) resource_id: String,
    pub(crate) node_id: String,
    pub(crate) snapshot: ResourceSnapshot,
    pub(crate) revision: Option<String>,
    pub(crate) updated_ms: i64,
    pub(crate) source_event_id: Option<String>,
}

pub(crate) trait ResourceIdentityRepository {
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

#[derive(Clone)]
pub(crate) struct Repositories {
    resource_identities: ResourceIdentityStore,
    resource_snapshots: ResourceSnapshotStore,
}

impl Repositories {
    pub(crate) fn memory() -> Self {
        let state = Arc::new(Mutex::new(RepositoryState::default()));
        Self {
            resource_identities: ResourceIdentityStore::new(Arc::clone(&state)),
            resource_snapshots: ResourceSnapshotStore::new(state),
        }
    }

    pub(crate) fn resource_identities(&self) -> &ResourceIdentityStore {
        &self.resource_identities
    }

    pub(crate) fn resource_snapshots(&self) -> &ResourceSnapshotStore {
        &self.resource_snapshots
    }
}

#[derive(Default)]
struct RepositoryState {
    identities: HashMap<String, ResourceIdentityRecord>,
    identity_keys: HashMap<IdentityKey, String>,
    latest_snapshots: HashMap<String, ResourceSnapshotRecord>,
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
    fn put(&self, record: ResourceIdentityRecord) -> Result<(), StorageError> {
        let mut state = lock_state(&self.state)?;
        for key in &record.identity_keys {
            if let Some(existing_id) = state.identity_keys.get(key)
                && existing_id != &record.id
            {
                return Err(StorageError::IdentityConflict(format!(
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
    state.lock().map_err(|_| StorageError::LockPoisoned)
}
