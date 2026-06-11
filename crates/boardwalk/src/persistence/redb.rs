use std::path::Path;
use std::sync::Arc;

use ::redb::{Database, ReadableTable, TableDefinition};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{
    IdentityKey, NodeConfigRecord, NodeConfigRepository, PeerConfigRecord, PeerConfigRepository,
    PeerConnectionStatusRecord, PeerConnectionStatusRepository, Repositories,
    ResourceIdentityRecord, ResourceIdentityRepository, ResourceSnapshotRecord,
    ResourceSnapshotRepository, StorageError,
};

const RESOURCE_IDENTITIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("resource_identities_v1");
const RESOURCE_SNAPSHOTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("resource_snapshot_records_v1");
const NODE_CONFIGS: TableDefinition<&str, &[u8]> = TableDefinition::new("node_configs_v1");
const PEER_CONFIGS: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_configs_v1");
const PEER_CONNECTION_STATUS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("peer_connection_status_v1");

#[derive(Clone)]
pub(crate) struct RedbRepositories {
    resource_identities: RedbResourceIdentityRepository,
    resource_snapshots: RedbResourceSnapshotRepository,
    node_config: RedbNodeConfigRepository,
    peer_configs: RedbPeerConfigRepository,
    peer_connection_status: RedbPeerConnectionStatusRepository,
}

impl RedbRepositories {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(storage_error)?;
        }
        let db = Arc::new(Database::create(path).map_err(storage_error)?);
        Self::from_database(db)
    }

    pub(crate) fn from_database(db: Arc<Database>) -> Result<Self, StorageError> {
        materialize_tables(&db)?;
        Ok(Self {
            resource_identities: RedbResourceIdentityRepository::new(Arc::clone(&db)),
            resource_snapshots: RedbResourceSnapshotRepository::new(Arc::clone(&db)),
            node_config: RedbNodeConfigRepository::new(Arc::clone(&db)),
            peer_configs: RedbPeerConfigRepository::new(Arc::clone(&db)),
            peer_connection_status: RedbPeerConnectionStatusRepository::new(db),
        })
    }

    pub(crate) fn resource_identities(&self) -> &RedbResourceIdentityRepository {
        &self.resource_identities
    }

    pub(crate) fn resource_snapshots(&self) -> &RedbResourceSnapshotRepository {
        &self.resource_snapshots
    }

    pub(crate) fn node_config(&self) -> &RedbNodeConfigRepository {
        &self.node_config
    }

    pub(crate) fn peer_configs(&self) -> &RedbPeerConfigRepository {
        &self.peer_configs
    }

    pub(crate) fn peer_connection_status(&self) -> &RedbPeerConnectionStatusRepository {
        &self.peer_connection_status
    }
}

impl Repositories for RedbRepositories {
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

#[derive(Clone)]
pub(crate) struct RedbResourceIdentityRepository {
    db: Arc<Database>,
}

impl RedbResourceIdentityRepository {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl ResourceIdentityRepository for RedbResourceIdentityRepository {
    fn get(&self, id: &str) -> Result<Option<ResourceIdentityRecord>, StorageError> {
        get_json(&self.db, RESOURCE_IDENTITIES, id)
    }

    fn put(&self, record: ResourceIdentityRecord) -> Result<(), StorageError> {
        for existing in list_json::<ResourceIdentityRecord>(&self.db, RESOURCE_IDENTITIES)? {
            if existing.id == record.id {
                continue;
            }
            if identity_keys_overlap(&existing.identity_keys, &record.identity_keys) {
                return Err(StorageError::Conflict(format!(
                    "identity key is already assigned to `{}`",
                    existing.id
                )));
            }
        }
        put_json(&self.db, RESOURCE_IDENTITIES, &record.id, &record)
    }

    fn find_by_identity_key(
        &self,
        key: &IdentityKey,
    ) -> Result<Option<ResourceIdentityRecord>, StorageError> {
        for record in list_json::<ResourceIdentityRecord>(&self.db, RESOURCE_IDENTITIES)? {
            if record
                .identity_keys
                .iter()
                .any(|candidate| candidate == key)
            {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }
}

#[derive(Clone)]
pub(crate) struct RedbResourceSnapshotRepository {
    db: Arc<Database>,
}

impl RedbResourceSnapshotRepository {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl ResourceSnapshotRepository for RedbResourceSnapshotRepository {
    fn upsert_latest(&self, record: ResourceSnapshotRecord) -> Result<(), StorageError> {
        put_json(&self.db, RESOURCE_SNAPSHOTS, &record.resource_id, &record)
    }

    fn latest(&self, resource_id: &str) -> Result<Option<ResourceSnapshotRecord>, StorageError> {
        get_json(&self.db, RESOURCE_SNAPSHOTS, resource_id)
    }
}

#[derive(Clone)]
pub(crate) struct RedbNodeConfigRepository {
    db: Arc<Database>,
}

impl RedbNodeConfigRepository {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl NodeConfigRepository for RedbNodeConfigRepository {
    fn put(&self, record: NodeConfigRecord) -> Result<(), StorageError> {
        put_json(
            &self.db,
            NODE_CONFIGS,
            crate::persistence::LOCAL_NODE_SENTINEL_KEY,
            &record,
        )?;
        put_json(&self.db, NODE_CONFIGS, &record.node_id, &record)
    }

    fn get(&self, node_id: &str) -> Result<Option<NodeConfigRecord>, StorageError> {
        get_json(&self.db, NODE_CONFIGS, node_id)
    }

    fn get_local(&self) -> Result<Option<NodeConfigRecord>, StorageError> {
        if let Some(record) = get_json::<NodeConfigRecord>(
            &self.db,
            NODE_CONFIGS,
            crate::persistence::LOCAL_NODE_SENTINEL_KEY,
        )? {
            return Ok(Some(record));
        }
        Ok(list_json::<NodeConfigRecord>(&self.db, NODE_CONFIGS)?
            .into_iter()
            .max_by_key(|record| record.updated_ms))
    }
}

#[derive(Clone)]
pub(crate) struct RedbPeerConfigRepository {
    db: Arc<Database>,
}

impl RedbPeerConfigRepository {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl PeerConfigRepository for RedbPeerConfigRepository {
    fn put(&self, record: PeerConfigRecord) -> Result<(), StorageError> {
        put_json(&self.db, PEER_CONFIGS, &record.route_name, &record)
    }

    fn get_by_route(&self, route_name: &str) -> Result<Option<PeerConfigRecord>, StorageError> {
        get_json(&self.db, PEER_CONFIGS, route_name)
    }
}

#[derive(Clone)]
pub(crate) struct RedbPeerConnectionStatusRepository {
    db: Arc<Database>,
}

impl RedbPeerConnectionStatusRepository {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl PeerConnectionStatusRepository for RedbPeerConnectionStatusRepository {
    fn put_latest(&self, record: PeerConnectionStatusRecord) -> Result<(), StorageError> {
        put_json(
            &self.db,
            PEER_CONNECTION_STATUS,
            &record.route_name,
            &record,
        )
    }

    fn latest_by_route(
        &self,
        route_name: &str,
    ) -> Result<Option<PeerConnectionStatusRecord>, StorageError> {
        get_json(&self.db, PEER_CONNECTION_STATUS, route_name)
    }
}

fn materialize_tables(db: &Database) -> Result<(), StorageError> {
    let txn = db.begin_write().map_err(storage_error)?;
    txn.open_table(RESOURCE_IDENTITIES).map_err(storage_error)?;
    txn.open_table(RESOURCE_SNAPSHOTS).map_err(storage_error)?;
    txn.open_table(NODE_CONFIGS).map_err(storage_error)?;
    txn.open_table(PEER_CONFIGS).map_err(storage_error)?;
    txn.open_table(PEER_CONNECTION_STATUS)
        .map_err(storage_error)?;
    txn.commit().map_err(storage_error)?;
    Ok(())
}

fn put_json<T: Serialize>(
    db: &Database,
    table: TableDefinition<&str, &[u8]>,
    key: &str,
    record: &T,
) -> Result<(), StorageError> {
    let bytes = serde_json::to_vec(record).map_err(storage_error)?;
    let txn = db.begin_write().map_err(storage_error)?;
    {
        let mut table = txn.open_table(table).map_err(storage_error)?;
        table.insert(key, bytes.as_slice()).map_err(storage_error)?;
    }
    txn.commit().map_err(storage_error)?;
    Ok(())
}

fn get_json<T: DeserializeOwned>(
    db: &Database,
    table: TableDefinition<&str, &[u8]>,
    key: &str,
) -> Result<Option<T>, StorageError> {
    let txn = db.begin_read().map_err(storage_error)?;
    let table = txn.open_table(table).map_err(storage_error)?;
    match table.get(key).map_err(storage_error)? {
        Some(value) => Ok(Some(
            serde_json::from_slice(value.value()).map_err(storage_corrupt)?,
        )),
        None => Ok(None),
    }
}

fn list_json<T: DeserializeOwned>(
    db: &Database,
    table: TableDefinition<&str, &[u8]>,
) -> Result<Vec<T>, StorageError> {
    let txn = db.begin_read().map_err(storage_error)?;
    let table = txn.open_table(table).map_err(storage_error)?;
    let mut out = Vec::new();
    for item in table.iter().map_err(storage_error)? {
        let (_, value) = item.map_err(storage_error)?;
        out.push(serde_json::from_slice(value.value()).map_err(storage_corrupt)?);
    }
    Ok(out)
}

fn identity_keys_overlap(left: &[IdentityKey], right: &[IdentityKey]) -> bool {
    left.iter()
        .any(|key| right.iter().any(|other| other == key))
}

fn storage_error(err: impl std::fmt::Display) -> StorageError {
    StorageError::Unavailable(err.to_string())
}

fn storage_corrupt(err: impl std::fmt::Display) -> StorageError {
    StorageError::Corrupt(err.to_string())
}
