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
use crate::registry::{PeerConnectionRecord, ResourceRecord};
use crate::runtime::ResourceSnapshot;

const RESOURCE_IDENTITIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("resource_identities_v1");
const LEGACY_RESOURCES: TableDefinition<&str, &[u8]> = TableDefinition::new("resources");
const RESOURCE_SNAPSHOTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("resource_snapshot_records_v1");
const LEGACY_RESOURCE_LATEST_SNAPSHOTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("resource_latest_snapshots_v1");
const NODE_CONFIGS: TableDefinition<&str, &[u8]> = TableDefinition::new("node_configs_v1");
const PEER_CONFIGS: TableDefinition<&str, &[u8]> = TableDefinition::new("peers_v2");
const PEER_CONNECTION_STATUS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("peer_connection_status_v1");
const LEGACY_PEER_CONNECTIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("peer_connections_v1");

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
        if let Some(record) = get_json(&self.db, RESOURCE_IDENTITIES, id)? {
            return Ok(Some(record));
        }
        legacy_resource_by_id(&self.db, id)
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
        if let Some(existing) = legacy_resource_by_identity_key(&self.db, &record.identity_keys)?
            && existing.id != record.id
        {
            return Err(StorageError::Conflict(format!(
                "identity key is already assigned to `{}`",
                existing.id
            )));
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
        legacy_resource_by_identity_key(&self.db, std::slice::from_ref(key))
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
        let mut legacy_snapshot = record.snapshot.clone();
        legacy_snapshot.id.clone_from(&record.resource_id);
        put_json(&self.db, RESOURCE_SNAPSHOTS, &record.resource_id, &record)?;
        put_json(
            &self.db,
            LEGACY_RESOURCE_LATEST_SNAPSHOTS,
            &record.resource_id,
            &legacy_snapshot,
        )
    }

    fn latest(&self, resource_id: &str) -> Result<Option<ResourceSnapshotRecord>, StorageError> {
        if let Some(record) = get_json(&self.db, RESOURCE_SNAPSHOTS, resource_id)? {
            return Ok(Some(record));
        }
        legacy_latest_snapshot_by_id(&self.db, resource_id)
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
        put_json(&self.db, NODE_CONFIGS, &record.node_id, &record)
    }

    fn get(&self, node_id: &str) -> Result<Option<NodeConfigRecord>, StorageError> {
        get_json(&self.db, NODE_CONFIGS, node_id)
    }

    fn get_local(&self) -> Result<Option<NodeConfigRecord>, StorageError> {
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
        let legacy_record = peer_connection_record_from_status(&record)?;
        put_json(
            &self.db,
            PEER_CONNECTION_STATUS,
            &record.route_name,
            &record,
        )?;
        put_json(
            &self.db,
            LEGACY_PEER_CONNECTIONS,
            &record.route_name,
            &legacy_record,
        )
    }

    fn latest_by_route(
        &self,
        route_name: &str,
    ) -> Result<Option<PeerConnectionStatusRecord>, StorageError> {
        if let Some(record) = get_json(&self.db, PEER_CONNECTION_STATUS, route_name)? {
            return Ok(Some(record));
        }
        legacy_peer_connection_by_route(&self.db, route_name)
    }
}

fn materialize_tables(db: &Database) -> Result<(), StorageError> {
    let txn = db.begin_write().map_err(storage_error)?;
    txn.open_table(RESOURCE_IDENTITIES).map_err(storage_error)?;
    txn.open_table(LEGACY_RESOURCES).map_err(storage_error)?;
    txn.open_table(RESOURCE_SNAPSHOTS).map_err(storage_error)?;
    txn.open_table(LEGACY_RESOURCE_LATEST_SNAPSHOTS)
        .map_err(storage_error)?;
    txn.open_table(NODE_CONFIGS).map_err(storage_error)?;
    txn.open_table(PEER_CONFIGS).map_err(storage_error)?;
    txn.open_table(PEER_CONNECTION_STATUS)
        .map_err(storage_error)?;
    txn.open_table(LEGACY_PEER_CONNECTIONS)
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

fn legacy_resource_by_id(
    db: &Database,
    id: &str,
) -> Result<Option<ResourceIdentityRecord>, StorageError> {
    let Some(record) = get_json::<ResourceRecord>(db, LEGACY_RESOURCES, id)? else {
        return Ok(None);
    };
    Ok(Some(resource_identity_from_legacy(record)))
}

fn legacy_resource_by_identity_key(
    db: &Database,
    keys: &[IdentityKey],
) -> Result<Option<ResourceIdentityRecord>, StorageError> {
    for record in list_json::<ResourceRecord>(db, LEGACY_RESOURCES)? {
        let identity = resource_identity_from_legacy(record);
        if identity_keys_overlap(&identity.identity_keys, keys) {
            return Ok(Some(identity));
        }
    }
    Ok(None)
}

fn resource_identity_from_legacy(record: ResourceRecord) -> ResourceIdentityRecord {
    let identity_keys = match record.name.as_ref() {
        Some(name) => vec![IdentityKey::static_name(record.type_.clone(), name.clone())],
        None => vec![IdentityKey::static_unnamed(record.type_.clone())],
    };
    ResourceIdentityRecord {
        id: record.id.to_string(),
        kind: record.type_,
        name: record.name,
        identity_keys,
        labels: Default::default(),
        created_ms: 0,
        updated_ms: 0,
    }
}

fn legacy_latest_snapshot_by_id(
    db: &Database,
    resource_id: &str,
) -> Result<Option<ResourceSnapshotRecord>, StorageError> {
    let Some(snapshot) =
        get_json::<ResourceSnapshot>(db, LEGACY_RESOURCE_LATEST_SNAPSHOTS, resource_id)?
    else {
        return Ok(None);
    };
    Ok(Some(resource_snapshot_from_legacy(snapshot)))
}

fn resource_snapshot_from_legacy(snapshot: ResourceSnapshot) -> ResourceSnapshotRecord {
    ResourceSnapshotRecord {
        resource_id: snapshot.id.clone(),
        node_id: snapshot.node.clone(),
        revision: snapshot.revision.clone(),
        snapshot,
        updated_ms: 0,
        source_event_id: None,
    }
}

fn legacy_peer_connection_by_route(
    db: &Database,
    route_name: &str,
) -> Result<Option<PeerConnectionStatusRecord>, StorageError> {
    let Some(record) = get_json::<PeerConnectionRecord>(db, LEGACY_PEER_CONNECTIONS, route_name)?
    else {
        return Ok(None);
    };
    Ok(Some(peer_connection_status_from_legacy(record)))
}

fn peer_connection_status_from_legacy(record: PeerConnectionRecord) -> PeerConnectionStatusRecord {
    PeerConnectionStatusRecord {
        connection_id: record.connection_id.to_string(),
        peer_id: record.peer_id,
        route_name: record.route_name,
        direction: record.direction,
        status: record.status,
        negotiated_capabilities: record.negotiated_capabilities,
        updated_ms: record.updated_ms,
    }
}

fn peer_connection_record_from_status(
    record: &PeerConnectionStatusRecord,
) -> Result<PeerConnectionRecord, StorageError> {
    Ok(PeerConnectionRecord {
        connection_id: uuid::Uuid::parse_str(&record.connection_id).map_err(storage_corrupt)?,
        peer_id: record.peer_id.clone(),
        route_name: record.route_name.clone(),
        direction: record.direction,
        status: record.status,
        negotiated_capabilities: record.negotiated_capabilities,
        updated_ms: record.updated_ms,
    })
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
