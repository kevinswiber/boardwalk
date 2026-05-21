//! Persistent registries for resources and peers, backed by redb.

#![forbid(unsafe_code)]
// redb's error variants are intentionally large (rich diagnostic info).
// Registry calls aren't on hot paths, so the stack cost is fine.
#![allow(clippy::result_large_err)]

use std::path::{Path, PathBuf};

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::peer::{PeerCapabilities, PeerConnectionStatus};

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("redb db: {0}")]
    Db(#[from] redb::DatabaseError),
    #[error("redb txn: {0}")]
    Txn(#[from] redb::TransactionError),
    #[error("redb table: {0}")]
    Table(#[from] redb::TableError),
    #[error("redb commit: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("redb storage: {0}")]
    Storage(#[from] redb::StorageError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRecord {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub type_: String,
    pub name: Option<String>,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
    pub peer_id: String,
    pub route_name: String,
    pub node_id: Option<String>,
    pub display_name: Option<String>,
    pub allowed_capabilities: PeerCapabilities,
    pub updated_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerConnectionDirection {
    Initiator,
    Acceptor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConnectionRecord {
    pub connection_id: Uuid,
    pub peer_id: String,
    pub route_name: String,
    pub direction: PeerConnectionDirection,
    pub status: PeerConnectionStatus,
    pub negotiated_capabilities: PeerCapabilities,
    pub updated_ms: i64,
}

const RESOURCE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("resources");
const PEERS: TableDefinition<&str, &[u8]> = TableDefinition::new("peers_v2");
const PEER_CONNECTIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_connections_v1");

#[allow(dead_code)]
pub struct Config {
    pub root: PathBuf,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            root: PathBuf::from(".boardwalk"),
        }
    }
}

pub struct Registry {
    db: Database,
}

impl Registry {
    /// Open (or create) a redb-backed registry at `path`. The parent
    /// directory is created if it doesn't exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RegistryError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path)?;
        // Materialize the tables so first reads don't fail.
        let txn = db.begin_write()?;
        txn.open_table(RESOURCE_TABLE)?;
        txn.open_table(PEERS)?;
        txn.open_table(PEER_CONNECTIONS)?;
        txn.commit()?;
        Ok(Self { db })
    }

    // -- resources -------------------------------------------------------

    pub fn put_resource(&self, rec: &ResourceRecord) -> Result<(), RegistryError> {
        let bytes = serde_json::to_vec(rec)?;
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(RESOURCE_TABLE)?;
            t.insert(rec.id.to_string().as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_resource(&self, id: &Uuid) -> Result<Option<ResourceRecord>, RegistryError> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(RESOURCE_TABLE)?;
        match t.get(id.to_string().as_str())? {
            Some(av) => Ok(Some(serde_json::from_slice(av.value())?)),
            None => Ok(None),
        }
    }

    pub fn list_resources(&self) -> Result<Vec<ResourceRecord>, RegistryError> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(RESOURCE_TABLE)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            let (_, av) = item?;
            out.push(serde_json::from_slice(av.value())?);
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn delete_resource(&self, id: &Uuid) -> Result<bool, RegistryError> {
        let txn = self.db.begin_write()?;
        let removed = {
            let mut t = txn.open_table(RESOURCE_TABLE)?;
            t.remove(id.to_string().as_str())?.is_some()
        };
        txn.commit()?;
        Ok(removed)
    }

    /// Find an existing resource by (kind, name) identity. Returns the
    /// first match. Used at boot to restore stable resource IDs.
    pub fn find_resource_by_identity(
        &self,
        type_: &str,
        name: Option<&str>,
    ) -> Result<Option<ResourceRecord>, RegistryError> {
        for rec in self.list_resources()? {
            if rec.type_ == type_ && rec.name.as_deref() == name {
                return Ok(Some(rec));
            }
        }
        Ok(None)
    }

    // -- peers ------------------------------------------------------------

    pub fn put_peer(&self, rec: &PeerRecord) -> Result<(), RegistryError> {
        let bytes = serde_json::to_vec(rec)?;
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(PEERS)?;
            t.insert(rec.route_name.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_peer(&self, name: &str) -> Result<Option<PeerRecord>, RegistryError> {
        self.get_peer_by_route(name)
    }

    #[allow(dead_code)]
    pub fn get_peer_by_route(&self, route_name: &str) -> Result<Option<PeerRecord>, RegistryError> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        match t.get(route_name)? {
            Some(av) => Ok(Some(serde_json::from_slice(av.value())?)),
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub fn list_peers(&self) -> Result<Vec<PeerRecord>, RegistryError> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            let (_, av) = item?;
            out.push(serde_json::from_slice(av.value())?);
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn delete_peer(&self, name: &str) -> Result<bool, RegistryError> {
        let txn = self.db.begin_write()?;
        let removed = {
            let mut t = txn.open_table(PEERS)?;
            t.remove(name)?.is_some()
        };
        txn.commit()?;
        Ok(removed)
    }

    pub fn put_peer_connection(&self, rec: &PeerConnectionRecord) -> Result<(), RegistryError> {
        let bytes = serde_json::to_vec(rec)?;
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(PEER_CONNECTIONS)?;
            t.insert(rec.route_name.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn latest_peer_connection(
        &self,
        route_name: &str,
    ) -> Result<Option<PeerConnectionRecord>, RegistryError> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEER_CONNECTIONS)?;
        match t.get(route_name)? {
            Some(av) => Ok(Some(serde_json::from_slice(av.value())?)),
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub fn list_peer_connections(&self) -> Result<Vec<PeerConnectionRecord>, RegistryError> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEER_CONNECTIONS)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            let (_, av) = item?;
            out.push(serde_json::from_slice(av.value())?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Map;

    use super::*;

    fn temp_db() -> (Registry, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("boardwalk.redb");
        let reg = Registry::open(&path).unwrap();
        (reg, dir)
    }

    #[test]
    fn resources_round_trip() {
        let (reg, _dir) = temp_db();
        let id = Uuid::new_v4();
        let rec = ResourceRecord {
            id,
            type_: "led".into(),
            name: Some("L".into()),
            properties: Map::new(),
        };
        reg.put_resource(&rec).unwrap();
        let got = reg.get_resource(&id).unwrap().unwrap();
        assert_eq!(got.id, id);
        assert_eq!(got.type_, "led");
        assert_eq!(reg.list_resources().unwrap().len(), 1);
        assert!(reg.delete_resource(&id).unwrap());
        assert!(reg.get_resource(&id).unwrap().is_none());
    }

    #[test]
    fn peers_round_trip() {
        let (reg, _dir) = temp_db();
        let rec = PeerRecord {
            peer_id: "peer-cloud".into(),
            route_name: "cloud".into(),
            node_id: Some("node-cloud-1".into()),
            display_name: Some("Cloud".into()),
            allowed_capabilities: crate::peer::PeerCapabilities::resource_read(),
            updated_ms: 0,
        };
        reg.put_peer(&rec).unwrap();
        let got = reg.get_peer("cloud").unwrap().unwrap();
        assert_eq!(got.peer_id, "peer-cloud");
        assert_eq!(got.route_name, "cloud");
        assert_eq!(reg.list_peers().unwrap().len(), 1);
    }

    #[test]
    fn old_peer_status_rows_are_not_decoded_as_new_peer_records() {
        let (reg, _dir) = temp_db_with_old_peer_status_rows();

        assert!(reg.list_peers().unwrap().is_empty());
        assert!(reg.list_peer_connections().unwrap().is_empty());
    }

    #[test]
    fn durable_peer_and_peer_connection_records_are_separate() {
        let (reg, _dir) = temp_db();
        let peer = PeerRecord {
            peer_id: "peer-hub".into(),
            route_name: "hub".into(),
            node_id: Some("node-hub-1".into()),
            display_name: Some("Hub".into()),
            allowed_capabilities: crate::peer::PeerCapabilities::resource_read(),
            updated_ms: 1,
        };
        let connection = PeerConnectionRecord {
            connection_id: Uuid::new_v4(),
            peer_id: "peer-hub".into(),
            route_name: "hub".into(),
            direction: PeerConnectionDirection::Acceptor,
            status: crate::peer::PeerConnectionStatus::Connected,
            negotiated_capabilities: crate::peer::PeerCapabilities::resource_read(),
            updated_ms: 2,
        };

        reg.put_peer(&peer).unwrap();
        reg.put_peer_connection(&connection).unwrap();

        assert_eq!(
            reg.get_peer_by_route("hub").unwrap().unwrap().peer_id,
            "peer-hub"
        );
        assert_eq!(
            reg.latest_peer_connection("hub")
                .unwrap()
                .unwrap()
                .connection_id,
            connection.connection_id
        );
    }

    #[test]
    fn reconnect_updates_connection_without_changing_durable_peer_id() {
        let (reg, _dir) = temp_db();
        let peer = PeerRecord {
            peer_id: "peer-hub".into(),
            route_name: "hub".into(),
            node_id: Some("node-hub-1".into()),
            display_name: Some("Hub".into()),
            allowed_capabilities: crate::peer::PeerCapabilities::resource_read(),
            updated_ms: 1,
        };
        reg.put_peer(&peer).unwrap();

        let first_connection_id = Uuid::new_v4();
        reg.put_peer_connection(&PeerConnectionRecord {
            connection_id: first_connection_id,
            peer_id: "peer-hub".into(),
            route_name: "hub".into(),
            direction: PeerConnectionDirection::Acceptor,
            status: crate::peer::PeerConnectionStatus::Connected,
            negotiated_capabilities: crate::peer::PeerCapabilities::resource_read(),
            updated_ms: 2,
        })
        .unwrap();
        let second_connection_id = Uuid::new_v4();
        reg.put_peer_connection(&PeerConnectionRecord {
            connection_id: second_connection_id,
            peer_id: "peer-hub".into(),
            route_name: "hub".into(),
            direction: PeerConnectionDirection::Acceptor,
            status: crate::peer::PeerConnectionStatus::Connected,
            negotiated_capabilities: crate::peer::PeerCapabilities::resource_read(),
            updated_ms: 3,
        })
        .unwrap();

        assert_eq!(
            reg.get_peer_by_route("hub").unwrap().unwrap().peer_id,
            "peer-hub"
        );
        let latest = reg.latest_peer_connection("hub").unwrap().unwrap();
        assert_eq!(latest.connection_id, second_connection_id);
        assert_ne!(latest.connection_id, first_connection_id);
    }

    fn temp_db_with_old_peer_status_rows() -> (Registry, tempfile::TempDir) {
        use url::Url;

        #[derive(Serialize)]
        struct OldPeerStatusRow {
            id: Uuid,
            name: String,
            url: Url,
            direction: String,
            status: String,
            updated_ms: i64,
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("boardwalk.redb");
        let db = Database::create(&path).unwrap();
        let old_peers: TableDefinition<&str, &[u8]> = TableDefinition::new("peers");
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(old_peers).unwrap();
            let row = OldPeerStatusRow {
                id: Uuid::new_v4(),
                name: "hub".into(),
                url: "peer://hub/".parse().unwrap(),
                direction: "acceptor".into(),
                status: "connected".into(),
                updated_ms: 1,
            };
            let bytes = serde_json::to_vec(&row).unwrap();
            table.insert("hub", bytes.as_slice()).unwrap();
        }
        txn.commit().unwrap();
        drop(db);

        (Registry::open(&path).unwrap(), dir)
    }
}
