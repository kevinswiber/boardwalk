//! Persistent registries for devices and peers, backed by redb.

#![forbid(unsafe_code)]
// redb's error variants are intentionally large (rich diagnostic info).
// Registry calls aren't on hot paths, so the stack cost is fine.
#![allow(clippy::result_large_err)]

use std::path::{Path, PathBuf};

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

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
pub struct DeviceRecord {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub type_: String,
    pub name: Option<String>,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
    pub id: Uuid,
    pub name: String,
    pub url: Url,
    pub direction: PeerDirection,
    pub status: PeerStatus,
    pub updated_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerDirection {
    Initiator,
    Acceptor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PeerStatus {
    Connecting,
    Connected,
    Disconnected,
    Failed,
}

const DEVICES: TableDefinition<&str, &[u8]> = TableDefinition::new("devices");
const PEERS: TableDefinition<&str, &[u8]> = TableDefinition::new("peers");

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
        txn.open_table(DEVICES)?;
        txn.open_table(PEERS)?;
        txn.commit()?;
        Ok(Self { db })
    }

    // -- devices ----------------------------------------------------------

    pub fn put_device(&self, rec: &DeviceRecord) -> Result<(), RegistryError> {
        let bytes = serde_json::to_vec(rec)?;
        let txn = self.db.begin_write()?;
        {
            let mut t = txn.open_table(DEVICES)?;
            t.insert(rec.id.to_string().as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn get_device(&self, id: &Uuid) -> Result<Option<DeviceRecord>, RegistryError> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(DEVICES)?;
        match t.get(id.to_string().as_str())? {
            Some(av) => Ok(Some(serde_json::from_slice(av.value())?)),
            None => Ok(None),
        }
    }

    pub fn list_devices(&self) -> Result<Vec<DeviceRecord>, RegistryError> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(DEVICES)?;
        let mut out = Vec::new();
        for item in t.iter()? {
            let (_, av) = item?;
            out.push(serde_json::from_slice(av.value())?);
        }
        Ok(out)
    }

    pub fn delete_device(&self, id: &Uuid) -> Result<bool, RegistryError> {
        let txn = self.db.begin_write()?;
        let removed = {
            let mut t = txn.open_table(DEVICES)?;
            t.remove(id.to_string().as_str())?.is_some()
        };
        txn.commit()?;
        Ok(removed)
    }

    /// Find an existing device by (type, name) identity. Returns the
    /// first match. Used at boot to restore stable device IDs.
    pub fn find_device_by_identity(
        &self,
        type_: &str,
        name: Option<&str>,
    ) -> Result<Option<DeviceRecord>, RegistryError> {
        for rec in self.list_devices()? {
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
            t.insert(rec.name.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn get_peer(&self, name: &str) -> Result<Option<PeerRecord>, RegistryError> {
        let txn = self.db.begin_read()?;
        let t = txn.open_table(PEERS)?;
        match t.get(name)? {
            Some(av) => Ok(Some(serde_json::from_slice(av.value())?)),
            None => Ok(None),
        }
    }

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

    pub fn delete_peer(&self, name: &str) -> Result<bool, RegistryError> {
        let txn = self.db.begin_write()?;
        let removed = {
            let mut t = txn.open_table(PEERS)?;
            t.remove(name)?.is_some()
        };
        txn.commit()?;
        Ok(removed)
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
    fn devices_round_trip() {
        let (reg, _dir) = temp_db();
        let id = Uuid::new_v4();
        let rec = DeviceRecord {
            id,
            type_: "led".into(),
            name: Some("L".into()),
            properties: Map::new(),
        };
        reg.put_device(&rec).unwrap();
        let got = reg.get_device(&id).unwrap().unwrap();
        assert_eq!(got.id, id);
        assert_eq!(got.type_, "led");
        assert_eq!(reg.list_devices().unwrap().len(), 1);
        assert!(reg.delete_device(&id).unwrap());
        assert!(reg.get_device(&id).unwrap().is_none());
    }

    #[test]
    fn peers_round_trip() {
        let (reg, _dir) = temp_db();
        let rec = PeerRecord {
            id: Uuid::new_v4(),
            name: "cloud".into(),
            url: "http://example.com/".parse().unwrap(),
            direction: PeerDirection::Initiator,
            status: PeerStatus::Connecting,
            updated_ms: 0,
        };
        reg.put_peer(&rec).unwrap();
        let got = reg.get_peer("cloud").unwrap().unwrap();
        assert_eq!(got.url.as_str(), "http://example.com/");
        assert_eq!(reg.list_peers().unwrap().len(), 1);
    }
}
