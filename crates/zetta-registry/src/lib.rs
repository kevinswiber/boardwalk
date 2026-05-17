//! Persistent registries for devices and peers.
//!
//! Full redb wiring lands in M5; this file shapes the API.

#![forbid(unsafe_code)]

use std::path::PathBuf;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceRecord {
    pub id: Uuid,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerDirection { Initiator, Acceptor }

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PeerStatus { Connecting, Connected, Disconnected, Failed }

/// Configuration for opening a Zetta registry.
pub struct Config {
    pub root: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self { root: PathBuf::from(".zetta") }
    }
}

pub struct Registry {
    _cfg: Config,
}

impl Registry {
    pub fn open(cfg: Config) -> Result<Self, RegistryError> {
        Ok(Self { _cfg: cfg })
    }
}
