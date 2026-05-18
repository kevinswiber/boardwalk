//! Owned registry of resources and actors on a single node.

use std::collections::HashMap;
use std::sync::Arc;

use super::executor::{ActorHandle, ActorSlot};
use super::resource::{ResourceCtx, ResourceError};
use crate::http::ResourceSnapshot;

/// One registered entry in the directory. Holds the live actor task
/// handle so the node can shut it down deterministically.
pub(crate) struct Entry {
    pub id: String,
    pub kind: String,
    pub handle: ActorHandle,
    pub task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// In-memory registry of resources/actors hosted on a node. Order of
/// registration is preserved so the HTTP layer can render listings
/// deterministically.
#[derive(Default)]
pub struct ResourceDirectory {
    entries: Vec<Arc<Entry>>,
    by_id: HashMap<String, usize>,
}

impl ResourceDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn entries(&self) -> &[Arc<Entry>] {
        &self.entries
    }

    #[allow(dead_code)] // used by phase 5 resource routes
    pub(crate) fn get_by_id(&self, id: &str) -> Option<Arc<Entry>> {
        self.by_id.get(id).map(|i| self.entries[*i].clone())
    }

    /// Insert a fully-formed entry. Returns an error if the id is
    /// already taken.
    pub(crate) fn insert(
        &mut self,
        id: String,
        kind: String,
        slot: ActorSlot,
    ) -> Result<(), ResourceError> {
        if self.by_id.contains_key(&id) {
            return Err(ResourceError::Internal(format!(
                "duplicate resource id: {id}"
            )));
        }
        let entry = Arc::new(Entry {
            id: id.clone(),
            kind,
            handle: slot.handle,
            task: tokio::sync::Mutex::new(Some(slot.task)),
        });
        self.by_id.insert(id, self.entries.len());
        self.entries.push(entry);
        Ok(())
    }
}

impl Entry {
    /// Send a `Snapshot` command to the actor task and await the
    /// reply. The returned snapshot's `id`, `kind`, and `node` are
    /// overwritten from the directory entry so each actor does not
    /// need to remember its own identity.
    pub(crate) async fn snapshot(
        &self,
        ctx: ResourceCtx,
        node: &str,
    ) -> Result<ResourceSnapshot, ResourceError> {
        let mut snap = self.handle.snapshot(ctx).await?;
        snap.id = self.id.clone();
        snap.kind = self.kind.clone();
        snap.node = node.to_string();
        Ok(snap)
    }
}
