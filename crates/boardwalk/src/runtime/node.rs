//! `Node` — the runtime unit for a single host process.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use uuid::Uuid;

use super::actor::{Actor, ActorCtx};
use super::context::Publisher;
use super::directory::ResourceDirectory;
use super::executor::ActorHandle;
use super::resource::{ResourceCtx, ResourceError};
use crate::events::{EventBus, StreamRegistry};
use crate::http::ResourceSnapshot;

/// Builder for a node runtime. Constructs the event bus, the shared
/// `StreamRegistry`, and the directory using the same single-registry
/// construction pattern the event-envelope work established.
pub struct NodeBuilder {
    id: String,
    actor_queue_capacity: usize,
}

impl NodeBuilder {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            actor_queue_capacity: 32,
        }
    }

    pub fn actor_queue_capacity(mut self, capacity: usize) -> Self {
        self.actor_queue_capacity = capacity.max(1);
        self
    }

    pub fn build(self) -> Node {
        let stream_registry = StreamRegistry::new();
        let bus = EventBus::with_registry(stream_registry.clone());
        Node {
            id: self.id,
            bus,
            stream_registry,
            directory: Arc::new(RwLock::new(ResourceDirectory::new())),
            actor_queue_capacity: self.actor_queue_capacity,
        }
    }
}

/// Runtime unit. One process can host several `Node`s, though the
/// boardwalk-server CLI builds exactly one.
pub struct Node {
    id: String,
    bus: EventBus,
    stream_registry: StreamRegistry,
    directory: Arc<RwLock<ResourceDirectory>>,
    actor_queue_capacity: usize,
}

impl Node {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn events(&self) -> &EventBus {
        &self.bus
    }

    pub fn stream_registry(&self) -> &StreamRegistry {
        &self.stream_registry
    }

    pub(crate) async fn directory_read(
        &self,
    ) -> tokio::sync::RwLockReadGuard<'_, ResourceDirectory> {
        self.directory.read().await
    }

    /// Register an actor and assign a fresh `ResourceId`. The
    /// registration happens under a write lock so the directory's
    /// id-uniqueness invariant is observable atomically.
    pub async fn register_actor<A: Actor>(&self, actor: A) -> Result<String, ResourceError> {
        let id = Uuid::new_v4().to_string();
        self.register_with_id(id.clone(), actor).await?;
        Ok(id)
    }

    /// Register an actor with a caller-supplied id. Returns an error
    /// if the id is already taken. The uniqueness check happens before
    /// the actor task is spawned so a duplicate id never runs
    /// `on_start` or leaks a detached task.
    pub async fn register_with_id<A: Actor>(
        &self,
        id: String,
        actor: A,
    ) -> Result<(), ResourceError> {
        let spec = actor.spec();
        let kind = spec.kind.clone();
        let labels = spec.labels.clone();
        let publisher = Publisher::new(self.bus.clone(), self.stream_registry.clone());
        let actor_ctx = ActorCtx::new(self.id.clone(), id.clone(), kind.clone(), labels)
            .with_publisher(publisher);

        // Hold the write lock across spawn so the uniqueness check and
        // the entry insertion are atomic. Spawning is cheap (channel +
        // task creation) so this doesn't block other registrations
        // meaningfully.
        let mut dir = self.directory.write().await;
        if dir.contains_id(&id) {
            return Err(ResourceError::Internal(format!(
                "duplicate resource id: {id}"
            )));
        }
        let (handle, task) =
            ActorHandle::spawn_with_task(actor, self.actor_queue_capacity, actor_ctx);
        let slot = super::executor::ActorSlot { handle, task };
        dir.insert(id, kind, slot)
    }

    /// Returns the current set of resource snapshots in registration
    /// order. Each snapshot's `id`/`kind`/`node` are sourced from the
    /// directory entry rather than the actor's own report.
    pub async fn resources(&self) -> Vec<ResourceSnapshot> {
        let entries = {
            let dir = self.directory.read().await;
            dir.entries().to_vec()
        };
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            let ctx = ResourceCtx::new_test();
            if let Ok(snap) = entry.snapshot(ctx, &self.id).await {
                out.push(snap);
            }
        }
        out
    }

    /// Stop every actor under this node. Each actor receives
    /// `on_stop` and then its task is joined. `within` bounds how
    /// long the node will wait for each actor; tasks that have not
    /// exited by then are aborted.
    pub async fn shutdown(&self, within: Duration) {
        let entries = {
            let dir = self.directory.read().await;
            dir.entries().to_vec()
        };
        for entry in entries {
            let _ = entry.handle.shutdown(within).await;
            let mut task_slot = entry.task.lock().await;
            if let Some(task) = task_slot.take() {
                let abort_handle = task.abort_handle();
                if tokio::time::timeout(within, task).await.is_err() {
                    // Timed out waiting for the task to exit on its
                    // own; force-abort so the task is actually gone
                    // (dropping a JoinHandle detaches but does not
                    // abort).
                    abort_handle.abort();
                }
            }
        }
    }
}
