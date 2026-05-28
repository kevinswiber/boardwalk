//! `Node` — the runtime unit for a single host process.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use uuid::Uuid;

use super::actor::{Actor, ActorCtx};
use super::context::Publisher;
use super::directory::ResourceDirectory;
use super::executor::{ActorHandle, ActorSlot};
use super::resource::{ResourceCtx, ResourceError, ResourceSnapshot};
use super::transition::{ActorSpec, ResourceSpec};
use crate::events::{EventBus, StreamRegistry};

pub(crate) enum ResourceSnapshotRead {
    Available(ResourceSnapshot),
    Unavailable {
        resource_id: String,
        placeholder: ResourceSnapshot,
    },
    Failed,
}

/// Builder for a node runtime. Constructs the event bus, the shared
/// `StreamRegistry`, and the directory using the same single-registry
/// construction pattern the event-envelope work established.
pub struct NodeBuilder {
    id: String,
    actor_queue_capacity: usize,
    pending_actors: Vec<PendingActor>,
}

impl NodeBuilder {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            actor_queue_capacity: 32,
            pending_actors: Vec::new(),
        }
    }

    pub fn actor_queue_capacity(mut self, capacity: usize) -> Self {
        self.actor_queue_capacity = capacity.max(1);
        self
    }

    pub fn register_actor<A: Actor>(self, actor: A) -> Self {
        let id = Uuid::new_v4().to_string();
        self.register_with_id(id, actor)
            .unwrap_or_else(|err| panic!("generated actor id must be unique: {err:?}"))
    }

    pub fn register_with_id<A: Actor>(
        mut self,
        id: impl Into<String>,
        actor: A,
    ) -> Result<Self, ResourceError> {
        let id = id.into();
        if self.pending_actors.iter().any(|pending| pending.id == id) {
            return Err(ResourceError::Internal(format!(
                "duplicate resource id: {id}"
            )));
        }
        self.pending_actors.push(PendingActor::new(id, actor));
        Ok(self)
    }

    /// Build the node, panicking if pending actor registration fails.
    ///
    /// Use [`NodeBuilder::try_build`] when the caller needs to handle
    /// registration failures explicitly.
    pub fn build(self) -> Node {
        self.try_build()
            .unwrap_or_else(|err| panic!("NodeBuilder::build failed: {err:?}"))
    }

    /// Build the node and return any pending actor registration error.
    pub fn try_build(self) -> Result<Node, ResourceError> {
        let stream_registry = StreamRegistry::new();
        let bus = EventBus::with_registry(stream_registry.clone());
        let mut directory = ResourceDirectory::new();
        for pending in self.pending_actors {
            (pending.register)(
                &mut directory,
                &self.id,
                &bus,
                &stream_registry,
                self.actor_queue_capacity,
            )?;
        }
        Ok(Node {
            id: self.id,
            bus,
            stream_registry,
            directory: Arc::new(RwLock::new(directory)),
            actor_queue_capacity: self.actor_queue_capacity,
        })
    }
}

type RegisterPendingActor = Box<
    dyn FnOnce(
            &mut ResourceDirectory,
            &str,
            &EventBus,
            &StreamRegistry,
            usize,
        ) -> Result<(), ResourceError>
        + Send,
>;

struct PendingActor {
    id: String,
    register: RegisterPendingActor,
}

impl PendingActor {
    fn new<A: Actor>(id: String, actor: A) -> Self {
        let register_id = id.clone();
        let register = Box::new(
            move |directory: &mut ResourceDirectory,
                  node_id: &str,
                  bus: &EventBus,
                  stream_registry: &StreamRegistry,
                  actor_queue_capacity: usize| {
                if directory.contains_id(&register_id) {
                    return Err(ResourceError::Internal(format!(
                        "duplicate resource id: {register_id}"
                    )));
                }
                let (kind, slot, spec) = spawn_actor_entry(
                    actor,
                    node_id,
                    register_id.clone(),
                    bus,
                    stream_registry,
                    actor_queue_capacity,
                );
                directory.insert(register_id, kind, slot, spec)
            },
        );
        Self { id, register }
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
        let (kind, slot, spec) = spawn_actor_entry(
            actor,
            &self.id,
            id.clone(),
            &self.bus,
            &self.stream_registry,
            self.actor_queue_capacity,
        );
        dir.insert(id, kind, slot, spec)
    }

    /// Returns the current set of resource snapshots in registration
    /// order. Each snapshot's `id`/`kind`/`node` are sourced from the
    /// directory entry rather than the actor's own report.
    pub async fn resources(&self) -> Vec<ResourceSnapshot> {
        self.resource_snapshot_reads()
            .await
            .into_iter()
            .filter_map(|read| match read {
                ResourceSnapshotRead::Available(snapshot) => Some(snapshot),
                ResourceSnapshotRead::Unavailable { placeholder, .. } => Some(placeholder),
                ResourceSnapshotRead::Failed => None,
            })
            .collect()
    }

    pub(crate) async fn resource_snapshot_reads(&self) -> Vec<ResourceSnapshotRead> {
        let entries = {
            let dir = self.directory.read().await;
            dir.entries().to_vec()
        };
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            let ctx = ResourceCtx::new_test();
            out.push(match entry.snapshot(ctx, &self.id).await {
                Ok(snapshot) => ResourceSnapshotRead::Available(snapshot),
                Err(ResourceError::Unavailable(_)) => ResourceSnapshotRead::Unavailable {
                    resource_id: entry.id.clone(),
                    placeholder: entry.unavailable_snapshot(&self.id),
                },
                Err(_) => ResourceSnapshotRead::Failed,
            });
        }
        out
    }

    pub(crate) async fn resource_snapshot(
        &self,
        id: &str,
    ) -> Result<Option<ResourceSnapshot>, ResourceError> {
        let entry = {
            let dir = self.directory.read().await;
            dir.get_by_id(id)
        };
        let Some(entry) = entry else {
            return Ok(None);
        };
        let ctx = ResourceCtx::new_test();
        entry.snapshot(ctx, &self.id).await.map(Some)
    }

    pub(crate) async fn actor_specs(&self) -> Vec<ActorSpec> {
        let entries = {
            let dir = self.directory.read().await;
            dir.entries().to_vec()
        };
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            let ctx = ResourceCtx::new_test();
            if let Ok(spec) = entry.actor_spec(ctx, &self.id).await {
                out.push(spec);
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

fn spawn_actor_entry<A: Actor>(
    actor: A,
    node_id: &str,
    id: String,
    bus: &EventBus,
    stream_registry: &StreamRegistry,
    actor_queue_capacity: usize,
) -> (String, ActorSlot, ResourceSpec) {
    let spec = actor.spec();
    let kind = spec.kind.clone();
    let labels = spec.labels.clone();
    let publisher = Publisher::new(bus.clone(), stream_registry.clone());
    let actor_ctx =
        ActorCtx::new(node_id.to_string(), id, kind.clone(), labels).with_publisher(publisher);
    let (handle, task) = ActorHandle::spawn_with_task(actor, actor_queue_capacity, actor_ctx);
    let slot = ActorSlot { handle, task };
    (kind, slot, spec)
}
