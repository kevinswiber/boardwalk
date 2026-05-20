//! Runtime model: the `Resource` / `Actor` split, contexts, the
//! `Node` unit, the resource directory, the bounded-execution
//! `ActorHandle`, and the app-facing `NodeHandle`/`ResourceProxy`
//! surface.

#![forbid(unsafe_code)]

mod actor;
mod context;
mod directory;
mod executor;
mod handle;
mod node;
mod resource;
mod transition;

pub use actor::{Actor, ActorCtx, ActorError, TransitionCtx, TransitionError};
pub use context::{CommandId, RequestCtx};
pub(crate) use context::{EmissionContext, EnvelopePlan, publish_envelope};
pub use directory::ResourceDirectory;
pub use executor::{ActorHandle, NodePolicy, PendingTransition};
pub use handle::{ActorProxy, NodeHandle, NodeHandleError, ResourceProxy};
pub use node::{Node, NodeBuilder};
pub use resource::{
    DynFuture, RESERVED_FIELDS, Resource, ResourceCtx, ResourceError, ResourceSnapshot,
    SnapshotStreamSpec, TransitionAffordance, sanitize_properties,
};
pub use transition::{
    ActorSpec, Effect, FieldSpec, Idempotency, JobHandle, ResourceKind, ResourceSpec, StateName,
    StreamKind, StreamSpec, TransitionInput, TransitionName, TransitionOutcome,
    TransitionResultKind, TransitionSpec,
};

/// Query types re-exported under `runtime::query` so apps depend on
/// the runtime, not on the HTTP layer.
pub mod query {
    pub use crate::query::{
        ComparisonOp, FieldPath, Literal, Predicate, Projection, Query, matches,
    };
}
