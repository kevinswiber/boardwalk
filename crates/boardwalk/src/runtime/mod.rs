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

pub use actor::{Actor, ActorCtx, ActorError, TransitionCtx, TransitionError};
pub use context::{CommandId, RequestCtx};
pub use directory::ResourceDirectory;
pub use executor::{ActorHandle, NodePolicy, PendingTransition};
pub use handle::{ActorProxy, NodeHandle, NodeHandleError, ResourceProxy};
pub use node::{Node, NodeBuilder};
pub use resource::{DynFuture, Resource, ResourceCtx, ResourceError};

/// Query types re-exported under `runtime::query` so apps depend on
/// the runtime, not on the HTTP layer.
pub mod query {
    pub use crate::query::{
        ComparisonOp, FieldPath, Literal, Predicate, Projection, Query, matches,
    };
}
