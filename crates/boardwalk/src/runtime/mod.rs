//! Runtime model: the `Resource` / `Actor` split, contexts, and
//! typed errors that the HTTP and event layers compose with.
//!
//! Later phases of the resource/actor refactor move the `Node`
//! runtime and resource directory into this module. This file
//! currently only exposes the trait + context surface.

#![forbid(unsafe_code)]

mod actor;
mod context;
mod resource;

pub use actor::{Actor, ActorCtx, ActorError, TransitionCtx, TransitionError};
pub use context::{CommandId, RequestCtx};
pub use resource::{DynFuture, Resource, ResourceCtx, ResourceError};
