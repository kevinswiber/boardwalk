//! Boardwalk — resource / actor runtime with hypermedia federation.
//!
//! Boardwalk models addressable state as [`Resource`] values, drives
//! executable state machines as [`Actor`] values inside a [`NodeBuilder`]
//! / `Node` runtime, and renders queryable [`ResourceSnapshot`] values
//! through resource routes such as `/resources`,
//! `/resources/{id}`, and `/resources/{id}/transitions/{transition}`.
//! `Boardwalk::new().use_actor(...).listen(...)` assembles that `Node`
//! with the reusable HTTP, WebSocket, and peer route stack. Use
//! [`NodeBuilder`] directly when an application wants the in-process
//! Resource/Actor runtime without starting Boardwalk's HTTP server.
//!
//! Common imports for the Resource/Actor surface:
//!
//! ```rust
//! use boardwalk::{
//!     Actor, NodeBuilder, Resource, ResourceSnapshot, SlowConsumerPolicy,
//!     TransitionInput, TransitionOutcome,
//! };
//! ```
//!
//! The `examples/job-runner` package shows async transition acceptance
//! with job resources, explicit actor event publishing, and
//! stream-specific slow-consumer policy choices. See
//! [the README](https://github.com/kevinswiber/boardwalk) for an
//! introduction.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod caql;
pub mod events;
mod http;
pub mod links;
mod peer;
mod persistence;
pub mod query;
pub mod runtime;
mod secret;
mod server;
mod siren;
mod tunnel;

#[cfg(test)]
mod internal_runtime_tests;

pub use boardwalk_macros::{actor, on_start, on_stop, transition};

pub use crate::events::SlowConsumerPolicy;
pub use crate::peer::{PeerAdmission, PeerCapability, PeerConfigError, PeerLink};
pub use crate::runtime::{
    AcceptedJob, Actor, ActorSpec, AdmittedPeer, CallerProvenance, Effect, FieldSpec, Idempotency,
    Node, NodeBuilder, NodeHandle, Resource, ResourceKind, ResourceSnapshot,
    ResourceSnapshotBuilder, ResourceSpec, SnapshotStreamSpec, StateName, StreamKind, StreamSpec,
    TransitionAffordance, TransitionInput, TransitionName, TransitionOutcome, TransitionResultKind,
    TransitionSpec,
};
pub use crate::server::Boardwalk;
pub use crate::tunnel::PEER_TOKEN_ID_HEADER;

pub mod prelude {
    //! One-stop authoring import for `Resource`/`Actor` implementors.

    pub use boardwalk_macros::{actor, on_start, on_stop, transition};

    pub use crate::runtime::{
        AcceptedJob, Actor, ActorCtx, ActorError, ActorSpec, AdmittedPeer, CallerProvenance,
        DynFuture, Effect, FieldSpec, Idempotency, Resource, ResourceCtx, ResourceError,
        ResourceKind, ResourceSnapshot, ResourceSnapshotBuilder, ResourceSpec, SnapshotStreamSpec,
        StateName, StreamKind, StreamSpec, TransitionAffordance, TransitionCtx, TransitionError,
        TransitionInput, TransitionName, TransitionOutcome, TransitionResultKind, TransitionSpec,
    };
}
