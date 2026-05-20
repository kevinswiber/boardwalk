//! Boardwalk — resource / actor runtime with hypermedia federation.
//!
//! Boardwalk models addressable state as [`Resource`] values, drives
//! executable state machines as [`Actor`] values inside a [`NodeBuilder`]
//! / `Node` runtime, and renders queryable [`ResourceSnapshot`] values
//! through resource routes such as `/resources`,
//! `/resources/{id}`, and `/resources/{id}/transitions/{transition}`.
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

pub mod caql;
pub mod events;
mod http;
mod peer;
pub mod query;
mod registry;
pub mod runtime;
mod server;
mod siren;
mod tunnel;

#[cfg(test)]
mod internal_adapter_tests;

pub use boardwalk_macros::{actor, transition};

pub use crate::events::SlowConsumerPolicy;
pub use crate::runtime::{
    Actor, ActorSpec, Effect, FieldSpec, Idempotency, JobHandle, Node, NodeBuilder, NodeHandle,
    Resource, ResourceKind, ResourceSnapshot, ResourceSpec, SnapshotStreamSpec, StateName,
    StreamKind, StreamSpec, TransitionAffordance, TransitionInput, TransitionName,
    TransitionOutcome, TransitionResultKind, TransitionSpec,
};
pub use crate::server::Boardwalk;
