//! Boardwalk — resource / actor runtime with hypermedia federation.
//!
//! Boardwalk models addressable state as [`Resource`] values, drives
//! executable state machines as [`Actor`] values inside a [`NodeBuilder`]
//! / `Node` runtime, and renders queryable [`ResourceSnapshot`] values
//! through resource routes such as `/resources`,
//! `/resources/{id}`, and `/resources/{id}/transitions/{transition}`.
//!
//! The crate root still includes older server-adapter exports during
//! the API transition. New Resource/Actor code should use the module
//! imports below until the crate-root facade is narrowed around the new
//! runtime vocabulary.
//!
//! Common imports for the Resource/Actor surface:
//!
//! ```rust
//! use boardwalk::core::{TransitionInput, TransitionOutcome};
//! use boardwalk::events::SlowConsumerPolicy;
//! use boardwalk::http::ResourceSnapshot;
//! use boardwalk::runtime::{Actor, NodeBuilder, Resource};
//! ```
//!
//! The `examples/job-runner` package shows async transition acceptance
//! with job resources, explicit actor event publishing, and
//! stream-specific slow-consumer policy choices. See
//! [the README](https://github.com/kevinswiber/boardwalk) for an
//! introduction.

#![forbid(unsafe_code)]

pub mod caql;
pub mod core;
pub mod events;
pub mod http;
pub mod peer;
pub mod query;
pub mod registry;
pub mod runtime;
pub mod server;
pub mod siren;
pub mod tunnel;

pub use boardwalk_macros::{actor, device, transition};

// Existing crate-root re-exports while the Resource/Actor facade settles.
pub use crate::core::{Device, DeviceConfig, DeviceError, TransitionInput};
pub use crate::http::{App, AppError, DeviceProxy, Scout, ScoutCtx, ServerHandle};
pub use crate::server::Boardwalk;
