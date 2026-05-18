//! The `Resource` trait and supporting types.
//!
//! A `Resource` is the addressable, read-side projection of state on
//! a node. It does not have to be executable; metadata, peer
//! references, and other read-only entities implement only this
//! trait. The executable variant lives in `Actor`.

use std::future::Future;
use std::pin::Pin;

use crate::core::ResourceSpec;
use crate::http::ResourceSnapshot;

/// Pinned, boxed `Future` alias used by the trait methods so the
/// signatures stay readable while still being object-safe.
pub type DynFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Per-request context passed to `Resource::snapshot`. Carries the
/// node id and any forwarded request metadata. Kept opaque so future
/// task work can attach correlation IDs without touching the trait.
#[derive(Clone, Debug, Default)]
pub struct ResourceCtx {
    // Real fields land when the `Node` runtime wires this in (Phase 3).
    _placeholder: (),
}

impl ResourceCtx {
    /// Test-only constructor used by trait-shape compile tests. Real
    /// callers build a `ResourceCtx` through the `Node` runtime.
    pub fn new_test() -> Self {
        Self::default()
    }
}

/// Read-only failure modes for `Resource::snapshot`. The HTTP boundary
/// maps these onto 404 / 503 / 500 in later phases.
#[derive(Debug)]
pub enum ResourceError {
    NotFound(String),
    Unavailable(String),
    Internal(String),
}

/// Addressable read-only projection on a node.
pub trait Resource: Send + Sync + 'static {
    /// Declarative description of the resource kind: properties
    /// schema, labels, declared streams.
    fn spec(&self) -> ResourceSpec;

    /// Current snapshot. Reads are async because the resource may
    /// live behind a runtime queue (Phase 3 wires this up).
    fn snapshot<'a>(
        &'a self,
        ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>>;
}
