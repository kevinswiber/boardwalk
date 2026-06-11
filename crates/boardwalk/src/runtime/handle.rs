//! App-facing handles into a `Node`: `NodeHandle`, `ResourceProxy`,
//! `ActorProxy`. Apps query the node by CaQL, then read snapshots or
//! invoke transitions on the returned proxies. This surface uses only
//! Resource/Actor/Node vocabulary.

// missing_docs: this module predates the crate-wide gate; its public
// items still need a documentation sweep (tracked follow-up). New code
// here should be documented anyway.
#![allow(missing_docs)]
use std::sync::Arc;

use super::actor::{TransitionCtx, TransitionError};
use super::context::RequestCtx;
use super::directory::Entry;
use super::node::Node;
use super::resource::{ResourceCtx, ResourceError, ResourceSnapshot, TransitionOutcome};
use super::transition::TransitionInput;
use crate::query::{self as query_eval};

/// Cloneable handle into a node from app/scout code.
#[derive(Clone)]
pub struct NodeHandle {
    node: Arc<Node>,
}

impl NodeHandle {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }

    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub(crate) async fn resource(&self, id: &str) -> Option<ResourceProxy> {
        let entry = {
            let dir = self.node.directory_read().await;
            dir.get_by_id(id)
        }?;
        Some(ResourceProxy::new(entry, self.node.clone()))
    }

    /// Parse a CaQL string and return one `ResourceProxy` per
    /// matching resource. Invalid CaQL surfaces as `Err`.
    pub async fn query(&self, ql: &str) -> Result<Vec<ResourceProxy>, NodeHandleError> {
        let parsed =
            crate::caql::parse(ql).map_err(|e| NodeHandleError::QueryParse(e.to_string()))?;
        // Snapshot the entry list under the read lock, then release
        // the lock before awaiting any actor snapshots. Holding the
        // directory lock across `entry.snapshot(...).await` would let
        // a slow actor block new registrations.
        let entries = {
            let dir = self.node.directory_read().await;
            dir.entries().to_vec()
        };
        let mut matches = Vec::new();
        for entry in entries {
            let ctx = ResourceCtx::new_test();
            let snap = match entry.snapshot(ctx, self.node.id()).await {
                Ok(snap) => snap,
                Err(_) => continue,
            };
            let v = snap.to_query_value();
            if query_eval::matches(&parsed, &v).unwrap_or(false) {
                matches.push(ResourceProxy::new(entry.clone(), self.node.clone()));
            }
        }
        Ok(matches)
    }
}

/// Errors from the node-side app handle. Distinct from the transition
/// error model because handle-level failures (query parse) carry no
/// per-transition causation.
#[derive(Debug)]
pub enum NodeHandleError {
    QueryParse(String),
    Internal(String),
}

/// Read + execute proxy onto one registered resource. Because every
/// directory entry is backed by an actor, `transition` is always
/// available; read-only kinds return `TransitionError::NotAllowed`.
#[derive(Clone)]
pub struct ResourceProxy {
    entry: Arc<Entry>,
    node: Arc<Node>,
}

impl ResourceProxy {
    pub(crate) fn new(entry: Arc<Entry>, node: Arc<Node>) -> Self {
        Self { entry, node }
    }

    pub fn id(&self) -> &str {
        &self.entry.id
    }

    pub fn kind(&self) -> &str {
        &self.entry.kind
    }

    pub async fn snapshot(&self) -> Result<ResourceSnapshot, ResourceError> {
        let ctx = ResourceCtx::new_test();
        self.entry.snapshot(ctx, self.node.id()).await
    }

    pub async fn transition(
        &self,
        name: &str,
        input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        let ctx = TransitionCtx::with_node(RequestCtx::default(), self.node.clone());
        self.entry
            .handle
            .transition_with_ctx(ctx, name, input)
            .await
    }

    /// Variant of `transition` that lets callers carry their own
    /// `TransitionCtx`. Apps that have already lifted `RequestCtx`
    /// from an inbound request (e.g. the HTTP boundary) use this so
    /// emitted envelopes pick up the request's `traceparent` /
    /// `x-request-id` and the context's minted `CommandId`.
    pub async fn transition_with_ctx(
        &self,
        ctx: TransitionCtx,
        name: &str,
        input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        self.entry
            .handle
            .transition_with_ctx(ctx, name, input)
            .await
    }
}

/// Alias kept for symmetry with the trait split: an `ActorProxy` is a
/// `ResourceProxy` known to point at an actor. The runtime exposes
/// only this convenience type today.
pub type ActorProxy = ResourceProxy;
