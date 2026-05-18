//! App-facing handles into a `Node`: `NodeHandle`, `ResourceProxy`,
//! `ActorProxy`. Apps query the node by CaQL, then read snapshots or
//! invoke transitions on the returned proxies. None of this surface
//! mentions devices.

use std::sync::Arc;

use super::actor::{TransitionCtx, TransitionError};
use super::context::RequestCtx;
use super::directory::Entry;
use super::node::Node;
use super::resource::{ResourceCtx, ResourceError};
use crate::core::{TransitionInput, TransitionOutcome};
use crate::http::ResourceSnapshot;
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

    /// Parse a CaQL string and return one `ResourceProxy` per
    /// matching resource. Invalid CaQL surfaces as `Err`.
    pub async fn query(&self, ql: &str) -> Result<Vec<ResourceProxy>, NodeHandleError> {
        let parsed =
            crate::caql::parse(ql).map_err(|e| NodeHandleError::QueryParse(e.to_string()))?;
        let dir = self.node.directory_read().await;
        let mut matches = Vec::new();
        for entry in dir.entries() {
            let ctx = ResourceCtx::new_test();
            let snap = match entry.snapshot(ctx, self.node.id()).await {
                Ok(snap) => snap,
                Err(_) => continue,
            };
            let v = snap.to_query_value();
            if query_eval::matches(&parsed, &v).unwrap_or(false) {
                matches.push(ResourceProxy {
                    entry: entry.clone(),
                    node: self.node.clone(),
                });
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
}

/// Alias kept for symmetry with the trait split: an `ActorProxy` is a
/// `ResourceProxy` known to point at an actor. The runtime exposes
/// only this convenience type today.
pub type ActorProxy = ResourceProxy;
