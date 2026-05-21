use std::sync::Arc;

use crate::events::{EventBus, SubscribeOpts, Subscription, SubscriptionId, TopicPattern};
use crate::registry::Registry;
use crate::runtime::{
    ActorSpec, Node, NodeHandle, RequestCtx, ResourceError, ResourceSnapshot, ResourceSnapshotRead,
    TransitionCtx, TransitionError, TransitionInput, TransitionOutcome,
};

/// Runtime owned by the HTTP layer and shared with peer/stream routes.
pub struct Core {
    pub name: String,
    pub bus: EventBus,
    node: Arc<Node>,
    registry: Option<Arc<Registry>>,
}

#[derive(Debug)]
pub(crate) enum ResourceReadError {
    NotFound,
    Unavailable(String),
    Internal(String),
}

#[derive(Debug)]
pub(crate) enum ResourceTransitionError {
    NotFound,
    InvalidInput(String),
    NotAllowed(String),
    Conflict(String),
    Busy,
    BackpressureRequired,
    Timeout,
    Unavailable(String),
    Internal(String),
}

impl Core {
    #[allow(dead_code)]
    pub fn from_node(node: Arc<Node>) -> Arc<Self> {
        Self::from_node_with_name(node.id().to_string(), node)
    }

    pub(crate) fn from_node_with_name(name: impl Into<String>, node: Arc<Node>) -> Arc<Self> {
        Self::from_node_with_name_and_registry(name, node, None)
    }

    pub(crate) fn from_node_with_name_and_registry(
        name: impl Into<String>,
        node: Arc<Node>,
        registry: Option<Arc<Registry>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            bus: node.events().clone(),
            node,
            registry,
        })
    }

    pub async fn list_resources(&self) -> Vec<ResourceSnapshot> {
        self.node
            .resource_snapshot_reads()
            .await
            .into_iter()
            .filter_map(|read| match read {
                ResourceSnapshotRead::Available(snapshot) => {
                    self.persist_latest_resource_snapshot(&snapshot);
                    Some(snapshot)
                }
                ResourceSnapshotRead::Unavailable {
                    resource_id,
                    placeholder,
                } => self
                    .latest_resource_snapshot(&resource_id)
                    .ok()
                    .flatten()
                    .or(Some(placeholder)),
                ResourceSnapshotRead::Failed => None,
            })
            .collect()
    }

    pub async fn query_resources(
        &self,
        ql: &str,
    ) -> Result<Vec<ResourceSnapshot>, crate::query::QueryError> {
        let query = crate::caql::parse(ql)?;
        let mut matches = Vec::new();
        for snapshot in self.list_resources().await {
            if crate::query::matches(&query, &snapshot.to_query_value())? {
                matches.push(snapshot);
            }
        }
        Ok(matches)
    }

    pub async fn get_resource(
        &self,
        id: &str,
    ) -> Result<Option<ResourceSnapshot>, ResourceReadError> {
        match self.node.resource_snapshot(id).await {
            Ok(Some(snapshot)) => {
                self.persist_latest_resource_snapshot(&snapshot);
                Ok(Some(snapshot))
            }
            Ok(None) => Ok(None),
            Err(ResourceError::Unavailable(message)) => {
                self.snapshot_from_repository_or_unavailable(id, message)
            }
            Err(err) => Err(resource_read_error(err)),
        }
    }

    pub async fn actor_specs(&self) -> Vec<ActorSpec> {
        self.node.actor_specs().await
    }

    pub(crate) fn subscribe_events(
        &self,
        pattern: TopicPattern,
        opts: SubscribeOpts,
    ) -> Subscription {
        self.bus.subscribe(pattern, opts)
    }

    pub(crate) fn unsubscribe_events(&self, id: SubscriptionId) -> bool {
        self.bus.unsubscribe(id)
    }

    pub async fn run_resource_transition(
        &self,
        id: &str,
        name: &str,
        input: TransitionInput,
        request: RequestCtx,
    ) -> Result<TransitionOutcome, ResourceTransitionError> {
        let handle = NodeHandle::new(self.node.clone());
        let Some(proxy) = handle.resource(id).await else {
            return Err(ResourceTransitionError::NotFound);
        };

        let ctx = TransitionCtx::with_node(request, self.node.clone());
        let outcome = proxy
            .transition_with_ctx(ctx, name, input)
            .await
            .map_err(resource_transition_error)?;
        match outcome {
            TransitionOutcome::Completed { output, .. } => {
                let snapshot = proxy
                    .snapshot()
                    .await
                    .map_err(resource_error_to_transition)?;
                self.persist_latest_resource_snapshot(&snapshot);
                Ok(TransitionOutcome::Completed { output, snapshot })
            }
            TransitionOutcome::Accepted { .. } => Ok(outcome),
        }
    }

    fn snapshot_from_repository_or_unavailable(
        &self,
        id: &str,
        message: String,
    ) -> Result<Option<ResourceSnapshot>, ResourceReadError> {
        match self.latest_resource_snapshot(id) {
            Ok(Some(snapshot)) => Ok(Some(snapshot)),
            Ok(None) => Err(ResourceReadError::Unavailable(message)),
            Err(err) => Err(ResourceReadError::Internal(err)),
        }
    }

    fn persist_latest_resource_snapshot(&self, snapshot: &ResourceSnapshot) {
        if let Some(registry) = self.registry.as_ref()
            && let Err(err) = registry.put_latest_resource_snapshot(snapshot)
        {
            tracing::warn!(error = %err, resource_id = %snapshot.id, "failed to persist latest resource snapshot");
        }
    }

    fn latest_resource_snapshot(
        &self,
        resource_id: &str,
    ) -> Result<Option<ResourceSnapshot>, String> {
        let Some(registry) = self.registry.as_ref() else {
            return Ok(None);
        };
        registry
            .latest_resource_snapshot(resource_id)
            .map_err(|err| {
                tracing::warn!(error = %err, resource_id, "failed to read latest resource snapshot");
                "storage unavailable".to_string()
            })
    }
}

fn resource_read_error(err: ResourceError) -> ResourceReadError {
    match err {
        ResourceError::NotFound(_) => ResourceReadError::NotFound,
        ResourceError::Unavailable(msg) => ResourceReadError::Unavailable(msg),
        ResourceError::Internal(msg) => ResourceReadError::Internal(msg),
    }
}

fn resource_error_to_transition(err: ResourceError) -> ResourceTransitionError {
    match err {
        ResourceError::NotFound(_) => ResourceTransitionError::NotFound,
        ResourceError::Unavailable(msg) => ResourceTransitionError::Unavailable(msg),
        ResourceError::Internal(msg) => ResourceTransitionError::Internal(msg),
    }
}

fn resource_transition_error(err: TransitionError) -> ResourceTransitionError {
    match err {
        TransitionError::InvalidInput(msg) => ResourceTransitionError::InvalidInput(msg),
        TransitionError::NotAllowed(msg) => ResourceTransitionError::NotAllowed(msg),
        TransitionError::Conflict(msg) => ResourceTransitionError::Conflict(msg),
        TransitionError::Busy => ResourceTransitionError::Busy,
        TransitionError::BackpressureRequired => ResourceTransitionError::BackpressureRequired,
        TransitionError::Timeout => ResourceTransitionError::Timeout,
        TransitionError::ResourceNotFound(_) => ResourceTransitionError::NotFound,
        TransitionError::Internal(msg) => ResourceTransitionError::Internal(msg),
    }
}

pub(crate) fn now_ms() -> i64 {
    use time::OffsetDateTime;
    (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64
}
