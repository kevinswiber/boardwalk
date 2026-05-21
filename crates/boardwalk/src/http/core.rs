use std::sync::Arc;

use crate::events::{EventBus, SubscribeOpts, Subscription, SubscriptionId, TopicPattern};
use crate::runtime::{
    ActorSpec, Node, NodeHandle, RequestCtx, ResourceError, ResourceSnapshot, TransitionCtx,
    TransitionError, TransitionInput, TransitionOutcome,
};

/// Runtime owned by the HTTP layer and shared with peer/stream routes.
pub struct Core {
    pub name: String,
    pub bus: EventBus,
    node: Arc<Node>,
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
        Arc::new(Self {
            name: name.into(),
            bus: node.events().clone(),
            node,
        })
    }

    pub async fn list_resources(&self) -> Vec<ResourceSnapshot> {
        self.node.resources().await
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
        let handle = NodeHandle::new(self.node.clone());
        let Some(proxy) = handle.resource(id).await else {
            return Ok(None);
        };
        proxy
            .snapshot()
            .await
            .map(Some)
            .map_err(resource_read_error)
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
                Ok(TransitionOutcome::Completed { output, snapshot })
            }
            TransitionOutcome::Accepted { .. } => Ok(outcome),
        }
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
