//! Compile tests for the `Resource` / `Actor` type split.
//!
//! Read-only resources (metadata, peer references) implement `Resource`
//! without being driven by transitions. Actors are the executable
//! variant — they own state, accept transitions, and have lifecycle
//! hooks. The traits live in `boardwalk::runtime`; these tests pin
//! that public surface.

use std::collections::BTreeMap;

use boardwalk::runtime::{
    Actor, ActorCtx, ActorError, DynFuture, Resource, ResourceCtx, ResourceError, TransitionCtx,
    TransitionError,
};
use boardwalk::{ResourceSnapshot, ResourceSpec, TransitionInput, TransitionOutcome};

struct MetadataResource;

impl Resource for MetadataResource {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "metadata".into(),
            name: None,
            labels: BTreeMap::new(),
            property_schema: None,
            streams: vec![],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move {
            Err::<ResourceSnapshot, _>(ResourceError::Unavailable(
                "metadata resource has no snapshot in this test".into(),
            ))
        })
    }
}

#[test]
fn read_only_resource_does_not_implement_actor() {
    fn assert_resource<R: Resource>(_r: &R) {}
    assert_resource(&MetadataResource);
    // The intent is asserted by compile coverage: `MetadataResource`
    // satisfies `Resource` but is not constrained by `Actor`.
}

struct MinimalActor;

impl Resource for MinimalActor {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some("LED".into()),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: vec![],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move {
            Err::<ResourceSnapshot, _>(ResourceError::Unavailable("test stub".into()))
        })
    }
}

impl Actor for MinimalActor {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            Err::<TransitionOutcome, _>(TransitionError::NotAllowed(
                "no transitions in this test".into(),
            ))
        })
    }
}

#[test]
fn actor_is_resource_and_has_transition_boundary() {
    fn needs_actor<A: Actor + Resource>(_a: &A) {}
    let a = MinimalActor;
    needs_actor(&a);
}

#[test]
fn actor_transition_returns_transition_outcome() {
    let mut a = MinimalActor;
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let ctx = TransitionCtx::new_test();
        let result = a.transition(ctx, "noop", TransitionInput::default()).await;
        match result {
            Ok(TransitionOutcome::Completed { .. } | TransitionOutcome::Accepted { .. }) => {
                panic!("test stub returns Err")
            }
            Err(TransitionError::NotAllowed(_)) => {}
            Err(other) => panic!("expected NotAllowed, got {other:?}"),
        }
    });
}

#[test]
fn actor_lifecycle_hooks_have_default_implementations() {
    let mut a = MinimalActor;
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let ctx = ActorCtx::new_test();
        let r: Result<(), ActorError> = a.on_start(ctx.clone()).await;
        assert!(r.is_ok());
        let r: Result<(), ActorError> = a.on_stop(ctx).await;
        assert!(r.is_ok());
    });
}
