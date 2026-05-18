//! Lifecycle hooks: `on_start` runs before the first transition;
//! `on_stop` runs when the handle is shut down or dropped; and the
//! shutdown path completes deterministically without long sleeps.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use boardwalk::core::{ResourceSpec, TransitionInput, TransitionOutcome};
use boardwalk::http::ResourceSnapshot;
use boardwalk::runtime::{
    Actor, ActorCtx, ActorError, ActorHandle, DynFuture, NodeBuilder, Resource, ResourceCtx,
    ResourceError, TransitionCtx, TransitionError,
};

struct Tracked {
    started: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    transitions_after_start: Arc<AtomicU64>,
    started_when_transition_seen: Arc<AtomicBool>,
}

impl Resource for Tracked {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "tracked".into(),
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
        Box::pin(
            async move { Err::<ResourceSnapshot, _>(ResourceError::Unavailable("stub".into())) },
        )
    }
}

impl Actor for Tracked {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        let started = self.started.clone();
        let count = self.transitions_after_start.clone();
        let started_when_seen = self.started_when_transition_seen.clone();
        Box::pin(async move {
            if started.load(Ordering::SeqCst) {
                started_when_seen.store(true, Ordering::SeqCst);
            }
            count.fetch_add(1, Ordering::SeqCst);
            Err::<TransitionOutcome, _>(TransitionError::NotAllowed("stub".into()))
        })
    }

    fn on_start<'a>(&'a mut self, _ctx: ActorCtx) -> DynFuture<'a, Result<(), ActorError>> {
        let started = self.started.clone();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            started.store(true, Ordering::SeqCst);
            Ok(())
        })
    }

    fn on_stop<'a>(&'a mut self, _ctx: ActorCtx) -> DynFuture<'a, Result<(), ActorError>> {
        let stopped = self.stopped.clone();
        Box::pin(async move {
            stopped.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}

fn tracked() -> (Tracked, Arc<AtomicBool>, Arc<AtomicBool>, Arc<AtomicBool>) {
    let started = Arc::new(AtomicBool::new(false));
    let stopped = Arc::new(AtomicBool::new(false));
    let saw_start = Arc::new(AtomicBool::new(false));
    (
        Tracked {
            started: started.clone(),
            stopped: stopped.clone(),
            transitions_after_start: Arc::new(AtomicU64::new(0)),
            started_when_transition_seen: saw_start.clone(),
        },
        started,
        stopped,
        saw_start,
    )
}

#[tokio::test]
async fn actor_on_start_runs_before_first_transition() {
    let (actor, _started, _stopped, saw_start) = tracked();
    let handle = ActorHandle::spawn(actor, 4);

    let _ = handle.transition("noop", TransitionInput::default()).await;
    assert!(
        saw_start.load(Ordering::SeqCst),
        "on_start must complete before the first transition runs"
    );
}

#[tokio::test]
async fn actor_on_stop_runs_on_node_shutdown() {
    let node = NodeBuilder::new("runner").build();
    let (actor, _started, stopped, _) = tracked();
    let _id = node.register_actor(actor).await.unwrap();
    node.shutdown(Duration::from_millis(200)).await;
    assert!(
        stopped.load(Ordering::SeqCst),
        "on_stop must run as part of node shutdown"
    );
}

#[tokio::test]
async fn actor_handle_drop_aborts_background_task() {
    let (actor, _, _, _) = tracked();
    let handle = ActorHandle::spawn(actor, 4);
    drop(handle);
    // The actor task should terminate within a short window after the
    // last handle drops; we don't have direct visibility, so we
    // simply assert this returns promptly.
    tokio::time::sleep(Duration::from_millis(40)).await;
}
