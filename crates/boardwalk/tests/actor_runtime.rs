//! Bounded, per-actor command serialization.
//!
//! The runtime gives each actor a single execution slot and a bounded
//! pending-command queue. Calls to `ActorHandle::transition` are
//! serialized; calls to `try_transition` reject with `Busy` when the
//! pending queue is full.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use boardwalk::core::{ResourceSpec, TransitionInput, TransitionOutcome};
use boardwalk::http::ResourceSnapshot;
use boardwalk::runtime::{
    Actor, ActorHandle, DynFuture, Resource, ResourceCtx, ResourceError, TransitionCtx,
    TransitionError,
};

struct Slow {
    delay: Duration,
    enters: Arc<AtomicU64>,
    exits: Arc<AtomicU64>,
}

impl Resource for Slow {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "slow".into(),
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

impl Actor for Slow {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        let enters = self.enters.clone();
        let exits = self.exits.clone();
        let delay = self.delay;
        Box::pin(async move {
            enters.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(delay).await;
            exits.fetch_add(1, Ordering::SeqCst);
            Err::<TransitionOutcome, _>(TransitionError::NotAllowed("stub".into()))
        })
    }
}

fn slow_actor(delay_ms: u64) -> (Slow, Arc<AtomicU64>, Arc<AtomicU64>) {
    let enters = Arc::new(AtomicU64::new(0));
    let exits = Arc::new(AtomicU64::new(0));
    (
        Slow {
            delay: Duration::from_millis(delay_ms),
            enters: enters.clone(),
            exits: exits.clone(),
        },
        enters,
        exits,
    )
}

#[tokio::test]
async fn actor_transitions_are_serialized_per_actor() {
    let (actor, enters, _exits) = slow_actor(80);
    let handle = ActorHandle::spawn(actor, 4);

    let h1 = handle.clone();
    let h2 = handle.clone();
    let a = tokio::spawn(async move {
        let _ = h1.transition("a", TransitionInput::default()).await;
    });
    let b = tokio::spawn(async move {
        let _ = h2.transition("b", TransitionInput::default()).await;
    });
    let start = Instant::now();
    let _ = tokio::join!(a, b);
    let elapsed = start.elapsed();
    assert_eq!(enters.load(Ordering::SeqCst), 2);
    assert!(
        elapsed >= Duration::from_millis(150),
        "two serialized 80ms transitions must take >= 150ms; took {elapsed:?}"
    );
}

#[tokio::test]
async fn actor_command_queue_is_bounded() {
    let (actor, _enters, _exits) = slow_actor(200);
    let handle = ActorHandle::spawn(actor, 1);

    // Kick off two transitions to occupy the worker + the single
    // buffer slot. Both `transition` calls await capacity, so they
    // happily proceed.
    let h1 = handle.clone();
    let h2 = handle.clone();
    tokio::spawn(async move {
        let _ = h1.transition("a", TransitionInput::default()).await;
    });
    tokio::spawn(async move {
        let _ = h2.transition("b", TransitionInput::default()).await;
    });

    // Give the runtime a moment to advance the first transition into
    // the worker; the second message now occupies the buffer.
    tokio::time::sleep(Duration::from_millis(40)).await;

    let result = handle.try_transition("c", TransitionInput::default());
    match result {
        Err(TransitionError::Busy | TransitionError::BackpressureRequired) => {}
        Err(other) => panic!("expected Busy/BackpressureRequired, got {other:?}"),
        Ok(_) => panic!("expected try_transition to reject when full"),
    }
}

#[tokio::test]
async fn different_actors_run_independently() {
    let (a_actor, _ae, _ax) = slow_actor(200);
    let (b_actor, _be, _bx) = slow_actor(0);
    let h_a = ActorHandle::spawn(a_actor, 4);
    let h_b = ActorHandle::spawn(b_actor, 4);

    let a_run = tokio::spawn(async move {
        let _ = h_a.transition("slow", TransitionInput::default()).await;
    });

    let start = Instant::now();
    let _ = h_b.transition("fast", TransitionInput::default()).await;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "fast actor should not wait on slow actor; took {elapsed:?}"
    );

    let _ = a_run.await;
}
