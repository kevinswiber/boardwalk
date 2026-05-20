//! Per-actor execution: a bounded mpsc channel feeding a single
//! task that owns the `Actor` and processes one transition at a time.
//!
//! Exposes an `ActorHandle` whose `transition` awaits capacity and
//! whose `try_transition` rejects with `TransitionError::Busy` when
//! the pending queue is full.

use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::actor::{Actor, ActorCtx, TransitionCtx, TransitionError};
use super::context::RequestCtx;
use super::resource::{Resource, ResourceCtx, ResourceError, ResourceSnapshot};
use super::transition::{TransitionInput, TransitionOutcome};

/// One message queued to an actor. The runtime never exposes
/// `Stop` directly; it's sent by `ActorHandle::shutdown` so the actor
/// drains in order before its task ends.
enum Command {
    /// `ctx` is boxed to keep `Command` small: a `TransitionCtx` with
    /// an attached `RequestCtx` and an `ActorCtx` is several hundred
    /// bytes, while `Snapshot`/`Stop` are roughly pointer-sized.
    /// Boxing avoids paying that footprint on every queued message.
    Transition {
        name: String,
        input: TransitionInput,
        ctx: Box<TransitionCtx>,
        reply: oneshot::Sender<Result<TransitionOutcome, TransitionError>>,
    },
    Snapshot {
        ctx: ResourceCtx,
        reply: oneshot::Sender<Result<ResourceSnapshot, ResourceError>>,
    },
    /// Drains the actor: runs `on_stop` and exits the task. The
    /// actor's identity is carried on the task itself, so the
    /// caller does not provide an `ActorCtx`.
    Stop { reply: oneshot::Sender<()> },
}

/// Cloneable handle to a running actor. Drops to the actor's task
/// channel; when every handle is dropped, the task exits.
#[derive(Clone)]
pub struct ActorHandle {
    tx: mpsc::Sender<Command>,
}

/// Per-actor owned handles the `Node` keeps so shutdown can join
/// the task and await `on_stop`.
pub(crate) struct ActorSlot {
    pub handle: ActorHandle,
    pub task: JoinHandle<()>,
}

/// Pending transition handle returned by `try_transition`. Awaiting
/// it produces the eventual transition outcome.
pub struct PendingTransition {
    rx: oneshot::Receiver<Result<TransitionOutcome, TransitionError>>,
}

impl PendingTransition {
    pub async fn await_outcome(self) -> Result<TransitionOutcome, TransitionError> {
        match self.rx.await {
            Ok(result) => result,
            Err(_) => Err(TransitionError::Internal(
                "actor task dropped reply slot".into(),
            )),
        }
    }
}

impl ActorHandle {
    /// Spawn a task that owns `actor` and serves transition commands
    /// off a bounded mpsc channel of size `capacity`. Runs
    /// `Actor::on_start` (with a default `ActorCtx`) before draining
    /// any messages so transitions see an initialised actor. Real
    /// node-managed actors use `spawn_with_task` and pass an
    /// `ActorCtx` carrying their resource identity.
    pub fn spawn<A: Actor>(actor: A, capacity: usize) -> Self {
        let (handle, _task) = Self::spawn_with_task(actor, capacity, ActorCtx::default());
        handle
    }

    /// Same as `spawn` but also returns the `JoinHandle` of the actor
    /// task and accepts an `ActorCtx` carrying the resource identity
    /// to hand to `on_start` / `on_stop`.
    pub(crate) fn spawn_with_task<A: Actor>(
        actor: A,
        capacity: usize,
        actor_ctx: ActorCtx,
    ) -> (Self, JoinHandle<()>) {
        let capacity = capacity.max(1);
        let (tx, mut rx) = mpsc::channel::<Command>(capacity);
        let task = tokio::spawn(async move {
            let mut actor = actor;
            // Run on_start with the actor's identity before draining
            // transitions.
            let _ = actor.on_start(actor_ctx.clone()).await;
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    Command::Transition {
                        name,
                        input,
                        ctx,
                        reply,
                    } => {
                        let enriched = (*ctx).with_actor(actor_ctx.clone());
                        let outcome = actor.transition(enriched, &name, input).await;
                        let _ = reply.send(outcome);
                    }
                    Command::Snapshot { ctx, reply } => {
                        // `Actor: Resource`, so this resolves to the
                        // `Resource::snapshot` impl on the concrete
                        // actor type.
                        let snap = Resource::snapshot(&actor, ctx).await;
                        let _ = reply.send(snap);
                    }
                    Command::Stop { reply } => {
                        // Use the actor's identity, not a default.
                        let _ = actor.on_stop(actor_ctx.clone()).await;
                        let _ = reply.send(());
                        break;
                    }
                }
            }
        });
        (ActorHandle { tx }, task)
    }

    /// Send a transition and await the outcome. Awaits queue capacity
    /// rather than rejecting; use `try_transition` for non-blocking
    /// behavior with explicit backpressure.
    pub async fn transition(
        &self,
        name: &str,
        input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        self.transition_with_ctx(
            TransitionCtx::new(RequestCtx::default(), "actor"),
            name,
            input,
        )
        .await
    }

    /// Same as `transition` but lets the caller carry their own
    /// `TransitionCtx` (carrying request correlation, command id, and
    /// node reference).
    pub async fn transition_with_ctx(
        &self,
        ctx: TransitionCtx,
        name: &str,
        input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(Command::Transition {
                name: name.to_string(),
                input,
                ctx: Box::new(ctx),
                reply: rtx,
            })
            .await
            .map_err(|_| TransitionError::Internal("actor task terminated".into()))?;
        match rrx.await {
            Ok(result) => result,
            Err(_) => Err(TransitionError::Internal(
                "actor task dropped reply slot".into(),
            )),
        }
    }

    /// Send a `Snapshot` command and await the actor's reply.
    pub(crate) async fn snapshot(
        &self,
        ctx: ResourceCtx,
    ) -> Result<ResourceSnapshot, ResourceError> {
        let (rtx, rrx) = oneshot::channel();
        if self
            .tx
            .send(Command::Snapshot { ctx, reply: rtx })
            .await
            .is_err()
        {
            return Err(ResourceError::Unavailable("actor task terminated".into()));
        }
        match rrx.await {
            Ok(result) => result,
            Err(_) => Err(ResourceError::Internal(
                "actor task dropped reply slot".into(),
            )),
        }
    }

    /// Send a stop signal and wait for `on_stop` to complete. Returns
    /// `true` if the actor task acknowledged the stop within `within`;
    /// `false` on timeout or if the actor task had already exited.
    /// The actor's identity is carried on the spawned task itself —
    /// callers do not need to provide an `ActorCtx`.
    pub(crate) async fn shutdown(&self, within: Duration) -> bool {
        let (rtx, rrx) = oneshot::channel();
        if self.tx.send(Command::Stop { reply: rtx }).await.is_err() {
            return false;
        }
        matches!(tokio::time::timeout(within, rrx).await, Ok(Ok(())))
    }

    /// Non-blocking enqueue. Returns `TransitionError::Busy` when the
    /// pending queue is full and `Internal` when the actor task has
    /// terminated.
    pub fn try_transition(
        &self,
        name: &str,
        input: TransitionInput,
    ) -> Result<PendingTransition, TransitionError> {
        let (rtx, rrx) = oneshot::channel();
        let ctx = TransitionCtx::new(RequestCtx::default(), "actor");
        self.tx
            .try_send(Command::Transition {
                name: name.to_string(),
                input,
                ctx: Box::new(ctx),
                reply: rtx,
            })
            .map_err(|e| match e {
                TrySendError::Full(_) => TransitionError::Busy,
                TrySendError::Closed(_) => {
                    TransitionError::Internal("actor task terminated".into())
                }
            })?;
        Ok(PendingTransition { rx: rrx })
    }
}

/// Policy knobs shared across the node's actors. The node reads
/// `actor_queue_capacity` to size each actor's command channel; the
/// struct will gain bus and coalesce settings as those layers grow.
#[derive(Clone, Debug)]
pub struct NodePolicy {
    pub actor_queue_capacity: usize,
}

impl Default for NodePolicy {
    fn default() -> Self {
        Self {
            actor_queue_capacity: 32,
        }
    }
}
