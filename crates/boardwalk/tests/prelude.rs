#![allow(unused_imports)]

use boardwalk::prelude::*;

fn _refs() {
    fn _needs<T>() {}
    _needs::<TransitionInput>();
    _needs::<TransitionOutcome>();
    _needs::<ResourceSnapshot>();
    _needs::<ResourceSpec>();
    _needs::<TransitionSpec>();
    _needs::<TransitionAffordance>();
    _needs::<SnapshotStreamSpec>();
    _needs::<StreamSpec>();

    let _: Option<&dyn Resource> = None;
    let _: Option<&dyn Actor> = None;
    let _ = core::any::type_name::<(ActorCtx, ResourceCtx, TransitionCtx)>();
    let _ = core::any::type_name::<(ActorError, ResourceError, TransitionError)>();
    let _ = core::any::type_name::<(Effect, Idempotency, StreamKind, TransitionResultKind)>();
}
