use std::collections::BTreeMap;

use boardwalk::prelude::*;

#[derive(Default)]
struct Boot {
    started: bool,
    stopped: bool,
}

impl Resource for Boot {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "boot".into(),
            ..Default::default()
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async move {
            Ok(ResourceSnapshot {
                id: String::new(),
                kind: "boot".into(),
                name: None,
                state: None,
                node: String::new(),
                properties: Default::default(),
                labels: BTreeMap::new(),
                transitions: vec![],
                streams: vec![],
                revision: None,
                metadata: Default::default(),
            })
        })
    }
}

#[boardwalk::actor]
impl Boot {
    #[boardwalk::on_start]
    async fn boot(&mut self, _ctx: ActorCtx) -> Result<(), ActorError> {
        self.started = true;
        Ok(())
    }

    #[boardwalk::on_stop]
    async fn teardown(&mut self, _ctx: ActorCtx) -> Result<(), ActorError> {
        self.stopped = true;
        Ok(())
    }
}

#[tokio::test]
async fn macro_passes_through_lifecycle_hooks() {
    let mut b = Boot::default();
    Actor::on_start(&mut b, ActorCtx::new_test()).await.unwrap();
    assert!(
        b.started,
        "macro-generated on_start should run the marked method"
    );
    Actor::on_stop(&mut b, ActorCtx::new_test()).await.unwrap();
    assert!(b.stopped);
}
