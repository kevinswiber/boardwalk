use boardwalk::prelude::*;
use serde_json::{Map, json};

use crate::api::{JobHandle, SubmitJob};
use crate::job::{Job, example_labels};

#[derive(Debug)]
pub(crate) struct JobQueue {
    name: String,
    submitted: u64,
}

impl JobQueue {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            submitted: 0,
        }
    }
}

impl Resource for JobQueue {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "job.queue".into(),
            name: Some(self.name.clone()),
            labels: example_labels(),
            property_schema: None,
            streams: vec![],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let mut properties = Map::new();
        properties.insert("submitted_count".into(), json!(self.submitted));
        properties.insert("queued_count".into(), json!(0));
        properties.insert("running_count".into(), json!(0));
        properties.insert("succeeded_count".into(), json!(0));
        properties.insert("failed_count".into(), json!(0));
        properties.insert("cancelled_count".into(), json!(0));
        let snapshot = ResourceSnapshot::builder("job.queue")
            .name(self.name.clone())
            .state("open")
            .properties(properties)
            .labels(example_labels())
            .transitions(vec![TransitionAffordance::available(submit_spec())])
            .build();
        Box::pin(async move { Ok(snapshot) })
    }
}

#[boardwalk::actor]
impl JobQueue {
    #[boardwalk::transition]
    async fn submit(
        &mut self,
        ctx: TransitionCtx,
        input: TransitionInput,
    ) -> Result<TransitionOutcome, TransitionError> {
        let input = input.deserialize::<SubmitJob>()?;
        let id = ctx
            .register_actor(Job::from_submit(self.name.clone(), input))
            .await?;
        self.submitted += 1;
        let handle = JobHandle::for_job(id);
        TransitionOutcome::accepted(handle.to_outcome_job(true), &handle)
    }
}

fn submit_spec() -> TransitionSpec {
    TransitionSpec::async_job("submit")
        .title("Submit job")
        .allowed_states(["open"])
        .idempotency(Idempotency::Supported)
        .effect(Effect::Unsafe)
}
