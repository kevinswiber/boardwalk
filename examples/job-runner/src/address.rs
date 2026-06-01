//! Example-local resource-address vocabulary. Centralizes the node, job
//! kind, and resource id so the typed `ResourceRef` is built in exactly one
//! place and reused for resource hrefs, accepted-job handles, and stream
//! subscription URLs.

use boardwalk::links::ResourceRef;

use crate::NODE_NAME;

pub(crate) const JOB_KIND: &str = "job";

pub(crate) fn job_resource(job_id: &str) -> ResourceRef<'_> {
    ResourceRef::new(NODE_NAME, JOB_KIND, job_id)
}
