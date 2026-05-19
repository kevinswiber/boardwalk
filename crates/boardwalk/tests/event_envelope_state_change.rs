//! `Core::run_transition` and the `Node` actor executor mint
//! `resource.state.changed` envelopes through the shared
//! `StreamRegistry` and now populate request correlation, command
//! causation, and W3C trace context from the carrying context.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue};
use boardwalk::core::{ResourceSpec, StreamSpec, TransitionInput, TransitionOutcome};
use boardwalk::events::{SubscribeOpts, TopicPattern};
use boardwalk::http::{Core, CoreBuilder, ResourceSnapshot, router};
use boardwalk::runtime::{
    Actor, DynFuture, NodeBuilder, NodeHandle, RequestCtx, Resource, ResourceCtx, ResourceError,
    TransitionCtx, TransitionError,
};
use boardwalk::{Device, DeviceConfig, DeviceError};
use futures::future::BoxFuture;
use uuid::Uuid;

const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

#[derive(Default)]
struct DeviceLed {
    on: bool,
}

impl Device for DeviceLed {
    fn config(&self, cfg: &mut DeviceConfig) {
        cfg.type_("led")
            .name("LED")
            .state(if self.on { "on" } else { "off" })
            .when("off", &["turn-on"])
            .when("on", &["turn-off"])
            .monitor("state");
    }
    fn state(&self) -> &str {
        if self.on { "on" } else { "off" }
    }
    fn transition<'a>(
        &'a mut self,
        name: &'a str,
        _input: TransitionInput,
    ) -> BoxFuture<'a, Result<(), DeviceError>> {
        Box::pin(async move {
            match name {
                "turn-on" => {
                    self.on = true;
                    Ok(())
                }
                "turn-off" => {
                    self.on = false;
                    Ok(())
                }
                other => Err(DeviceError::Invalid(format!("unknown {other}"))),
            }
        })
    }
}

async fn boot_with_led() -> (Arc<Core>, Uuid) {
    let mut b = CoreBuilder::new("hub");
    let id = b.add_device(DeviceLed::default());
    let core = b.build();
    (core, id)
}

#[tokio::test]
async fn state_transition_publishes_envelope_with_resource_state_changed_kind() {
    let (core, id) = boot_with_led().await;
    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    core.run_transition(
        &id,
        "turn-on",
        TransitionInput::default(),
        RequestCtx::default(),
    )
    .await
    .expect("turn-on succeeds");

    let env = sub.rx.recv().await.expect("state-change envelope");
    assert_eq!(env.payload_kind, "resource.state.changed");
    assert_eq!(env.payload_version, 1);
    assert_eq!(env.data, serde_json::Value::String("on".to_string()));
}

#[tokio::test]
async fn successive_state_transitions_get_strictly_increasing_sequence() {
    let (core, id) = boot_with_led().await;
    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    for name in ["turn-on", "turn-off", "turn-on"] {
        core.run_transition(&id, name, TransitionInput::default(), RequestCtx::default())
            .await
            .expect("transition succeeds");
    }

    let a = sub.rx.recv().await.unwrap();
    let b = sub.rx.recv().await.unwrap();
    let c = sub.rx.recv().await.unwrap();
    assert_eq!(a.sequence, 1);
    assert_eq!(b.sequence, 2);
    assert_eq!(c.sequence, 3);
}

#[tokio::test]
async fn state_transition_envelope_stream_id_uses_bw_uri_scheme() {
    let (core, id) = boot_with_led().await;
    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    core.run_transition(
        &id,
        "turn-on",
        TransitionInput::default(),
        RequestCtx::default(),
    )
    .await
    .unwrap();
    let env = sub.rx.recv().await.unwrap();
    let stream_id = env.stream_id.as_str();
    assert!(
        stream_id.starts_with("bw://hub/resources/"),
        "expected bw://hub/resources/... prefix; got {stream_id}"
    );
    assert!(
        stream_id.ends_with("/streams/state"),
        "expected /streams/state suffix; got {stream_id}"
    );
}

/// `Node` actor executor: when a transition runs through an actor
/// command queue, the resulting `resource.state.changed` envelope
/// carries the inbound `RequestCtx`'s correlation + W3C trace context
/// and the freshly-minted `CommandId` from the carrying `TransitionCtx`.
#[tokio::test]
async fn runtime_transition_event_carries_request_correlation_and_command_causation() {
    let node = Arc::new(NodeBuilder::new("hub").build());
    let id = node
        .register_actor(Led::default())
        .await
        .expect("register Led actor");

    let topic = format!("hub/led/{id}/state");
    let mut sub = node.events().subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    let mut headers = HeaderMap::new();
    headers.insert("traceparent", HeaderValue::from_static(TRACEPARENT));
    headers.insert("x-request-id", HeaderValue::from_static("req-123"));
    let request = RequestCtx::from_headers(&headers);

    let handle = NodeHandle::new(node.clone());
    let proxies = handle
        .query("where kind = \"led\"")
        .await
        .expect("CaQL parses");
    let proxy = proxies
        .into_iter()
        .find(|p| p.id() == id)
        .expect("led proxy");

    let ctx = TransitionCtx::with_node(request, node.clone());
    let command_id = ctx.command_id().as_str().to_owned();

    proxy
        .transition_with_ctx(ctx, "turn-on", TransitionInput::default())
        .await
        .expect("transition succeeds");

    let env = sub.rx.recv().await.expect("state-change envelope");
    assert_eq!(
        env.correlation_id.as_ref().map(|c| c.0.as_str()),
        Some("req-123")
    );
    assert_eq!(
        env.causation_id.as_ref().map(|c| c.0.as_str()),
        Some(command_id.as_str())
    );
    let trace = env
        .trace_context
        .as_ref()
        .expect("trace_context must be set when traceparent is present");
    assert_eq!(trace.traceparent, TRACEPARENT);
}

/// The form-url-encoded device route is still wired to `Core` and is
/// scheduled to be replaced by the JSON `/resources` transition
/// endpoint. Until that replacement lands, the form path must
/// participate in the same correlation/causation/trace contract — both
/// to validate the bridge into `RequestCtx`/`TransitionCtx` and to
/// avoid silently leaving form-issued commands uncorrelated.
#[tokio::test]
async fn legacy_form_transition_event_is_populated_until_form_route_is_removed() {
    let mut b = CoreBuilder::new("hub");
    let id = b.add_device(DeviceLed::default());
    let core = b.build();
    let app = router(core.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let topic = format!("hub/led/{id}/state");
    let mut sub = core.bus.subscribe(
        TopicPattern::parse(&topic).unwrap(),
        SubscribeOpts::default(),
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/servers/hub/devices/{id}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .header("traceparent", TRACEPARENT)
        .header("x-request-id", "req-123")
        .body("action=turn-on")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let env = sub.rx.recv().await.expect("state-change envelope");
    assert_eq!(
        env.correlation_id.as_ref().map(|c| c.0.as_str()),
        Some("req-123")
    );
    let causation = env
        .causation_id
        .as_ref()
        .expect("causation_id must be set for form-route transitions");
    assert!(
        !causation.0.is_empty(),
        "causation_id should carry the minted command id"
    );
    let trace = env
        .trace_context
        .as_ref()
        .expect("trace_context must be set when traceparent is present");
    assert_eq!(trace.traceparent, TRACEPARENT);
}

/// Test fixture: a minimal LED `Actor` that toggles a boolean and
/// reports `"on"`/`"off"` from `Resource::snapshot`. Returns a
/// `Completed` outcome from `turn-on` so the executor can observe a
/// state change.
#[derive(Default)]
struct Led {
    on: bool,
}

impl Led {
    fn snapshot_value(&self) -> ResourceSnapshot {
        ResourceSnapshot {
            id: "ignored".into(),
            kind: "led".into(),
            name: Some("LED".into()),
            state: Some(if self.on { "on".into() } else { "off".into() }),
            node: "ignored".into(),
            properties: serde_json::Map::new(),
            labels: BTreeMap::new(),
            transitions: vec![],
            streams: vec![],
            revision: None,
            metadata: serde_json::Map::new(),
        }
    }
}

impl Resource for Led {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some("LED".into()),
            labels: BTreeMap::new(),
            property_schema: None,
            streams: vec![StreamSpec {
                name: "state".into(),
                kind: Default::default(),
            }],
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        let snap = self.snapshot_value();
        Box::pin(async move { Ok(snap) })
    }
}

impl Actor for Led {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            match name {
                "turn-on" => {
                    self.on = true;
                    Ok(TransitionOutcome::Completed {
                        output: None,
                        snapshot: self.snapshot_value(),
                    })
                }
                "turn-off" => {
                    self.on = false;
                    Ok(TransitionOutcome::Completed {
                        output: None,
                        snapshot: self.snapshot_value(),
                    })
                }
                other => Err(TransitionError::NotAllowed(other.into())),
            }
        })
    }
}
