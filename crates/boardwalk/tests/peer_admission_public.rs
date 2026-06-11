//! Public capability-scoped admission surface (research 0002 / F-07).

use std::collections::BTreeMap;
use std::time::Duration;

use boardwalk::runtime::{
    Actor, DynFuture, Resource, ResourceCtx, ResourceError, TransitionCtx, TransitionError,
};
use boardwalk::{
    Boardwalk, PeerAdmission, PeerCapability, PeerConfigError, PeerLink, ResourceSnapshot,
    ResourceSpec, TransitionInput, TransitionOutcome,
};

#[derive(Default)]
struct Counter {
    n: u32,
}

impl Resource for Counter {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "counter".into(),
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
            let mut props = serde_json::Map::new();
            props.insert("n".into(), serde_json::Value::from(self.n));
            Ok(ResourceSnapshot {
                id: "ignored".into(),
                kind: "counter".into(),
                name: None,
                state: Some("ready".into()),
                node: "test".into(),
                properties: props,
                labels: BTreeMap::new(),
                transitions: vec![],
                streams: vec![],
                revision: None,
                metadata: serde_json::Map::new(),
            })
        })
    }
}

impl Actor for Counter {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            Err::<TransitionOutcome, _>(TransitionError::NotAllowed("test stub".into()))
        })
    }
}

#[test]
fn admission_surface_is_spellable_from_the_crate_root() -> Result<(), PeerConfigError> {
    // The full consumer spelling from research 0002 q2's contract.
    let _cloud = Boardwalk::new().name("cloud").accept_peer(
        PeerAdmission::shared_token("hub", "kid-1", "secret")?
            .expected_node_id("node-hub-7f3a")
            .allow([
                PeerCapability::ResourceRead,
                PeerCapability::StreamSubscribe,
                PeerCapability::TransitionInvoke,
            ]),
    );
    let _hub = Boardwalk::new().name("hub").link_peer(
        PeerLink::new("ws://127.0.0.1:4444", "hub")?
            .token("kid-1", "secret")
            .node_id("node-hub-7f3a")
            .request_capabilities([PeerCapability::ResourceRead]),
    );
    Ok(())
}

#[test]
#[should_panic(expected = "invalid peer admission config")]
fn accept_peer_token_panics_on_invalid_route_name() {
    let _ = Boardwalk::new()
        .name("cloud")
        .accept_peer_token("hub name", "kid-1", "secret");
}

#[test]
#[should_panic(expected = "invalid peer url")]
fn link_panics_on_invalid_url() {
    let _ = Boardwalk::new().name("hub").link("not a url");
}

#[test]
fn accept_peer_token_still_admits_at_resource_read_ceiling() {
    // Behavior pin: the convenience delegates to PeerAdmission::shared_token,
    // keeping the read-only default ceiling (no second silent behavioral break).
    // End-to-end ceiling assertion is covered by the F-07 guard (5.2);
    // here it is enough that valid input still builds.
    let _ = Boardwalk::new()
        .name("cloud")
        .accept_peer_token("hub", "kid-1", "secret");
}

#[tokio::test]
async fn token_bound_capability_scoped_link_serves_remote_resources() {
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud = Boardwalk::new().name("cloud").accept_peer(
        PeerAdmission::shared_token("hub", "kid-1", "secret")
            .unwrap()
            .allow([
                PeerCapability::ResourceRead,
                PeerCapability::StreamSubscribe,
            ]),
    );
    tokio::spawn(cloud.listen_until_on(cloud_listener, std::future::pending()));

    let hub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hub = Boardwalk::new()
        .name("hub")
        .use_actor(Counter::default())
        .link_peer(
            PeerLink::new(format!("http://{cloud_addr}"), "hub")
                .unwrap()
                .token("kid-1", "secret")
                .request_capabilities([PeerCapability::ResourceRead]),
        );
    tokio::spawn(hub.listen_until_on(hub_listener, std::future::pending()));

    // There is no public link-established observation API, so poll the
    // cloud's gateway until it serves the hub's resources (bounded).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) =
            reqwest::get(format!("http://{cloud_addr}/servers/hub/resources")).await
            && response.status() == reqwest::StatusCode::OK
        {
            let body: serde_json::Value = response.json().await.unwrap();
            let entities = body
                .get("entities")
                .and_then(|e| e.as_array())
                .cloned()
                .unwrap_or_default();
            assert!(
                entities.iter().any(|entity| {
                    entity
                        .pointer("/properties/kind")
                        .and_then(|kind| kind.as_str())
                        == Some("counter")
                }),
                "gateway should serve the hub's counter resource, got: {body}"
            );
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cloud did not serve hub resources through the tokened link within 10s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
