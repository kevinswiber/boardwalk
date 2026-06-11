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

/// Probe actor whose `report` transition echoes the caller provenance
/// it observed, so end-to-end tests can assert what an embedded actor
/// sees.
#[derive(Default)]
struct ProvenanceProbe;

impl Resource for ProvenanceProbe {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "provenance-probe".into(),
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
            Ok(ResourceSnapshot {
                id: "ignored".into(),
                kind: "provenance-probe".into(),
                name: None,
                state: Some("ready".into()),
                node: "test".into(),
                properties: serde_json::Map::new(),
                labels: BTreeMap::new(),
                transitions: vec![],
                streams: vec![],
                revision: None,
                metadata: serde_json::Map::new(),
            })
        })
    }
}

impl Actor for ProvenanceProbe {
    fn transition<'a>(
        &'a mut self,
        ctx: TransitionCtx,
        name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async move {
            if name != "report" {
                return Err(TransitionError::NotAllowed("unknown transition".into()));
            }
            let provenance = ctx.provenance();
            let output = serde_json::json!({
                "is_local": provenance.is_local(),
                "forwarded_by": provenance.forwarded_by(),
                "peer_is_some": provenance.peer().is_some(),
            });
            Ok(TransitionOutcome::Completed {
                output: Some(output),
                snapshot: ResourceSnapshot {
                    id: "ignored".into(),
                    kind: "provenance-probe".into(),
                    name: None,
                    state: Some("ready".into()),
                    node: "test".into(),
                    properties: serde_json::Map::new(),
                    labels: BTreeMap::new(),
                    transitions: vec![],
                    streams: vec![],
                    revision: None,
                    metadata: serde_json::Map::new(),
                },
            })
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

/// Boots a cloud (token admission at `allow`) and a hub running the
/// provenance probe (link requesting read + invoke). Returns
/// `(cloud_addr, hub_addr, probe_resource_id)` once the link is live.
async fn boot_probe_pair() -> (std::net::SocketAddr, std::net::SocketAddr, String) {
    boot_probe_pair_with_admission(
        PeerAdmission::shared_token("hub", "kid-1", "secret")
            .unwrap()
            .allow([
                PeerCapability::ResourceRead,
                PeerCapability::TransitionInvoke,
            ]),
    )
    .await
}

/// Same topology as [`boot_probe_pair`] parameterized over the cloud's
/// admission config, so ceiling-pair tests cannot drift apart. The hub
/// always requests read + invoke; the negotiated set is whatever the
/// supplied admission allows.
async fn boot_probe_pair_with_admission(
    admission: PeerAdmission,
) -> (std::net::SocketAddr, std::net::SocketAddr, String) {
    let cloud_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cloud_addr = cloud_listener.local_addr().unwrap();
    let cloud = Boardwalk::new().name("cloud").accept_peer(admission);
    tokio::spawn(cloud.listen_until_on(cloud_listener, std::future::pending()));

    let hub_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hub_addr = hub_listener.local_addr().unwrap();
    let hub = Boardwalk::new()
        .name("hub")
        .use_actor(ProvenanceProbe)
        .link_peer(
            PeerLink::new(format!("http://{cloud_addr}"), "hub")
                .unwrap()
                .token("kid-1", "secret")
                .request_capabilities([
                    PeerCapability::ResourceRead,
                    PeerCapability::TransitionInvoke,
                ]),
        );
    tokio::spawn(hub.listen_until_on(hub_listener, std::future::pending()));

    // Poll until the cloud serves the hub's probe resource (no public
    // link-established observation API), then return its id.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) =
            reqwest::get(format!("http://{cloud_addr}/servers/hub/resources")).await
            && response.status() == reqwest::StatusCode::OK
        {
            let body: serde_json::Value = response.json().await.unwrap();
            if let Some(id) = body
                .get("entities")
                .and_then(|e| e.as_array())
                .and_then(|entities| {
                    entities.iter().find_map(|entity| {
                        (entity.pointer("/properties/kind").and_then(|k| k.as_str())
                            == Some("provenance-probe"))
                        .then(|| entity.pointer("/properties/id")?.as_str())
                        .flatten()
                        .map(str::to_string)
                    })
                })
            {
                return (cloud_addr, hub_addr, id);
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cloud did not serve the hub probe resource within 10s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn forwarded_invoke_carries_gateway_provenance() {
    let (cloud_addr, _hub_addr, id) = boot_probe_pair().await;
    let response = reqwest::Client::new()
        .post(format!(
            "http://{cloud_addr}/servers/hub/resources/{id}/transitions/report"
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let output = body.get("output").expect("transition output");
    assert_eq!(output["is_local"], serde_json::json!(false));
    assert_eq!(output["forwarded_by"], serde_json::json!("cloud"));
    // Anonymous public caller: the gateway has no admission state to
    // attest, so the hub sees no admitted caller. This is the honest
    // permanent outcome of this plan; populating the caller requires
    // the M1.3 caller-ingress follow-on.
    assert_eq!(output["peer_is_some"], serde_json::json!(false));
}

#[tokio::test]
async fn forged_provenance_headers_on_public_listener_are_ignored() {
    let (_cloud_addr, hub_addr, id) = boot_probe_pair().await;
    let response = reqwest::Client::new()
        .post(format!(
            "http://{hub_addr}/resources/{id}/transitions/report"
        ))
        .header("boardwalk-forwarded-by", "cloud")
        .header("boardwalk-caller-peer-id", "peer-fake")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let output = body.get("output").expect("transition output");
    assert_eq!(output["is_local"], serde_json::json!(true));
    assert_eq!(output["forwarded_by"], serde_json::Value::Null);
    assert_eq!(output["peer_is_some"], serde_json::json!(false));
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

#[tokio::test]
async fn token_admitted_without_allow_cannot_invoke_a_transition_through_the_gateway() {
    // F-07 regression guard (negative half): the default ceiling is
    // resource.read only — no .allow call.
    let (cloud_addr, _hub_addr, id) = boot_probe_pair_with_admission(
        PeerAdmission::shared_token("hub", "kid-1", "secret").unwrap(),
    )
    .await;

    // Reads work at the default ceiling (the pair booted means the
    // poll already saw a 200 read through the gateway).
    let read = reqwest::get(format!("http://{cloud_addr}/servers/hub/resources"))
        .await
        .unwrap();
    assert_eq!(read.status(), reqwest::StatusCode::OK);

    // The operative path is denied post-admission.
    let response = reqwest::Client::new()
        .post(format!(
            "http://{cloud_addr}/servers/hub/resources/{id}/transitions/report"
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(response.text().await.unwrap(), "peer capability denied");
}

#[tokio::test]
async fn allow_with_transition_invoke_forwards_the_transition() {
    // F-07 regression guard (positive half): .allow is sufficient to
    // open the operative path.
    let (cloud_addr, _hub_addr, id) = boot_probe_pair_with_admission(
        PeerAdmission::shared_token("hub", "kid-1", "secret")
            .unwrap()
            .allow([
                PeerCapability::ResourceRead,
                PeerCapability::TransitionInvoke,
            ]),
    )
    .await;

    let response = reqwest::Client::new()
        .post(format!(
            "http://{cloud_addr}/servers/hub/resources/{id}/transitions/report"
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "widened ceiling must forward the invoke: {}",
        response.status()
    );
    // The probe actor observed the invocation.
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["output"]["forwarded_by"], serde_json::json!("cloud"));
}
