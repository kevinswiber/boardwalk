//! Lightweight checks that docs/ files cover the current public
//! contracts. Greps for stable keywords so that any rename or
//! omission breaks the test loudly. Not a substitute for hand-reading
//! the docs.

fn read(rel: &str) -> String {
    // tests run from the crate directory.
    let path = format!("../../{rel}");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("could not read {path}: {e}"))
}

const PUBLIC_DOCS: &[&str] = &[
    "README.md",
    "docs/getting-started.md",
    "docs/resources.md",
    "docs/events.md",
    "docs/event-envelope.md",
    "docs/caql.md",
    "docs/peers.md",
    "examples/hello-led/README.md",
    "examples/job-runner/README.md",
    "crates/boardwalk/src/lib.rs",
];

const PUBLIC_MARKDOWN_DOCS: &[&str] = &[
    "README.md",
    "docs/getting-started.md",
    "docs/resources.md",
    "docs/events.md",
    "docs/event-envelope.md",
    "docs/caql.md",
    "docs/peers.md",
    "examples/hello-led/README.md",
    "examples/job-runner/README.md",
];

const PUBLIC_SMOKE_SCRIPTS: &[&str] = &["scripts/smoke-ndjson.sh", "scripts/smoke-ws.sh"];

fn public_docs(paths: &[&str]) -> String {
    paths
        .iter()
        .map(|path| format!("\n<!-- {path} -->\n{}", read(path)))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn caql_docs_mention_new_grammar_and_error_envelope() {
    let s = read("docs/caql.md");
    assert!(s.contains("contains"), "caql.md should mention `contains`");
    assert!(s.contains("exists"), "caql.md should mention `exists`");
    assert!(
        s.contains("kind"),
        "caql.md should mention the canonical `kind` field"
    );
    assert!(
        s.contains("400"),
        "caql.md should describe the 400 error response"
    );
    assert!(
        s.contains("ResourceSnapshot") || s.contains("snapshot"),
        "caql.md should describe the resource query target"
    );
    for required in ["allowedStates", "result", "idempotency", "requiredScopes"] {
        assert!(
            s.contains(required),
            "caql.md should show the richer transition affordance field `{required}`"
        );
    }
}

#[test]
fn resources_docs_mention_resource_actor_contract() {
    let s = read("docs/resources.md");
    assert!(
        s.contains("ResourceSnapshot"),
        "resources.md should reference ResourceSnapshot"
    );
    assert!(
        s.contains("kind"),
        "resources.md should mention the canonical `kind` field"
    );
    assert!(
        s.contains("Resource"),
        "resources.md should mention Resource"
    );
    assert!(s.contains("Actor"), "resources.md should mention Actor");
    assert!(s.contains("Node"), "resources.md should mention Node");
    assert!(
        s.contains("TransitionOutcome"),
        "resources.md should mention TransitionOutcome"
    );
}

#[test]
fn docs_reference_resource_routes_only() {
    let s = public_docs(PUBLIC_DOCS);

    for required in [
        "/resources",
        "/resources/{id}",
        "/resources/{id}/transitions/{transition}",
        "/servers/{name}/resources",
        "/servers/{name}/resources/{id}/transitions/{transition}",
    ] {
        assert!(s.contains(required), "docs should mention `{required}`");
    }

    for old_route in [
        "/servers/{name}/devices",
        "/servers/<name>/devices",
        "/servers/hub/devices",
        "/devices/{id}",
    ] {
        assert!(
            !s.contains(old_route),
            "public docs should not present old resource route `{old_route}`"
        );
    }
}

#[test]
fn persistence_docs_do_not_claim_event_sourcing() {
    let s = public_docs(&["docs/resources.md", "docs/peers.md", "docs/events.md"]);

    for forbidden in [
        "event sourced",
        "event-sourced",
        "event sourcing",
        "event-sourcing",
        "event-sourced source of truth",
    ] {
        assert!(
            !s.contains(forbidden),
            "public docs should not claim event sourcing: {forbidden}"
        );
    }
}

#[test]
fn public_docs_keep_snapshot_and_event_history_contract_honest() {
    let s = public_docs(&["docs/resources.md", "docs/peers.md", "docs/events.md"]);

    for required in [
        "latest snapshot",
        "read/restart projection",
        "internal repository boundaries",
        "redb",
        "optional append-only event history",
        "peer config",
        "latest connection status",
    ] {
        assert!(
            s.contains(required),
            "public persistence docs should state `{required}`"
        );
    }

    for forbidden in [
        "full event sourcing",
        "source-of-truth event log",
        "remote shadow reconstruction",
        "third-party stores",
    ] {
        assert!(
            !s.contains(forbidden),
            "public persistence docs should not overclaim `{forbidden}`"
        );
    }
}

#[test]
fn crate_docs_show_resource_actor_imports() {
    let s = read("crates/boardwalk/src/lib.rs");
    for required in [
        "Resource",
        "Actor",
        "NodeBuilder",
        "ResourceSnapshot",
        "TransitionOutcome",
        "SlowConsumerPolicy",
    ] {
        assert!(
            s.contains(required),
            "crate docs should mention `{required}`"
        );
    }
}

#[test]
fn docs_describe_reusable_actor_http_runtime() {
    let resources = read("docs/resources.md");
    assert!(
        resources.contains("Boardwalk::new().use_actor"),
        "resources.md should say Boardwalk actor registration is exposed through reusable HTTP"
    );
    assert!(
        resources.contains("use_actor_with_id"),
        "resources.md should mention stable actor ids for the job-runner queue"
    );

    let getting_started = read("docs/getting-started.md");
    assert!(
        getting_started.contains("workspace fixture"),
        "getting-started.md should label boardwalk_mock_led as a workspace-only fixture"
    );
    assert!(
        getting_started.contains("not published"),
        "getting-started.md should not imply boardwalk_mock_led is an external dependency"
    );
    assert!(
        getting_started.contains("reusable Boardwalk HTTP router"),
        "getting-started.md should name the reusable actor HTTP path"
    );

    let peers = read("docs/peers.md");
    assert!(
        peers.contains("Boardwalk::new().use_actor"),
        "peers.md should show actor registration through the Boardwalk builder"
    );
    assert!(
        peers.contains("listen_until_on"),
        "peers.md should document graceful serving on an already-bound listener"
    );
    assert!(
        peers.contains("`NodeBuilder`"),
        "peers.md should explain the relationship to the actor runtime"
    );

    let hello_led = read("examples/hello-led/README.md");
    assert!(
        hello_led.contains("NodeBuilder"),
        "hello-led README should explain the in-process NodeBuilder path"
    );
    assert!(
        hello_led.contains("Boardwalk"),
        "hello-led README should point HTTP users at Boardwalk"
    );

    let job_runner = read("examples/job-runner/README.md");
    assert!(
        job_runner.contains("cargo run -p boardwalk-job-runner-example"),
        "job-runner README should use the real workspace package name"
    );
    assert!(
        job_runner.contains("Boardwalk::new()"),
        "job-runner README should show the reusable Boardwalk builder"
    );
    assert!(
        job_runner.contains("use_actor_with_id"),
        "job-runner README should show stable actor registration"
    );
    assert!(
        job_runner.contains("reusable HTTP, WebSocket, and peer route stack"),
        "job-runner README should describe the final reusable route stack"
    );

    let crate_docs = read("crates/boardwalk/src/lib.rs");
    assert!(
        crate_docs.contains("Common imports for the Resource/Actor surface"),
        "crate rustdoc should introduce the Resource/Actor import surface"
    );
    assert!(
        !crate_docs.contains("older server-adapter exports"),
        "crate rustdoc should not describe transitional root exports"
    );
}

#[test]
fn smoke_scripts_target_long_running_job_runner_example() {
    let s = public_docs(PUBLIC_SMOKE_SCRIPTS);

    assert!(
        s.contains("cargo run -p boardwalk-job-runner-example"),
        "smoke scripts should tell users to run the long-running job-runner example"
    );
    assert!(
        s.contains("/servers/runner"),
        "smoke scripts should probe the job-runner server"
    );
    assert!(
        s.contains("QUEUE_ID=${QUEUE_ID:-queue-default}") && s.contains("/transitions/submit"),
        "smoke scripts should trigger the job queue actor through the resource transition route"
    );
    assert!(
        s.contains("runner/job/*/progress"),
        "WS smoke should subscribe to job-runner progress events"
    );

    for stale in [
        "hello-led",
        "/servers/hub/devices",
        "hub/led",
        "action=turn-on",
    ] {
        assert!(
            !s.contains(stale),
            "smoke scripts should not depend on stale hello-led runtime detail `{stale}`"
        );
    }
}

#[test]
fn docs_do_not_describe_transitional_runtime_adapter_caveats() {
    let s = public_docs(PUBLIC_DOCS);
    for forbidden in [
        "server adapter",
        "server-adapter",
        "legacy adapter",
        "private adapter",
        "example-local HTTP adapter",
        "example-local actor-backed adapter",
    ] {
        assert!(
            !s.contains(forbidden),
            "public docs should not describe transitional runtime caveat `{forbidden}`"
        );
    }
}

#[test]
fn events_docs_show_transition_correlation_and_causation() {
    let s = read("docs/events.md");
    assert!(
        s.contains("correlationId"),
        "events.md should document transition correlationId"
    );
    assert!(
        s.contains("\"correlationId\": \"req-123\""),
        "events.md should show populated transition correlationId"
    );
    assert!(
        s.contains("causationId"),
        "events.md should document transition causationId"
    );
    assert!(
        s.contains("\"causationId\": \""),
        "events.md should show populated transition causationId"
    );
}

#[test]
fn public_docs_have_no_process_markers() {
    let s = public_docs(PUBLIC_DOCS);
    let markers: &[&[u8]] = &[
        &[0x47, 0x75, 0x6d, 0x62, 0x6f],
        &[0x2e, 0x67, 0x75, 0x6d, 0x62, 0x6f],
        &[0x50, 0x6c, 0x61, 0x6e, 0x20, 0x30, 0x30, 0x30, 0x33],
        &[0x54, 0x61, 0x73, 0x6b, 0x20, 0x37, 0x2e, 0x34],
        &[0x66, 0x69, 0x6e, 0x64, 0x69, 0x6e, 0x67, 0x73, 0x2f],
    ];
    for marker in markers {
        let token = std::str::from_utf8(marker).unwrap();
        assert!(
            !s.contains(token),
            "public docs should not mention reserved process marker `{token}`"
        );
    }
}

#[test]
fn markdown_docs_do_not_teach_old_identifiers() {
    let s = public_docs(PUBLIC_MARKDOWN_DOCS);
    let old_identifiers = [
        "DeviceConfig",
        "DeviceError",
        "DeviceProxy",
        "ServerHandle",
        "use_device",
        "#[device]",
        "docs/devices",
        "devices.md",
    ];
    for old in old_identifiers {
        assert!(
            !s.contains(old),
            "markdown docs should not teach old scaffolding identifier `{old}`"
        );
    }
}

#[test]
fn docs_event_envelope_documents_envelope_version_and_policies() {
    let s = read("docs/event-envelope.md");
    assert!(s.contains("envelopeVersion"));
    assert!(s.contains("eventId"));
    assert!(s.contains("sequence"));
    assert!(s.contains("SlowConsumerPolicy"));
    assert!(s.contains("Disconnect"));
    assert!(s.contains("DropNewest"));
    assert!(s.contains("stream-gap"));
    assert!(s.contains("broadcast_lag"));
    assert!(
        s.contains("Coalesce { key_path }"),
        "event-envelope.md should describe the shipped Coalesce policy with its `key_path` parameter"
    );
    assert!(
        s.contains("non-coalescible"),
        "event-envelope.md should pin the missing-key contract (envelopes whose key path does not resolve are non-coalescible)"
    );
    assert!(
        !s.contains("intentionally deferred"),
        "event-envelope.md still describes Coalesce as deferred even though it ships now"
    );
}

#[test]
fn docs_events_document_ndjson_replay_and_policy_query() {
    let s = read("docs/events.md");
    for required in [
        "\"stream\"",
        "replay=true",
        "outboundCapacity",
        "slowConsumerPolicy",
        "coalesceKey",
        "node/kind/resource/stream",
    ] {
        assert!(
            s.contains(required),
            "events.md should document NDJSON `{required}`"
        );
    }
}

#[test]
fn docs_events_documents_envelope_fields_and_stream_gap() {
    let s = read("docs/events.md");
    assert!(s.contains("eventId"));
    assert!(s.contains("streamId"));
    assert!(s.contains("\"stream\":\"state\""));
    assert!(s.contains("sequence"));
    assert!(s.contains("stream-gap"));
    assert!(s.contains("slow_consumer"));
    assert!(s.contains("replay=true"));
    assert!(s.contains("slowConsumerPolicy"));
    assert!(s.contains("coalesceKey"));
}

#[test]
fn public_docs_do_not_advertise_supported_wildcard_query_scope() {
    let s = public_docs(PUBLIC_DOCS);
    assert!(
        !s.contains("server=* by default"),
        "public docs should not advertise wildcard peer query scope before policy and limits exist"
    );
}

#[test]
fn peers_doc_matches_current_peer_subprotocol() {
    let s = read("docs/peers.md");
    assert!(
        s.contains("boardwalk-peer/3"),
        "peers.md should document the current peer tunnel subprotocol"
    );
    for stale in ["boardwalk-peer/1", "boardwalk-peer/2"] {
        assert!(
            !s.contains(stale),
            "peers.md must not document stale peer subprotocol `{stale}`"
        );
    }
}

#[test]
fn peers_doc_does_not_claim_implicit_local_peer_admission() {
    let s = read("docs/peers.md");
    for stale in [
        "remains a trusted local-development peer initiator",
        "remains a trusted local-development",
    ] {
        assert!(
            !s.contains(stale),
            "peers.md must not claim unconfigured clouds accept local peers implicitly: `{stale}`"
        );
    }
}

#[test]
fn peers_doc_describes_admission_capabilities_and_limits() {
    let s = read("docs/peers.md");
    for required in [
        "accept_peer_token",
        "accept_peer",
        "PeerAdmission",
        "PeerLink",
        "link_peer",
        ".allow(",
        "request_capabilities",
        "allow_unauthenticated_local_peers",
        "peer admission is not configured",
        "route name",
        "expected node id",
        "resource.read",
        "resource.query",
        "stream.subscribe",
        "transition.invoke",
        "resource.register",
        "peer.admin",
        "server=*",
        "unsupported-federation-query",
        "/peer-management",
        "404",
    ] {
        assert!(
            s.contains(required),
            "peers.md should document peer boundary detail `{required}`"
        );
    }
    let stale = "public outbound token-bound links are not available yet";
    assert!(
        !s.contains(stale),
        "peers.md must not retain stale claim `{stale}`"
    );
}

#[test]
fn peers_doc_states_default_ceiling_and_widening() {
    let s = read("docs/peers.md");
    assert!(
        s.contains("admits its peer at the `resource.read` ceiling")
            || s.contains("default ceiling is `resource.read`"),
        "peers.md must state accept_peer_token's default ceiling"
    );
}

#[test]
fn peers_doc_documents_the_admission_tracing_contract() {
    let s = read("docs/peers.md");
    assert!(
        s.contains("boardwalk::admission"),
        "peers.md must document the deny-decision tracing target"
    );
}

#[test]
fn public_docs_do_not_overclaim_federation_or_enterprise_auth() {
    let s = public_docs(PUBLIC_DOCS);
    for forbidden in [
        "OAuth support",
        "mTLS support",
        "RBAC support",
        "recursive federation",
        "event history storage",
        "server=* by default",
        "Worker resource",
    ] {
        assert!(
            !s.contains(forbidden),
            "public docs should not claim unsupported `{forbidden}`"
        );
    }
}
