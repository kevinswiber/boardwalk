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
];

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
fn docs_reference_resource_routes_not_device_routes() {
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
            "public docs should not present old device route `{old_route}`"
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
fn docs_disclose_current_actor_http_adapter_boundaries() {
    let resources = read("docs/resources.md");
    assert!(
        resources.contains("Boardwalk::new().use_actor"),
        "resources.md should say Boardwalk actor registration is exposed through reusable HTTP"
    );
    assert!(
        resources.contains("example-local adapter"),
        "resources.md should keep the job-runner adapter boundary explicit"
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

    let peers = read("docs/peers.md");
    assert!(
        peers.contains("Boardwalk::new().use_actor"),
        "peers.md should show actor registration through the Boardwalk builder"
    );
    assert!(
        peers.contains("`NodeBuilder`"),
        "peers.md should explain the relationship to the actor runtime"
    );

    let crate_docs = read("crates/boardwalk/src/lib.rs");
    assert!(
        crate_docs.contains("Common imports for the Resource/Actor surface"),
        "crate rustdoc should introduce the Resource/Actor import surface"
    );
    assert!(
        !crate_docs.contains("older server-adapter exports"),
        "crate rustdoc should not describe transitional root device exports"
    );
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
fn public_docs_have_no_private_planning_terms() {
    let s = public_docs(PUBLIC_DOCS);
    let private_terms = [
        format!("{}bo", "Gum"),
        format!(".{}", "gumbo"),
        format!("Plan {}", "0003"),
        format!("Task {}", "7.4"),
        format!("{}/", "findings"),
    ];
    for private in private_terms {
        assert!(
            !s.contains(private.as_str()),
            "public docs should not mention private planning term `{private}`"
        );
    }
}

#[test]
fn markdown_docs_do_not_teach_old_device_identifiers() {
    let s = public_docs(PUBLIC_MARKDOWN_DOCS);
    let old_identifiers = [
        "DeviceConfig".to_string(),
        "DeviceError".to_string(),
        "DeviceProxy".to_string(),
        "ServerHandle".to_string(),
        format!("use_{}", "device"),
        format!("#[{}]", "device"),
        format!("docs/{}", "devices"),
        format!("{}.md", "devices"),
    ];
    for old in old_identifiers {
        assert!(
            !s.contains(old.as_str()),
            "markdown docs should not teach old device scaffolding identifier `{old}`"
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
fn docs_events_documents_envelope_fields_and_stream_gap() {
    let s = read("docs/events.md");
    assert!(s.contains("eventId"));
    assert!(s.contains("streamId"));
    assert!(s.contains("sequence"));
    assert!(s.contains("stream-gap"));
    assert!(s.contains("slow_consumer"));
}
