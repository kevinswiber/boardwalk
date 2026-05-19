//! Lightweight checks that docs/ files cover the survivor public
//! contracts. Greps for stable keywords so that any rename or
//! omission breaks the test loudly. Not a substitute for hand-reading
//! the docs.

fn read(rel: &str) -> String {
    // tests run from the crate directory.
    let path = format!("../../{rel}");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("could not read {path}: {e}"))
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
}

#[test]
fn devices_docs_mention_resource_snapshot() {
    let s = read("docs/devices.md");
    assert!(
        s.contains("ResourceSnapshot"),
        "devices.md should reference ResourceSnapshot direction"
    );
    assert!(
        s.contains("kind"),
        "devices.md should mention the canonical `kind` field"
    );
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
