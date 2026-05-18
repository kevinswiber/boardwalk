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
