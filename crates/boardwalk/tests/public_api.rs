//! Public API guardrails for the Resource / Actor crate facade.

use std::path::{Path, PathBuf};

use boardwalk::runtime::{DynFuture, ResourceCtx, ResourceError, TransitionCtx, TransitionError};
use boardwalk::{
    Actor, Boardwalk, Resource, ResourceSnapshot, ResourceSpec, TransitionInput, TransitionOutcome,
};

fn read(rel: &str) -> String {
    // Tests run from the crate directory.
    let path = format!("../../{rel}");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("could not read {path}: {e}"))
}

fn repo_path(rel: &str) -> PathBuf {
    Path::new("../..").join(rel)
}

fn pub_use_blocks(source: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current = String::new();
    let mut collecting = false;

    for line in source.lines() {
        let trimmed = line.trim();
        if collecting {
            current.push(' ');
            current.push_str(trimmed);
            if trimmed.ends_with(';') {
                blocks.push(current.clone());
                current.clear();
                collecting = false;
            }
        } else if trimmed.starts_with("pub use ") {
            current.push_str(trimmed);
            if trimmed.ends_with(';') {
                blocks.push(current.clone());
                current.clear();
            } else {
                collecting = true;
            }
        }
    }

    blocks
}

fn contains_ident(source: &str, ident: &str) -> bool {
    source
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .any(|token| token == ident)
}

fn is_ident_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn contains_call(source: &str, ident: &str) -> bool {
    source.match_indices(ident).any(|(start, _)| {
        let end = start + ident.len();
        let bytes = source.as_bytes();
        let has_ident_before = start > 0 && is_ident_byte(bytes[start - 1]);
        let has_ident_after = bytes.get(end).is_some_and(|byte| is_ident_byte(*byte));
        if has_ident_before || has_ident_after {
            return false;
        }

        bytes[end..]
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            == Some(b'(')
    })
}

fn contains_arc_core_type(source: &str) -> bool {
    source
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect::<String>()
        .contains("Arc<Core>")
}

fn collect_files(root: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root).unwrap_or_else(|e| panic!("could not read {root:?}: {e}"))
    {
        let entry = entry.unwrap_or_else(|e| panic!("could not read entry under {root:?}: {e}"));
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, out);
        } else if matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("rs" | "md")
        ) {
            out.push(path);
        }
    }
}

fn legacy_source_roots() -> Vec<PathBuf> {
    vec![
        repo_path("crates/boardwalk/src"),
        repo_path("crates/boardwalk/tests"),
        repo_path("examples"),
        repo_path("docs"),
        repo_path("README.md"),
    ]
}

const LEGACY_GUARD_TEST_FILES: &[&str] = &[
    "crates/boardwalk/tests/docs.rs",
    "crates/boardwalk/tests/public_api.rs",
];

fn is_legacy_guard_test_file(path: &Path) -> bool {
    let rel = path.strip_prefix(Path::new("../..")).unwrap_or(path);
    LEGACY_GUARD_TEST_FILES
        .iter()
        .any(|guard| rel == Path::new(guard))
}

#[test]
fn legacy_adapter_vocabulary_is_removed_from_source_surface() {
    let mut paths = Vec::new();
    for root in legacy_source_roots() {
        if root.is_dir() {
            collect_files(&root, &mut paths);
        } else {
            paths.push(root);
        }
    }

    let forbidden = [
        "device",
        "devices",
        "Device",
        "DEVICE",
        "DeviceConfig",
        "DeviceSnapshot",
        "DeviceCtx",
        "DeviceProxy",
        "DeviceHandle",
        "DeviceRecord",
        "DeviceError",
        "ServerHandle",
        "CoreBuilder",
    ];

    let mut offenders = Vec::new();
    for path in paths {
        if is_legacy_guard_test_file(&path) {
            continue;
        }

        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("could not read {path:?}: {e}"));

        for snippet in forbidden {
            if source.contains(snippet) {
                offenders.push(format!("{} contains `{snippet}`", path.display()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "legacy adapter vocabulary must be removed from the source surface:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn source_surface_does_not_describe_transitional_runtime_adapters() {
    let roots = [
        repo_path("crates/boardwalk/src"),
        repo_path("examples"),
        repo_path("docs"),
        repo_path("README.md"),
    ];
    let mut paths = Vec::new();
    for root in roots {
        if root.is_dir() {
            collect_files(&root, &mut paths);
        } else {
            paths.push(root);
        }
    }

    let forbidden = [
        "server adapter",
        "server-adapter",
        "legacy adapter",
        "private adapter",
        "example-local HTTP adapter",
        "example-local actor-backed adapter",
        "internal_adapter_tests",
    ];
    let mut offenders = Vec::new();
    for path in paths {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("could not read {path:?}: {e}"));
        for snippet in forbidden {
            if source.contains(snippet) {
                offenders.push(format!("{} contains `{snippet}`", path.display()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "final runtime docs/source must not describe transitional adapter paths:\n{}",
        offenders.join("\n")
    );
}

#[derive(Default)]
struct FacadeLed;

impl Resource for FacadeLed {
    fn spec(&self) -> ResourceSpec {
        ResourceSpec {
            kind: "led".into(),
            name: Some("Facade LED".into()),
            ..Default::default()
        }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: ResourceCtx,
    ) -> DynFuture<'a, Result<ResourceSnapshot, ResourceError>> {
        Box::pin(async {
            Ok(ResourceSnapshot {
                id: "ignored".into(),
                kind: "led".into(),
                name: Some("Facade LED".into()),
                state: Some("off".into()),
                node: "ignored".into(),
                properties: serde_json::Map::new(),
                labels: Default::default(),
                transitions: Vec::new(),
                streams: Vec::new(),
                revision: None,
                metadata: serde_json::Map::new(),
            })
        })
    }
}

impl Actor for FacadeLed {
    fn transition<'a>(
        &'a mut self,
        _ctx: TransitionCtx,
        _name: &'a str,
        _input: TransitionInput,
    ) -> DynFuture<'a, Result<TransitionOutcome, TransitionError>> {
        Box::pin(async { Err(TransitionError::NotAllowed("no transitions".into())) })
    }
}

#[test]
fn boardwalk_builder_accepts_resource_actor_without_private_adapter_traits() {
    let _server = Boardwalk::new().name("hub").use_actor(FacadeLed);
}

#[test]
fn public_api_exports_resource_actor_names() {
    let lib = read("crates/boardwalk/src/lib.rs");
    let blocks = pub_use_blocks(&lib);

    for ident in [
        "Device",
        "DeviceConfig",
        "DeviceError",
        "DeviceProxy",
        "ServerHandle",
        "Scout",
        "App",
        "device",
    ] {
        let offenders: Vec<_> = blocks
            .iter()
            .filter(|block| contains_ident(block, ident))
            .cloned()
            .collect();
        assert!(
            offenders.is_empty(),
            "crate root must not re-export `{ident}`; found {offenders:#?}"
        );
    }
}

#[test]
fn public_facade_exports_only_intended_api() {
    use boardwalk::{
        Actor, Boardwalk, Resource, ResourceSnapshot, StreamSpec, TransitionInput,
        TransitionOutcome, TransitionSpec,
    };

    fn assert_public<T: ?Sized>() {}
    assert_public::<Boardwalk>();
    assert_public::<TransitionInput>();
    assert_public::<TransitionOutcome>();
    assert_public::<TransitionSpec>();
    assert_public::<ResourceSnapshot>();
    assert_public::<StreamSpec>();
    assert_public::<dyn Actor>();
    assert_public::<dyn Resource>();

    let lib = read("crates/boardwalk/src/lib.rs");
    for module in [
        "core", "http", "peer", "registry", "server", "siren", "tunnel",
    ] {
        let declaration = format!("pub mod {module};");
        assert!(
            !lib.contains(&declaration),
            "crate root must not expose broad internal module `{module}`"
        );
    }
}

#[test]
fn runtime_owns_final_resource_and_transition_contracts() {
    use boardwalk::runtime::{
        ActorSpec, Effect, FieldSpec, Idempotency, JobHandle, ResourceKind, ResourceSnapshot,
        ResourceSpec, SnapshotStreamSpec, StateName, StreamKind, StreamSpec, TransitionAffordance,
        TransitionInput, TransitionName, TransitionOutcome, TransitionResultKind, TransitionSpec,
    };

    fn assert_public<T>() {}
    assert_public::<ActorSpec>();
    assert_public::<Effect>();
    assert_public::<FieldSpec>();
    assert_public::<Idempotency>();
    assert_public::<JobHandle>();
    assert_public::<ResourceKind>();
    assert_public::<ResourceSnapshot>();
    assert_public::<ResourceSpec>();
    assert_public::<SnapshotStreamSpec>();
    assert_public::<StateName>();
    assert_public::<StreamKind>();
    assert_public::<StreamSpec>();
    assert_public::<TransitionAffordance>();
    assert_public::<TransitionInput>();
    assert_public::<TransitionName>();
    assert_public::<TransitionOutcome>();
    assert_public::<TransitionResultKind>();
    assert_public::<TransitionSpec>();

    let root_snapshot: Option<boardwalk::ResourceSnapshot> = None;
    let runtime_snapshot: Option<ResourceSnapshot> = root_snapshot;
    let _: Option<ResourceSnapshot> = runtime_snapshot;

    let runtime_dir = repo_path("crates/boardwalk/src/runtime");
    let mut runtime_files = Vec::new();
    collect_files(&runtime_dir, &mut runtime_files);
    let mut runtime_offenders = Vec::new();
    for path in runtime_files {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("could not read {path:?}: {e}"));
        if source.contains("crate::http") {
            runtime_offenders.push(path.display().to_string());
        }
    }
    assert!(
        runtime_offenders.is_empty(),
        "runtime contract modules must not import HTTP-owned types:\n{}",
        runtime_offenders.join("\n")
    );

    assert!(
        !repo_path("crates/boardwalk/src/core.rs").exists(),
        "final Resource/Actor contracts should not require the removed compatibility module"
    );

    let runtime = read("crates/boardwalk/src/runtime/mod.rs");
    let runtime_public_uses = pub_use_blocks(&runtime);
    for snippet in ["RESERVED_FIELDS", "sanitize_properties"] {
        let offenders: Vec<_> = runtime_public_uses
            .iter()
            .filter(|block| {
                block.starts_with("pub use resource::") && contains_ident(block, snippet)
            })
            .cloned()
            .collect();
        assert!(
            offenders.is_empty(),
            "runtime facade must not publicly re-export implementation helper `{snippet}`; found {offenders:#?}"
        );
    }
}

#[test]
fn registry_uses_resources_table_without_old_table_fallback() {
    let registry = read("crates/boardwalk/src/registry.rs");
    assert!(
        registry.contains("TableDefinition::new(\"resources\")"),
        "resource registry should persist records in the `resources` table"
    );
    for snippet in [
        "TableDefinition::new(\"devices\")",
        "open_table(DEVICES",
        "DEVICE_TABLE",
        "devices table",
    ] {
        assert!(
            !registry.contains(snippet),
            "resource registry must not keep old persistence fallback `{snippet}`"
        );
    }
}

#[test]
fn boardwalk_builder_does_not_expose_private_adapter_surface() {
    let server = read("crates/boardwalk/src/server.rs");
    let routes = read("crates/boardwalk/src/http/routes.rs");
    for snippet in [
        "pub fn use_app",
        "pub fn use_scout",
        "pub fn register_factory",
        "pub fn register_actor_factory",
        "pub fn build(self) -> anyhow::Result<Built>",
        "pub struct Built",
        "pub use crate::peer::PeerAcceptors",
    ] {
        assert!(
            !server.contains(snippet),
            "Boardwalk public facade must not expose private adapter surface `{snippet}`"
        );
    }
    assert!(
        !routes.contains("Boardwalk::register_factory"),
        "resource routes must not point users at removed builder APIs"
    );
    assert!(
        server.contains("pub fn use_actor<A: Actor>"),
        "Boardwalk should expose actor-native registration"
    );
    assert!(
        server.contains("pub async fn listen_on"),
        "Boardwalk should expose serving on an already-bound listener"
    );
    assert!(
        server.contains("pub async fn listen_until_on"),
        "Boardwalk should expose graceful serving on an already-bound listener"
    );
    assert!(
        !server.contains("Vec<Box<dyn Device>>"),
        "Boardwalk must not collect boxed private adapter resources"
    );
}

#[test]
fn peer_and_stream_routes_do_not_carry_parallel_private_runtime_handles() {
    let peer = read("crates/boardwalk/src/peer.rs");
    let peer_streams = read("crates/boardwalk/src/http/peer_streams.rs");
    let routes = read("crates/boardwalk/src/http/routes.rs");

    for (name, source) in [
        ("src/peer.rs", peer.as_str()),
        ("src/http/peer_streams.rs", peer_streams.as_str()),
        ("src/http/routes.rs", routes.as_str()),
    ] {
        for ident in ["DeviceSnapshot", "DeviceHandle"] {
            assert!(
                !contains_ident(source, ident),
                "{name} must not route peers or streams through legacy `{ident}` lookups"
            );
        }
        for call in ["list_devices", "get_device", "run_transition"] {
            assert!(
                !contains_call(source, call),
                "{name} must not route peers or streams through legacy `{call}(...)` lookups"
            );
        }
    }

    assert!(
        !contains_arc_core_type(&peer),
        "PeerClient must use the router's AppState instead of carrying a parallel Core handle"
    );
}

#[test]
fn proc_macros_generate_actor_surface_only() {
    let macros = read("crates/boardwalk-macros/src/lib.rs");
    let snippets = [
        "pub fn device(",
        "::boardwalk::Device",
        "::boardwalk::DeviceConfig",
        "::boardwalk::DeviceError",
        "#[device]",
    ];
    for snippet in snippets {
        assert!(
            !macros.contains(snippet),
            "boardwalk-macros must not expose legacy snippet `{snippet}`"
        );
    }
}

#[test]
fn tests_and_examples_do_not_import_legacy_root_surface() {
    let roots = [
        repo_path("crates/boardwalk/tests"),
        repo_path("examples"),
        repo_path("README.md"),
        repo_path("docs"),
    ];
    let mut paths = Vec::new();
    for root in roots {
        if root.is_dir() {
            collect_files(&root, &mut paths);
        } else {
            paths.push(root);
        }
    }

    let forbidden = [
        "boardwalk::{Device",
        "boardwalk::{Boardwalk, Device",
        "boardwalk::Device",
        "boardwalk::device",
        "#[boardwalk::device]",
        "#[device]",
        ".use_device(",
    ];
    let mut offenders = Vec::new();
    for path in paths {
        if is_legacy_guard_test_file(&path) {
            continue;
        }

        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("could not read {path:?}: {e}"));
        for snippet in &forbidden {
            if source.contains(snippet) {
                offenders.push(format!("{} contains `{snippet}`", path.display()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "public tests/examples/docs still use legacy root surface:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn final_resource_contract_replaces_old_characterizations() {
    let root = repo_path("crates/boardwalk/tests/internal");
    let mut paths = Vec::new();
    collect_files(&root, &mut paths);

    let mut offenders = Vec::new();
    for path in paths {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("could not read {path:?}: {e}"));
        if source.contains("__pending_replacement") {
            offenders.push(format!(
                "{} contains `__pending_replacement`",
                path.display()
            ));
        }
        let old_route = "/servers/hub/devices";
        if source.contains(old_route) {
            offenders.push(format!(
                "{} fetches `{old_route}`; stale route behavior belongs in router-level tests",
                path.display()
            ));
        }
    }

    assert!(
        offenders.is_empty(),
        "old characterization tests still need final Resource/Actor/Node replacements:\n{}",
        offenders.join("\n")
    );
}
