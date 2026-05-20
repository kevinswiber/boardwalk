//! Public API guardrails for the Resource / Actor crate facade.

use std::path::{Path, PathBuf};

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

const PERMITTED_LEGACY_PUBLIC_REFERENCES: &[&str] = &[];

#[test]
fn public_api_no_longer_exports_device_names() {
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
fn boardwalk_builder_does_not_expose_private_adapter_surface() {
    let server = read("crates/boardwalk/src/server.rs");
    for snippet in [
        "pub fn use_actor",
        "pub fn use_app",
        "pub fn use_scout",
        "pub fn register_factory",
        "pub fn build(self) -> anyhow::Result<Built>",
        "pub struct Built",
        "pub use crate::peer::PeerAcceptors",
    ] {
        assert!(
            !server.contains(snippet),
            "Boardwalk public facade must not expose private adapter surface `{snippet}`"
        );
    }
}

#[test]
fn proc_macros_no_longer_generate_device_surface() {
    let macros = read("crates/boardwalk-macros/src/lib.rs");
    let snippets = [
        format!("pub fn {}(", "device"),
        format!("::boardwalk::{}", "Device"),
        format!("::boardwalk::{}", "DeviceConfig"),
        format!("::boardwalk::{}", "DeviceError"),
        format!("#[{}]", "device"),
    ];
    for snippet in snippets {
        assert!(
            !macros.contains(snippet.as_str()),
            "boardwalk-macros must not expose legacy device snippet `{snippet}`"
        );
    }
}

#[test]
fn tests_and_examples_do_not_import_device_root_surface() {
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
        format!("boardwalk::{{{}", "Device"),
        format!("boardwalk::{{Boardwalk, {}", "Device"),
        format!("boardwalk::{}", "Device"),
        format!("boardwalk::{}", "device"),
        format!("#[boardwalk::{}]", "device"),
        format!("#[{}]", "device"),
        format!(".use_{}(", "device"),
    ];
    let mut offenders = Vec::new();
    for path in paths {
        let rel = path.strip_prefix(Path::new("../..")).unwrap_or(&path);
        let rel = rel.to_string_lossy();
        if PERMITTED_LEGACY_PUBLIC_REFERENCES.contains(&rel.as_ref()) {
            continue;
        }

        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("could not read {path:?}: {e}"));
        for snippet in &forbidden {
            if source.contains(snippet.as_str()) {
                offenders.push(format!("{} contains `{snippet}`", path.display()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "public tests/examples/docs still use legacy root device surface:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn final_resource_contract_replaces_device_characterizations() {
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
        if source.contains("/servers/hub/devices") {
            offenders.push(format!(
                "{} fetches `/servers/hub/devices`; legacy route behavior belongs in \
                 `src/http/routes.rs` router-level tests",
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
