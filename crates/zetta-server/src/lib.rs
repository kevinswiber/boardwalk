//! Top-level builder. See `docs/07-api-ergonomics.md`.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use url::Url;
use zetta_core::{App, Device, Scout};

#[derive(Default)]
pub struct Zetta {
    name: Option<String>,
    peers: Vec<Url>,
    // The plugins are erased here so the builder stays mono-typed at
    // each call. Real wiring follows in M8.
    devices: Vec<Box<dyn Device>>,
    scouts: Vec<Arc<dyn Scout>>,
    apps: Vec<Arc<dyn App>>,
}

impl Zetta {
    pub fn new() -> Self { Self::default() }

    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }

    pub fn use_device<D: Device>(mut self, d: D) -> Self {
        self.devices.push(Box::new(d));
        self
    }

    pub fn use_scout<S: Scout>(mut self, s: S) -> Self {
        self.scouts.push(Arc::new(s));
        self
    }

    pub fn use_app<A: App>(mut self, a: A) -> Self {
        self.apps.push(Arc::new(a));
        self
    }

    pub fn link(mut self, url: impl AsRef<str>) -> Self {
        if let Ok(u) = Url::parse(url.as_ref()) {
            self.peers.push(u);
        }
        self
    }

    pub async fn listen(self, _addr: SocketAddr) -> anyhow::Result<()> {
        tracing::info!(name = ?self.name, "zetta listening (stub)");
        // Real implementation: M8.
        Ok(())
    }
}
