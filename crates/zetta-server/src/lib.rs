//! Top-level builder for assembling a Zetta server.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use url::Url;
use zetta_core::Device;
use zetta_http::{router_with, AppState, Core, CoreBuilder, PeerHandler, PeerInitState};
pub use zetta_peer::PeerAcceptors;
use zetta_peer::PeerClient;

pub struct Zetta {
    name: String,
    peers: Vec<Url>,
    builder: CoreBuilder,
}

impl Default for Zetta {
    fn default() -> Self { Self::new() }
}

impl Zetta {
    pub fn new() -> Self {
        let name = "zetta".to_string();
        Self { name: name.clone(), peers: Vec::new(), builder: CoreBuilder::new(name) }
    }

    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = n.into();
        let mut new_builder = CoreBuilder::new(self.name.clone());
        std::mem::swap(&mut new_builder, &mut self.builder);
        self
    }

    pub fn use_device<D: Device>(mut self, d: D) -> Self {
        self.builder.add_device(d);
        self
    }

    pub fn link(mut self, url: impl AsRef<str>) -> Self {
        match Url::parse(url.as_ref()) {
            Ok(u) => self.peers.push(u),
            Err(e) => tracing::warn!(?e, url = url.as_ref(), "ignoring invalid peer url"),
        }
        self
    }

    /// Bind and serve. Blocks until the listener stops.
    pub async fn listen(self, addr: SocketAddr) -> anyhow::Result<()> {
        let built = self.build();
        tracing::info!(%addr, "zetta-rs listening");
        let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
        axum::serve(listener, built.router).await.context("serve")
    }

    /// Build the runtime + router + spawn peer clients without binding.
    /// Useful for integration tests.
    pub fn build(self) -> Built {
        let core: Arc<Core> = self.builder.build();
        let peer_init = PeerInitState::default();
        let acceptors = PeerAcceptors::new();

        let handler: PeerHandler = {
            let acceptors = acceptors.clone();
            Arc::new(move |peer_name, connection_id, upgraded| {
                let acceptors = acceptors.clone();
                Box::pin(async move {
                    acceptors.on_upgraded(peer_name, connection_id, upgraded).await;
                })
            })
        };

        let state = AppState {
            core: core.clone(),
            peer_handler: Some(handler),
            peer_init: peer_init.clone(),
        };
        let router = router_with(state);

        let mut peer_tasks = Vec::new();
        for url in self.peers {
            let local_name = self.name.clone();
            let pc = PeerClient::new(
                url,
                local_name,
                router.clone(),
                peer_init.clone(),
                core.clone(),
            );
            peer_tasks.push(pc.spawn());
        }

        Built { core, peer_tasks, router, acceptors }
    }
}

/// Materialized server pieces, returned by `Zetta::build()`.
pub struct Built {
    pub core: Arc<Core>,
    pub peer_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub router: axum::Router,
    pub acceptors: PeerAcceptors,
}
