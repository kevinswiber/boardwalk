use std::sync::Arc;
use std::time::Duration;

use boardwalk::core::TransitionInput;
use boardwalk::events::{SubscribeOpts, TopicPattern};
use boardwalk::runtime::{NodeBuilder, NodeHandle};
use boardwalk_mock_led::Led;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let node = Arc::new(NodeBuilder::new("hub").build());
    let id = node.register_actor(Led::default()).await.map_err(debug)?;
    let topic = format!("hub/led/{id}/state");
    let mut events = node.events().subscribe(
        TopicPattern::parse(&topic).map_err(debug)?,
        SubscribeOpts::default(),
    );

    let handle = NodeHandle::new(node.clone());
    let led = handle
        .query("where kind = \"led\"")
        .await
        .map_err(debug)?
        .into_iter()
        .find(|resource| resource.id() == id)
        .ok_or_else(|| anyhow::anyhow!("registered LED was not queryable"))?;

    let before = led.snapshot().await.map_err(debug)?;
    println!("registered {} in state {}", id, state(&before));

    led.transition("turn-on", TransitionInput::default())
        .await
        .map_err(debug)?;

    let event = tokio::time::timeout(Duration::from_secs(1), events.rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("state event subscription closed"))?;
    println!("{} -> {}", event.stream_id.as_str(), event.data);

    let after = led.snapshot().await.map_err(debug)?;
    println!("state is now {}", state(&after));

    node.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

fn state(snapshot: &boardwalk::http::ResourceSnapshot) -> &str {
    snapshot.state.as_deref().unwrap_or("unknown")
}

fn debug(err: impl std::fmt::Debug) -> anyhow::Error {
    anyhow::anyhow!("{err:?}")
}
