use std::net::SocketAddr;

use boardwalk::Boardwalk;
use boardwalk_mock_led::Led;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let addr: SocketAddr = "0.0.0.0:1337".parse()?;
    Boardwalk::new()
        .name("hub")
        .use_device(Led::default())
        .listen(addr)
        .await
}
