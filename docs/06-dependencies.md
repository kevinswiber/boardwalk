# Dependencies

All choices are May 2026. Versions pinned to current stable; `^x.y`
ranges in actual Cargo.toml. Rationale and alternatives kept brief here
— the deeper survey lives in the build notes.

| Concern         | Crate                              | Version | Why                                                         |
|-----------------|------------------------------------|---------|-------------------------------------------------------------|
| Runtime         | `tokio`                            | 1.x LTS | Universal; everything else assumes it.                       |
| HTTP server     | `axum`                             | 0.8     | Tower-based; clean WS, content-neg, easy router-as-service.  |
| HTTP/2          | `h2`                               | 0.4     | Required for tunnel role reversal over arbitrary streams.    |
| Hyper (low)     | `hyper`                            | 1.x     | For `serve_connection` on an `Upgraded` stream.              |
| WebSocket       | `tokio-tungstenite`                | 0.26    | Both client and server; gives raw socket post-upgrade.       |
| Tunnel adapter  | `hyper::upgrade::on`               | (hyper) | Yields `Upgraded: AsyncRead + AsyncWrite`.                   |
| JSON            | `serde` + `serde_json`             | 1.x     | Obvious.                                                     |
| MIME            | `mime`                             | 0.3     | Content-type matching for Siren / NDJSON.                    |
| Siren           | `boardwalk-siren` (in-repo)            | —       | No maintained external crate.                                |
| KV store        | `redb`                             | 4.x     | Pure Rust, ACID, single-file, typed tables.                  |
| UUID            | `uuid`                             | 1.x     | Features: `v4`, `v7`, `serde`.                               |
| Tracing         | `tracing`                          | 0.1     | Plus `tracing-subscriber` 0.3, `tracing-appender`.           |
| Builder         | `bon`                              | 3.x     | Compile-time checked required fields.                        |
| Config          | `figment`                          | 0.10    | Layered TOML/env/CLI.                                         |
| CaQL parser     | `chumsky`                          | 1.x     | Best diagnostics; good for DSLs.                              |
| Regex           | `regex`                            | 1.x     | For topic regex segments and CaQL `like`.                     |
| TLS             | `rustls` + `tokio-rustls`          | 0.23/0.26 | Default to `aws-lc-rs` provider.                           |
| URL             | `url`                              | 2.x     | URL parsing for links and peer URLs.                          |
| Time            | `time` or `chrono`                 | latest  | Choose one — `time` 0.3 is sufficient and lighter. Default: `time`. |
| Errors          | `thiserror` + `anyhow`             | 1.x     | Typed library errors + ad-hoc runtime errors.                 |
| Async traits    | (native) + `async-trait` as needed | —       | Rust 1.75+ has AFIT; `async-trait` only where required.      |
| Backoff/jitter  | `backon`                           | 1.x     | Composable exponential backoff (or hand-roll; trivial).      |
| Testing         | `insta`, `rstest`, `tokio` (test)  | latest  | Snapshot Siren responses, parameterize.                       |
| Test HTTP       | `wiremock`                         | 0.6     | For peer-client behavioral tests.                             |

## Crates we considered and rejected

- **`actix-web`** — actor model conflicts with passing a raw `Upgraded`
  stream into a service. Axum's tower integration is simpler.
- **`fastwebsockets`** — perf is real but ergonomics and spec
  compliance trade-offs aren't worth it for this workload.
- **`sled`** — pre-1.0 with stalled format stability work.
- **`rusqlite`** — overkill; we have two registries, not a database.
- **`derive_builder`** — fine, but `bon` gives compile-time validation.
- **`pest`** — separate grammar file is a friction multiplier for a
  small DSL; chumsky AST integration is cleaner.
- **`nom`/`winnow`** — error messages weaker than chumsky's for a DSL
  end users will write by hand.

## Hard requirement check — HTTP/2 role reversal

This is the only "if it doesn't work we restart" dependency. Verified
from `h2` docs:

```rust
pub fn h2::server::handshake<T>(io: T) -> Handshake<T, Bytes>
    where T: AsyncRead + AsyncWrite + Unpin;

pub async fn h2::client::handshake<T>(io: T)
    -> Result<(SendRequest<Bytes>, Connection<T, Bytes>), Error>
    where T: AsyncRead + AsyncWrite + Unpin;
```

Both are generic over the IO type and use "prior knowledge" mode —
no Upgrade headers, no ALPN. We feed each function whichever side's
role it needs to play. Task #7 in the roadmap is a standalone
proof-of-concept that wires these two endpoints over a `tokio::io::duplex`
pair without any sockets at all, to validate the approach.
