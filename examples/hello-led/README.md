# hello-led

Minimal in-process Resource/Actor example.

```sh
cargo run -p hello-led
```

The example builds a `Node` with `NodeBuilder`, registers
`boardwalk_mock_led::Led` as an `Actor`, queries it through
`NodeHandle`, drives the `turn-on` transition, and reads the explicitly
published `state` event.

This package does not start an HTTP server. Use the `Boardwalk` builder
when an actor should be reachable through `/resources`, `/events`, and
peer-forwarded routes; see `examples/job-runner` for that reusable
runtime path.
