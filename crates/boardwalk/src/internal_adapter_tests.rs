#[path = "../tests/internal/apps.rs"]
mod apps;
#[path = "../tests/internal/event_envelope_minting.rs"]
mod event_envelope_minting;
#[path = "../tests/internal/event_envelope_state_change.rs"]
mod event_envelope_state_change;
#[path = "../tests/internal/event_id_to_stream_replay.rs"]
mod event_id_to_stream_replay;
#[path = "../tests/internal/factory.rs"]
mod factory;
#[path = "../tests/internal/http.rs"]
mod http;
#[path = "../tests/internal/http_stream_eager_unsub.rs"]
mod http_stream_eager_unsub;
#[path = "../tests/internal/http_stream_slow_consumer_gap.rs"]
mod http_stream_slow_consumer_gap;
#[path = "../tests/internal/metadata.rs"]
mod metadata;
#[path = "../tests/internal/observe.rs"]
mod observe;
#[path = "../tests/internal/peer.rs"]
mod peer;
#[path = "../tests/internal/peer_broadcast_lag_emits_stream_gap.rs"]
mod peer_broadcast_lag_emits_stream_gap;
#[path = "../tests/internal/peer_event_wire_shape.rs"]
mod peer_event_wire_shape;
#[path = "../tests/internal/peer_ndjson_envelope_preserved.rs"]
mod peer_ndjson_envelope_preserved;
#[path = "../tests/internal/peer_resources.rs"]
mod peer_resources;
#[path = "../tests/internal/persist.rs"]
mod persist;
#[path = "../tests/internal/replay_records_on_publish.rs"]
mod replay_records_on_publish;
#[path = "../tests/internal/resource_actor_http_core.rs"]
mod resource_actor_http_core;
#[path = "../tests/internal/resource_event_wire_shape.rs"]
mod resource_event_wire_shape;
#[path = "../tests/internal/resource_hypermedia.rs"]
mod resource_hypermedia;
#[path = "../tests/internal/resource_query.rs"]
mod resource_query;
#[path = "../tests/internal/resource_snapshot_shape.rs"]
mod resource_snapshot_shape;
#[path = "../tests/internal/resource_transitions.rs"]
mod resource_transitions;
#[path = "../tests/internal/resource_ws_lifecycle.rs"]
mod resource_ws_lifecycle;
#[path = "../tests/internal/scout.rs"]
mod scout;
#[path = "../tests/internal/shutdown.rs"]
mod shutdown;
#[path = "../tests/internal/streams.rs"]
mod streams;
#[path = "../tests/internal/tls.rs"]
mod tls;
#[path = "../tests/internal/ws_bounded_outbound.rs"]
mod ws_bounded_outbound;
#[path = "../tests/internal/ws_subscription_cap.rs"]
mod ws_subscription_cap;
