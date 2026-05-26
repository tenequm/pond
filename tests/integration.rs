// One binary for all integration tests so `cargo test` links once and
// libtest's thread pool covers the whole suite. Each former
// `tests/<name>.rs` is now a module under `tests/integration/<name>.rs`.
// `tests/integration.rs` is itself a test-binary crate root, so children
// at the same directory level need explicit `#[path]` to point inside the
// `integration/` folder.

#[path = "integration/claude_code_ingest.rs"]
mod claude_code_ingest;
#[path = "integration/embed.rs"]
mod embed;
#[path = "integration/lance_smoke.rs"]
mod lance_smoke;
#[path = "integration/recovery.rs"]
mod recovery;
#[path = "integration/remote_backend.rs"]
mod remote_backend;
#[path = "integration/restore.rs"]
mod restore;
#[path = "integration/search.rs"]
mod search;
#[path = "integration/session_scoped_pk.rs"]
mod session_scoped_pk;
#[path = "integration/store_concurrency.rs"]
mod store_concurrency;
#[path = "integration/transport_http.rs"]
mod transport_http;
#[path = "integration/transport_mcp.rs"]
mod transport_mcp;
