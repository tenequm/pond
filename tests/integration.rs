// One binary for all integration tests so `cargo test` links once and
// libtest's thread pool covers the whole suite. Each former
// `tests/<name>.rs` is now a module under `tests/integration/<name>.rs`.
// `tests/integration.rs` is itself a test-binary crate root, so children
// at the same directory level need explicit `#[path]` to point inside the
// `integration/` folder.

// x86_64-linux trait-solving evaluates the `Send` bound on the create-table
// future deeper than aarch64-darwin, overflowing the default limit of 128
// (E0275) when this binary links the index-fold + search suites together.
#![recursion_limit = "512"]

#[path = "integration/adapter/mod.rs"]
mod adapter;
#[path = "integration/copy.rs"]
mod copy;
#[path = "integration/embed.rs"]
mod embed;
#[path = "integration/index_fold.rs"]
mod index_fold;
#[path = "integration/lance_smoke.rs"]
mod lance_smoke;
#[path = "integration/optimize_under_contention.rs"]
mod optimize_under_contention;
#[path = "integration/recovery.rs"]
mod recovery;
#[path = "integration/remote_backend.rs"]
mod remote_backend;
#[path = "integration/s3_backend.rs"]
mod s3_backend;
#[path = "integration/search.rs"]
mod search;
#[path = "integration/session_scoped_pk.rs"]
mod session_scoped_pk;
#[path = "integration/sql.rs"]
mod sql;
#[path = "integration/store_concurrency.rs"]
mod store_concurrency;
#[path = "integration/sync.rs"]
mod sync;
#[path = "integration/transport_http.rs"]
mod transport_http;
#[path = "integration/transport_mcp.rs"]
mod transport_mcp;
