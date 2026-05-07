# Crate Shortlist

The handful of crates that show up in almost every Rust application. One minimal example each. None are required; pull in as you need them.

For a curated wider catalog, see [blessed.rs](https://blessed.rs/crates).

## `serde` and `serde_json`

Serialization. Derive macros do everything.

```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct User {
    id: u64,
    email: String,
}

let json = r#"{"id": 1, "email": "a@b.com"}"#;
let user: User = serde_json::from_str(json)?;

let back = serde_json::to_string_pretty(&user)?;
```

Other formats: `serde_yaml`, `toml`, `bincode`, `serde_qs`, `rmp-serde` (MessagePack). Same derive, different crate.

Common attributes:
```rust
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]   // userId on the wire, user_id in Rust
struct Payload {
    user_id: u64,

    #[serde(default)]                // missing field uses Default::default()
    tags: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
}
```

## `tokio`

Async runtime. The default. See `async-basics.md` for the deep dive.

```toml
tokio = { version = "1", features = ["full"] }
```

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sleep = tokio::time::sleep(std::time::Duration::from_millis(100));
    sleep.await;
    Ok(())
}
```

## `anyhow`

App error handling. Use this in binaries.

```toml
anyhow = "1"
```

```rust
use anyhow::{Context, Result, bail};

fn run() -> Result<()> {
    let cfg = std::fs::read("config.toml").context("reading config.toml")?;
    if cfg.is_empty() {
        bail!("config is empty");
    }
    Ok(())
}
```

## `thiserror`

Library error enums.

```toml
thiserror = "2"
```

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("io error")]
    Io(#[from] std::io::Error),
}
```

## `clap`

CLI argument parsing. Derive-based; you write a struct, you have a CLI.

```toml
clap = { version = "4", features = ["derive"] }
```

```rust
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about = "A widget tool")]
struct Args {
    /// Path to the input file
    input: std::path::PathBuf,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Number of widgets to make
    #[arg(short, long, default_value_t = 1)]
    count: u32,
}

fn main() {
    let args = Args::parse();
    println!("{args:?}");
}
```

Subcommands:

```rust
#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand)]
enum Cmd {
    Add { path: String },
    Remove { path: String },
}
```

## `reqwest`

HTTP client. Async by default; the `blocking` feature gives a sync API.

```toml
reqwest = { version = "0.13", features = ["json"] }
```

```rust
#[derive(serde::Deserialize)]
struct Repo {
    full_name: String,
    stargazers_count: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("my-app/0.1")
        .build()?;

    let repo: Repo = client
        .get("https://api.github.com/repos/rust-lang/rust")
        .send()
        .await?
        .json()
        .await?;

    println!("{}: {} stars", repo.full_name, repo.stargazers_count);
    Ok(())
}
```

For tiny sync tools where you do not want a tokio dep, `ureq` is the lightweight alternative.

## `tracing` and `tracing-subscriber`

Structured logging. The default in 2026 for any async code (replaces `log`).

```toml
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

```rust
use tracing::{info, warn, error, instrument};
use tracing_subscriber::EnvFilter;

#[instrument]                                              // logs entry/exit
async fn handle(user_id: u64) -> anyhow::Result<()> {
    info!(user_id, "handling user");
    if user_id == 0 {
        warn!("got zero user");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())   // RUST_LOG=info
        .init();

    handle(1).await?;
    Ok(())
}
```

Run with `RUST_LOG=info cargo run`. Use `error!`, `warn!`, `info!`, `debug!`, `trace!`. Add structured fields by passing them as named args: `info!(user_id, action = "create", "user created")`.

## `axum`

Web framework. Built on `tokio` + `hyper` + `tower`. The 2026 default.

```toml
axum = "0.8"
tokio = { version = "1", features = ["full"] }
```

```rust
use axum::{routing::get, Router, Json};
use serde::Serialize;

#[derive(Serialize)]
struct Health { ok: bool }

async fn root() -> &'static str {
    "hello"
}

async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/users/{id}", get(|axum::extract::Path(id): axum::extract::Path<u64>| async move { format!("user {id}") }));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

Path params, query params, JSON body, state, middleware all extract via the `FromRequest`/`FromRequestParts` traits. The axum docs are excellent.

**0.8 breaking changes** (if you find a 0.7 tutorial): path captures use `/{id}` and `/{*rest}` instead of `/:id` and `/*rest`; `Option<T>` extractors require the new `OptionalFromRequestParts` trait; `Host` extractor moved to `axum-extra`; WebSocket `Message` uses `Bytes`/`Utf8Bytes` instead of `Vec<u8>`/`String`. MSRV is 1.78.

## `sqlx`

Async SQL with compile-time-checked queries. Postgres, MySQL, SQLite.

```toml
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "macros", "migrate"] }
```

```rust
use sqlx::PgPool;

#[derive(sqlx::FromRow)]
struct User {
    id: i64,
    email: String,
}

async fn find_user(pool: &PgPool, id: i64) -> sqlx::Result<Option<User>> {
    sqlx::query_as!(
        User,
        "SELECT id, email FROM users WHERE id = $1",
        id
    )
    .fetch_optional(pool)
    .await
}
```

The `query_as!` macro connects to your dev database at compile time to verify the SQL and types. To work offline (CI without a DB), run `cargo sqlx prepare` and commit the generated `.sqlx/` directory. (sqlx 0.6+ replaced the single `sqlx-data.json` file with this directory; older guides may still reference the old path.)

For multi-crate workspaces, run `cargo sqlx prepare --workspace` to produce a single `.sqlx/` at the workspace root. In CI, `cargo sqlx prepare --check` exits non-zero when stored metadata is stale. To force offline mode without the CLI, set `SQLX_OFFLINE=true`.

Migrations: `sqlx migrate add init`, write SQL, `sqlx migrate run`.

## `chrono` (and `jiff`)

Dates and times. As of Jan 2026 the chrono maintainer announced soft-deprecation and recommends `jiff` (BurntSushi) for new code. Reality in May 2026:

- `chrono` 0.4 is still production-safe and integrates cleanly with `serde`, `sqlx`, `serde_json`, and the rest of the ecosystem.
- `jiff` is the recommended successor, but still pre-1.0 (latest 0.2.x). Major projects are migrating (kube-rs done, arrow-rs proposed, jj-vcs, k8s-openapi).

Pick `chrono` if you need ecosystem integration today. Pick `jiff` for new code that can tolerate pre-1.0 churn and where you want correct timezone-aware arithmetic out of the box.

```toml
chrono = { version = "0.4", features = ["serde"] }
# or
jiff   = { version = "0.2", features = ["serde"] }
```

```rust
// chrono
use chrono::{DateTime, Utc};

let now: DateTime<Utc> = Utc::now();
let parsed: DateTime<Utc> = "2026-04-29T12:00:00Z".parse()?;
let in_an_hour = now + chrono::Duration::hours(1);

// jiff (equivalent)
use jiff::Timestamp;

let now: Timestamp = Timestamp::now();
let parsed: Timestamp = "2026-04-29T12:00:00Z".parse()?;
let in_an_hour = now + 1.hour();
```

## Honorable Mentions

Not on the day-1 list, but you will run into these:

| Crate | Use |
|---|---|
| `rayon` | Parallel iterators. `par_iter()` on a `Vec` and your CPU cores light up |
| `regex` | Regular expressions |
| `uuid` | UUID generation and parsing |
| `dotenvy` | Load `.env` into environment variables |
| `config` | Layered config (file + env + CLI) |
| `bytes` | Efficient byte buffer handling |
| `futures` | Future combinators not in `std` |
| `parking_lot` | Faster `Mutex`/`RwLock` than `std::sync` (in some workloads) |
| `dashmap` | Concurrent hashmap |
| `indexmap` | Hashmap that preserves insertion order |
| `once_cell` / `LazyLock` (std 1.80+) | Lazy global initialization |
| `crossbeam-channel` | Multi-producer, multi-consumer sync channels |
| `tower` | Service abstraction (used by axum middleware) |
| `tonic` | gRPC |
| `redis` | Redis client |
| `mongodb` | MongoDB driver |
