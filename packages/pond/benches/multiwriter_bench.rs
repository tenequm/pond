#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Concurrent multi-writer OCC against a real object store - the proof
//! `s3_backend.rs` defers to "a real bucket" because s3s-fs implements
//! `If-None-Match: *` non-atomically and can't expose lost-write races.
//!
//! Models the shared-bucket fleet: N sandboxes each cron-running `pond sync`
//! into one bucket. Run one process per writer (each gets a disjoint id space
//! via `--writer-id`), then a `--verify-only` pass asserts the union landed -
//! a lost write shows up as a final count below `writers * rounds * sessions`.
//! Creds resolve from the user config's `[creds.*]` like any pond command.
//!
//!   # bootstrap the tables once (avoids the cold-start create race below),
//!   # then launch the writers concurrently against the same prefix:
//!   cargo bench --bench multiwriter_bench -- --s3-url s3+https://host/bucket/pfx --writer-id 0 --rounds 4 --sessions 50 &
//!   cargo bench --bench multiwriter_bench -- --s3-url s3+https://host/bucket/pfx --writer-id 1 --rounds 4 --sessions 50 &
//!   wait
//!   cargo bench --bench multiwriter_bench -- --s3-url s3+https://host/bucket/pfx --verify-only
//!
//! Cold-start race: if every writer opens a never-created table at once, only
//! one wins the create and the rest error `_versions not found`. It is
//! self-healing (they succeed once the table exists) and non-destructive, but
//! bootstrap with one writer first, and stagger cron minutes so concurrent
//! commits on one manifest don't drive OCC retry storms (tail latency).

use std::{path::PathBuf, time::Instant};

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use pond::{
    config::{self, Config},
    sessions::Store,
    substrate::RuntimeCaps,
    wire::{ProviderOptions, Session},
};

#[derive(Parser)]
#[command(about = "pond concurrent multi-writer OCC benchmark against an object store")]
struct Args {
    /// Remote storage URL (spec.md#storage-url-grammar).
    #[arg(long, value_name = "URL")]
    s3_url: String,
    /// This writer's disjoint id space: ids start at writer_id * 1_000_000, so
    /// concurrent writers never collide on content - only on manifests.
    #[arg(long, default_value_t = 0)]
    writer_id: usize,
    #[arg(long, default_value_t = 4)]
    rounds: usize,
    /// Sessions committed per round (one batched commit to the sessions table).
    #[arg(long, default_value_t = 50)]
    sessions: usize,
    /// Open the store and print row counts only - the union assertion after a
    /// fleet of writers has finished.
    #[arg(long)]
    verify_only: bool,
    #[arg(long, hide = true)]
    bench: bool,
}

fn make_session(id: usize) -> Session {
    Session {
        id: format!("01HXYMW{id:011}"),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(
            &serde_json::json!({"x": format!("/tmp/mw/{id}")}),
            "x",
        )
        .unwrap(),
        options: ProviderOptions::new(),
    }
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p).floor() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config_path = config::default_config_path(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    );
    let cfg = Config::load(&config_path)?;
    let storage = pond::substrate::StorageUrl::parse(&args.s3_url)?;
    let resolved = storage.resolve(&cfg.creds)?;
    let store = Store::open_with_options(
        resolved.lance_url(),
        resolved.options.clone(),
        RuntimeCaps::default(),
    )
    .await?;

    if args.verify_only {
        let (sessions, messages, parts) = store.row_counts().await?;
        println!("verify: sessions={sessions} messages={messages} parts={parts}");
        return Ok(());
    }

    let base = args.writer_id * 1_000_000;
    let mut commit_ms: Vec<u128> = Vec::with_capacity(args.rounds);
    let wall = Instant::now();
    for r in 0..args.rounds {
        let round_base = base + r * 10_000;
        let batch: Vec<Session> = (0..args.sessions)
            .map(|i| make_session(round_base + i))
            .collect();
        let t = Instant::now();
        store.upsert_sessions(&batch).await?;
        commit_ms.push(t.elapsed().as_millis());
    }
    let total = wall.elapsed().as_millis();
    commit_ms.sort_unstable();
    println!(
        "writer {:>2}: rounds={} sessions/round={} | round p50={}ms p95={}ms max={}ms | wall={}ms",
        args.writer_id,
        args.rounds,
        args.sessions,
        percentile(&commit_ms, 0.5),
        percentile(&commit_ms, 0.95),
        commit_ms.last().copied().unwrap_or(0),
        total,
    );
    Ok(())
}
