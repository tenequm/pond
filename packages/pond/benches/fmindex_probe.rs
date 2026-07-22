#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Substring-index probe over the real `parts.variant_data` corpus: FM-Index vs
//! ngram vs FTS, on-disk size / build cost / query latency / resident RSS. This
//! is the regression tool behind issue #47 - it measures why substring search on
//! tool bodies wants ngram on a narrow Utf8 column, not FM on `variant_data`.
//!
//! Run against a copy of a real store's `parts.lance` (never the reference copy):
//!   prep  : decode variant_data (JSONB) to a plain Utf8 `text` column and write
//!           a fresh dataset - both ngram and SQL `contains` require Utf8/LargeUtf8,
//!           so this is the apples-to-apples substrate for the comparison.
//!   build : build fm|ngram|fts on the `text` column; report build wall-time,
//!           peak RSS, and the `_indices/` on-disk size delta.
//!   query : fresh process - `contains(text, <needle>)` count (fm/ngram); reports
//!           latency, hits, and the resident-RSS delta (the mmap-vs-heap answer).
//!   ftsq  : fresh process - FTS `match` over `<needle>` (token search, not
//!           substring); reports the same, to expose the token-vs-substring gap.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use lance::Dataset;
use lance::dataset::WriteParams;
use lance::deps::arrow_array::{
    Array, BinaryArray, LargeBinaryArray, LargeStringArray, RecordBatch, RecordBatchIterator,
    StringArray,
};
use lance::deps::arrow_schema::{DataType, Field, Schema};
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::scalar::{
    BuiltinIndexType, FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams,
};
use tokio_stream::StreamExt;

const SRC_COL: &str = "variant_data";
const TEXT_COL: &str = "text";

fn rss_kb() -> u64 {
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn spawn_rss_sampler(peak: Arc<AtomicU64>, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            peak.fetch_max(rss_kb(), Ordering::Relaxed);
            std::thread::sleep(std::time::Duration::from_millis(150));
        }
    })
}

fn dir_size_bytes(path: &Path) -> u64 {
    let mut total = 0;
    let Ok(rd) = std::fs::read_dir(path) else {
        return 0;
    };
    for entry in rd.flatten() {
        let p = entry.path();
        if let Ok(md) = entry.metadata() {
            total += if md.is_dir() {
                dir_size_bytes(&p)
            } else {
                md.len()
            };
        }
    }
    total
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn col_to_strings(arr: &dyn Array) -> Result<StringArray> {
    let mut b = lance::deps::arrow_array::builder::StringBuilder::new();
    macro_rules! push_all {
        ($a:expr) => {
            for i in 0..$a.len() {
                if $a.is_valid(i) {
                    b.append_value($a.value(i));
                } else {
                    b.append_null();
                }
            }
        };
    }
    match arr.data_type() {
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            push_all!(a);
        }
        DataType::LargeUtf8 => {
            let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            push_all!(a);
        }
        DataType::Binary => {
            let a = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
            for i in 0..a.len() {
                if a.is_valid(i) {
                    b.append_value(String::from_utf8_lossy(a.value(i)));
                } else {
                    b.append_null();
                }
            }
        }
        DataType::LargeBinary => {
            let a = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            for i in 0..a.len() {
                if a.is_valid(i) {
                    b.append_value(String::from_utf8_lossy(a.value(i)));
                } else {
                    b.append_null();
                }
            }
        }
        other => anyhow::bail!("unexpected variant_data type: {other:?}"),
    }
    Ok(b.finish())
}

async fn prep(src: &str, dst: &str) -> Result<()> {
    let ds = Dataset::open(src).await.context("open src")?;
    let mut scanner = ds.scan();
    scanner.project(&[SRC_COL])?;
    let mut stream = scanner.try_into_stream().await?;

    let schema = Arc::new(Schema::new(vec![Field::new(
        TEXT_COL,
        DataType::Utf8,
        true,
    )]));
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut raw_bytes: u64 = 0;
    let mut nonnull: u64 = 0;
    let mut printed = false;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let arr = batch.column(0);
        if !printed {
            println!("src variant_data ty    = {:?}", arr.data_type());
            printed = true;
        }
        let s = col_to_strings(arr.as_ref())?;
        for i in 0..s.len() {
            if s.is_valid(i) {
                raw_bytes += s.value(i).len() as u64;
                nonnull += 1;
            }
        }
        batches.push(RecordBatch::try_new(schema.clone(), vec![Arc::new(s)])?);
    }
    println!("nonnull                = {nonnull}");
    println!(
        "text raw bytes         = {:.1} MiB ({raw_bytes} B)",
        mib(raw_bytes)
    );

    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(reader, dst, Some(WriteParams::default()))
        .await
        .context("write text dataset")?;
    println!("wrote Utf8 dataset     = {dst}");
    Ok(())
}

fn index_kind(kind: &str) -> Result<(IndexType, BuiltinIndexType, String)> {
    match kind {
        "fm" => Ok((IndexType::Fm, BuiltinIndexType::Fm, "text_fm".into())),
        "ngram" => Ok((
            IndexType::NGram,
            BuiltinIndexType::NGram,
            "text_ngram".into(),
        )),
        "fts" => Ok((
            IndexType::Inverted,
            BuiltinIndexType::Inverted,
            "text_fts".into(),
        )),
        other => anyhow::bail!("unknown index kind: {other} (want fm|ngram|fts)"),
    }
}

async fn build(path: &str, kind: &str) -> Result<()> {
    let (index_type, builtin, index_name) = index_kind(kind)?;
    let peak = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = spawn_rss_sampler(peak.clone(), stop.clone());

    let mut ds = Dataset::open(path).await.context("open dataset")?;
    let rows = ds.count_rows(None).await?;
    println!("rows                   = {rows}");

    let indices_dir = Path::new(path).join("_indices");
    let before = dir_size_bytes(&indices_dir);

    println!("building {kind}-Index ...");
    let started = Instant::now();
    if kind == "fts" {
        let params = InvertedIndexParams::default()
            .base_tokenizer("simple".to_owned())
            .stem(true)
            .remove_stop_words(false);
        ds.create_index_builder(&[TEXT_COL], IndexType::Inverted, &params)
            .name("text_fts".to_owned())
            .replace(true)
            .await
            .context("create fts index")?;
    } else {
        let params = ScalarIndexParams::for_builtin(builtin);
        ds.create_index_builder(&[TEXT_COL], index_type, &params)
            .name(index_name)
            .replace(true)
            .await
            .with_context(|| format!("create {kind} index"))?;
    }
    let build_secs = started.elapsed().as_secs_f64();

    let after = dir_size_bytes(&indices_dir);
    let index_bytes = after.saturating_sub(before);

    stop.store(true, Ordering::Relaxed);
    let _ = sampler.join();

    println!("--- RESULTS (build {kind}) ---");
    println!("build wall-time        = {build_secs:.1} s");
    println!(
        "peak process RSS       = {:.0} MiB",
        peak.load(Ordering::Relaxed) as f64 / 1024.0
    );
    println!(
        "index on-disk size     = {:.1} MiB ({index_bytes} B)",
        mib(index_bytes)
    );
    Ok(())
}

async fn query(path: &str, needle: &str) -> Result<()> {
    let ds = Dataset::open(path).await.context("open dataset")?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let base_kb = rss_kb();

    let filter = format!("contains({TEXT_COL}, '{}')", needle.replace('\'', "''"));
    println!("filter                 = {filter}");
    let started = Instant::now();
    let count = ds
        .count_rows(Some(filter))
        .await
        .context("count_rows(contains)")?;
    let secs = started.elapsed().as_secs_f64();

    let mut after_kb = rss_kb();
    for _ in 0..6 {
        after_kb = after_kb.max(rss_kb());
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    }

    println!("--- RESULTS (query) ---");
    println!("hits                   = {count}");
    println!("query latency          = {secs:.3} s");
    println!(
        "RSS baseline           = {:.0} MiB",
        base_kb as f64 / 1024.0
    );
    println!(
        "RSS after query        = {:.0} MiB",
        after_kb as f64 / 1024.0
    );
    println!(
        "RSS delta (resident)   = {:.0} MiB",
        after_kb.saturating_sub(base_kb) as f64 / 1024.0
    );
    Ok(())
}

async fn ftsq(path: &str, needle: &str) -> Result<()> {
    let ds = Dataset::open(path).await.context("open dataset")?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let base_kb = rss_kb();

    let fts = FullTextSearchQuery::new(needle.to_owned()).with_column(TEXT_COL.to_owned())?;
    println!("fts match terms        = {needle:?} (simple+stem, OR of terms)");
    let started = Instant::now();
    let mut scanner = ds.scan();
    scanner.full_text_search(fts)?;
    scanner.project::<&str>(&[])?;
    let count = scanner.count_rows().await.context("fts count_rows")?;
    let secs = started.elapsed().as_secs_f64();

    let mut after_kb = rss_kb();
    for _ in 0..6 {
        after_kb = after_kb.max(rss_kb());
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    }
    println!("--- RESULTS (fts query) ---");
    println!("hits                   = {count}");
    println!("query latency          = {secs:.3} s");
    println!(
        "RSS baseline           = {:.0} MiB",
        base_kb as f64 / 1024.0
    );
    println!(
        "RSS after query        = {:.0} MiB",
        after_kb as f64 / 1024.0
    );
    println!(
        "RSS delta (resident)   = {:.0} MiB",
        after_kb.saturating_sub(base_kb) as f64 / 1024.0
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("");
    match mode {
        "prep" => {
            let src = args
                .get(2)
                .context("prep <src parts.lance> <dst text.lance>")?;
            let dst = args
                .get(3)
                .context("prep <src parts.lance> <dst text.lance>")?;
            prep(src, dst).await
        }
        "build" => {
            let path = args.get(2).context("build <text.lance> <fm|ngram>")?;
            let kind = args.get(3).map(|s| s.as_str()).unwrap_or("fm");
            build(path, kind).await
        }
        "query" => {
            let path = args.get(2).context("query <text.lance> <needle>")?;
            let needle = args
                .get(3)
                .map(|s| s.as_str())
                .unwrap_or("rustup override set");
            query(path, needle).await
        }
        "ftsq" => {
            let path = args.get(2).context("ftsq <text.lance> <needle>")?;
            let needle = args
                .get(3)
                .map(|s| s.as_str())
                .unwrap_or("rustup override set");
            ftsq(path, needle).await
        }
        _ => anyhow::bail!("usage: fmindex_probe <prep|build|query> ..."),
    }
}
