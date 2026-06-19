#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]
#![allow(unreachable_pub, dead_code)]

//! FTS tokenizer *quality* bench: the durable, in-tree successor to the
//! `docs/researches/tokenizer-experiment-*` harness (originally a temp
//! `POND_EXP_*`-env reindex + `/tmp` `run_config.sh`/`score.py`, recovered from
//! pond's own session history and reimplemented here so it survives).
//!
//! For each candidate tokenizer config it rebuilds the `search_text` inverted
//! index in place, runs the frozen query set, and reports Success@K / P@1 / MRR
//! per stratum so a tokenizer change can be judged on real retrieval quality -
//! not just index size / RAM (that is `serve_mem_bench --tokenizer-sweep`).
//!
//! Configs (the recovered T0-T4 matrix; T5 = RRF(T1 ngram, T4 word) derived):
//!   T0 ngram 3-3  (the original production control)
//!   T1 ngram 3-5  (the former production tokenizer, before word+stem)
//!   T2 ngram 4-5  (drops short-token coverage)
//!   T3 simple     (word tokenizer + ascii-folding, no stemming)
//!   T4 simple+stem(English) + ascii-folding
//!
//! Ground truth (`docs/researches/tokenizer-experiment-queries.tsv`): EN queries
//! pin a session-id `prefix:`; UK queries carry an `anchor:` phrase resolved at
//! runtime to the message-ids whose `search_text` contains it (so the harness is
//! corpus-portable - point it at any `--storage-path`/`--data-dir`).
//!
//! Scoring adaptation vs the 2026 original: today's `pond search` groups hits by
//! session, and EN ground truth is a session prefix, so Success@K is scored over
//! the top-K *sessions* (the agent injects 1-3 whole sessions), not flat message
//! hits. This is the faithful modern analog of the recovered `score.py`.
//!
//! Run:
//!   cargo bench --bench tokenizer_quality_bench -- --data-dir /tmp/pond-full-corpus
//!   cargo bench --bench tokenizer_quality_bench -- --storage-path s3+https://host/bucket/prefix

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::scalar::InvertedIndexParams;
use pond::{
    PROTOCOL_VERSION,
    config::{Config, SearchConfig},
    embed::LazyEmbedder,
    handlers::pond_search,
    sessions::Store,
    sql::{self, Mode, Tables},
    substrate::{ResolvedStorage, RuntimeCaps, StorageUrl, Table},
    wire::{SearchEnvelope, SearchFilters, SearchModeWire, SearchRequest},
};

#[derive(Parser)]
#[command(about = "FTS tokenizer quality bench: Success@K per stratum across tokenizers")]
struct Args {
    /// Local pond data dir to open (and rebuild indexes in). Mutually exclusive
    /// with --storage-path. The dir's indexes are rebuilt in place - point it at
    /// a throwaway copy, never production.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Remote store URL; creds resolved from --config. Indexes are rebuilt in
    /// place on the remote store - use a benchmarking copy, not the live store.
    #[arg(long)]
    storage_path: Option<String>,
    /// Config file for creds (default `~/.config/pond/config.toml`).
    #[arg(long)]
    config: Option<PathBuf>,
    /// Frozen query set (id, lang, stratum, query, ground_truth).
    #[arg(
        long,
        default_value = "docs/researches/tokenizer-experiment-queries.tsv"
    )]
    queries: PathBuf,
    /// Top-K sessions considered a "hit window" for Success@K.
    #[arg(long, default_value_t = 3)]
    success_at: usize,
    /// Sessions per search (the retrieval depth scored over).
    #[arg(long, default_value_t = 10)]
    limit: usize,
    /// Restrict the run to a comma-separated subset of config ids by prefix
    /// (e.g. `--only T1,T4` to compare current ngram vs word+stem only). Each
    /// rebuild is minutes on 2M rows, so this is the fast path for a targeted
    /// re-measure. Default: the full matrix.
    #[arg(long)]
    only: Option<String>,
    /// Ignored; absorbs the `--bench` cargo passes to `harness = false` targets.
    #[arg(long, hide = true)]
    bench: bool,
}

impl Args {
    /// The matrix rows this run will build, honoring `--only`.
    fn selected(&self) -> Vec<&'static TokConfig> {
        match &self.only {
            None => MATRIX.iter().collect(),
            Some(list) => {
                let wanted: Vec<&str> = list.split(',').map(str::trim).collect();
                MATRIX
                    .iter()
                    .filter(|tc| wanted.iter().any(|w| tc.id.starts_with(w)))
                    .collect()
            }
        }
    }
}

#[derive(Clone)]
struct TokConfig {
    id: &'static str,
    base: &'static str,
    nmin: u32,
    nmax: u32,
    fold: bool,
    stem: bool,
}

/// The recovered T0-T4 matrix. T5 is derived offline by RRF, not built.
const MATRIX: &[TokConfig] = &[
    TokConfig {
        id: "T0 ngram3-3",
        base: "ngram",
        nmin: 3,
        nmax: 3,
        fold: false,
        stem: false,
    },
    TokConfig {
        id: "T1 ngram3-5",
        base: "ngram",
        nmin: 3,
        nmax: 5,
        fold: false,
        stem: false,
    },
    TokConfig {
        id: "T2 ngram4-5",
        base: "ngram",
        nmin: 4,
        nmax: 5,
        fold: false,
        stem: false,
    },
    TokConfig {
        id: "T3 simple",
        base: "simple",
        nmin: 0,
        nmax: 0,
        fold: true,
        stem: false,
    },
    TokConfig {
        id: "T4 simple+stem",
        base: "simple",
        nmin: 0,
        nmax: 0,
        fold: true,
        stem: true,
    },
];

const CONFIG_IDS: &[&str] = &[
    "T0 ngram3-3",
    "T1 ngram3-5",
    "T2 ngram4-5",
    "T3 simple",
    "T4 simple+stem",
    "T5 RRF(T1,T4)",
];

enum GroundTruth {
    /// EN: a hit session matches if its id starts with one of these 8-char prefixes.
    Prefix(Vec<String>),
    /// UK: an `anchor:` phrase, resolved to the message-ids that contain it.
    Anchor(String, HashSet<String>),
}

struct Query {
    id: String,
    lang: String,
    stratum: String,
    query: String,
    gt: GroundTruth,
}

enum OpenTarget {
    Local(PathBuf),
    Remote(Box<ResolvedStorage>),
}

fn resolve_target(args: &Args) -> Result<OpenTarget> {
    if let Some(sp) = &args.storage_path {
        let path = args.config.clone().unwrap_or_else(|| {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_default();
            home.join(".config").join("pond").join("config.toml")
        });
        let config =
            Config::load(&path).with_context(|| format!("load config {}", path.display()))?;
        let url = StorageUrl::parse(sp).context("parse --storage-path")?;
        Ok(OpenTarget::Remote(Box::new(
            url.resolve(&config.creds).context("resolve creds")?,
        )))
    } else {
        let dir = args
            .data_dir
            .clone()
            .context("pass --data-dir (local copy) or --storage-path")?;
        Ok(OpenTarget::Local(dir))
    }
}

async fn open_store(target: &OpenTarget) -> Result<Store> {
    match target {
        OpenTarget::Local(dir) => Ok(Store::open_local(dir).await?),
        OpenTarget::Remote(r) => {
            Ok(
                Store::open_with_options(r.lance_url(), r.options.clone(), RuntimeCaps::default())
                    .await?,
            )
        }
    }
}

async fn open_messages_dataset(target: &OpenTarget) -> Result<Dataset> {
    match target {
        OpenTarget::Local(dir) => {
            let p = dir.join("messages.lance");
            Ok(Dataset::open(p.to_str().context("non-utf8 path")?).await?)
        }
        OpenTarget::Remote(r) => {
            let base = r.lance_url().as_str().trim_end_matches('/');
            Ok(DatasetBuilder::from_uri(format!("{base}/messages.lance"))
                .with_storage_options(r.options.clone())
                .load()
                .await?)
        }
    }
}

/// Rebuild the `search_text` FTS index under `cfg`, reusing pond's production
/// index name so it replaces the existing one (no duplicate inverted indexes).
async fn rebuild_index(dataset: &mut Dataset, cfg: &TokConfig) -> Result<()> {
    let mut params = InvertedIndexParams::default()
        .base_tokenizer(cfg.base.to_owned())
        .lower_case(true)
        .ascii_folding(cfg.fold)
        .stem(cfg.stem)
        .remove_stop_words(false);
    if cfg.base == "ngram" {
        params = params.ngram_min_length(cfg.nmin).ngram_max_length(cfg.nmax);
    }
    dataset
        .create_index_builder(&["search_text"], IndexType::Inverted, &params)
        .name("messages_search_text_fts".to_owned())
        .replace(true)
        .await?;
    Ok(())
}

fn parse_queries(path: &std::path::Path) -> Result<Vec<Query>> {
    let body = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut out = Vec::new();
    for line in body.lines().skip(1) {
        if line.trim().is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        if f.len() < 5 {
            continue;
        }
        let (id, lang, stratum, query, gt_raw) = (f[0], f[1], f[2], f[3], f[4]);
        let gt = if let Some(rest) = gt_raw.strip_prefix("prefix:") {
            GroundTruth::Prefix(
                rest.split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect(),
            )
        } else if let Some(anchor) = gt_raw.strip_prefix("anchor:") {
            GroundTruth::Anchor(anchor.to_owned(), HashSet::new())
        } else {
            continue;
        };
        out.push(Query {
            id: id.to_owned(),
            lang: lang.to_owned(),
            stratum: stratum.to_owned(),
            query: query.to_owned(),
            gt,
        });
    }
    Ok(out)
}

/// Resolve each `anchor:` query's target message-ids by scanning `search_text`
/// for the phrase via pond's own read-only SQL (corpus-portable, no extra deps).
async fn resolve_anchors(store: &Store, queries: &mut [Query]) -> Result<()> {
    let tables = Tables {
        sessions: Some(store.dataset(Table::Sessions).await?),
        messages: Some(store.dataset(Table::Messages).await?),
        parts: Some(store.dataset(Table::Parts).await?),
    };
    for q in queries.iter_mut() {
        if let GroundTruth::Anchor(anchor, set) = &mut q.gt {
            let escaped = anchor.replace('\'', "''");
            let sql =
                format!("SELECT message_id FROM messages WHERE search_text LIKE '%{escaped}%'");
            match sql::run(&tables, &sql, Mode::InlineJson, 1000).await {
                Ok(sql::Outcome::InlineJson(json)) => {
                    if let Some(rows) = json.get("rows").and_then(|r| r.as_array()) {
                        for row in rows {
                            if let Some(id) = row.get("message_id").and_then(|v| v.as_str()) {
                                set.insert(id.to_owned());
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => anyhow::bail!("anchor resolve {:?}: {e:?}", q.id),
            }
        }
    }
    Ok(())
}

fn search_request(query: &str, limit: usize) -> SearchRequest {
    SearchRequest {
        protocol_version: PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query: query.to_owned(),
        mode: SearchModeWire::Fts,
        sort_by: pond::wire::SortBy::Relevance,
        filters: SearchFilters::default(),
        limit,
    }
}

/// Ranked top sessions for a query: `(session_id, [message_ids])` in result order.
async fn ranked_sessions(
    store: &Store,
    embedder: &LazyEmbedder,
    cfg: &SearchConfig,
    query: &str,
    limit: usize,
) -> Result<Vec<(String, Vec<String>)>> {
    match pond_search(store, embedder, search_request(query, limit), cfg).await {
        SearchEnvelope::Success(resp) => Ok(resp
            .sessions
            .into_iter()
            .map(|s| {
                (
                    s.session_id,
                    s.matches.into_iter().map(|m| m.message_id).collect(),
                )
            })
            .collect()),
        SearchEnvelope::Error(e) => anyhow::bail!("search {query:?} failed: {e:?}"),
    }
}

/// 1-based rank of the first session matching ground truth, else 0.
fn first_match_rank(gt: &GroundTruth, ranked: &[(String, Vec<String>)]) -> usize {
    for (i, (sid, mids)) in ranked.iter().enumerate() {
        let hit = match gt {
            GroundTruth::Prefix(prefixes) => prefixes.iter().any(|p| sid.starts_with(p.as_str())),
            GroundTruth::Anchor(_, set) => mids.iter().any(|m| set.contains(m)),
        };
        if hit {
            return i + 1;
        }
    }
    0
}

/// RRF-fuse two ranked session lists (T1 ngram weight 1.0, T4 word weight 0.4,
/// k=60 - the recovered score.py weighting).
fn rrf(a: &[(String, Vec<String>)], b: &[(String, Vec<String>)]) -> Vec<(String, Vec<String>)> {
    let mut score: HashMap<&str, f64> = HashMap::new();
    let mut mids: HashMap<&str, &Vec<String>> = HashMap::new();
    for (list, w) in [(a, 1.0_f64), (b, 0.4_f64)] {
        for (i, (sid, ms)) in list.iter().enumerate() {
            *score.entry(sid.as_str()).or_insert(0.0) += w / (60.0 + (i as f64) + 1.0);
            mids.entry(sid.as_str()).or_insert(ms);
        }
    }
    let mut ranked: Vec<&str> = score.keys().copied().collect();
    ranked.sort_by(|x, y| {
        score[y]
            .partial_cmp(&score[x])
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked
        .into_iter()
        .map(|sid| (sid.to_owned(), mids[sid].clone()))
        .collect()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let target = resolve_target(&args)?;
    let cfg = SearchConfig::default();
    let embedder = LazyEmbedder::candle();

    let mut queries = parse_queries(&args.queries)?;
    println!("=== FTS tokenizer quality bench ===");
    println!(
        "queries          {} from {}",
        queries.len(),
        args.queries.display()
    );
    println!(
        "success@{}, top-{} sessions/query",
        args.success_at, args.limit
    );
    println!();

    // Resolve UK anchors once (LIKE scan, tokenizer-independent).
    {
        let store = open_store(&target).await?;
        resolve_anchors(&store, &mut queries).await?;
    }
    for q in &queries {
        if let GroundTruth::Anchor(a, set) = &q.gt
            && set.is_empty()
        {
            println!(
                "  WARN {}: anchor {a:?} resolved to 0 messages in this corpus",
                q.id
            );
        }
    }

    // ranks[config_id][query_id] = 1-based first-match rank (0 = miss).
    let mut ranks: HashMap<String, HashMap<String, usize>> = HashMap::new();
    // Keep T1 and T4 ranked lists per query to derive T5.
    let mut t1: HashMap<String, Vec<(String, Vec<String>)>> = HashMap::new();
    let mut t4: HashMap<String, Vec<(String, Vec<String>)>> = HashMap::new();

    for tc in args.selected() {
        eprintln!("[{}] rebuilding index...", tc.id);
        let mut ds = open_messages_dataset(&target).await?;
        rebuild_index(&mut ds, tc).await?;
        drop(ds);
        let store = open_store(&target).await?;
        let mut per_query = HashMap::new();
        for q in &queries {
            let ranked = ranked_sessions(&store, &embedder, &cfg, &q.query, args.limit).await?;
            per_query.insert(q.id.clone(), first_match_rank(&q.gt, &ranked));
            if tc.id == "T1 ngram3-5" {
                t1.insert(q.id.clone(), ranked);
            } else if tc.id == "T4 simple+stem" {
                t4.insert(q.id.clone(), ranked);
            }
        }
        ranks.insert(tc.id.to_owned(), per_query);
        drop(store);
    }

    // T5 = RRF(T1, T4), scored offline.
    let mut t5 = HashMap::new();
    for q in &queries {
        let fused = rrf(
            t1.get(&q.id).map_or(&[][..], |v| v),
            t4.get(&q.id).map_or(&[][..], |v| v),
        );
        t5.insert(q.id.clone(), first_match_rank(&q.gt, &fused));
    }
    ranks.insert("T5 RRF(T1,T4)".to_owned(), t5);

    // Only report the configs that actually ran (honors `--only`), in the
    // canonical CONFIG_IDS order. T5 is always present (derived above).
    let cols: Vec<&str> = CONFIG_IDS
        .iter()
        .copied()
        .filter(|cid| ranks.contains_key(*cid))
        .collect();

    // Per-stratum Success@K table (rows = strata, cols = configs).
    let mut strata: Vec<(String, String)> = Vec::new();
    for q in &queries {
        let key = (q.lang.clone(), q.stratum.clone());
        if !strata.contains(&key) {
            strata.push(key);
        }
    }

    let succ = |cid: &str, lang: &str, stratum: &str| -> (usize, usize) {
        let qs: Vec<&Query> = queries
            .iter()
            .filter(|q| q.lang == lang && q.stratum == stratum)
            .collect();
        let hits = qs
            .iter()
            .filter(|q| {
                let r = ranks[cid].get(&q.id).copied().unwrap_or(0);
                r >= 1 && r <= args.success_at
            })
            .count();
        (hits, qs.len())
    };

    print!("{:<22}", "stratum (n)");
    for cid in &cols {
        print!("{cid:>16}");
    }
    println!();
    println!("{}", "-".repeat(22 + 16 * cols.len()));
    for (lang, stratum) in &strata {
        let n = queries
            .iter()
            .filter(|q| &q.lang == lang && &q.stratum == stratum)
            .count();
        print!("{:<22}", format!("{lang}/{stratum} ({n})"));
        for cid in &cols {
            let (h, _) = succ(cid, lang, stratum);
            print!("{:>16}", format!("{h}/{n}"));
        }
        println!();
    }
    println!("{}", "-".repeat(22 + 16 * cols.len()));
    // Totals + per-language totals (Success@K).
    for label in ["en", "uk", "ALL"] {
        print!("{:<22}", format!("{label} total"));
        for cid in &cols {
            let qs: Vec<&Query> = queries
                .iter()
                .filter(|q| label == "ALL" || q.lang == label)
                .collect();
            let hits = qs
                .iter()
                .filter(|q| {
                    let r = ranks[*cid].get(&q.id).copied().unwrap_or(0);
                    r >= 1 && r <= args.success_at
                })
                .count();
            print!("{:>16}", format!("{}/{}", hits, qs.len()));
        }
        println!();
    }

    // JSON for diffing across runs.
    let mut json_rows = Vec::new();
    for cid in &cols {
        let p1 = queries
            .iter()
            .filter(|q| ranks[*cid].get(&q.id).copied().unwrap_or(0) == 1)
            .count();
        let s_at = queries
            .iter()
            .filter(|q| {
                let r = ranks[*cid].get(&q.id).copied().unwrap_or(0);
                r >= 1 && r <= args.success_at
            })
            .count();
        let mrr: f64 = queries
            .iter()
            .map(|q| match ranks[*cid].get(&q.id).copied().unwrap_or(0) {
                0 => 0.0,
                r => 1.0 / r as f64,
            })
            .sum::<f64>()
            / queries.len() as f64;
        json_rows.push(serde_json::json!({
            "config": cid, "p@1": p1, "success@k": s_at, "n": queries.len(),
            "mrr": (mrr * 1000.0).round() / 1000.0,
        }));
    }
    println!();
    println!(
        "JSON {}",
        serde_json::json!({"success_at": args.success_at, "overall": json_rows})
    );
    Ok(())
}
