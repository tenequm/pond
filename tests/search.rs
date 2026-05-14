//! Stage 2 search tests: RRF math, the recency-boost formula, filter-predicate
//! construction, the distance-metric mapping, the `explain_plan` prefilter
//! pushdown assertion, and the synthetic IVF_PQ index-activation check. Every
//! test runs on every `cargo test` - no model weights, no `#[ignore]`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::{Duration, Utc};
use lance_linalg::distance::MetricType;
use pond::{
    config::{Config, Distance},
    datasets::{EMBEDDING_DIM, EmbeddingRow},
    search::{RankedList, build_filter, make_preview, recency_boost, rrf_merge},
    substrate::{PondStore, metric_type},
    wire::{ProjectMatch, SearchFilters},
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Pure functions
// ---------------------------------------------------------------------------

#[test]
fn rrf_merge_fuses_retrievers_and_reports_provenance() {
    let lists = [
        RankedList {
            retriever: "vector",
            ids: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
        },
        RankedList {
            retriever: "fts",
            ids: vec!["b".to_owned(), "a".to_owned(), "d".to_owned()],
        },
    ];
    let merged = rrf_merge(&lists, 60);

    // "a" (ranks 1,2) and "b" (ranks 2,1) have equal fused scores; the tie
    // breaks on message_id, so "a" sorts first. Both beat the single-retriever
    // "c" and "d".
    assert_eq!(merged[0].message_id, "a");
    assert_eq!(merged[1].message_id, "b");
    assert_eq!(merged[0].matched_via, vec!["vector", "fts"]);
    assert!(merged[0].score > merged[2].score);

    let c = merged.iter().find(|hit| hit.message_id == "c").unwrap();
    assert_eq!(c.matched_via, vec!["vector"]);
    let d = merged.iter().find(|hit| hit.message_id == "d").unwrap();
    assert_eq!(d.matched_via, vec!["fts"]);
}

#[test]
fn recency_boost_matches_the_kb_formula() {
    let now = Utc::now();
    // Caps at +0.2 at age zero.
    assert!((recency_boost(now, now) - 0.2).abs() < 1e-6);
    // One half-life (7 days) decays by exactly 1/e.
    let week = recency_boost(now - Duration::days(7), now);
    assert!((week - 0.2 / std::f64::consts::E).abs() < 1e-3);
    // A year out is effectively zero.
    assert!(recency_boost(now - Duration::days(365), now) < 1e-3);
    // Future timestamps clamp to the cap rather than exceeding it.
    assert!((recency_boost(now + Duration::days(1), now) - 0.2).abs() < 1e-6);
}

#[test]
fn make_preview_truncates_at_code_point_boundary() {
    let short = "a short preview";
    assert_eq!(make_preview(short), short);

    let long = "x".repeat(800);
    let preview = make_preview(&long);
    assert!(preview.ends_with("..."));
    assert_eq!(preview.chars().count(), 503);
}

#[test]
fn build_filter_pushes_down_each_predicate() {
    let filters = SearchFilters {
        project: Some("/Users/me/pond".to_owned()),
        project_match: ProjectMatch::Exact,
        session_id: Some("01HXY".to_owned()),
        source_agent: Some("claude-code".to_owned()),
        role: Some("assistant".to_owned()),
        from_date: Some("2026-01-01".to_owned()),
        to_date: Some("2026-05-01".to_owned()),
        min_score: 0.0,
    };
    let sql = build_filter(&filters).unwrap();
    assert!(sql.contains("project = '/Users/me/pond'"));
    assert!(sql.contains("session_id = '01HXY'"));
    assert!(sql.contains("source_agent = 'claude-code'"));
    assert!(sql.contains("role = 'assistant'"));
    assert!(sql.contains("timestamp >="));
    assert!(sql.contains("timestamp <="));
}

#[test]
fn build_filter_is_null_ignores_the_project_value() {
    let filters = SearchFilters {
        project: Some("ignored".to_owned()),
        project_match: ProjectMatch::IsNull,
        ..SearchFilters::default()
    };
    assert_eq!(build_filter(&filters).unwrap(), "project IS NULL");
}

#[test]
fn build_filter_rejects_bad_role_and_date() {
    let bad_role = SearchFilters {
        role: Some("wizard".to_owned()),
        ..SearchFilters::default()
    };
    assert!(build_filter(&bad_role).is_err());

    let bad_date = SearchFilters {
        from_date: Some("01-01-2026".to_owned()),
        ..SearchFilters::default()
    };
    assert!(build_filter(&bad_date).is_err());
}

#[test]
fn empty_filters_produce_no_predicate() {
    assert_eq!(build_filter(&SearchFilters::default()).unwrap(), "");
}

#[test]
fn build_filter_contains_escapes_like_wildcards() {
    let filters = SearchFilters {
        project: Some("/Users/me/my_project".to_owned()),
        project_match: ProjectMatch::Contains,
        ..SearchFilters::default()
    };
    let sql = build_filter(&filters).unwrap();
    // `_` is a LIKE wildcard and is everywhere in real paths; it must be escaped
    // so `my_project` matches literally, with an ESCAPE clause naming the char.
    assert!(
        sql.contains(r"my\_project"),
        "underscore must be escaped: {sql}"
    );
    assert!(
        sql.contains(r"ESCAPE '\'"),
        "predicate must declare the escape char: {sql}"
    );
}

#[test]
fn metric_type_maps_each_registry_distance() {
    assert_eq!(metric_type(Distance::Cosine), MetricType::Cosine);
    assert_eq!(metric_type(Distance::L2), MetricType::L2);
    assert_eq!(metric_type(Distance::Dot), MetricType::Dot);
}

// ---------------------------------------------------------------------------
// Synthetic datasets (no model, no ingest)
// ---------------------------------------------------------------------------

/// Build `count` synthetic embedding rows with deterministic pseudo-random
/// vectors of the production dimension, spread across a handful of sessions.
fn synthetic_rows(count: usize, model_id: &str) -> Vec<EmbeddingRow> {
    let now = Utc::now();
    (0..count)
        .map(|i| {
            let mut vector = Vec::with_capacity(EMBEDDING_DIM);
            let mut state = (i as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(1);
            for _ in 0..EMBEDDING_DIM {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                #[allow(clippy::cast_precision_loss)]
                let unit = (state >> 33) as f32 / (1u64 << 31) as f32;
                vector.push(unit - 1.0);
            }
            EmbeddingRow {
                message_id: format!("msg-{i}"),
                model_id: model_id.to_owned(),
                chunk_index: 0,
                vector,
                session_id: format!("session-{}", i % 8),
                source_agent: "claude-code".to_owned(),
                project: Some(format!("/proj/{}", i % 4)),
                role: if i % 2 == 0 { "user" } else { "assistant" }.to_owned(),
                timestamp: now - Duration::seconds(i as i64),
            }
        })
        .collect()
}

#[tokio::test]
async fn filtered_vector_scan_pushes_scalar_predicate_into_the_index() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let model = Config::builtin().embeddings.default_model("local")?;

    // 4 synthetic rows: `synthetic_rows` cycles `session-{i % 8}`, so 4 is the
    // smallest count where `session-3` (the filter value below) is a real
    // partition. Scalar-index pushdown is volume-independent - the planner emits
    // a `ScalarIndexQuery` for an indexed equality whenever the index exists, so
    // a larger corpus produces the identical plan.
    store
        .upsert_embeddings(&synthetic_rows(4, &model.id))
        .await?;
    store.ensure_embedding_indices(&model).await?;

    let query = vec![0.01_f32; EMBEDDING_DIM];
    let plan = store
        .explain_vector_plan(&query, 10, "session_id = 'session-3'")
        .await?;

    // The load-bearing assertion (design.md 3.3): the predicate is served by a
    // scalar-index node, not a postfilter `FilterExec`. (A `FilterExec` for the
    // KNN-internal `_distance IS NOT NULL` is expected and unrelated.)
    assert!(
        plan.contains("ScalarIndexQuery"),
        "expected a ScalarIndexQuery node in the plan:\n{plan}",
    );
    let predicate_postfiltered = plan
        .lines()
        .any(|line| line.contains("FilterExec") && line.contains("session_id"));
    assert!(
        !predicate_postfiltered,
        "the scalar predicate must not fall back to a FilterExec postfilter:\n{plan}",
    );
    Ok(())
}

#[tokio::test]
async fn vector_index_activates_past_the_row_threshold() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let model = Config::builtin().embeddings.default_model("local")?;

    // 256 rows is the hard floor: the IVF_PQ index uses `num_bits = 8`, so its
    // PQ trainer needs one row per code centroid (2^8 = 256) - fewer fails with
    // "Not enough rows to train PQ". The thresholds below straddle that count by
    // exactly one, so the test exercises the `row_count >= threshold` boundary.
    let rows = synthetic_rows(256, &model.id);
    let planted = rows[0].clone();
    store.upsert_embeddings(&rows).await?;

    // Just below threshold (256 < 257): no vector index yet.
    store
        .ensure_embedding_indices_with_threshold(&model, 257)
        .await?;
    assert!(
        !store
            .embedding_index_names()
            .await?
            .iter()
            .any(|name| name == "embeddings_vector_ivfpq"),
        "vector index must not build below the activation threshold",
    );

    // At the threshold (256 >= 256): the IVF_PQ index builds.
    store
        .ensure_embedding_indices_with_threshold(&model, 256)
        .await?;
    let indices = store.embedding_index_names().await?;
    assert!(
        indices.iter().any(|name| name == "embeddings_vector_ivfpq"),
        "IVF_PQ index should build past the activation threshold: {indices:?}",
    );

    // A query whose vector is a planted row returns that row.
    let hits = store.vector_search(&planted.vector, 10, "").await?;
    assert!(
        hits.iter().any(|(id, _)| id == &planted.message_id),
        "planted vector should be retrievable via the index",
    );
    Ok(())
}
