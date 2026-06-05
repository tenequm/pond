#![allow(clippy::expect_used, clippy::unwrap_used)]

//! `pond_sql_query` over the stdio-MCP transport (spec.md#protocol): an
//! in-process rmcp client drives read-only SQL against a synthetic corpus.
//! Asserts inline aggregation, the hard read-only gate (DROP/INSERT rejected as
//! tool errors), `vector`-column omission, the `fts()` UDTF, and the
//! parquet/ndjson export round-trip through the `pond-sql-export://` resource.

use chrono::Utc;
use pond::{
    PROTOCOL_VERSION,
    embed::{EmbedWorker, Embedder},
    handlers::{IngestEvent, pond_ingest},
    sessions::{Store, embedding_dim},
    substrate::MaintenancePolicy,
    transport::{AppState, mcp::PondMcp},
    wire::{IngestEnvelope, IngestRequest, Message, Part, PartKind, Provenance, Session},
};
use rmcp::{
    ClientHandler, ServiceExt,
    model::{CallToolRequestParams, CallToolResult, ReadResourceRequestParams, ResourceContents},
};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

const SESSION_ID: &str = "sql-test-session";
const PROJECT: &str = "pond-sql-test";

fn s(value: &str) -> Option<pond::adapter::Extracted<String>> {
    pond::adapter::extract_str(&serde_json::json!({ "x": value }), "x")
}

/// Deterministic, content-dependent vectors - no model weights needed.
struct FakeBackend;

impl Embedder for FakeBackend {
    fn device(&self) -> &str {
        "fake"
    }

    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts
            .iter()
            .map(|text| {
                let bytes = text.as_bytes();
                (0..embedding_dim())
                    .map(|i| {
                        let byte = bytes.get(i % bytes.len().max(1)).copied().unwrap_or(0);
                        f32::from(byte) / 255.0
                    })
                    .collect()
            })
            .collect())
    }
}

#[derive(Clone, Default)]
struct TestClient;

impl ClientHandler for TestClient {}

/// One session with a user message and an assistant message, each carrying a
/// text part, so `GROUP BY role` yields two rows and `parts` is populated.
async fn synthetic_state(temp: &TempDir) -> anyhow::Result<AppState> {
    let store = Store::open_local(temp.path()).await?;

    let session = Session {
        id: SESSION_ID.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: s(PROJECT).unwrap(),
        options: Default::default(),
    };
    let user = Message::User {
        id: "m-user".to_owned(),
        session_id: SESSION_ID.to_owned(),
        timestamp: Utc::now(),
        options: Default::default(),
    };
    let assistant = Message::Assistant {
        id: "m-asst".to_owned(),
        session_id: SESSION_ID.to_owned(),
        timestamp: Utc::now(),
        options: Default::default(),
    };
    let user_text = Part {
        session_id: SESSION_ID.to_owned(),
        id: "p-user".to_owned(),
        message_id: "m-user".to_owned(),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: Default::default(),
        kind: PartKind::Text {
            text: s("what is the answer"),
        },
    };
    let asst_text = Part {
        session_id: SESSION_ID.to_owned(),
        id: "p-asst".to_owned(),
        message_id: "m-asst".to_owned(),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: Default::default(),
        kind: PartKind::Text {
            text: s("the answer is forty-two"),
        },
    };

    let envelope = pond_ingest(
        &store,
        IngestRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            events: vec![
                IngestEvent::Session(session),
                IngestEvent::Message(user),
                IngestEvent::Message(assistant),
                IngestEvent::Part(user_text),
                IngestEvent::Part(asst_text),
            ],
        },
    )
    .await;
    assert!(
        matches!(envelope, IngestEnvelope::Success(_)),
        "synthetic ingest should succeed: {envelope:?}",
    );
    let backend = FakeBackend;
    EmbedWorker::new(&store, &backend).run().await?;
    store
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?
        .into_result()?;

    Ok(AppState {
        store: Arc::new(store),
        embedder: Arc::new(pond::embed::LazyEmbedder::from_loaded(Arc::new(backend))),
        search: pond::config::SearchConfig::default(),
    })
}

fn first_text(result: &CallToolResult) -> Option<&str> {
    result
        .content
        .iter()
        .find_map(|content| content.raw.as_text().map(|text| text.text.as_str()))
}

fn resource_link_uri(result: &CallToolResult) -> Option<String> {
    result
        .content
        .iter()
        .find_map(|content| content.raw.as_resource_link().map(|link| link.uri.clone()))
}

#[tokio::test(flavor = "multi_thread")]
async fn pond_sql_query_over_mcp() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let state = synthetic_state(&temp).await?;

    let (server_transport, client_transport) = tokio::io::duplex(1 << 16);
    let server = PondMcp::new(state);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = TestClient.serve(client_transport).await?;

    let call = async |args: serde_json::Value| {
        client
            .call_tool(
                CallToolRequestParams::new("pond_sql_query")
                    .with_arguments(args.as_object().unwrap().clone()),
            )
            .await
    };

    // The new tool advertises its result-size cap alongside the others.
    let tools = client.list_all_tools().await?;
    let cap = tools
        .iter()
        .find(|tool| tool.name == "pond_sql_query")
        .and_then(|tool| tool.meta.as_ref())
        .and_then(|meta| meta.0.get("anthropic/maxResultSizeChars"))
        .and_then(serde_json::Value::as_i64);
    assert_eq!(cap, Some(80_000), "pond_sql_query carries a size cap");

    // 1. Inline aggregation: GROUP BY over the role column.
    let result = call(json!({
        "sql": "SELECT role, count(*) AS n FROM messages GROUP BY role ORDER BY role"
    }))
    .await?;
    assert_ne!(result.is_error, Some(true), "aggregation should succeed");
    let text = first_text(&result).expect("inline result is text");
    assert!(text.contains("assistant"), "groups assistant: {text}");
    assert!(text.contains("user"), "groups user: {text}");
    assert!(text.contains("2 row(s)"), "two role groups: {text}");

    // 2. Read-only is hard-enforced: writes / DDL come back as tool errors.
    for write in [
        "DROP TABLE messages",
        "INSERT INTO messages (id) VALUES ('x')",
        "COPY (SELECT 1) TO '/tmp/x.parquet'",
        "SELECT 1; SELECT 2",
    ] {
        let result = call(json!({ "sql": write })).await?;
        assert_eq!(
            result.is_error,
            Some(true),
            "non-SELECT must be a tool error: {write}"
        );
    }

    // 3. SELECT * works and the embedding `vector` column is omitted.
    let result = call(json!({ "sql": "SELECT * FROM messages" })).await?;
    assert_ne!(result.is_error, Some(true), "select * should succeed");
    let text = first_text(&result).expect("inline result is text");
    assert!(
        text.contains("session_id"),
        "shows canonical columns: {text}"
    );
    assert!(
        text.contains("embedding_model"),
        "keeps text columns: {text}"
    );
    assert!(!text.contains("vector"), "omits the vector column: {text}");

    // 4. BM25 search-in-SQL via the fts() table function executes and composes
    // with ordinary SQL (projection / LIMIT) around it.
    let result = call(json!({
        "sql": "SELECT id FROM fts('messages', \
                '{\"match\":{\"column\":\"search_text\",\"terms\":\"answer\"}}') LIMIT 5"
    }))
    .await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "fts() should execute: {result:?}"
    );

    // 5. ndjson export round-trips through the pond-sql-export:// resource.
    let result = call(json!({
        "sql": "SELECT role, project FROM messages ORDER BY role",
        "output": "ndjson"
    }))
    .await?;
    assert_ne!(result.is_error, Some(true), "ndjson export should succeed");
    assert!(
        first_text(&result).is_some_and(|text| text.contains("Exported")),
        "export carries a text summary"
    );
    let uri = resource_link_uri(&result).expect("export returns a resource_link");
    assert!(
        uri.starts_with("pond-sql-export://"),
        "custom scheme: {uri}"
    );
    assert!(uri.ends_with(".ndjson"), "ndjson extension: {uri}");
    let read = client
        .read_resource(ReadResourceRequestParams::new(uri))
        .await?;
    match read.contents.first().expect("resource has contents") {
        ResourceContents::TextResourceContents { text, .. } => {
            assert!(text.contains("assistant"), "ndjson holds the rows: {text}");
            assert!(text.contains(PROJECT), "ndjson holds the project: {text}");
        }
        other => panic!("ndjson export should read back as text, got {other:?}"),
    }

    // 6. parquet export round-trips as a base64 blob with the PAR1 magic.
    let result = call(json!({
        "sql": "SELECT role FROM messages",
        "output": "parquet"
    }))
    .await?;
    assert_ne!(result.is_error, Some(true), "parquet export should succeed");
    let uri = resource_link_uri(&result).expect("export returns a resource_link");
    assert!(uri.ends_with(".parquet"), "parquet extension: {uri}");
    let read = client
        .read_resource(ReadResourceRequestParams::new(uri))
        .await?;
    match read.contents.first().expect("resource has contents") {
        ResourceContents::BlobResourceContents { blob, .. } => {
            // base64 of bytes starting with the parquet magic "PAR1" begins "UEFS".
            assert!(blob.starts_with("UEFS"), "parquet blob has PAR1 magic");
        }
        other => panic!("parquet export should read back as a blob, got {other:?}"),
    }

    // An unknown export id is a clean not-found, not a traversal.
    let missing = client
        .read_resource(ReadResourceRequestParams::new(
            "pond-sql-export://../etc/passwd",
        ))
        .await;
    assert!(missing.is_err(), "invalid export id is rejected");

    client.cancel().await?;
    let _ = server_handle.await;
    Ok(())
}
