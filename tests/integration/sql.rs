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
            // Embedded newline: the inline table must collapse it to a
            // literal `\n` so the row renders as one physical line.
            text: s("what is the answer\nreally"),
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
    let tool_call = Part {
        session_id: SESSION_ID.to_owned(),
        id: "p-call".to_owned(),
        message_id: "m-asst".to_owned(),
        ordinal: 1,
        provenance: Provenance::Conversational,
        options: Default::default(),
        kind: PartKind::ToolCall {
            call_id: s("call-1"),
            name: s("Bash"),
            params: json!({ "command": "echo hi" }),
            provider_executed: false,
        },
    };
    let tool_result = Part {
        session_id: SESSION_ID.to_owned(),
        id: "p-result".to_owned(),
        message_id: "m-asst".to_owned(),
        ordinal: 2,
        provenance: Provenance::Conversational,
        options: Default::default(),
        kind: PartKind::ToolResult {
            call_id: s("call-1"),
            name: s("Bash"),
            is_failure: false,
            // Array-valued result: the lenient json_get_string must serialize
            // it instead of aborting the scan (strict jsonb to_str fails).
            result: json!([{ "type": "text", "text": "hi there" }]),
        },
    };

    let envelope = pond_ingest(
        &store,
        IngestRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            // Stream order matters: parts attach to the current message, so
            // each message's parts must follow it before the next message.
            events: vec![
                IngestEvent::Session(session),
                IngestEvent::Message(user),
                IngestEvent::Part(user_text),
                IngestEvent::Message(assistant),
                IngestEvent::Part(asst_text),
                IngestEvent::Part(tool_call),
                IngestEvent::Part(tool_result),
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
        text.contains("message_id"),
        "the messages key is the renamed message_id: {text}"
    );
    assert!(
        text.contains("embedding_model"),
        "keeps text columns: {text}"
    );
    assert!(!text.contains("vector"), "omits the vector column: {text}");

    // 4. BM25 search-in-SQL via the fts() table function executes and composes
    // with ordinary SQL (projection / LIMIT) around it - and exposes the
    // renamed message_id key like the messages view does.
    let result = call(json!({
        "sql": "SELECT message_id FROM fts('messages', \
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
        "format": "ndjson"
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
        "format": "parquet"
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

    // 7. format=json was removed; it is now a clean unknown-format error
    // (ndjson is the machine-readable JSON path, delivered as an export).
    let result = call(json!({
        "sql": "SELECT role FROM messages",
        "format": "json"
    }))
    .await?;
    assert_eq!(result.is_error, Some(true), "format=json is rejected");
    let text = first_text(&result).expect("error carries a message");
    assert!(
        text.contains("ndjson"),
        "error names the valid formats: {text}"
    );

    // 8. EXPLAIN passes the read-only gate and returns a plan.
    let result = call(json!({ "sql": "EXPLAIN SELECT role FROM messages" })).await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "EXPLAIN should succeed: {result:?}"
    );
    let text = first_text(&result).expect("EXPLAIN renders inline");
    assert!(
        text.to_ascii_lowercase().contains("plan"),
        "EXPLAIN output mentions a plan: {text}"
    );

    // 9. Explicit projection of the `vector` column is a loud tool error,
    // not the prior silent-empty result.
    let result = call(json!({ "sql": "SELECT vector FROM messages" })).await?;
    assert_eq!(
        result.is_error,
        Some(true),
        "SELECT vector must be an isError tool result"
    );
    let text = first_text(&result).expect("error carries a message");
    assert!(
        text.contains("pond_search"),
        "error redirects to pond_search: {text}"
    );

    // 10. Inline metrics footer reports elapsed time.
    let result = call(json!({ "sql": "SELECT 1" })).await?;
    let text = first_text(&result).expect("inline result");
    assert!(
        text.contains(" ms"),
        "metrics footer carries elapsed ms: {text}"
    );

    // 12. The `query` param name is accepted as an alias for `sql` (agents
    // guess it; previously a hard -32602 deserialization error).
    let result = call(json!({ "query": "SELECT count(*) AS n FROM parts" })).await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "query alias should work: {result:?}"
    );

    // 13. information_schema self-discovery works.
    let result = call(json!({
        "sql": "SELECT column_name FROM information_schema.columns \
                WHERE table_name = 'messages' ORDER BY column_name"
    }))
    .await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "information_schema should be enabled: {result:?}"
    );
    let text = first_text(&result).expect("inline result");
    assert!(text.contains("search_text"), "lists columns: {text}");

    // 14. The parts `data` blob column is hidden from the SQL schema, and the
    // error teaches the fix (variant_data + schema doc).
    let result = call(json!({ "sql": "SELECT data FROM parts" })).await?;
    assert_eq!(
        result.is_error,
        Some(true),
        "parts.data is not selectable: {result:?}"
    );
    let text = first_text(&result).expect("error carries a message");
    assert!(text.contains("hint:"), "error carries a hint: {text}");
    assert!(
        text.contains("variant_data"),
        "hint redirects to variant_data: {text}"
    );
    let result = call(json!({
        "sql": "SELECT count(*) AS n FROM information_schema.columns \
                WHERE table_name = 'parts' AND column_name = 'data'"
    }))
    .await?;
    let text = first_text(&result).expect("inline result");
    assert!(text.contains("| 0 |"), "data absent from schema: {text}");

    // 15. Tool analytics: json_get_string drills the tool name out of
    // variant_data.
    let result = call(json!({
        "sql": "SELECT json_get_string(variant_data, 'name') AS tool, count(*) AS n \
                FROM parts WHERE type = 'tool_call' GROUP BY tool"
    }))
    .await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "tool analytics should work: {result:?}"
    );
    let text = first_text(&result).expect("inline result");
    assert!(text.contains("Bash"), "extracts the tool name: {text}");

    // 16. Lenient json_get_string: an array-valued field serializes to JSON
    // text instead of aborting the scan with InvalidCast.
    let result = call(json!({
        "sql": "SELECT json_get_string(variant_data, 'result') AS r \
                FROM parts WHERE type = 'tool_result'"
    }))
    .await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "lenient json_get_string should serialize non-strings: {result:?}"
    );
    let text = first_text(&result).expect("inline result");
    assert!(text.contains("hi there"), "serialized array result: {text}");

    // 17. Renamed keys: messages.message_id and sessions.session_id filter
    // and join under their self-describing names (the view inlines, so the
    // equality predicate still reaches the Lance scan).
    let result = call(json!({
        "sql": "SELECT m.message_id, s.session_id \
                FROM messages m JOIN sessions s ON m.session_id = s.session_id \
                WHERE m.message_id = 'm-user'"
    }))
    .await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "renamed keys should join and filter: {result:?}"
    );
    let text = first_text(&result).expect("inline result");
    assert!(text.contains("m-user"), "filters by message_id: {text}");
    assert!(text.contains(SESSION_ID), "joins by session_id: {text}");

    // 18. contains_tokens as a WHERE predicate: the natural filter-form of
    // full-text search (all words must match).
    let result = call(json!({
        "sql": "SELECT message_id FROM messages \
                WHERE contains_tokens(search_text, 'forty two')"
    }))
    .await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "contains_tokens should filter: {result:?}"
    );
    let text = first_text(&result).expect("inline result");
    assert!(
        text.contains("m-asst"),
        "matches the assistant message: {text}"
    );

    // 19. fts() in WHERE is the classic predicate-form misuse: it must fail
    // at plan time with a redirect to contains_tokens, not DataFusion's
    // "Invalid function 'fts'. Did you mean 'cos'?".
    let result = call(json!({
        "sql": "SELECT message_id FROM messages WHERE \
                fts('messages', '{\"match\":{}}')"
    }))
    .await?;
    assert_eq!(
        result.is_error,
        Some(true),
        "WHERE fts(...) must be a tool error: {result:?}"
    );
    let text = first_text(&result).expect("error carries a message");
    assert!(
        text.contains("contains_tokens"),
        "error redirects to contains_tokens: {text}"
    );

    // 20. any_value aggregates (Postgres 16 / DuckDB name agents reach for).
    let result = call(json!({
        "sql": "SELECT session_id, any_value(project) AS project \
                FROM messages GROUP BY session_id"
    }))
    .await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "any_value should aggregate: {result:?}"
    );
    let text = first_text(&result).expect("inline result");
    assert!(text.contains(PROJECT), "any_value picks a value: {text}");

    // 21. Variadic json_get_*: a key path walks nested objects (the
    // datafusion-functions-json convention).
    let result = call(json!({
        "sql": "SELECT json_get_string(variant_data, 'params', 'command') AS cmd \
                FROM parts WHERE type = 'tool_call'"
    }))
    .await?;
    assert_ne!(
        result.is_error,
        Some(true),
        "variadic json_get_string should walk the path: {result:?}"
    );
    let text = first_text(&result).expect("inline result");
    assert!(text.contains("echo hi"), "walks params.command: {text}");

    // 22. CAST / `::` on JSONB columns is rejected at plan time with the fix
    // (runtime behavior is data-dependent and can silently return garbage).
    for sql in [
        "SELECT CAST(variant_data AS VARCHAR) FROM parts",
        "SELECT variant_data::text FROM parts",
    ] {
        let result = call(json!({ "sql": sql })).await?;
        assert_eq!(
            result.is_error,
            Some(true),
            "JSONB cast must be a tool error: {sql}"
        );
        let text = first_text(&result).expect("error carries a message");
        assert!(text.contains("json_extract"), "error names the fix: {text}");
    }

    // 23. Embedded newlines in cell values collapse to a literal `\n` so each
    // row renders as one physical line.
    let result = call(json!({
        "sql": "SELECT search_text FROM messages WHERE role = 'user'"
    }))
    .await?;
    let text = first_text(&result).expect("inline result");
    assert!(
        text.contains("answer\\nreally"),
        "newline collapses to literal backslash-n: {text}"
    );
    assert!(
        !text.contains("answer\nreally"),
        "no raw newline inside a row: {text}"
    );

    client.cancel().await?;
    let _ = server_handle.await;
    Ok(())
}
