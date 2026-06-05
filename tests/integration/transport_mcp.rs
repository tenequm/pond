#![allow(clippy::expect_used, clippy::unwrap_used)]

//! stdio-MCP transport (spec.md#protocol): the `pond_search` / `pond_get`
//! tools are driven by an in-process rmcp client over a `duplex` pipe. Asserts
//! the `tools/list` size-cap annotations, the round-trip response shape, and
//! the JSON-RPC error mapping for unknown sessions.

use chrono::Utc;
use pond::{
    PROTOCOL_VERSION,
    embed::{EmbedWorker, Embedder},
    handlers::{IngestEvent, pond_ingest},
    sessions::{Store, embedding_dim},
    substrate::MaintenancePolicy,
    transport::{AppState, mcp::PondMcp},
    wire::{IngestEnvelope, IngestRequest},
    wire::{Message, Part, PartKind, Provenance, Session},
};
use rmcp::{
    ClientHandler, ServiceError, ServiceExt,
    model::{CallToolRequestParams, CallToolResult},
};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

const SESSION_ID: &str = "mcp-test-session";
const MESSAGE_ID: &str = "mcp-test-message";
const REASONING_TEXT: &str = "weighing the options before answering";

/// Build an `Option<Extracted<String>>` for test fixtures. Integration tests
/// can't see `Extracted::from_test_value` (cfg-test-gated inside the pond
/// crate), so we go through the public `extract_str` producer on a
/// synthetic JSON source.
fn s(value: &str) -> Option<pond::adapter::Extracted<String>> {
    pond::adapter::extract_str(&serde_json::json!({"x": value}), "x")
}

/// Deterministic, content-dependent vectors - no model weights, exact f32s.
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

/// A store holding one synthetic assistant message with a reasoning part and a
/// text part. Built via the `pond_ingest` handler directly - the claude-code
/// fixtures carry no reasoning parts, and placeholder rendering needs one.
async fn synthetic_state(temp: &TempDir) -> anyhow::Result<AppState> {
    let store = Store::open_local(temp.path()).await?;

    let session = Session {
        id: SESSION_ID.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "pond-mcp-test"}), "x")
            .unwrap(),
        options: Default::default(),
    };
    let message = Message::Assistant {
        id: MESSAGE_ID.to_owned(),
        session_id: SESSION_ID.to_owned(),
        timestamp: Utc::now(),
        options: Default::default(),
    };
    let reasoning = Part {
        session_id: SESSION_ID.to_owned(),
        id: "mcp-test-part-reasoning".to_owned(),
        message_id: MESSAGE_ID.to_owned(),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: Default::default(),
        kind: PartKind::Reasoning {
            text: s(REASONING_TEXT),
        },
    };
    let text = Part {
        session_id: SESSION_ID.to_owned(),
        id: "mcp-test-part-text".to_owned(),
        message_id: MESSAGE_ID.to_owned(),
        ordinal: 1,
        provenance: Provenance::Conversational,
        options: Default::default(),
        kind: PartKind::Text {
            text: s("the answer is forty-two"),
        },
    };
    let tool_call = Part {
        session_id: SESSION_ID.to_owned(),
        id: "mcp-test-part-toolcall".to_owned(),
        message_id: MESSAGE_ID.to_owned(),
        ordinal: 2,
        provenance: Provenance::Conversational,
        options: Default::default(),
        kind: PartKind::ToolCall {
            call_id: s("toolu_mcptest"),
            name: s("Bash"),
            params: json!({}),
            provider_executed: false,
        },
    };

    let envelope = pond_ingest(
        &store,
        IngestRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            events: vec![
                IngestEvent::Session(session),
                IngestEvent::Message(message),
                IngestEvent::Part(reasoning),
                IngestEvent::Part(text),
                IngestEvent::Part(tool_call),
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

/// The MCP surface returns the rendered transcript as a text block and no
/// `structured_content` (it would shadow the transcript on the Claude Code
/// client), so the test asserts on the transcript text directly.
fn tool_text(result: &CallToolResult) -> &str {
    assert!(
        result.structured_content.is_none(),
        "MCP results are transcript-only; structured data lives on the HTTP /v1 API"
    );
    result
        .content
        .first()
        .and_then(|content| content.raw.as_text())
        .map(|text| text.text.as_str())
        .expect("a tool result should carry a text block")
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_tools_round_trip_with_size_caps_and_error_mapping() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let state = synthetic_state(&temp).await?;

    let (server_transport, client_transport) = tokio::io::duplex(8192);
    let server = PondMcp::new(state);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = TestClient.serve(client_transport).await?;

    let tools = client.list_all_tools().await?;
    let meta_chars = |name: &str| -> Option<i64> {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .and_then(|tool| tool.meta.as_ref())
            .and_then(|meta| meta.0.get("anthropic/maxResultSizeChars"))
            .and_then(serde_json::Value::as_i64)
    };
    assert_eq!(meta_chars("pond_search"), Some(80_000));
    assert_eq!(meta_chars("pond_get"), Some(200_000));

    // pond_search runs over the MCP transport and returns a success envelope.
    let result = client
        .call_tool(
            CallToolRequestParams::new("pond_search")
                .with_arguments(json!({ "query": "answer" }).as_object().unwrap().clone()),
        )
        .await?;
    let search = tool_text(&result);
    assert!(
        search.starts_with("pond_search: 1 matching messages, showing 1 hits from 1 sessions."),
        "search transcript header states the totals: {search}"
    );
    assert!(
        search.contains(SESSION_ID),
        "the session header carries the session id: {search}"
    );
    assert!(
        search.contains("the answer is forty-two"),
        "the matched text is rendered: {search}"
    );

    let result = client
        .call_tool(
            CallToolRequestParams::new("pond_get").with_arguments(
                json!({ "session_id": SESSION_ID })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await?;
    let conversational = tool_text(&result);
    assert!(
        conversational.starts_with(&format!("pond_get: session {SESSION_ID} (conversational)")),
        "get transcript header names the session and mode: {conversational}"
    );
    assert!(
        conversational.contains("key:"),
        "the get transcript carries a key legend: {conversational}"
    );
    assert!(
        conversational.contains("the answer is forty-two"),
        "conversational mode renders the message text: {conversational}"
    );
    assert!(
        conversational.contains("-> Bash [toolu_mcptest]"),
        "conversational mode surfaces the tool_call as a one-liner: {conversational}"
    );
    assert!(
        !conversational.contains(REASONING_TEXT),
        "conversational mode elides reasoning: {conversational}"
    );

    let result = client
        .call_tool(
            CallToolRequestParams::new("pond_get").with_arguments(
                json!({ "session_id": SESSION_ID, "response_mode": "verbatim" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await?;
    let verbatim = tool_text(&result);
    assert!(
        verbatim.contains(REASONING_TEXT),
        "verbatim mode renders the reasoning part in full: {verbatim}"
    );

    // A wire error (unknown session) surfaces as a JSON-RPC tool error.
    let missing = client
        .call_tool(
            CallToolRequestParams::new("pond_get").with_arguments(
                json!({ "session_id": "no-such-session" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await;
    assert!(
        missing.is_err(),
        "an unknown session should be a tool error"
    );
    let ServiceError::McpError(error) = missing.unwrap_err() else {
        panic!("expected MCP tool error");
    };
    let data = error
        .data
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .expect("tool error data is an object");
    assert_eq!(data.get("pond_code"), Some(&json!("not_found")));
    assert_eq!(data.get("retryable"), Some(&json!(false)));

    client.cancel().await?;
    let _ = server_handle.await;
    Ok(())
}
