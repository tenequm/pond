#![allow(clippy::expect_used, clippy::unwrap_used)]

//! stdio-MCP transport (design.md 3.6, 2.2, kb parity contract): the `pond_search`
//! / `pond_get` tools are driven by an in-process rmcp client over a `duplex`
//! pipe. Asserts the kb-parity field mapping (`conversation_id` -> `session_id`)
//! and the MCP-only placeholder rendering for excluded parts (design.md 3.6.3).

use chrono::Utc;
use pond::{
    PROTOCOL_VERSION,
    config::Config,
    embed::{EmbedBackend, EmbedWorker},
    ingest::{IngestEvent, pond_ingest},
    substrate::PondStore,
    transport::{AppState, mcp::PondMcp},
    types::{Message, Part, PartKind, Session},
    wire::{GetResponse, GetResult, IngestEnvelope, IngestRequest, SearchResponse},
};
use rmcp::{
    ClientHandler, ServiceExt,
    model::{CallToolRequestParams, CallToolResult},
};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

const SESSION_ID: &str = "mcp-test-session";
const MESSAGE_ID: &str = "mcp-test-message";
const REASONING_TEXT: &str = "weighing the options before answering";

/// Deterministic, content-dependent vectors - no model weights, exact f32s.
struct FakeBackend {
    dim: usize,
}

impl EmbedBackend for FakeBackend {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts
            .iter()
            .map(|text| {
                let bytes = text.as_bytes();
                (0..self.dim)
                    .map(|i| {
                        let byte = bytes.get(i % bytes.len().max(1)).copied().unwrap_or(0);
                        f32::from(byte) / 255.0
                    })
                    .collect()
            })
            .collect())
    }

    fn dim(&self) -> usize {
        self.dim
    }
}

#[derive(Clone, Default)]
struct TestClient;

impl ClientHandler for TestClient {}

/// A store holding one synthetic assistant message with a reasoning part and a
/// text part. Built via the `pond_ingest` handler directly - the claude-code
/// fixtures carry no reasoning parts, and placeholder rendering needs one.
async fn synthetic_state(temp: &TempDir) -> anyhow::Result<AppState> {
    let store = PondStore::open(temp.path()).await?;

    let session = Session {
        id: SESSION_ID.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: Some("pond-mcp-test".to_owned()),
        options: Default::default(),
    };
    let message = Message::Assistant {
        id: MESSAGE_ID.to_owned(),
        session_id: SESSION_ID.to_owned(),
        timestamp: Utc::now(),
        options: Default::default(),
    };
    let reasoning = Part {
        id: "mcp-test-part-reasoning".to_owned(),
        message_id: MESSAGE_ID.to_owned(),
        ordinal: 0,
        options: Default::default(),
        kind: PartKind::Reasoning {
            text: REASONING_TEXT.to_owned(),
        },
    };
    let text = Part {
        id: "mcp-test-part-text".to_owned(),
        message_id: MESSAGE_ID.to_owned(),
        ordinal: 1,
        options: Default::default(),
        kind: PartKind::Text {
            text: "the answer is forty-two".to_owned(),
        },
    };

    let envelope = pond_ingest(
        &store,
        IngestRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: "local".to_owned(),
            events: vec![
                IngestEvent::Session(session),
                IngestEvent::Message(message),
                IngestEvent::Part(reasoning),
                IngestEvent::Part(text),
            ],
        },
    )
    .await;
    assert!(
        matches!(envelope, IngestEnvelope::Success(_)),
        "synthetic ingest should succeed: {envelope:?}",
    );
    store.ensure_indices().await?;

    let model = Config::builtin().embeddings.default_model("local")?;
    let backend = FakeBackend {
        dim: model.dim as usize,
    };
    EmbedWorker::new(&store, &backend, &model)?.run().await?;
    store.ensure_embedding_indices(&model).await?;

    Ok(AppState {
        store: Arc::new(store),
        embedder: Arc::new(backend),
    })
}

fn tool_text(result: &CallToolResult) -> &str {
    result
        .content
        .first()
        .and_then(|content| content.raw.as_text())
        .map(|text| text.text.as_str())
        .expect("a tool result should carry a text block")
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_tools_honor_kb_parity_and_placeholder_rendering() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let state = synthetic_state(&temp).await?;

    let (server_transport, client_transport) = tokio::io::duplex(8192);
    let server = PondMcp::new(state);
    let server_handle = tokio::spawn(async move {
        server.serve(server_transport).await?.waiting().await?;
        anyhow::Ok(())
    });
    let client = TestClient.serve(client_transport).await?;

    // pond_search runs over the MCP transport and returns a success envelope.
    let result = client
        .call_tool(
            CallToolRequestParams::new("pond_search")
                .with_arguments(json!({ "query": "answer" }).as_object().unwrap().clone()),
        )
        .await?;
    let search: SearchResponse = serde_json::from_str(tool_text(&result))?;
    assert_eq!(
        search.total,
        match &search.result {
            pond::wire::SearchResultBody::Hits { hits } => hits.len(),
            pond::wire::SearchResultBody::Groups { groups } => groups.len(),
        },
        "search response total should match the body length",
    );

    // pond_get with `conversation_id` maps to the wire `session_id` filter (kb
    // parity), and excluded reasoning parts render as a placeholder, not raw.
    let result = client
        .call_tool(
            CallToolRequestParams::new("pond_get").with_arguments(
                json!({ "conversation_id": SESSION_ID })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await?;
    let response: GetResponse = serde_json::from_str(tool_text(&result))?;
    let GetResult::Session(stored) = response.result else {
        panic!("expected a session result");
    };
    assert_eq!(
        stored.session.id, SESSION_ID,
        "conversation_id -> session_id"
    );
    let parts = &stored.messages[0].parts;
    assert!(
        parts
            .iter()
            .all(|part| !matches!(part.kind, PartKind::Reasoning { .. })),
        "default include_thinking=false: no raw reasoning parts over MCP",
    );
    let placeholder = parts.iter().find_map(|part| match &part.kind {
        PartKind::Text { text } if text.starts_with("[reasoning:") => Some(text.as_str()),
        _ => None,
    });
    assert_eq!(
        placeholder,
        Some(format!("[reasoning: {} chars]", REASONING_TEXT.chars().count()).as_str()),
        "excluded reasoning renders as a [reasoning: N chars] placeholder",
    );

    // With include_thinking=true the raw reasoning part comes back in full.
    let result = client
        .call_tool(
            CallToolRequestParams::new("pond_get").with_arguments(
                json!({ "conversation_id": SESSION_ID, "include_thinking": true })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await?;
    let response: GetResponse = serde_json::from_str(tool_text(&result))?;
    let GetResult::Session(stored) = response.result else {
        panic!("expected a session result");
    };
    assert!(
        stored.messages[0].parts.iter().any(|part| matches!(
            &part.kind,
            PartKind::Reasoning { text } if text == REASONING_TEXT
        )),
        "include_thinking=true returns the raw reasoning part",
    );

    // A wire error (unknown session) surfaces as a JSON-RPC tool error.
    let missing = client
        .call_tool(
            CallToolRequestParams::new("pond_get").with_arguments(
                json!({ "conversation_id": "no-such-session" })
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

    client.cancel().await?;
    let _ = server_handle.await;
    Ok(())
}
