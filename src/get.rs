use serde_json::json;

use crate::{
    substrate::PondStore,
    types::{PartKind, StoredMessage, StoredSession},
    wire::{ErrorCode, GetEnvelope, GetRequest, GetResponse, GetResult, error, validate_protocol},
};

pub async fn pond_get(store: &PondStore, request: GetRequest) -> GetEnvelope {
    if let Err(error) = validate_protocol(request.protocol_version) {
        return GetEnvelope::Error(error);
    }

    let result = match (&request.session_id, &request.message_id, &request.up_to) {
        (Some(session_id), None, up_to) => {
            session_scope(store, &request, session_id, up_to.as_deref()).await
        }
        (None, Some(message_id), None) => message_scope(store, &request, message_id).await,
        (None, Some(_), Some(_)) => Err(error(
            ErrorCode::ValidationFailed,
            "up_to is valid only with session_id",
            json!({"field": "up_to"}),
        )),
        (Some(_), Some(_), _) => Err(error(
            ErrorCode::ValidationFailed,
            "session_id and message_id are mutually exclusive",
            json!({"field": "session_id"}),
        )),
        (None, None, _) => Err(error(
            ErrorCode::ValidationFailed,
            "one of session_id or message_id is required",
            json!({"field": "session_id"}),
        )),
    };

    match result {
        Ok(result) => GetEnvelope::Success(GetResponse {
            result,
            request_id: crate::wire::new_request_id(),
        }),
        Err(error) => GetEnvelope::Error(error),
    }
}

async fn session_scope(
    store: &PondStore,
    request: &GetRequest,
    session_id: &str,
    up_to: Option<&str>,
) -> Result<GetResult, crate::wire::ErrorEnvelope> {
    let Some(mut stored) = store.get_session(session_id).await.map_err(storage_error)? else {
        return Err(error(
            ErrorCode::NotFound,
            "session not found",
            json!({"kind": "session", "pk": session_id}),
        ));
    };

    if let Some(up_to) = up_to {
        let Some(index) = stored
            .messages
            .iter()
            .position(|message| message.message.id() == up_to)
        else {
            return Err(error(
                ErrorCode::NotFound,
                "up_to message not found in session",
                json!({"kind": "message", "pk": [session_id, up_to]}),
            ));
        };
        stored.messages.truncate(index + 1);
    }

    let max_messages = request.max_messages.min(1000);
    if stored.messages.len() > max_messages {
        stored.messages = stored.messages[stored.messages.len() - max_messages..].to_vec();
    }
    filter_session(
        &mut stored,
        request.include_thinking,
        request.include_tool_results,
    );
    Ok(GetResult::Session(stored))
}

async fn message_scope(
    store: &PondStore,
    request: &GetRequest,
    message_id: &str,
) -> Result<GetResult, crate::wire::ErrorEnvelope> {
    let Some((session, mut messages)) = store
        .get_message_context(message_id, request.context_depth)
        .await
        .map_err(storage_error)?
    else {
        return Err(error(
            ErrorCode::NotFound,
            "message not found",
            json!({"kind": "message", "pk": message_id}),
        ));
    };
    filter_messages(
        &mut messages,
        request.include_thinking,
        request.include_tool_results,
    );
    Ok(GetResult::Message { session, messages })
}

fn filter_session(session: &mut StoredSession, include_thinking: bool, include_tool_results: bool) {
    filter_messages(
        &mut session.messages,
        include_thinking,
        include_tool_results,
    );
}

fn filter_messages(
    messages: &mut Vec<StoredMessage>,
    include_thinking: bool,
    include_tool_results: bool,
) {
    for message in messages.iter_mut() {
        message.parts.retain(|part| match &part.kind {
            PartKind::Reasoning { .. } => include_thinking,
            PartKind::ToolResult { .. } => include_tool_results,
            PartKind::ToolApprovalRequest { .. } | PartKind::ToolApprovalResponse { .. } => false,
            PartKind::Text { .. } | PartKind::File { .. } | PartKind::ToolCall { .. } => true,
        });
    }

    messages.retain(|message| {
        message.message.role() != crate::types::Role::Tool || !message.parts.is_empty()
    });
}

fn storage_error(error_value: anyhow::Error) -> crate::wire::ErrorEnvelope {
    error(
        ErrorCode::StorageUnavailable,
        "storage operation failed",
        json!({"underlying": error_value.to_string()}),
    )
}
