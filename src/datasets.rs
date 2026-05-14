use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use lance::blob::{BlobArrayBuilder, blob_field};
use lance::dataset::{AutoCleanupParams, WriteParams};
use lance::deps::arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
    StringArray, TimestampMicrosecondArray,
};
use lance::deps::arrow_schema::{DataType, Field, Schema, TimeUnit};
use lance_file::version::LanceFileVersion;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::types::{FileData, Message, Part, PartKind, Role, Session};

pub const SESSIONS: &str = "sessions.lance";
pub const MESSAGES: &str = "messages.lance";
pub const PARTS: &str = "parts.lance";
pub const EMBEDDINGS: &str = "embeddings.lance";

/// Fixed embedding vector dimension (Qwen3-Embedding-0.6B, design.md 3.2.4).
/// A future model with a different dim activates a second `embeddings` table.
pub const EMBEDDING_DIM: usize = 1024;

pub fn write_params() -> WriteParams {
    WriteParams {
        data_storage_version: Some(LanceFileVersion::V2_2),
        enable_v2_manifest_paths: true,
        enable_stable_row_ids: true,
        auto_cleanup: Some(AutoCleanupParams {
            interval: 20,
            older_than: chrono::TimeDelta::days(30),
        }),
        ..WriteParams::default()
    }
}

pub fn session_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("id", DataType::Utf8, false),
        Field::new("parent_session_id", DataType::Utf8, true),
        Field::new("parent_message_id", DataType::Utf8, true),
        Field::new("source_agent", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("project", DataType::Utf8, true),
        Field::new("options", DataType::Utf8, false),
    ]))
}

pub fn message_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("session_id", DataType::Utf8, false),
        primary_field("id", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("role", DataType::Utf8, false),
        Field::new("source_agent", DataType::Utf8, false),
        Field::new("project", DataType::Utf8, true),
        Field::new("content", DataType::Utf8, true),
        Field::new("search_text", DataType::Utf8, true),
        Field::new("options", DataType::Utf8, false),
    ]))
}

pub fn part_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("message_id", DataType::Utf8, false),
        primary_field("id", DataType::Utf8, false),
        Field::new("ordinal", DataType::Int32, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("options", DataType::Utf8, false),
        Field::new("variant_data", DataType::Utf8, false),
        blob_field("data", true),
    ]))
}

pub fn embedding_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        primary_field("message_id", DataType::Utf8, false),
        primary_field("model_id", DataType::Utf8, false),
        primary_field("chunk_index", DataType::Int32, false),
        Field::new("vector", embedding_vector_type(), false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("source_agent", DataType::Utf8, false),
        Field::new("project", DataType::Utf8, true),
        Field::new("role", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]))
}

pub fn empty_batch(schema: Arc<Schema>) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .map(|field| lance::deps::arrow_array::new_empty_array(field.data_type()))
        .collect();
    RecordBatch::try_new(schema, arrays).context("failed to build empty Lance batch")
}

pub fn empty_reader(
    schema: Arc<Schema>,
) -> Result<
    RecordBatchIterator<
        std::vec::IntoIter<Result<RecordBatch, lance::deps::arrow_schema::ArrowError>>,
    >,
> {
    let batch = empty_batch(schema.clone())?;
    Ok(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}

pub struct MessageBatchRow<'a> {
    pub message: &'a Message,
    pub source_agent: &'a str,
    pub project: Option<&'a str>,
    pub search_text: Option<&'a str>,
}

/// One row of the `embeddings` dataset: a (Message, model, chunk) vector with
/// the filter columns denormalized from `messages` (design.md 3.2.4).
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingRow {
    pub message_id: String,
    pub model_id: String,
    pub chunk_index: i32,
    pub vector: Vec<f32>,
    pub session_id: String,
    pub source_agent: String,
    pub project: Option<String>,
    pub role: String,
    pub timestamp: DateTime<Utc>,
}

fn embedding_vector_type() -> DataType {
    DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float32, true)),
        EMBEDDING_DIM as i32,
    )
}

pub fn embeddings_batch(rows: &[EmbeddingRow]) -> Result<RecordBatch> {
    let schema = embedding_schema();
    let mut flat = Vec::with_capacity(rows.len() * EMBEDDING_DIM);
    for row in rows {
        if row.vector.len() != EMBEDDING_DIM {
            anyhow::bail!(
                "embedding for message {} has dim {}, expected {EMBEDDING_DIM}",
                row.message_id,
                row.vector.len(),
            );
        }
        flat.extend_from_slice(&row.vector);
    }
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        EMBEDDING_DIM as i32,
        Arc::new(Float32Array::from(flat)),
        None,
    )
    .context("failed to build embedding vector column")?;

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.message_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.model_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.chunk_index).collect::<Vec<_>>(),
            )),
            Arc::new(vectors),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.session_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.source_agent.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.project.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.role.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|row| micros(row.timestamp))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
        ],
    )
    .context("failed to build embeddings batch")
}

pub fn session_batch(session: &Session) -> Result<RecordBatch> {
    sessions_batch(std::slice::from_ref(session))
}

pub fn sessions_batch(sessions: &[Session]) -> Result<RecordBatch> {
    let schema = session_schema();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.parent_session_id.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.parent_message_id.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.source_agent.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    sessions
                        .iter()
                        .map(|session| micros(session.created_at))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| session.project.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                sessions
                    .iter()
                    .map(|session| json_string(&session.options))
                    .collect::<Result<Vec<_>>>()?,
            )),
        ],
    )
    .context("failed to build session batch")
}

pub fn message_batch(
    message: &Message,
    source_agent: &str,
    project: Option<&str>,
    search_text: Option<&str>,
) -> Result<RecordBatch> {
    messages_batch(&[MessageBatchRow {
        message,
        source_agent,
        project,
        search_text,
    }])
}

pub fn messages_batch(rows: &[MessageBatchRow<'_>]) -> Result<RecordBatch> {
    let schema = message_schema();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.message.session_id())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.message.id()).collect::<Vec<_>>(),
            )),
            Arc::new(
                TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|row| micros(row.message.timestamp()))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.message.role().as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.source_agent).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.project).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.message.system_content())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.search_text).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| json_string(row.message.options()))
                    .collect::<Result<Vec<_>>>()?,
            )),
        ],
    )
    .context("failed to build message batch")
}

pub fn part_batch(part: &Part) -> Result<RecordBatch> {
    parts_batch(std::slice::from_ref(part))
}

pub fn parts_batch(parts: &[Part]) -> Result<RecordBatch> {
    let schema = part_schema();
    let mut variant_data = Vec::with_capacity(parts.len());
    let mut blobs = BlobArrayBuilder::new(parts.len());
    for part in parts {
        variant_data.push(part_variant_json(&part.kind)?);
        match &part.kind {
            PartKind::File { data, .. } => match data {
                FileData::String(value) => blobs.push_bytes(value.as_bytes())?,
                FileData::Bytes(value) => blobs.push_bytes(value)?,
                FileData::Url(value) => blobs.push_uri(value)?,
            },
            PartKind::Text { .. }
            | PartKind::Reasoning { .. }
            | PartKind::ToolCall { .. }
            | PartKind::ToolResult { .. }
            | PartKind::ToolApprovalRequest { .. }
            | PartKind::ToolApprovalResponse { .. } => blobs.push_empty()?,
        }
    }

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                parts
                    .iter()
                    .map(|part| part.message_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                parts
                    .iter()
                    .map(|part| part.id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                parts.iter().map(|part| part.ordinal).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                parts
                    .iter()
                    .map(|part| part.kind.type_name())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                parts
                    .iter()
                    .map(|part| json_string(&part.options))
                    .collect::<Result<Vec<_>>>()?,
            )),
            Arc::new(StringArray::from(variant_data)),
            blobs.finish()?,
        ],
    )
    .context("failed to build parts batch")
}

pub fn session_from_batch(batch: &RecordBatch, row: usize) -> Result<Session> {
    Ok(Session {
        id: string(batch, "id", row)?.context("session id is null")?,
        parent_session_id: string(batch, "parent_session_id", row)?,
        parent_message_id: string(batch, "parent_message_id", row)?,
        source_agent: string(batch, "source_agent", row)?.context("source_agent is null")?,
        created_at: datetime(batch, "created_at", row)?,
        project: string(batch, "project", row)?,
        options: json_parse(&string(batch, "options", row)?.context("options is null")?)?,
    })
}

pub fn message_from_batch(batch: &RecordBatch, row: usize) -> Result<Message> {
    let id = string(batch, "id", row)?.context("message id is null")?;
    let session_id = string(batch, "session_id", row)?.context("message session_id is null")?;
    let timestamp = datetime(batch, "timestamp", row)?;
    let options = json_parse(&string(batch, "options", row)?.context("message options is null")?)?;

    match string(batch, "role", row)?
        .context("message role is null")?
        .as_str()
    {
        "system" => Ok(Message::System {
            id,
            session_id,
            timestamp,
            content: string(batch, "content", row)?.unwrap_or_default(),
            options,
        }),
        "user" => Ok(Message::User {
            id,
            session_id,
            timestamp,
            options,
        }),
        "assistant" => Ok(Message::Assistant {
            id,
            session_id,
            timestamp,
            options,
        }),
        "tool" => Ok(Message::Tool {
            id,
            session_id,
            timestamp,
            options,
        }),
        other => anyhow::bail!("unknown message role {other}"),
    }
}

/// Decode one `messages` row (projected by `PondStore::pending_messages_stream`)
/// into a `PendingMessage` for the embed worker.
pub fn pending_message_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<crate::substrate::PendingMessage> {
    Ok(crate::substrate::PendingMessage {
        message_id: string(batch, "id", row)?.context("message id is null")?,
        session_id: string(batch, "session_id", row)?.context("session_id is null")?,
        source_agent: string(batch, "source_agent", row)?.context("source_agent is null")?,
        project: string(batch, "project", row)?,
        role: string(batch, "role", row)?.context("role is null")?,
        timestamp: datetime(batch, "timestamp", row)?,
        search_text: string(batch, "search_text", row)?.context("search_text is null")?,
    })
}

pub fn part_from_batch(batch: &RecordBatch, row: usize) -> Result<Part> {
    let type_name = string(batch, "type", row)?.context("part type is null")?;
    let variant_data = string(batch, "variant_data", row)?.context("variant_data is null")?;
    Ok(Part {
        message_id: string(batch, "message_id", row)?.context("part message_id is null")?,
        id: string(batch, "id", row)?.context("part id is null")?,
        ordinal: int32(batch, "ordinal", row)?,
        options: json_parse(&string(batch, "options", row)?.context("part options is null")?)?,
        kind: part_kind_from_json(&type_name, &variant_data)?,
    })
}

pub fn string(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<String>> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("column {name} is not Utf8"))?;
    if array.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(array.value(row).to_owned()))
    }
}

fn int32(batch: &RecordBatch, name: &str, row: usize) -> Result<i32> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .with_context(|| format!("column {name} is not Int32"))?;
    Ok(array.value(row))
}

pub fn float32(batch: &RecordBatch, name: &str, row: usize) -> Result<f32> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<Float32Array>()
        .with_context(|| format!("column {name} is not Float32"))?;
    Ok(array.value(row))
}

pub fn datetime(batch: &RecordBatch, name: &str, row: usize) -> Result<DateTime<Utc>> {
    let array = batch
        .column_by_name(name)
        .with_context(|| format!("missing column {name}"))?
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .with_context(|| format!("column {name} is not timestamp_micros"))?;
    Utc.timestamp_micros(array.value(row))
        .single()
        .context("timestamp is out of range")
}

pub fn role_from_string(role: &str) -> Result<Role> {
    match role {
        "system" => Ok(Role::System),
        "user" => Ok(Role::User),
        "assistant" => Ok(Role::Assistant),
        "tool" => Ok(Role::Tool),
        other => anyhow::bail!("unknown role {other}"),
    }
}

fn primary_field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable).with_metadata(
        [(
            "lance-schema:unenforced-primary-key".to_owned(),
            "true".to_owned(),
        )]
        .into(),
    )
}

fn micros(timestamp: DateTime<Utc>) -> i64 {
    timestamp.timestamp_micros()
}

fn json_string<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("failed to serialize JSON field")
}

fn json_parse<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).context("failed to parse JSON field")
}

fn part_variant_json(kind: &PartKind) -> Result<String> {
    let value = serde_json::to_value(kind)?;
    let mut object = value
        .as_object()
        .cloned()
        .context("part variant did not serialize to an object")?;
    object.remove("type");
    serde_json::to_string(&object).context("failed to serialize part variant")
}

fn part_kind_from_json(type_name: &str, variant_data: &str) -> Result<PartKind> {
    let mut value = json_parse::<Value>(variant_data)?;
    let object = value
        .as_object_mut()
        .context("part variant data is not an object")?;
    object.insert("type".to_owned(), Value::String(type_name.to_owned()));
    serde_json::from_value(value).context("failed to parse part kind")
}
