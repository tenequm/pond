#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use chrono::Duration;
use lance::Dataset;
use lance::blob::{BlobArrayBuilder, blob_field};
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteParams};
use lance::deps::arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use lance::deps::arrow_schema::{DataType, Field, Schema};
use lance_file::version::LanceFileVersion;

#[tokio::test]
async fn merge_insert_uses_unenforced_primary_key_for_find_or_create() -> anyhow::Result<()> {
    let uri = temp_uri()?;
    let schema = keyed_schema();

    let initial = batch(&schema, &[1, 2], &[10, 20])?;
    let reader = RecordBatchIterator::new([Ok(initial)], schema.clone());
    let dataset = Arc::new(Dataset::write(reader, &uri, Some(write_params_v2_2())).await?);

    let source = batch(&schema, &[2, 3], &[200, 30])?;
    let source_reader = RecordBatchIterator::new([Ok(source)], schema.clone());
    let (dataset, stats) = MergeInsertBuilder::try_new(dataset, Vec::new())?
        .try_build()?
        .execute_reader(Box::new(source_reader))
        .await?;

    assert_eq!(stats.num_inserted_rows, 1);
    assert_eq!(stats.num_updated_rows, 0);
    assert_eq!(dataset.count_rows(None).await?, 3);
    assert_eq!(dataset.count_rows(Some("id = 3".to_owned())).await?, 1);
    assert_eq!(dataset.count_rows(Some("value = 200".to_owned())).await?, 0);

    Ok(())
}

#[tokio::test]
async fn merge_insert_do_nothing_skips_insert_only_rows() -> anyhow::Result<()> {
    let uri = temp_uri()?;
    let schema = keyed_schema();

    let initial = batch(&schema, &[1, 2], &[10, 20])?;
    let reader = RecordBatchIterator::new([Ok(initial)], schema.clone());
    let dataset = Arc::new(Dataset::write(reader, &uri, Some(write_params_v2_2())).await?);

    let source = batch(&schema, &[2, 4], &[200, 400])?;
    let source_reader = RecordBatchIterator::new([Ok(source)], schema.clone());
    let (dataset, stats) = MergeInsertBuilder::try_new(dataset, Vec::new())?
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::DoNothing)
        .try_build()?
        .execute_reader(Box::new(source_reader))
        .await?;

    assert_eq!(stats.num_inserted_rows, 0);
    assert_eq!(stats.num_updated_rows, 1);
    assert_eq!(dataset.count_rows(None).await?, 2);
    assert_eq!(dataset.count_rows(Some("id = 4".to_owned())).await?, 0);
    assert_eq!(dataset.count_rows(Some("value = 200".to_owned())).await?, 1);

    Ok(())
}

#[tokio::test]
async fn blob_v2_struct_column_round_trips() -> anyhow::Result<()> {
    let uri = temp_uri()?;

    let mut blobs = BlobArrayBuilder::new(2);
    blobs.push_bytes(b"pond")?;
    blobs.push_empty()?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        blob_field("data", true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2])), blobs.finish()?],
    )?;
    let reader = RecordBatchIterator::new([Ok(batch)], schema);
    let dataset = Arc::new(Dataset::write(reader, &uri, Some(write_params_v2_2())).await?);

    let blobs = dataset.take_blobs_by_indices(&[0, 1], "data").await?;

    assert_eq!(blobs.len(), 2);
    assert_eq!(blobs[0].read().await?.as_ref(), b"pond");
    assert!(blobs[1].read().await?.is_empty());

    Ok(())
}

#[tokio::test]
async fn cleanup_old_versions_accepts_delete_unverified() -> anyhow::Result<()> {
    let uri = temp_uri()?;
    let schema = keyed_schema();

    let initial = batch(&schema, &[1], &[10])?;
    let reader = RecordBatchIterator::new([Ok(initial)], schema.clone());
    let dataset = Arc::new(Dataset::write(reader, &uri, Some(write_params_v2_2())).await?);

    let source = batch(&schema, &[2], &[20])?;
    let source_reader = RecordBatchIterator::new([Ok(source)], schema);
    let (dataset, _) = MergeInsertBuilder::try_new(dataset, Vec::new())?
        .try_build()?
        .execute_reader(Box::new(source_reader))
        .await?;

    dataset
        .cleanup_old_versions(Duration::zero(), Some(true), None)
        .await?;

    Ok(())
}

fn temp_uri() -> anyhow::Result<String> {
    Ok(tempfile::tempdir()?.keep().to_string_lossy().into_owned())
}

fn write_params_v2_2() -> WriteParams {
    WriteParams {
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..WriteParams::default()
    }
}

fn keyed_schema() -> Arc<Schema> {
    let id = Field::new("id", DataType::Int32, false).with_metadata(
        [(
            "lance-schema:unenforced-primary-key".to_owned(),
            "true".to_owned(),
        )]
        .into(),
    );

    Arc::new(Schema::new(vec![
        id,
        Field::new("value", DataType::Int32, false),
    ]))
}

fn batch(schema: &Arc<Schema>, ids: &[i32], values: &[i32]) -> anyhow::Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
        ],
    )?)
}
