//! Vendor smoke test: pond's session-row upserts depend on Lance's
//! `MergeInsertBuilder` treating an unenforced primary key as a find-or-create
//! match key. If Lance ever changed that behavior, every Store upsert would
//! silently double-insert. The remaining test pins that one contract.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use lance::Dataset;
use lance::dataset::{MergeInsertBuilder, WriteParams};
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
