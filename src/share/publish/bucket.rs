//! [`BucketPublisher`]: writes a [`ShareArtifact`](super::super::ShareArtifact)
//! to a public bucket via `object_store`, mirroring the config -> bucket
//! client construction already used by `substrate::export_write` /
//! `substrate::storage_check`.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor};
use object_store::{Attribute, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use crate::{
    config::CredsSet,
    share::{ShareArtifact, SharePublisher},
    substrate::StorageUrl,
};

pub struct BucketPublisher {
    bucket_url: StorageUrl,
    creds: BTreeMap<String, CredsSet>,
    /// Public origin serving `bucket_url`'s contents. `None` falls back to
    /// printing the resolved storage URL of the written object - not
    /// necessarily browser-clickable for a remote bucket, but exactly right
    /// for a local `file://` target (the `--to` smoke-test path).
    public_base_url: Option<String>,
}

impl BucketPublisher {
    pub fn new(
        bucket: &str,
        creds: BTreeMap<String, CredsSet>,
        public_base_url: Option<String>,
    ) -> Result<Self> {
        let bucket_url =
            StorageUrl::parse(bucket).with_context(|| format!("invalid share bucket URL {bucket:?}"))?;
        Ok(Self {
            bucket_url,
            creds,
            public_base_url,
        })
    }
}

#[async_trait::async_trait]
impl SharePublisher for BucketPublisher {
    async fn publish(&self, id: &str, artifact: &ShareArtifact) -> Result<String> {
        let resolved = self.bucket_url.resolve(&self.creds)?;
        let params = ObjectStoreParams {
            storage_options_accessor: (!resolved.options.is_empty()).then(|| {
                Arc::new(StorageOptionsAccessor::with_static_options(
                    resolved.options.clone(),
                ))
            }),
            ..Default::default()
        };
        let object_uri = format!(
            "{}/{}.{}",
            resolved.lance_url().as_str().trim_end_matches('/'),
            id,
            artifact.ext,
        );
        let registry = Arc::new(ObjectStoreRegistry::default());
        let (store, path) = ObjectStore::from_uri_and_params(registry, &object_uri, &params)
            .await
            .with_context(|| format!("failed to open share bucket for {object_uri}"))?;

        // A browser must render this, not download it - explicit Content-Type
        // is required, unlike every other object_store write in pond (Lance's
        // own format doesn't care, and JSONL exports are meant to be
        // downloaded). See docs/overview/share-feature.md.
        let opts = PutOptions {
            attributes: [(Attribute::ContentType, artifact.content_type.clone())]
                .into_iter()
                .collect(),
            mode: PutMode::Overwrite,
            ..Default::default()
        };
        let put = store
            .inner
            .put_opts(&path, PutPayload::from(artifact.bytes.clone()), opts)
            .await;
        match put {
            Ok(_) => {}
            // `LocalFileSystem` rejects any `put_opts` with attributes set at
            // all (no Content-Type concept for a local file) - every real S3-
            // compatible backend (R2, S3, Hetzner, MinIO, B2) supports it, so
            // this only matters for a local `--to file://` smoke test. Retry
            // once without attributes rather than failing a publish that
            // would otherwise fully succeed.
            Err(object_store::Error::NotImplemented { .. }) => {
                store
                    .inner
                    .put(&path, PutPayload::from(artifact.bytes.clone()))
                    .await
                    .with_context(|| format!("failed to publish share artifact to {object_uri}"))?;
            }
            Err(error) => {
                return Err(anyhow::anyhow!(error))
                    .with_context(|| format!("failed to publish share artifact to {object_uri}"));
            }
        }

        Ok(match &self.public_base_url {
            Some(base) => format!("{}/{}.{}", base.trim_end_matches('/'), id, artifact.ext),
            None => object_uri,
        })
    }
}
