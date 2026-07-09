//! `BucketPublisher` against the in-process s3s-fs S3 simulator, following
//! `s3_backend.rs`'s fixture pattern: publish an artifact, read it back
//! straight off the filesystem the fixture serves from, and confirm both the
//! bytes and the `Content-Type` header the browser needs to render (not
//! download) the shared page.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::net::SocketAddr;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use object_store::ObjectStoreExt;
use pond::{
    config::CredsSet,
    share::{ShareArtifact, SharePublisher, publish::bucket::BucketPublisher},
};
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s_fs::FileSystem;
use tempfile::TempDir;
use tokio::net::TcpListener;

const ACCESS_KEY: &str = "test-access-key";
const SECRET_KEY: &str = "test-secret-key";

struct S3sFixture {
    endpoint: String,
    _root: TempDir,
}

async fn spawn_s3s_fs(bucket: &str) -> anyhow::Result<S3sFixture> {
    let root = TempDir::new()?;
    std::fs::create_dir(root.path().join(bucket))?;

    let fs = FileSystem::new(root.path()).map_err(|e| anyhow::anyhow!("FileSystem::new: {e:?}"))?;
    let mut b = S3ServiceBuilder::new(fs);
    b.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
    let service = b.build();

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let endpoint = format!("http://{addr}");

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let service = service.clone();
            tokio::spawn(async move {
                let _ = ConnBuilder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });

    Ok(S3sFixture {
        endpoint,
        _root: root,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn bucket_publisher_writes_bytes_and_content_type_and_returns_public_url() -> anyhow::Result<()> {
    let fx = spawn_s3s_fs("pond-shares").await?;
    let endpoint_host = fx.endpoint.trim_start_matches("http://");
    let bucket_url = format!("s3+http://{endpoint_host}/pond-shares/");

    let mut creds = BTreeMap::new();
    creds.insert(
        "share".to_owned(),
        CredsSet {
            scope: Some(bucket_url.clone()),
            access_key_id: Some(ACCESS_KEY.to_owned()),
            secret_access_key: Some(SECRET_KEY.to_owned()),
            ..CredsSet::default()
        },
    );

    let publisher = BucketPublisher::new(
        &bucket_url,
        creds.clone(),
        Some("https://shares.example.com".to_owned()),
    )?;
    let artifact = ShareArtifact {
        bytes: b"<!DOCTYPE html><html><body>hi</body></html>".to_vec(),
        content_type: "text/html; charset=utf-8".to_owned(),
        ext: "html".to_owned(),
    };
    let url = publisher.publish("share_test123", &artifact).await?;
    assert_eq!(url, "https://shares.example.com/share_test123.html");

    // Read the object back straight off the fixture's filesystem (s3s-fs
    // maps s3://<bucket>/<key> to <root>/<bucket>/<key>) to confirm the PUT
    // actually landed with the right bytes, independent of pond's own read
    // path.
    let object_path = fx._root.path().join("pond-shares").join("share_test123.html");
    let written = std::fs::read(&object_path).expect("published object must exist on disk");
    assert_eq!(written, artifact.bytes);

    // The whole point of BucketPublisher setting `PutOptions::attributes`
    // explicitly: without it, a browser downloads the shared page instead of
    // rendering it. Confirm the S3 GET response actually carries it back,
    // going through the same StorageUrl -> resolve -> ObjectStore
    // construction BucketPublisher itself uses (pond's `s3+http` fat-URL
    // grammar isn't a scheme Lance's registry understands directly) - not
    // just that PUT accepted the option.
    let get_url = pond::substrate::StorageUrl::parse(&format!(
        "s3+http://{endpoint_host}/pond-shares/share_test123.html"
    ))?;
    let resolved = get_url.resolve(&creds)?;
    let params = lance_io::object_store::ObjectStoreParams {
        storage_options_accessor: (!resolved.options.is_empty()).then(|| {
            std::sync::Arc::new(lance_io::object_store::StorageOptionsAccessor::with_static_options(
                resolved.options.clone(),
            ))
        }),
        ..Default::default()
    };
    let registry = std::sync::Arc::new(lance_io::object_store::ObjectStoreRegistry::default());
    let (store, path) = lance_io::object_store::ObjectStore::from_uri_and_params(
        registry,
        resolved.lance_url().as_str(),
        &params,
    )
    .await?;
    let got = store.inner.get(&path).await?;
    assert_eq!(
        got.attributes.get(&object_store::Attribute::ContentType),
        Some(&object_store::AttributeValue::from("text/html; charset=utf-8".to_owned())),
        "GET must return the Content-Type set on PUT, or a browser downloads the page instead of rendering it",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn bucket_publisher_falls_back_to_storage_url_with_no_public_base_url() -> anyhow::Result<()> {
    let fx = spawn_s3s_fs("pond-shares").await?;
    let endpoint_host = fx.endpoint.trim_start_matches("http://");
    let bucket_url = format!("s3+http://{endpoint_host}/pond-shares/");

    let mut creds = BTreeMap::new();
    creds.insert(
        "share".to_owned(),
        CredsSet {
            scope: Some(bucket_url.clone()),
            access_key_id: Some(ACCESS_KEY.to_owned()),
            secret_access_key: Some(SECRET_KEY.to_owned()),
            ..CredsSet::default()
        },
    );

    let publisher = BucketPublisher::new(&bucket_url, creds, None)?;
    let artifact = ShareArtifact {
        bytes: b"hi".to_vec(),
        content_type: "text/html; charset=utf-8".to_owned(),
        ext: "html".to_owned(),
    };
    let url = publisher.publish("share_test456", &artifact).await?;
    // No public_base_url configured: falls back to the resolved storage URL
    // of the written object - not a public link for a remote bucket, but
    // directly usable for a local file:// smoke-test target.
    assert!(
        url.ends_with("/share_test456.html"),
        "expected a storage-URL fallback, got: {url}",
    );
    assert!(!url.starts_with("https://shares.example.com"));

    Ok(())
}
