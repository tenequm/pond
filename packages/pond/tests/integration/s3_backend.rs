//! Real S3-wire backend via in-process s3s-fs (spec.md#substrate,
//! spec.md#lance-chokepoints-storage). Proves Lance's commit handler reaches
//! `If-None-Match: *` -> 412 PreconditionFailed end-to-end, which `memory://`
//! sidesteps.
#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "tests fail by panicking"
)]

use std::collections::HashMap;
use std::net::SocketAddr;

use axum::http::{Extensions, HeaderMap, Method, Uri, header::CONTENT_LENGTH};
use chrono::Utc;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use pond::{
    sessions::Store,
    wire::{ProviderOptions, Session},
};
use s3s::auth::SimpleAuth;
use s3s::route::S3Route;
use s3s::service::S3ServiceBuilder;
use s3s::{Body, S3Request, S3Response, S3Result};
use s3s_fs::FileSystem;

const ACCESS_KEY: &str = "test-access-key";
const SECRET_KEY: &str = "test-secret-key";
use tempfile::TempDir;
use tokio::net::TcpListener;
use url::Url;

struct S3sFixture {
    endpoint: String,
    _root: TempDir,
}

/// Hetzner's Ceph answers an UploadPartCopy without `Content-Length` with
/// 411, and object_store 0.14 sends it that way; s3s-fs alone accepts it.
struct CephPartCopy;

#[async_trait::async_trait]
impl S3Route for CephPartCopy {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        _: &mut Extensions,
    ) -> bool {
        *method == Method::PUT
            && uri
                .query()
                .is_some_and(|query| query.contains("partNumber="))
            && headers.contains_key("x-amz-copy-source")
            && !headers.contains_key(CONTENT_LENGTH)
    }

    async fn call(&self, _: S3Request<Body>) -> S3Result<S3Response<Body>> {
        Err(s3s::s3_error!(MissingContentLength))
    }
}

async fn spawn_s3s_fs(bucket: &str, ceph_part_copy: bool) -> anyhow::Result<S3sFixture> {
    let root = TempDir::new()?;
    // s3s-fs maps `s3://<bucket>/<key>` to `<root>/<bucket>/<key>`; Lance's
    // S3 provider does not create buckets, so the dir must exist up front.
    std::fs::create_dir(root.path().join(bucket))?;

    let fs = FileSystem::new(root.path()).map_err(|e| anyhow::anyhow!("FileSystem::new: {e:?}"))?;
    let mut b = S3ServiceBuilder::new(fs);
    b.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
    if ceph_part_copy {
        b.set_route(CephPartCopy);
    }
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

fn storage_options(endpoint: &str) -> HashMap<String, String> {
    HashMap::from([
        ("access_key_id".into(), ACCESS_KEY.into()),
        ("secret_access_key".into(), SECRET_KEY.into()),
        ("region".into(), "us-east-1".into()),
        ("endpoint".into(), endpoint.into()),
        ("allow_http".into(), "true".into()),
        ("virtual_hosted_style_request".into(), "false".into()),
    ])
}

fn make_session(id: usize) -> Session {
    Session {
        id: format!("01HXY{id:08}"),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": format!("/tmp/p/{id}")}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    }
}

/// `pond storage check` probe against the real S3 wire: the fat-URL grammar
/// folds the fixture endpoint in, a scoped creds set resolves, and the
/// conditional-put pair proves the If-None-Match -> 412 path. Then the same
/// probe with a wrong secret must classify as the auth failure naming the
/// set (403 -> `PermissionDenied` in object_store).
#[tokio::test(flavor = "multi_thread")]
async fn config_check_probe_passes_and_classifies_auth_failure() -> anyhow::Result<()> {
    use pond::config::CredsSet;
    use pond::substrate::{CheckFailure, StorageUrl, storage_check};
    use std::collections::BTreeMap;

    let fx = spawn_s3s_fs("pond-check", false).await?;
    let endpoint_host = fx.endpoint.trim_start_matches("http://");
    let url = StorageUrl::parse(&format!("s3+http://{endpoint_host}/pond-check/data"))?;
    let mut creds = BTreeMap::new();
    creds.insert(
        "test".to_owned(),
        // Minimal set on purpose: no region (the s3+ scheme default must
        // carry the probe) and no virtual_hosted_style_request (an IP
        // endpoint host must auto-select path-style, which is all s3s-fs
        // serves).
        CredsSet {
            access_key_id: Some(ACCESS_KEY.to_owned()),
            secret_access_key: Some(SECRET_KEY.to_owned()),
            ..CredsSet::default()
        },
    );
    let resolved = url.resolve(&creds)?;
    storage_check(&resolved)
        .await
        .expect("probe must pass against the s3s fixture");

    creds.get_mut("test").expect("set exists").secret_access_key = Some("wrong-secret".to_owned());
    let resolved = url.resolve(&creds)?;
    match storage_check(&resolved).await {
        Err(CheckFailure::Auth { set, .. }) => assert_eq!(set, "test"),
        other => panic!("wrong secret must classify as Auth, got: {other:?}"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn s3s_fs_in_process_round_trips_a_session() -> anyhow::Result<()> {
    let fx = spawn_s3s_fs("pond", false).await?;
    let url = Url::parse("s3://pond/data")?;
    let store = Store::open_with_options(
        &url,
        storage_options(&fx.endpoint),
        pond::substrate::RuntimeCaps::default(),
    )
    .await?;

    store.upsert_sessions(&[make_session(1)]).await?;
    let (sessions, _, _) = store.row_counts().await?;
    assert_eq!(sessions, 1);
    Ok(())
}

/// A backend that rejects the copy Lance creates tables with (Hetzner's
/// Ceph, see `CephPartCopy`) fails the check naming the fix, and passes once
/// the creds set's `extra` switches to a plain CopyObject.
#[tokio::test(flavor = "multi_thread")]
async fn config_check_fails_when_the_table_creation_copy_is_rejected() -> anyhow::Result<()> {
    use pond::config::CredsSet;
    use pond::substrate::{CheckFailure, StorageUrl, storage_check};
    use std::collections::BTreeMap;

    let fx = spawn_s3s_fs("pond-ceph", true).await?;
    let endpoint_host = fx.endpoint.trim_start_matches("http://");
    let url = StorageUrl::parse(&format!("s3+http://{endpoint_host}/pond-ceph/data"))?;
    let mut set = CredsSet {
        access_key_id: Some(ACCESS_KEY.to_owned()),
        secret_access_key: Some(SECRET_KEY.to_owned()),
        ..CredsSet::default()
    };
    let creds = BTreeMap::from([("test".to_owned(), set.clone())]);
    match storage_check(&url.resolve(&creds)?).await {
        Err(CheckFailure::CopyRejected { .. }) => {}
        other => panic!("a rejected table-creation copy must fail the check, got: {other:?}"),
    }

    set.extra.insert(
        "aws_copy_if_not_exists".to_owned(),
        "header:If-None-Match:*".to_owned(),
    );
    let creds = BTreeMap::from([("test".to_owned(), set)]);
    storage_check(&url.resolve(&creds)?)
        .await
        .expect("a plain CopyObject passes the check");
    Ok(())
}

/// Lance 14's default copy cannot create a table on a Ceph-like backend;
/// the plain CopyObject pond defaults to on Hetzner can.
#[tokio::test(flavor = "multi_thread")]
async fn plain_copy_creates_tables_on_a_ceph_like_backend() -> anyhow::Result<()> {
    let fx = spawn_s3s_fs("pond", true).await?;
    let Err(error) = Store::open_with_options(
        &Url::parse("s3://pond/default-copy")?,
        storage_options(&fx.endpoint),
        pond::substrate::RuntimeCaps::default(),
    )
    .await
    else {
        panic!(
            "this open succeeding means object_store now sends Content-Length on \
             UploadPartCopy, and the Hetzner copy default in apply_remote_storage_defaults can go"
        );
    };
    assert!(format!("{error:#}").contains("411"), "{error:#}");

    let mut options = storage_options(&fx.endpoint);
    options.insert(
        "aws_copy_if_not_exists".into(),
        "header:If-None-Match:*".into(),
    );
    let store = Store::open_with_options(
        &Url::parse("s3://pond/plain-copy")?,
        options,
        pond::substrate::RuntimeCaps::default(),
    )
    .await?;
    store.upsert_sessions(&[make_session(1)]).await?;
    assert_eq!(store.row_counts().await?.0, 1);
    Ok(())
}

// OCC against real S3 is verified by Lance's own `test_concurrent_writers`
// (lance/src/io/commit/s3_test.rs). We can't repeat that contract against
// s3s-fs in-process: s3s-fs implements `If-None-Match: *` as a non-atomic
// stat-then-write (crates/s3s-fs/src/s3.rs:691 -> :749), so two concurrent
// `PutMode::Create` calls can both pass the existence check, both write, and
// the second silently overwrites the first. Real S3, Hetzner (Ceph RGW),
// MinIO, and R2 do this atomically; s3s-fs does not. The smoke test above is
// sufficient to verify pond's `s3://` wire path; OCC must be verified against
// a real bucket via `cargo bench --bench backend_bench -- --s3-url s3://...`.
