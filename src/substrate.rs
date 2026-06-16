//! The storage substrate (spec.md#substrate): pond's one seam to Lance,
//! generic over consumers.

use crate::{
    RetryPolicy,
    config::{self, CredsSet},
    handlers::NamespaceIdent,
    sessions::{self},
};
use anyhow::{Context, Result, anyhow, bail};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::index::DatasetIndexRemapperOptions;
use lance::dataset::optimize::{CompactionOptions, commit_compaction, plan_compaction};
pub use lance::dataset::write::merge_insert::MergeStats;
use lance::dataset::write::merge_insert::SourceDedupeBehavior;
use lance::dataset::{InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode};
pub use lance::dataset::{WriteParams, WriteStats};
use lance::deps::arrow_array::{Array, RecordBatch, RecordBatchIterator, StringArray};
use lance::deps::datafusion::physical_plan::SendableRecordBatchStream;
use lance::index::DatasetIndexExt;
use lance::index::DatasetIndexInternalExt;
use lance::index::vector::VectorIndexParams;
use lance::session::Session;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::{BuiltinIndexType, InvertedIndexParams, ScalarIndexParams};
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor, uri_to_url,
};
use lance_linalg::distance::MetricType;
use lance_namespace::LanceNamespace;
use lance_namespace::error::{ErrorCode, NamespaceError};
use lance_namespace::models::DescribeTableRequest;
use lance_namespace_impls::ConnectBuilder;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OnceCell};
use tokio_stream::StreamExt;
use url::Url;
/// Embedded-row count at which pond builds the IVF_PQ vector index on
/// `messages.vector` (spec.md#search). Below it, vector search runs a
/// brute-force flat scan - exact and fast at small and medium scale, and
/// IVF_PQ cannot train well on fewer vectors anyway.
pub const VECTOR_INDEX_ACTIVATION_ROWS: usize = 100_000;

/// Default minimum unindexed-fragment count required before a per-intent
/// append/rebuild step is admitted into `optimize_table_indices`. Lower
/// values make each commit smaller and more frequent (bad on remote
/// stores); higher values let fragments accumulate behind the brute-force
/// fallback. 4 is the floor of the documented 4-8 band.
pub const DEFAULT_INDEX_LAG_THRESHOLD: usize = 4;

static INDEX_LAG_THRESHOLD_RUNTIME: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Seed the process-wide index-lag threshold from `[maintenance].index_lag_threshold`.
/// First call wins (mirrors `embed::init_model_id` / `sessions::init_embedding_dim`).
pub fn init_index_lag_threshold(value: usize) {
    INDEX_LAG_THRESHOLD_RUNTIME.get_or_init(|| value);
}

pub fn index_lag_threshold() -> usize {
    INDEX_LAG_THRESHOLD_RUNTIME
        .get()
        .copied()
        .unwrap_or(DEFAULT_INDEX_LAG_THRESHOLD)
}

// ---------------------------------------------------------------------------
// Storage addresses (spec.md#storage-url-grammar)
// ---------------------------------------------------------------------------

/// A parsed pond storage address. The fat-URL grammar
/// (`s3+https://host/bucket/prefix`) folds the endpoint into the address so
/// it can never desync from the bucket (the litestream out-of-band-endpoint
/// failure class); parsing splits it back into the URL Lance opens plus the
/// `object_store` options the endpoint implies.
#[derive(Debug, Clone, PartialEq)]
pub struct StorageUrl {
    /// The address as written, canonicalized (scheme/host lowercased by
    /// `url`, default port stripped, recognized query params removed). Scope
    /// matching (spec.md#creds-scope-match) and display use this form.
    canonical: Url,
    /// The URL handed to Lance.
    lance: Url,
    /// Options implied by the scheme - lowest precedence in assembly.
    scheme_options: Vec<(&'static str, String)>,
    /// Recognized `?key=value` params - highest precedence.
    query_options: Vec<(&'static str, String)>,
    /// `?creds=<name>`: explicit set binding, beats scope matching.
    creds_pointer: Option<String>,
    /// Endpoint pieces for the `s3+` schemes. The final endpoint URL depends
    /// on the resolved `virtual_hosted_style_request` value (object_store
    /// wants the bucket inside the endpoint host under virtual-hosted
    /// addressing), so it is assembled at resolve time, not parse time.
    endpoint: Option<S3Endpoint>,
}

#[derive(Debug, Clone, PartialEq)]
struct S3Endpoint {
    scheme: &'static str,
    /// host[:port]
    authority: String,
    bucket: String,
}

/// Query params pond recognizes (and strips before the URL reaches Lance).
/// Anything else is a hard error - a typoed param must not silently reach
/// the object store as part of the path.
const RECOGNIZED_QUERY_PARAMS: [&str; 3] = ["creds", "region", "virtual_hosted_style_request"];

impl StorageUrl {
    /// Parse a storage address (spec.md#storage-url-grammar): bare/`~` paths,
    /// `file://`, `s3://`, `s3+https://` / `s3+http://`, `gs://`, `az://`,
    /// and the test-only `memory://` / `shared-memory://`.
    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            bail!("storage path is empty");
        }
        // Bare paths, `~/...`, and `file://` go through Lance's own
        // `uri_to_url` so pond accepts exactly what Lance accepts.
        if !trimmed.contains("://") || trimmed.starts_with("file://") {
            let url =
                uri_to_url(trimmed).with_context(|| format!("invalid storage path {trimmed:?}"))?;
            // Bare paths percent-encode `?` (a legal filename character), so
            // only an explicit `file://...?x=y` parses a query here. No local
            // scheme takes one; reject like the remote schemes do instead of
            // silently carrying it into the path Lance opens.
            if url.query().is_some() {
                bail!("storage URL {trimmed:?} carries query params; local URLs take none");
            }
            return Ok(Self::plain(url));
        }
        let url =
            Url::parse(trimmed).with_context(|| format!("invalid storage URL {trimmed:?}"))?;
        // RFC 3986 deprecates userinfo; argv/history/ps/logs leak it. Never.
        if !url.username().is_empty() || url.password().is_some() {
            bail!(
                "storage URL {trimmed:?} embeds credentials; put them in [creds.*] (or POND_CREDS_*) instead"
            );
        }
        match url.scheme() {
            "memory" | "shared-memory" => {
                if url.query().is_some() {
                    bail!(
                        "storage URL {trimmed:?} carries query params; {}:// URLs take none",
                        url.scheme(),
                    );
                }
                Ok(Self::plain(url))
            }
            "s3" | "gs" => {
                let (canonical, query_options, creds_pointer) = strip_query(url)?;
                let mut lance = canonical.clone();
                lance.set_query(None);
                Ok(Self {
                    canonical,
                    lance,
                    scheme_options: Vec::new(),
                    query_options,
                    creds_pointer,
                    endpoint: None,
                })
            }
            "s3+https" | "s3+http" => {
                let (mut canonical, query_options, creds_pointer) = strip_query(url)?;
                let tls = canonical.scheme() == "s3+https";
                // `url` treats non-special schemes' default ports as
                // explicit; strip them so scope matching can't split on
                // `:443` vs nothing.
                if canonical.port() == Some(if tls { 443 } else { 80 }) {
                    let _ = canonical.set_port(None);
                }
                let host = canonical
                    .host_str()
                    .ok_or_else(|| anyhow!("storage URL {trimmed:?} has no endpoint host"))?;
                let endpoint_authority = match canonical.port() {
                    Some(port) => format!("{host}:{port}"),
                    None => host.to_owned(),
                };
                let mut segments = canonical.path().trim_start_matches('/').splitn(2, '/');
                let bucket = segments.next().unwrap_or_default().to_owned();
                let prefix = segments.next().unwrap_or_default().to_owned();
                if bucket.is_empty() {
                    bail!(
                        "storage URL {trimmed:?} is missing the bucket: the form is {}://host/bucket/prefix",
                        canonical.scheme(),
                    );
                }
                let lance = Url::parse(&format!("s3://{bucket}/{prefix}")).with_context(|| {
                    format!("storage URL {trimmed:?}: bucket/prefix do not form a valid s3:// URL")
                })?;
                let scheme = if tls { "https" } else { "http" };
                // Virtual-hosted is the Hetzner / R2 / B2 default, but an IP
                // host can't carry a bucket subdomain (`bucket.127.0.0.1`
                // does not resolve), so MinIO-style IP endpoints flip to
                // path-style. Override either way via the creds-set field or
                // `?virtual_hosted_style_request=`. Note: `url` keeps IPv4
                // hosts as `Host::Domain` on non-special schemes, hence the
                // explicit IpAddr parse; IPv6 brackets still need the Host
                // match.
                let virtual_hosted = host.parse::<std::net::IpAddr>().is_err()
                    && !matches!(canonical.host(), Some(url::Host::Ipv6(_)));
                let scheme_options = vec![
                    ("allow_http", (!tls).to_string()),
                    ("virtual_hosted_style_request", virtual_hosted.to_string()),
                    // S3-compatible stores ignore the SigV4 region, so a
                    // deterministic default (the DuckDB / litestream
                    // convention) beats Lance's env-chain fallback, where a
                    // stray AWS_REGION changes behavior. Real AWS (`s3://`,
                    // no endpoint) auto-resolves the bucket region inside
                    // Lance instead. Override: creds-set field or ?region=.
                    ("region", "us-east-1".to_owned()),
                ];
                Ok(Self {
                    canonical,
                    lance,
                    scheme_options,
                    query_options,
                    creds_pointer,
                    endpoint: Some(S3Endpoint {
                        scheme,
                        authority: endpoint_authority,
                        bucket,
                    }),
                })
            }
            "az" => {
                let (canonical, query_options, creds_pointer) = strip_query(url)?;
                let account = canonical
                    .host_str()
                    .ok_or_else(|| anyhow!("storage URL {trimmed:?} has no account: the form is az://account/container/prefix"))?
                    .to_owned();
                let mut segments = canonical.path().trim_start_matches('/').splitn(2, '/');
                let container = segments.next().unwrap_or_default();
                if container.is_empty() {
                    bail!(
                        "storage URL {trimmed:?} is missing the container: the form is az://account/container/prefix"
                    );
                }
                let prefix = segments.next().unwrap_or_default();
                let lance = Url::parse(&format!("az://{container}/{prefix}"))
                    .with_context(|| format!("storage URL {trimmed:?}: container/prefix do not form a valid az:// URL"))?;
                Ok(Self {
                    canonical,
                    lance,
                    scheme_options: vec![("account_name", account)],
                    query_options,
                    creds_pointer,
                    endpoint: None,
                })
            }
            other => bail!(
                "storage URL scheme {other:?} not recognized; use a local path, s3://, s3+https://, s3+http://, gs://, or az://"
            ),
        }
    }

    /// A scheme with no creds machinery: canonical == lance, no options.
    fn plain(url: Url) -> Self {
        Self {
            canonical: url.clone(),
            lance: url,
            scheme_options: Vec::new(),
            query_options: Vec::new(),
            creds_pointer: None,
            endpoint: None,
        }
    }

    /// The URL Lance opens (endpoint folded into options, not the URL).
    pub fn lance_url(&self) -> &Url {
        &self.lance
    }

    /// The canonical as-written address - what scope matching compares
    /// against and what display surfaces show (it carries the endpoint).
    pub fn canonical(&self) -> &Url {
        &self.canonical
    }

    pub fn is_local(&self) -> bool {
        config::is_local(&self.canonical)
    }

    /// Render for human output: local URLs as plain paths, remote verbatim.
    pub fn display(&self) -> String {
        config::display(&self.canonical)
    }

    /// Whether this scheme authenticates at all. `file`, `memory`, and
    /// `shared-memory` take no credentials; resolution skips them entirely.
    fn takes_credentials(&self) -> bool {
        !matches!(
            self.canonical.scheme(),
            "file" | "file+uring" | "memory" | "shared-memory"
        )
    }

    /// Resolve this address against the configured creds sets
    /// (spec.md#creds-scope-match): `?creds=` pointer > longest scoped
    /// prefix match > the scope-less catch-all > none (object_store's
    /// ambient SDK chain). Option assembly, later wins: scheme-derived ->
    /// matched set (non-secret fields + `extra`, then materialized secrets)
    /// -> URL query params.
    pub fn resolve(&self, creds: &BTreeMap<String, CredsSet>) -> Result<ResolvedStorage> {
        if !self.takes_credentials() {
            return Ok(ResolvedStorage {
                storage: self.clone(),
                options: HashMap::new(),
                binding: CredsBinding::NotApplicable,
            });
        }
        let matched: Option<(&String, &CredsSet, BindVia)> = match &self.creds_pointer {
            Some(name) => {
                let set = creds.get(name).ok_or_else(|| {
                    anyhow!(
                        "URL names ?creds={name} but no [creds.{name}] set is configured; define it or drop the pointer"
                    )
                })?;
                Some((name, set, BindVia::Pointer))
            }
            None => {
                let mut best: Option<(&String, &CredsSet, String)> = None;
                for (name, set) in creds {
                    let Some(scope) = &set.scope else { continue };
                    let scope_url = parse_scope(scope).with_context(|| {
                        format!("[creds.{name}] scope {scope:?} is not a valid URL prefix")
                    })?;
                    if scope_matches(&scope_url, &self.canonical)
                        && best
                            .as_ref()
                            .is_none_or(|(_, _, len)| scope_url.as_str().len() > len.len())
                    {
                        best = Some((name, set, scope_url.as_str().to_owned()));
                    }
                }
                match best {
                    Some((name, set, _)) => Some((name, set, BindVia::Scope)),
                    None => creds
                        .iter()
                        .find(|(_, set)| set.scope.is_none())
                        .map(|(name, set)| (name, set, BindVia::CatchAll)),
                }
            }
        };
        let mut options: HashMap<String, String> = self
            .scheme_options
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect();
        let binding = match matched {
            None => CredsBinding::Ambient,
            Some((name, set, via)) => {
                if let Some(region) = &set.region {
                    options.insert("region".to_owned(), region.clone());
                }
                if let Some(virtual_hosted) = set.virtual_hosted_style_request {
                    options.insert(
                        "virtual_hosted_style_request".to_owned(),
                        virtual_hosted.to_string(),
                    );
                }
                for (key, value) in &set.extra {
                    options.insert(key.clone(), value.clone());
                }
                if let Some(value) = materialize_secret(
                    name,
                    "access_key_id",
                    set.access_key_id.as_deref(),
                    set.access_key_id_file.as_deref(),
                    None,
                )? {
                    options.insert("access_key_id".to_owned(), value);
                }
                if let Some(value) = materialize_secret(
                    name,
                    "secret_access_key",
                    set.secret_access_key.as_deref(),
                    set.secret_access_key_file.as_deref(),
                    set.secret_access_key_command.as_deref(),
                )? {
                    options.insert("secret_access_key".to_owned(), value);
                }
                CredsBinding::Set {
                    name: name.clone(),
                    via,
                }
            }
        };
        for (key, value) in &self.query_options {
            options.insert((*key).to_owned(), value.clone());
        }
        // The endpoint is assembled last: under virtual-hosted addressing
        // object_store expects the bucket inside the endpoint host, so the
        // URL depends on the final virtual_hosted_style_request value. An
        // explicit endpoint in `extra` wins (the escape hatch).
        if let Some(endpoint) = &self.endpoint
            && !options.keys().any(|key| {
                key.eq_ignore_ascii_case("endpoint") || key.eq_ignore_ascii_case("aws_endpoint")
            })
        {
            let virtual_hosted = options
                .get("virtual_hosted_style_request")
                .is_some_and(|value| value == "true");
            let url = if virtual_hosted {
                format!(
                    "{}://{}.{}",
                    endpoint.scheme, endpoint.bucket, endpoint.authority
                )
            } else {
                format!("{}://{}", endpoint.scheme, endpoint.authority)
            };
            options.insert("endpoint".to_owned(), url);
        }
        Ok(ResolvedStorage {
            storage: self.clone(),
            options,
            binding,
        })
    }
}

/// (canonical URL, recognized query options, `?creds=` pointer).
type StrippedQuery = (Url, Vec<(&'static str, String)>, Option<String>);

/// Pull recognized query params off the URL; reject unrecognized ones.
fn strip_query(url: Url) -> Result<StrippedQuery> {
    let mut query_options = Vec::new();
    let mut creds_pointer = None;
    for (key, value) in url.query_pairs() {
        match RECOGNIZED_QUERY_PARAMS
            .iter()
            .find(|known| **known == key.as_ref())
        {
            Some(&"creds") => creds_pointer = Some(value.into_owned()),
            Some(known) => query_options.push((*known, value.into_owned())),
            None => bail!(
                "storage URL query param {key:?} not recognized (known: {})",
                RECOGNIZED_QUERY_PARAMS.join(", "),
            ),
        }
    }
    let mut canonical = url;
    canonical.set_query(None);
    Ok((canonical, query_options, creds_pointer))
}

/// Parse a `[creds.*] scope` URL prefix into the same canonical form
/// `StorageUrl::parse` produces, so comparison is exact.
pub(crate) fn parse_scope(scope: &str) -> Result<Url> {
    let mut url = Url::parse(scope.trim())?;
    if !url.username().is_empty() || url.password().is_some() {
        bail!("scope embeds credentials");
    }
    if url.query().is_some() {
        bail!("scope carries query params; scopes are plain URL prefixes");
    }
    match (url.scheme(), url.port()) {
        ("s3+https", Some(443)) | ("s3+http", Some(80)) => {
            let _ = url.set_port(None);
        }
        _ => {}
    }
    Ok(url)
}

/// spec.md#creds-scope-match: scheme, host, and port equal; path matches at
/// `/` segment boundaries only (`.../pond` does not match `.../pond-2`). No
/// cross-scheme normalization: a `s3+https://host/bucket/` scope does not
/// match a `s3://bucket/` URL.
fn scope_matches(scope: &Url, address: &Url) -> bool {
    if scope.scheme() != address.scheme()
        || scope.host_str() != address.host_str()
        || scope.port() != address.port()
    {
        return false;
    }
    let scope_path = scope.path().trim_end_matches('/');
    let address_path = address.path().trim_end_matches('/');
    address_path == scope_path
        || address_path
            .strip_prefix(scope_path)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// How a creds set got bound to a URL - surfaced in binding lines so a wrong
/// match is visible before any auth error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindVia {
    /// `?creds=<name>` pointer on the URL.
    Pointer,
    /// Longest-prefix `scope` match.
    Scope,
    /// The scope-less catch-all set.
    CatchAll,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CredsBinding {
    /// A `[creds.<name>]` set bound to this URL.
    Set { name: String, via: BindVia },
    /// No set matched; object_store's ambient SDK chain applies (AWS_* env,
    /// shared credentials file, IMDS/container metadata). A documented
    /// invariant, not an accident - instance profiles and OIDC work with
    /// zero pond config.
    Ambient,
    /// Local / in-memory scheme; credentials don't apply.
    NotApplicable,
}

impl CredsBinding {
    /// One-line human rendering for binding lines and `pond config show`.
    pub fn describe(&self) -> String {
        match self {
            Self::Set { name, via } => {
                let via = match via {
                    BindVia::Pointer => "?creds",
                    BindVia::Scope => "scope match",
                    BindVia::CatchAll => "catch-all",
                };
                format!("creds {name} ({via})")
            }
            Self::Ambient => "ambient chain".to_owned(),
            Self::NotApplicable => "local (no credentials)".to_owned(),
        }
    }
}

/// A storage address with its options assembled and secrets materialized -
/// everything `Store::open_with_options` needs, plus the binding for
/// display.
#[derive(Debug, Clone)]
pub struct ResolvedStorage {
    storage: StorageUrl,
    pub options: HashMap<String, String>,
    pub binding: CredsBinding,
}

impl ResolvedStorage {
    pub fn lance_url(&self) -> &Url {
        self.storage.lance_url()
    }

    pub fn display(&self) -> String {
        self.storage.display()
    }
}

/// Names of defined creds sets that bound to none of this invocation's URLs
/// (spec.md#creds-scope-match: misbinding must never be silent). Empty when
/// the invocation touched no credential-taking URL - a local-only command
/// must not nag about sets kept for remote work.
pub fn unmatched_creds_sets<'c>(
    resolved: &[&ResolvedStorage],
    creds: &'c BTreeMap<String, CredsSet>,
) -> Vec<&'c str> {
    if resolved
        .iter()
        .all(|entry| matches!(entry.binding, CredsBinding::NotApplicable))
    {
        return Vec::new();
    }
    creds
        .keys()
        .filter(|name| {
            !resolved.iter().any(|entry| {
                matches!(&entry.binding, CredsBinding::Set { name: bound, .. } if bound == *name)
            })
        })
        .map(String::as_str)
        .collect()
}

/// Materialize one logical secret from its inline / `_file` / `_command`
/// variant (validation guarantees at most one is set).
fn materialize_secret(
    set: &str,
    field: &str,
    inline: Option<&str>,
    file: Option<&std::path::Path>,
    command: Option<&str>,
) -> Result<Option<String>> {
    if let Some(value) = inline {
        return Ok(Some(value.to_owned()));
    }
    if let Some(path) = file {
        let text = std::fs::read_to_string(path).with_context(|| {
            format!(
                "[creds.{set}] {field}_file: failed to read {}",
                path.display()
            )
        })?;
        return Ok(Some(strip_one_newline(text)));
    }
    if let Some(command) = command {
        return Ok(Some(run_secret_command(set, field, command)?));
    }
    Ok(None)
}

/// Run a `*_command` secret source. Output is cached per command text per
/// process, so N URLs resolving through one set cost one subprocess.
fn run_secret_command(set: &str, field: &str, command: &str) -> Result<String> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<HashMap<String, String>>> =
        std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(hit) = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(command)
    {
        return Ok(hit.clone());
    }
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .output()
        .with_context(|| format!("[creds.{set}] {field}_command failed to spawn: {command}"))?;
    if !output.status.success() {
        bail!(
            "[creds.{set}] {field}_command exited {}: {command}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim_end(),
        );
    }
    let value = strip_one_newline(
        String::from_utf8(output.stdout)
            .with_context(|| format!("[creds.{set}] {field}_command output is not UTF-8"))?,
    );
    cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(command.to_owned(), value.clone());
    Ok(value)
}

/// Strip exactly one trailing newline (the one `echo` / `op read` append);
/// anything beyond that is part of the secret.
fn strip_one_newline(mut text: String) -> String {
    if text.ends_with('\n') {
        text.pop();
        if text.ends_with('\r') {
            text.pop();
        }
    }
    text
}

/// `pond storage check` failure classes, each with its own exit code at the
/// CLI so cron and CI can branch on them. Display carries only the
/// fix-naming lead; the underlying error is exposed separately through
/// [`CheckFailure::concise_cause`] so surfaces stay one readable line
/// instead of trailing the upstream chain (Lance flattens its inner errors
/// into each level's Display, so the raw chain prints the same failure
/// several times over).
#[derive(Debug, thiserror::Error)]
pub enum CheckFailure {
    #[error(
        "authentication failed and no creds set matched this URL; add one with `pond creds add` (or set POND_CREDS_*), or provide ambient AWS_* credentials"
    )]
    NoCreds { source: anyhow::Error },
    #[error("authentication failed using creds set {set:?}; check its keys and scope")]
    Auth { set: String, source: anyhow::Error },
    #[error(
        "backend does not enforce conditional writes (If-None-Match); concurrent pond writers would corrupt each other - {detail}"
    )]
    OccUnsupported { detail: String },
    #[error("storage probe failed")]
    Io { source: anyhow::Error },
}

impl CheckFailure {
    /// The root cause, condensed to one operator-readable line: the deepest
    /// error in the chain with upstream noise stripped - Lance's bug-report
    /// boilerplate, internal `<WORKSPACE>` source locations, and the repeated
    /// wrapper text that follows them. `None` for `OccUnsupported`, whose
    /// `detail` is already curated into its Display.
    pub fn concise_cause(&self) -> Option<String> {
        let source = match self {
            Self::NoCreds { source } | Self::Auth { source, .. } | Self::Io { source } => source,
            Self::OccUnsupported { .. } => return None,
        };
        Some(condense_error_chain(source))
    }
}

/// One-line root cause for a probe error. Takes the deepest chain entry
/// (each outer Lance/object_store layer re-prints its inner error, so the
/// deepest is the least redundant), cuts at the first internal source
/// location (everything after it is upstream re-printing), strips Lance's
/// bug-report boilerplate, and middle-truncates - the tail is kept because
/// wrapped transport errors put the root (DNS, connect) at the end.
fn condense_error_chain(error: &anyhow::Error) -> String {
    let mut text = error
        .chain()
        .last()
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("{error:#}"));
    if let Some(pos) = text.find(", <WORKSPACE>") {
        text.truncate(pos);
    }
    text = text.replace(
        "Encountered internal error. Please file a bug report at https://github.com/lance-format/lance/issues. ",
        "",
    );
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    const HEAD: usize = 120;
    const TAIL: usize = 120;
    let chars: Vec<char> = line.chars().collect();
    if chars.len() > HEAD + TAIL + 5 {
        let head: String = chars[..HEAD].iter().collect();
        let tail: String = chars[chars.len() - TAIL..].iter().collect();
        format!("{head} ... {tail}")
    } else {
        line
    }
}

/// Probe a resolved storage destination end-to-end (spec.md#substrate): a
/// conditional `PutMode::Create` pair proving the `If-None-Match` -> 412 OCC
/// primitive Lance's commit handler relies on, then read-back and delete of
/// the synthetic key.
pub async fn storage_check(resolved: &ResolvedStorage) -> std::result::Result<(), CheckFailure> {
    use object_store::{Error as OsError, ObjectStoreExt, PutMode, PutOptions, PutPayload};

    let classify =
        |error: OsError, step: &str| classify_check_error(error, &resolved.binding, step);

    let probe_uri = format!(
        "{}/_config-check/{}",
        resolved.lance_url().as_str().trim_end_matches('/'),
        uuid::Uuid::now_v7(),
    );
    let params = ObjectStoreParams {
        storage_options_accessor: (!resolved.options.is_empty()).then(|| {
            Arc::new(StorageOptionsAccessor::with_static_options(
                resolved.options.clone(),
            ))
        }),
        ..Default::default()
    };
    let registry = Arc::new(ObjectStoreRegistry::default());
    let (store, path) = ObjectStore::from_uri_and_params(registry, &probe_uri, &params)
        .await
        .map_err(|error| CheckFailure::Io {
            source: anyhow!(error).context(format!("failed to open object store for {probe_uri}")),
        })?;

    let body: &[u8] = b"pond storage check";
    let create = PutOptions::from(PutMode::Create);
    store
        .inner
        .put_opts(&path, PutPayload::from_static(body), create.clone())
        .await
        .map_err(|error| classify(error, "initial conditional put"))?;
    // The probe key exists from here on: run the remaining steps, then
    // best-effort delete it whatever they returned - a failed probe must
    // not leave litter behind.
    let outcome = async {
        // The second create MUST lose: this is the `If-None-Match: *` -> 412
        // primitive multi-writer OCC stands on. A backend that lets it
        // through (or rejects the header) silently overwrites concurrent
        // commits.
        match store
            .inner
            .put_opts(&path, PutPayload::from_static(body), create)
            .await
        {
            Err(OsError::AlreadyExists { .. }) => {}
            Ok(_) => {
                return Err(CheckFailure::OccUnsupported {
                    detail: "a second create over an existing key succeeded".to_owned(),
                });
            }
            Err(OsError::NotImplemented { .. }) => {
                return Err(CheckFailure::OccUnsupported {
                    detail: "the backend rejects conditional puts as unimplemented".to_owned(),
                });
            }
            Err(error) => return Err(classify(error, "conditional-put probe")),
        }
        let read_back = store
            .inner
            .get(&path)
            .await
            .map_err(|error| classify(error, "read-back"))?
            .bytes()
            .await
            .map_err(|error| classify(error, "read-back body"))?;
        if read_back.as_ref() != body {
            return Err(CheckFailure::Io {
                source: anyhow!("read-back returned different bytes than written"),
            });
        }
        Ok(())
    }
    .await;
    let cleanup = store.inner.delete(&path).await;
    outcome?;
    cleanup.map_err(|error| classify(error, "cleanup delete"))?;
    Ok(())
}

/// Map an `object_store` error onto the check's failure classes: an auth
/// error is attributed to the bound creds set when one matched, and to the
/// (empty) ambient chain when none did; everything else is I/O.
fn classify_check_error(
    error: object_store::Error,
    binding: &CredsBinding,
    step: &str,
) -> CheckFailure {
    use object_store::Error as OsError;
    // Lance erases a missing-credentials failure into a `Generic` error - the
    // typed `Unauthenticated` never surfaces for an empty provider chain - so
    // also match the AWS SDK's rendered `CredentialsNotLoaded` signal. Both
    // are auth-class: attributed to the bound set, else the empty ambient chain.
    let auth_class = matches!(
        error,
        OsError::Unauthenticated { .. } | OsError::PermissionDenied { .. }
    ) || {
        let rendered = error.to_string();
        rendered.contains("CredentialsNotLoaded")
            || rendered.contains("no providers in chain provided credentials")
    };
    match (auth_class, binding) {
        (true, CredsBinding::Set { name, .. }) => CheckFailure::Auth {
            set: name.clone(),
            source: anyhow!(error).context(step.to_owned()),
        },
        (true, _) => CheckFailure::NoCreds {
            source: anyhow!(error).context(step.to_owned()),
        },
        (false, _) => CheckFailure::Io {
            source: anyhow!(error).context(step.to_owned()),
        },
    }
}

/// Per-task fragment-count backstop: tasks this wide always run, bounding
/// manifest growth even when the amplification veto would skip them. As
/// policy cap, 0 disables the veto (tests).
pub const DEFAULT_COMPACTION_FRAGMENT_CAP: usize = 64;

/// Fragments are sized by bytes, not Lance's 1M-row default: kilobyte-average
/// rows make a row target tolerate multi-GiB fragments that compaction
/// re-rewrites wholesale to absorb tiny appends (~190 GiB/day of churn).
pub const TARGET_FRAGMENT_BYTES: u64 = 256 * 1024 * 1024;

const MIN_TARGET_ROWS_PER_FRAGMENT: u64 = 50_000;
/// Ceiling = Lance's own default.
const MAX_TARGET_ROWS_PER_FRAGMENT: u64 = 1024 * 1024;

/// Keep a task only when the merged-in remainder is >= largest/this:
/// size-tiered amortization, O(log n) lifetime rewrites per row.
pub const COMPACTION_ABSORB_FACTOR: u64 = 4;

/// Default manifest-retention window for the safe cleanup pass. Matches
/// LanceDB's recommended OSS-operator practice (lancedb docs: performance.mdx,
/// tables/update.mdx). With `delete_unverified=false`, Lance's 7-day
/// in-progress guard still protects unverified files regardless of this value
/// (`UNVERIFIED_THRESHOLD_DAYS` in lance/dataset/cleanup.rs).
pub fn default_cleanup_older_than() -> chrono::Duration {
    chrono::Duration::days(1)
}

/// Resolved per-call inputs to the storage-maintenance pass. Built from
/// `[maintenance]` (and any per-invocation CLI override) at the entry point;
/// threaded down to `optimize_table_compact` so the substrate never re-reads
/// `Config` itself.
#[derive(Debug, Clone, Copy)]
pub struct MaintenancePolicy {
    /// See [`DEFAULT_COMPACTION_FRAGMENT_CAP`]; `0` disables the veto.
    pub compaction_fragment_cap: usize,
    /// Manifest-retention window handed to `cleanup_old_versions`.
    pub cleanup_older_than: chrono::Duration,
}

impl MaintenancePolicy {
    /// Veto off: run every task Lance plans (the optimize tests assume this).
    pub fn always_compact() -> Self {
        Self {
            compaction_fragment_cap: 0,
            cleanup_older_than: default_cleanup_older_than(),
        }
    }
}

struct FragmentStat {
    /// `None` when the manifest lacks any file's size.
    bytes: Option<u64>,
    rows: u64,
    deleted_rows: u64,
}

/// Data-file bytes of one fragment; `None` (poisoning) when any size is
/// missing from the manifest.
fn fragment_bytes(fragment: &lance::table::format::Fragment) -> Option<u64> {
    fragment.files.iter().try_fold(0u64, |total, file| {
        Some(total + file.file_size_bytes.get()?.get())
    })
}

fn fragment_stat(fragment: &lance::table::format::Fragment) -> FragmentStat {
    FragmentStat {
        bytes: fragment_bytes(fragment),
        rows: fragment.physical_rows.unwrap_or(0) as u64,
        deleted_rows: fragment
            .deletion_file
            .as_ref()
            .and_then(|deletions| deletions.num_deleted_rows)
            .unwrap_or(0) as u64,
    }
}

/// Rows per [`TARGET_FRAGMENT_BYTES`] at the table's average row size.
fn derived_target_rows(stats: &[FragmentStat]) -> usize {
    let (mut bytes, mut rows) = (0u64, 0u64);
    for stat in stats {
        if let Some(fragment_bytes) = stat.bytes
            && stat.rows > 0
        {
            bytes += fragment_bytes;
            rows += stat.rows;
        }
    }
    if bytes == 0 || rows == 0 {
        return MAX_TARGET_ROWS_PER_FRAGMENT as usize;
    }
    let avg_row_bytes = (bytes / rows).max(1);
    (TARGET_FRAGMENT_BYTES / avg_row_bytes)
        .clamp(MIN_TARGET_ROWS_PER_FRAGMENT, MAX_TARGET_ROWS_PER_FRAGMENT) as usize
}

/// Amplification veto: skip tasks that mostly rewrite one big fragment to
/// absorb fresh appends. Deletion-materialization tasks always pass (vetoing
/// them would leave tombstones unreclaimed forever); compared in bytes when
/// every file size is known, rows otherwise.
fn keep_task(stats: &[FragmentStat], cap: usize, deletion_threshold: f32) -> bool {
    if stats.iter().any(|stat| {
        stat.rows > 0 && (stat.deleted_rows as f32 / stat.rows as f32) > deletion_threshold
    }) {
        return true;
    }
    if stats.len() >= cap {
        return true;
    }
    let weights: Vec<u64> = if stats.iter().all(|stat| stat.bytes.is_some()) {
        stats.iter().filter_map(|stat| stat.bytes).collect()
    } else {
        stats.iter().map(|stat| stat.rows).collect()
    };
    let total: u64 = weights.iter().sum();
    let largest = weights.iter().copied().max().unwrap_or(0);
    (total - largest) * COMPACTION_ABSORB_FACTOR >= largest
}

/// Declarative description of one index pond keeps on a table. Created when
/// its trigger fires; folded forward by `pond optimize`.
#[derive(Debug, Clone)]
pub struct IndexIntent {
    /// Stable on-disk name. Must match across runs so existence checks
    /// resolve.
    pub name: &'static str,
    /// Column the index covers.
    pub column: &'static str,
    /// Condition evaluated against the live dataset before each cycle.
    pub trigger: IndexTrigger,
    /// How the params are built at create time. Some intents have static
    /// params (FTS, scalars); IVF_PQ needs the row count to size partitions.
    pub params: IndexParamsKind,
}

/// When an [`IndexIntent`] should exist on disk.
#[derive(Debug, Clone)]
pub enum IndexTrigger {
    /// Build whenever the table has any rows. Used for FTS and scalar
    /// indices: there is no training cost worth delaying.
    OnAnyRows,
    /// Build when `count(<column> IS NOT NULL) >= threshold`. Used for the
    /// IVF_PQ vector index, which trains poorly on too few vectors.
    OnNonNullCount {
        column: &'static str,
        threshold: usize,
    },
}

/// The lance-native shape of an [`IndexIntent`]'s params, dispatched to the
/// right `IndexParams` at create time.
#[derive(Debug, Clone)]
pub enum IndexParamsKind {
    /// `BuiltinIndexType::BTree` -> [`IndexType::BTree`];
    /// `BuiltinIndexType::Bitmap` -> [`IndexType::Bitmap`]; etc.
    Scalar(BuiltinIndexType),
    /// `InvertedIndexParams` with a character `ngram` tokenizer in the
    /// `[min, max]` range and stemming / stop-words off
    /// (spec.md#search-language-neutral-index).
    InvertedFtsNgram { min: u32, max: u32 },
    /// `VectorIndexParams::ivf_pq` with cosine metric (e5 vectors are
    /// L2-normalized). `sub_vectors = embedding_dim / 8` and `num_bits = 8`
    /// are pond's conventions; `max_iters` caps kmeans. Partitions follow
    /// LanceDB's documented `num_rows // 4096` guidance, floored at one.
    IvfPqCosine {
        sub_vectors: usize,
        num_bits: u8,
        max_iters: usize,
    },
}

impl IndexTrigger {
    async fn should_create(&self, dataset: &Dataset) -> Result<bool> {
        match self {
            Self::OnAnyRows => Ok(dataset.count_rows(None).await? > 0),
            Self::OnNonNullCount { column, threshold } => {
                let count = dataset
                    .count_rows(Some(format!("{column} IS NOT NULL")))
                    .await?;
                Ok(count >= *threshold)
            }
        }
    }
}

impl IndexParamsKind {
    fn index_type(&self) -> IndexType {
        match self {
            Self::Scalar(BuiltinIndexType::Bitmap) => IndexType::Bitmap,
            Self::Scalar(_) => IndexType::BTree,
            Self::InvertedFtsNgram { .. } => IndexType::Inverted,
            Self::IvfPqCosine { .. } => IndexType::Vector,
        }
    }

    async fn build(&self, dataset: &Dataset) -> Result<Box<dyn lance::index::IndexParams>> {
        match self {
            Self::Scalar(kind) => Ok(Box::new(ScalarIndexParams::for_builtin(kind.clone()))),
            Self::InvertedFtsNgram { min, max } => Ok(Box::new(
                InvertedIndexParams::default()
                    .base_tokenizer("ngram".to_owned())
                    .ngram_min_length(*min)
                    .ngram_max_length(*max)
                    .stem(false)
                    .remove_stop_words(false),
            )),
            Self::IvfPqCosine {
                sub_vectors,
                num_bits,
                max_iters,
            } => {
                let count = dataset
                    .count_rows(Some("vector IS NOT NULL".to_owned()))
                    .await?;
                let partitions = count.checked_div(4096).unwrap_or(0).max(1);
                Ok(Box::new(VectorIndexParams::ivf_pq(
                    partitions,
                    *num_bits,
                    *sub_vectors,
                    MetricType::Cosine,
                    *max_iters,
                )))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStatus {
    pub table: Table,
    pub intent_name: String,
    pub fragments_covered: usize,
    pub unindexed_fragments: usize,
    pub unindexed_rows: usize,
    pub exists: bool,
}

/// Anyhow-chain sentinel pond attaches when `retry_lance` exhausts attempts
/// against an OCC commit-conflict failure (spec.md#protocol). The wire layer
/// downcasts to this type to classify the outcome as `conflict` rather than
/// the generic `storage_unavailable`.
#[derive(Debug, Clone, Copy)]
pub struct ConflictExhausted {
    pub attempts: u8,
}

impl std::fmt::Display for ConflictExhausted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "commit conflict exhausted after {} attempt(s)",
            self.attempts
        )
    }
}

impl std::error::Error for ConflictExhausted {}

/// Per-phase result for one table's pass through `Handle::optimize_table`.
/// spec.md#substrate 3.7 (`lance-index-maintenance`): the indices phase and the
/// compaction phase get independent retry budgets and independent commits,
/// so a hot writer that starves the Rewrite cannot abort the index Update.
#[derive(Debug)]
pub enum PhaseOutcome {
    /// Phase attempted and committed work.
    Ok,
    /// Phase attempted; no work was needed.
    Noop,
    /// Phase attempted; OCC retry budget exhausted on conflict (the operator
    /// can rerun later once the hot writer quiesces).
    SkippedConflict,
    /// Phase failed with a non-conflict error.
    Failed(anyhow::Error),
    /// Phase not requested by the caller (e.g. compaction skipped under
    /// `Store::build_indices_only`).
    NotAttempted,
}

impl PhaseOutcome {
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }
}

/// What `Handle::optimize_table` did for one table.
#[derive(Debug)]
pub struct TableOptimizeOutcome {
    pub table: Table,
    pub indices: PhaseOutcome,
    pub compaction: PhaseOutcome,
}

/// Boundary event during one `Handle::optimize_table` pass. The CLI binds a
/// progress callback to render a live spinner; library callers pass `None`.
#[derive(Debug, Clone)]
pub enum OptimizeEvent {
    PhaseStart {
        table: Table,
        phase: OptimizePhase,
        detail: Option<String>,
    },
    PhaseDone {
        table: Table,
        phase: OptimizePhase,
        elapsed_ms: u64,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum OptimizePhase {
    Compact,
    Cleanup,
    IndexCreate,
    IndexRebuild,
    IndexAppend,
}

impl OptimizePhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Compact => "compact",
            Self::Cleanup => "cleanup",
            Self::IndexCreate => "index-create",
            Self::IndexRebuild => "index-rebuild",
            Self::IndexAppend => "index-append",
        }
    }
}

pub type OptimizeProgressFn = Box<dyn Fn(OptimizeEvent) + Send + Sync>;

fn emit(progress: Option<&OptimizeProgressFn>, event: OptimizeEvent) {
    if let Some(callback) = progress {
        callback(event);
    }
}

/// True when the chain root is one of Lance's commit-conflict variants
/// (`CommitConflict`, `RetryableCommitConflict`, `TooMuchWriteContention`).
/// Everything else (timeouts, IAM denials, disk errors) is not a conflict.
pub fn is_commit_conflict(error: &anyhow::Error) -> bool {
    error.downcast_ref::<lance::Error>().is_some_and(|err| {
        matches!(
            err,
            lance::Error::CommitConflict { .. }
                | lance::Error::RetryableCommitConflict { .. }
                | lance::Error::TooMuchWriteContention { .. }
        )
    })
}

/// True when `retry_lance` exhausted retries against an OCC conflict and
/// attached `ConflictExhausted` to the chain head.
fn is_conflict_exhausted(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<ConflictExhausted>())
}

/// On-disk byte totals for the three session datasets, plus everything else
/// under the data-dir root. Sized by listing through Lance's object-store
/// layer (spec.md#lance-chokepoints-storage) so `file://` and `s3://` behave alike.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableSizes {
    pub sessions: u64,
    pub messages: u64,
    pub parts: u64,
    pub other: u64,
    pub sessions_data: DataLiveness,
    pub messages_data: DataLiveness,
    pub parts_data: DataLiveness,
}

/// `data/` bytes on disk vs bytes the latest manifest references; the gap is
/// superseded versions awaiting the cleanup retention window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DataLiveness {
    pub on_disk: u64,
    /// `None` when the manifest lacks any referenced file's size.
    pub live: Option<u64>,
}

impl DataLiveness {
    pub fn dead(&self) -> Option<u64> {
        self.live.map(|live| self.on_disk.saturating_sub(live))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValue {
    String(String),
    Int32(i32),
    Raw(String),
}
impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}
impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}
impl From<i32> for ScalarValue {
    fn from(value: i32) -> Self {
        Self::Int32(value)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    Eq(&'static str, ScalarValue),
    Ne(&'static str, ScalarValue),
    IsNull(&'static str),
    IsNotNull(&'static str),
    In(&'static str, Vec<ScalarValue>),
    LikeContains(&'static str, String),
    /// Regex match. Emitted as `regexp_like(<col>, '<pat>')`. Never pushes
    /// down to BTREE indexes (Lance's scalar-index-expr parser ignores it),
    /// so the filter is a full-scan-with-predicate - acceptable for
    /// human-driven `--project re:...` queries, not for hot paths.
    Regex(&'static str, String),
    Gte(&'static str, ScalarValue),
    Lte(&'static str, ScalarValue),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}
impl Predicate {
    pub fn to_lance(&self) -> String {
        match self {
            Self::Eq(column, value) => format!("{column} = {}", value.to_lance()),
            Self::Ne(column, value) => format!("{column} <> {}", value.to_lance()),
            Self::IsNull(column) => format!("{column} IS NULL"),
            Self::IsNotNull(column) => format!("{column} IS NOT NULL"),
            Self::In(column, values) => {
                let values = values
                    .iter()
                    .map(ScalarValue::to_lance)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{column} IN ({values})")
            }
            Self::LikeContains(column, value) => {
                format!("{column} LIKE {} ESCAPE '\\'", like_contains(value))
            }
            Self::Regex(column, pattern) => {
                format!("regexp_like({column}, {})", quoted_string(pattern))
            }
            Self::Gte(column, value) => format!("{column} >= {}", value.to_lance()),
            Self::Lte(column, value) => format!("{column} <= {}", value.to_lance()),
            Self::And(predicates) => predicates
                .iter()
                .map(Self::to_lance)
                .filter(|predicate| !predicate.is_empty())
                .collect::<Vec<_>>()
                .join(" AND "),
            Self::Or(predicates) => {
                // Wrap in parens so the disjunction composes safely as a child
                // of an outer `And` (SQL `OR` binds looser than `AND`).
                let body = predicates
                    .iter()
                    .map(Self::to_lance)
                    .filter(|predicate| !predicate.is_empty())
                    .collect::<Vec<_>>()
                    .join(" OR ");
                if body.is_empty() {
                    String::new()
                } else {
                    format!("({body})")
                }
            }
            Self::Not(inner) => {
                let body = inner.to_lance();
                if body.is_empty() {
                    String::new()
                } else {
                    format!("NOT ({body})")
                }
            }
        }
    }
}
/// Read-side options for `Handle::scan`: optional prefilter predicate and
/// optional projection. Default = no filter, all columns.
#[derive(Default)]
pub struct ScanOpts<'a> {
    pub predicate: Option<&'a Predicate>,
    pub projection: Option<&'a [&'a str]>,
}

impl<'a> ScanOpts<'a> {
    pub fn project_only(projection: &'a [&'a str]) -> Self {
        Self {
            predicate: None,
            projection: Some(projection),
        }
    }
    pub fn with_predicate_and_projection(
        predicate: &'a Predicate,
        projection: &'a [&'a str],
    ) -> Self {
        Self {
            predicate: Some(predicate),
            projection: Some(projection),
        }
    }
}

impl ScalarValue {
    fn to_lance(&self) -> String {
        match self {
            Self::String(value) => quoted_string(value),
            Self::Int32(value) => value.to_string(),
            Self::Raw(value) => value.clone(),
        }
    }
}
/// Lance cache caps in bytes. `None` lets the substrate pick the backend-aware
/// default (local FS gets a tighter cap; object stores stay near Lance's
/// defaults). Wired through `Store::open_with_options` from `[runtime]`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeCaps {
    pub index_cache_bytes: Option<usize>,
    pub metadata_cache_bytes: Option<usize>,
}

impl RuntimeCaps {
    pub fn from_config(config: &crate::config::RuntimeConfig) -> Self {
        Self {
            index_cache_bytes: config.index_cache_bytes,
            metadata_cache_bytes: config.metadata_cache_bytes,
        }
    }
}

/// Local-FS default: tight enough that a long-lived `pond mcp` lands well
/// under the 500 MiB target without measurable latency cost vs Lance's 6 GiB
/// default (see `benches/serve_mem_bench.rs --cap-sweep`).
const LOCAL_INDEX_CACHE_BYTES: usize = 256 * 1024 * 1024;
const LOCAL_METADATA_CACHE_BYTES: usize = 128 * 1024 * 1024;
/// Object-store defaults: latency to refill is per-page, so keep more in cache.
const REMOTE_INDEX_CACHE_BYTES: usize = 2 * 1024 * 1024 * 1024;
const REMOTE_METADATA_CACHE_BYTES: usize = 512 * 1024 * 1024;

fn resolve_cache_caps(location: &Url, caps: RuntimeCaps) -> (usize, usize) {
    let (index_default, metadata_default) = if config::is_local(location) {
        (LOCAL_INDEX_CACHE_BYTES, LOCAL_METADATA_CACHE_BYTES)
    } else {
        (REMOTE_INDEX_CACHE_BYTES, REMOTE_METADATA_CACHE_BYTES)
    };
    (
        caps.index_cache_bytes.unwrap_or(index_default),
        caps.metadata_cache_bytes.unwrap_or(metadata_default),
    )
}

pub struct Handle {
    datasets: DatasetSet,
    retry: RetryPolicy,
    /// One `lance::Session` shared across all three datasets. Carries the
    /// metadata + index caches and the `ObjectStoreRegistry` (which holds
    /// the underlying object_store / S3 client). Sharing the session means
    /// one cache pool covers all three tables and one S3 client serves all
    /// three datasets - load-bearing on object-store backends where a
    /// per-dataset client would mean 3x the connection pools and 3x the
    /// credential refreshes (lance/src/dataset/builder.rs:509-517).
    #[allow(dead_code)]
    session: Arc<Session>,
    /// The `lance-namespace` catalog seam. v1 uses the Directory impl;
    /// future hosted pond swaps to "rest" without touching read/write paths
    /// (spec.md#lance-chokepoints-catalog).
    nm: Arc<dyn LanceNamespace>,
    /// Namespace identifier this handle binds to. v1 is always `root()`; the
    /// typed seam matches `resolve_namespace`'s return so multi-namespace
    /// routing can land without churning call sites (spec.md#wire-namespace-resolution).
    nm_ident: NamespaceIdent,
    /// Object-store options threaded through every `DatasetBuilder` and
    /// `Dataset::write` call so refresh / index-creation paths inherit the
    /// same credentials and region as the initial open. Empty on local-FS
    /// installs.
    storage_options: HashMap<String, String>,
    /// Data-dir URL the handle was opened against. `pond status` reads this
    /// to display where the bytes live and to decide whether to walk a local
    /// directory or issue a remote `LIST` for sizing.
    location: Url,
    /// Cached `parts.lance` open metadata, used the first time a caller asks
    /// for parts. Holds the namespace probe shape so the lazy open re-uses the
    /// same `lance-chokepoints-catalog` path as the eager opens for sessions/messages.
    parts_refresh_after: Duration,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Handle")
            .field("datasets", &self.datasets)
            .field("retry", &self.retry)
            .field("nm_ident", &self.nm_ident)
            .field("storage_options", &self.storage_options)
            .field("location", &self.location)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    Sessions,
    Messages,
    Parts,
}
impl Table {
    pub fn as_str(self) -> &'static str {
        self.label()
    }

    fn label(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::Messages => "messages",
            Self::Parts => "parts",
        }
    }
}
#[derive(Debug)]
struct DatasetSet {
    sessions: Mutex<CachedDataset>,
    messages: Mutex<CachedDataset>,
    /// `parts.lance` opens lazily on the first read or write that needs it:
    /// any `pond_get` (every mode reads parts to build summaries), grouped
    /// search hydrating user-hit summaries, or ingest with Part events. A
    /// process that does none of those skips the file, saving its metadata
    /// pages and file handle at cold-open. The OnceCell makes init
    /// single-flight; the inner `Mutex<CachedDataset>` then behaves identically
    /// to the other two.
    parts: OnceCell<Mutex<CachedDataset>>,
}
#[derive(Debug)]
struct CachedDataset {
    dataset: Dataset,
    last_refresh: Instant,
    refresh_after: Duration,
}
impl CachedDataset {
    async fn latest(&mut self) -> Result<Dataset> {
        if self.last_refresh.elapsed() >= self.refresh_after {
            self.dataset.checkout_latest().await?;
            self.last_refresh = Instant::now();
        }
        Ok(self.dataset.clone())
    }
    fn replace(&mut self, dataset: Dataset) {
        self.dataset = dataset;
        self.last_refresh = Instant::now();
    }
}

/// Outcome of one [`Handle::append_stream`] write. Lance's `execute_stream`
/// returns only the new `Dataset` (no write summary), so these totals are
/// captured from the cumulative `WriteStats` ticks - the same source the live
/// progress reads - plus pond's own OCC attempt counter.
#[derive(Debug, Clone, Copy, Default)]
pub struct AppendStats {
    pub rows: u64,
    pub bytes_written: u64,
    pub files_written: u64,
    pub attempts: u32,
}

impl Handle {
    /// Open without storage options or explicit cache caps. Backend-aware
    /// defaults from `[runtime]` apply.
    pub async fn open(location: &Url) -> Result<Self> {
        Self::open_with_options(location, HashMap::new(), RuntimeCaps::default()).await
    }

    /// Open with object-store options handed through to Lance verbatim, plus
    /// the resolved `[runtime]` cache caps. Object-store keys are the
    /// `object_store` crate's standard config names; pond does not parse them.
    /// Opening datasets never performs index work; index lifecycle lives under
    /// `Handle::optimize_table`. `parts.lance` opens lazily on first use.
    pub async fn open_with_options(
        location: &Url,
        mut storage_options: HashMap<String, String>,
        caps: RuntimeCaps,
    ) -> Result<Self> {
        if let Some(path) = config::local_path(location) {
            tokio::fs::create_dir_all(&path).await.with_context(|| {
                format!(
                    "failed to create data dir {}; fix the storage destination ([storage].path in config) or re-run `pond init`",
                    path.display()
                )
            })?;
        } else {
            apply_remote_storage_defaults(&mut storage_options);
        }
        // One Session shared across all three datasets so metadata/index
        // caches and the object_store registry (and thus any S3 client) are
        // pooled rather than duplicated three times. Caps are sized by the
        // `[runtime]` block; explicit values from `caps` win, otherwise the
        // local/remote backend default kicks in.
        let (index_cache_bytes, metadata_cache_bytes) = resolve_cache_caps(location, caps);
        let session = Arc::new(Session::new(
            index_cache_bytes,
            metadata_cache_bytes,
            Arc::new(ObjectStoreRegistry::default()),
        ));
        // Build the lance-namespace catalog seam once (spec.md#lance-chokepoints-catalog).
        // The `root` property is whatever URL the Directory impl understands;
        // `uri_to_url` (lance-io/object_store.rs) accepts both bare paths and
        // URLs, so passing the scheme-qualified URL for local FS works the
        // same as the bare-path form. Trailing slash stripped for clean logs.
        let root = location.as_str().trim_end_matches('/').to_string();
        let mut connect = ConnectBuilder::new("dir")
            .property("root", root)
            .session(session.clone());
        // Object-store credentials/region/endpoint flow into the namespace
        // via the `storage.<key>` property convention (lance-namespace-impls
        // dir.rs from_properties: lines 423-436).
        for (key, value) in &storage_options {
            connect = connect.property(format!("storage.{key}"), value.clone());
        }
        let nm: Arc<dyn LanceNamespace> = connect
            .connect()
            .await
            .context("failed to connect lance Directory namespace")?;
        let nm_ident = NamespaceIdent::root();
        // spec.md#lance-handle-freshness: refresh window is scheme-keyed. Local-FS
        // manifest reads are microsecond-cheap, so `0` (always-refresh) is
        // essentially free and removes the stale-read window entirely. Object
        // stores have real per-call cost; `5s` caps manifest fetch overhead at
        // acceptable lag for human-driven queries.
        let refresh_after = if config::is_local(location) {
            Duration::ZERO
        } else {
            Duration::from_secs(5)
        };
        let handle = Self {
            datasets: DatasetSet {
                sessions: Mutex::new(CachedDataset {
                    dataset: open_or_create_via_ns(
                        &nm,
                        &nm_ident,
                        sessions::SESSIONS,
                        sessions::session_schema(),
                        &session,
                        &storage_options,
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                messages: Mutex::new(CachedDataset {
                    dataset: open_or_create_via_ns(
                        &nm,
                        &nm_ident,
                        sessions::MESSAGES,
                        sessions::message_schema(),
                        &session,
                        &storage_options,
                    )
                    .await?,
                    last_refresh: Instant::now(),
                    refresh_after,
                }),
                parts: OnceCell::new(),
            },
            retry: RetryPolicy::default(),
            session,
            nm,
            nm_ident,
            storage_options,
            location: location.clone(),
            parts_refresh_after: refresh_after,
        };
        Ok(handle)
    }

    pub fn location(&self) -> &Url {
        &self.location
    }

    /// Read-only view of the `storage_options` the handle was opened with.
    /// `pond status` needs them to instantiate a raw `object_store` client
    /// that can `LIST` the remote bucket for sizing.
    pub fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }

    /// Object-store URI for a `pond_sql_query` export artifact:
    /// `<location>/exports/<name>`. A sibling of the `*.lance` table dirs;
    /// the Directory namespace tracks tables in its `__manifest` table rather
    /// than by listing prefixes, so this prefix is never seen as a table
    /// (lance-namespace-impls dir/manifest.rs). Never `register_table`'d.
    fn export_uri(&self, name: &str) -> String {
        format!(
            "{}/exports/{name}",
            self.location.as_str().trim_end_matches('/')
        )
    }

    /// `ObjectStoreParams` carrying the handle's `storage_options` so raw
    /// object-store opens (export I/O, `table_sizes` listing) inherit the same
    /// credentials/region as the dataset opens. Empty options -> no accessor.
    fn object_store_params(&self) -> ObjectStoreParams {
        ObjectStoreParams {
            storage_options_accessor: (!self.storage_options.is_empty()).then(|| {
                Arc::new(StorageOptionsAccessor::with_static_options(
                    self.storage_options.clone(),
                ))
            }),
            ..Default::default()
        }
    }

    /// Write a `pond_sql_query` export artifact, reusing the handle's
    /// storage_options so S3 installs inherit the same credentials.
    pub(crate) async fn export_write(&self, name: &str, bytes: &[u8]) -> Result<()> {
        let uri = self.export_uri(name);
        let registry = Arc::new(ObjectStoreRegistry::default());
        let (store, path) =
            ObjectStore::from_uri_and_params(registry, &uri, &self.object_store_params())
                .await
                .with_context(|| format!("failed to open object store for {uri}"))?;
        store
            .put(&path, bytes)
            .await
            .with_context(|| format!("failed to write export {uri}"))?;
        Ok(())
    }

    /// Read a `pond_sql_query` export artifact back (for the
    /// `pond-sql-export://` MCP resource).
    pub(crate) async fn export_read(&self, name: &str) -> Result<Vec<u8>> {
        let uri = self.export_uri(name);
        let registry = Arc::new(ObjectStoreRegistry::default());
        let (store, path) =
            ObjectStore::from_uri_and_params(registry, &uri, &self.object_store_params())
                .await
                .with_context(|| format!("failed to open object store for {uri}"))?;
        let bytes = store
            .read_one_all(&path)
            .await
            .with_context(|| format!("failed to read export {uri}"))?;
        Ok(bytes.to_vec())
    }

    /// Local filesystem path of an export artifact, when the data dir is
    /// `file://`. The stdio MCP client shares this filesystem, so it can read
    /// the file directly (e.g. duckdb/polars) instead of pulling base64 via
    /// `resources/read`. `None` on object-store installs.
    pub(crate) fn export_local_path(&self, name: &str) -> Option<std::path::PathBuf> {
        if self.location.scheme() != "file" {
            return None;
        }
        let dir = self.location.to_file_path().ok()?;
        Some(dir.join("exports").join(name))
    }

    pub async fn row_counts(&self) -> Result<(usize, usize, usize)> {
        Ok((
            self.count_rows(Table::Sessions).await?,
            self.count_rows(Table::Messages).await?,
            self.count_rows(Table::Parts).await?,
        ))
    }

    /// Insert-only merge: append new rows, never overwrite a matched PK.
    /// Returns rows inserted. The fold lives separately under
    /// `Handle::optimize_table` (spec.md#lance-index-maintenance).
    pub(crate) async fn merge_insert(
        &self,
        table: Table,
        batch: RecordBatch,
        row_count: usize,
    ) -> Result<u64> {
        self.merge_insert_stats(table, batch, row_count)
            .await
            .map(|stats| stats.num_inserted_rows + stats.num_updated_rows)
    }

    /// Insert-only merge that surfaces Lance's full `MergeStats`. Callers that
    /// need bytes written, file count, or OCC retry count (e.g. `pond copy`'s
    /// progress display) use this; the thin wrapper above keeps the
    /// affected-rows return for everyone else.
    pub(crate) async fn merge_insert_stats(
        &self,
        table: Table,
        batch: RecordBatch,
        row_count: usize,
    ) -> Result<MergeStats> {
        self.merge(
            table,
            batch,
            row_count,
            "merge_insert",
            WhenMatched::DoNothing,
            WhenNotMatched::InsertAll,
        )
        .await
    }

    /// Update-only merge: `WhenMatched::UpdateAll` on matched PKs; unmatched
    /// rows dropped. The fold lives separately under `Handle::optimize_table`.
    pub(crate) async fn merge_update(
        &self,
        table: Table,
        batch: RecordBatch,
        row_count: usize,
    ) -> Result<u64> {
        self.merge(
            table,
            batch,
            row_count,
            "merge_update",
            WhenMatched::UpdateAll,
            WhenNotMatched::DoNothing,
        )
        .await
        .map(|stats| stats.num_inserted_rows + stats.num_updated_rows)
    }

    /// Shared merge path for [`Self::merge_insert`] and [`Self::merge_update`].
    /// Returns Lance's `MergeStats` verbatim so the progress layer can read
    /// `bytes_written` / `num_files_written` / `num_attempts` without a second
    /// round-trip; the thin wrappers above project to `u64` for callers that
    /// only need the affected-rows count.
    async fn merge(
        &self,
        table: Table,
        batch: RecordBatch,
        row_count: usize,
        op: &'static str,
        when_matched: WhenMatched,
        when_not_matched: WhenNotMatched,
    ) -> Result<MergeStats> {
        if row_count == 0 {
            return Ok(MergeStats::default());
        }
        let started = Instant::now();
        let result = self
            .retry_lance(table.label(), || async {
                let mut cached = self.cached(table).await?.lock().await;
                let existing = cached.latest().await?;
                let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
                let mut builder = MergeInsertBuilder::try_new(Arc::new(existing), Vec::new())?;
                builder.when_matched(when_matched.clone());
                builder.when_not_matched(when_not_matched.clone());
                // pond presents each PK at most once per batch; FirstSeen keeps
                // the first occurrence rather than failing (Lance's default).
                builder.source_dedupe_behavior(SourceDedupeBehavior::FirstSeen);
                // Cleanup is operator-driven via `pond optimize`; the
                // per-commit auto hook would add a LIST per write on remote
                // backends without changing the steady-state retention.
                builder.skip_auto_cleanup(true);
                let (dataset, stats) = builder
                    .try_build()?
                    .execute_reader(Box::new(reader))
                    .await?;
                cached.replace(dataset.as_ref().clone());
                Ok(stats)
            })
            .await;
        let skipped = result
            .as_ref()
            .map(|s| s.num_skipped_duplicates)
            .unwrap_or(0);
        tracing::info!(
            target: "pond::perf",
            op,
            table = %table.label(),
            rows = row_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            skipped,
            "merge",
        );
        result
    }

    /// Append a streamed source into `table` under a single commit - the
    /// bandwidth-bound counterpart to [`Self::merge`]. spec.md#session-durable-copy:
    /// rows that cannot collide on the destination (absent sessions) take this
    /// path. `Append` never joins or probes the target, so its cost is the
    /// bytes written, not the per-batch commit + key-scan that `merge_insert`
    /// pays - the fix for store-to-store copy being commit-latency-bound on
    /// remote object stores.
    ///
    /// `make_source` is a *factory*, not a prebuilt stream: a Lance scan stream
    /// is one-shot, so an OCC retry rebuilds it. A single per-call
    /// `WriteCumulative` (shared across attempts, NOT fresh per attempt) makes
    /// the progress fold exact under retries - see
    /// [`crate::progress::CopyState::record_write_progress`].
    pub(crate) async fn append_stream<F, Fut>(
        &self,
        table: Table,
        make_source: F,
        state: Option<&Arc<crate::progress::CopyState>>,
    ) -> Result<AppendStats>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<SendableRecordBatchStream>>,
    {
        let state = state.cloned();
        let cum = Arc::new(crate::progress::WriteCumulative::default());
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        // Byte-sized fragments, not Lance's 90 GB default: kilobyte rows would
        // otherwise pack multi-GiB fragments that compaction rewrites
        // wholesale (see TARGET_FRAGMENT_BYTES). Reuse the create params so the
        // appended fragments match the table's storage version / row-id mode.
        let mut params = sessions::write_params_for_create();
        params.mode = WriteMode::Append;
        params.max_bytes_per_file = TARGET_FRAGMENT_BYTES as usize;

        let started = Instant::now();
        self.retry_lance(table.label(), || {
            let make_source = &make_source;
            let cum = cum.clone();
            let state = state.clone();
            let attempts = attempts.clone();
            let params = &params;
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut cached = self.cached(table).await?.lock().await;
                let existing = cached.latest().await?;
                let stream = make_source().await?;
                let builder = InsertBuilder::new(Arc::new(existing))
                    .with_params(params)
                    .progress(move |stats| match &state {
                        // Both arms advance `cum`; the live fold only runs when
                        // a reporter is attached.
                        Some(state) => state.record_write_progress(table, &cum, &stats),
                        None => {
                            cum.observe(&stats);
                        }
                    });
                let new_dataset = builder.execute_stream(stream).await?;
                cached.replace(new_dataset);
                Ok::<_, anyhow::Error>(())
            }
        })
        .await?;

        let attempts = attempts.load(std::sync::atomic::Ordering::Relaxed);
        if let Some(state) = &state {
            state.record_append(attempts);
        }
        let stats = AppendStats {
            rows: cum.rows(),
            bytes_written: cum.bytes(),
            files_written: cum.files(),
            attempts,
        };
        tracing::info!(
            target: "pond::perf",
            op = "append",
            table = %table.label(),
            rows = stats.rows,
            files = stats.files_written,
            attempts,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "append",
        );
        Ok(stats)
    }

    /// Run the table-local maintenance cycle for the supplied index intents.
    /// BTree is rebuilt from scratch to dodge Lance v7.0.0-beta.16's flat
    /// BTree combine bug; Bitmap, FTS, and IVF_PQ fold via append.
    ///
    /// spec.md#substrate 3.7 (`lance-index-maintenance`): indices and compaction
    /// commit independently and use independent retry budgets, so a hot writer
    /// that starves compaction (Rewrite) does not abort the index build
    /// (Update) the operator actually asked for.
    pub async fn optimize_table(
        &self,
        table: Table,
        intents: &[IndexIntent],
        progress: Option<&OptimizeProgressFn>,
        policy: &MaintenancePolicy,
    ) -> TableOptimizeOutcome {
        let compaction = self
            .run_optimize_compact_phase(table, progress, policy)
            .await;
        let indices = self
            .run_optimize_indices_phase(table, intents, progress)
            .await;
        TableOptimizeOutcome {
            table,
            indices,
            compaction,
        }
    }

    /// Run only the indices phase for one table. Used by the optimize embed
    /// stage's tail
    /// to fold newly written vectors into the indices without paying the
    /// compaction retry budget while embed itself may still be writing.
    pub async fn optimize_table_indices_only(
        &self,
        table: Table,
        intents: &[IndexIntent],
        progress: Option<&OptimizeProgressFn>,
    ) -> PhaseOutcome {
        self.run_optimize_indices_phase(table, intents, progress)
            .await
    }

    async fn run_optimize_indices_phase(
        &self,
        table: Table,
        intents: &[IndexIntent],
        progress: Option<&OptimizeProgressFn>,
    ) -> PhaseOutcome {
        if intents.is_empty() {
            return PhaseOutcome::Noop;
        }
        let result = self
            .retry_lance(table.label(), || async {
                let mut guard = self.cached(table).await?.lock().await;
                let mut dataset = guard.latest().await?;
                let did_work =
                    optimize_table_indices(&mut dataset, intents, table, progress).await?;
                guard.replace(dataset);
                Ok::<_, anyhow::Error>(did_work)
            })
            .await;
        match result {
            Ok(true) => PhaseOutcome::Ok,
            Ok(false) => PhaseOutcome::Noop,
            Err(error) if is_conflict_exhausted(&error) => PhaseOutcome::SkippedConflict,
            Err(error) => PhaseOutcome::Failed(error),
        }
    }

    async fn run_optimize_compact_phase(
        &self,
        table: Table,
        progress: Option<&OptimizeProgressFn>,
        policy: &MaintenancePolicy,
    ) -> PhaseOutcome {
        let result = self
            .retry_lance(table.label(), || async {
                let mut guard = self.cached(table).await?.lock().await;
                let mut dataset = guard.latest().await?;
                optimize_table_compact(&mut dataset, table, progress, policy).await?;
                guard.replace(dataset);
                Ok::<_, anyhow::Error>(())
            })
            .await;
        match result {
            Ok(()) => PhaseOutcome::Ok,
            Err(error) if is_conflict_exhausted(&error) => PhaseOutcome::SkippedConflict,
            Err(error) => PhaseOutcome::Failed(error),
        }
    }

    pub async fn rebuild_index(&self, table: Table, intent: &IndexIntent) -> Result<()> {
        self.retry_lance(table.label(), || async {
            let mut guard = self.cached(table).await?.lock().await;
            let mut dataset = guard.latest().await?;
            rebuild_index(&mut dataset, intent).await?;
            guard.replace(dataset);
            Ok(())
        })
        .await
    }

    pub async fn index_status(
        &self,
        table: Table,
        intents: &[IndexIntent],
    ) -> Result<Vec<IndexStatus>> {
        let dataset = self.dataset(table).await?;
        index_status(table, &dataset, intents).await
    }

    pub(crate) async fn dataset(&self, table: Table) -> Result<Dataset> {
        let mut cached = self.cached(table).await?.lock().await;
        cached.latest().await
    }
    /// Build a prefiltered `Scanner` for `table`. Composable read entry
    /// point for callers that need to layer extra builder calls
    /// (`full_text_search`, `nearest`) on top of pond's predicate seam.
    /// Routine scans should prefer `Handle::scan`.
    pub(crate) async fn scanner(
        &self,
        table: Table,
        predicate: Option<&Predicate>,
    ) -> Result<lance::dataset::scanner::Scanner> {
        let dataset = self.dataset(table).await?;
        scanner_with_prefilter(&dataset, predicate)
    }
    /// Single read entry point: prefilter via `predicate`, optionally
    /// project, return the prepared `Scanner` (spec.md#lance-chokepoints-read).
    pub async fn scan(
        &self,
        table: Table,
        opts: ScanOpts<'_>,
    ) -> Result<lance::dataset::scanner::Scanner> {
        let mut scanner = self.scanner(table, opts.predicate).await?;
        if let Some(projection) = opts.projection {
            scanner.project(projection)?;
        }
        Ok(scanner)
    }
    pub(crate) async fn scan_batch(
        &self,
        table: Table,
        predicate: Option<&Predicate>,
        projection: &[&str],
    ) -> Result<RecordBatch> {
        let opts = ScanOpts {
            predicate,
            projection: (!projection.is_empty()).then_some(projection),
        };
        self.scan(table, opts)
            .await?
            .try_into_batch()
            .await
            .context("scan failed")
    }
    pub async fn count_rows(&self, table: Table) -> Result<usize> {
        self.dataset(table)
            .await?
            .count_rows(None)
            .await
            .map_err(Into::into)
    }
    /// Collect the primary-key (`id`) set for `table`. Storage verification
    /// compares these sets across two stores: matching row counts can still
    /// hide divergent membership, so proving a destination is a complete
    /// superset of a source needs the ids, not the cardinalities
    /// (spec.md#substrate, `lance-deterministic-pk`).
    pub async fn collect_ids(&self, table: Table) -> Result<std::collections::HashSet<String>> {
        let batch = self.scan_batch(table, None, &["id"]).await?;
        let ids = batch
            .column_by_name("id")
            .context("scan projection dropped the id column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("id column is not Utf8")?;
        Ok(ids.iter().flatten().map(str::to_owned).collect())
    }
    /// Names of every index on `messages` - the vector-index tests read this.
    #[cfg(test)]
    pub(crate) async fn messages_index_names(&self) -> Result<Vec<String>> {
        let dataset = self.dataset(Table::Messages).await?;
        let indices = dataset.load_indices().await?;
        Ok(indices.iter().map(|index| index.name.clone()).collect())
    }

    /// Count rows in `table` not yet covered by `index_name`. Manifest-only;
    /// a missing index reports the whole table. Powers `pond status`.
    pub(crate) async fn unindexed_row_count(
        &self,
        table: Table,
        index_name: &str,
    ) -> Result<usize> {
        let dataset = self.dataset(table).await?;
        let fragments = dataset
            .unindexed_fragments(index_name)
            .await
            .with_context(|| format!("unindexed_fragments failed for {}", table.label()))?;
        Ok(fragments
            .iter()
            .map(|fragment| fragment.num_rows().unwrap_or(0))
            .sum())
    }

    /// Drop the named index. Used by the `pond optimize --force-embed` model-swap path
    /// to retire an IVF_PQ whose centroids belong to the old distance
    /// space, before the next write re-bootstraps it over the new model's
    /// vectors. Errors when the index does not exist; callers may swallow
    /// that.
    pub(crate) async fn drop_index(&self, table: Table, name: &str) -> Result<()> {
        let mut guard = self.cached(table).await?.lock().await;
        let mut dataset = guard.latest().await?;
        dataset
            .drop_index(name)
            .await
            .with_context(|| format!("drop_index({name}) failed for {}", table.label()))?;
        guard.replace(dataset);
        Ok(())
    }

    /// Resolve each table's stored location through the namespace catalog
    /// (spec.md#lance-chokepoints-catalog) - no hardcoded `.lance` suffix.
    async fn table_location(&self, table_name: &str) -> Result<String> {
        let request = DescribeTableRequest {
            id: Some(self.nm_ident.as_table_id(table_name)),
            ..Default::default()
        };
        let response = self
            .nm
            .describe_table(request)
            .await
            .with_context(|| format!("failed to describe table {table_name}"))?;
        response
            .location
            .with_context(|| format!("namespace returned no location for table {table_name}"))
    }

    /// Whether the store holds synced data yet. `open` eagerly creates empty
    /// `sessions`/`messages` datasets, but `parts` opens lazily on first write
    /// (see `open_with_options`), so its presence is the "has been synced"
    /// signal - letting read-only surfaces (`pond status`)
    /// render an empty state instead of erroring on the first `parts` describe.
    pub async fn initialized(&self) -> Result<bool> {
        let request = DescribeTableRequest {
            id: Some(self.nm_ident.as_table_id(sessions::PARTS)),
            ..Default::default()
        };
        match self.nm.describe_table(request).await {
            Ok(_) => Ok(true),
            Err(error) if is_namespace_error_code(&error, ErrorCode::TableNotFound) => Ok(false),
            Err(error) => {
                Err(anyhow::Error::from(error)).context("failed to probe table existence")
            }
        }
    }

    /// On-disk byte totals for the three datasets plus the data-dir remainder.
    /// Every byte is sized by listing through Lance's object store
    /// (spec.md#lance-chokepoints-storage), identical for `file://` and `s3://`.
    pub async fn table_sizes(&self) -> Result<TableSizes> {
        let registry = Arc::new(ObjectStoreRegistry::default());
        let params = self.object_store_params();

        let sessions = self
            .listed_size(
                &registry,
                &params,
                &self.table_location(sessions::SESSIONS).await?,
            )
            .await?;
        let messages = self
            .listed_size(
                &registry,
                &params,
                &self.table_location(sessions::MESSAGES).await?,
            )
            .await?;
        let parts = self
            .listed_size(
                &registry,
                &params,
                &self.table_location(sessions::PARTS).await?,
            )
            .await?;
        // `other` is whatever sits under the data-dir root but not in the three
        // tables (config.toml, stray index temp files): root total minus them.
        let root_total = self
            .listed_size(&registry, &params, self.location.as_str())
            .await?;
        let other = root_total.saturating_sub(sessions + messages + parts);
        let sessions_data = self
            .data_liveness(&registry, &params, Table::Sessions, sessions::SESSIONS)
            .await?;
        let messages_data = self
            .data_liveness(&registry, &params, Table::Messages, sessions::MESSAGES)
            .await?;
        let parts_data = self
            .data_liveness(&registry, &params, Table::Parts, sessions::PARTS)
            .await?;
        Ok(TableSizes {
            sessions,
            messages,
            parts,
            other,
            sessions_data,
            messages_data,
            parts_data,
        })
    }

    async fn data_liveness(
        &self,
        registry: &Arc<ObjectStoreRegistry>,
        params: &ObjectStoreParams,
        table: Table,
        table_name: &str,
    ) -> Result<DataLiveness> {
        let location = self.table_location(table_name).await?;
        let data_dir = format!("{}/data", location.trim_end_matches('/'));
        let on_disk = self.listed_size(registry, params, &data_dir).await?;
        let dataset = self.dataset(table).await?;
        let live = dataset
            .get_fragments()
            .iter()
            .try_fold(0u64, |total, fragment| {
                Some(total + fragment_bytes(fragment.metadata())?)
            });
        Ok(DataLiveness { on_disk, live })
    }

    /// Sum `ObjectMeta.size` for every object recursively under `uri`.
    async fn listed_size(
        &self,
        registry: &Arc<ObjectStoreRegistry>,
        params: &ObjectStoreParams,
        uri: &str,
    ) -> Result<u64> {
        let (store, base) = ObjectStore::from_uri_and_params(registry.clone(), uri, params)
            .await
            .with_context(|| format!("failed to open object store for {uri}"))?;
        let mut listing = store.list(Some(base));
        let mut total = 0u64;
        while let Some(meta) = listing.next().await {
            let meta = meta.with_context(|| format!("listing {uri} failed"))?;
            total += meta.size;
        }
        Ok(total)
    }
    async fn cached(&self, table: Table) -> Result<&Mutex<CachedDataset>> {
        match table {
            Table::Sessions => Ok(&self.datasets.sessions),
            Table::Messages => Ok(&self.datasets.messages),
            Table::Parts => self.parts_cached().await,
        }
    }

    /// Open `parts.lance` on first use (spec.md#datasets). Single-flight via
    /// `OnceCell`; once initialized, behaves identically to the other two.
    async fn parts_cached(&self) -> Result<&Mutex<CachedDataset>> {
        self.datasets
            .parts
            .get_or_try_init(|| async {
                let dataset = open_or_create_via_ns(
                    &self.nm,
                    &self.nm_ident,
                    sessions::PARTS,
                    sessions::part_schema(),
                    &self.session,
                    &self.storage_options,
                )
                .await?;
                Ok::<_, anyhow::Error>(Mutex::new(CachedDataset {
                    dataset,
                    last_refresh: Instant::now(),
                    refresh_after: self.parts_refresh_after,
                }))
            })
            .await
    }
    async fn retry_lance<T, Fut, Op>(&self, label: &str, mut operation: Op) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>>,
        Op: FnMut() -> Fut,
    {
        let mut attempt = 0u8;
        loop {
            attempt = attempt.saturating_add(1);
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < self.retry.attempts => {
                    let backoff = self.backoff(attempt);
                    // `{:#}` walks anyhow's cause chain inline; `%error` (Display)
                    // drops everything below the top-level message.
                    let error_chain = format!("{error:#}");
                    tracing::warn!(
                        label,
                        attempt,
                        ?backoff,
                        error = %error_chain,
                        "retrying Lance operation"
                    );
                    tokio::time::sleep(backoff).await;
                }
                Err(error) => {
                    let error_chain = format!("{error:#}");
                    tracing::warn!(
                        label,
                        attempt,
                        error = %error_chain,
                        "Lance operation exhausted retries"
                    );
                    // spec.md#protocol: surface OCC failures as a typed `conflict`
                    // rather than the generic `storage_unavailable` bucket. The
                    // chain root is a `lance::Error` (commit-conflict family) when
                    // pond's retry layer exhausted because the manifest could not
                    // be advanced; everything else (timeouts, IAM, disk) stays
                    // `storage_unavailable`.
                    if is_commit_conflict(&error) {
                        return Err(error.context(ConflictExhausted { attempts: attempt }));
                    }
                    return Err(error);
                }
            }
        }
    }
    fn backoff(&self, attempt: u8) -> Duration {
        let shift = u32::from(attempt.saturating_sub(1));
        let multiplier = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        let base = self.retry.initial_backoff.saturating_mul(multiplier);
        // Symmetric +/- `jitter` factor de-correlates concurrent retriers on
        // a contended manifest (spec.md#lance-retry-jitter); clamped to `max_backoff`.
        let factor = (1.0 + self.retry.jitter * (fastrand::f64() * 2.0 - 1.0)).max(0.0);
        base.mul_f64(factor).min(self.retry.max_backoff)
    }
}
/// Compaction phase: plan + amplification veto + execute + `cleanup_old_versions`,
/// one retry block, separate from the indices phase so a lost Rewrite race
/// does not abort index work.
///
/// Vetoes Lance-planned tasks instead of pre-gating on pond fragment math:
/// Lance bins split at index-coverage boundaries, so pond predictions diverge
/// from what Lance actually rewrites (the old run-sum gate latched open and
/// rewrote a 665 MiB tail fragment every 5-min sync). Only whole planned
/// tasks are filtered, so OCC and conflict semantics are untouched.
///
/// spec.md#lance-index-maintenance mandates FRI on by default, but at
/// v7.0.0-beta.16 `defer_index_remap=true` together with `stable-row-ids`
/// panics in `optimize.rs::commit_compaction` with "defer_index_remap
/// requires row_addrs but none were provided": `rewrite_files` skips
/// row_addrs when stable row ids are on, then the FRI builder demands
/// them. With stable_row_ids the remap step is already a no-op
/// (`optimize.rs:1490`: `needs_remapping = !uses_stable_row_ids() &&
/// !defer_index_remap`), so running without FRI is correct - we only
/// lose the documented concurrency-with-index-build benefit. Flip to
/// `true` once upstream fixes the conflict.
async fn optimize_table_compact(
    dataset: &mut Dataset,
    table: Table,
    progress: Option<&OptimizeProgressFn>,
    policy: &MaintenancePolicy,
) -> Result<()> {
    let stats: Vec<FragmentStat> = dataset
        .get_fragments()
        .iter()
        .map(|fragment| fragment_stat(fragment.metadata()))
        .collect();
    let compaction = CompactionOptions {
        target_rows_per_fragment: derived_target_rows(&stats),
        max_bytes_per_file: Some(TARGET_FRAGMENT_BYTES as usize),
        defer_index_remap: false,
        ..CompactionOptions::default()
    };

    let mut plan = plan_compaction(dataset, &compaction).await?;
    if policy.compaction_fragment_cap > 0 {
        plan.tasks.retain(|task| {
            let task_stats: Vec<FragmentStat> = task.fragments.iter().map(fragment_stat).collect();
            let keep = keep_task(
                &task_stats,
                policy.compaction_fragment_cap,
                compaction.materialize_deletions_threshold,
            );
            if !keep {
                tracing::debug!(
                    target: "pond::perf",
                    table = table.as_str(),
                    fragments = task_stats.len(),
                    "compaction task vetoed: merge dominated by one large fragment",
                );
            }
            keep
        });
    }
    if plan.tasks.is_empty() {
        tracing::debug!(
            target: "pond::perf",
            table = table.as_str(),
            "compaction skipped: no task to run",
        );
    } else {
        emit(
            progress,
            OptimizeEvent::PhaseStart {
                table,
                phase: OptimizePhase::Compact,
                detail: None,
            },
        );
        let started = Instant::now();
        let mut completed = Vec::with_capacity(plan.tasks.len());
        for task in plan.compaction_tasks() {
            completed.push(task.execute(dataset).await?);
        }
        commit_compaction(
            dataset,
            completed,
            Arc::new(DatasetIndexRemapperOptions::default()),
            &compaction,
        )
        .await?;
        emit(
            progress,
            OptimizeEvent::PhaseDone {
                table,
                phase: OptimizePhase::Compact,
                elapsed_ms: started.elapsed().as_millis() as u64,
            },
        );
    }

    // Safe GC only. delete_unverified=false keeps Lance's 7-day in-progress
    // guard, so this never races a concurrent writer (spec.md#concurrency); GC
    // runs outside OCC, so the guard is what makes it safe on any backend.
    emit(
        progress,
        OptimizeEvent::PhaseStart {
            table,
            phase: OptimizePhase::Cleanup,
            detail: None,
        },
    );
    let started = Instant::now();
    dataset
        .cleanup_old_versions(policy.cleanup_older_than, Some(false), Some(false))
        .await
        .context("cleanup_old_versions failed during index optimize")?;
    emit(
        progress,
        OptimizeEvent::PhaseDone {
            table,
            phase: OptimizePhase::Cleanup,
            elapsed_ms: started.elapsed().as_millis() as u64,
        },
    );

    Ok(())
}

/// Indices phase: per-intent create/rebuild + batched `optimize_indices(append)`
/// for incremental families. Returns `true` if anything committed.
async fn optimize_table_indices(
    dataset: &mut Dataset,
    intents: &[IndexIntent],
    table: Table,
    progress: Option<&OptimizeProgressFn>,
) -> Result<bool> {
    let existing = dataset.load_indices().await?;
    let existing_names: std::collections::HashSet<String> =
        existing.iter().map(|index| index.name.clone()).collect();

    let mut append_indices: Vec<String> = Vec::new();
    let mut did_work = false;

    for intent in intents {
        let exists = existing_names.contains(intent.name);

        if !exists {
            if !intent.trigger.should_create(dataset).await? {
                continue;
            }
            let params = intent.params.build(dataset).await?;
            let index_type = intent.params.index_type();
            tracing::info!(
                index = intent.name,
                column = intent.column,
                "creating Lance index (trigger fired)",
            );
            emit(
                progress,
                OptimizeEvent::PhaseStart {
                    table,
                    phase: OptimizePhase::IndexCreate,
                    detail: Some(intent.name.to_owned()),
                },
            );
            let started = Instant::now();
            dataset
                .create_index(
                    &[intent.column],
                    index_type,
                    Some(intent.name.to_owned()),
                    params.as_ref(),
                    false,
                )
                .await
                .with_context(|| format!("failed to create index {}", intent.name))?;
            emit(
                progress,
                OptimizeEvent::PhaseDone {
                    table,
                    phase: OptimizePhase::IndexCreate,
                    elapsed_ms: started.elapsed().as_millis() as u64,
                },
            );
            did_work = true;
            continue;
        }

        let unindexed = dataset.unindexed_fragments(intent.name).await?;
        if unindexed.is_empty() {
            continue;
        }
        // Lag guard: let fragments accumulate behind the brute-force fallback
        // rather than firing a commit per tiny append. Threshold is operator-
        // tunable via `[maintenance].index_lag_threshold`.
        if unindexed.len() < index_lag_threshold() {
            continue;
        }
        match intent.params {
            IndexParamsKind::Scalar(BuiltinIndexType::BTree) => {
                let params = intent.params.build(dataset).await?;
                let index_type = intent.params.index_type();
                tracing::debug!(
                    target: "pond::perf",
                    index = intent.name,
                    column = intent.column,
                    "rebuilding Lance BTree index",
                );
                emit(
                    progress,
                    OptimizeEvent::PhaseStart {
                        table,
                        phase: OptimizePhase::IndexRebuild,
                        detail: Some(intent.name.to_owned()),
                    },
                );
                let started = Instant::now();
                dataset
                    .create_index(
                        &[intent.column],
                        index_type,
                        Some(intent.name.to_owned()),
                        params.as_ref(),
                        true,
                    )
                    .await
                    .with_context(|| format!("failed to rebuild index {}", intent.name))?;
                emit(
                    progress,
                    OptimizeEvent::PhaseDone {
                        table,
                        phase: OptimizePhase::IndexRebuild,
                        elapsed_ms: started.elapsed().as_millis() as u64,
                    },
                );
                did_work = true;
            }
            IndexParamsKind::Scalar(BuiltinIndexType::Bitmap)
            | IndexParamsKind::InvertedFtsNgram { .. }
            | IndexParamsKind::IvfPqCosine { .. } => {
                append_indices.push(intent.name.to_owned());
            }
            IndexParamsKind::Scalar(_) => {
                let params = intent.params.build(dataset).await?;
                emit(
                    progress,
                    OptimizeEvent::PhaseStart {
                        table,
                        phase: OptimizePhase::IndexRebuild,
                        detail: Some(intent.name.to_owned()),
                    },
                );
                let started = Instant::now();
                dataset
                    .create_index(
                        &[intent.column],
                        intent.params.index_type(),
                        Some(intent.name.to_owned()),
                        params.as_ref(),
                        true,
                    )
                    .await
                    .with_context(|| format!("failed to rebuild index {}", intent.name))?;
                emit(
                    progress,
                    OptimizeEvent::PhaseDone {
                        table,
                        phase: OptimizePhase::IndexRebuild,
                        elapsed_ms: started.elapsed().as_millis() as u64,
                    },
                );
                did_work = true;
            }
        }
    }

    if !append_indices.is_empty() {
        let to_append = append_indices.clone();
        emit(
            progress,
            OptimizeEvent::PhaseStart {
                table,
                phase: OptimizePhase::IndexAppend,
                detail: Some(append_indices.join(", ")),
            },
        );
        let started = Instant::now();
        dataset
            .optimize_indices(&OptimizeOptions::append().index_names(to_append))
            .await
            .context("optimize_indices(append) failed during index optimize")?;
        emit(
            progress,
            OptimizeEvent::PhaseDone {
                table,
                phase: OptimizePhase::IndexAppend,
                elapsed_ms: started.elapsed().as_millis() as u64,
            },
        );
        tracing::debug!(
            target: "pond::perf",
            indices = ?append_indices,
            "appended trailing fragments into indices",
        );
        did_work = true;
    }

    Ok(did_work)
}

async fn rebuild_index(dataset: &mut Dataset, intent: &IndexIntent) -> Result<()> {
    if !intent.trigger.should_create(dataset).await? {
        return Ok(());
    }
    let params = intent.params.build(dataset).await?;
    dataset
        .create_index(
            &[intent.column],
            intent.params.index_type(),
            Some(intent.name.to_owned()),
            params.as_ref(),
            true,
        )
        .await
        .with_context(|| format!("failed to rebuild index {}", intent.name))?;
    Ok(())
}

async fn index_status(
    table: Table,
    dataset: &Dataset,
    intents: &[IndexIntent],
) -> Result<Vec<IndexStatus>> {
    let existing = dataset.load_indices().await?;
    let existing_names: std::collections::HashSet<String> =
        existing.iter().map(|index| index.name.clone()).collect();
    let total_fragments = dataset.get_fragments().len();
    let total_rows = dataset.count_rows(None).await?;
    let mut statuses = Vec::with_capacity(intents.len());
    for intent in intents {
        let exists = existing_names.contains(intent.name);
        if !exists {
            statuses.push(IndexStatus {
                table,
                intent_name: intent.name.to_owned(),
                fragments_covered: 0,
                unindexed_fragments: total_fragments,
                unindexed_rows: total_rows,
                exists,
            });
            continue;
        }
        let unindexed = dataset
            .unindexed_fragments(intent.name)
            .await
            .with_context(|| format!("unindexed_fragments failed for {}", table.label()))?;
        let unindexed_fragments = unindexed.len();
        let unindexed_rows = unindexed
            .iter()
            .map(|fragment| fragment.num_rows().unwrap_or(0))
            .sum();
        statuses.push(IndexStatus {
            table,
            intent_name: intent.name.to_owned(),
            fragments_covered: total_fragments.saturating_sub(unindexed_fragments),
            unindexed_fragments,
            unindexed_rows,
            exists,
        });
    }
    Ok(statuses)
}

/// Open the table at `table_name` via the namespace; create + initialize on
/// `TableNotFound`. Schema-checks the on-disk dataset against pond's
/// expectation so a stale data dir surfaces early.
///
/// Probes via `nm.describe_table` directly rather than `DatasetBuilder::from_namespace`:
/// the builder re-wraps an already-`Namespace`-wrapped error
/// (lance/src/dataset/builder.rs:142), so going through it would force a
/// chain-walk to classify `TableNotFound`. The direct probe stays at one
/// wrap level and downcasts cleanly. Managed-versioning hookup (REST
/// namespace external-manifest commits) is not wired here; v1 ships
/// Directory v2 only.
async fn open_or_create_via_ns(
    nm: &Arc<dyn LanceNamespace>,
    nm_ident: &NamespaceIdent,
    table_name: &str,
    schema: lance::deps::arrow_schema::SchemaRef,
    session: &Arc<Session>,
    storage_options: &HashMap<String, String>,
) -> Result<Dataset> {
    let table_id = nm_ident.as_table_id(table_name);

    let request = DescribeTableRequest {
        id: Some(table_id.clone()),
        ..Default::default()
    };
    match nm.describe_table(request).await {
        Ok(response) => {
            let location = response.location.with_context(|| {
                format!("namespace returned no location for table {table_name}")
            })?;
            let mut builder = DatasetBuilder::from_uri(&location).with_session(session.clone());
            if !storage_options.is_empty() {
                builder = builder.with_storage_options(storage_options.clone());
            }
            let dataset = builder
                .load()
                .await
                .with_context(|| format!("failed to open table {table_name}"))?;
            ensure_schema_matches(&dataset, schema.as_ref(), table_name)?;
            return Ok(dataset);
        }
        Err(error) => match &error {
            error if is_namespace_error_code(error, ErrorCode::TableNotFound) => {
                // fall through to create
            }
            _ => {
                return Err(anyhow::Error::from(error))
                    .with_context(|| format!("failed to describe table {table_name}"));
            }
        },
    }

    // Create path: pond seeds an empty dataset with the canonical schema so
    // every subsequent open lands on a real Lance dataset, not a phantom.
    let mut write_params = sessions::write_params_for_create();
    write_params.session = Some(session.clone());
    write_params.mode = WriteMode::Create;
    if !storage_options.is_empty() {
        write_params.store_params = Some(ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                storage_options.clone(),
            ))),
            ..Default::default()
        });
    }
    let reader = sessions::empty_reader(schema)?;
    Dataset::write_into_namespace(reader, nm.clone(), table_id, Some(write_params))
        .await
        .with_context(|| format!("failed to create table {table_name}"))
}

// lance-namespace sometimes nests one `lance::Error::Namespace` inside another
// before the underlying `NamespaceError`; walk the whole `.source()` chain
// rather than only matching the outer variant.
fn is_namespace_error_code(error: &lance::Error, code: ErrorCode) -> bool {
    if !matches!(error, lance::Error::Namespace { .. }) {
        return false;
    }
    std::iter::successors(Some(error as &(dyn std::error::Error + 'static)), |link| {
        link.source()
    })
    .filter_map(|link| link.downcast_ref::<NamespaceError>())
    .any(|inner| inner.code() == code)
}

fn scanner_with_prefilter(
    dataset: &Dataset,
    predicate: Option<&Predicate>,
) -> Result<lance::dataset::scanner::Scanner> {
    let mut scanner = dataset.scan();
    scanner.prefilter(true);
    if let Some(predicate) = predicate {
        let filter = predicate.to_lance();
        if !filter.is_empty() {
            scanner.filter(&filter)?;
        }
    }
    Ok(scanner)
}
fn ensure_schema_matches(
    dataset: &Dataset,
    expected: &lance::deps::arrow_schema::Schema,
    table_name: &str,
) -> Result<()> {
    use lance::deps::arrow_schema::DataType;
    use std::collections::BTreeSet;
    let actual = lance::deps::arrow_schema::Schema::from(dataset.schema());
    let actual_names: BTreeSet<&str> = actual.fields().iter().map(|f| f.name().as_str()).collect();
    let expected_names: BTreeSet<&str> = expected
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    if actual_names != expected_names {
        anyhow::bail!(
            "table {table_name} has columns {actual_names:?} but this pond build expects \
             {expected_names:?} - the on-disk store predates a schema change; delete the \
             data directory and re-run `pond ingest`",
        );
    }
    // Catch a vector-dim change (configured `[embeddings].dim` differs from
    // the on-disk vector column width) early with a friendly message. Lance
    // would otherwise reject the next write with an opaque schema-mismatch
    // error inside the `merge_update` path.
    for actual_field in actual.fields() {
        let Some(expected_field) = expected.field_with_name(actual_field.name()).ok() else {
            continue;
        };
        if let (DataType::FixedSizeList(_, actual_dim), DataType::FixedSizeList(_, expected_dim)) =
            (actual_field.data_type(), expected_field.data_type())
            && actual_dim != expected_dim
        {
            tracing::warn!(
                table = table_name,
                column = actual_field.name(),
                actual_dim,
                expected_dim,
                "embedding dimension differs from config; open proceeds because model swaps are operator-driven",
            );
        }
    }
    Ok(())
}
/// Object-store defaults injected for any non-local pond location. Each key
/// is only set when neither the user-provided key nor its env-var-form alias
/// is already present, so explicit overrides in `[storage]` always win.
/// `aws_unsigned_payload` is gated on a custom endpoint (the marker for
/// S3-compatible stores like Hetzner, MinIO, R2), where the SHA256 payload
/// signature is wasted work the server does not validate.
fn apply_remote_storage_defaults(options: &mut HashMap<String, String>) {
    fn set_default(options: &mut HashMap<String, String>, aliases: &[&str], value: &str) {
        if aliases
            .iter()
            .any(|alias| options.keys().any(|k| k.eq_ignore_ascii_case(alias)))
        {
            return;
        }
        options.insert(aliases[0].to_owned(), value.to_owned());
    }
    set_default(options, &["pool_idle_timeout"], "300 seconds");
    set_default(options, &["connect_timeout"], "10 seconds");
    let has_custom_endpoint = ["aws_endpoint", "endpoint"]
        .iter()
        .any(|alias| options.keys().any(|k| k.eq_ignore_ascii_case(alias)));
    if has_custom_endpoint {
        set_default(
            options,
            &["aws_unsigned_payload", "unsigned_payload"],
            "true",
        );
    }
}

fn quoted_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn like_contains(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('\'', "''");
    format!("'%{escaped}%'")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use tempfile::TempDir;

    fn set(scope: Option<&str>) -> CredsSet {
        CredsSet {
            scope: scope.map(str::to_owned),
            access_key_id: Some("AKIA".to_owned()),
            secret_access_key: Some("shh".to_owned()),
            ..CredsSet::default()
        }
    }

    fn opts(resolved: &ResolvedStorage, key: &str) -> Option<String> {
        resolved.options.get(key).cloned()
    }

    #[test]
    fn storage_url_translation_table() {
        // file (Lance's `uri_to_url` appends the trailing slash; `child_uri`
        // trims it downstream)
        let local = StorageUrl::parse("/srv/pond").unwrap();
        assert_eq!(local.lance_url().as_str(), "file:///srv/pond/");
        assert!(local.is_local());
        assert!(local.scheme_options.is_empty());
        // s3 passthrough
        let aws = StorageUrl::parse("s3://bucket/prefix").unwrap();
        assert_eq!(aws.lance_url().as_str(), "s3://bucket/prefix");
        assert!(aws.scheme_options.is_empty());
        // s3+https: TLS stays on, virtual-hosted defaults on for domain
        // hosts, region defaults deterministically. The endpoint is
        // assembled at resolve time with the bucket folded into the host
        // (object_store's virtual-hosted convention).
        let fat = StorageUrl::parse("s3+https://nbg1.example.com/my-pond/sub").unwrap();
        assert_eq!(fat.lance_url().as_str(), "s3://my-pond/sub");
        assert_eq!(
            fat.scheme_options,
            vec![
                ("allow_http", "false".to_owned()),
                ("virtual_hosted_style_request", "true".to_owned()),
                ("region", "us-east-1".to_owned()),
            ],
        );
        let resolved = fat.resolve(&BTreeMap::new()).unwrap();
        assert_eq!(
            opts(&resolved, "endpoint").as_deref(),
            Some("https://my-pond.nbg1.example.com"),
        );
        assert_eq!(opts(&resolved, "region").as_deref(), Some("us-east-1"));
        // s3+http on an IP host: allow_http flips, path-style auto-selected
        // (a bucket subdomain on an IP can't resolve), port survives.
        let plain = StorageUrl::parse("s3+http://127.0.0.1:9000/pond").unwrap();
        assert_eq!(plain.lance_url().as_str(), "s3://pond/");
        assert_eq!(plain.scheme_options[0], ("allow_http", "true".to_owned()));
        assert_eq!(
            plain.scheme_options[1],
            ("virtual_hosted_style_request", "false".to_owned()),
        );
        let resolved = plain.resolve(&BTreeMap::new()).unwrap();
        assert_eq!(
            opts(&resolved, "endpoint").as_deref(),
            Some("http://127.0.0.1:9000"),
        );
        // An explicit endpoint in `extra` is the escape hatch and wins.
        let mut pinned = BTreeMap::new();
        pinned.insert(
            "default".to_owned(),
            CredsSet {
                extra: [(
                    "endpoint".to_owned(),
                    "https://pinned.example.com".to_owned(),
                )]
                .into_iter()
                .collect(),
                ..CredsSet::default()
            },
        );
        let resolved = fat.resolve(&pinned).unwrap();
        assert_eq!(
            opts(&resolved, "endpoint").as_deref(),
            Some("https://pinned.example.com"),
        );
        // gs passthrough
        let gcs = StorageUrl::parse("gs://bucket/p").unwrap();
        assert_eq!(gcs.lance_url().as_str(), "gs://bucket/p");
        // az: account folds into options
        let azure = StorageUrl::parse("az://acct/container/p").unwrap();
        assert_eq!(azure.lance_url().as_str(), "az://container/p");
        assert_eq!(
            azure.scheme_options,
            vec![("account_name", "acct".to_owned())]
        );
        // tests-only schemes pass through untouched
        let shared = StorageUrl::parse("shared-memory://pond-test-x/").unwrap();
        assert_eq!(shared.lance_url().as_str(), "shared-memory://pond-test-x/");
    }

    #[test]
    fn storage_url_rejects_bad_shapes() {
        // RFC 3986 userinfo is a leak class, never accepted.
        let err = StorageUrl::parse("s3+https://user:pass@host/bucket")
            .expect_err("userinfo must be rejected")
            .to_string();
        assert!(
            err.contains("creds"),
            "error must name the alternative: {err}"
        );
        // Missing bucket.
        assert!(StorageUrl::parse("s3+https://host").is_err());
        assert!(StorageUrl::parse("az://acct").is_err());
        // Unknown scheme names the grammar.
        let err = StorageUrl::parse("ftp://host/x")
            .expect_err("ftp")
            .to_string();
        assert!(err.contains("s3+https"), "got: {err}");
        // Unrecognized query params die loudly.
        let err = StorageUrl::parse("s3://b/p?regoin=x")
            .expect_err("typo")
            .to_string();
        assert!(err.contains("regoin"), "got: {err}");
        // Query params on local / in-memory schemes die just as loudly -
        // no silent carry into the URL Lance opens.
        let err = StorageUrl::parse("memory://x?creds=y")
            .expect_err("memory query")
            .to_string();
        assert!(err.contains("query params"), "got: {err}");
        let err = StorageUrl::parse("file:///x?creds=y")
            .expect_err("file query")
            .to_string();
        assert!(err.contains("query params"), "got: {err}");
        // `?` in a bare path is a filename character, not a query.
        assert!(StorageUrl::parse("/tmp/a?b").is_ok());
    }

    #[test]
    fn storage_url_canonicalizes_ports_and_keeps_percent_encoding() {
        // Default port strips so scope matching can't split on `:443`.
        let with_port = StorageUrl::parse("s3+https://host:443/bucket/p").unwrap();
        let without = StorageUrl::parse("s3+https://host/bucket/p").unwrap();
        assert_eq!(with_port.canonical(), without.canonical());
        // Non-default port survives into the assembled endpoint.
        let odd = StorageUrl::parse("s3+https://host:8443/bucket").unwrap();
        let resolved = odd.resolve(&BTreeMap::new()).unwrap();
        assert_eq!(
            resolved.options.get("endpoint").map(String::as_str),
            Some("https://bucket.host:8443"),
        );
        // Percent-encoded prefix passes through to the Lance URL verbatim.
        let encoded = StorageUrl::parse("s3+https://host/bucket/pre%20fix").unwrap();
        assert_eq!(encoded.lance_url().as_str(), "s3://bucket/pre%20fix");
    }

    #[test]
    fn query_params_strip_and_apply_over_set_fields() {
        let mut creds = BTreeMap::new();
        creds.insert(
            "default".to_owned(),
            CredsSet {
                region: Some("from-set".to_owned()),
                virtual_hosted_style_request: Some(false),
                ..set(None)
            },
        );
        let url = StorageUrl::parse(
            "s3+https://host/bucket/p?region=from-query&virtual_hosted_style_request=true",
        )
        .unwrap();
        // Stripped before Lance sees the URL.
        assert_eq!(url.lance_url().as_str(), "s3://bucket/p");
        assert!(url.canonical().query().is_none());
        let resolved = url.resolve(&creds).unwrap();
        // Assembly precedence: scheme < set < query.
        assert_eq!(opts(&resolved, "region").as_deref(), Some("from-query"));
        assert_eq!(
            opts(&resolved, "virtual_hosted_style_request").as_deref(),
            Some("true"),
        );
        // virtual_hosted=true (query) -> the bucket rides in the endpoint host.
        assert_eq!(
            opts(&resolved, "endpoint").as_deref(),
            Some("https://bucket.host"),
        );
    }

    #[test]
    fn scope_matching_binds_by_longest_prefix_at_segment_boundaries() {
        let mut creds = BTreeMap::new();
        creds.insert("all".to_owned(), set(None));
        creds.insert("bucket".to_owned(), set(Some("s3+https://host/pond/")));
        creds.insert("deep".to_owned(), set(Some("s3+https://host/pond/sub")));

        let bind = |input: &str| {
            StorageUrl::parse(input)
                .unwrap()
                .resolve(&creds)
                .unwrap()
                .binding
        };
        // Longest match wins.
        assert_eq!(
            bind("s3+https://host/pond/sub/x"),
            CredsBinding::Set {
                name: "deep".to_owned(),
                via: BindVia::Scope
            },
        );
        assert_eq!(
            bind("s3+https://host/pond/other"),
            CredsBinding::Set {
                name: "bucket".to_owned(),
                via: BindVia::Scope
            },
        );
        // Segment boundary: `/pond` does not match `/pond-2`.
        assert_eq!(
            bind("s3+https://host/pond-2"),
            CredsBinding::Set {
                name: "all".to_owned(),
                via: BindVia::CatchAll
            },
        );
        // No cross-scheme normalization: the scoped sets don't match s3://.
        assert_eq!(
            bind("s3://pond/sub"),
            CredsBinding::Set {
                name: "all".to_owned(),
                via: BindVia::CatchAll
            },
        );
        // Default-port spelling matches the portless scope.
        assert_eq!(
            bind("s3+https://host:443/pond/x"),
            CredsBinding::Set {
                name: "bucket".to_owned(),
                via: BindVia::Scope
            },
        );
        // `?creds=` pointer beats every scope...
        assert_eq!(
            bind("s3+https://host/pond/sub/x?creds=all"),
            CredsBinding::Set {
                name: "all".to_owned(),
                via: BindVia::Pointer
            },
        );
        // ...and a pointer to a missing set is an error, not a fallback.
        let err = StorageUrl::parse("s3://b/p?creds=nope")
            .unwrap()
            .resolve(&creds)
            .expect_err("missing set")
            .to_string();
        assert!(err.contains("creds=nope"), "got: {err}");

        // No sets at all -> ambient chain; local URLs skip resolution.
        let empty = BTreeMap::new();
        assert_eq!(
            StorageUrl::parse("s3://b/p")
                .unwrap()
                .resolve(&empty)
                .unwrap()
                .binding,
            CredsBinding::Ambient,
        );
        assert_eq!(
            StorageUrl::parse("/srv/pond")
                .unwrap()
                .resolve(&creds)
                .unwrap()
                .binding,
            CredsBinding::NotApplicable,
        );
    }

    #[test]
    fn unmatched_sets_are_reported_only_on_remote_invocations() {
        let mut creds = BTreeMap::new();
        creds.insert("used".to_owned(), set(Some("s3://bucket/")));
        creds.insert("idle".to_owned(), set(Some("s3://other/")));

        let remote = StorageUrl::parse("s3://bucket/p")
            .unwrap()
            .resolve(&creds)
            .unwrap();
        assert_eq!(unmatched_creds_sets(&[&remote], &creds), vec!["idle"]);

        // A purely local invocation must not nag about remote-only sets.
        let local = StorageUrl::parse("/srv/pond")
            .unwrap()
            .resolve(&creds)
            .unwrap();
        assert!(unmatched_creds_sets(&[&local], &creds).is_empty());
    }

    #[test]
    fn secrets_materialize_from_file_and_command() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("key");
        std::fs::write(&key_path, "from-file\n").unwrap();
        let mut creds = BTreeMap::new();
        creds.insert(
            "default".to_owned(),
            CredsSet {
                access_key_id_file: Some(key_path),
                // Two trailing newlines: exactly one is stripped.
                secret_access_key_command: Some("printf 'from-command\\n\\n'".to_owned()),
                ..CredsSet::default()
            },
        );
        let url = StorageUrl::parse("s3://bucket/p").unwrap();
        let resolved = url.resolve(&creds).unwrap();
        assert_eq!(
            opts(&resolved, "access_key_id").as_deref(),
            Some("from-file")
        );
        assert_eq!(
            opts(&resolved, "secret_access_key").as_deref(),
            Some("from-command\n"),
        );

        // A failing command surfaces its text and exit status.
        let mut failing = BTreeMap::new();
        failing.insert(
            "default".to_owned(),
            CredsSet {
                secret_access_key_command: Some("exit 3".to_owned()),
                ..CredsSet::default()
            },
        );
        let err = url
            .resolve(&failing)
            .expect_err("command must fail")
            .to_string();
        assert!(err.contains("exit 3"), "got: {err}");

        // The command cache: one subprocess per command text per process.
        let marker = dir.path().join("runs");
        let command = format!("echo run >> {} && echo secret", marker.display());
        let mut counted = BTreeMap::new();
        counted.insert(
            "default".to_owned(),
            CredsSet {
                secret_access_key_command: Some(command),
                ..CredsSet::default()
            },
        );
        url.resolve(&counted).unwrap();
        url.resolve(&counted).unwrap();
        let runs = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(runs.lines().count(), 1, "command must run exactly once");
    }

    #[test]
    fn check_errors_classify_by_kind_and_binding() {
        let auth_error = || object_store::Error::Unauthenticated {
            path: "k".to_owned(),
            source: "denied".into(),
        };
        let bound = CredsBinding::Set {
            name: "work".to_owned(),
            via: BindVia::Scope,
        };
        // Auth-class error with a bound set names the set...
        match classify_check_error(auth_error(), &bound, "put") {
            CheckFailure::Auth { set, .. } => assert_eq!(set, "work"),
            other => panic!("want Auth, got {other:?}"),
        }
        // ...and without one, points at the (empty) ambient chain.
        assert!(matches!(
            classify_check_error(auth_error(), &CredsBinding::Ambient, "put"),
            CheckFailure::NoCreds { .. },
        ));
        let denied = object_store::Error::PermissionDenied {
            path: "k".to_owned(),
            source: "403".into(),
        };
        assert!(matches!(
            classify_check_error(denied, &bound, "put"),
            CheckFailure::Auth { .. },
        ));
        // Anything else is I/O, set or no set.
        let missing = object_store::Error::NotFound {
            path: "k".to_owned(),
            source: "404".into(),
        };
        assert!(matches!(
            classify_check_error(missing, &bound, "get"),
            CheckFailure::Io { .. },
        ));
        // Lance wraps an empty-creds chain as a `Generic` error, never the
        // typed `Unauthenticated`; the rendered `CredentialsNotLoaded` is the
        // signal. Bound -> Auth (the set is wrong), unbound -> NoCreds.
        let no_creds = || object_store::Error::Generic {
            store: "S3",
            source: "Failed to get AWS credentials: CredentialsNotLoaded".into(),
        };
        assert!(matches!(
            classify_check_error(no_creds(), &bound, "put"),
            CheckFailure::Auth { .. },
        ));
        assert!(matches!(
            classify_check_error(no_creds(), &CredsBinding::Ambient, "put"),
            CheckFailure::NoCreds { .. },
        ));
    }

    #[test]
    fn concise_cause_strips_upstream_noise_to_one_line() {
        // The shape Lance actually produces: bug-report boilerplate, the real
        // cause, an internal source location, then the same text re-printed.
        let inner = "Encountered internal error. Please file a bug report at \
                     https://github.com/lance-format/lance/issues. Failed to get AWS \
                     credentials: CredentialsNotLoaded, <WORKSPACE>/src/object_store/providers/aws.rs:401:21: \
                     Encountered internal error. Please file a bug report at \
                     https://github.com/lance-format/lance/issues. Failed to get AWS \
                     credentials: CredentialsNotLoaded";
        let failure = CheckFailure::NoCreds {
            source: anyhow!(inner.to_owned()).context("initial conditional put"),
        };
        let cause = failure.concise_cause().expect("auth-class carries a cause");
        assert_eq!(cause, "Failed to get AWS credentials: CredentialsNotLoaded");
        // Display carries only the fix-naming lead, no chain.
        assert!(
            !failure.to_string().contains("file a bug report"),
            "lead must not trail the chain: {failure}"
        );
        // OccUnsupported's detail is already curated into Display.
        let occ = CheckFailure::OccUnsupported {
            detail: "put-if-none-match ignored".to_owned(),
        };
        assert!(occ.concise_cause().is_none());
        // Oversized single-line causes middle-truncate, keeping the tail
        // (wrapped transport errors put the root cause at the end).
        let long = CheckFailure::Io {
            source: anyhow!(format!("{} dns error: lookup failed", "x".repeat(500))),
        };
        let cause = long.concise_cause().expect("io carries a cause");
        assert!(cause.contains(" ... "), "long causes truncate: {cause}");
        assert!(
            cause.ends_with("dns error: lookup failed"),
            "the tail survives: {cause}"
        );
    }

    #[tokio::test]
    async fn storage_check_passes_on_memory_backend() {
        let resolved = StorageUrl::parse("memory://check/probe")
            .unwrap()
            .resolve(&BTreeMap::new())
            .unwrap();
        storage_check(&resolved).await.expect("memory probe passes");
    }

    fn stat(bytes: u64) -> FragmentStat {
        FragmentStat {
            bytes: Some(bytes),
            rows: bytes / 1_000,
            deleted_rows: 0,
        }
    }

    #[test]
    fn compaction_veto_blocks_absorb_keeps_peers() {
        // One 665 MiB tail fragment + tiny appends -> vetoed.
        let absorb = [stat(665_000_000), stat(1_000_000), stat(2_000_000)];
        assert!(!keep_task(&absorb, 64, 0.1));
        // Peer-sized merge halves fragment count -> kept.
        let peers = [stat(300_000_000), stat(300_000_000)];
        assert!(keep_task(&peers, 64, 0.1));
        // Remainder reaches largest / COMPACTION_ABSORB_FACTOR -> kept.
        let tiered = [stat(400_000), stat(60_000), stat(40_000)];
        assert!(keep_task(&tiered, 64, 0.1));
    }

    #[test]
    fn compaction_veto_passes_deletions_and_cap() {
        let mut deleting = stat(665_000_000);
        deleting.deleted_rows = deleting.rows / 5;
        assert!(keep_task(&[deleting, stat(1_000)], 64, 0.1));

        let wide: Vec<FragmentStat> = std::iter::once(stat(665_000_000))
            .chain(std::iter::repeat_with(|| stat(1_000)).take(63))
            .collect();
        assert!(keep_task(&wide, 64, 0.1));
    }

    #[test]
    fn compaction_veto_falls_back_to_rows_on_unknown_sizes() {
        let mut unknown = stat(665_000_000);
        unknown.bytes = None;
        // Rows comparison: 665k vs 3k -> still vetoed.
        assert!(!keep_task(
            &[unknown, stat(1_000_000), stat(2_000_000)],
            64,
            0.1
        ));
    }

    #[test]
    fn derived_target_rows_tracks_row_size_and_clamps() {
        // ~1.3 KiB rows -> ~200k-row target.
        let parts_like = [FragmentStat {
            bytes: Some(665_000_000),
            rows: 511_000,
            deleted_rows: 0,
        }];
        let target = derived_target_rows(&parts_like);
        assert!((150_000..300_000).contains(&target), "{target}");
        // No usable sizes -> Lance default.
        let unknown = [FragmentStat {
            bytes: None,
            rows: 511_000,
            deleted_rows: 0,
        }];
        assert_eq!(
            derived_target_rows(&unknown),
            MAX_TARGET_ROWS_PER_FRAGMENT as usize
        );
        // Tiny rows clamp at the ceiling, huge rows at the floor.
        let tiny = [FragmentStat {
            bytes: Some(1_000_000),
            rows: 100_000,
            deleted_rows: 0,
        }];
        assert_eq!(
            derived_target_rows(&tiny),
            MAX_TARGET_ROWS_PER_FRAGMENT as usize
        );
        let huge = [FragmentStat {
            bytes: Some(1_000_000_000),
            rows: 100,
            deleted_rows: 0,
        }];
        assert_eq!(
            derived_target_rows(&huge),
            MIN_TARGET_ROWS_PER_FRAGMENT as usize
        );
    }

    #[test]
    fn namespace_error_code_walks_wrapped_chain() {
        let direct = lance::Error::namespace_source(Box::new(NamespaceError::TableNotFound {
            message: "missing".into(),
        }));
        assert!(is_namespace_error_code(&direct, ErrorCode::TableNotFound));

        let wrapped = lance::Error::namespace_source(Box::new(direct));
        assert!(is_namespace_error_code(&wrapped, ErrorCode::TableNotFound));

        let other_code =
            lance::Error::namespace_source(Box::new(NamespaceError::NamespaceNotFound {
                message: "nope".into(),
            }));
        assert!(!is_namespace_error_code(
            &other_code,
            ErrorCode::TableNotFound
        ));

        let not_namespace = lance::Error::internal("unrelated");
        assert!(!is_namespace_error_code(
            &not_namespace,
            ErrorCode::TableNotFound
        ));
    }

    /// Round-trip: opening a fresh data dir through `lance-namespace`
    /// produces all three tables, and `Handle::scan` returns an empty batch
    /// for each (no spurious schema mismatch, no namespace error).
    #[tokio::test]
    async fn store_opens_via_namespace_and_scan_works() -> Result<()> {
        let temp = TempDir::new()?;
        let url = Url::from_directory_path(temp.path())
            .map_err(|()| anyhow::anyhow!("temp path is not absolute"))?;
        let handle = Handle::open(&url).await?;
        // Each table has its own PK column; project the canonical one so the
        // scan is exercised end-to-end (catalog -> dataset -> scanner -> batch).
        let cases: [(Table, &[&str]); 3] = [
            (Table::Sessions, &["id"]),
            (Table::Messages, &["id"]),
            (Table::Parts, &["id"]),
        ];
        for (table, projection) in cases {
            let scanner = handle
                .scan(table, ScanOpts::project_only(projection))
                .await?;
            let batch = scanner.try_into_batch().await?;
            assert_eq!(batch.num_rows(), 0, "fresh table should be empty");
        }
        Ok(())
    }
}
