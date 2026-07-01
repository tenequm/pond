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
use lance::dataset::optimize::{
    CompactionMode, CompactionOptions, commit_compaction, plan_compaction,
};
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
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_io::object_store::{
    ChainedWrappingObjectStore, ObjectStore, ObjectStoreParams, ObjectStoreRegistry,
    StorageOptionsAccessor, WrappingObjectStore, uri_to_url,
};
use lance_linalg::distance::MetricType;
use lance_namespace::LanceNamespace;
use lance_namespace::error::{ErrorCode, NamespaceError};
use lance_namespace::models::DescribeTableRequest;
use lance_namespace_impls::ConnectBuilder;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OnceCell};
use tokio_stream::StreamExt;
use url::Url;
/// Embedded-row count at which pond builds the IVF_SQ vector index on
/// `messages.vector` (spec.md#search). Below it, vector search runs a
/// brute-force flat scan - exact and fast at small and medium scale, and
/// IVF_SQ cannot train well on fewer vectors anyway.
pub const VECTOR_INDEX_ACTIVATION_ROWS: usize = 100_000;

/// Segment count at which an incremental index fold consolidates instead of
/// appending. Each `optimize_indices(append)` writes a new same-name segment
/// (lance `num_indices_to_merge=0`), and every vector/FTS query reads the
/// probed partition or token postings from *every* segment - so unbounded
/// delta growth multiplies per-query object-store round-trips. At this many
/// segments pond folds with `merge` to collapse them back into one.
pub const DELTA_MERGE_THRESHOLD: usize = 4;

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
    // Toward Lance's 1 h floor: fewer retained manifest versions = cheaper
    // remote open (spec.md#search). The append fast-path already curbs the
    // version churn that earlier forced a wider window.
    chrono::Duration::hours(1)
}

/// `pond sync` runs every few minutes; reclaiming old manifest versions on
/// every run pays the full version-log walk over S3 (~9 s measured on the real
/// corpus) to free roughly one version. Amortize by cleaning only when a
/// table's manifest version is a multiple of this many commits. Explicit
/// `pond optimize` and the one-shot `pond copy` keep interval 1 (clean every
/// run) so maintenance and durability moves are never skipped.
pub const DEFAULT_SYNC_CLEANUP_INTERVAL: u64 = 16;

/// `pond sync` defers a scalar (BTree/bitmap) index fold until its unindexed
/// tail reaches this many rows. Lance 7.0.0 ignores `OptimizeOptions::append()`
/// for scalar indexes and rewrites the whole index file on every fold
/// (O(index size), not O(delta)), so folding on every tiny sync pays a full
/// rewrite for a handful of new rows. Batching amortizes that rewrite; the
/// deferred tail stays correct for get/count/sql (they scan it) with scan cost
/// bounded by this cap, and vector/FTS still fold every run so search recall is
/// unaffected. `pond optimize`/`pond copy` fold every run (threshold `0`).
pub const DEFAULT_SYNC_SCALAR_FOLD_ROWS: usize = 50_000;

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
    /// Run `cleanup_old_versions` for a table only when its manifest version is
    /// a multiple of this (`1` = every optimize). The frequent `pond sync` path
    /// raises it so most syncs skip the version-log walk; see
    /// [`DEFAULT_SYNC_CLEANUP_INTERVAL`].
    pub cleanup_interval: u64,
    /// Defer a scalar (BTree/bitmap) index fold until its unindexed tail reaches
    /// this many rows; `0` folds every run. The frequent `pond sync` path raises
    /// it so most syncs skip the full scalar-index rewrite Lance 7.0.0 does on
    /// every fold; see [`DEFAULT_SYNC_SCALAR_FOLD_ROWS`].
    pub scalar_fold_row_threshold: usize,
}

impl MaintenancePolicy {
    /// Veto off: run every task Lance plans (the optimize tests assume this).
    pub fn always_compact() -> Self {
        Self {
            compaction_fragment_cap: 0,
            cleanup_older_than: default_cleanup_older_than(),
            cleanup_interval: 1,
            scalar_fold_row_threshold: 0,
        }
    }

    /// Amortize version cleanup over `interval` commits - the frequent
    /// `pond sync` path uses this so most syncs skip the version-log walk.
    #[must_use]
    pub fn with_cleanup_interval(mut self, interval: u64) -> Self {
        self.cleanup_interval = interval.max(1);
        self
    }

    /// Amortize the scalar-index fold over its unindexed tail - the frequent
    /// `pond sync` path uses this so most syncs skip the full scalar-index
    /// rewrite Lance 7.0.0 does on every fold.
    #[must_use]
    pub fn with_scalar_fold_row_threshold(mut self, threshold: usize) -> Self {
        self.scalar_fold_row_threshold = threshold;
        self
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

/// Candidacy/merge target: HALF the rows a [`TARGET_FRAGMENT_BYTES`] fragment
/// holds at the table's average row size. Compaction byte-caps every output
/// fragment at [`TARGET_FRAGMENT_BYTES`] (`max_bytes_per_file`), so deriving the
/// target at the FULL byte budget made `target == the largest fragment
/// compaction can produce`: no output could ever satisfy `physical_rows >=
/// target`, so the table was re-compacted every sync for a net-zero fragment
/// change (measured ~100-120s/sync on the remote store, 30->30 fragments).
/// Halving leaves 2x headroom so a byte-capped fragment lands comfortably above
/// the target and FREEZES, making compaction productive (merge small -> freeze
/// -> stop) instead of perpetual churn.
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
    (TARGET_FRAGMENT_BYTES / 2 / avg_row_bytes)
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
    /// params (FTS, scalars); IVF_SQ needs the row count to size partitions.
    pub params: IndexParamsKind,
}

/// When an [`IndexIntent`] should exist on disk.
#[derive(Debug, Clone)]
pub enum IndexTrigger {
    /// Build whenever the table has any rows. Used for FTS and scalar
    /// indices: there is no training cost worth delaying.
    OnAnyRows,
    /// Build when `count(<column> IS NOT NULL) >= threshold`. Used for the
    /// IVF_SQ vector index, which trains poorly on too few vectors.
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
    /// `InvertedIndexParams` with the word-level `simple` tokenizer plus
    /// English stemming, stop-words off (spec.md#search-language-neutral-index).
    /// Word retrieval beats character ngram ~2x on the real corpus at ~4x less
    /// index weight; substring/symbol lookup stays on the SQL `LIKE` /
    /// `contains_tokens` path, not here.
    InvertedFtsWord,
    /// `VectorIndexParams::with_ivf_sq_params` with cosine metric (e5 vectors
    /// are L2-normalized). 8-bit scalar quantization stores per-dimension codes
    /// in the index itself, so kNN computes distances from the prewarmed
    /// partition with no refine pass - PQ+refine instead re-reads ~k*factor
    /// exact vectors from the data files as scattered per-row GETs, the
    /// dominant per-query S3 request storm on a throttling remote store
    /// (spec.md#search). `max_iters` caps kmeans; partitions follow LanceDB's
    /// documented `num_rows // 4096` guidance, floored at one.
    IvfSqCosine { num_bits: u16, max_iters: usize },
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
            Self::Scalar(BuiltinIndexType::ZoneMap) => IndexType::ZoneMap,
            Self::Scalar(_) => IndexType::BTree,
            Self::InvertedFtsWord => IndexType::Inverted,
            Self::IvfSqCosine { .. } => IndexType::Vector,
        }
    }

    async fn build(&self, dataset: &Dataset) -> Result<Box<dyn lance::index::IndexParams>> {
        match self {
            Self::Scalar(kind) => Ok(Box::new(ScalarIndexParams::for_builtin(kind.clone()))),
            Self::InvertedFtsWord => Ok(Box::new(
                InvertedIndexParams::default()
                    .base_tokenizer("simple".to_owned())
                    .stem(true)
                    .remove_stop_words(false),
            )),
            Self::IvfSqCosine {
                num_bits,
                max_iters,
            } => {
                let count = dataset
                    .count_rows(Some("vector IS NOT NULL".to_owned()))
                    .await?;
                let partitions = count.checked_div(4096).unwrap_or(0).max(1);
                let mut ivf = IvfBuildParams::new(partitions);
                ivf.max_iters = *max_iters;
                let sq = SQBuildParams {
                    num_bits: *num_bits,
                    ..Default::default()
                };
                Ok(Box::new(VectorIndexParams::with_ivf_sq_params(
                    MetricType::Cosine,
                    ivf,
                    sq,
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
    /// Intra-index liveness, forwarded from Lance's `IndexBuildProgress`
    /// callbacks (FTS tokenize/copy, IVF train/shuffle/merge, BTree/Bitmap
    /// build stages). Fires many times per index between `PhaseStart` /
    /// `PhaseDone`; the spinner just overwrites its message each tick.
    IndexStage {
        table: Table,
        index: String,
        stage: String,
        completed: u64,
        total: Option<u64>,
        unit: String,
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

/// `Arc` rather than `Box` so the same callback can be cloned into the
/// `PondIndexProgress` Arc that Lance's `IndexBuildProgress` builder demands -
/// otherwise intra-index stage events have no path back to the CLI spinner.
pub type OptimizeProgressFn = Arc<dyn Fn(OptimizeEvent) + Send + Sync>;

fn emit(progress: Option<&OptimizeProgressFn>, event: OptimizeEvent) {
    if let Some(callback) = progress {
        callback(event);
    }
}

/// Bridges Lance's `IndexBuildProgress` async callbacks (`stage_start`,
/// `stage_progress`, `stage_complete`) into pond's `OptimizeEvent::IndexStage`
/// stream so the CLI spinner can show "fts tokenize_docs 1.4M / 2M rows"
/// instead of going dark for 10-20 minutes during a single `create_index` or
/// `optimize_indices` call. Remembers the active stage's `total` / `unit` so
/// `stage_progress` (which only carries `completed`) can render a full
/// fraction. Emissions are throttled to one every 100ms; FTS's per-batch
/// `stage_progress` calls would otherwise contend the spinner mutex.
struct PondIndexProgress {
    callback: OptimizeProgressFn,
    table: Table,
    index: String,
    state: std::sync::Mutex<PondIndexStageState>,
}

// `IndexBuildProgress` requires `Debug`; the `callback` field is
// `Arc<dyn Fn...>` which has no `Debug` impl, so derive doesn't apply.
impl std::fmt::Debug for PondIndexProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PondIndexProgress")
            .field("table", &self.table)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct PondIndexStageState {
    total: Option<u64>,
    unit: String,
    last_emit: Option<Instant>,
}

impl PondIndexProgress {
    fn new(callback: OptimizeProgressFn, table: Table, index: String) -> Arc<Self> {
        Arc::new(Self {
            callback,
            table,
            index,
            state: std::sync::Mutex::new(PondIndexStageState::default()),
        })
    }
}

#[async_trait::async_trait]
impl lance_index::progress::IndexBuildProgress for PondIndexProgress {
    async fn stage_start(&self, stage: &str, total: Option<u64>, unit: &str) -> lance::Result<()> {
        if let Ok(mut state) = self.state.lock() {
            state.total = total;
            state.unit = unit.to_owned();
            state.last_emit = Some(Instant::now());
        }
        (self.callback)(OptimizeEvent::IndexStage {
            table: self.table,
            index: self.index.clone(),
            stage: stage.to_owned(),
            completed: 0,
            total,
            unit: unit.to_owned(),
        });
        Ok(())
    }

    async fn stage_progress(&self, stage: &str, completed: u64) -> lance::Result<()> {
        let (total, unit) = {
            let Ok(mut state) = self.state.lock() else {
                return Ok(());
            };
            let now = Instant::now();
            if let Some(prev) = state.last_emit
                && now.duration_since(prev) < Duration::from_millis(100)
            {
                return Ok(());
            }
            state.last_emit = Some(now);
            (state.total, state.unit.clone())
        };
        (self.callback)(OptimizeEvent::IndexStage {
            table: self.table,
            index: self.index.clone(),
            stage: stage.to_owned(),
            completed,
            total,
            unit,
        });
        Ok(())
    }

    async fn stage_complete(&self, stage: &str) -> lance::Result<()> {
        let (total, unit) = {
            let Ok(state) = self.state.lock() else {
                return Ok(());
            };
            (state.total, state.unit.clone())
        };
        (self.callback)(OptimizeEvent::IndexStage {
            table: self.table,
            index: self.index.clone(),
            stage: stage.to_owned(),
            completed: total.unwrap_or(0),
            total,
            unit,
        });
        Ok(())
    }
}

fn lance_progress(
    progress: Option<&OptimizeProgressFn>,
    table: Table,
    index: &str,
) -> Arc<dyn lance_index::progress::IndexBuildProgress> {
    match progress {
        Some(callback) => PondIndexProgress::new(callback.clone(), table, index.to_owned()),
        None => Arc::new(lance_index::progress::NoopIndexBuildProgress),
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
/// Object-store defaults: latency to refill is per-page, so keep more in cache
/// than local - but bounded above the warm working set, not Lance's 6 GiB.
/// Post word-tokenizer FTS that set is ~450 MB (simple invert + IVF_SQ aux), so
/// 1 GiB holds both indices warm with headroom while capping the RSS ceiling.
const REMOTE_INDEX_CACHE_BYTES: usize = 1024 * 1024 * 1024;
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
    /// Freshness window applied to the lazily-opened `sessions` and `parts`
    /// datasets when they first open, matching the eager `messages` open's
    /// scheme-keyed `refresh_after`.
    lazy_refresh_after: Duration,
    /// Object-store wrapper (index disk cache + io-trace) applied on every
    /// dataset open, including the lazy sessions/parts opens and any re-open.
    index_wrapper: Option<Arc<dyn WrappingObjectStore>>,
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
    /// `sessions.lance` opens lazily, like `parts`: the search request path
    /// reads only `messages`. Writers (ingest), `pond status`, restore, and the
    /// daemon's background index-cache GC open it on first use.
    sessions: OnceCell<Mutex<CachedDataset>>,
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
    fn new(dataset: Dataset, refresh_after: Duration) -> Self {
        Self {
            dataset,
            last_refresh: Instant::now(),
            refresh_after,
        }
    }
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
/// captured from the cumulative `WriteStats` ticks plus pond's own OCC attempt
/// counter.
#[derive(Debug, Clone, Copy, Default)]
pub struct AppendStats {
    pub rows: u64,
    pub bytes_written: u64,
    pub files_written: u64,
    pub attempts: u32,
}

/// Monotonic high-water fold over the cumulative `WriteStats` ticks
/// `append_stream` receives. Lance restarts a stream's cumulative counters from
/// zero on each OCC retry, so `fetch_max` keeps the fold monotonic - a retry
/// contributes nothing until it passes the prior mark, making `AppendStats`
/// exact under retries.
#[derive(Default)]
struct WriteAccum {
    rows: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
    files: std::sync::atomic::AtomicU64,
}

impl WriteAccum {
    fn observe(&self, stats: &WriteStats) {
        use std::sync::atomic::Ordering::Relaxed;
        self.rows.fetch_max(stats.rows_written, Relaxed);
        self.bytes.fetch_max(stats.bytes_written, Relaxed);
        self.files.fetch_max(stats.files_written as u64, Relaxed);
    }
    fn rows(&self) -> u64 {
        self.rows.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn bytes(&self) -> u64 {
        self.bytes.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn files(&self) -> u64 {
        self.files.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Append-mode write params. Byte-sized fragments, not Lance's 90 GB default:
/// kilobyte rows would otherwise pack multi-GiB fragments that compaction
/// rewrites wholesale (see `TARGET_FRAGMENT_BYTES`). Reuses the create params so
/// appended fragments match the table's storage version / row-id mode.
fn append_write_params() -> WriteParams {
    let mut params = sessions::write_params_for_create();
    params.mode = WriteMode::Append;
    params.max_bytes_per_file = TARGET_FRAGMENT_BYTES as usize;
    params
}

impl Handle {
    /// Open without storage options or explicit cache caps. Backend-aware
    /// defaults from `[runtime]` apply.
    pub async fn open(location: &Url) -> Result<Self> {
        Self::open_with_options(location, HashMap::new(), RuntimeCaps::default()).await
    }

    /// Live size in bytes of the shared Lance session caches (index + metadata).
    /// Walks the caches, so it is not cheap - bench/diagnostic use only.
    pub fn lance_cache_bytes(&self) -> u64 {
        self.session.size_bytes()
    }

    /// Open with object-store options handed through to Lance verbatim, plus
    /// the resolved `[runtime]` cache caps. Object-store keys are the
    /// `object_store` crate's standard config names; pond does not parse them.
    /// Opening datasets never performs index work; index lifecycle lives under
    /// `Handle::optimize_table`. `sessions.lance` and `parts.lance` open lazily
    /// on first use.
    pub async fn open_with_options(
        location: &Url,
        storage_options: HashMap<String, String>,
        caps: RuntimeCaps,
    ) -> Result<Self> {
        Self::open_with_options_cached(location, storage_options, caps, None).await
    }

    /// Like [`Self::open_with_options`], plus an `_indices/*` disk cache rooted
    /// at `index_cache_dir` (caller supplies it, mirroring `ensure_rowmap`) so a
    /// fresh process skips the cold index load. Ignored for local-FS stores.
    pub async fn open_with_options_cached(
        location: &Url,
        mut storage_options: HashMap<String, String>,
        caps: RuntimeCaps,
        index_cache_dir: Option<PathBuf>,
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
        let index_wrapper = index_store_wrapper(location, index_cache_dir.as_deref());
        let handle = Self {
            datasets: DatasetSet {
                sessions: OnceCell::new(),
                messages: Mutex::new(CachedDataset::new(
                    open_or_create_via_ns(
                        &nm,
                        &nm_ident,
                        sessions::MESSAGES,
                        sessions::message_schema(),
                        &session,
                        &storage_options,
                        index_wrapper.clone(),
                    )
                    .await?,
                    refresh_after,
                )),
                parts: OnceCell::new(),
            },
            retry: RetryPolicy::default(),
            session,
            nm,
            nm_ident,
            storage_options,
            location: location.clone(),
            lazy_refresh_after: refresh_after,
            index_wrapper,
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

    /// The OCC write-commit seam (spec.md#lance-chokepoints-write): every write -
    /// `merge` and the append paths - runs through here. It takes the cached
    /// handle's lock, hands `execute` the latest dataset, commits the dataset
    /// `execute` returns, and keeps the cache coherent - all under retry.
    /// `execute` builds the table-specific builder, runs it, and returns the new
    /// dataset plus its own stats payload; it reruns per OCC attempt, so it owns
    /// what it needs. Write-type specifics (params, stats, tracing) stay with the
    /// caller.
    async fn write_committed<E, Fut, P>(&self, table: Table, execute: E) -> Result<P>
    where
        E: Fn(Arc<Dataset>) -> Fut,
        Fut: std::future::Future<Output = Result<(Dataset, P)>>,
    {
        self.write_committed_with(table, |_| true, execute).await
    }

    /// [`Self::write_committed`] with a retry gate (see
    /// [`Self::retry_lance_filtered`]). `merge_insert` is idempotent on retry
    /// (`WhenMatched::DoNothing` re-reads and no-ops), so it retries everything;
    /// the bare `Append` path passes [`is_commit_conflict`] so a post-commit
    /// transient fault surfaces rather than re-appending into a duplicate.
    async fn write_committed_with<E, Fut, P, R>(
        &self,
        table: Table,
        should_retry: R,
        execute: E,
    ) -> Result<P>
    where
        E: Fn(Arc<Dataset>) -> Fut,
        Fut: std::future::Future<Output = Result<(Dataset, P)>>,
        R: Fn(&anyhow::Error) -> bool,
    {
        self.retry_lance_filtered(table.label(), should_retry, || {
            let execute = &execute;
            async move {
                let mut cached = self.cached(table).await?.lock().await;
                let existing = cached.latest().await?;
                let (dataset, payload) = execute(Arc::new(existing)).await?;
                cached.replace(dataset);
                Ok(payload)
            }
        })
        .await
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
            .write_committed(table, |existing| {
                let batch = batch.clone();
                let when_matched = when_matched.clone();
                let when_not_matched = when_not_matched.clone();
                async move {
                    let schema = batch.schema();
                    let reader = RecordBatchIterator::new([Ok(batch)], schema);
                    let mut builder = MergeInsertBuilder::try_new(existing, Vec::new())?;
                    builder.when_matched(when_matched);
                    builder.when_not_matched(when_not_matched);
                    // pond presents each PK at most once per batch; FirstSeen keeps
                    // the first occurrence rather than failing (Lance's default).
                    builder.source_dedupe_behavior(SourceDedupeBehavior::FirstSeen);
                    // Cleanup is operator-driven via `pond optimize`; the per-commit
                    // auto hook would add a LIST per write on remote backends without
                    // changing the steady-state retention.
                    builder.skip_auto_cleanup(true);
                    let (dataset, stats) = builder
                        .try_build()?
                        .execute_reader(Box::new(reader))
                        .await?;
                    Ok((dataset.as_ref().clone(), stats))
                }
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
    /// is one-shot, so an OCC retry rebuilds it. A single per-call `WriteAccum`
    /// (shared across attempts, NOT fresh per attempt) makes the row/byte/file
    /// fold exact under retries.
    ///
    /// Unlike [`Self::append_batches`] this keeps the retry-everything
    /// `write_committed`: a transient fault during the large streamed upload
    /// almost always precedes the manifest commit (the rebuilt source re-uploads
    /// and the orphaned fragments are GC'd, no duplicate), so failing a full
    /// bulk copy on every transient to close the narrow lost-ack-after-commit
    /// window is the wrong trade. That rare window is surfaced by the copy
    /// verify's duplicate check instead (spec.md#session-movement-complete).
    pub(crate) async fn append_stream<F, Fut>(
        &self,
        table: Table,
        make_source: F,
    ) -> Result<AppendStats>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<SendableRecordBatchStream>>,
    {
        let cum = Arc::new(WriteAccum::default());
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let started = Instant::now();
        self.write_committed(table, |existing| {
            let make_source = &make_source;
            let cum = cum.clone();
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let stream = make_source().await?;
                let dataset = InsertBuilder::new(existing)
                    .with_params(&append_write_params())
                    .progress(move |stats| cum.observe(&stats))
                    .execute_stream(stream)
                    .await?;
                Ok((dataset, ()))
            }
        })
        .await?;

        let attempts = attempts.load(std::sync::atomic::Ordering::Relaxed);
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

    /// [`Self::append_stream`] for batches pond already holds in memory (the sync
    /// write path) instead of a source-store scan. Row count is taken from the
    /// batches - exact under OCC retry without depending on the progress tick.
    ///
    /// Retries only on a commit *conflict*, not on transient faults: `Append`
    /// has no row-level idempotency, so re-running it after a manifest commit
    /// that landed but whose ack was lost would duplicate the rows. A conflict
    /// proves the commit did not land (re-append is safe); anything else
    /// surfaces and the caller's re-plan-from-current-state re-run heals it
    /// without doubling rows (spec.md#lance-deterministic-pk).
    pub(crate) async fn append_batches(
        &self,
        table: Table,
        batches: Vec<RecordBatch>,
    ) -> Result<AppendStats> {
        let total_rows: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
        if total_rows == 0 {
            return Ok(AppendStats::default());
        }
        let cum = Arc::new(WriteAccum::default());
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let started = Instant::now();
        self.write_committed_with(table, is_commit_conflict, |existing| {
            let cum = cum.clone();
            let attempts = attempts.clone();
            let batches = batches.clone();
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let dataset = InsertBuilder::new(existing)
                    .with_params(&append_write_params())
                    .progress(move |stats| cum.observe(&stats))
                    .execute(batches)
                    .await?;
                Ok((dataset, ()))
            }
        })
        .await?;

        let attempts = attempts.load(std::sync::atomic::Ordering::Relaxed);
        let stats = AppendStats {
            rows: total_rows,
            bytes_written: cum.bytes(),
            files_written: cum.files(),
            attempts,
        };
        tracing::info!(
            target: "pond::perf",
            op = "append_batches",
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
    /// Every index family folds incrementally via `optimize_indices`; none is
    /// rebuilt from scratch (spec.md#lance-index-maintenance).
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
            .run_optimize_indices_phase(table, intents, progress, policy.scalar_fold_row_threshold)
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
        // Threshold 0: this tail-fold path always folds scalar indexes; only
        // `pond sync` batches them (`with_scalar_fold_row_threshold`).
        self.run_optimize_indices_phase(table, intents, progress, 0)
            .await
    }

    async fn run_optimize_indices_phase(
        &self,
        table: Table,
        intents: &[IndexIntent],
        progress: Option<&OptimizeProgressFn>,
        scalar_fold_row_threshold: usize,
    ) -> PhaseOutcome {
        if intents.is_empty() {
            return PhaseOutcome::Noop;
        }
        let result = self
            .retry_lance(table.label(), || async {
                let mut guard = self.cached(table).await?.lock().await;
                let mut dataset = guard.latest().await?;
                let did_work = optimize_table_indices(
                    &mut dataset,
                    intents,
                    table,
                    progress,
                    scalar_fold_row_threshold,
                )
                .await?;
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

    pub async fn rebuild_index(
        &self,
        table: Table,
        intent: &IndexIntent,
        progress: Option<&OptimizeProgressFn>,
    ) -> Result<()> {
        emit(
            progress,
            OptimizeEvent::PhaseStart {
                table,
                phase: OptimizePhase::IndexRebuild,
                detail: Some(intent.name.to_owned()),
            },
        );
        let started = Instant::now();
        let result = self
            .retry_lance(table.label(), || async {
                let mut guard = self.cached(table).await?.lock().await;
                let mut dataset = guard.latest().await?;
                rebuild_index(&mut dataset, intent, progress, table).await?;
                guard.replace(dataset);
                Ok(())
            })
            .await;
        emit(
            progress,
            OptimizeEvent::PhaseDone {
                table,
                phase: OptimizePhase::IndexRebuild,
                elapsed_ms: started.elapsed().as_millis() as u64,
            },
        );
        result
    }

    /// Lance `cleanup_old_versions` for one table: reclaim files no manifest
    /// within the retention window references. No compaction and no new commit -
    /// it only deletes superseded files, so no OCC retry is needed.
    pub async fn cleanup_table_versions(
        &self,
        table: Table,
        older_than: chrono::Duration,
    ) -> Result<()> {
        let mut guard = self.cached(table).await?.lock().await;
        let dataset = guard.latest().await?;
        dataset
            .cleanup_old_versions(older_than, Some(false), Some(false))
            .await
            .with_context(|| format!("cleanup_old_versions failed for {}", table.label()))?;
        Ok(())
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

    /// Whether `messages` carries an index named `name`. Manifest-only and
    /// cache-backed (`load_indices` hits the dataset index cache), so it is
    /// cheap enough to gate `Scanner::fast_search` per query: fast-search
    /// returns an empty plan when the index is absent, so the retrievers must
    /// only opt in once it exists.
    pub(crate) async fn messages_has_index(&self, name: &str) -> Result<bool> {
        let dataset = self.dataset(Table::Messages).await?;
        let indices = dataset.load_indices().await?;
        Ok(indices.iter().any(|index| index.name == name))
    }

    /// Reclaim cached `_indices/<uuid>` dirs no longer referenced by any table's
    /// manifest. No-op for local stores or a never-populated cache. Best-effort:
    /// a new index version naturally re-fetches, so an over-eager prune only
    /// costs one re-download.
    pub(crate) async fn prune_index_cache(&self, cache_dir: &std::path::Path) {
        if config::is_local(&self.location) {
            return;
        }
        let root = cache_dir.join(store_key(&self.location)).join("indices");
        if !root.exists() {
            return;
        }
        let mut keep = std::collections::HashSet::new();
        for table in [Table::Sessions, Table::Messages, Table::Parts] {
            let Ok(dataset) = self.dataset(table).await else {
                return;
            };
            let Ok(indices) = dataset.load_indices().await else {
                return;
            };
            keep.extend(indices.iter().map(|index| index.uuid.to_string()));
        }
        prune_stale_uuid_dirs(&root, &keep);
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

    /// Which table owns the named index, if any. Used by
    /// `pond optimize --drop-index <name>` to route the drop to the right
    /// dataset without sequentially probing-and-swallowing errors (the prior
    /// loop hid permission/network failures behind "no such index"). Runs the
    /// three `load_indices` calls in parallel; an error here is a real I/O
    /// failure and propagates with context.
    pub(crate) async fn find_index_owner(&self, name: &str) -> Result<Option<Table>> {
        let list = |table: Table| async move {
            let dataset = self.dataset(table).await?;
            let names: Vec<String> = dataset
                .load_indices()
                .await
                .with_context(|| format!("load_indices failed for {}", table.label()))?
                .iter()
                .map(|index| index.name.clone())
                .collect();
            Ok::<_, anyhow::Error>(names)
        };
        let (sessions, messages, parts) = tokio::try_join!(
            list(Table::Sessions),
            list(Table::Messages),
            list(Table::Parts),
        )?;
        for (table, names) in [
            (Table::Sessions, sessions),
            (Table::Messages, messages),
            (Table::Parts, parts),
        ] {
            if names.iter().any(|n| n == name) {
                return Ok(Some(table));
            }
        }
        Ok(None)
    }

    /// Drop the named index. Used by the `pond optimize --force-embed` model-swap path
    /// to retire an IVF_SQ whose centroids belong to the old distance
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

    /// Whether the store holds synced data yet. `open` eagerly creates only the
    /// `messages` dataset; `sessions` and `parts` open lazily on first use
    /// (see `open_with_options`), so `parts`' presence is the "has been synced"
    /// signal - letting read-only surfaces (`pond status`) render an empty state
    /// instead of erroring on the first `parts` describe.
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
            Table::Sessions => self.sessions_cached().await,
            Table::Messages => Ok(&self.datasets.messages),
            Table::Parts => self.parts_cached().await,
        }
    }

    /// Open `sessions.lance` on first use (spec.md#datasets). The search request
    /// path reads only `messages`; the daemon's background index-cache GC
    /// (`prune_index_cache`) opens this on a `serve`/`mcp` process. Single-flight
    /// via `OnceCell`, like `parts`.
    async fn sessions_cached(&self) -> Result<&Mutex<CachedDataset>> {
        self.lazy_cached(
            &self.datasets.sessions,
            sessions::SESSIONS,
            sessions::session_schema,
        )
        .await
    }

    /// Open `parts.lance` on first use (spec.md#datasets). Single-flight via
    /// `OnceCell`; once initialized, behaves identically to the other two.
    async fn parts_cached(&self) -> Result<&Mutex<CachedDataset>> {
        self.lazy_cached(&self.datasets.parts, sessions::PARTS, sessions::part_schema)
            .await
    }

    /// Shared lazy-open path for the `sessions`/`parts` `OnceCell`s. `schema` is a
    /// thunk so the (local-CPU) schema build happens only on the cold init, not
    /// on every cache hit.
    async fn lazy_cached<'a>(
        &self,
        cell: &'a OnceCell<Mutex<CachedDataset>>,
        table_name: &str,
        schema: fn() -> lance::deps::arrow_schema::SchemaRef,
    ) -> Result<&'a Mutex<CachedDataset>> {
        cell.get_or_try_init(|| async {
            let dataset = open_or_create_via_ns(
                &self.nm,
                &self.nm_ident,
                table_name,
                schema(),
                &self.session,
                &self.storage_options,
                self.index_wrapper.clone(),
            )
            .await?;
            Ok::<_, anyhow::Error>(Mutex::new(CachedDataset::new(
                dataset,
                self.lazy_refresh_after,
            )))
        })
        .await
    }
    async fn retry_lance<T, Fut, Op>(&self, label: &str, operation: Op) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>>,
        Op: FnMut() -> Fut,
    {
        // Default: retry every transient fault (spec.md#lance-retry-jitter).
        self.retry_lance_filtered(label, |_| true, operation).await
    }

    /// Like [`Self::retry_lance`] but `should_retry` gates which errors are
    /// retried. [`Self::append_batches`] passes [`is_commit_conflict`]: a commit
    /// conflict means this writer's commit did NOT land, so re-running the
    /// operation is safe; any other error (notably a transient fault that may
    /// have arrived *after* the manifest commit landed - the lost-ack case) is
    /// surfaced instead of retried, because `Append` has no row-level
    /// idempotency and a blind re-append would duplicate. The caller's
    /// operation re-plans from current state on its own re-run, which is the
    /// idempotent recovery (spec.md#lance-deterministic-pk).
    async fn retry_lance_filtered<T, Fut, Op, R>(
        &self,
        label: &str,
        should_retry: R,
        mut operation: Op,
    ) -> Result<T>
    where
        Fut: std::future::Future<Output = Result<T>>,
        Op: FnMut() -> Fut,
        R: Fn(&anyhow::Error) -> bool,
    {
        let mut attempt = 0u8;
        loop {
            attempt = attempt.saturating_add(1);
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < self.retry.attempts && should_retry(&error) => {
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
        // Binary-copy eligible fragments (concatenate encoded pages, no
        // decode/re-encode) and fall back to Reencode automatically for blob
        // (parts), deletion-bearing, or schema-varied fragments. ~27% faster on
        // the messages/sessions reencode path, safe everywhere else.
        compaction_mode: Some(CompactionMode::TryBinaryCopy),
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
    //
    // Gated: the walk over the version log is round-trip-bound on object stores
    // (~9s measured on the real corpus) and reclaims ~one version per run, so
    // the frequent `pond sync` path amortizes it over `cleanup_interval`
    // commits rather than paying it every sync (`pond optimize`/`pond copy`
    // keep interval 1). Skipping only delays reclaiming old versions - the next
    // due cleanup sweeps the accumulated backlog - so it is always safe.
    if cleanup_due(dataset.version_id(), policy.cleanup_interval) {
        emit(
            progress,
            OptimizeEvent::PhaseStart {
                table,
                phase: OptimizePhase::Cleanup,
                detail: None,
            },
        );
        let started = Instant::now();
        // Lance v7 `cleanup_old_versions` removes orphan files inside
        // `_indices/<uuid>/` but does NOT remove the parent dir, so failed/no-op
        // index merges accumulate empty UUID dirs forever (one inode each).
        // Harmless beyond inode pressure; tracked upstream. No pond-side FS sweep
        // here (spec.md#concurrency: Lance-native maintenance only).
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
    }

    Ok(())
}

/// Gate for the version-cleanup walk: at interval `<= 1` it runs every optimize;
/// otherwise only when the manifest `version` is a multiple of it. A run whose
/// version steps past a multiple defers to the next one, so the gap between
/// cleanups is bounded and - since version 0 is a multiple of every interval -
/// cleanup always eventually fires; it is never skipped indefinitely.
fn cleanup_due(version: u64, interval: u64) -> bool {
    interval <= 1 || version.is_multiple_of(interval)
}

/// Indices phase: create absent indexes, then fold trailing fragments into
/// every existing index via batched `optimize_indices` (append, or merge once a
/// family's delta segments reach `DELTA_MERGE_THRESHOLD`). Returns `true` if
/// anything committed.
async fn optimize_table_indices(
    dataset: &mut Dataset,
    intents: &[IndexIntent],
    table: Table,
    progress: Option<&OptimizeProgressFn>,
    scalar_fold_row_threshold: usize,
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
                .create_index_builder(&[intent.column], index_type, params.as_ref())
                .name(intent.name.to_owned())
                .replace(false)
                .progress(lance_progress(progress, table, intent.name))
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

        // Fold every trailing fragment so the index is always current after an
        // optimize - no lag threshold. A tiny tail (rows written between this
        // fold and the next) stays invisible to `fast_search` queries until the
        // next fold, and the `DELTA_MERGE_THRESHOLD` cadence keeps the per-fold
        // segment from accumulating without bound (spec.md#search).
        let unindexed = dataset.unindexed_fragments(intent.name).await?;
        if unindexed.is_empty() {
            continue;
        }
        // Batch scalar folds: Lance 7.0.0 ignores `append()` for scalar and
        // rewrites the whole BTree/bitmap file on every fold (O(index size),
        // not O(delta)), so folding on every tiny sync is near-pure waste.
        // Defer until the unindexed tail is worth one rewrite. get/count/sql
        // read the deferred tail via scan (correct, only slower, bounded by the
        // threshold); vector/FTS below always fold, so search recall is
        // unaffected (spec.md#search). `pond optimize`/`pond copy` pass 0.
        if scalar_fold_row_threshold > 0 && matches!(intent.params, IndexParamsKind::Scalar(_)) {
            let tail_rows: usize = unindexed
                .iter()
                .map(|fragment| fragment.num_rows().unwrap_or(0))
                .sum();
            if tail_rows < scalar_fold_row_threshold {
                tracing::debug!(
                    target: "pond::perf",
                    index = intent.name,
                    tail_rows,
                    threshold = scalar_fold_row_threshold,
                    "deferring scalar index fold (unindexed tail below threshold)",
                );
                continue;
            }
        }
        // Every family folds incrementally via `optimize_indices` (the
        // append/merge batch below) - no full rebuild. BTree rewrites its index
        // file by merging the existing sorted pages with only the new fragments'
        // data; Bitmap/FTS/IVF_SQ accumulate delta segments. None re-scans
        // already-indexed source (spec.md#lance-index-maintenance).
        append_indices.push(intent.name.to_owned());
    }

    if !append_indices.is_empty() {
        // Per-index segment count from the manifest loaded above (delta segments
        // share the intent name). Indices that have piled up
        // `DELTA_MERGE_THRESHOLD` segments fold with `merge` (collapse to one);
        // the rest take the cheap append. Splitting keeps each query reading few
        // segments without paying a consolidation on every tiny fold.
        let segment_count = |name: &str| {
            existing
                .iter()
                .filter(|index| index.name.as_str() == name)
                .count()
        };
        let (to_merge, to_append): (Vec<String>, Vec<String>) = append_indices
            .iter()
            .cloned()
            .partition(|name| segment_count(name) >= DELTA_MERGE_THRESHOLD);

        emit(
            progress,
            OptimizeEvent::PhaseStart {
                table,
                phase: OptimizePhase::IndexAppend,
                detail: Some(append_indices.join(", ")),
            },
        );
        let started = Instant::now();
        if !to_append.is_empty() {
            dataset
                .optimize_indices(&OptimizeOptions::append().index_names(to_append))
                .await
                .context("optimize_indices(append) failed during index optimize")?;
        }
        if !to_merge.is_empty() {
            dataset
                .optimize_indices(
                    &OptimizeOptions::merge(DELTA_MERGE_THRESHOLD).index_names(to_merge),
                )
                .await
                .context("optimize_indices(merge) failed during index optimize")?;
        }
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
            "folded trailing fragments into indices",
        );
        did_work = true;
    }

    Ok(did_work)
}

async fn rebuild_index(
    dataset: &mut Dataset,
    intent: &IndexIntent,
    progress: Option<&OptimizeProgressFn>,
    table: Table,
) -> Result<()> {
    if !intent.trigger.should_create(dataset).await? {
        return Ok(());
    }
    let params = intent.params.build(dataset).await?;
    dataset
        .create_index_builder(
            &[intent.column],
            intent.params.index_type(),
            params.as_ref(),
        )
        .name(intent.name.to_owned())
        .replace(true)
        .progress(lance_progress(progress, table, intent.name))
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
/// Diagnostic S3 IO tracing. Inert unless [`io_trace::enable`] is called
/// before the store opens; then a shared `IOTracker` is injected as the
/// object-store wrapper on every dataset read open, counting exactly how many
/// GETs (and bytes, and - under the `io-trace` feature - which paths) each
/// query issues against a remote store. Used by `serve_mem_bench --io-trace`
/// to attribute the per-query S3 request load. Not a production code path.
pub mod io_trace {
    use lance_io::utils::tracking_store::{IOTracker, IoStats};
    use std::sync::{Arc, OnceLock};

    static TRACKER: OnceLock<IOTracker> = OnceLock::new();

    /// Arm tracing. Must run before the store opens so the wrapper is applied
    /// when the datasets' object store is built.
    pub fn enable() {
        let _ = TRACKER.set(IOTracker::default());
    }

    /// The shared tracker as an object-store wrapper, when armed.
    pub(super) fn wrapper() -> Option<Arc<IOTracker>> {
        TRACKER.get().map(|tracker| Arc::new(tracker.clone()))
    }

    /// Read and reset the IO accumulated since the last call.
    pub fn take() -> Option<IoStats> {
        TRACKER.get().map(IOTracker::incremental_stats)
    }
}

/// On-disk cache for `_indices/*` so a fresh process serves the IVF + FTS index
/// from local disk instead of re-loading it from the object store on every
/// cold-start (spec.md#search). Scoped to `_indices/*` because those files are
/// immutable and UUID-addressed, so a hit is always correct and a new index is
/// an automatic miss; data (served by the rowmap) and manifests (need freshness)
/// pass through. A `WrappingObjectStore`, so it stays inside the object-store
/// layer rather than reaching around it.
pub mod index_cache {
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
    };
    use std::collections::HashMap;
    use std::ops::Range;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use lance_io::object_store::WrappingObjectStore;

    fn is_index_path(location: &ObjPath) -> bool {
        AsRef::<str>::as_ref(location).contains("_indices/")
    }

    /// Drop conditional headers (etag/if-modified): they reference the remote
    /// object and would spuriously fail against the local cache copy.
    fn local_opts(options: &GetOptions) -> GetOptions {
        GetOptions {
            range: options.range.clone(),
            head: options.head,
            ..Default::default()
        }
    }

    /// `WrappingObjectStore` factory: holds the per-store cache root and hands a
    /// `CachingStore` to every dataset open on this store.
    #[derive(Debug)]
    pub struct IndexDiskCache {
        local: Arc<LocalFileSystem>,
        inflight: Arc<Mutex<HashMap<ObjPath, Arc<tokio::sync::Mutex<()>>>>>,
    }

    impl IndexDiskCache {
        /// `LocalFileSystem` requires the prefix to exist, so create it first.
        pub fn new(root: PathBuf) -> std::io::Result<Self> {
            std::fs::create_dir_all(&root)?;
            Ok(Self {
                local: Arc::new(LocalFileSystem::new_with_prefix(&root)?),
                inflight: Arc::new(Mutex::new(HashMap::new())),
            })
        }
    }

    impl WrappingObjectStore for IndexDiskCache {
        fn wrap(&self, _store_prefix: &str, inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
            Arc::new(CachingStore {
                inner,
                local: self.local.clone(),
                inflight: self.inflight.clone(),
            })
        }
    }

    #[derive(Debug)]
    struct CachingStore {
        inner: Arc<dyn ObjectStore>,
        local: Arc<LocalFileSystem>,
        inflight: Arc<Mutex<HashMap<ObjPath, Arc<tokio::sync::Mutex<()>>>>>,
    }

    impl std::fmt::Display for CachingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CachingStore({})", self.inner)
        }
    }

    impl CachingStore {
        fn flight_lock(&self, location: &ObjPath) -> Arc<tokio::sync::Mutex<()>> {
            self.inflight
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .entry(location.clone())
                .or_default()
                .clone()
        }

        /// Fetch the whole object once, write it (`LocalFileSystem::put` stages +
        /// renames atomically), then serve the requested range from the copy. The
        /// per-path single-flight coalesces a process's concurrent first reads of
        /// one file into a single fetch; cross-process writes race safely since
        /// the bytes are identical and the rename is atomic.
        async fn populate_and_serve(
            &self,
            location: &ObjPath,
            options: GetOptions,
        ) -> OsResult<GetResult> {
            let lock = self.flight_lock(location);
            let _guard = lock.lock().await;
            let result = self.fetch_under_flight(location, options).await;
            // Drop the entry so the map stays bounded as index versions churn.
            // Unconditionally safe (singleflight idiom): any waiter already holds
            // its own `lock` clone, and a later miss re-creates the entry but
            // finds the file cached.
            self.inflight
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(location);
            result
        }

        async fn fetch_under_flight(
            &self,
            location: &ObjPath,
            options: GetOptions,
        ) -> OsResult<GetResult> {
            if let Ok(result) = self.local.get_opts(location, local_opts(&options)).await {
                return Ok(result);
            }
            let bytes = self.inner.get(location).await?.bytes().await?;
            if self
                .local
                .put(location, PutPayload::from_bytes(bytes))
                .await
                .is_ok()
                && let Ok(result) = self.local.get_opts(location, local_opts(&options)).await
            {
                return Ok(result);
            }
            // Cache write or re-read failed (e.g. disk full): serve from origin.
            self.inner.get_opts(location, options).await
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CachingStore {
        async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
            if !is_index_path(location) {
                return self.inner.get_opts(location, options).await;
            }
            match self.local.get_opts(location, local_opts(&options)).await {
                Ok(result) => Ok(result),
                Err(object_store::Error::NotFound { .. }) => {
                    self.populate_and_serve(location, options).await
                }
                Err(_) => self.inner.get_opts(location, options).await,
            }
        }

        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_ranges(
            &self,
            location: &ObjPath,
            ranges: &[Range<u64>],
        ) -> OsResult<Vec<Bytes>> {
            if is_index_path(location) {
                // Through get_opts so the first touch caches the whole object.
                let mut out = Vec::with_capacity(ranges.len());
                for range in ranges {
                    let opts = GetOptions {
                        range: Some(range.clone().into()),
                        ..Default::default()
                    };
                    out.push(self.get_opts(location, opts).await?.bytes().await?);
                }
                return Ok(out);
            }
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<ObjPath>>,
        ) -> BoxStream<'static, OsResult<ObjPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&ObjPath>,
            offset: &ObjPath,
        ) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, opts: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    #[cfg(test)]
    mod tests {
        #![allow(clippy::unwrap_used)]
        use super::*;
        use object_store::memory::InMemory;

        async fn read(store: &Arc<dyn ObjectStore>, path: &ObjPath) -> Option<Vec<u8>> {
            store
                .get(path)
                .await
                .ok()?
                .bytes()
                .await
                .ok()
                .map(|b| b.to_vec())
        }

        #[tokio::test]
        async fn caches_index_files_and_passes_data_through() {
            let temp = tempfile::tempdir().unwrap();
            let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let index_path = ObjPath::from("d/messages.lance/_indices/uuid1/index.idx");
            let data_path = ObjPath::from("d/messages.lance/data/x.lance");
            inner
                .put(&index_path, PutPayload::from_static(b"INDEX"))
                .await
                .unwrap();
            inner
                .put(&data_path, PutPayload::from_static(b"DATA"))
                .await
                .unwrap();

            let cache = IndexDiskCache::new(temp.path().join("indices")).unwrap();
            let store = cache.wrap("test", inner.clone());

            assert_eq!(
                read(&store, &index_path).await.as_deref(),
                Some(&b"INDEX"[..])
            );
            assert_eq!(
                read(&store, &data_path).await.as_deref(),
                Some(&b"DATA"[..])
            );

            // Delete both from the origin. The index file is served from the
            // local cache; the data file (never cached) is now gone.
            inner.delete(&index_path).await.unwrap();
            inner.delete(&data_path).await.unwrap();
            assert_eq!(
                read(&store, &index_path).await.as_deref(),
                Some(&b"INDEX"[..])
            );
            assert_eq!(read(&store, &data_path).await, None);

            // A range read of the cached index slices the local copy.
            let slice = store.get_range(&index_path, 1..4).await.unwrap();
            assert_eq!(slice.as_ref(), b"NDE");
        }
    }
}

/// Stable filesystem-safe key for a store URL: same URL -> same key, so sibling
/// pond processes share one on-disk cache and distinct stores never collide.
/// Shared by the rowmap (`sessions.rs`) and the index disk cache.
pub(crate) fn store_key(location: &Url) -> String {
    blake3::hash(location.as_str().as_bytes()).to_hex()[..16].to_owned()
}

/// Reclaim cached `_indices/<uuid>` dirs whose UUID is not in `keep`. Recurses
/// to each `_indices` dir (the bucket prefix varies) and prunes its dead UUID
/// children. Best-effort; unlink-safe (POSIX keeps an in-flight reader's inode).
fn prune_stale_uuid_dirs(dir: &std::path::Path, keep: &std::collections::HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if entry.file_name() == "_indices" {
            let Ok(children) = std::fs::read_dir(&path) else {
                continue;
            };
            for child in children.flatten() {
                if child.path().is_dir()
                    && !keep.contains(child.file_name().to_string_lossy().as_ref())
                {
                    let _ = std::fs::remove_dir_all(child.path());
                }
            }
        } else {
            prune_stale_uuid_dirs(&path, keep);
        }
    }
}

/// The object-store wrapper applied to every dataset open: the `_indices/*`
/// disk cache (remote stores only, when a cache dir is supplied) chained with
/// the diagnostic io-trace wrapper. `None` when neither is active.
fn index_store_wrapper(
    location: &Url,
    index_cache_dir: Option<&std::path::Path>,
) -> Option<Arc<dyn WrappingObjectStore>> {
    let mut wrappers: Vec<Arc<dyn WrappingObjectStore>> = Vec::new();
    if let Some(dir) = index_cache_dir
        && !config::is_local(location)
    {
        let root = dir.join(store_key(location)).join("indices");
        match index_cache::IndexDiskCache::new(root) {
            Ok(cache) => wrappers.push(Arc::new(cache)),
            Err(error) => tracing::warn!(%error, "index disk cache disabled; reads hit the store"),
        }
    }
    if let Some(tracker) = io_trace::wrapper() {
        wrappers.push(tracker);
    }
    match wrappers.len() {
        0 => None,
        1 => Some(wrappers.remove(0)),
        _ => Some(Arc::new(ChainedWrappingObjectStore::new(wrappers))),
    }
}

async fn open_or_create_via_ns(
    nm: &Arc<dyn LanceNamespace>,
    nm_ident: &NamespaceIdent,
    table_name: &str,
    schema: lance::deps::arrow_schema::SchemaRef,
    session: &Arc<Session>,
    storage_options: &HashMap<String, String>,
    wrapper: Option<Arc<dyn WrappingObjectStore>>,
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
            match wrapper {
                Some(wrapper) => {
                    builder = builder.with_store_params(ObjectStoreParams {
                        object_store_wrapper: Some(wrapper),
                        storage_options_accessor: (!storage_options.is_empty()).then(|| {
                            Arc::new(StorageOptionsAccessor::with_static_options(
                                storage_options.clone(),
                            ))
                        }),
                        ..Default::default()
                    });
                }
                None => {
                    if !storage_options.is_empty() {
                        builder = builder.with_storage_options(storage_options.clone());
                    }
                }
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
    // `request_timeout` bounds a single object-store request (one range GET/PUT),
    // not a whole scan - a streaming read issues many small requests, each well
    // under this. We keep it deliberately tight as a HARD BARRIER: a single
    // request exceeding 60s means a design/infra problem to fix (chunk the read,
    // use change-data-feed, fix the endpoint), never something to paper over with
    // a longer timeout. An explicit `[storage]` override still wins.
    set_default(options, &["request_timeout"], "60 seconds");
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

    #[test]
    fn prune_keeps_live_uuid_dirs_and_drops_dead_ones() {
        let temp = TempDir::new().unwrap();
        let indices = temp.path().join("bkt/messages.lance/_indices");
        for uuid in ["live", "dead"] {
            std::fs::create_dir_all(indices.join(uuid)).unwrap();
            std::fs::write(indices.join(uuid).join("index.idx"), b"x").unwrap();
        }
        let keep = std::collections::HashSet::from(["live".to_owned()]);
        prune_stale_uuid_dirs(temp.path(), &keep);
        assert!(indices.join("live").exists());
        assert!(!indices.join("dead").exists());
    }

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
    fn cleanup_due_gates_on_version_interval() {
        // interval <= 1 always cleans (pond optimize / pond copy / tests).
        assert!(cleanup_due(0, 1));
        assert!(cleanup_due(7, 1));
        assert!(cleanup_due(5, 0));
        // interval N: only on multiples (the amortized pond sync path).
        assert!(cleanup_due(0, 16));
        assert!(cleanup_due(16, 16));
        assert!(cleanup_due(48, 16));
        assert!(!cleanup_due(15, 16));
        assert!(!cleanup_due(17, 16));
        assert!(!cleanup_due(31, 16));
    }

    #[test]
    fn derived_target_rows_tracks_row_size_and_clamps() {
        // ~1.3 KiB rows -> ~100k-row target (half the byte budget, for freeze
        // headroom over the 256 MiB output cap).
        let parts_like = [FragmentStat {
            bytes: Some(665_000_000),
            rows: 511_000,
            deleted_rows: 0,
        }];
        let target = derived_target_rows(&parts_like);
        assert!((80_000..150_000).contains(&target), "{target}");
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
