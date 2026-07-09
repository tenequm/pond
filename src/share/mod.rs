//! `pond share <session-id>`: render one session to a self-contained static
//! artifact and publish it to a public URL. Two independent trait seams -
//! [`ShareRenderer`] (session -> artifact bytes) and [`SharePublisher`]
//! (artifact -> public URL) - keep "what the file looks like" and "where it
//! lands" swappable without touching each other. See
//! `docs/overview/share-feature.md` for the full design rationale.

pub mod publish;
pub mod render;

use anyhow::{Context, Result, anyhow, bail};
use std::io::IsTerminal;

use crate::{
    config::{Config, ShareViewer},
    sessions::{SessionWithMessages, Store},
};

/// The rendered output of a [`ShareRenderer`], ready for a [`SharePublisher`]
/// to write verbatim.
pub struct ShareArtifact {
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub ext: String,
}

/// Session -> artifact bytes. Implementations must be fully self-contained
/// (no external fetches at render time) so the resulting artifact is a single
/// static file.
pub trait ShareRenderer {
    fn render(&self, session: &SessionWithMessages) -> Result<ShareArtifact>;
}

/// Artifact -> public URL. `id` is a caller-generated unique identifier
/// (no extension); implementations decide the storage key.
#[async_trait::async_trait]
pub trait SharePublisher {
    async fn publish(&self, id: &str, artifact: &ShareArtifact) -> Result<String>;
}

/// CLI-facing arguments for `pond share`, decoupled from clap so [`run`] stays
/// testable without a `Cli`/`Command` in scope.
pub struct ShareArgs {
    pub session_id: String,
    /// Ad-hoc publish destination, overriding `[share].bucket`. Reuses the
    /// same `StorageUrl`/`[creds.*]` resolution as every other storage
    /// address in pond - no special "matching scope required" rule.
    pub to: Option<String>,
    /// Overrides `[share].viewer` for this run.
    pub viewer: Option<ShareViewer>,
    /// Skip the interactive confirm prompt.
    pub yes: bool,
    /// Open the published URL in the default browser after publishing.
    pub open: bool,
}

/// No-op seam for a future secret-scrub pass (out of scope for v1 - see
/// docs/plans/2607-09-share-sessions-public-link.md#9). Called unconditionally
/// right after rendering so a real redaction implementation slots in here
/// without changing any call site.
fn redact(_artifact: &mut ShareArtifact) {}

/// `share_<uuidv7>` - the one ID-generation convention already in the
/// codebase (substrate.rs, wire.rs, transport.rs all use `Uuid::now_v7()`).
/// Every call mints a fresh id: no dedupe, no overwrite policy to resolve in
/// v1 (collision is practically impossible).
fn generate_id() -> String {
    format!("share_{}", uuid::Uuid::now_v7())
}

fn renderer_for(loaded: &Config, viewer: ShareViewer) -> Box<dyn ShareRenderer> {
    match viewer {
        ShareViewer::Html => Box::new(render::HtmlRenderer::with_max_inline_image_bytes(
            loaded
                .share
                .max_inline_image_bytes
                .unwrap_or(render::DEFAULT_MAX_INLINE_IMAGE_BYTES),
        )),
    }
}

/// Resolve the publish destination: `--to` (ad-hoc, falls back to the
/// resolved storage URL as the printed "public" URL when no
/// `[share].public_base_url` applies) or `[share].bucket` +
/// `[share].public_base_url` from config.
fn publisher_for(loaded: &Config, to: Option<&str>) -> Result<Box<dyn SharePublisher>> {
    let bucket = match to {
        Some(url) => url,
        None => loaded.share.bucket.as_deref().ok_or_else(|| {
            anyhow!(
                "no [share].bucket configured and no --to given; run `pond config schema` \
                 for an example [share] block, or pass --to <url>"
            )
        })?,
    };
    let public_base_url = to.is_none().then(|| loaded.share.public_base_url.clone()).flatten();
    match loaded.share.provider.unwrap_or_default() {
        crate::config::ShareProvider::Bucket => Ok(Box::new(publish::bucket::BucketPublisher::new(
            bucket,
            loaded.creds.clone(),
            public_base_url,
        )?)),
    }
}

fn open_in_browser(url: &str) -> Result<()> {
    let status = if cfg!(target_os = "macos") {
        std::process::Command::new("open").arg(url).status()
    } else if cfg!(target_os = "windows") {
        std::process::Command::new("cmd")
            .args(["/c", "start", "", url])
            .status()
    } else {
        std::process::Command::new("xdg-open").arg(url).status()
    }
    .context("failed to launch the browser")?;
    if !status.success() {
        bail!("browser launcher exited with {status}");
    }
    Ok(())
}

/// `pond share <session-id>`: load the session, render it, confirm, publish,
/// print the URL. `store` is the already-opened *data* store (where the
/// session lives) - distinct from the share bucket, which `publisher_for`
/// resolves separately from `[share]`/`--to`.
pub async fn run(store: &Store, loaded: &Config, args: ShareArgs) -> Result<()> {
    let Some(session) = store
        .get_session(&args.session_id)
        .await
        .with_context(|| format!("failed to load session {}", args.session_id))?
    else {
        bail!("session {} not found", args.session_id);
    };

    let viewer = args.viewer.unwrap_or(loaded.share.viewer);
    let renderer = renderer_for(loaded, viewer);
    let mut artifact = renderer.render(&session)?;
    redact(&mut artifact);

    if !args.yes {
        if !std::io::stdin().is_terminal() {
            bail!(
                "`pond share` publishes a full transcript to a public URL with no redaction; \
                 stdin is not a terminal, so confirm with `pond share {} --yes`",
                args.session_id
            );
        }
        crate::output::line(&crate::output::paint(
            "This publishes the full transcript to a public URL. No redaction is applied - \
             anything in the transcript, including secrets, becomes public.",
            crate::output::yellow(),
        ))?;
        let confirmed = cliclack::confirm("Publish this transcript to a public URL?")
            .initial_value(false)
            .interact()
            .context("share confirmation prompt failed; nothing published")?;
        if !confirmed {
            crate::output::line("share: cancelled; nothing published")?;
            return Ok(());
        }
    }

    let publisher = publisher_for(loaded, args.to.as_deref())?;
    let id = generate_id();
    let url = publisher.publish(&id, &artifact).await?;
    crate::output::line(&format!(
        "{} {}",
        crate::output::paint("share:", crate::output::dim()),
        url,
    ))?;

    if args.open {
        open_in_browser(&url)?;
    }
    Ok(())
}
