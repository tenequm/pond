---
title: Share a session via a public link (`pond share`)
date: 2026-07-09
status: planned
owner: TBD
branch: feat/no-embed-sync-and-watch (or a fresh feat/share branch)
inspiration: /Users/abhishek/Applications/pi-share-hf
supersedes: none
tags: [share, publishing, static-hosting, r2, traits, cli]
---

# Share a session via a public link

Add a `pond share <session-id>` command that publishes one session's transcript to a
**public link**. Inspired by `pi-share-hf` (which redacts traces and uploads them to a
Hugging Face dataset), but reshaped for pond: pond's data already lives in object storage
(R2), so sharing is a **static export**, not a running service.

## 0. Read first

- `docs/spec.md`: storage-url-grammar, creds-scope-match, storage-env-mirror, storage-configless.
- `src/config.rs`: `Config`, `StorageConfig`, `CredsSet`, the `[creds.<name>]` scope resolver, `DEFAULT_CONFIG_TOML` / `pond config schema`.
- `src/sessions.rs`: `Store`, `get_session` → `SessionWithMessages`; `session_view`.
- `src/handlers.rs`: `pond_get` (`GetResponse`/`GetSession`/`GetResult` in `src/wire.rs`) — the trimmed conversational view.
- `src/render.rs`: existing transcript rendering (reuse for HTML).
- `src/main.rs`: `enum Command`, `HELP_TEMPLATE`, the `match cli.command` dispatch, and the `help_template_lists_every_subcommand` test.
- `CLAUDE.md` (repo): strict lints — no `unwrap`/`expect`/`println!`/`todo!`/`unsafe`; use `pond::output` helpers.

## 1. Guiding principle: sharing is a static file, not a server

pond's data already sits in R2. The minimal, most robust way to share a trace is to render
one session into **a single self-contained static artifact** and drop it into a public
location. Serving is then free and stateless — a public R2 bucket, R2 + custom domain,
Cloudflare Pages, or GitHub Pages all just serve static files. No dynamic backend, no viewer
process to keep alive.

This model is also what makes the feature cleanly pluggable: "where the file lands" and
"what the file looks like" become two independent, swappable interfaces.

## 2. Architecture: two trait seams

```
Session ─▶ ShareRenderer ─▶ ShareArtifact{bytes, content_type, ext} ─▶ SharePublisher ─▶ public URL
            (viewer format)                                             (hosting provider)
```

### 2.1 `ShareRenderer` — session → artifact bytes

```rust
pub struct ShareArtifact {
    pub bytes: Vec<u8>,
    pub content_type: String, // e.g. "text/html; charset=utf-8"
    pub ext: String,          // e.g. "html"
}

pub trait ShareRenderer {
    fn render(&self, session: &SessionWithMessages) -> anyhow::Result<ShareArtifact>;
}
```

- **v1: `HtmlRenderer`** — a self-contained, styled HTML page (all CSS inlined, no external
  fetches — theme-aware light/dark).
  - **Port the design from agentsview's Go static exporter** — `internal/server/export.go`
    (`/Users/abhishek/Applications/agentsview`): an inline `html/template` (~200 lines) with
    embedded light/dark CSS, code-fence / inline-code / `[Thinking]` / tool-block formatting,
    and a proven "publish to a public URL" precedent (`humaPublishSession` → GitHub Gist +
    htmlpreview). It's MIT-licensed Go; **reimplement in Rust** (askama / minijinja, or a plain
    `format!` template) fed from pond's `GetResponse` / `SessionWithMessages`, reusing the
    block structure already in `render.rs`. Take the HTML/CSS structure + the Gist-publish idea;
    do not vendor Go code.
  - **Render images (pond is lossless — do NOT drop them).** agentsview deliberately discards
    image/binary parts (`"[binary content]"` placeholder in `internal/parser/*.go`). pond
    retains them, so the HTML renderer must render image parts inline — embed as `data:` URIs
    in `<img>` so the artifact stays self-contained (no external fetch, matches the "single
    static file" model). Handle: (a) image parts already inline as base64/bytes in Lance;
    (b) large images → size guard + a config cap (e.g. `[share].max_inline_image_bytes`),
    falling back to a placeholder above the cap so a shared page can't balloon unbounded.
- **later: `JsonRenderer`** — emit structured JSON and let a hosted viewer app render it;
  smaller artifacts, viewer improves retroactively. New impl, no rewrite. agentsview's tree-aware
  Markdown exporter (`generateExportMarkdownTree`) is the reference for a cheap `MarkdownRenderer`
  variant too.

**Open rendering item — audio (deferred, plan-only):** Claude voice sessions carry audio parts.
A later `HtmlRenderer` revision should render audio inline (self-contained `<audio>` with a
`data:` URI, same size-cap discipline as images) plus any transcript text. Blocked on a real
voice-session fixture (user will share one). Not in v1 scope — reserve the part-type dispatch in
the renderer so audio slots in beside text/tool/image without restructuring.

Config switch: `[share].viewer = "html" | "json"` (default `html`).

### 2.2 `SharePublisher` — artifact → public URL

```rust
pub trait SharePublisher {
    /// Write the artifact under `id` and return the public URL a browser can open.
    async fn publish(&self, id: &str, artifact: &ShareArtifact) -> anyhow::Result<String>;
}
```

- **v1: `BucketPublisher`** — writes `<id>.<ext>` to a public bucket via the `object_store`
  dep pond already has (R2 / S3 / GCS). Public URL = `{public_base_url}/{id}.{ext}`.
- **later: `GitHubPagesPublisher`** (commit to a `gh-pages` repo), `SelfHostedPublisher`
  (POST to a user server), `HostedPublisher` (pond.locker managed service). Same trait.

A tiny provider registry maps `[share].provider` → impl. Unimplemented providers return a
clean `anyhow::bail!("share provider '<x>' is not implemented yet")` — never `todo!`.

### 2.3 Why not a new crate yet

Start as a **`src/share/` module directory** (`mod.rs`, `render.rs`, `publish/{mod,bucket}.rs`).
It depends on `wire.rs`/`render.rs`/`sessions.rs`, which live in the main crate; pond is not a
Cargo workspace today, so a `crates/pond-share` would force a workspace restructure for no v1
benefit. The module is already fully trait-based and extracts cleanly into a crate later once
the provider set stabilizes.

## 3. Credentials: reuse the existing scope resolver (no new machinery)

pond binds `[creds.<name>]` to URLs by **scope prefix, longest match wins**. The user keeps
storage creds and publishing creds separate — this maps directly: give the share bucket its
own `[creds.<name>]` whose `scope` is the share-bucket URL. The existing resolver picks
`creds.share` for the share bucket and `creds.default` for the data bucket automatically.

```toml
[storage]                                  # data (unchanged)
path = "s3+https://acct.r2.cloudflarestorage.com/pond-data"

[creds.default]                            # storage creds
access_key_id     = "…"
secret_access_key = "…"

[share]                                    # NEW
provider        = "bucket"                 # switchable: bucket | github-pages | hosted
bucket          = "s3+https://acct.r2.cloudflarestorage.com/pond-shares"
public_base_url = "https://shares.example.com"   # what maps to that bucket publicly
# viewer        = "html"                   # switchable later: html | json
# max_inline_image_bytes = "2 MiB"         # per-image cap for inline data: URIs (placeholder above)

[creds.share]                              # SEPARATE publishing creds
scope             = "s3+https://acct.r2.cloudflarestorage.com/pond-shares"
access_key_id     = "…"
secret_access_key = "…"
```

Env mirror: expose `POND_SHARE_PROVIDER`, `POND_SHARE_BUCKET`, `POND_SHARE_PUBLIC_BASE_URL`
(+ optional `POND_SHARE_VIEWER`) consistent with the existing `POND_*` mirror.

## 4. The command

```
pond share <session-id> [--yes] [--to <url>] [--viewer html|json] [--open]
```

Flow:
1. Load config; resolve `[share]` (or `--to` override for an ad-hoc bucket URL).
2. `Store::get_session(id)` → `SessionWithMessages` (existing read path).
3. `ShareRenderer::render(&session)` → `ShareArtifact`.
4. **Confirm gate (v1 = confirm-only, no scrub):** print a clear warning
   ("This publishes the full transcript to a public URL. No redaction is applied — anything
   in the transcript, including secrets, becomes public.") and require `--yes` (or an
   interactive y/N when attached to a TTY). Leave a no-op `redact(&mut ShareArtifact)` seam so
   a later scrub pass slots in without restructuring.
5. Generate `id` (short random slug; use existing id util if present, else a small hex/ULID gen).
6. `SharePublisher::publish(id, &artifact)` → public URL.
7. Print the URL via `pond::output`; `--open` opens it in the browser.

## 5. Wiring `Command::Share` (three coordinated edits + snapshot)

The `help_template_lists_every_subcommand` test enforces all three:
1. `enum Command` in `src/main.rs`: add `Share { … }` with `#[command(...)]` doc/help/display_order.
2. `HELP_TEMPLATE` const: add the `share` line.
3. `match cli.command`: add the arm calling `share::run(...)`.
4. Add `mod share;` (bin-only module, like `watch`/`schedule`).
5. New snapshot `src/snapshots/pond__tests__help_share.snap`.

## 6. Files to touch

| File | Change |
|---|---|
| `src/config.rs` | `ShareConfig` + `[share]` block on `Config`, env mirror, defaults, `DEFAULT_CONFIG_TOML` example, validation |
| `src/share/mod.rs` (new) | `ShareArtifact`, `ShareRenderer`/`SharePublisher` traits, provider registry, `run()`, confirm + `redact()` seam, id-gen |
| `src/share/render.rs` (new) | `HtmlRenderer` (self-contained HTML) — ported from agentsview `internal/server/export.go` design, **with inline image rendering** (data: URIs) + reserved audio dispatch |
| `src/share/publish/{mod,bucket}.rs` (new) | `BucketPublisher` over `object_store` |
| `src/main.rs` | `Command::Share` variant + `HELP_TEMPLATE` line + match arm + `mod share;` |
| `src/snapshots/pond__tests__help_share.snap` (new) | help snapshot |
| `docs/spec.md` | document `pond share` verb + `[share]` config + creds-scope note |
| `README.md` / site | short usage section |

## 7. Tests

- insta help snapshot for `pond share --help`.
- insta snapshot of `HtmlRenderer` output for a fixture `SessionWithMessages` — include a
  fixture with an **image part** and assert it renders as an inline `data:` URI `<img>` (and
  that an oversized image falls back to the placeholder per `max_inline_image_bytes`).
- `BucketPublisher` against the in-memory `s3s` / `s3s-fs` S3 simulator already used in tests:
  publish → read back the object → assert bytes + content-type, and assert the returned URL is
  `{public_base_url}/{id}.html`.
- Config: `[share]` parses, env mirror overrides, and `creds.share` scope resolves to the share
  bucket while `creds.default` still resolves to the data bucket.

## 8. Dependencies

None new for v1 — `object_store` already covers R2. (A future `GitHubPagesPublisher` may pull
in `reqwest` or git; deferred.)

## 9. Explicitly out of scope for v1 (roadmap)

- JSON + hosted viewer (`JsonRenderer`) and a Markdown renderer (agentsview
  `generateExportMarkdownTree` as reference).
- **Audio rendering** for Claude voice sessions (inline `<audio>` data: URI + transcript) —
  blocked on a real voice-session fixture; renderer part-type dispatch is reserved for it now.
- `GitHubPagesPublisher` / `SelfHostedPublisher` / `HostedPublisher` (pond.locker managed
  storage + sync at scale for hassle-free setup).
- Secret redaction / scrub pass (the `redact()` seam is reserved for it).
- `pond share list` / `pond share rm <id>` management + expiry.
- Per-session OG images.
- Sharing a whole project / multi-session bundle.

## 10. Open questions

- ID scheme: random slug vs content hash (dedupe re-shares) vs user-supplied `--id`.
- Overwrite policy when the same session is shared twice (stable id vs new id each time).
- Whether `--to` ad-hoc publishing should require a matching `[creds.*]` scope or accept
  ambient credentials.
