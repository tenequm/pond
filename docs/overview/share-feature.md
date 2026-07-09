# `pond share`: architecture and implementation notes

Reference doc for the `pond share <session-id>` feature (spec'd in
`docs/plans/2607-09-share-sessions-public-link.md`). Captures how the feature maps onto pond's
existing code, and the non-obvious details a fresh read of the plan doc alone wouldn't surface.
Written before implementation started, so it stays useful even if the feature is picked up later
or by someone else.

## Why a static export, not a server

pond's data already lives in object storage (R2/S3/GCS). Rendering one session to a
self-contained HTML file and dropping it in a public bucket needs no new serving
infrastructure — a public bucket, R2 + custom domain, or any static host just works. This is
also what makes the feature cleanly pluggable: "what the file looks like" (`ShareRenderer`) and
"where it lands" (`SharePublisher`) are independent, swappable interfaces:

```
Session ──▶ ShareRenderer ──▶ ShareArtifact{bytes, content_type, ext} ──▶ SharePublisher ──▶ public URL
```

v1 ships exactly one of each: `HtmlRenderer` and `BucketPublisher`. Both are traits so a
`JsonRenderer` (hosted-viewer model) or a `GitHubPagesPublisher` slot in later without touching
call sites.

## Where it lives: `pub mod share` in the lib crate, not bin-only

pond has two kinds of CLI-adjacent modules. `watch`/`schedule`/`init` are declared as bare
`mod` in `src/main.rs` (main.rs:37-42) with a comment explaining why: "no library caller" — they
wrap OS-level daemon/scheduler integration that only the binary needs. `share` is different: its
core logic (render a session to HTML, write bytes to a bucket) depends only on library types
(`wire::SessionWithMessages`, `config::Config`, `substrate::StorageUrl`) and is generically
useful — testable as a unit, and, per the plan doc's own framing, meant to "extract cleanly into
a crate later." So `share` is `pub mod share;` in `src/lib.rs`, alongside `sessions`, `wire`,
`render`, `handlers`. `main.rs` only adds the thin CLI-parsing layer: a `Command::Share` variant
and a match arm that calls `pond::share::run(...).await?` — the same shape as
`Command::Copy => run_copy(...).await?`.

## The data path: `get_session`, not `session_view`

`pond_get`'s session scope (`handlers.rs:833-889`) calls `Store::session_view`, which is
deliberately paginated and budget-bounded (200KB, `handlers.rs:792`) for agent context windows —
non-text parts arrive as one-line `PartSummary`s, not full bodies, unless you scope to a single
`message_id`.

A share artifact needs the opposite: the whole session, every part in full. That's
`Store::get_session(session_id)` (sessions.rs:1364), which returns `SessionWithMessages { session,
messages: Vec<MessageWithParts> }` where each `MessageWithParts.parts: Vec<Part>` carries the
complete `PartKind` body — no pagination, no summarization. This is the same call `pond_export`
already uses (`handlers.rs:634-715`, the `pond copy --to *.jsonl` path) — its
`for message_with_parts in stored.messages { for part in message_with_parts.parts { ... } }` walk
is the shape the HTML renderer should mirror.

One consequence: `get_session` takes no `namespace` parameter, unlike `pond_get`'s request path.
So `pond share` doesn't need a `--namespace` flag at all — it would have nothing to plumb it
into.

## Rendering: port the design, not the code, from agentsview

`agentsview`'s Go static exporter (`internal/server/export.go`) is the reference for the HTML/CSS
shape — it's MIT-licensed and has a working "publish transcript as a public link" precedent
(`humaPublishSession`, Gist + htmlpreview.github.io). pond reimplements the *design* in Rust, not
the Go:

- **Theming**: CSS custom properties under `:root` for light values, a `:root.dark { ... }`
  override block, and a plain `<button onclick="document.documentElement.classList.toggle('dark')">`
  — no JS framework, no `prefers-color-scheme` media query, no persistence. It's a static,
  cacheable single file; agentsview's choice to keep theming to one inline toggle fits that model.
- **No templating engine.** Nothing in pond's dependency tree does templating (no askama/
  minijinja/tera), and `render.rs`'s own convention for the existing plain-text transcript
  renderer is to write straight into a `String` via `write!`/`writeln!` (see
  `render_get_transcript`, render.rs:272-423). The HTML renderer follows the same style — a
  `String` buffer plus a small hand-rolled `escape_html` (pond has no HTML-escaping crate either;
  agentsview does the same thing with Go's `html.EscapeString`).
- **Part-kind dispatch.** `render_part_full` (render.rs:493-554) is the existing exhaustive match
  over `PartKind` — the reference for what each variant needs, ported to emit HTML blocks instead
  of plain-text lines. Its `File` branch is instructive by omission: it only ever prints
  `[file <label>]` from `file_name`/`media_type` and never touches the `data` field. Actual
  image/file *content* rendering has zero precedent anywhere in pond — it's genuinely new code.

### Images: pond is lossless, agentsview isn't

agentsview deliberately drops image content — Amp's parser collapses any array whose first
element is `{"type":"image",...}` to the literal string `"[binary content]"`
(`internal/parser/amp.go:246-252`). pond keeps everything, so the renderer must actually render
images inline.

Images arrive as `PartKind::File { media_type: Some("image/..."), data: FileData, .. }` — there's
no distinct "image" part kind. `FileData` has three variants, and they need different handling:

- **`FileData::String(s)`** — already base64 text. Confirmed by tracing where this gets
  populated: `adapter/claude_code.rs:1105-1117`'s `file_part()` pulls
  `source.get("data").and_then(Value::as_str)` straight out of Anthropic's own image-block shape
  (`{"source":{"type":"base64","media_type":"image/png","data":"<base64>"}}`) into
  `FileData::String(bytes.to_owned())`. So for this variant, the renderer builds
  `data:{media_type};base64,{s}` directly — no re-encoding.
- **`FileData::Bytes(b)`** — raw bytes, needs `base64::engine::general_purpose::STANDARD.encode(b)`
  first (same `base64::Engine` import pattern already used in `transport.rs:177` for MCP resource
  blobs).
- **`FileData::Url(u)`** — an external reference. Can't be inlined without pond fetching it itself
  (out of scope for a renderer — no network calls), so it becomes a direct `<img src="{u}">`. This
  is a deliberate, documented exception to "fully self-contained": the artifact still makes zero
  *pond-initiated* fetches, but a viewer's browser will hit that URL.

Both `String` and `Bytes` are size-capped by `[share].max_inline_image_bytes` (default 2 MiB)
before encoding — over the cap renders a placeholder instead of the image, so one oversized
attachment can't balloon the artifact unboundedly.

Audio is explicitly deferred (blocked on a real voice-session fixture), but the dispatch is
reserved: it's the same `File` match arm, one level down (`audio/*` media type gets a "not yet
supported" placeholder today), so wiring in real `<audio>` rendering later needs no
restructuring.

## Config: `[share]` reuses the creds resolver, gets no env mirror

`[creds.<name>]` scope resolution (spec.md#creds-scope-match, implemented in
`StorageUrl::resolve`, substrate.rs:296-342) already does exactly what a separate publishing-creds
set needs: longest-prefix scope match, wins over a scope-less catch-all, wins over ambient SDK
credentials. `[creds.share]` is just one more entry in the existing
`Config.creds: BTreeMap<String, CredsSet>` — no new credential machinery, no new resolver.

One thing the original plan doc got wrong by analogy: it assumed `[share]` would get
`POND_SHARE_*` env vars, mirroring how `[storage]`/`[creds]` work. It doesn't work that way.
`env_mirror()` (config.rs:668-692) is a hand-written `figment` filter that recognizes exactly two
key shapes — `storage_path` and `creds_*` — and rejects everything else (deliberately: clap owns
its own `POND_*` vars like `POND_HOST`/`POND_PORT`, so an unfiltered prefix would misfire on
those). No other config section (`[embeddings]`, `[search]`, `[maintenance]`, `[runtime]`) is
env-mirrored either. `[share]` follows that majority convention and stays file-only in v1;
extending the filter is a real, separate change (regex, spec.md#storage-env-mirror doc update,
new tests) that can be added later without restructuring anything else.

## The one new failure mode: `Content-Type`

Every existing `object_store` write in pond is either Lance-internal (content-type is irrelevant
to a columnar format Lance itself reads back) or a JSONL export meant to be downloaded, not
rendered (`export_write`, substrate.rs:1963-1977; `storage_check`'s probe, substrate.rs:720-750).
Neither sets `object_store::Attribute::ContentType` on the `PutOptions`. A shared HTML page is the
first write in the codebase where that matters: without an explicit
`Content-Type: text/html`, R2/S3 defaults to `application/octet-stream`, and a browser opening the
public URL downloads the file instead of rendering it. `BucketPublisher::publish` sets this
explicitly — worth flagging in review since it's new, easy to silently get wrong, and only
manifests as a broken user-facing behavior (not a compile or test failure, if a test only checks
byte-for-byte content and not headers).

## ID generation: follow the one convention that already exists

No `ulid`/`nanoid`/slug generator exists anywhere in pond. The established pattern for a fresh
unique ID is `uuid::Uuid::now_v7()` (time-ordered UUIDv7), already used in
`substrate.rs:729` (storage-check probe key), `wire.rs:736` (`req_{uuid}` request ids), and
`transport.rs:892` (export filenames). `pond share` follows the same shape:
`format!("share_{}", Uuid::now_v7())`. Every invocation mints a fresh id — no dedupe, no
overwrite-policy question to resolve in v1, since collision is practically impossible. Adding a
content-hash id (for re-share dedupe) or a user-supplied `--id` later is purely additive.
