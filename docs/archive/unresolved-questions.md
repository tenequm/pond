# Unresolved questions

Surfaced during the 2026-05-07 design review against `docs/references/{opencode, kilocode, pi-mono, otel-genai-semconv.md}`. Items already tracked in `design.md` section 19 are not duplicated here.

Each entry: question, why it matters, working recommendation. None are blocking the v1 stack, but each should be answered before schema lock.

---

## U1. S3 concurrency: "no external coordinator" claim is incomplete

**Where**: `design.md` section 9 ("Concurrency model") and section 17 ("Stateless workers, Lance OCC for concurrency, no external coordinator").

**Issue**: LanceDB docs (`storage/index.mdx`) state plainly: "Plain S3 lacks atomic writes, so multiple writers against the same table need an external commit coordinator. LanceDB ships a DynamoDB-backed coordinator behind the `s3+ddb://` URI scheme." The "no external coordinator" claim only holds on local FS, GCS, and Azure. AWS hosted is special.

**Why it matters**: hosted pond on AWS S3 with multiple MCP processes writing the same namespace will silently corrupt manifests without `s3+ddb://`.

**Working recommendation**: pick one:
- (a) require `s3+ddb://` for hosted on AWS; document the DynamoDB table contract (hash key `base_uri` (string), range key `version` (number)) and IAM permissions in section 9.
- (b) constrain to single-writer-per-table on AWS via a higher-level lease; treat multi-writer as GCS/Azure-only.
- (c) recommend GCS/Azure as the default hosted backend.

**Decision needed before**: any AWS hosted deployment.

---

## U2. ToolCallPart: user-defined vs provider-defined need a discriminator

**Where**: `design.md` section 6 ("ToolCallPart covers both user-defined tool calls (with handlers) and provider-defined tool calls (e.g. OpenAI WebSearch, executed server-side, no handler) - pond stores both identically").

**Issue**: OTel GenAI semconv treats these as separate discriminator values: `tool_call` (function call the runtime owes a result for) vs `server_tool_call` (provider executed it server-side, no result expected). "Stored identically" is fine; "queried identically" is not - replay logic and conformance tests need to know which calls are orphaned-by-design.

**Working recommendation**: keep one Part type but add a `tool_type` field on `ToolCallPart` mirroring OTel's `gen_ai.tool.type` registry: `function` (user-defined), `server` (provider-side), `mcp` (MCP server), `extension` (other). Default `function`. No new variant in the union.

**Decision needed before**: ReplayProvider seam impl (section 8).

---

## U3. Token usage columns: provider-faithful split

**Where**: `design.md` section 7 (`messages.lance`: "role, model, provider, tokens, cost, finish_reason, parent_message_id").

**Issue**: OTel GenAI splits `gen_ai.usage.input_tokens` / `output_tokens`, and notes that **cache-token semantics differ per provider** (Anthropic reports cache tokens separately and they MUST be added to `input_tokens`; OpenAI/Azure include them already). A single `tokens` integer loses provider-faithful accounting and breaks any future cost-recompute over historical sessions.

**Working recommendation**: model `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_creation_tokens` as separate columns on `messages.lance`. All nullable. Mirror OTel attribute names so a future tracing/observability layer is a column rename, not a schema change.

**Decision needed before**: schema lock.

---

## U4. Provider value registry: adopt OTel's closed list

**Where**: `design.md` section 7 (`messages.lance` carries a `provider` column; section 6 mentions Anthropic + OpenAI + Bedrock + Gemini).

**Issue**: OTel `gen_ai.provider.name` is a closed registry of 15 vendors (`anthropic`, `openai`, `vertex_ai`, `bedrock`/`aws_bedrock`, `azure_ai_inference`, `azure_ai_openai`, `cohere`, `mistral`, `xai`, `groq`, `deepseek`, `ibm_watsonx_ai`, `vertex`, `vercel`). Pond inventing its own provider strings creates ambiguity (`bedrock` vs `aws-bedrock` vs `amazon-bedrock`).

**Working recommendation**: adopt OTel's `gen_ai.provider.name` registry verbatim as the allowed values for the `provider` column. Document the enum in section 6 alongside the canonical types.

**Decision needed before**: schema lock.

---

## U5. Index cache freshness: drop the manifest ETag plan, use `read_consistency_interval`

**Where**: `design.md` section 11 ("Index cache freshness for long-lived MCP processes: ETag check on dataset manifest before serving each query") and section 18 ("Index cache refresh policy - ETag check is the chosen mechanism").

**Issue**: LanceDB has a built-in `read_consistency_interval` connection option (`0` = check every read, `N` seconds = poll, unset = never auto-refresh). Same effect, no custom code, no ETag book-keeping.

**Working recommendation**: replace the ETag mechanism with `read_consistency_interval = 0` (or a small integer for hosted) on the LanceDB connection. Update sections 11 and 18.

**Decision needed before**: section 17 lock.

---

## U6. Lance file format version: resolve naming vs LanceDB knob

**Where**: `design.md` section 19.3 ("lock to `2.2+` for new datasets").

**Issue**: LanceDB's actual configuration knob is `new_table_data_storage_version` with values `legacy` and `stable`. The `2.2+` reference doesn't map cleanly. Blob v2 + Map type already work on `stable`.

**Working recommendation**: rewrite 19.3 in LanceDB's terms: "set `new_table_data_storage_version = stable` at connection level for all new tables; `legacy` only for compatibility with pre-0.10 readers." Keep the Blob API note (`{"lance-encoding:blob": "true"}` field metadata). Resolves 19.3.

**Decision needed before**: section 5 lock.

---

## U7. EmbeddingProvider seam: pond-native or thin wrapper over LanceDB's registry

**Where**: `design.md` section 8 (EmbeddingProvider seam, `bge-small-en-v1.5 via fastembed-rs`).

**Issue**: LanceDB ships an embedding-function registry with a Rust `EmbeddingFunction` trait, query-time auto-embed, and a `$var:` runtime-config pattern. A pond-native trait works but reinvents wheel and forfeits query-time auto-embed (`table.search("text string")` becomes a vector search transparently).

**Working recommendation**: define pond's `EmbeddingProvider` as a thin wrapper that registers the chosen function on the LanceDB connection at startup. Day 1 default still bge-small-en-v1.5 via fastembed-rs, but it lives behind LanceDB's registry rather than as a parallel system. Inherits query-time auto-embed for free.

**Decision needed before**: section 8 seam impl.

---

## U8. NamespaceResolver: use Lance's namespace clients instead of a custom trait

**Where**: `design.md` section 19.7 (working recommendation: "define NamespaceResolver as a Rust trait Day 1, ship single env-based default impl").

**Issue**: LanceDB already exposes two namespace client implementations: `dir` (filesystem/prefix) and `rest` (REST catalog). The seam pond actually needs is "choose a Lance namespace impl + how to derive the path from a request" - not a fully custom trait.

**Working recommendation**: define the seam as `fn(request) -> (lance_namespace_client, path: Vec<String>)`. Day 1 personal pond returns `dir` + `[]`. Day 2 hosted returns `dir` + `[tenant_id]` or `rest` + `[tenant_id]`. Cheaper than a custom trait, plays directly with section 7's "namespace = bucket prefix" claim. Resolves 19.7.

**Decision needed before**: hosted deploy planning.

---

## U9. Wire contract: anchor on OTel GenAI JSON schemas

**Where**: `design.md` section 13 (boundary rules) and section 15 (deferred: "Published wire-contract package - JSON Schemas, OpenAPI, MCP tool manifest as committed artifacts").

**Issue**: OTel `gen-ai-input-messages.json`, `gen-ai-output-messages.json`, `gen-ai-system-instructions.json`, `gen-ai-tool-definitions.json`, `gen-ai-retrieval-documents.json` are already-versioned, vendor-neutral payload shapes for the exact things pond stores. Inventing parallel JSON Schemas duplicates work.

**Working recommendation**: when activating section 15's wire-contract package, derive pond's published JSON Schemas from the OTel schemas (subset + additive harness extensions like `CompactionPart`, `RetryPart`). Saves design time; gives observability tooling free interop. Note this in section 15.

**Decision needed before**: first non-MCP facade.

---

## U10. Conformance test matrix: adopt pi-mono fixtures

**Where**: `design.md` section 3 hard requirements ("Cross-provider replay") and section 16 references.

**Issue**: `references/pi-mono/packages/ai/test/cross-provider-handoff.test.ts` covers 30+ provider/model pairs (Anthropic, Google, OpenAI, Azure, Bedrock, xAI, Groq, Mistral, ...) plus orphaned tool-call and image-tool-result fixtures. Pond's design cites these but doesn't plan to adopt them as actual tests.

**Working recommendation**: add an explicit acceptance gate in section 3: "Cross-provider replay must pass the pi-mono conformance matrix (port to Rust, same fixture set)." Without it, "lossless cross-provider replay" is unfalsifiable.

**Decision needed before**: ReplayProvider impl.

---

## U11. Streaming variants: now removed from §6, but live-write reactivation needs a model

**Where**: `design.md` section 6 (just edited) and section 15 ("Live-write MCP tools").

**Issue**: When section 15's live-write tools (`pond_commit`, `pond_session_open`) are activated, pond will see streaming events. opencode/kilocode unify them into a single Part with `time.start`/`time.end` + state. pi-mono keeps streaming events out of storage entirely (separate `AssistantMessageEvent` union, only the final assembled message is persisted).

**Working recommendation**: when activating live-write, follow pi-mono: streaming events do **not** enter `parts.lance`. Live-write writes the final message Part on completion, plus optional `StepStartPart` / `StepFinishPart` for sub-step accounting (already in §6's harness extensions). This preserves append-only semantics and keeps OCC contention rare.

**Decision needed before**: section 15 activation (not v1).
