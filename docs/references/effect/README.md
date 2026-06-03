# effect reference snapshot

Two upstream sources, copied verbatim with original package-relative paths preserved.

| Source | Local clone | Commit | Snapshot date |
|--------|-------------|--------|---------------|
| https://github.com/Effect-TS/effect-smol (v4 experimental, the new home of `effect/unstable/ai`) | `~/pjv/effect-ts/effect-smol/` | `c5d7373d20e3daf0d9529271f2f632ed68f5b336` | 2026-05-08 |
| https://github.com/Effect-TS/effect (v3 stable, larger provider matrix) | `~/pjv/effect-ts/effect/` | `3585f25110fca7af6aeec3f59c3fc05b20c8d316` | 2026-05-08 |

Browse the upstream files at the exact snapshot commits:

- effect-smol tree: https://github.com/Effect-TS/effect-smol/tree/c5d7373d20e3daf0d9529271f2f632ed68f5b336
- effect tree: https://github.com/Effect-TS/effect/tree/3585f25110fca7af6aeec3f59c3fc05b20c8d316

## Why these files

Reference for the canonical `effect/unstable/ai` Prompt + Response part unions, tool / toolkit / MCP shapes, and per-provider mapping code that pond's specification points at as the moat-shape for its own Rust serde types (see `docs/spec.md#model`). Files preserve their original package-relative paths under this directory so the origin of every line is unambiguous.

Generated client files (`Generated.ts`) were excluded from every provider package - they are auto-emitted from upstream OpenAPI specs and are too large to be useful in a snapshot. The hand-authored mapping logic, telemetry, error types, and config were copied in full.

## Layout

```
effect-smol/   # v4 experimental - main reference
  packages/effect/src/unstable/ai/...   # canonical types
  packages/ai/anthropic/src/...          # provider mapping
  packages/ai/openai/src/...
  packages/ai/openai-compat/src/...
  packages/ai/openrouter/src/...

effect/        # v3 stable - cross-reference + extra providers
  packages/ai/ai/src/...                  # v3 canonical types (compare against v4)
  packages/ai/amazon-bedrock/src/...      # provider not yet ported to v4
  packages/ai/google/src/...              # provider not yet ported to v4
```

## What's in `effect-smol/packages/effect/src/unstable/ai`

Source of truth for the v4 `effect/unstable/ai` namespace.

| File | Purpose |
|------|---------|
| `index.ts` | Module barrel with extensive jsdoc per submodule. |
| `Prompt.ts` | Prompt + Message + Part unions. Roles `system | user | assistant | tool`. Part types: `text`, `reasoning`, `file`, `tool-call`, `tool-result`, `tool-approval-request`, `tool-approval-response`. `BasePart`/`BaseMessage` carry `ProviderOptions`. |
| `Response.ts` | Non-streaming `Part<Tools>` union and streaming `StreamPart<Tools>` union. Includes `text-start`/`text-delta`/`text-end`, `reasoning-*`, `tool-params-*`, `tool-call`, `tool-result`, `tool-approval-request`, `file`, `source` (document/url), `response-metadata`, `finish`, `error`. `FinishReason` literal set: `stop | length | content-filter | tool-calls | error | pause | other | unknown`. `Usage` token accounting. `HttpRequestDetails` / `HttpResponseDetails`. |
| `Tool.ts` | `Tool` (user-defined), `ProviderDefined` (host-executed, e.g. OpenAI web_search), `Dynamic` (JSON-Schema-only). `FailureMode` (`error | return`), `NeedsApproval`, handler context. |
| `Toolkit.ts` | Collection of `Tool` instances + handler binding (`toLayer`). |
| `AiError.ts` | Top-level wrapper carrying `module` + `method` + a discriminated `reason`. Reasons: `RateLimitError`, `QuotaExhaustedError`, `AuthenticationError`, `ContentPolicyError`, `InvalidRequestError`, `InvalidUserInputError`, `InternalProviderError`, `NetworkError`, `InvalidOutputError`, `StructuredOutputError`, `UnsupportedSchemaError`, `UnknownError`, `ToolNotFoundError`, `ToolParameterValidationError`, `InvalidToolResultError`, `ToolResultEncodingError`, `ToolConfigurationError`. Each reason exposes `isRetryable`; some expose `retryAfter`. |
| `Chat.ts` | Stateful conversation handle layered on `LanguageModel`. |
| `LanguageModel.ts` | Provider-agnostic interface: `generateText`, `generateObject`, `streamText`, tool calling. |
| `IdGenerator.ts` | Pluggable ID generator service with custom alphabet / prefix / separator / size. |
| `Telemetry.ts` | OpenTelemetry GenAI semantic-convention helpers (`addGenAIAnnotations`). |
| `Model.ts` | Provider-tagged `Layer` factory (`Model.make("anthropic", "claude-3-5-haiku", layer)`). |
| `EmbeddingModel.ts` | Embedding service. |
| `Tokenizer.ts` | Tokenize and truncate utilities. |
| `McpSchema.ts` / `McpServer.ts` | MCP (Model Context Protocol) schema and server primitives. |
| `ResponseIdTracker.ts` | Response-side ID tracker. |
| `AnthropicStructuredOutput.ts` / `OpenAiStructuredOutput.ts` | Codec transformations to fit Schema-based structured outputs into provider-specific JSON-schema constraints. |

## What's in `effect-smol/packages/ai/{anthropic,openai,openai-compat,openrouter}/src`

Per-provider mapping packages. Each follows the same shape:

- `index.ts` - barrel.
- `<Provider>Client.ts` - HTTP client / transport layer.
- `<Provider>Config.ts` - config tag.
- `<Provider>Error.ts` - provider-specific error mapping into `AiError`.
- `<Provider>LanguageModel.ts` - the live `LanguageModel` implementation; converts canonical `Prompt`/`Response` parts into the provider's wire format and back.
- `<Provider>Telemetry.ts` - provider-specific span attributes.
- `<Provider>Tool.ts` - tool argument adapter, plus `providerDefined` constructors for built-in tools (web search, code execution, computer use, etc.).

`openai/src` additionally has `OpenAiSchema.ts`, `OpenAiClientGenerated.ts`, and `OpenAiEmbeddingModel.ts`. `openai-compat/src` is the OpenAI-compatible client used for hosts that mimic the OpenAI API surface.

`Generated.ts` was excluded from every provider (1.6M for openai, 422K for openrouter, 607K for anthropic). Browse them upstream when needed:

- https://github.com/Effect-TS/effect-smol/blob/c5d7373d20e3daf0d9529271f2f632ed68f5b336/packages/ai/anthropic/src/Generated.ts
- https://github.com/Effect-TS/effect-smol/blob/c5d7373d20e3daf0d9529271f2f632ed68f5b336/packages/ai/openai/src/Generated.ts
- https://github.com/Effect-TS/effect-smol/blob/c5d7373d20e3daf0d9529271f2f632ed68f5b336/packages/ai/openrouter/src/Generated.ts

## What's in `effect/packages/ai/ai/src`

The v3 stable equivalent of `effect-smol/packages/effect/src/unstable/ai`. Useful for diffing the v3 -> v4 evolution of the canonical types. Same module names, different shapes: in particular, v3 used `AiInput` / `AiResponse` naming where v4 uses `Prompt` / `Response`, and the v4 part schema introduces `ProviderOptions` decoding defaults.

## What's in `effect/packages/ai/{amazon-bedrock,google}/src`

Hand-authored provider mapping for Amazon Bedrock and Google Gemini, which had not been ported to effect-smol at the snapshot commit. `EventStreamEncoding.ts` in `amazon-bedrock` carries the AWS event-stream framing decoder. `Generated.ts` was excluded from `google/src` for the same reason as the others.

The v3 anthropic / openai / openrouter providers (`effect/packages/ai/{anthropic,openai,openrouter}/src`) were intentionally not copied - the v4 versions in `effect-smol` are the reference, and v3 of those providers can be browsed at https://github.com/Effect-TS/effect/tree/3585f25110fca7af6aeec3f59c3fc05b20c8d316/packages/ai if needed.

## Maintenance

To refresh:

```
git -C ~/pjv/effect-ts/effect-smol pull
git -C ~/pjv/effect-ts/effect pull
# re-run the copy steps documented in the conversation that produced this snapshot
```

Then update the SHAs and snapshot date at the top of this README.

Browse anything verbatim at the pinned commit using:

- `https://github.com/Effect-TS/effect-smol/blob/c5d7373d20e3daf0d9529271f2f632ed68f5b336/<path>`
- `https://github.com/Effect-TS/effect/blob/3585f25110fca7af6aeec3f59c3fc05b20c8d316/<path>`

where `<path>` is the relative path under each subtree of this directory.
