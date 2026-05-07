# kilocode reference snapshot

Source: https://github.com/kilo-org/kilocode
Local clone: `~/pjv/kilo-org/kilocode/`
Commit: `0722158de154694558c6e88c3f4935181a281619`
Snapshot date: 2026-05-07

## Why these files

Reference for designing pond's canonical Part union and message/session schemas (see `/docs/design.md` section 6). kilocode's `packages/opencode/` directory is a fork/extension of opencode, so much of this overlaps with `docs/references/opencode/`. Worth keeping both: kilocode adds editor-context, plan-followup logic, and a few schema deltas.

## Ranked relevance (most useful first)

1. `packages/opencode/src/session/message-v2.ts` (1328 lines) - canonical Part union (TextPart, ReasoningPart, FilePart, ToolPart, StepStartPart, StepFinishPart, SnapshotPart, PatchPart, AgentPart, RetryPart, CompactionPart, SubtaskPart) + ToolState + EditorContext + ResourceSource/FileSource/SymbolSource + bus events.
2. `packages/opencode/src/session/schema.ts` - SessionID, MessageID, PartID brands.
3. `packages/opencode/src/session/prompt.ts` (2007 lines) - PromptInput schema (input variants without ambient IDs); ModelRef; OutputFormat.
4. `packages/sdk/js/src/v2/gen/types.gen.ts` (7416 lines) - generated SDK types mirroring (1).
5. `packages/opencode/src/session/session.sql.ts` - Drizzle storage schema (SessionTable, MessageTable, PartTable). Parts persisted as JSON blobs.
6. `packages/opencode/src/session/message.ts` - legacy message schema (ToolCall, ToolPartialCall, ToolResult, ToolInvocation, MessagePart). Predecessor to message-v2.
7. `packages/opencode/src/kilocode/session/prompt.ts` - kilocode-specific KiloSessionPrompt (plan followup, permission guarding, EditorContext integration).
8. `packages/opencode/src/kilocode/editor-context.ts` - EditorContext interface + helpers (staticEnvLines, environmentDetails).
9. `packages/kilo-vscode/webview-ui/src/types/messages/parts.ts` - frontend Part subset + PartDelta for streaming UI.
10. `packages/opencode/src/provider/sdk/copilot/chat/convert-to-openai-compatible-chat-messages.ts` - canonical-to-OpenAI conversion (replay shape).
11. `packages/opencode/src/session/processor.ts` - tool-call lifecycle (updateToolCall, completeToolCall).
12. `packages/opencode/src/provider/transform.ts` - provider option normalization.
13. `packages/opencode/src/kilocode/session/index.ts` - kilocode session events (TurnOpen, TurnClose).

## kilocode-specific extensions vs opencode baseline

- `editorContext` field on `User` message (visible files, open tabs, active file, shell). Pond's design already calls this out in section 6.
- `EditorContext` schema lives in `packages/opencode/src/kilocode/editor-context.ts`.
- Plan-followup logic + cost propagation in `KiloSessionPrompt` namespace.
- Subtask, Agent, StepStart/StepFinish, Compaction with `tail_start_id`, Retry parts (all aligned with pond's "Harness extensions" list).

## Notes on shape

- Effect Schema source of truth, Zod overlay via `.zod` accessor.
- Streaming/non-streaming variants **unified**: no TextStartPart/TextDeltaPart/TextEndPart - streaming flows through bus events (`PartDelta`) instead. Pond's design currently lists separate streaming variants - reconcile against this.
- ToolPart unified (no ToolCallPart vs ToolResultPart split); state discriminator: pending / running / completed / error.
- Storage: JSON blobs in SQLite via Drizzle; sessions carry `parent_id` for forks.
