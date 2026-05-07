# pi-mono reference snapshot

Source: https://github.com/badlogic/pi-mono
Local clone: `~/pjv/badlogic/pi-mono/`
Commit: `801db80b65210b25462bce1700675e073fe3dbe5`
Snapshot date: 2026-05-07

## Why these files

Pond's design doc cites pi-mono twice (`/docs/design.md` section 16):
- **Lifted**: leaf-cursor branching via `parent_message_id` graph + the conformance test matrix (cross-provider handoff, image-tool-result, tool-call-without-result, unicode-surrogate).
- **Rejected**: pi-mono's silent-skip-malformed-line ingest. Pond requires schema-validated decode with logged errors.

This dir keeps both the patterns we want to copy and the exact code we explicitly reject, so we have ground truth in front of us when implementing.

## Ranked relevance (most useful first)

### Canonical types

1. `packages/ai/src/types.ts` - Message/Part union: UserMessage, AssistantMessage, ToolResultMessage; TextContent, ThinkingContent, ImageContent, ToolCall; AssistantMessageEvent (streaming start/delta/end variants); Usage, StopReason.
2. `packages/agent/src/types.ts` - AgentMessage extension union, AgentToolCall, AgentToolResult, AgentContext, ThinkingLevel enum.
3. `packages/coding-agent/src/core/messages.ts` - custom variants: BashExecutionMessage, BranchSummaryMessage, CompactionSummaryMessage; `convertToLlm()` shows custom-role -> LLM-compatible mapping.

### Session storage + leaf-cursor branching

4. `packages/coding-agent/src/core/session-manager.ts` - **the central file**. SessionEntry union (SessionMessageEntry, CompactionEntry, BranchSummaryEntry, CustomEntry, LabelEntry); SessionHeader; `parentId` linked-list DAG; `CURRENT_SESSION_VERSION = 3`; `buildSessionContext()` walks parent chain to root. Also contains the malformed-line silent-skip pond rejects (lines 288-299, 445-453, 556-561).
5. `packages/web-ui/src/storage/types.ts` - SessionMetadata, SessionData, StorageBackend / StorageTransaction abstraction.
6. `packages/coding-agent/src/core/extensions/types.ts` - branching API: `fork(entryId, position: "before" | "at")`, `navigateTree()` for leaf-cursor movement, ReplacedSessionContext, SessionBeforeForkEvent, SessionTreeEvent.
7. `packages/coding-agent/test/agent-session-branching.test.ts` - concrete fork test: entryId selection, leaf restoration, new-session creation post-fork.

### Cross-provider replay + conformance matrix

8. `packages/ai/src/providers/transform-messages.ts` - `transformMessages()` for replay: tool-call-ID normalization, redacted thinking blocks, synthetic empty tool results for orphaned calls, image downgrade for non-vision models.
9. `packages/ai/test/cross-provider-handoff.test.ts` - the conformance matrix: 30+ provider/model pairs (Anthropic, Google, OpenAI, Azure, Bedrock, xAI, Groq, Mistral, ...). Caches generated contexts then replays across providers.
10. `packages/ai/test/tool-call-without-result.test.ts` - orphaned tool-call fixture.
11. `packages/ai/test/image-tool-result.test.ts` - image content in tool results + image downgrade.

### Streaming protocol

12. `packages/ai/src/utils/event-stream.ts` - AssistantMessageEventStream (async iterable), AssistantMessageEvent union (start, text_start/delta/end, thinking_start/delta/end, toolcall_start/delta/end, done, error).

## Structural notes

- **Typing**: plain TypeScript interfaces + discriminated unions. No Zod, no Effect. Streaming events live in a separate union (`AssistantMessageEvent`) discriminated by `type`, distinct from the persisted message types. Pond's design currently lists Start/Delta/End as Part variants in section 6 - reconcile against this (pi-mono keeps streaming events out of storage; opencode unifies them with a `time` field).
- **Branching primitive**: every `SessionEntry` carries `id` (8-char short UUID) + `parentId` (string | null). DAG is reconstructed by walking parent chain from a leafId. `fork(entryId)` writes a new session file with the given entry as new root. This is the pattern pond's section 19.5 should keep.
- **Malformed-line silent-skip (rejected by pond)**: `packages/coding-agent/src/core/session-manager.ts` lines 288-299 (`parseSessionEntries`), 445-453 (`loadEntriesFromFile`), 556-561 (`buildSessionInfo`). Each wraps `JSON.parse(line)` in try/catch with a `// Skip malformed lines` comment. Pond's SourceAdapter must instead emit a logged decode error per failed line.
