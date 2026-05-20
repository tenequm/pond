# Codex-CLI Legacy Rollout Format

## How to use this document

This is an investigation-and-fix plan for one defect: 4 of 171 codex-cli rollout files fail to ingest. It is not yet a committed-to design - the first open question is whether to fix at all. Line numbers are investigation snapshots from `src/adapter/codex_cli.rs`; verify against current code before acting.

## Symptom

`pond sync codex-cli` reports 4 schema errors and drops those sessions from the corpus:

```
codex-cli schema error at <file>:1: first row must be session_meta
```

The 4 files, all from September 2025:

- `~/.codex/sessions/2025/09/10/rollout-2025-09-10T13-48-29-ec08b106-c89d-4804-8e4a-17a3615fb4b3.jsonl`
- `~/.codex/sessions/2025/09/10/rollout-2025-09-10T15-22-09-6bd2e5eb-1537-4437-85d3-7d3000e07470.jsonl`
- `~/.codex/sessions/2025/09/13/rollout-2025-09-13T05-30-14-64da700c-b082-493e-b08f-6e3adc4f034f.jsonl`
- `~/.codex/sessions/2025/09/13/rollout-2025-09-13T05-30-17-67c52f3f-d25e-4194-a006-93de58f28d7c.jsonl`

They are counted as `err` in the sync summary, not skipped cleanly, and they re-parse and re-fail on every sync (see affected code, `peek_id_and_mtime`).

## Root cause

These files use an older Codex rollout schema, pre-dating the `session_meta` envelope. A legacy file is, in effect, the current payload objects written un-enveloped, plus a bare first row and `record_type:"state"` noise. Three structural differences:

1. First row. Legacy is a bare metadata object with no `type`, fields at top level: `{"id":"...","timestamp":"...","instructions":null,"git":{...}}`. Current is `{"type":"session_meta","payload":{"id":...,"timestamp":...,"cwd":...,...}}`.
2. State markers. Legacy interleaves `{"record_type":"state"}` rows - discriminator key `record_type`, not `type`.
3. Message rows. Legacy writes payloads directly: `{"type":"message","role":"user","content":[{"type":"input_text","text":"..."}]}`. Current wraps them: `{"type":"response_item","payload":{"type":"message",...}}`. A legacy data row is essentially the current `payload`, without the `response_item` envelope.

## Affected code (`src/adapter/codex_cli.rs`)

- `session_meta()` ~360-453: hard error at ~374-379 (`first row must be session_meta`). First failure point.
- `peek_id_and_mtime()` ~346-358: returns `None` for the legacy first row, so legacy files are never freshness-skipped - they re-parse and re-fail every sync.
- `events_from_row()` ~461-528: dispatches on `type == "response_item"`; legacy `type:"message"` rows all fall to the non-`response_item` branch.
- `raw_carrier_event()` ~538: that branch produces `Message::System { content: None }` with the row in `options`. Legacy conversation text would land here - not searchable, not visible via `get`.

## Fix options

- A. Full legacy parse (recommended). Detect the legacy schema, parse the bare first row into `Session`, drop `record_type:"state"`, and route each legacy data row through the existing payload logic (a legacy row == a current `payload`). Reuses most of `events_from_row`.
- B. Carry-only. Relax `session_meta` to accept the bare first row and let the existing carrier path scoop the rest. Cheap, but every message becomes a content-less `System` carrier - useless for search and `get`. Rejected.
- C. Clean skip. Detect legacy and emit a `Skipped` with a clear reason instead of an `err`. Honest, no parser growth, zero recovered data. Acceptable fallback if the 4 files are judged not worth the code.

## Recommended plan (option A)

1. Confirm scope. Inspect all 4 files end to end, plus any other pre-October-2025 rollouts, to enumerate every legacy record type (`message`, `function_call`, `function_call_output`, `reasoning`, `record_type:"state"`, anything else). The three differences above come from a 4-line sample.
2. Add legacy detection: first row has no `type` but carries top-level `id` + `timestamp`.
3. `session_meta()`: in legacy mode read `id` / `timestamp` / `git` / `instructions` from the top level instead of from `payload`.
4. `peek_id_and_mtime()`: recognize the legacy first row so legacy files freshness-skip like any other once ingested.
5. `events_from_row()`: in legacy mode treat the row itself as the payload and drop `record_type:"state"`; reuse the existing `payload.type` match arms.
6. Native restore: `raw_record` already stores the verbatim row, so native replay is automatic - confirm a legacy session round-trips byte-exact.

## Open questions

- Q1. Are 4 legacy files (~2% of the codex corpus, all September 2025) worth a parser branch, or is option C the right minimalist call?
- Q2. Is the legacy schema a single stable format, or did Codex iterate within it? Only two dates were sampled.
- Q3. Does legacy carry `function_call` / `function_call_output` / `reasoning` rows, and are they also un-enveloped?

## Validation

- Add a trimmed legacy rollout under `tests/fixtures/adapter/codex_cli/` and a parse test.
- After the fix: the 4 files ingest with non-zero message counts; `pond get` and `pond search` return their content.
- `cargo build`, `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` all green.

## Out of scope

The same sync logged `function_call_output.output exceeded cap; truncated to sentinel`. That is a by-design size cap on tool output, not a defect. If the cap value needs revisiting, track it separately.
