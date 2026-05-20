# Anthropic Managed Agents - session event samples

This directory holds exported session event logs from the Anthropic Managed
Agents playground (the hosted "Claude Agent Skills" / agent runtime that powers
managed agent sessions). These are reference artifacts: the canonical wire shape
of how Anthropic itself represents a session as an append-only event log.

## Source

- Product: Anthropic Managed Agents (playground)
- Export action: the playground exposes a download of the session event stream
  as a single JSON file.
- Capture date of the included sample: 2026-04-09 (timestamps inside the file).

## Files

- `session-events-sesn_011CZtfjofzReJvDfnT7eMJr.json` - one full session, 15
  events. In the original capture the file was ~156 KB on disk; the bulk of
  the byte count was inside two `agent.tool_result` events whose payloads
  contain scraped HTML from GitHub README pages (the agent used the
  `web_fetch` tool). Those payloads, along with the final synthesized
  comparison emitted by the model, have been replaced with `<snipped: ...>`
  placeholders in this sample, so the on-disk size is now much smaller while
  the event-stream shape is preserved.

The `sesn_...` segment of the filename is the session ID. It is opaque (ULID
style, prefixed by Anthropic resource type) and safe to keep verbatim.

## Anonymization

The original capture contained no personal or secret content beyond what is
already public:

- No local filesystem paths (`/Users/<name>/...`)
- No API keys, JWTs, Bearer tokens (the only `Bearer YOUR_API_KEY` substring
  is a literal placeholder inside a third-party README that the agent fetched)
- No personal emails (any contact emails that appeared inside fetched README
  HTML were public business contacts; they have since been snipped along with
  the rest of the README payload)
- No private IPs (only `127.0.0.1` inside README documentation)
- No personal names in user-authored message text
- No internal organization or workspace identifiers

The user prompt asks the agent to compare two public GitHub repositories. The
real URLs have been replaced with placeholder org/repo names
(`myorg/myproject` and `acme/otherproject`); the same placeholders appear in
the matching `agent.tool_use.input.url` fields and in the `title` of the
corresponding `agent.tool_result` document blocks, so the tool_use /
tool_result coupling that makes this sample useful as a wire-format reference
is preserved. The scraped README bodies and the agent's synthesized
comparison have been replaced with `<snipped: ...>` placeholders.

If a future export does contain sensitive content, apply these substitutions
before committing:

- `/Users/<name>/` -> `/Users/USER/`
- API keys / Bearer tokens / JWTs -> `<REDACTED_API_KEY>`
- Personal emails -> `user@example.com`
- Personal names in message text -> `Alice`, `Bob`
- Real org / workspace names -> `example-org`
- Cloud account IDs and public IPs -> placeholders
- Preserve all event IDs (`sevt_...`), session IDs (`sesn_...`), timestamps,
  event type discriminators, tool names, role markers, and JSON field names.

## Event-format primer

The file is a single JSON array. Each element is one event. Events arrive in
processed-at order (the file is already sorted by `processed_at`).

Every event has the same minimal envelope:

- `id` - server-assigned event ID, prefix `sevt_`. Opaque.
- `processed_at` - ISO-8601 UTC timestamp with millisecond precision.
- `type` - the discriminator. This is the single field that tells you what
  shape the rest of the event has.

There is no separate `event_type` field; `type` is the discriminator. Anthropic
uses dotted namespaces: `session.*`, `user.*`, `agent.*`, `span.*`.

Observed `type` values in this sample, in the order they appear in a typical
"user asks, agent fetches two URLs in parallel, agent answers" turn:

1. `session.status_running` - session moved from idle to running. No payload
   beyond the envelope.
2. `user.message` - user input. Adds a `content` array of typed blocks; the
   only block kind here is `{ "type": "text", "text": "..." }`. This is the
   same block model used by the Messages API.
3. `span.model_request_start` - opens a model-call span. Spans are paired with
   `span.*_end` events later in the stream; correlation is via event IDs (see
   `model_request_start_id` below).
4. `agent.thinking` - marker event emitted when the model produces extended
   thinking output. No payload in this sample (the thinking text itself is
   not exported, only the fact that thinking occurred).
5. `agent.message` - assistant-authored content. Same `content: [...]` block
   shape as `user.message`. An agent turn can produce multiple `agent.message`
   events interleaved with tool_use / tool_result events.
6. `agent.tool_use` - the model decided to call a tool. Carries:
   - `name` - tool name (e.g. `web_fetch`).
   - `input` - the JSON arguments the model produced for the tool.
   - `evaluated_permission` - the runtime's permission decision for this
     specific call, e.g. `"allow"`. This is Managed-Agents-specific; the
     Messages API has no equivalent field.
   The event's own `id` is the tool-use ID that the matching tool_result will
   reference.
7. `agent.tool_result` - the tool's response. Carries:
   - `tool_use_id` - back-reference to the `agent.tool_use` event ID.
   - `is_error` - boolean.
   - `content` - array of typed blocks; here the block kind is
     `{ "type": "document", "title": "...", "source": { "type": "text",
     "media_type": "text/plain", "data": "..." } }`. The block model mirrors
     the Messages API tool_result content shape.
8. `span.model_request_end` - closes the span opened by a matching
   `span.model_request_start`. Carries:
   - `model_request_start_id` - back-reference to the opening event ID.
   - `is_error` - boolean.
   - `model_usage` - token counters: `input_tokens`, `output_tokens`,
     `cache_creation_input_tokens`, `cache_read_input_tokens`.
9. `session.status_idle` - session returned to idle. Carries `stop_reason`
   (e.g. `{ "type": "end_turn" }`), which mirrors the `stop_reason` field on a
   Messages API response.

### Correlation pattern

The wire format uses ID back-references, not nesting, to express relationships:

- `agent.tool_result.tool_use_id` -> `agent.tool_use.id`
- `span.model_request_end.model_request_start_id` -> `span.model_request_start.id`

This is consistent with treating each event as an independent, immutable
record. Reconstructing the message-tool-result tree is a reader's job.

### Two assistant blocks per model call

In this sample, the model emits an `agent.message` containing only narration
text ("I'll fetch both repositories simultaneously to compare them!") and then
emits two `agent.tool_use` events in the same span before the
`span.model_request_end` fires. So a single span can produce multiple events of
different `type`s, all sharing the implicit parent of the most recent
`span.model_request_start`. Span end is signaled explicitly by
`span.model_request_end`, not implied by the next event.

## Relationship to pond's append-only-log framing

Pond's `docs/spec.md#append-only` rule cites the Anthropic
Managed Agents session-as-event-log framing. This sample is the canonical
reference for what that framing looks like on the wire. Specifically:

- A session is a flat, time-ordered sequence of typed events. There is no
  parent_message_id, no thread tree, no in-place edits. New facts are appended
  as new events with new IDs.
- Tool calls and tool results are first-class events on the same log as user
  and assistant messages, not nested inside a message object.
- Span lifecycle (start / end) is also represented as events, not as a wrapper
  around the events that occurred during the span.
- All state transitions (running / idle, stop_reason, token usage) are events.
  Replaying the log in order is sufficient to reconstruct any derived view.

Pond's design extends this same shape across multiple agent runtimes (Claude
Code, Codex, custom agents) - a Managed Agents session would map to a pond
session whose entries are these events, with `(timestamp, id)` providing total
order per session.

## Open questions for future schema work

- What does the event stream look like when the model emits multiple
  `agent.message` text blocks alternating with `agent.tool_use` blocks? This
  sample only shows narration-then-tools.
- Are there `*.error` event types, or does error reporting ride on the
  existing `is_error` boolean on tool_result / span end?
- Are there events for streaming partial token deltas, or is the export
  always coalesced into completed-message granularity? (This sample is
  coalesced.)
- What event(s) represent permission prompts that resolve to "deny"? Here the
  only observed value of `evaluated_permission` is `"allow"`.

Capture more samples covering these cases before locking schema decisions.
