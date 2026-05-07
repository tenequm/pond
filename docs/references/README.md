# References

Snapshots of external schemas we keep beside the design doc so we always have ground truth in front of us when designing pond's canonical types.

Snapshot date: 2026-05-07. Each subdir's README pins the upstream commit it was taken from.

## Layout

| Path | Source | Why |
|------|--------|-----|
| `opencode/` | https://github.com/anomalyco/opencode | Effect Schema canonical Part union + SDK-generated TypeScript; provider replay shapes; storage schema. The closest existing model to what pond's design section 6 describes. |
| `kilocode/` | https://github.com/kilo-org/kilocode | Fork of opencode. Adds `editorContext` on UserMessage, plan-followup logic, kilocode-specific session events. |
| `pi-mono/` | https://github.com/badlogic/pi-mono | Source of pond's leaf-cursor branching (`parent_message_id` graph) and conformance test matrix (cross-provider handoff, image-tool-result, tool-call-without-result). Also contains the silent-skip-malformed-line ingest pattern pond explicitly rejects (see pi-mono README for line numbers). |
| `otel-genai-semconv.md` | https://github.com/open-telemetry/semantic-conventions-genai (commit `914c6f46...`) | Synthesized GenAI semantic-conventions reference: `gen_ai.*` attribute registry, span shapes, events, JSON schemas for input/output messages and tool definitions, metrics, value registries. Useful for naming + the message-payload JSON schemas. |

## Maintenance

To refresh a snapshot, re-run the corresponding clone (`git -C ~/pjv/<owner>/<repo> pull`), re-copy the listed files, and update the commit SHA + date in that subdir's README. Don't reorganize: file paths under each subdir mirror their upstream paths so origin is unambiguous.
