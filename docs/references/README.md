# References

Upstream schemas that shaped pond's canonical types, documented here as pointers. The vendored source snapshots are not redistributed; clone the listed repos when you need the ground-truth code in front of you.

| Source | Why it matters |
|--------|----------------|
| [sst/opencode](https://github.com/sst/opencode) | Effect Schema canonical Part union + SDK-generated TypeScript; provider replay shapes; storage schema. The closest existing model to what `docs/spec.md#adapters` describes. |
| [kilo-org/kilocode](https://github.com/kilo-org/kilocode) | Fork of opencode. Adds `editorContext` on UserMessage, plan-followup logic, kilocode-specific session events. |
| [badlogic/pi-mono](https://github.com/badlogic/pi-mono) | Source of pond's pi-coding-agent leaf-cursor branching (`parent_message_id` graph) and conformance test matrix (cross-provider handoff, image-tool-result, tool-call-without-result). |
| [Effect-TS/effect](https://github.com/Effect-TS/effect) (v3) and [Effect-TS/effect-smol](https://github.com/Effect-TS/effect-smol) (v4) | The canonical `effect/unstable/ai` Prompt + Response part unions, Tool / Toolkit / MCP shapes, and per-provider mapping code (Anthropic, OpenAI, OpenAI-compatible, OpenRouter, Amazon Bedrock, Google). The shape pond's design copies as Rust serde types. |
| [open-telemetry/semantic-conventions](https://github.com/open-telemetry/semantic-conventions) | GenAI semantic-conventions reference (`gen_ai.*` attribute registry, span shapes, events, JSON schemas, metrics). Synthesized locally in `otel-genai-semconv.md`. Useful for naming + message-payload JSON schemas. |

## Deployment references

Topologies assembled from primitives pond already ships - no new machinery, just the shape and its tradeoffs written down. Dated, because a deployment reference ages with the surface it describes.

| Reference | What it covers |
|-----------|----------------|
| [`2608-06-pi-fleet-capture.md`](2608-06-pi-fleet-capture.md) | A fleet of headless pi workers with a pond sidecar each, one store per tenant, a central read side, and the split-embedding cost lever. Runnable at [`ops/examples/pi-fleet/`](../../ops/examples/pi-fleet). |
