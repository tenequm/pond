# OpenTelemetry GenAI Semantic Conventions - Reference

Self-contained reference of the OpenTelemetry Generative AI semantic conventions
(`gen_ai.*`), including provider-specific extensions for OpenAI, Anthropic,
Azure AI Inference, AWS Bedrock, and Model Context Protocol (MCP). Captures
attribute registry, span shapes, events, metrics, value registries, and
stability per attribute.

- Source (local clone): `~/pjv/open-telemetry/semantic-conventions-genai/`
- Source (upstream): `https://github.com/open-telemetry/semantic-conventions`,
  rendered at `https://opentelemetry.io/docs/specs/semconv/gen-ai/`
- Fetch date: 2026-05-07
- Clone commit SHA: `914c6f4641e7be0b9fb032830cb478c4b833bd25`
- Stability: every attribute / span / event / metric in the active GenAI
  registry is currently `Development` (experimental). Generic referenced
  attributes such as `error.type`, `server.address`, `server.port`,
  `client.address`, `client.port`, `exception.*`, `network.transport`,
  `network.protocol.name`, `network.protocol.version` are `Stable`.

## 1. Attribute registry (`gen_ai.*`)

All attributes below are defined in `model/gen-ai/registry.yaml`. Every entry
has stability `Development` unless otherwise noted.

### 1.1 Provider and operation discriminators

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `gen_ai.provider.name` | enum (string) | Development | The Generative AI provider as identified by the client or server instrumentation. | Acts as the discriminator for provider-flavored telemetry. SHOULD be set consistently with provider-specific attributes. See section 5.1 for allowed values. |
| `gen_ai.operation.name` | enum (string) | Development | The name of the operation being performed. | See section 5.2. Span name is usually `{operation} {model}` (or `{operation} {agent.name}`, `{operation} {data_source.id}`, `{operation} {tool.name}`). |
| `gen_ai.output.type` | enum (string) | Development | Represents the content type requested by the client. | Modality, not format. Allowed values: `text`, `json`, `image`, `speech`. |

### 1.2 Request parameters

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `gen_ai.request.model` | string | Development | The name of the GenAI model a request is being made to. | Use exact vendor name; for fine-tuned models use the more specific name. Example: `gpt-4`. |
| `gen_ai.request.max_tokens` | int | Development | The maximum number of tokens the model generates for a request. | |
| `gen_ai.request.choice.count` | int | Development | The target number of candidate completions to return. | Conditionally required if available and `!= 1`. |
| `gen_ai.request.temperature` | double | Development | The temperature setting for the GenAI request. | |
| `gen_ai.request.top_p` | double | Development | The top_p sampling setting for the GenAI request. | |
| `gen_ai.request.top_k` | double | Development | The top_k sampling setting for the GenAI request. | Type is `double` (not int) per registry. |
| `gen_ai.request.stop_sequences` | string[] | Development | List of sequences that the model will use to stop generating further tokens. | |
| `gen_ai.request.frequency_penalty` | double | Development | The frequency penalty setting for the GenAI request. | |
| `gen_ai.request.presence_penalty` | double | Development | The presence penalty setting for the GenAI request. | |
| `gen_ai.request.encoding_formats` | string[] | Development | The encoding formats requested in an embeddings operation, if specified. | Some providers call these embedding types. |
| `gen_ai.request.seed` | int | Development | Requests with same seed value more likely to return same result. | |
| `gen_ai.request.stream` | boolean | Development | Indicates whether the GenAI request was made in streaming mode. | If unset, assume non-streaming. |

### 1.3 Response

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `gen_ai.response.id` | string | Development | The unique identifier for the completion. | Example: `chatcmpl-123`. |
| `gen_ai.response.model` | string | Development | The name of the model that generated the response. | Use exact vendor name. |
| `gen_ai.response.finish_reasons` | string[] | Development | Array of reasons the model stopped generating tokens, corresponding to each generation received. | One element per choice/candidate. See section 5.3. |
| `gen_ai.response.time_to_first_chunk` | double | Development | Time to first chunk in a streaming response, measured from request issuance, in seconds. | Recommended for streaming requests. |

### 1.4 Token usage

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `gen_ai.usage.input_tokens` | int | Development | The number of tokens used in the GenAI input (prompt). | SHOULD include all input token types, including cached tokens. For Anthropic this is `input_tokens + cache_read + cache_creation`. |
| `gen_ai.usage.cache_read.input_tokens` | int | Development | The number of input tokens served from a provider-managed cache. | Value SHOULD be included in `gen_ai.usage.input_tokens`. |
| `gen_ai.usage.cache_creation.input_tokens` | int | Development | The number of input tokens written to a provider-managed cache. | Value SHOULD be included in `gen_ai.usage.input_tokens`. |
| `gen_ai.usage.output_tokens` | int | Development | The number of tokens used in the GenAI response (completion). | |
| `gen_ai.usage.reasoning.output_tokens` | int | Development | The number of output tokens used for reasoning (chain-of-thought, extended thinking). | Value SHOULD be included in `gen_ai.usage.output_tokens`. |
| `gen_ai.token.type` | enum (string) | Development | The type of token being counted. | Used as a metric attribute. Allowed values: `input`, `output`. The legacy `completion` member exists, deprecated and renamed to `output`. |

### 1.5 Conversation, agent, workflow, prompt

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `gen_ai.conversation.id` | string | Development | Unique identifier for a conversation (session, thread). | Example: `conv_5j66UpCpwteGg4YSxUnt7lPY`. Used to correlate messages within the same session. |
| `gen_ai.agent.id` | string | Development | The unique identifier of the GenAI agent. | |
| `gen_ai.agent.name` | string | Development | Human-readable name of the GenAI agent provided by the application. | |
| `gen_ai.agent.description` | string | Development | Free-form description of the GenAI agent provided by the application. | |
| `gen_ai.agent.version` | string | Development | The version of the GenAI agent. | |
| `gen_ai.workflow.name` | string | Development | Human-readable name of the GenAI workflow provided by the application. | First chain in LangChain, crew name in CrewAI, etc. |
| `gen_ai.prompt.name` | string | Development | The name of the prompt that uniquely identifies it. | Example: `analyze-code`. |

### 1.6 Tools

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `gen_ai.tool.name` | string | Development | Name of the tool utilized by the agent. | |
| `gen_ai.tool.call.id` | string | Development | The tool call identifier. | |
| `gen_ai.tool.description` | string | Development | The tool description. | |
| `gen_ai.tool.type` | string | Development | Type of the tool utilized by the agent. | Common values: `function`, `extension`, `datastore`. |
| `gen_ai.tool.call.arguments` | any (object) | Development | Parameters passed to the tool call. | Sensitive. Structured form preferred; on spans MAY be JSON string. |
| `gen_ai.tool.call.result` | any (object) | Development | The result returned by the tool call. | Sensitive. Structured form preferred; on spans MAY be JSON string. |
| `gen_ai.tool.definitions` | any (array) | Development | The list of tool definitions available to the GenAI agent or model. | Follows JSON schema in `docs/gen-ai/gen-ai-tool-definitions.json`. Recommended only via opt-in. |

### 1.7 Messages and instructions

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `gen_ai.system_instructions` | any (array) | Development | System message or instructions provided separately from chat history. | Sensitive. Follows JSON schema in `docs/gen-ai/gen-ai-system-instructions.json`. |
| `gen_ai.input.messages` | any (array) | Development | Chat history provided to the model as input. | Sensitive (PII). Messages MUST be in the order sent to the model. Follows JSON schema in `docs/gen-ai/gen-ai-input-messages.json`. |
| `gen_ai.output.messages` | any (array) | Development | Messages returned by the model where each message is a specific response (choice, candidate). | Sensitive (PII). Each message corresponds to exactly one choice. Follows JSON schema in `docs/gen-ai/gen-ai-output-messages.json`. |

### 1.8 Retrieval / data source / embeddings

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `gen_ai.data_source.id` | string | Development | The data source identifier (vector store, document collection, ...). | MAY be combined with `db.*` attributes. |
| `gen_ai.retrieval.query.text` | string | Development | The query text used for retrieval. | Sensitive. Opt-in. |
| `gen_ai.retrieval.documents` | any (array) | Development | The documents retrieved (id + score, extensible). | Follows JSON schema in `docs/gen-ai/gen-ai-retrieval-documents.json`. Opt-in. |
| `gen_ai.embeddings.dimension.count` | int | Development | The number of dimensions of the resulting embeddings. | |

### 1.9 Evaluation

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `gen_ai.evaluation.name` | string | Development | The name of the evaluation metric used for the GenAI response. | Examples: `Relevance`, `IntentResolution`. |
| `gen_ai.evaluation.score.value` | double | Development | The evaluation score returned by the evaluator. | |
| `gen_ai.evaluation.score.label` | string | Development | Human-readable label for evaluation. | Low cardinality. Examples: `relevant`, `not_relevant`, `pass`, `fail`. |
| `gen_ai.evaluation.explanation` | string | Development | Free-form explanation for the assigned score. | |

### 1.10 OpenAI-specific (`openai.*`)

Defined in `model/openai/registry.yaml`.

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `openai.api.type` | enum (string) | Development | The type of OpenAI API being used. | Values: `chat_completions`, `responses`. |
| `openai.request.service_tier` | enum (string) | Development | The service tier requested. | Values: `auto`, `default` (other strings allowed). |
| `openai.response.service_tier` | string | Development | The service tier used for the response. | Examples: `scale`, `default`. |
| `openai.response.system_fingerprint` | string | Development | Fingerprint to track changes in the GenAI environment. | Example: `fp_44709d6fcb`. |

### 1.11 AWS Bedrock-specific (`aws.bedrock.*`)

Defined in `model/aws-bedrock/registry.yaml`.

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `aws.bedrock.guardrail.id` | string | Development | The unique identifier of the AWS Bedrock Guardrail. | Required on `aws.bedrock.inference.client` spans when applicable. |
| `aws.bedrock.knowledge_base.id` | string | Development | The unique identifier of the AWS Bedrock Knowledge base. | |

### 1.12 Azure-specific

Azure AI Inference reuses `gen_ai.*` attributes and adds:

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `azure.resource_provider.namespace` | string | Stable (Azure registry) | The Azure resource provider namespace. | For Azure AI Inference SHOULD be `Microsoft.CognitiveServices`. |

### 1.13 MCP attributes (`mcp.*`)

Defined in `model/mcp/registry.yaml`. All `Development`.

| Name | Type | Status | Brief | Notes |
| --- | --- | --- | --- | --- |
| `mcp.method.name` | enum (string) | Development | The name of the JSON-RPC method or notification. | See section 5.9 for the full enum. |
| `mcp.session.id` | string | Development | Identifies an MCP session. | |
| `mcp.resource.uri` | string | Development | The value of the resource URI. | Provided in `resources/read`, `resources/subscribe`, `resources/unsubscribe`, `notifications/resources/updated`. |
| `mcp.protocol.version` | string | Development | The version of the Model Context Protocol used. | Example: `2025-06-18`. |

MCP spans also reference the stable cross-cutting attributes
`jsonrpc.request.id`, `jsonrpc.protocol.version`, `rpc.response.status_code`,
`network.transport`, `network.protocol.name`, `network.protocol.version`,
`server.address`, `server.port`, `client.address`, `client.port`, `error.type`.

## 2. Span / operation shapes

Defined in `model/gen-ai/spans.yaml`. All spans `Development`.

### 2.1 Operation-name registry (`gen_ai.operation.name`)

| Operation | Brief |
| --- | --- |
| `chat` | Chat completion (e.g. OpenAI Chat API). |
| `generate_content` | Multimodal content generation (e.g. Gemini). |
| `text_completion` | Text completions (e.g. OpenAI Completions, legacy). |
| `embeddings` | Embeddings creation. |
| `retrieval` | Retrieval such as a vector-store search. |
| `create_agent` | Create a GenAI agent. |
| `invoke_agent` | Invoke a GenAI agent. |
| `execute_tool` | Execute a tool. |
| `invoke_workflow` | Invoke a multi-agent / orchestrated workflow. |

### 2.2 Span: `gen_ai.inference.client` (chat / generate_content / text_completion)

- Span kind: `CLIENT` (MAY be `INTERNAL` for in-process models)
- Name: `{gen_ai.operation.name} {gen_ai.request.model}`
- Required: `gen_ai.provider.name`, `gen_ai.operation.name`
- Conditionally required: `gen_ai.request.model` (if available),
  `gen_ai.request.choice.count` (when `!= 1`), `gen_ai.request.seed` (when set
  in request), `gen_ai.output.type` (when client requested an output type),
  `gen_ai.request.stream` (only if streaming), `gen_ai.request.top_k` (when
  applicable), `gen_ai.conversation.id` (when available), `error.type` (on
  error), `server.port` (when `server.address` is set)
- Recommended: `gen_ai.request.max_tokens`, `temperature`, `top_p`,
  `stop_sequences`, `frequency_penalty`, `presence_penalty`,
  `gen_ai.response.id`, `gen_ai.response.model`,
  `gen_ai.response.finish_reasons`, `gen_ai.response.time_to_first_chunk`
  (streaming), `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`,
  `gen_ai.usage.cache_read.input_tokens`,
  `gen_ai.usage.cache_creation.input_tokens`,
  `gen_ai.usage.reasoning.output_tokens`, `server.address`
- Opt-in (likely sensitive): `gen_ai.system_instructions`,
  `gen_ai.input.messages`, `gen_ai.output.messages`, `gen_ai.tool.definitions`
- Sampling-relevant: `gen_ai.provider.name`, `gen_ai.operation.name`,
  `server.address`, `server.port`, `gen_ai.request.model`

### 2.3 Span: `gen_ai.embeddings.client`

- Span kind: `CLIENT`
- Name: `{gen_ai.operation.name} {gen_ai.request.model}` (operation = `embeddings`)
- Required: `gen_ai.provider.name`, `gen_ai.operation.name`
- Recommended: `gen_ai.request.encoding_formats`, `gen_ai.usage.input_tokens`,
  `gen_ai.embeddings.dimension.count`, `gen_ai.response.model`,
  `gen_ai.request.model`, `server.address`, `server.port`
- Conditionally required: `error.type` (on error), `server.port` (if address
  set)

### 2.4 Span: `gen_ai.retrieval.client`

- Span kind: `CLIENT`
- Name: `{gen_ai.operation.name} {gen_ai.data_source.id}` (operation =
  `retrieval`)
- Conditionally required: `gen_ai.provider.name` (when applicable),
  `gen_ai.data_source.id` (when applicable), `error.type` (on error)
- Recommended: `gen_ai.request.top_k`
- Opt-in: `gen_ai.retrieval.query.text`, `gen_ai.retrieval.documents`

### 2.5 Span: `gen_ai.create_agent.client`

- Span kind: `CLIENT`
- Name: `create_agent {gen_ai.agent.name}`
- Required: `gen_ai.provider.name`, `gen_ai.operation.name`
- Conditionally required: `gen_ai.agent.id`, `gen_ai.agent.name`,
  `gen_ai.agent.description`, `gen_ai.agent.version` (each "if applicable" or
  "if provided by the application"), `error.type`, `server.port`
- Opt-in: `gen_ai.system_instructions`

### 2.6 Span: `gen_ai.invoke_agent.client` and `gen_ai.invoke_agent.internal`

- Span kind: `CLIENT` (remote agent service) or `INTERNAL` (in-process
  framework like LangChain or CrewAI)
- Name: `invoke_agent {gen_ai.agent.name}` (or just `invoke_agent`)
- Required: `gen_ai.provider.name`, `gen_ai.operation.name`,
  `gen_ai.request.model` (only required when applicable)
- Conditionally required: `gen_ai.request.model` (if available),
  `gen_ai.request.choice.count` (`!= 1`), `gen_ai.request.seed`,
  `gen_ai.output.type`, `gen_ai.conversation.id`, `gen_ai.agent.id`,
  `gen_ai.agent.name`, `gen_ai.agent.description`, `gen_ai.agent.version`,
  `gen_ai.data_source.id`, `error.type`
- Recommended: `gen_ai.request.max_tokens`, `temperature`, `top_p`,
  `stop_sequences`, `frequency_penalty`, `presence_penalty`,
  `gen_ai.response.finish_reasons`, full `gen_ai.usage.*` set
- Opt-in: `gen_ai.system_instructions`, `gen_ai.input.messages`,
  `gen_ai.output.messages`, `gen_ai.tool.definitions`

### 2.7 Span: `gen_ai.execute_tool.internal`

- Span kind: `INTERNAL`
- Name: `execute_tool {gen_ai.tool.name}`
- Required: `gen_ai.operation.name` (= `execute_tool`), `gen_ai.tool.name`
- Recommended (if available): `gen_ai.tool.call.id`,
  `gen_ai.tool.description`, `gen_ai.tool.type`
- Opt-in: `gen_ai.tool.call.arguments`, `gen_ai.tool.call.result`
- Conditionally required: `error.type` (on error)
- Note: MCP tool calls MAY be covered by this span (with MCP-specific
  attributes added) or by an `mcp.client` span; not both.

### 2.8 Span: `gen_ai.invoke_workflow.internal`

- Span kind: `INTERNAL`
- Name: `invoke_workflow {gen_ai.workflow.name}`
- Required: `gen_ai.operation.name` (= `invoke_workflow`)
- Conditionally required: `gen_ai.workflow.name` (when available),
  `error.type` (on error)
- Opt-in: `gen_ai.input.messages`, `gen_ai.output.messages`
- Reported by frameworks that distinguish workflow from agent (e.g. CrewAI
  crews). Not reported by frameworks where workflow agents already emit
  `invoke_agent` spans (e.g. ADK).

### 2.9 Span refinements (provider extensions)

| Refinement id | Base span | Provider | Notes |
| --- | --- | --- | --- |
| `openai.inference.client` | `gen_ai.inference.client` | OpenAI | `gen_ai.provider.name` MUST be `openai`. Adds `openai.api.type`, `openai.request.service_tier`, `openai.response.service_tier`, `openai.response.system_fingerprint`. `gen_ai.request.model` is required. |
| `azure.ai.inference.client` | `gen_ai.inference.client` | Azure AI Inference | `gen_ai.provider.name` MUST be `azure.ai.inference`. Adds `azure.resource_provider.namespace`. `server.port` only required if non-default (443). |
| `anthropic.inference.client` | `gen_ai.inference.client` | Anthropic | `gen_ai.provider.name` MUST be `anthropic`. `top_k` recommended. `gen_ai.usage.input_tokens` = `input_tokens + cache_read_input_tokens + cache_creation_input_tokens`. |
| `aws.bedrock.inference.client` | `gen_ai.inference.client` | AWS Bedrock | `gen_ai.provider.name` MUST be `aws.bedrock`. Adds `aws.bedrock.guardrail.id` (required), `aws.bedrock.knowledge_base.id` (recommended). `top_k` recommended. |

### 2.10 MCP spans

Defined in `model/mcp/spans.yaml`.

#### `mcp.client`
- Span kind: `CLIENT`
- Name: `{mcp.method.name} {target}` where `target` SHOULD match
  `gen_ai.tool.name` or `gen_ai.prompt.name` (or just `{mcp.method.name}`)
- Required: `mcp.method.name`
- Conditionally required: `gen_ai.tool.name` (tool-specific operation),
  `gen_ai.prompt.name` (prompt-specific operation), `mcp.resource.uri` (when
  the request includes a resource URI), `jsonrpc.request.id` (when a request
  rather than a notification), `error.type` (on error),
  `rpc.response.status_code` (if response contains an error code)
- Recommended: `mcp.session.id`, `mcp.protocol.version`,
  `gen_ai.operation.name` (= `execute_tool` for tool calls),
  `network.transport`, `network.protocol.name`, `network.protocol.version`,
  `jsonrpc.protocol.version`, `server.address`, `server.port`
- Opt-in: `gen_ai.tool.call.arguments`, `gen_ai.tool.call.result`
- If outer GenAI instrumentation is already tracing the tool execution, a
  separate `mcp.client` span SHOULD NOT be emitted; instead MCP attributes
  SHOULD be added to the existing `execute_tool` span.

#### `mcp.server`
- Span kind: `SERVER`
- Same shape as `mcp.client` but with `client.address`, `client.port`
  recommended in place of `server.*`.

## 3. Events / log record bodies

Defined in `model/gen-ai/events.yaml`.

In the current schema (post v1.36.0), the historical per-role events
(`gen_ai.user.message`, `gen_ai.system.message`, `gen_ai.assistant.message`,
`gen_ai.tool.message`, `gen_ai.choice`) have been REMOVED and replaced by
- the structured attributes `gen_ai.system_instructions`,
  `gen_ai.input.messages`, `gen_ai.output.messages` (recorded on spans or
  events), and
- a single envelope event `gen_ai.client.inference.operation.details` that
  carries the same attribute set as the inference span.

Instrumentations that still emit the old event names are migrating under the
`OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental` flag; they remain
in v1.36.0 of the conventions but are not part of the current model.

### 3.1 Event: `gen_ai.client.inference.operation.details`

Opt-in. Same attribute set as `gen_ai.inference.client` span (section 2.2):
required `gen_ai.operation.name` and `gen_ai.provider.name`, plus the full
recommended / opt-in attribute set including `gen_ai.input.messages`,
`gen_ai.output.messages`, `gen_ai.system_instructions`,
`gen_ai.tool.definitions`, `gen_ai.conversation.id`, full request / response /
usage attributes.

### 3.2 Event: `gen_ai.evaluation.result`

Captures the result of evaluating a GenAI output. SHOULD be parented to the
operation span being evaluated (or set `gen_ai.response.id`).

| Attribute | Requirement | Type |
| --- | --- | --- |
| `gen_ai.evaluation.name` | required | string |
| `gen_ai.evaluation.score.value` | conditionally required (if applicable) | double |
| `gen_ai.evaluation.score.label` | conditionally required (if applicable) | string |
| `gen_ai.evaluation.explanation` | recommended | string |
| `gen_ai.response.id` | recommended (when available) | string |
| `error.type` | conditionally required (on error) | string |

### 3.3 Event: `gen_ai.client.operation.exception`

Recorded when a Generative AI client operation fails (API errors, rate
limits, timeouts, model errors). Severity SHOULD be `WARN` (severity number
13). Implementations MAY copy attributes from the corresponding client span.

| Attribute | Requirement | Type |
| --- | --- | --- |
| `exception.type` | conditionally required (if `exception.message` not set) | string |
| `exception.message` | conditionally required (if `exception.type` not set) | string |
| `exception.stacktrace` | recommended | string |

### 3.4 JSON Schemas referenced by events / message attributes

The four JSON schemas live under `docs/gen-ai/`. Required for any
instrumentation that records `gen_ai.input.messages`, `gen_ai.output.messages`,
`gen_ai.system_instructions`, `gen_ai.tool.definitions`, or
`gen_ai.retrieval.documents`.

#### 3.4.1 `gen-ai-input-messages.json` (array of `InputMessage`)

`InputMessage` properties:
- `role` (required, enum + free-form string): one of `system`, `user`,
  `assistant`, `tool`, or any string.
- `parts` (required, array of part objects, each polymorphic on `type`).
- `name` (optional string): name of the participant.

Allowed `parts[*].type` discriminators (input messages):
- `text` -> `TextPart { type, content }`
- `tool_call` -> `ToolCallRequestPart { type, name, id?, arguments? }`
- `tool_call_response` -> `ToolCallResponsePart { type, response, id? }`
- `server_tool_call` -> `ServerToolCallPart { type, name, server_tool_call, id? }`
- `server_tool_call_response` -> `ServerToolCallResponsePart { type, server_tool_call_response, id? }`
- `blob` -> `BlobPart { type, modality, content (base64), mime_type? }`
- `file` -> `FilePart { type, modality, file_id, mime_type? }`
- `uri` -> `UriPart { type, modality, uri, mime_type? }`
- `reasoning` -> `ReasoningPart { type, content }`
- any other string -> `GenericPart { type, ... }` (extensible)

`Modality` enum: `image`, `video`, `audio`.

#### 3.4.2 `gen-ai-output-messages.json` (array of `OutputMessage`)

Same part types as input messages plus a required `finish_reason` field on
each message.

`FinishReason` enum: `stop`, `length`, `content_filter`, `tool_call`,
`error` (free-form strings allowed).

#### 3.4.3 `gen-ai-system-instructions.json` (array of parts)

Array of parts, each structured like a message part (`text`, `blob`, `file`,
`uri`, etc.). Most commonly:
```
[{ "type": "text", "content": "<system prompt>" }]
```

#### 3.4.4 `gen-ai-tool-definitions.json` (array of tool definitions)

Each item is either:
- `FunctionToolDefinition { type: "function", name, description?, parameters? (JSON Schema draft-07) }`
- `GenericToolDefinition { type, name, ... }` (extensible)

#### 3.4.5 `gen-ai-retrieval-documents.json` (array of documents)

`RetrievalDocument { id (string, required), score (number, required), ... }`
(extensible).

## 4. Metrics

### 4.1 GenAI metrics (`model/gen-ai/metrics.yaml`)

| Name | Instrument | Unit | Stability | Brief | Required attributes |
| --- | --- | --- | --- | --- | --- |
| `gen_ai.client.token.usage` | histogram | `{token}` | Development | Number of input and output tokens used. | `gen_ai.provider.name`, `gen_ai.operation.name`, `gen_ai.token.type` (`input`/`output`) |
| `gen_ai.client.operation.duration` | histogram | `s` | Development | GenAI operation duration. | `gen_ai.provider.name`, `gen_ai.operation.name`. `error.type` conditionally required on errors. |
| `gen_ai.client.operation.time_to_first_chunk` | histogram | `s` | Development | Time to receive the first chunk of a streaming response. | `gen_ai.provider.name`, `gen_ai.operation.name`. Streaming only. |
| `gen_ai.client.operation.time_per_output_chunk` | histogram | `s` | Development | Time per output chunk after the first. | `gen_ai.provider.name`, `gen_ai.operation.name`. Streaming only. |
| `gen_ai.server.request.duration` | histogram | `s` | Development | Server-side request duration (time-to-last-byte / last token). | `gen_ai.provider.name`, `gen_ai.operation.name`. `error.type` conditionally on errors. |
| `gen_ai.server.time_per_output_token` | histogram | `s` | Development | Time per output token after the first. | `gen_ai.provider.name`, `gen_ai.operation.name`. |
| `gen_ai.server.time_to_first_token` | histogram | `s` | Development | Time to first token for successful responses. | `gen_ai.provider.name`, `gen_ai.operation.name`. |

Common recommended metric attributes (`metric_attributes.gen_ai`):
`server.address`, `server.port` (if address set), `gen_ai.request.model`,
`gen_ai.response.model`, `gen_ai.operation.name`, `gen_ai.provider.name`.

OpenAI metric refinements add recommended `openai.response.service_tier` and
`openai.response.system_fingerprint` to:
- `openai.client.token.usage` (refines `gen_ai.client.token.usage`)
- `openai.client.operation.duration` (refines `gen_ai.client.operation.duration`)

### 4.2 MCP metrics (`model/mcp/metrics.yaml`)

| Name | Instrument | Unit | Stability | Brief |
| --- | --- | --- | --- | --- |
| `mcp.client.operation.duration` | histogram | `s` | Development | Duration of an MCP request / notification observed on the sender. |
| `mcp.server.operation.duration` | histogram | `s` | Development | Duration observed on the receiver. |
| `mcp.client.session.duration` | histogram | `s` | Development | Duration of the MCP session, observed on the client. |
| `mcp.server.session.duration` | histogram | `s` | Development | Duration of the MCP session, observed on the server. |

Operation metric attributes (required: `mcp.method.name`; conditionally
required: `error.type`, `rpc.response.status_code`, `gen_ai.tool.name`,
`gen_ai.prompt.name`; recommended: `mcp.protocol.version`, `network.*`,
`jsonrpc.protocol.version`, `server.address` / `server.port`).
Session metric attributes use only `mcp.protocol.version`, `network.*`, plus
`error.type` if the session ends with an error.

## 5. Value registries

### 5.1 `gen_ai.provider.name`

All `Development`. Values:
`openai`, `gcp.gen_ai`, `gcp.vertex_ai`, `gcp.gemini`, `anthropic`, `cohere`,
`azure.ai.inference`, `azure.ai.openai`, `ibm.watsonx.ai`, `aws.bedrock`,
`perplexity`, `x_ai`, `deepseek`, `groq`, `mistral_ai`.

Other values are allowed but should be documented by instrumentation.

### 5.2 `gen_ai.operation.name`

All `Development`. Values: `chat`, `generate_content`, `text_completion`,
`embeddings`, `retrieval`, `create_agent`, `invoke_agent`, `execute_tool`,
`invoke_workflow`. Provider-specific operations may use additional values.

### 5.3 `gen_ai.response.finish_reasons` element / `OutputMessage.finish_reason`

Defined in the JSON schema, not in the YAML registry. Standard values:
`stop`, `length`, `content_filter`, `tool_call`, `error`. Free-form strings
allowed for provider-specific reasons.

### 5.4 `gen_ai.token.type`

Values: `input`, `output`. The legacy member id `completion` (value `output`)
exists for backwards compatibility, deprecated and renamed to `output`.

### 5.5 `gen_ai.output.type`

Values: `text`, `json`, `image`, `speech`. Future expansion may add
`gen_ai.output.{type}.*` attributes.

### 5.6 `gen_ai.tool.type` (open string, common values)

`function` (client-side execution given parameters from the model),
`extension` (agent-side, agent calls external API directly), `datastore`
(retrieval-augmented data access).

### 5.7 `openai.api.type`

Values: `chat_completions`, `responses`.

### 5.8 `openai.request.service_tier`

Values: `auto`, `default`. (Other strings allowed.)

### 5.9 `mcp.method.name`

JSON-RPC method names. All `Development`.

| Value | Brief |
| --- | --- |
| `notifications/cancelled` | Cancel a previously-issued request. |
| `initialize` | Initialize the MCP client. |
| `notifications/initialized` | Client has been initialized. |
| `notifications/progress` | Progress for a long-running operation. |
| `ping` | Liveness check. |
| `resources/list` | List resources. |
| `resources/templates/list` | List resource templates. |
| `resources/read` | Read a resource. |
| `notifications/resources/list_changed` | Resource list changed. |
| `resources/subscribe` | Subscribe to a resource. |
| `resources/unsubscribe` | Unsubscribe from a resource. |
| `notifications/resources/updated` | A resource has been updated. |
| `prompts/list` | List prompts. |
| `prompts/get` | Get a prompt. |
| `notifications/prompts/list_changed` | Prompt list changed. |
| `tools/list` | List tools. |
| `tools/call` | Call a tool. |
| `notifications/tools/list_changed` | Tool list changed. |
| `logging/setLevel` | Set logging level. |
| `notifications/message` | Log / message notification. |
| `sampling/createMessage` | Create a sampling message. |
| `completion/complete` | Complete a prompt. |
| `roots/list` | List roots. |
| `notifications/roots/list_changed` | Root list changed. |
| `elicitation/create` | Server requests additional information from the user via the client. |

## 6. Notes on stability and migration

- Every `gen_ai.*`, `openai.*`, `aws.bedrock.*`, and `mcp.*` attribute, span,
  event, and metric in the active model is `Development` (experimental).
- The pre-v1.36.0 per-role events (`gen_ai.user.message`,
  `gen_ai.system.message`, `gen_ai.assistant.message`, `gen_ai.tool.message`,
  `gen_ai.choice`) are no longer in the current model. Instrumentations that
  emitted them should keep doing so by default and adopt the new model when
  `OTEL_SEMCONV_STABILITY_OPT_IN=gen_ai_latest_experimental` is set.
- The `gen_ai.token.type` value `completion` is deprecated and renamed to
  `output`.
- Span kind for inference spans is normally `CLIENT`; `INTERNAL` is allowed
  for in-process model calls.
- Span status SHOULD be `ERROR` whenever `error.type` is present.
- Sensitive attributes (`gen_ai.input.messages`, `gen_ai.output.messages`,
  `gen_ai.system_instructions`, `gen_ai.tool.call.arguments`,
  `gen_ai.tool.call.result`, `gen_ai.tool.definitions`,
  `gen_ai.retrieval.query.text`, `gen_ai.retrieval.documents`) are opt-in and
  instrumentations MAY allow filtering or truncation.

## 7. Source files used (relative to clone root)

YAML model:
- `model/gen-ai/registry.yaml`
- `model/gen-ai/spans.yaml`
- `model/gen-ai/events.yaml`
- `model/gen-ai/metrics.yaml`
- `model/openai/registry.yaml`
- `model/aws-bedrock/registry.yaml`
- `model/mcp/registry.yaml`
- `model/mcp/common.yaml`
- `model/mcp/spans.yaml`
- `model/mcp/metrics.yaml`

Rendered docs (cross-checked against website):
- `docs/gen-ai/gen-ai-spans.md`
- `docs/gen-ai/gen-ai-agent-spans.md`
- `docs/gen-ai/gen-ai-events.md`
- `docs/gen-ai/gen-ai-metrics.md`
- `docs/gen-ai/gen-ai-exceptions.md`
- `docs/gen-ai/openai.md`
- `docs/gen-ai/anthropic.md`
- `docs/gen-ai/azure-ai-inference.md`
- `docs/gen-ai/aws-bedrock.md`
- `docs/gen-ai/mcp.md`

JSON schemas (mandatory for structured `gen_ai.*` array attributes):
- `docs/gen-ai/gen-ai-input-messages.json`
- `docs/gen-ai/gen-ai-output-messages.json`
- `docs/gen-ai/gen-ai-system-instructions.json`
- `docs/gen-ai/gen-ai-tool-definitions.json`
- `docs/gen-ai/gen-ai-retrieval-documents.json`
