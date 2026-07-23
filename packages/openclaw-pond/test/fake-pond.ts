// A fake pond MCP endpoint over an in-memory transport pair. Records the tool
// arguments the plugin forwards (golden-request assertions) and returns canned
// text (golden-response assertions), matching pond's real text-only tool output.
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from "@modelcontextprotocol/sdk/types.js";
import { PondMcpClient } from "../src/mcp.js";

// `rpcError` mirrors pond's envelope-error channel (Err(to_error_data) ->
// JSON-RPC error -> client throws McpError): not_found, validation_failed, the
// get_message wrong-id hint. Plain string / isError mirror CallToolResult text.
export type FakeResponse = string | { text: string; isError: true } | { rpcError: string };

export type FakePondOptions = {
  responses?: Partial<Record<string, (args: Record<string, unknown>) => FakeResponse>>;
};

export type RecordedCall = { name: string; args: Record<string, unknown> };

export type FakePond = {
  client: PondMcpClient;
  calls: RecordedCall[];
  close: () => Promise<void>;
};

const TOOL_NAMES = ["pond_search", "pond_get_session", "pond_get_message", "pond_sql"];

export async function createFakePond(options: FakePondOptions = {}): Promise<FakePond> {
  const calls: RecordedCall[] = [];
  const server = new Server(
    { name: "fake-pond", version: "0.0.0" },
    { capabilities: { tools: {} } },
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: TOOL_NAMES.map((name) => ({
      name,
      description: name,
      inputSchema: { type: "object" as const },
    })),
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const name = request.params.name;
    const args = (request.params.arguments ?? {}) as Record<string, unknown>;
    calls.push({ name, args });
    const responder = options.responses?.[name];
    const response: FakeResponse = responder ? responder(args) : `ok:${name}`;
    if (typeof response === "string") {
      return { content: [{ type: "text", text: response }] };
    }
    if ("rpcError" in response) {
      throw new McpError(ErrorCode.InvalidParams, response.rpcError);
    }
    return { content: [{ type: "text", text: response.text }], isError: true };
  });

  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  const client = new PondMcpClient();
  await client.connect(clientTransport);

  return {
    client,
    calls,
    close: async () => {
      await client.close();
      await server.close();
    },
  };
}
