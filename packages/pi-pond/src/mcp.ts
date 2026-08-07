// Thin MCP client over the official SDK. One client, two dial modes: a child
// `pond serve --transport stdio` (managed) or an external `pond serve` over
// streamable HTTP (url). Transports are injectable so tests drive an in-memory
// pair against a fake pond endpoint.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";

export type PondCallResult = { ok: true; text: string } | { ok: false; error: string };

const CLIENT_INFO = { name: "pi-pond", version: "0.1.0" } as const;

function extractText(content: unknown): string {
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .filter(
      (part): part is { type: "text"; text: string } =>
        !!part &&
        typeof part === "object" &&
        (part as { type?: unknown }).type === "text" &&
        typeof (part as { text?: unknown }).text === "string",
    )
    .map((part) => part.text)
    .join("\n");
}

export class PondMcpClient {
  private client: Client | null = null;

  get connected(): boolean {
    return this.client !== null;
  }

  async connect(
    transport: Transport,
    opts?: { onClose?: () => void; timeoutMs?: number },
  ): Promise<void> {
    const client = new Client(CLIENT_INFO, { capabilities: {} });
    if (opts?.onClose) {
      client.onclose = opts.onClose;
    }
    // The initialize handshake is where a hung child actually stalls; the SDK
    // default is 60s, so callers pass a deadline suited to a local sidecar.
    try {
      await client.connect(transport, opts?.timeoutMs ? { timeout: opts.timeoutMs } : undefined);
    } catch (error) {
      // A failed handshake may leave a spawned-but-hung child behind; closing
      // the client runs the transport teardown ladder so it cannot accumulate
      // across restart attempts.
      await client.close().catch(() => {});
      throw error;
    }
    this.client = client;
  }

  async listToolNames(opts?: { timeoutMs?: number }): Promise<string[]> {
    if (!this.client) {
      throw new Error("pond MCP client is not connected");
    }
    const result = await this.client.listTools(
      undefined,
      opts?.timeoutMs ? { timeout: opts.timeoutMs } : undefined,
    );
    return result.tools.map((tool) => tool.name);
  }

  async callTool(name: string, args: Record<string, unknown>): Promise<PondCallResult> {
    if (!this.client) {
      return { ok: false, error: "pond is not connected yet; the pond service is still starting" };
    }
    const result = (await this.client.callTool({ name, arguments: args })) as {
      content?: unknown;
      isError?: boolean;
    };
    const text = extractText(result.content);
    if (result.isError === true) {
      return { ok: false, error: text || `pond tool ${name} reported an error` };
    }
    return { ok: true, text };
  }

  async close(): Promise<void> {
    const client = this.client;
    this.client = null;
    if (client) {
      await client.close();
    }
  }
}

export function createStdioTransport(params: {
  command: string;
  args: string[];
  env?: Record<string, string>;
}): Transport {
  return new StdioClientTransport({
    command: params.command,
    args: params.args,
    ...(params.env ? { env: params.env } : {}),
  });
}

export function createHttpTransport(params: {
  url: string;
  headers?: Record<string, string>;
}): Transport {
  return new StreamableHTTPClientTransport(new URL(params.url), {
    ...(params.headers ? { requestInit: { headers: params.headers } } : {}),
  });
}
