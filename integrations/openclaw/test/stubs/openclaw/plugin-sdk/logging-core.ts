// Test double for `openclaw/plugin-sdk/logging-core`.
// The real `redactToolPayloadText` masks secret-shaped substrings before a tool
// payload is surfaced. This double reproduces enough of that behavior (bearer
// tokens, sk- keys, generic long hex/base64 secrets) for tests to assert
// redaction happens; the real implementation runs in production.
export function redactToolPayloadText(text: string): string {
  return text
    .replace(/\b(sk|pk|rk)-[A-Za-z0-9_-]{16,}\b/g, "[redacted]")
    .replace(/\bBearer\s+[A-Za-z0-9._-]{12,}\b/gi, "Bearer [redacted]")
    .replace(/\bghp_[A-Za-z0-9]{20,}\b/g, "[redacted]");
}
