// Test double for `openclaw/plugin-sdk/logging-core`.
// The real `redactToolPayloadText` masks secret-shaped substrings with a hint:
// first 6 chars + "…" + last 4 for tokens >= 18 chars, "***" below that
// (src/logging/redact.ts maskToken). Mirror that format so assertions written
// against this double stay green against the real SDK.
function maskToken(token: string): string {
  if (token.length < 18) return "***";
  return `${token.slice(0, 6)}…${token.slice(-4)}`;
}

export function redactToolPayloadText(text: string): string {
  return text
    .replace(/\b(?:sk|pk|rk)-[A-Za-z0-9_-]{16,}\b/g, maskToken)
    .replace(/\bBearer\s+([A-Za-z0-9._-]{12,})\b/gi, (_, token: string) => `Bearer ${maskToken(token)}`)
    .replace(/\bghp_[A-Za-z0-9]{20,}\b/g, maskToken);
}
