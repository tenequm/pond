// Parse the per-session rule lines out of pond's rendered search transcript so
// `/pond` can offer a picker over them.
//
// Why parse text instead of asking for JSON: pond's MCP surface renders text,
// and the alternative - shelling out to `pond search --format json` - would
// cold-load a second ~500 MB embedding model per keystroke. The bridge already
// holds a warm one. The parse is deliberately narrow: it reads only the
// `--- session [n] ... ---` header line (a stable, delimiter-shaped contract)
// plus the plain lines under the first hit as a snippet, and silently yields
// nothing if the shape ever changes - the tools keep working either way.
export type PondHit = {
  sessionId: string;
  project: string;
  sourceAgent: string;
  /** ISO-ish timestamp of the first rendered hit in this session, if present. */
  timestamp?: string;
  snippet: string;
};

const SESSION_RULE = /^-{3}\s+session \[\d+\][^|]*\|[^|]*\|\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s+-{3,}$/;
const HIT_RULE = /^-{3}\s+\[\d+\][^|]*\|[^|]*\|\s*(.+?)\s*\|\s*(.+?)\s*\|/;

function firstLine(text: string, max = 160): string {
  const line = text.split("\n").find((candidate) => candidate.trim().length > 0) ?? "";
  const trimmed = line.trim();
  return trimmed.length > max ? `${trimmed.slice(0, max - 1)}...` : trimmed;
}

export function parsePondHits(transcript: string): PondHit[] {
  const hits: PondHit[] = [];
  let current: PondHit | undefined;
  let snippetLines: string[] = [];
  let collecting = false;

  const flush = () => {
    if (current) {
      current.snippet = firstLine(snippetLines.join("\n"));
      hits.push(current);
    }
    current = undefined;
    snippetLines = [];
    collecting = false;
  };

  for (const line of transcript.split("\n")) {
    const session = SESSION_RULE.exec(line);
    if (session) {
      flush();
      current = {
        sessionId: session[3] ?? "",
        project: session[1] ?? "",
        sourceAgent: session[2] ?? "",
        snippet: "",
      };
      continue;
    }
    if (!current) {
      continue;
    }
    const hit = HIT_RULE.exec(line);
    if (hit) {
      // Only the FIRST hit of a session contributes the snippet; later ones
      // would just repeat the same session in the picker.
      if (snippetLines.length === 0) {
        current.timestamp = hit[1];
        collecting = true;
      } else {
        collecting = false;
      }
      continue;
    }
    if (collecting && line.trim().length > 0 && !line.startsWith("...")) {
      snippetLines.push(line);
    }
  }
  flush();
  return hits.filter((hit) => hit.sessionId.length > 0);
}

/** One picker row: what the user reads before choosing. */
export function hitLabel(hit: PondHit): string {
  const when = hit.timestamp ? hit.timestamp.split(" ")[0] : undefined;
  const where = hit.project.split("/").filter(Boolean).pop() ?? hit.project;
  return [when, hit.sourceAgent, where].filter(Boolean).join("  ");
}

/**
 * The compact reference block `/pond`'s insert action pastes into the editor.
 * Deliberately NOT the transcript: pond stays out of curation (spec 2.3), so
 * the model pulls detail through the tools instead of having it force-fed.
 */
export function hitReference(hit: PondHit): string {
  return [
    `Past session ${hit.sessionId}`,
    `  agent: ${hit.sourceAgent}`,
    `  project: ${hit.project}`,
    ...(hit.timestamp ? [`  when: ${hit.timestamp}`] : []),
    ...(hit.snippet ? [`  snippet: ${hit.snippet}`] : []),
    `Full transcript: use pond_get_session with id ${hit.sessionId}`,
  ].join("\n");
}
