// `pond resume` bridge: turn a stored session back into a pi session file and
// hand back the path to switch to.
//
// This is the one place the extension shells out rather than using the MCP
// bridge, and deliberately so: every pond MCP surface is hard-enforced
// read-only, and resume writes files. It is an operator action on the CLI, and
// it is cheap - no embedding model is loaded to serialize a session.
//
// Idempotence rides on pond's own refusal to overwrite: a second resume of the
// same session exits 3 and names the file that already exists, so "resume
// again" becomes "open the one you already have" instead of an error.

export type ExecLike = (
  command: string,
  args: string[],
  options?: { signal?: AbortSignal; timeout?: number },
) => Promise<{ stdout: string; stderr: string; code: number | null }>;

export type ResumeOutcome =
  | { ok: true; sessionFile: string; fidelity: string; alreadyResumed: boolean }
  | { ok: false; error: string };

const RESUME_TIMEOUT_MS = 60_000;

function asStrings(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : [];
}

/**
 * The `.jsonl` a pi session lives in. `pond resume` may write several files for
 * one lineage (a session plus its children), so the match is exact - the caller
 * decides whether falling back to another file in the list is safe.
 */
function pickSessionFile(files: string[], sessionId: string): string | undefined {
  return files.find((file) => file.endsWith(`_${sessionId}.jsonl`));
}

export async function resumeSession(params: {
  exec: ExecLike;
  binary: string;
  sessionId: string;
  /** Root the adapter's own layout is written under - `~/.pi/agent` for pi. */
  outDir: string;
  signal?: AbortSignal;
}): Promise<ResumeOutcome> {
  let result: Awaited<ReturnType<ExecLike>>;
  try {
    result = await params.exec(
      params.binary,
      [
        "resume",
        params.sessionId,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        params.outDir,
        "--format",
        "json",
      ],
      { ...(params.signal ? { signal: params.signal } : {}), timeout: RESUME_TIMEOUT_MS },
    );
  } catch (error) {
    return { ok: false, error: `pond resume failed to run: ${String(error)}` };
  }

  let doc: unknown;
  try {
    doc = JSON.parse(result.stdout);
  } catch {
    return {
      ok: false,
      error: result.stderr.trim() || `pond resume produced no JSON (exit ${result.code}).`,
    };
  }
  const document = (doc ?? {}) as Record<string, unknown>;

  // Exit 3 / already_exists is the happy path for a re-resume: the file pond
  // refused to overwrite is exactly the one to switch to.
  if (document.error === "already_exists") {
    // `existing` spans the WHOLE lineage (parent plus children), so there is no
    // safe fallback here: switching to a sibling's file would silently open a
    // different session than the one that was asked for.
    const existing = asStrings(document.existing);
    const file = pickSessionFile(existing, params.sessionId);
    if (file) {
      return { ok: true, sessionFile: file, fidelity: "existing", alreadyResumed: true };
    }
    return {
      ok: false,
      error:
        existing.length === 0
          ? "pond resume reported an existing file but did not name it."
          : `pond resume: the stored file for ${params.sessionId} is missing, but other files ` +
            `from its lineage are still on disk (${existing.join(", ")}). Move them aside and ` +
            "resume again, or resume the child session directly.",
    };
  }
  if (typeof document.error === "string") {
    return { ok: false, error: `pond resume: ${document.error}` };
  }

  const sessions = Array.isArray(document.sessions) ? document.sessions : [];
  const target = sessions
    .map((entry) => (entry ?? {}) as Record<string, unknown>)
    .find((entry) => entry.session_id === params.sessionId);
  if (!target) {
    return { ok: false, error: "pond resume did not report the requested session." };
  }
  // `target.files` is this one session's own list, so the first entry is a safe
  // fallback for an adapter that names files by something other than the id.
  const files = asStrings(target.files);
  const sessionFile = pickSessionFile(files, params.sessionId) ?? files[0];
  if (!sessionFile) {
    return { ok: false, error: "pond resume reported no file for the session." };
  }
  return {
    ok: true,
    sessionFile,
    fidelity: typeof target.actual_fidelity === "string" ? target.actual_fidelity : "unknown",
    alreadyResumed: false,
  };
}
