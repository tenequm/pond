// The `pond resume` bridge, driven against the exact JSON documents the verb
// emits (packages/pond/src/main.rs::run_resume).
import { describe, expect, it, vi } from "vitest";
import { resumeSession } from "../src/resume.ts";

const SESSION = "019dd55d-99a4-7344-aa11-d1d71d2c80fb";
const FILE = `/home/u/.pi/agent/sessions/--home-u-proj--/2026-08-06T00-00-01-000Z_${SESSION}.jsonl`;
const CHILD_FILE = "/home/u/.pi/agent/sessions/--home-u-proj--/2026-08-06T00-00-09-000Z_child.jsonl";

function exec(stdout: string, code = 0, stderr = "") {
  return vi.fn(async () => ({ stdout, stderr, code }));
}

describe("resumeSession", () => {
  it("asks pond for a pi session under pi's own agent dir", async () => {
    const run = exec(
      JSON.stringify({
        adapter: "pi-coding-agent",
        out_dir: "/home/u/.pi/agent",
        sessions: [
          { session_id: SESSION, actual_fidelity: "native", files: [FILE] },
        ],
      }),
    );
    const outcome = await resumeSession({
      exec: run,
      binary: "/usr/local/bin/pond",
      sessionId: SESSION,
      outDir: "/home/u/.pi/agent",
    });
    expect(run).toHaveBeenCalledWith(
      "/usr/local/bin/pond",
      [
        "resume",
        SESSION,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        "/home/u/.pi/agent",
        "--format",
        "json",
      ],
      expect.objectContaining({ timeout: expect.any(Number) }),
    );
    expect(outcome).toEqual({
      ok: true,
      sessionFile: FILE,
      fidelity: "native",
      alreadyResumed: false,
    });
  });

  it("treats an already-resumed session as success and switches to the existing file", async () => {
    const outcome = await resumeSession({
      exec: exec(
        JSON.stringify({ error: "already_exists", session_id: SESSION, existing: [FILE] }),
        3,
      ),
      binary: "pond",
      sessionId: SESSION,
      outDir: "/home/u/.pi/agent",
    });
    expect(outcome).toEqual({
      ok: true,
      sessionFile: FILE,
      fidelity: "existing",
      alreadyResumed: true,
    });
  });

  it("refuses to switch to a lineage sibling when the session's own file is gone", async () => {
    // `existing` spans parent AND children, so a fallback here would open a
    // different session than the one reported as already resumed.
    const outcome = await resumeSession({
      exec: exec(
        JSON.stringify({ error: "already_exists", session_id: SESSION, existing: [CHILD_FILE] }),
        3,
      ),
      binary: "pond",
      sessionId: SESSION,
      outDir: "/home/u/.pi/agent",
    });
    expect(outcome).toMatchObject({ ok: false });
    expect(outcome).toHaveProperty("error", expect.stringContaining(CHILD_FILE));
    expect(outcome).toHaveProperty("error", expect.stringContaining("lineage"));
  });

  it("picks the session's own file out of an already-resumed lineage", async () => {
    const outcome = await resumeSession({
      exec: exec(
        JSON.stringify({ error: "already_exists", session_id: SESSION, existing: [CHILD_FILE, FILE] }),
        3,
      ),
      binary: "pond",
      sessionId: SESSION,
      outDir: "/home/u/.pi/agent",
    });
    expect(outcome).toMatchObject({ ok: true, sessionFile: FILE, alreadyResumed: true });
  });

  it("picks the requested session's own file out of a restored lineage", async () => {
    const outcome = await resumeSession({
      exec: exec(
        JSON.stringify({
          sessions: [
            { session_id: "child", actual_fidelity: "native", files: [CHILD_FILE] },
            { session_id: SESSION, actual_fidelity: "native", files: [FILE] },
          ],
        }),
      ),
      binary: "pond",
      sessionId: SESSION,
      outDir: "/home/u/.pi/agent",
    });
    expect(outcome).toMatchObject({ ok: true, sessionFile: FILE });
  });

  it("surfaces a not-found as an error rather than switching to nothing", async () => {
    const outcome = await resumeSession({
      exec: exec(JSON.stringify({ error: "not_found", session_id: SESSION }), 1),
      binary: "pond",
      sessionId: SESSION,
      outDir: "/home/u/.pi/agent",
    });
    expect(outcome).toEqual({ ok: false, error: "pond resume: not_found" });
  });

  it("reports stderr when pond emitted no JSON at all", async () => {
    const outcome = await resumeSession({
      exec: exec("", 2, "unknown client \"pi\". Known: claude-code, pi-coding-agent"),
      binary: "pond",
      sessionId: SESSION,
      outDir: "/home/u/.pi/agent",
    });
    expect(outcome).toMatchObject({ ok: false });
    expect(outcome).toHaveProperty("error", expect.stringContaining("unknown client"));
  });
});
