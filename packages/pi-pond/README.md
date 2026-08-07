# pi-pond

Give [pi](https://github.com/earendil-works/pi) a memory that outlives the session.

`pi-pond` captures your pi sessions into a durable [pond](https://github.com/tenequm/pond) archive and projects pond's four read-only recall tools back into pi, so the agent can search and read **every past agent session on every machine** - pi, Claude Code, Codex, OpenClaw - not just this one. Plus a `/pond` command that finds an old session and resumes it right here.

```
pi install npm:pi-pond
```

That is the whole setup, assuming pond is installed. On first use the extension starts a pond that both serves the tools and keeps the archive current; if pond has no adapters configured at all, it enables the pi one itself.

## Install pond

```
brew install tenequm/tap/pond
```

or `cargo install pond`. Nothing else is required - `pond init` is only for building a cross-harness corpus by hand.

## What you get

**Four tools, available to the model.** `pond_search`, `pond_get_session`, `pond_get_message`, `pond_sql`. They return pond's own rendered transcripts unmodified, so the "read this next" hints pond writes reach the model intact. They default to the whole archive: cross-agent recall is the point.

**Capture.** The managed pond runs a periodic sync (every 5 minutes by default) that tails pi's session files. Nothing is pushed; pond reads what pi already writes.

**`/pond <query>`.** Searches, then shows the matching sessions:

- `enter` - **resume**: pond writes the session back out as a pi session file and switches this pi to it. Sessions captured from pi come back value-complete; sessions from another agent come back as a best-effort pi transcript. Resuming the same session twice just reopens the file you already have.
- `i` - **insert**: pastes a compact reference (session id, agent, date, snippet) into the editor. Not the transcript - the model pulls detail through the tools when it needs it.
- `esc` - close.

## What you do NOT get, on purpose

No memory slot, no auto-recall, no prompt injection, no summarizing. pond is an archive with a query surface; deciding what is worth remembering is the agent's job, not the plumbing's. Every pond MCP surface is hard-enforced read-only - the one thing that writes is `pond resume`, and only when you press enter.

## Configuration

Optional, at `~/.pi/agent/pond-pi.json`:

```json
{
  "mode": "managed",
  "syncIntervalMinutes": 5,
  "binaryPath": "/opt/homebrew/bin/pond"
}
```

- `mode: "managed"` (default) - the extension supervises its own `pond serve` child for the lifetime of the pi session, started lazily on the first tool call.
- `mode: "url"` - talk to an external `pond serve --transport http` instead, so many pi sessions share one process and one embedding model:

  ```json
  { "mode": "url", "url": "http://127.0.0.1:9797/mcp" }
  ```

The same file records your answer to the one-time capture prompt (`captureConsent`). You are asked once, only in an interactive session, and only when pond is already capturing something else on this machine but not pi - the case pond's own `--bootstrap` cannot cover. Either answer is remembered.

The prompt is not the only way your pond config can change. On a pond with **no** adapters configured at all, the managed child's `--bootstrap pi-coding-agent` enables the pi adapter on its first run, without asking - that is what makes the zero-config install work. It never touches a pond that already has adapters, and a disabled adapter stays disabled. If you would rather nothing be written on your behalf, run `pond init` first or use `mode: "url"`.

Where the archive lives is pond's business, not this extension's: set `POND_STORAGE_PATH` (or run `pond storage use <url>`) to point it at S3 or a shared volume.

## License

MIT
