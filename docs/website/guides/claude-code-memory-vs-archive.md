---
title: Claude Code memory vs a session archive
description: Claude Code persistent memory (CLAUDE.md, auto memory) keeps distilled facts; a session archive keeps the transcripts. What each is for, and how to run both.
---

# Claude Code memory vs a session archive [Memory carries a few facts into every session; an archive keeps the sessions those facts came from]

Claude Code has persistent memory built in, in two forms: `CLAUDE.md` files that you write, and auto memory, notes Claude writes for itself under `~/.claude/projects/<project>/memory/`. Both are loaded at the start of every session and both survive across sessions. Neither one is the conversation: transcripts are separate files, and Claude Code deletes them after 30 days by default. So the best memory tool for Claude Code depends on what you want remembered - a small set of facts that is always in context, or the full record of past sessions that you search when you need it. They are different jobs, and most setups want both.

## What memory is for

Memory is small, always loaded, and changes how the agent behaves without being asked. That is its strength.

**Built into Claude Code** ([How Claude remembers your project](https://code.claude.com/docs/en/memory), checked 2026-11-24):

- `CLAUDE.md` (or a repository's `AGENTS.md`) holds instructions you write: conventions, commands, architecture. It loads in full every session.
- Auto memory is on by default in local sessions. Claude saves four kinds of notes - your role and preferences, corrections you gave it, project decisions it cannot derive from the code, and pointers to outside resources - and the first 200 lines or 25KB of its `MEMORY.md` index load at session start. The files are machine-local and are not swept by the 30-day cleanup.

**Built into Codex** ([Memories](https://learn.chatgpt.com/docs/customization/memories), checked 2026-11-24): `AGENTS.md` for instructions, plus Memories, which are off by default (`[features] memories = true`). When on, Codex turns eligible past chats into local memory files in the background and injects them into later sessions.

**Memory layers** such as Mem0, Letta and claude-mem do the same job with more machinery: an extraction step decides what is worth keeping and a store hands it back. [How pond compares](/docs/compare#memory-layers) goes through them one by one.

If you want an agent that knows you use pnpm, that the API tests need a local Redis, and that the last reviewer asked for smaller PRs, this is the tool. An archive is the wrong shape for it: nothing in an archive is in context until something asks.

## What memory does not keep

The session. Every memory system works by deciding, at the time, what was worth keeping. What it passed over is not recoverable from it later.

Claude Code is explicit about what auto memory passes over. Its docs say Claude skips anything it can derive from the codebase, "such as architecture, file paths, or debugging fixes", and that it does not save something every session. That is a sound rule for a file that loads into every context window. It also means the debugging session itself - the error text, the three things that did not work, the command that finally did - is exactly what memory is designed not to hold. That lives in the transcript, and the transcript is on a [30-day timer](/docs/guides/claude-code-session-retention).

## What an archive does that memory does not

A session archive keeps the transcripts instead of a summary of them: every message, tool call, tool result and reasoning block, as the client wrote it. With pond that means:

- **Recall of the specifics.** "How did we fix the flaky upload test" returns the conversation where you fixed it, with the commands that were run, not a one-line note that it was fixed.
- **History from before you installed anything.** pond ingests the session files already on disk. A memory layer starts empty.
- **Every client in one place.** Claude Code's memory is Claude Code's; Codex's is Codex's. One search covers both, and [twelve other clients](/docs/guides/where-claude-code-stores-sessions).
- **Every machine.** Auto memory is machine-local. Several machines can sync into [one bucket](/docs/guides/several-machines).
- **No capture-time filter.** Nothing was thrown away, so a question nobody thought to ask at the time still has an answer. A memory layer can be rebuilt from an archive with whatever rules you like this month; an archive cannot be rebuilt from memories.
- **Counting.** The transcripts are also [rows you can query](/docs/guides/sql-analytics).

## What an archive does not do

It is not memory, and it is worth being exact about the difference.

- **Nothing loads by itself.** The agent has to search, which is a tool call, and what comes back costs tokens at that moment. An archive does not make the agent follow a rule.
- **It answers what you ask.** Full-text search matches whole words, so a vague question finds less than a specific one. Semantic search is available and off by default.
- **It keeps everything, secrets included.** If a tool printed a credential into a session, it is in the archive. pond does not redact today.
- **It only knows sessions it has seen.** A transcript deleted before the first sync is not in it.

## Memory layer or full transcripts?

| You want the agent to | Use |
|---|---|
| always follow a rule or convention | `CLAUDE.md` / `AGENTS.md` |
| pick up your corrections without you writing them down | auto memory, Codex Memories, or a memory layer |
| find how something was solved months ago, with the exact commands | an archive, searched on demand |
| answer "which tools fail most" or "what did we work on in March" | an archive, queried with SQL |
| continue an old session | the client's own resume while the file exists, [an archive after](/docs/guides/resume-session-in-another-client) |

The first two rows are about behavior. The last three are about the record. A memory tool is not a worse archive and an archive is not a worse memory tool.

## Run both

They do not overlap, so the setup is additive.

::::steps

## Leave memory on

Keep your `CLAUDE.md` and leave auto memory enabled. This is where preferences and standing rules belong.

## Add the archive

```sh
pond init              # storage, adapters, MCP server and the agent skill
pond sync              # ingest what is on disk now
pond schedule start    # keep it current, every 5 minutes
```

`pond init` registers pond as an MCP server for Claude Code and installs a skill that tells the agent when to reach for it. See [Connect your agents](/docs/get-started/connect-your-agents) for Codex and other clients.

## Point memory at the archive

Memory is good at holding one standing instruction, so give it this one, in `CLAUDE.md`:

```text
Before re-solving a problem that looks familiar, search pond for how it was solved before.
```

::::

After that the division of labor is plain: memory holds the short list of things that should always be true, and the archive holds everything that actually happened, for the times a short list is not enough.

## Next

- [Install](/docs/get-started/install) - Homebrew, Nix, Scoop, Cargo
- [How pond compares](/docs/compare) - memory layers, session search tools and proxy recorders, side by side
- [Claude Code deletes sessions after 30 days](/docs/guides/claude-code-session-retention) - what happens to the transcripts memory does not keep
- [MCP tools](/docs/reference/mcp-tools) - what an agent can ask the archive
