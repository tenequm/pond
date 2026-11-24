---
title: Claude Code deletes sessions after 30 days
description: Claude Code deletes session transcripts after 30 days by default (cleanupPeriodDays). What gets deleted, how to change it, and what can be recovered.
---

# Claude Code deletes your sessions after 30 days [What cleanupPeriodDays removes, how to change it, and what can and cannot be recovered afterwards]

Yes, Claude Code deletes old sessions. A background sweep removes local session transcripts once they are older than `cleanupPeriodDays`, and the default is 30 days. It runs after a session starts, shows no message, and a session you have not used for longer than the period simply stops appearing in the `/resume` picker. To keep sessions longer, set `cleanupPeriodDays` to a larger number in `~/.claude/settings.json`. A transcript that was already swept is gone as far as Claude Code is concerned: its docs describe the sweep as deletion and name no way to undo it.

Every detail in the next two sections is from Anthropic's docs - the [`cleanupPeriodDays` reference](https://code.claude.com/docs/en/settings-reference#cleanupperioddays) and [Application data](https://code.claude.com/docs/en/claude-directory#application-data) - checked 2026-11-24.

## What cleanupPeriodDays is

| Property | Value |
|---|---|
| What it sets | how many days Claude Code keeps session transcripts and other application data before deleting them |
| Default | `30` |
| Allowed values | a whole number of days, minimum `1`; `0` fails validation |
| Where to set it | any settings file - `~/.claude/settings.json` for every project |
| When deletion happens | a background sweep after a session starts, with no message |

To keep a year of history, or effectively everything:

```json
{
  "cleanupPeriodDays": 3650
}
```

There is no "never" value. Anthropic's own advice for long retention is a large number such as `3650`.

Four things the setting does not govern:

- **Sessions from Claude Desktop or Cowork.** Since Claude Code v2.1.248, transcripts of sessions started or most recently continued there are kept at any age, unless you set `desktopSessionCleanupPeriodDays`. The 30-day rule still applies to them when managed settings set `cleanupPeriodDays` or the HIPAA configuration applies.
- **Auto memory.** The notes under `projects/<project>/memory/` are excluded from the sweep.
- **Prompt history.** `~/.claude/history.jsonl`, the prompts you typed, stays until you delete it. It holds your side of the conversation only.
- **Runs with `--bare`.** A `claude -p --bare` run does not sweep.

An organization can also set the value for you through managed settings.

## What is deleted

The sweep works on files under `~/.claude/` older than the period:

- the session transcript, `projects/<project>/<session-id>.jsonl`
- its subagent transcripts and spilled tool results, in the session's own directory
- file checkpoints in `file-history/`, which is what makes rewind work for that session
- plan files, debug logs and pasted-text caches

The transcript is the part you cannot regenerate. It is the only record of what the agent was asked, what it ran, what came back, and why the change in your git history looks the way it does.

## Local retention is not Anthropic's retention

`cleanupPeriodDays` is about files on your machine. What Anthropic keeps on its servers is a separate policy that depends on your account: for consumer plans 30 days, or 5 years if you allow your data to be used for model improvement; 30 days for Team, Enterprise and API by default, with zero data retention available to some Enterprise organizations. See [Data usage](https://code.claude.com/docs/en/data-usage#data-retention) for the current terms. That is a retention policy, not a backup: the docs describe no way to download a local session's transcript from it once the file on your disk is gone.

## Deleting on purpose

To remove a project's sessions before they age out, `claude purge <path>` deletes its transcripts, its auto memory, task lists, debug logs, file-edit history and prompt-history lines after showing the plan and asking for confirmation (`--dry-run` to preview). To stop transcripts from being written at all, set `CLAUDE_CODE_SKIP_PROMPT_HISTORY`, or pass `--no-session-persistence` with `claude -p`.

## Can a deleted Claude Code session be recovered?

It depends on what else had a copy before the sweep ran.

- **From Claude Code:** no. The docs describe deletion, with no trash folder for transcripts and no undo command.
- **From a filesystem backup:** yes, if your backup covers `~/.claude` and still holds a snapshot from before the deletion. Put the `.jsonl` back in its `projects/<project>/` directory.
- **From a session archive:** yes, if the archive ingested the session while the file still existed.

That last condition is the whole point, so here it is without softening: **a session that was deleted before pond ever ingested it is gone.** pond reads files on disk. It cannot read a file that is no longer there, and installing it today recovers nothing that was swept last month.

## Keep sessions past the window

Two fixes, and they are not exclusive.

**Raise the number.** One line of JSON, nothing to install. It is the right fix if you work on one machine and only want Claude Code's own `/resume` to reach further back. The transcripts stay on that one disk, in a format that changes between Claude Code versions, and Codex or any other client you use keeps its own rules.

**Keep a second copy.** pond ingests each session into storage you own - a local directory or your own S3 bucket - and its contract is that a stored session survives the loss of its source. Once a session is in the store, Claude Code can delete the file and nothing changes on pond's side.

```sh
pond init              # choose storage, enable the Claude Code adapter
pond sync              # ingest everything on disk now
pond schedule start    # then sync every 5 minutes, so nothing ages out unseen
```

After that, a swept session is still searchable, and can be written back as the transcript Claude Code wrote:

```sh
pond search "the migration that broke staging"     # each hit shows its session id
pond get-session <session-id>                      # read it
pond resume <session-id> --to claude-code --out-dir ~/.claude/projects
```

`pond resume` into the client a session came from replays the stored records value for value, and it refuses to overwrite a file that already exists. `--out-dir ~/.claude/projects` puts the file back at `<project>/<session-id>.jsonl`, where Claude Code wrote it.

What pond does not do here: it does not stop or delay Claude Code's sweep, it does not redact secrets a tool printed into a transcript, and it covers only sessions that existed on a disk where `pond sync` ran.

## Next

- [Install](/docs/get-started/install) - Homebrew, Nix, Scoop, Cargo
- [Where Claude Code stores sessions](/docs/guides/where-claude-code-stores-sessions) - the paths and the transcript format
- [Claude Code memory vs a session archive](/docs/guides/claude-code-memory-vs-archive) - why auto memory surviving the sweep is not the same thing
- [Several machines, one bucket](/docs/guides/several-machines) - one archive for every machine you work on
