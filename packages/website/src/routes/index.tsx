import { createFileRoute } from '@tanstack/react-router'
import { pageHead } from '../seo.ts'

export const Route = createFileRoute('/')({
  head: () =>
    pageHead({
      title: 'pond - lossless storage and search for AI agent sessions',
      description:
        'Pond ingests every AI agent session - Claude Code, Codex, OpenCode, OpenClaw, oh-my-pi, Claude Desktop, Claude.ai, Pi, Letta Code, Grok, Antigravity, Devin, Hermes, nanoclaw - losslessly into storage you own, then makes the whole corpus searchable and SQL-queryable over MCP. One Rust binary, Lance-backed, local by default.',
      path: '/',
    }),
  component: () => <h1 className="text-3xl font-bold">pond home</h1>,
})
