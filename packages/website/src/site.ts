export const site = {
  title: 'pond',
  description: 'Lossless storage and search for AI agent sessions, across every agentic client.',
  url: 'https://pond.locker',
  repo: 'tenequm/pond',
  ogImage: 'https://pond.locker/og-image.png',
  socials: {
    github: 'https://github.com/tenequm/pond',
    telegram: 'https://t.me/tenequm',
    x: 'https://x.com/opwizardx',
  },
  topNav: [
    { text: 'Quickstart', link: '/docs/get-started/quickstart' },
    { text: 'Reference', link: '/docs/reference/cli' },
    { text: 'Spec', link: '/docs/spec' },
    { text: 'Changelog', link: '/docs/changelog' },
  ],
  sidebar: [
    { text: 'Why pond?', link: '/docs/why' },
    { text: 'How pond compares', link: '/docs/compare' },
    {
      text: 'Get started',
      items: [
        { text: 'Install', link: '/docs/get-started/install' },
        { text: 'Quickstart', link: '/docs/get-started/quickstart' },
        { text: 'Connect your agents', link: '/docs/get-started/connect-your-agents' },
      ],
    },
    {
      text: 'Session history',
      items: [
        { text: 'Where sessions are stored', link: '/docs/guides/where-claude-code-stores-sessions' },
        { text: 'Claude Code session retention', link: '/docs/guides/claude-code-session-retention' },
        { text: 'Codex session history', link: '/docs/guides/codex-session-history' },
        { text: 'Memory vs a session archive', link: '/docs/guides/claude-code-memory-vs-archive' },
      ],
    },
    {
      text: 'Guides',
      items: [
        { text: 'Remote storage', link: '/docs/guides/remote-storage' },
        { text: 'Several machines, one bucket', link: '/docs/guides/several-machines' },
        { text: 'Run a remote serve', link: '/docs/guides/remote-serve' },
        { text: 'Backup & restore', link: '/docs/guides/backup-and-restore' },
        { text: 'Import a Claude.ai export', link: '/docs/guides/import-claude-ai' },
        { text: 'SQL over your sessions', link: '/docs/guides/sql-analytics' },
        { text: 'Troubleshooting', link: '/docs/guides/troubleshooting' },
      ],
    },
    {
      text: 'Reference',
      items: [
        { text: 'CLI commands', link: '/docs/reference/cli' },
        { text: 'MCP tools', link: '/docs/reference/mcp-tools' },
        { text: 'Configuration & environment', link: '/docs/reference/configuration' },
        { text: 'Exit codes', link: '/docs/reference/exit-codes' },
      ],
    },
    { text: 'Specification', link: '/docs/spec' },
    { text: 'Changelog', link: '/docs/changelog' },
  ],
} as const

export function sidebarGroup(path: string) {
  for (const entry of site.sidebar) if ('items' in entry && entry.items.some((item) => item.link === path)) return entry
}

export const pageOrder = site.sidebar.flatMap<{ text: string; link: string }>((entry) =>
  'items' in entry ? entry.items : [entry],
)
