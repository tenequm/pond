export const site = {
  title: 'pond',
  description: 'Lossless storage and search for AI agent sessions, across every agentic client.',
  url: 'https://pond.locker',
  repo: 'tenequm/pond',
  ogImage: 'https://pond.locker/og-image.png',
  editBase: 'https://github.com/tenequm/pond/edit/main/',
  editText: 'Suggest changes to this page',
  socials: [
    { icon: 'github', link: 'https://github.com/tenequm/pond' },
    { icon: 'telegram', link: 'https://t.me/tenequm' },
    { icon: 'x', link: 'https://x.com/opwizardx' },
  ],
  topNav: [
    { text: 'Quickstart', link: '/get-started/quickstart' },
    { text: 'Reference', link: '/reference/cli' },
    { text: 'Spec', link: '/spec' },
    { text: 'Changelog', link: '/changelog' },
  ],
  sidebar: [
    { text: 'Why pond?', link: '/why' },
    { text: 'How pond compares', link: '/compare' },
    {
      text: 'Get started',
      items: [
        { text: 'Install', link: '/get-started/install' },
        { text: 'Quickstart', link: '/get-started/quickstart' },
        { text: 'Connect your agents', link: '/get-started/connect-your-agents' },
      ],
    },
    {
      text: 'Guides',
      items: [
        { text: 'Remote storage', link: '/guides/remote-storage' },
        { text: 'Several machines, one bucket', link: '/guides/several-machines' },
        { text: 'Run a remote serve', link: '/guides/remote-serve' },
        { text: 'Backup & restore', link: '/guides/backup-and-restore' },
        { text: 'Import a Claude.ai export', link: '/guides/import-claude-ai' },
        { text: 'Troubleshooting', link: '/guides/troubleshooting' },
      ],
    },
    {
      text: 'Reference',
      items: [
        { text: 'CLI commands', link: '/reference/cli' },
        { text: 'MCP tools', link: '/reference/mcp-tools' },
        { text: 'Configuration & environment', link: '/reference/configuration' },
        { text: 'Exit codes', link: '/reference/exit-codes' },
      ],
    },
    { text: 'Specification', link: '/spec' },
    { text: 'Changelog', link: '/changelog' },
  ],
} as const
