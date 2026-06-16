import { defineConfig } from 'vocs/config'

export default defineConfig({
  title: 'pond',
  description:
    'Lossless storage and hybrid search for AI agent sessions, across every agentic client.',
  rootDir: '.',
  // 'dynamic' (default) emits zero HTML; full-static prerenders one index.html
  // per route into dist/public/ for plain Cloudflare static-asset hosting.
  renderStrategy: 'full-static',
  editLink: {
    pattern: 'https://github.com/tenequm/pond/edit/main/docs/site/src/pages/:path',
    text: 'Suggest changes to this page',
  },
  socials: [{ icon: 'github', link: 'https://github.com/tenequm/pond' }],
  topNav: [
    { text: 'Quickstart', link: '/get-started/quickstart' },
    { text: 'Reference', link: '/reference/cli' },
    { text: 'Spec', link: '/specification' },
  ],
  sidebar: [
    { text: 'Why pond?', link: '/' },
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
        { text: 'Backup & restore', link: '/guides/backup-and-restore' },
        { text: 'Import a Claude.ai export', link: '/guides/import-claude-ai' },
      ],
    },
    {
      text: 'Reference',
      items: [
        { text: 'CLI commands', link: '/reference/cli' },
        { text: 'Configuration & environment', link: '/reference/configuration' },
        { text: 'Exit codes', link: '/reference/exit-codes' },
      ],
    },
    { text: 'Specification', link: '/specification' },
  ],
})
