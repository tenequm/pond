import { Changelog, defineConfig } from 'vocs/config'

const SPEC_ANCHOR = /\s*\{#([\w-]+)\}/g

// docs/spec.md marks its rules with `{#rule-id}` so code refs like
// `spec.md#lance-append-only` stay greppable in the raw file. Rewrite the
// literal markers into element ids at build time: clean rendering, and every
// rule becomes a deep-linkable anchor.
function remarkSpecAnchors() {
  const walk = (node: any, visit: (n: any) => void): void => {
    visit(node)
    for (const child of node.children ?? []) walk(child, visit)
  }
  return (tree: any) => {
    walk(tree, (node) => {
      if (node.type !== 'heading' && node.type !== 'paragraph') return
      let id: string | undefined
      for (const child of node.children ?? []) {
        if (child.type !== 'text' || !child.value.includes('{#')) continue
        child.value = child.value.replace(SPEC_ANCHOR, (_: string, anchor: string) => {
          id ??= anchor
          return ''
        })
      }
      if (id) node.data = { ...node.data, hProperties: { ...node.data?.hProperties, id } }
    })
  }
}

export default defineConfig({
  title: 'pond',
  description:
    'Lossless storage and search for AI agent sessions, across every agentic client.',
  baseUrl: 'https://pond.locker',
  iconUrl: '/icon.png',
  logoUrl: '/logo.png',
  ogImageUrl: 'https://pond.locker/og-image.png',
  rootDir: '.',
  // 'dynamic' (default) emits zero HTML; full-static prerenders one index.html
  // per route into dist/public/ for plain Cloudflare static-asset hosting.
  renderStrategy: 'full-static',
  head: {
    // Without this, vocs emits <base href={baseUrl}> and every relative link
    // (including in-page #anchors) resolves against production instead of the
    // host actually serving the page.
    base: false,
    script: [
      { src: '/mesh/script.js', defer: true, 'data-site-id': '878f707ff553' },
      // Ask AI is hidden via _root.css (vocs has no global off switch), but the
      // component stays mounted and its Cmd+I listener would still open the
      // dialog - swallow the shortcut in capture phase before it gets there.
      {
        innerHTML:
          "window.addEventListener('keydown',function(e){if((e.metaKey||e.ctrlKey)&&e.key.toLowerCase()==='i')e.stopImmediatePropagation()},true)",
      },
    ],
  },
  changelog: Changelog.github({ repo: 'tenequm/pond' }),
  // 'detect' compiles .md as plain CommonMark (no JSX/`{expr}` parsing), so the
  // symlinked spec.md renders without MDX-escaping its content.
  markdown: { format: 'detect', remarkPlugins: [remarkSpecAnchors] },
  editLink: {
    pattern: 'https://github.com/tenequm/pond/edit/main/docs/site/src/pages/:path',
    text: 'Suggest changes to this page',
  },
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
        { text: 'MCP tools', link: '/reference/mcp-tools' },
        { text: 'Configuration & environment', link: '/reference/configuration' },
        { text: 'Exit codes', link: '/reference/exit-codes' },
      ],
    },
    { text: 'Specification', link: '/spec' },
    { text: 'Changelog', link: '/changelog' },
  ],
})
