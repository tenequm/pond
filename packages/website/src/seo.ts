import { site } from './site.ts'

export const home = {
  title: 'pond - lossless storage and search for AI agent sessions',
  description: 'Pond ingests every AI agent session - Claude Code, Codex, OpenCode, OpenClaw, oh-my-pi, Claude Desktop, Claude.ai, Pi, Letta Code, Grok, Antigravity, Devin, Hermes, nanoclaw - losslessly into storage you own, then makes the whole corpus searchable and SQL-queryable over MCP. One Rust binary, Lance-backed, local by default.',
}

declare const __PAGE_DATES__: Record<string, string>

export function siteHead() {
  return {
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      // The migration preserves the public generator tag alongside the other head metadata.
      { name: 'generator', content: 'Waku' },
      { name: 'color-scheme', content: 'light dark' },
      { name: 'robots', content: 'index, follow' },
      { title: site.title },
    ],
    links: [{ rel: 'icon', href: '/icon.png', type: 'image/png' }],
    scripts: import.meta.env.PROD ? [{ src: '/mesh/script.js', defer: true, 'data-site-id': '878f707ff553' }] : [],
  }
}

export function pageHead({ title, description = site.description, path }: { title: string; description?: string; path: string }) {
  const pageTitle = title.replace(/ (?:-|\u2013) pond$/, '')
  const standaloneTitle = ['/', '/why', '/compare'].includes(path)
  const url = site.url + path
  return {
    meta: [
      { title: standaloneTitle ? pageTitle : `${pageTitle} \u2013 pond` },
      { name: 'description', content: description },
      { property: 'og:type', content: 'website' },
      { property: 'og:title', content: pageTitle },
      { property: 'og:site_name', content: site.title },
      { property: 'og:url', content: url },
      { property: 'og:description', content: description },
      { property: 'og:image', content: site.ogImage },
      { property: 'article:modified_time', content: __PAGE_DATES__[path] },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: pageTitle },
      { name: 'twitter:description', content: description },
      { name: 'twitter:image', content: site.ogImage },
    ],
    links: [{ rel: 'canonical', href: url }],
  }
}
