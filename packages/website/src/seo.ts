import { site } from './site.ts'

export const home = {
  title: 'pond - lossless storage and search for AI agent sessions',
  description: 'pond stores every AI agent session - Claude Code, Codex, OpenCode and more - losslessly in storage you own, searchable and SQL-queryable over MCP.',
}

export function siteHead() {
  return {
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { name: 'color-scheme', content: 'dark' },
      { name: 'robots', content: 'index, follow' },
      { title: site.title },
    ],
    links: [{ rel: 'icon', href: '/icon.png', type: 'image/png' }],
    scripts: import.meta.env.PROD ? [{ src: '/mesh/script.js', defer: true, 'data-site-id': '878f707ff553' }] : [],
  }
}

export function pageHead({ title, description = site.description, path }: { title: string; description?: string; path: string }) {
  // A title that already names the site stands alone, as on the old site.
  const standaloneTitle = title.toLowerCase().includes(site.title)
  const url = site.url + path
  return {
    meta: [
      { title: standaloneTitle ? title : `${title} \u2013 pond` },
      { name: 'description', content: description },
      { property: 'og:type', content: 'website' },
      { property: 'og:title', content: title },
      { property: 'og:site_name', content: site.title },
      { property: 'og:url', content: url },
      { property: 'og:description', content: description },
      { property: 'og:image', content: site.ogImage },
      { property: 'article:modified_time', content: __PAGE_DATES__[path] },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: title },
      { name: 'twitter:description', content: description },
      { name: 'twitter:image', content: site.ogImage },
      // The router renders these as JSON-LD scripts; its React head types do not list the key yet.
      ...(structuredData(path, title, description).map((data) => ({ 'script:ld+json': data })) as object[]),
    ],
    links: [{ rel: 'canonical', href: url }],
  }
}

// The home page tells search engines the site's name and what the software is; a docs page gives its place in the sidebar.
function structuredData(path: string, title: string, description: string) {
  const context = { '@context': 'https://schema.org' }
  if (path === '/') {
    return [
      { ...context, '@type': 'WebSite', name: site.title, url: site.url + '/' },
      {
        ...context,
        '@type': 'SoftwareApplication',
        name: site.title,
        description,
        url: site.url + '/',
        applicationCategory: 'DeveloperApplication',
        operatingSystem: 'macOS, Linux, Windows',
        license: 'https://www.apache.org/licenses/LICENSE-2.0',
        sameAs: site.socials.github,
        offers: { '@type': 'Offer', price: '0', priceCurrency: 'USD' },
      },
    ]
  }
  const group = site.sidebar.find((entry) => 'items' in entry && entry.items.some((item) => item.link === path))
  const trail = [
    { name: site.title, item: site.url + '/' },
    ...(group && 'items' in group ? [{ name: group.text, item: site.url + group.items[0].link }] : []),
    { name: title, item: site.url + path },
  ]
  return [
    {
      ...context,
      '@type': 'BreadcrumbList',
      itemListElement: trail.map((crumb, i) => ({ '@type': 'ListItem', position: i + 1, ...crumb })),
    },
  ]
}
