import { execFileSync } from 'node:child_process'
import { globSync, readFileSync } from 'node:fs'
import remarkDirective from 'remark-directive'
import remarkParse from 'remark-parse'
import remarkStringify from 'remark-stringify'
import { unified } from 'unified'
import type { Plugin } from 'vite'
import { parse } from 'yaml'
import { fetchReleases } from './markdown/plugin.ts'
import { home } from './src/seo.ts'
import { site } from './src/site.ts'

export function staticFiles(): Plugin {
  const paths: Record<string, string> = { '/': 'src/routes/index.tsx', '/spec': '../../docs/spec.md' }
  for (const file of globSync('**/*.md', { cwd: '../../docs/website' })) {
    paths['/' + file.slice(0, -3)] = '../../docs/website/' + file
  }
  const dates = Object.fromEntries(Object.entries(paths).map(([slug, file]) => {
    const stamp = execFileSync('git', ['log', '-1', '--format=%ct', '--', file], { encoding: 'utf8' }).trim()
    const date = stamp ? new Date(Number(stamp) * 1000) : new Date()
    return [slug, date.toISOString().replace('.000Z', 'Z')]
  }))
  return {
    name: 'pond-static-files',
    config: () => ({ define: { __PAGE_DATES__: JSON.stringify(dates) } }),
    async generateBundle() {
      if (this.environment.name !== 'client') return
      const emit = (fileName: string, source: string) => this.emitFile({ type: 'asset', fileName, source })
      const urls = Object.keys(paths).sort().map(path =>
        `  <url>\n    <loc>${site.url}${path}</loc>\n    <lastmod>${dates[path].slice(0, 10)}</lastmod>\n  </url>`)
      emit('sitemap.xml', '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + urls.join('\n') + '\n</urlset>\n')
      emit('robots.txt', `User-agent: *\nAllow: /\n\nSitemap: ${site.url}/sitemap.xml\n`)
      const order: string[] = site.sidebar.flatMap(item => 'items' in item ? item.items.map(i => i.link) : [item.link])
      const markdown = unified().use(remarkParse).use(remarkDirective).use(remarkStringify, {
        // Preserve the existing twins' escaping inside prose while leaving code and destinations intact.
        handlers: {
          text(node, _parent, state, info) {
            return state.safe(node.value, info).replace(/(?<!\\)([_<])/g, '\\$1')
          },
        },
      })
      const links: string[] = []
      const bodies: string[] = []
      for (const path of [...new Set([...order, ...Object.keys(paths).filter(p => p !== '/'), '/'])]) {
        const source = path === '/' ? '' : readFileSync(paths[path], 'utf8')
        const header = source.match(/^---\n([\s\S]*?)\n---\n\s*/)
        const body = source.slice(header?.[0].length ?? 0)
        const meta = path === '/' ? home : header ? parse(header[1]) : {}
        const title = meta.title ?? body.match(/^# (.+)$/m)?.[1]
        const slug = path === '/' ? '/index' : path
        links.push(`- [${title}](${slug})` + (meta.description ? ': ' + meta.description : ''))
        let expanded = body
        if (path === '/changelog') {
          const releases = await fetchReleases(site.repo)
          const notes = releases.map(r =>
            `## ${r.tag_name} (${r.published_at.slice(0, 10)})\n\n${r.body?.trim() ?? ''}`)
          expanded = body.replace('::changelog', () => notes.join('\n\n'))
        }
        const rendered = expanded ? String(markdown.processSync(expanded)) : ''
        emit(slug.slice(1) + '.md', path === '/spec' ? source : rendered)
        bodies.push(rendered.trimEnd())
      }
      const intro = `# ${site.title}\n\n${site.description}\n\n`
      emit('llms.txt', intro + links.join('\n'))
      emit('llms-full.txt', intro + '<!--\nSitemap:\n' + links.join('\n') + '\n-->\n\n' + bodies.join('\n\n'))
    },
  }
}
