import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { relative, resolve } from 'node:path'
import remarkDirective from 'remark-directive'
import remarkParse from 'remark-parse'
import remarkStringify from 'remark-stringify'
import { unified } from 'unified'
import type { Plugin } from 'vite'
import { compilePage, fetchReleases, pageSources, repoRoot } from './markdown/plugin.ts'
import { home } from './src/seo.ts'
import { pageOrder, site } from './src/site.ts'

// Last content change per page from one `git log`. A pure rename keeps the older date, so moving a
// file does not mark it updated; a file git has never seen gets the build time.
function pageDates(paths: Record<string, string>): Record<string, string> {
  const pages = new Map(Object.entries(paths).map(([path, file]) => [relative(repoRoot, file), path]))
  let log: string
  try {
    log = execFileSync('git', ['log', '--format=@%ct', '--name-status', '-M'], {
      cwd: repoRoot,
      encoding: 'utf8',
      maxBuffer: 256 * 1024 * 1024,
    })
  } catch (error) {
    throw new Error('page dates come from git history: build inside a git checkout with git on PATH', { cause: error })
  }
  const iso = (seconds: number) => new Date(seconds * 1000).toISOString().replace('.000Z', 'Z')
  const dates: Record<string, string> = {}
  let stamp = ''
  for (const line of log.split('\n')) {
    if (line.startsWith('@')) {
      stamp = iso(Number(line.slice(1)))
      continue
    }
    const [status, file, renamedTo] = line.split('\t')
    const page = pages.get(renamedTo ?? file)
    if (!page) continue
    if (renamedTo) pages.set(file, page)
    if (status !== 'R100') dates[page] ??= stamp
  }
  for (const path of Object.keys(paths)) dates[path] ??= iso(Math.floor(Date.now() / 1000))
  return dates
}

export function staticFiles(): Plugin {
  const paths: Record<string, string> = { '/': resolve(import.meta.dirname, 'src/routes/index.tsx'), ...pageSources() }
  const dates = pageDates(paths)
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
      const order: string[] = pageOrder.map(page => page.link)
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
        const body = source.replace(/^---\n[\s\S]*?\n---\n\s*/, '')
        const meta = path === '/' ? home : (await compilePage(paths[path], source)).meta
        const slug = path === '/' ? '/index' : path
        links.push(`- [${meta.title}](${slug})` + (meta.description ? ': ' + meta.description : ''))
        let expanded = body
        if (path === '/changelog') {
          const releases = await fetchReleases()
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
