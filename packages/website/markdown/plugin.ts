import { globSync, readFileSync, realpathSync } from 'node:fs'
import { resolve } from 'node:path'
import { compile } from '@mdx-js/mdx'
import type { Element, Nodes as HastNodes, Root as HastRoot } from 'hast'
import type { Root as MdastRoot, RootContent, Text } from 'mdast'
import { toString as mdastToString } from 'mdast-util-to-string'
import rehypeSlug from 'rehype-slug'
import remarkDirective from 'remark-directive'
import remarkFrontmatter from 'remark-frontmatter'
import remarkGfm from 'remark-gfm'
import remarkParse from 'remark-parse'
import { codeToHast } from 'shiki'
import { unified } from 'unified'
import { SKIP, visit } from 'unist-util-visit'
import type { VFile } from 'vfile'
import type { Plugin } from 'vite'
import { parse as parseYaml } from 'yaml'
import { site } from '../src/site.ts'

export type TocEntry = { depth: number; id: string; text: string }
export type SearchSection = { id: string; heading: string; text: string }
export type SearchDoc = { slug: string; title: string; sections: SearchSection[] }
export type PageMeta = {
  title: string
  subtitle?: string
  description?: string
  toc: TocEntry[]
}

export type Release = { tag_name: string; published_at: string; html_url: string; body: string | null; draft: boolean }
type PageData = { frontmatter?: Record<string, string>; h1?: string; subtitle?: string; toc?: TocEntry[]; sections?: SearchSection[] }

const CODE_THEME = 'github-dark-dimmed'
const CALLOUTS = new Set(['info', 'note', 'tip', 'warning', 'danger'])
const CONTAINERS = new Set(['steps', 'code-group'])
const SUBTITLE = /\s*\[([^\]]+)\]\s*$/

export const repoRoot = realpathSync(resolve(import.meta.dirname, '../../..'))

// URL path -> source file, for every Markdown page. Real paths, so they equal Vite's module ids.
export function pageSources(): Record<string, string> {
  const docs = resolve(repoRoot, 'docs/website')
  const pages = globSync('**/*.md', { cwd: docs }).map((file) => [`/docs/${file.slice(0, -'.md'.length)}`, resolve(docs, file)])
  return { '/docs/spec': resolve(repoRoot, 'docs/spec.md'), ...Object.fromEntries(pages) }
}

// vite dev serves an empty changelog when GitHub is unreachable; a build fails instead.
let lenient = false
let releases: Promise<Release[]> | undefined
export function fetchReleases(): Promise<Release[]> {
  const failed = (reason: string) =>
    new Error(`changelog: GitHub releases fetch failed (${reason}). Unauthenticated calls are rate-limited; set GITHUB_TOKEN and retry.`)
  releases ??= (async () => {
    const token = process.env.GITHUB_TOKEN ?? process.env.GH_TOKEN
    const headers: Record<string, string> = { accept: 'application/vnd.github+json' }
    if (token) headers.authorization = `Bearer ${token}`
    const all: Release[] = []
    for (let page = 1; ; page++) {
      const response = await fetch(`https://api.github.com/repos/${site.repo}/releases?per_page=100&page=${page}`, {
        headers,
        signal: AbortSignal.timeout(15_000),
      }).catch((error: Error) => {
        throw failed(error.message)
      })
      if (!response.ok) throw failed(`${response.status} ${response.statusText}`)
      const batch = (await response.json()) as Release[]
      all.push(...batch.filter((release) => !release.draft))
      if (batch.length < 100) return all
    }
  })().catch((error: Error) => {
    if (!lenient) throw error
    console.warn(`${error.message} The changelog page stays empty until the dev server restarts.`)
    return []
  })
  return releases
}

const parseBody = (markdown: string) => unified().use(remarkParse).use(remarkGfm).parse(markdown).children

function remarkPond() {
  return async (tree: MdastRoot, file: VFile) => {
    const data = file.data as PageData
    const source = String(file.value)
    const pending: Array<Promise<void>> = []

    // remark-directive claims every `:word` in prose; hand them back as text before anything reads a heading.
    visit(tree, 'textDirective', (node, index, parent) => {
      if (!parent || index === undefined || !node.position) return
      const textOf = (sibling?: { type: string }) => (sibling?.type === 'text' ? (sibling as Text).value : undefined)
      const before = textOf(parent.children[index - 1])
      const after = textOf(parent.children[index + 1])
      const own = source.slice(node.position.start.offset, node.position.end.offset)
      const start = before === undefined ? index : index - 1
      const count = 1 + Number(before !== undefined) + Number(after !== undefined)
      parent.children.splice(start, count, { type: 'text', value: (before ?? '') + own + (after ?? '') })
      return [SKIP, start + 1]
    })

    visit(tree, (node, index, parent) => {
      if (node.type === 'yaml') {
        data.frontmatter = parseYaml(node.value) ?? {}
        return
      }
      if (node.type === 'containerDirective') {
        if (CALLOUTS.has(node.name)) {
          const label = node.children[0]?.data && 'directiveLabel' in node.children[0].data ? node.children.shift() : undefined
          node.data = { hName: 'callout', hProperties: { type: node.name, title: label ? mdastToString(label) : undefined } }
        } else if (CONTAINERS.has(node.name)) {
          node.data = { hName: node.name }
        } else {
          file.fail(`unknown directive ":::${node.name}"; use one of ${[...CALLOUTS, ...CONTAINERS].join(', ')}`, node)
        }
        return
      }
      if (node.type === 'leafDirective' && parent && index !== undefined) {
        if (node.name !== 'changelog') file.fail(`unknown directive "::${node.name}"; the only one is ::changelog`, node)
        pending.push(
          fetchReleases().then((list) => {
            const nodes = list.map((release) => ({
              type: 'release',
              data: {
                hName: 'release',
                hProperties: { version: release.tag_name, date: release.published_at, url: release.html_url },
              },
              children: parseBody(release.body ?? ''),
            }))
            const siblings = parent.children as RootContent[]
            siblings.splice(siblings.indexOf(node as RootContent), 1, ...(nodes as unknown as RootContent[]))
          }),
        )
        return SKIP
      }
      if (node.type === 'heading' && node.depth === 1 && parent && index !== undefined && !data.h1) {
        const last = node.children.at(-1)
        const subtitle = last?.type === 'text' ? SUBTITLE.exec(last.value) : null
        if (last?.type === 'text' && subtitle) {
          last.value = last.value.slice(0, subtitle.index)
          data.subtitle = subtitle[1]
        }
        data.h1 = mdastToString(node)
        const wrapper = { type: 'pageTitle', data: { hName: 'page-title', hProperties: { subtitle: data.subtitle } }, children: [node] }
        parent.children[index] = wrapper as unknown as RootContent
        return SKIP
      }
    })
    await Promise.all(pending)
  }
}

async function highlight(code: string, lang: string): Promise<Element> {
  const render = (language: string) => codeToHast(code, { lang: language, theme: CODE_THEME })
  const root = await render(lang).catch(() => render('text'))
  return root.children[0] as Element
}

function rehypeHighlight() {
  return async (tree: HastRoot) => {
    const jobs: Array<Promise<void>> = []
    visit(tree, 'element', (node, index, parent) => {
      const code = node.children[0]
      if (node.tagName !== 'pre' || code?.type !== 'element' || code.tagName !== 'code' || !parent || index === undefined) return
      const classes = (code.properties.className as string[] | undefined) ?? []
      const lang = classes.find((name) => name.startsWith('language-'))?.slice('language-'.length)
      const meta = String((code.data as { meta?: string } | undefined)?.meta ?? '')
      const text = code.children[0]?.type === 'text' ? code.children[0].value.replace(/\n$/, '') : ''
      jobs.push(
        highlight(text, lang ?? 'text').then((pre) => {
          pre.properties = { ...pre.properties, dataLang: lang, dataTitle: /\[(.+)\]/.exec(meta)?.[1] }
          parent.children[index] = pre
        }),
      )
      return SKIP
    })
    await Promise.all(jobs)
  }
}

// The file's own size reserves the image's box before it loads. GIF is the only format pages use.
function rehypeImageSize() {
  return (tree: HastRoot) => {
    visit(tree, 'element', (node) => {
      const src = node.tagName === 'img' ? String(node.properties.src ?? '') : ''
      if (!src.startsWith('/') || !src.endsWith('.gif')) return
      const header = readFileSync(resolve(import.meta.dirname, '../public', src.slice(1)))
      node.properties.width = header.readUInt16LE(6)
      node.properties.height = header.readUInt16LE(8)
    })
  }
}

const textOf = (node: HastNodes): string =>
  node.type === 'text' ? node.value : 'children' in node ? node.children.map(textOf).join('') : ''

// One pass for both the outline and the search sections, so their ids always agree with the DOM.
function rehypeCollect() {
  return (tree: HastRoot, file: VFile) => {
    const toc: TocEntry[] = []
    const sections: SearchSection[] = [{ id: '', heading: '', text: '' }]
    const walk = (node: HastNodes, inRelease: boolean) => {
      if (node.type === 'text') {
        sections[sections.length - 1].text += node.value
        return
      }
      if (node.type !== 'element' && node.type !== 'root') return
      if (node.type === 'element') {
        if (node.tagName === 'release') {
          const version = String(node.properties.version)
          toc.push({ depth: 2, id: version, text: version })
          sections.push({ id: version, heading: version, text: '' })
          for (const child of node.children) walk(child, true)
          return
        }
        const depth = /^h([1-4])$/.exec(node.tagName)?.[1]
        if (depth && !inRelease) {
          const id = String(node.properties.id ?? '')
          const heading = textOf(node)
          if (depth === '2' || depth === '3') toc.push({ depth: Number(depth), id, text: heading })
          sections.push({ id: Number(depth) > 1 ? id : '', heading, text: '' })
          return
        }
      }
      for (const child of node.children) walk(child, inRelease)
      if (node.type === 'element' && /^(p|li|pre|tr|h[1-6]|blockquote)$/.test(node.tagName)) sections[sections.length - 1].text += '\n'
    }
    walk(tree, false)
    const data = file.data as PageData
    data.toc = toc
    data.sections = sections
      .map((section) => ({ ...section, text: section.text.replace(/\s+/g, ' ').trim() }))
      .filter((section) => section.heading || section.text)
  }
}

type CompiledPage = { code: string; meta: PageMeta; sections: SearchSection[] }
const cache = new Map<string, { source: string; page: Promise<CompiledPage> }>()

export function compilePage(path: string, source = readFileSync(path, 'utf8')): Promise<CompiledPage> {
  const cached = cache.get(path)
  if (cached?.source === source) return cached.page
  const page = (async () => {
    const file = await compile(
      { value: source, path },
      {
        format: 'md',
        remarkPlugins: [remarkFrontmatter, remarkGfm, remarkDirective, remarkPond],
        rehypePlugins: [rehypeSlug, rehypeHighlight, rehypeImageSize, rehypeCollect],
      },
    )
    const data = file.data as PageData
    const meta: PageMeta = {
      title: data.frontmatter?.title ?? data.h1 ?? '',
      subtitle: data.subtitle,
      description: data.frontmatter?.description,
      toc: data.toc ?? [],
    }
    return { code: String(file), meta, sections: data.sections ?? [] }
  })()
  cache.set(path, { source, page })
  return page
}

export function markdown(): Plugin {
  const SEARCH = 'virtual:pond-search'
  return {
    name: 'pond-markdown',
    enforce: 'pre',
    configResolved(config) {
      lenient = config.command === 'serve'
    },
    resolveId(id) {
      if (id === SEARCH) return `\0${SEARCH}`
    },
    async load(id) {
      if (id !== `\0${SEARCH}`) return
      const docs: SearchDoc[] = await Promise.all(
        Object.entries(pageSources()).map(async ([path, file]) => {
          this.addWatchFile(file)
          const { meta, sections } = await compilePage(file)
          return { slug: path.slice(1), title: meta.title, sections }
        }),
      )
      return `export default ${JSON.stringify(docs)}`
    },
    async transform(source, id) {
      const [path, query] = id.split('?')
      if (!path.endsWith('.md')) return
      const page = await compilePage(path, source)
      if (query === 'meta') return { code: `export const meta = ${JSON.stringify(page.meta)}`, map: null }
      return { code: page.code, map: null }
    },
  }
}
