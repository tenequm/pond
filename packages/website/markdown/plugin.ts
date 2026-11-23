import { execFileSync } from 'node:child_process'
import { globSync, readFileSync, realpathSync } from 'node:fs'
import { relative, resolve } from 'node:path'
import { compile } from '@mdx-js/mdx'
import type { Element, Nodes as HastNodes, Root as HastRoot } from 'hast'
import type { Heading, Paragraph, Root as MdastRoot, RootContent, Text } from 'mdast'
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

export type TocEntry = { depth: number; id: string; text: string }
export type SearchSection = { id: string; heading: string; text: string }
export type SearchDoc = { slug: string; title: string; sections: SearchSection[] }
export type PageMeta = {
  title: string
  subtitle?: string
  description?: string
  toc: TocEntry[]
  lastUpdated?: string
  source: string
}

export type Release = { tag_name: string; published_at: string; html_url: string; body: string | null; draft: boolean }
type PageData = { frontmatter?: Record<string, string>; h1?: string; subtitle?: string; toc?: TocEntry[]; sections?: SearchSection[] }

const CODE_THEME = 'github-dark-dimmed'
const CALLOUTS = new Set(['info', 'note', 'tip', 'warning', 'danger'])
const CONTAINERS = new Set(['steps', 'code-group'])
const SPEC_ANCHOR = /\s*\{#([\w-]+)\}/g
const SUBTITLE = /\s*\[([^\]]+)\]\s*$/

let releases: Promise<Release[]> | undefined
export function fetchReleases(repo: string): Promise<Release[]> {
  releases ??= (async () => {
    const headers: Record<string, string> = { accept: 'application/vnd.github+json' }
    if (process.env.GITHUB_TOKEN) headers.authorization = `Bearer ${process.env.GITHUB_TOKEN}`
    const all: Release[] = []
    for (let page = 1; ; page++) {
      const response = await fetch(`https://api.github.com/repos/${repo}/releases?per_page=100&page=${page}`, { headers })
      if (!response.ok) {
        throw new Error(
          `changelog: GitHub releases fetch failed (${response.status}). Unauthenticated calls rate-limit; set GITHUB_TOKEN and rebuild.`,
        )
      }
      const batch = (await response.json()) as Release[]
      all.push(...batch.filter((release) => !release.draft))
      if (batch.length < 100) return all
    }
  })()
  return releases
}

const parseBody = (markdown: string) => unified().use(remarkParse).use(remarkGfm).parse(markdown).children

function remarkPond(options: { repo: string }) {
  return async (tree: MdastRoot, file: VFile) => {
    const data = file.data as PageData
    const source = String(file.value)
    const pending: Array<Promise<void>> = []

    visit(tree, (node, index, parent) => {
      if (node.type === 'yaml') {
        data.frontmatter = parseYaml(node.value) ?? {}
        return
      }
      // remark-directive claims every `:word` in prose; hand back the ones that are not ours.
      if (node.type === 'textDirective' && parent && index !== undefined && node.position) {
        const text: Text = { type: 'text', value: source.slice(node.position.start.offset, node.position.end.offset) }
        parent.children[index] = text
        return SKIP
      }
      if (node.type === 'containerDirective') {
        const label = node.children[0]?.data && 'directiveLabel' in node.children[0].data ? node.children.shift() : undefined
        if (CALLOUTS.has(node.name)) {
          node.data = { hName: 'callout', hProperties: { type: node.name, title: label ? mdastToString(label) : undefined } }
        } else if (CONTAINERS.has(node.name)) {
          node.data = { hName: node.name }
        }
        return
      }
      if (node.type === 'leafDirective' && node.name === 'changelog' && parent && index !== undefined) {
        pending.push(
          fetchReleases(options.repo).then((list) => {
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
      if (node.type !== 'heading' && node.type !== 'paragraph') return
      anchorFromMarker(node)
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

// docs/spec.md marks its rules with `{#rule-id}` so code refs like `spec.md#lance-append-only`
// stay greppable in the raw file; each marker becomes that element's id.
function anchorFromMarker(node: Heading | Paragraph) {
  let id: string | undefined
  for (const child of node.children) {
    if (child.type !== 'text' || !child.value.includes('{#')) continue
    child.value = child.value.replace(SPEC_ANCHOR, (_, anchor: string) => {
      id ??= anchor
      return ''
    })
  }
  if (id) node.data = { ...node.data, hProperties: { ...node.data?.hProperties, id } }
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
      const lang = classes.find((name) => name.startsWith('language-'))?.slice('language-'.length) ?? 'text'
      const meta = String((code.data as { meta?: string } | undefined)?.meta ?? '')
      const text = code.children[0]?.type === 'text' ? code.children[0].value.replace(/\n$/, '') : ''
      jobs.push(
        highlight(text, lang).then((pre) => {
          pre.properties = { ...pre.properties, dataLang: lang, dataTitle: /\[(.+)\]/.exec(meta)?.[1] }
          parent.children[index] = pre
        }),
      )
      return SKIP
    })
    await Promise.all(jobs)
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
          if (Number(depth) > 1) toc.push({ depth: Number(depth), id, text: heading })
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

export function markdown(options: { repo: string }): Plugin {
  const SEARCH = 'virtual:pond-search'
  let root = process.cwd()
  let repoRoot = root
  const cache = new Map<string, { source: string; page: Promise<{ code: string; meta: PageMeta; sections: SearchSection[] }> }>()

  const lastUpdated = (path: string) => {
    try {
      return execFileSync('git', ['log', '-1', '--format=%cI', '--', path], { cwd: repoRoot, encoding: 'utf8' }).trim() || undefined
    } catch {
      return undefined
    }
  }

  function compilePage(source: string, path: string) {
    const cached = cache.get(path)
    if (cached?.source === source) return cached.page
    const page = (async () => {
      const file = await compile(
        { value: source, path },
        {
          format: 'md',
          remarkPlugins: [remarkFrontmatter, remarkGfm, remarkDirective, [remarkPond, options]],
          rehypePlugins: [rehypeSlug, rehypeHighlight, rehypeCollect],
        },
      )
      const data = file.data as PageData
      const real = realpathSync(path)
      const meta: PageMeta = {
        title: data.frontmatter?.title ?? data.h1 ?? '',
        subtitle: data.subtitle,
        description: data.frontmatter?.description,
        toc: data.toc ?? [],
        lastUpdated: lastUpdated(real),
        source: relative(repoRoot, real),
      }
      return { code: String(file), meta, sections: data.sections ?? [] }
    })()
    cache.set(path, { source, page })
    return page
  }

  return {
    name: 'pond-markdown',
    enforce: 'pre',
    configResolved(config) {
      root = config.root
      try {
        repoRoot = execFileSync('git', ['rev-parse', '--show-toplevel'], { cwd: root, encoding: 'utf8' }).trim()
      } catch {
        repoRoot = resolve(root, '../..')
      }
    },
    resolveId(id) {
      if (id === SEARCH) return `\0${SEARCH}`
    },
    async load(id) {
      if (id !== `\0${SEARCH}`) return
      const content = resolve(root, 'content')
      const files = [
        ...globSync('**/*.md', { cwd: content }).map((file) => ({ slug: file.replace(/\.md$/, ''), path: resolve(content, file) })),
        { slug: 'spec', path: resolve(root, '../../docs/spec.md') },
      ]
      const docs: SearchDoc[] = await Promise.all(
        files.map(async ({ slug, path }) => {
          this.addWatchFile(path)
          const { meta, sections } = await compilePage(readFileSync(path, 'utf8'), path)
          return { slug, title: meta.title, sections }
        }),
      )
      return `export default ${JSON.stringify(docs)}`
    },
    async transform(source, id) {
      const [path, query] = id.split('?')
      if (!path.endsWith('.md')) return
      const page = await compilePage(source, path)
      if (query === 'meta') return { code: `export const meta = ${JSON.stringify(page.meta)}`, map: null }
      return { code: page.code, map: null }
    },
  }
}
