import { compile } from '@mdx-js/mdx'
import GithubSlugger from 'github-slugger'
import { toString as mdastToString } from 'mdast-util-to-string'
import rehypeSlug from 'rehype-slug'
import remarkDirective from 'remark-directive'
import remarkFrontmatter from 'remark-frontmatter'
import remarkGfm from 'remark-gfm'
import remarkParse from 'remark-parse'
import { unified } from 'unified'
import { visit } from 'unist-util-visit'
import type { Plugin } from 'vite'
import { parse as parseYaml } from 'yaml'

export type TocEntry = { depth: number; id: string; text: string }
export type PageMeta = { title: string; description?: string; toc: TocEntry[] }

function readMeta(source: string): PageMeta {
  const tree = unified().use(remarkParse).use(remarkFrontmatter).use(remarkGfm).parse(source)
  const slugger = new GithubSlugger()
  let frontmatter: Record<string, string> = {}
  let h1 = ''
  const toc: TocEntry[] = []
  visit(tree, (node) => {
    if (node.type === 'yaml') frontmatter = parseYaml(node.value) ?? {}
    if (node.type !== 'heading') return
    const text = mdastToString(node)
    const id = slugger.slug(text)
    if (node.depth === 1) h1 ||= text
    else if (node.depth <= 3) toc.push({ depth: node.depth, id, text })
  })
  return { title: frontmatter.title ?? h1, description: frontmatter.description, toc }
}

export function markdown(): Plugin {
  return {
    name: 'pond-markdown',
    enforce: 'pre',
    async transform(source, id) {
      const [path, query] = id.split('?')
      if (!path.endsWith('.md')) return
      if (query === 'meta') return { code: `export const meta = ${JSON.stringify(readMeta(source))}`, map: null }
      const compiled = await compile(
        { value: source, path },
        {
          format: 'md',
          remarkPlugins: [remarkFrontmatter, remarkGfm, remarkDirective],
          rehypePlugins: [rehypeSlug],
        },
      )
      return { code: String(compiled), map: null }
    },
  }
}
