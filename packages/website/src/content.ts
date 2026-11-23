import { type ComponentType, lazy } from 'react'
import type { PageMeta, SearchDoc } from '../markdown/plugin.ts'

// MDX hands each mapped component its own element's props, so no single prop type fits.
// biome-ignore lint/suspicious/noExplicitAny: see above
type Body = ComponentType<{ components?: Record<string, ComponentType<any>> }>

const metaModules = import.meta.glob<PageMeta>(['../content/**/*.md', '../../../docs/spec.md'], {
  eager: true,
  query: '?meta',
  import: 'meta',
})
const bodyModules = import.meta.glob<{ default: Body }>(['../content/**/*.md', '../../../docs/spec.md'])

const slugOf = (key: string) => key.replace(/^.*\/(content|docs)\//, '').replace(/\.md$/, '')

export type Page = { slug: string; meta: PageMeta; Body: Body }

export const pages: Record<string, Page> = Object.fromEntries(
  Object.entries(metaModules).map(([key, meta]) => {
    const slug = slugOf(key)
    return [slug, { slug, meta, Body: lazy(bodyModules[key]) }]
  }),
)

export const loadSearchIndex = (): Promise<SearchDoc[]> => import('virtual:pond-search').then((module) => module.default)
