import type { ComponentType } from 'react'
import type { PageMeta } from '../markdown/plugin.ts'

// MDX hands each mapped component its own element's props, so no single prop type fits.
type Body = ComponentType<{ components?: Record<string, ComponentType<any>> }>

const metaModules = import.meta.glob<PageMeta>(['../content/**/*.md', '../../../docs/spec.md'], {
  eager: true,
  query: '?meta',
  import: 'meta',
})
const bodyModules = import.meta.glob<{ default: Body }>(['../content/**/*.md', '../../../docs/spec.md'])
const bodies = new Map<string, Body | Promise<Body>>()

export type Page = { slug: string; meta: PageMeta; body: () => Body | Promise<Body> }

export const pages: Record<string, Page> = Object.fromEntries(
  Object.entries(metaModules).map(([key, meta]) => {
    const slug = key.endsWith('/docs/spec.md') ? 'spec' : key.slice('../content/'.length, -'.md'.length)
    // The route loader awaits this, so the prerendered HTML and every client navigation render the
    // article at once; only hydration, where no loader runs, sees the promise and suspends.
    const body = () => {
      if (!bodies.has(slug)) {
        const loading = bodyModules[key]().then((module) => {
          bodies.set(slug, module.default)
          return module.default
        })
        bodies.set(slug, loading)
      }
      return bodies.get(slug)!
    }
    return [slug, { slug, meta, body }]
  }),
)
