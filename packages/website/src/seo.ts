import { site } from './site.ts'

export function siteHead() {
  return {
    meta: [{ charSet: 'utf-8' }, { name: 'viewport', content: 'width=device-width, initial-scale=1' }, { title: site.title }],
    links: [{ rel: 'icon', href: '/icon.png' }],
    scripts: [] as Array<Record<string, string | boolean>>,
  }
}

export function pageHead({ title, description, path }: { title: string; description?: string; path: string }) {
  return {
    meta: [{ title }, { name: 'description', content: description ?? site.description }],
    links: [{ rel: 'canonical', href: `${site.url}${path}` }],
  }
}
