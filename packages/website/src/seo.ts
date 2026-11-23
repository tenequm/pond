import { site } from './site.ts'

export function pageHead({ title, description, path }: { title: string; description?: string; path: string }) {
  return {
    meta: [{ title }, { name: 'description', content: description ?? site.description }],
    links: [{ rel: 'canonical', href: `${site.url}${path}` }],
  }
}
