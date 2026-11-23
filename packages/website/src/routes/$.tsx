import { createFileRoute, notFound } from '@tanstack/react-router'
import { Suspense } from 'react'
import { markdownComponents } from '../components/markdown.tsx'
import { pages } from '../content.ts'
import { pageHead } from '../seo.ts'

export const Route = createFileRoute('/$')({
  loader: ({ params }) => {
    const page = pages[params._splat ?? '']
    if (!page) throw notFound()
    return { slug: page.slug, meta: page.meta }
  },
  head: ({ loaderData }) =>
    loaderData
      ? pageHead({
          title: `${loaderData.meta.title} - pond`,
          description: loaderData.meta.description,
          path: `/${loaderData.slug}`,
        })
      : {},
  component: DocPage,
})

function DocPage() {
  const { slug } = Route.useLoaderData()
  const { Body } = pages[slug]
  return (
    <article className="p-4">
      <Suspense>
        <Body components={markdownComponents} />
      </Suspense>
    </article>
  )
}
