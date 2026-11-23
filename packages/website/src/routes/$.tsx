import { createFileRoute, notFound } from '@tanstack/react-router'
import { Suspense } from 'react'
import { Navigation } from '../components/navigation'
import { CopyButton } from '../components/copy'
import { DocFooter, Outline } from '../components/doc-navigation'
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
  const { slug, meta } = Route.useLoaderData()
  const { Body } = pages[slug]
  return (
    <>
      <Navigation />
      <main className="docs-main" id="content" key={slug}>
        <Outline slug={slug} meta={meta} />
        <article className="docs-page page-width prose">
          <Suspense>
            <Body components={markdownComponents} />
          </Suspense>
          <DocFooter slug={slug} meta={meta} />
          <CopyButton
            page
            read={async () => {
              const response = await fetch(`/${slug}.md`)
              if (!response.ok) throw new Error('Markdown is unavailable')
              return response.text()
            }}
          />
        </article>
      </main>
    </>
  )
}
