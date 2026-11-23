import { createRootRoute, HeadContent, Outlet, Scripts } from '@tanstack/react-router'
import { NotFound } from './404.tsx'
import { siteHead } from '../seo.ts'
import styles from '../styles.css?url'

export const Route = createRootRoute({
  head: () => {
    const head = siteHead()
    return {
      ...head,
      links: [
        ...head.links,
        { rel: 'preload', href: '/geist.woff2', as: 'font', type: 'font/woff2', crossOrigin: 'anonymous' },
        { rel: 'stylesheet', href: styles },
      ],
    }
  },
  component: Root,
  notFoundComponent: NotFound,
})

function Root() {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        <Outlet />
        <Scripts />
      </body>
    </html>
  )
}
