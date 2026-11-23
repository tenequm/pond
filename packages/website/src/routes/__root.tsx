import { createRootRoute, HeadContent, Link, Outlet, Scripts } from '@tanstack/react-router'
import { siteHead } from '../seo.ts'
import styles from '../styles.css?url'

export const Route = createRootRoute({
  head: () => {
    const head = siteHead()
    return { ...head, links: [...head.links, { rel: 'stylesheet', href: styles }] }
  },
  component: Root,
  notFoundComponent: () => <h1>Page not found</h1>,
})

function Root() {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        <nav className="flex gap-4 p-4">
          <Link to="/">home</Link>
          <Link to="/$" params={{ _splat: 'why' }}>
            why
          </Link>
          <Link to="/$" params={{ _splat: 'get-started/install' }}>
            install
          </Link>
        </nav>
        <Outlet />
        <Scripts />
      </body>
    </html>
  )
}
