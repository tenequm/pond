import { createRootRoute, HeadContent, Link, Outlet, Scripts } from '@tanstack/react-router'
import styles from '../styles.css?url'

export const Route = createRootRoute({
  head: () => ({
    meta: [{ charSet: 'utf-8' }, { name: 'viewport', content: 'width=device-width, initial-scale=1' }, { title: 'pond' }],
    links: [{ rel: 'stylesheet', href: styles }],
  }),
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
          <Link to="/$" params={{ _splat: 'why' }}>why</Link>
          <Link to="/$" params={{ _splat: 'get-started/install' }}>install</Link>
        </nav>
        <Outlet />
        <Scripts />
      </body>
    </html>
  )
}
