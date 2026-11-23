import tailwindcss from '@tailwindcss/vite'
import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import viteReact from '@vitejs/plugin-react'
import { defineConfig } from 'vite'
import { markdown, pageSources } from './markdown/plugin.ts'
import { staticFiles } from './static-files.ts'

export default defineConfig({
  server: { fs: { allow: ['../..'] } },
  plugins: [
    staticFiles(),
    markdown(),
    tailwindcss(),
    tanstackStart({
      prerender: {
        enabled: true,
        crawlLinks: true,
        failOnError: true,
        // A crawled `/page#section` link would render `/page` again.
        filter: (page) => !page.path.includes('#'),
        // React streams a suspended subtree into a hidden segment that only a script moves into place.
        onSuccess: ({ page, html }) => {
          if (html.includes('<template id="B:')) {
            throw new Error(`${page.path} suspended while prerendering; await its data in the route loader instead`)
          }
        },
      },
      pages: [
        ...['/', ...Object.keys(pageSources())].map((path) => ({ path })),
        { path: '/404', prerender: { outputPath: '/404.html' } },
      ],
    }),
    viteReact(),
  ],
})
