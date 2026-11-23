import { globSync } from 'node:fs'
import tailwindcss from '@tailwindcss/vite'
import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import viteReact from '@vitejs/plugin-react'
import { defineConfig } from 'vite'
import { markdown } from './markdown/plugin.ts'

const docPaths = globSync('**/*.md', { cwd: '../../docs/website' }).map((f) => `/${f.replace(/\.md$/, '')}`)

export default defineConfig({
  resolve: { tsconfigPaths: true },
  server: { port: 5173, fs: { allow: ['../..'] } },
  plugins: [
    markdown({ repo: 'tenequm/pond' }),
    tailwindcss(),
    tanstackStart({
      prerender: { enabled: true, crawlLinks: true, failOnError: true },
      pages: [
        ...['/', '/spec', ...docPaths].map((path) => ({ path })),
        { path: '/404', prerender: { outputPath: '/404.html' } },
      ],
    }),
    viteReact(),
  ],
})
