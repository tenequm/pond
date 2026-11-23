import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/404')({ component: () => <h1>Page not found</h1> })
