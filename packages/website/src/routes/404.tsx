import { createFileRoute, Link } from '@tanstack/react-router'
import { FileQuestionMark, House } from 'lucide-react'

export function NotFound() {
  return (
    <div className="missing">
      <span>
        <FileQuestionMark size={40} />
      </span>
      <h1>Page not found</h1>
      <p>The page you're looking for doesn't exist or has been moved.</p>
      <Link to="/" className="plate">
        <House size={16} />
        Back to home
      </Link>
    </div>
  )
}

export const Route = createFileRoute('/404')({ component: NotFound })
