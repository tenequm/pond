import { createFileRoute } from '@tanstack/react-router'
import { CodeBlock } from '../components/markdown.tsx'
import { Navigation, SiteLink } from '../components/navigation.tsx'
import { home, pageHead } from '../seo.ts'
import { site } from '../site.ts'

export const Route = createFileRoute('/')({
  head: () => pageHead({ ...home, path: '/' }),
  component: Home,
})

function Home() {
  const commands = [
    ['brew', ' install tenequm/tap/pond', '   # macOS / Linux'],
    ['pond', ' init', '   # guided setup'],
    ['pond', ' sync', '   # ingest and index'],
  ]
  return (
    <>
      <Navigation home />
      <main id="content" className="home-main">
        <article className="page-width">
          <div className="home">
            <img className="home-logo" src="/logo.png" alt="pond" />
            <p className="tagline">{site.description}</p>
            <CodeBlock data-lang="sh">
              <code>
                {commands.map(([command, args, comment], i) => (
                  <span className="line" key={i}>
                    <span style={{ color: '#f69d50' }}>{command}</span>
                    <span style={{ color: '#96d0ff' }}>{args}</span>
                    <span style={{ color: '#768390' }}>{comment}</span>
                  </span>
                ))}
              </code>
            </CodeBlock>
            <p>
              Works with Claude Code &middot; Codex CLI &middot; OpenCode &middot; OpenClaw &middot; oh-my-pi
              &middot; Claude Desktop &middot; Claude.ai &middot; Pi &middot; Letta Code &middot; Grok
              &middot; Antigravity &middot; Devin &middot; Hermes &middot; nanoclaw
            </p>
            <p>
              Runs on macOS &middot; Linux &middot; Windows -{' '}
              <SiteLink href="/get-started/install">all install options</SiteLink>
            </p>
            <div className="home-buttons">
              <SiteLink href="/get-started/quickstart" className="plate primary">
                Get started
              </SiteLink>
              <SiteLink href="/why" className="plate">
                Why pond?
              </SiteLink>
              <SiteLink href={site.socials.github} className="plate">
                GitHub
              </SiteLink>
            </div>
            <video
              src="/demo-search.mp4"
              autoPlay
              loop
              muted
              playsInline
              aria-label="Pond session search demo"
            />
            <p>
              Questions or feedback? Start a{' '}
              <a href={`${site.socials.github}/discussions`}>GitHub Discussion</a>, or DM me on{' '}
              <a href={site.socials.telegram}>Telegram</a> or <a href={site.socials.x}>X</a> - I
              answer personally.
            </p>
          </div>
        </article>
      </main>
    </>
  )
}
