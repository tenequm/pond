import { useNavigate } from '@tanstack/react-router'
import { ArrowLeft, ArrowRight, ArrowUp, ChevronRight, History, TextAlignStart } from 'lucide-react'
import { Popover } from 'radix-ui'
import { useEffect, useMemo, useRef, useState } from 'react'
import type { Page } from '../content'
import { site } from '../site'
import { SiteLink } from './navigation'

const order = site.sidebar.flatMap<{ text: string; link: string }>((entry) =>
  'items' in entry ? entry.items : [entry],
)

export function Outline({ slug, meta }: Pick<Page, 'slug' | 'meta'>) {
  const versions = slug === 'changelog'
  const items = useMemo(() => meta.toc.filter((item) => item.depth <= 3), [meta])
  const [active, setActive] = useState(items[0]?.id)
  const [scrolled, setScrolled] = useState(false)
  const [open, setOpen] = useState(false)
  const rail = useRef<HTMLElement>(null)
  useEffect(() => {
    function scroll() {
      const threshold = innerHeight * 0.2
      let lo = 0
      let hi = items.length
      while (lo < hi) {
        const mid = (lo + hi) >>> 1
        const heading = document.getElementById(items[mid].id)
        if (heading && heading.getBoundingClientRect().top <= threshold) lo = mid + 1
        else hi = mid
      }
      const atEnd =
        window.scrollY > 0 && window.scrollY + innerHeight >= document.documentElement.scrollHeight - 10
      setActive(items[atEnd ? items.length - 1 : Math.max(0, lo - 1)]?.id)
      setScrolled(window.scrollY > 100)
    }
    scroll()
    window.addEventListener('scroll', scroll, { passive: true })
    return () => window.removeEventListener('scroll', scroll)
  }, [items])
  useEffect(() => {
    const container = rail.current
    const link = container?.querySelector('[data-current="true"]')
    if (!container || !link) return
    const bounds = container.getBoundingClientRect()
    const target = link.getBoundingClientRect()
    if (target.bottom > bounds.bottom || target.top < bounds.top)
      container.scrollTop += target.top - bounds.top - 16
  }, [active])
  if (!items.length) return null
  const current = items.findIndex((item) => item.id === active)
  const parent = items.map((item) => item.depth).lastIndexOf(2, current)
  const list = (
    <ul className="outline-list">
      {items.map((item, i) => (
        <li key={item.id} data-track={i >= parent && i <= current}>
          <SiteLink
            href={`/${slug}#${item.id}`}
            style={{ paddingLeft: 12 + (item.depth - 2) * 16 }}
            aria-current={item.id === active ? 'location' : undefined}
            data-current={item.id === active}
            data-parent={i === parent}
            onClick={() => setOpen(false)}
          >
            {item.text}
          </SiteLink>
        </li>
      ))}
    </ul>
  )
  const label = versions ? (
    <>
      <History size={14} /> Versions
    </>
  ) : (
    <>
      <TextAlignStart size={14} /> On this page
    </>
  )
  return (
    <>
      {!versions && (
        <div className="mobile-outline">
          <Popover.Root open={open} onOpenChange={setOpen}>
            <Popover.Trigger>
              {label}
              <ChevronRight size={14} />
            </Popover.Trigger>
            <Popover.Portal>
              <Popover.Content align="start" sideOffset={8} className="outline-popup">
                {list}
              </Popover.Content>
            </Popover.Portal>
          </Popover.Root>
          {scrolled && (
            <button onClick={() => window.scrollTo({ top: 0, behavior: 'smooth' })}>
              <ArrowUp size={14} />
              Return to top
            </button>
          )}
        </div>
      )}
      <nav
        aria-label={versions ? 'Versions' : 'On this page'}
        className={versions ? 'versions-rail' : 'outline-rail'}
        ref={rail}
      >
        <div className="outline-label">{label}</div>
        {list}
      </nav>
    </>
  )
}

export function DocFooter({ slug, meta }: Pick<Page, 'slug' | 'meta'>) {
  const index = order.findIndex((item) => item.link === '/' + slug)
  const previous = order[index - 1]
  const next = order[index + 1]
  const navigate = useNavigate()
  useEffect(() => {
    function shortcut(event: KeyboardEvent) {
      if (
        !event.shiftKey ||
        event.ctrlKey ||
        event.altKey ||
        event.metaKey ||
        document.querySelector('[role="dialog"]') ||
        (event.target instanceof HTMLElement &&
          (event.target.isContentEditable || event.target.matches('input,textarea,select')))
      )
        return
      const item = event.key === 'ArrowLeft' ? previous : event.key === 'ArrowRight' ? next : undefined
      if (item) {
        event.preventDefault()
        void navigate({ to: item.link })
      }
    }
    window.addEventListener('keydown', shortcut)
    return () => window.removeEventListener('keydown', shortcut)
  }, [previous, next, navigate])
  return (
    <footer className="doc-footer">
      {meta.lastUpdated && meta.source.startsWith('docs/website/') && (
        <p>
          Last updated:{' '}
          <time dateTime={meta.lastUpdated}>
            {new Intl.DateTimeFormat('en-US', {
              month: '2-digit',
              day: '2-digit',
              year: 'numeric',
              hour: '2-digit',
              minute: '2-digit',
              timeZone: 'UTC',
            }).format(new Date(meta.lastUpdated))}
          </time>
        </p>
      )}
      <nav aria-label="Pagination">
        {[previous, next].map(
          (item, i) =>
            item && (
              <SiteLink key={item.link} href={item.link} className={i ? 'next' : 'previous'}>
                <span>
                  {!i && <ArrowLeft size={16} />}
                  {item.text}
                  {Boolean(i) && <ArrowRight size={16} />}
                </span>
                <small>
                  {i ? 'Next' : 'Previous'} <kbd>Shift</kbd>
                  <kbd>{i ? '\u2192' : '\u2190'}</kbd>
                </small>
              </SiteLink>
            ),
        )}
      </nav>
    </footer>
  )
}
