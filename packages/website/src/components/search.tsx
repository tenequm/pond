import { CornerDownRight, FileText, Hash, Search } from 'lucide-react'
import MiniSearch from 'minisearch'
import { Dialog } from 'radix-ui'
import { useEffect, useMemo, useRef, useState } from 'react'
import { plainClick, SiteLink } from './navigation.tsx'

type Hit = { href: string; title: string; titles: string; text: string }
type SearchDoc = (typeof import('virtual:pond-search'))['default'][number]

const words = (text: string) => text.split(/[^\p{L}\p{N}_]+/u).filter(Boolean)

// The index also holds the words inside a camelCase or snake_case identifier; a query is matched as typed.
function tokenize(text: string) {
  return words(text).flatMap((word) => {
    const parts = word
      .replace(/([a-z])([A-Z])/g, '$1 $2')
      .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
      .toLowerCase()
      .split(/[ _]+/)
      .filter(Boolean)
    const whole = word.toLowerCase()
    return parts.includes(whole) ? parts : [whole, ...parts]
  })
}

function buildIndex(docs: SearchDoc[]) {
  const engine = new MiniSearch<Hit>({
    fields: ['category', 'subtitle', 'text', 'title', 'titles'],
    idField: 'href',
    tokenize,
    searchOptions: {
      tokenize: words,
      combineWith: 'AND',
      fuzzy: 0.2,
      prefix: true,
      boost: { title: 4, subtitle: 3, text: 2, category: 1, titles: 1 },
    },
  })
  const byHref = new Map<string, Hit>()
  for (const doc of docs) {
    for (const section of doc.sections) {
      const href = '/' + doc.slug + (section.id ? '#' + section.id : '')
      const hit = {
        href,
        title: section.id ? section.heading : doc.title,
        titles: section.id ? [doc.title, ...section.ancestors].join(' > ') : '',
        text: section.text,
        category: doc.category,
        subtitle: section.id ? undefined : doc.subtitle,
      }
      engine.add(hit)
      byHref.set(href, hit)
    }
  }
  return { engine, byHref }
}

let cachedIndex: ReturnType<typeof buildIndex> | undefined
const historyKey = 'pond-docs-recent-searches'

export default function SearchPanel({ onClose }: { onClose: () => void }) {
  const [index, setIndex] = useState<typeof cachedIndex>(undefined)
  const [failed, setFailed] = useState(false)
  const [query, setQuery] = useState('')
  const [recent, setRecent] = useState<string[]>([])
  const [selected, setSelected] = useState(0)
  const list = useRef<HTMLUListElement>(null)
  const returnFocus = useRef(document.activeElement)
  useEffect(() => {
    let mounted = true
    void import('virtual:pond-search')
      .then(({ default: docs }) => {
        if (mounted) setIndex(cachedIndex ??= buildIndex(docs))
      })
      .catch(() => {
        if (mounted) setFailed(true)
      })
    try {
      const stored: unknown = JSON.parse(localStorage.getItem(historyKey) ?? '[]')
      if (Array.isArray(stored)) setRecent(stored.filter((item) => typeof item === 'string').slice(0, 5))
    } catch {
      /* Storage may be disabled. */
    }
    return () => {
      mounted = false
    }
  }, [])
  const terms = useMemo(() => query.trim().toLowerCase().split(/\s+/).filter(Boolean), [query])
  const hits = useMemo(() => {
    if (!index) return []
    if (!terms.length) return recent.flatMap((href) => {
      const hit = index.byHref.get(href)
      return hit ? [{ hit, matched: [] as string[] }] : []
    })
    return index.engine.search(query).slice(0, 20).map(({ id, terms: matched }) => ({
      hit: index.byHref.get(id)!,
      matched,
    }))
  }, [query, terms, recent, index])
  function choose(hit: Hit) {
    try {
      localStorage.setItem(
        historyKey,
        JSON.stringify([hit.href, ...recent.filter((href) => href !== hit.href)].slice(0, 5)),
      )
    } catch {
      /* Navigation also works without storage. */
    }
    onClose()
  }
  return (
    <Dialog.Root
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay className="overlay" />
        <Dialog.Content
          className="search-panel"
          onCloseAutoFocus={(event) => {
            event.preventDefault()
            if (returnFocus.current instanceof HTMLElement) returnFocus.current.focus({ preventScroll: true })
          }}
          onKeyDown={(event) => {
            // Enter and the arrows belong to an input method while it is composing.
            if (!hits.length || event.nativeEvent.isComposing || event.keyCode === 229) return
            if (event.key === 'Enter' && event.target instanceof HTMLInputElement) {
              event.preventDefault()
              list.current?.querySelector<HTMLAnchorElement>(`#search-hit-${selected} a`)?.click()
            }
            if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
              event.preventDefault()
              const i = (selected + (event.key === 'ArrowDown' ? 1 : -1) + hits.length) % hits.length
              setSelected(i)
              list.current?.children[i]?.scrollIntoView({ block: 'nearest' })
            }
          }}
        >
          <Dialog.Title className="sr-only">Search documentation</Dialog.Title>
          <Dialog.Description className="sr-only">
            Use the arrow keys to navigate and Enter to select.
          </Dialog.Description>
          <div className="search-input">
            <Search size={20} />
            <input
              type="text"
              role="combobox"
              aria-label="Search documentation"
              aria-autocomplete="list"
              aria-controls="search-results"
              aria-expanded={!!hits.length}
              aria-activedescendant={hits.length ? `search-hit-${selected}` : undefined}
              placeholder="Search..."
              autoComplete="off"
              spellCheck={false}
              value={query}
              onChange={(e) => {
                setQuery(e.target.value)
                setSelected(0)
              }}
            />
          </div>
          <div className="search-scroll">
            {!hits.length && (
              <p>
                {failed
                  ? 'Search could not load.'
                  : terms.length
                    ? index
                      ? 'No results found'
                      : 'Loading...'
                    : 'Start typing to search...'}
              </p>
            )}
            {!terms.length && !!hits.length && <small>Recent searches</small>}
            <ul id="search-results" ref={list} role="listbox" aria-label="Search results">
              {hits.map(({ hit, matched }, i) => {
                const words = [...new Set([...terms, ...matched])].sort((a, b) => b.length - a.length)
                const pattern = words.length
                  ? new RegExp('(' + words.map((word) => word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|') + ')', 'gi')
                  : null
                const highlight = (text: string) => pattern
                  ? text.split(pattern).map((part, i) => (i % 2 ? <mark key={i}>{part}</mark> : part))
                  : text
                const match = pattern ? Math.max(0, hit.text.search(pattern)) : 0
                const start = Math.max(0, match - 80)
                const jump = !hit.titles && hit.title.toLowerCase() === query.trim().toLowerCase()
                const Icon = jump ? CornerDownRight : hit.href.includes('#') ? Hash : FileText
                return (
                  <li key={hit.href} id={`search-hit-${i}`} role="option" aria-selected={selected === i}>
                    <SiteLink href={hit.href} onClick={(event) => plainClick(event) && choose(hit)}>
                      <Icon size={16} />
                      <div>
                        {jump ? (
                          <>
                            Jump to <strong>{highlight(hit.title)}</strong>
                          </>
                        ) : (
                          <>
                            {hit.titles && <small>{hit.titles}</small>}
                            <strong>{highlight(hit.title)}</strong>
                            <span>
                              {start > 0 && '\u2026'}
                              {highlight(hit.text.slice(start, match + 160))}
                            </span>
                          </>
                        )}
                      </div>
                    </SiteLink>
                  </li>
                )
              })}
            </ul>
          </div>
          <div className="search-help">
            <kbd>&uarr;</kbd>
            <kbd>&darr;</kbd> navigate <kbd>&#8629;</kbd> select <kbd>esc</kbd> close
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
