import { CornerDownRight, FileText, Hash, Search } from 'lucide-react'
import { Dialog } from 'radix-ui'
import { useEffect, useMemo, useRef, useState } from 'react'
import { plainClick, SiteLink } from './navigation.tsx'

type Hit = { href: string; page: string; heading: string; text: string }
const historyKey = 'pond-docs-recent-searches'

export default function SearchPanel({ onClose }: { onClose: () => void }) {
  const [index, setIndex] = useState<Hit[]>([])
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
        if (!mounted) return
        setIndex(
          docs.flatMap((doc) =>
            doc.sections.map((section) => ({
              href: '/' + doc.slug + (section.id ? '#' + section.id : ''),
              heading: section.heading || doc.title,
              page: doc.title,
              text: section.text,
            })),
          ),
        )
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
    if (!terms.length) return recent.flatMap((href) => index.find((hit) => hit.href === href) ?? [])
    return index
      .map((hit) => {
        const heading = hit.heading.toLowerCase()
        const text = hit.text.toLowerCase()
        const scores = terms.map((term) =>
          heading === term
            ? 20
            : heading.includes(term)
              ? 8
              : hit.page.toLowerCase().includes(term)
                ? 4
                : text.includes(term)
                  ? 1
                  : 0,
        )
        return { hit, score: scores.includes(0) ? 0 : scores.reduce<number>((a, b) => a + b, 0) }
      })
      .filter((entry) => entry.score)
      .sort((a, b) => b.score - a.score)
      .slice(0, 20)
      .map((entry) => entry.hit)
  }, [terms, recent, index])
  const highlightPattern = useMemo(
    () =>
      terms.length
        ? new RegExp(
            '(' + terms.map((term) => term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|') + ')',
            'gi',
          )
        : null,
    [terms],
  )
  function highlight(text: string) {
    return highlightPattern
      ? text.split(highlightPattern).map((part, i) => (i % 2 ? <mark key={i}>{part}</mark> : part))
      : text
  }
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
                    ? index.length
                      ? 'No results found'
                      : 'Loading...'
                    : 'Start typing to search...'}
              </p>
            )}
            {!terms.length && !!hits.length && <small>Recent searches</small>}
            <ul id="search-results" ref={list} role="listbox" aria-label="Search results">
              {hits.map((hit, i) => {
                const match = terms.length ? hit.text.toLowerCase().indexOf(terms[0]) : 0
                const start = Math.max(0, match - 80)
                const jump =
                  terms.length > 0 &&
                  !hit.href.includes('#') &&
                  hit.page.toLowerCase() === query.trim().toLowerCase()
                const Icon = jump ? CornerDownRight : hit.href.includes('#') ? Hash : FileText
                return (
                  <li key={hit.href} id={`search-hit-${i}`} role="option" aria-selected={selected === i}>
                    <SiteLink href={hit.href} onClick={(event) => plainClick(event) && choose(hit)}>
                      <Icon size={16} />
                      <div>
                        {jump ? (
                          <>
                            Jump to <strong>{highlight(hit.page)}</strong>
                          </>
                        ) : (
                          <>
                            {hit.page !== hit.heading && <small>{hit.page}</small>}
                            <strong>{highlight(hit.heading)}</strong>
                            <span>
                              {start > 0 && '\u2026'}
                              {highlight(hit.text.slice(start, Math.max(0, match) + 160))}
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
