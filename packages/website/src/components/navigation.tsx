import { Link, useLocation } from '@tanstack/react-router'
import { ChevronDown, TextAlignJustify, Search, X } from 'lucide-react'
import { Dialog, DropdownMenu } from 'radix-ui'
import { type ComponentProps, lazy, Suspense, useEffect, useState } from 'react'
import { site } from '../site'

const SearchPanel = lazy(() => import('./search'))

export function SiteLink({ href = '', children, ...props }: ComponentProps<'a'>) {
  if (/^(?:[a-z][\w+.-]*:|\/\/)/i.test(href)) {
    return (
      <a href={href} target="_blank" rel="noopener noreferrer" {...props}>
        {children}
      </a>
    )
  }
  const [path, hash] = href.split('#')
  return (
    <Link
      {...props}
      to={path || '.'}
      hash={hash}
      activeOptions={{ exact: true, includeHash: Boolean(hash) }}
      activeProps={{ className: '' }}
    >
      {children}
    </Link>
  )
}

function SidebarLinks({ onNavigate }: { onNavigate?: () => void }) {
  return (
    <nav className="sidebar-links" aria-label="Documentation">
      {site.sidebar.map((entry, index) =>
        'items' in entry ? (
          <section key={entry.text}>
            <h2>{entry.text}</h2>
            {entry.items.map((item) => (
              <SiteLink key={item.link} href={item.link} onClick={onNavigate}>
                {item.text}
              </SiteLink>
            ))}
          </section>
        ) : (
          <SiteLink
            className={index && 'items' in site.sidebar[index - 1] ? 'after-group' : undefined}
            key={entry.link}
            href={entry.link}
            onClick={onNavigate}
          >
            {entry.text}
          </SiteLink>
        ),
      )}
    </nav>
  )
}

function Socials() {
  return (
    <div className="socials">
      {site.socials.map(({ icon, link }) => (
        <a
          key={icon}
          href={link}
          target="_blank"
          rel="noopener noreferrer"
          aria-label={{ github: 'GitHub', telegram: 'Telegram', x: 'X (Twitter)' }[icon]}
        >
          <span style={{ maskImage: `url(/social-${icon}.svg)` }} />
        </a>
      ))}
    </div>
  )
}

export function Navigation({ home = false }: { home?: boolean }) {
  const path = useLocation({ select: (l) => l.pathname.replace(/\/$/, '') })
  const [menu, setMenu] = useState(false)
  const [search, setSearch] = useState(false)
  const [hideTop, setHideTop] = useState(false)
  useEffect(() => {
    function shortcut(event: KeyboardEvent) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        setMenu(false)
        setSearch((open) => !open)
      }
    }
    let lastY = window.scrollY
    function scroll() {
      if (Math.abs(window.scrollY - lastY) < 10) return
      setHideTop(window.scrollY > 48 && window.scrollY > lastY)
      lastY = window.scrollY
    }
    window.addEventListener('keydown', shortcut)
    window.addEventListener('scroll', scroll, { passive: true })
    return () => {
      window.removeEventListener('keydown', shortcut)
      window.removeEventListener('scroll', scroll)
    }
  }, [])
  const logo = (
    <SiteLink className="brand" href="/" aria-label="pond home">
      <img src="/logo.png" alt="Logo" />
    </SiteLink>
  )
  return (
    <>
      <a className="skip" href="#content">
        Skip to content
      </a>
      {!home && (
        <div className="sidebar-gutter">
          <div className="sidebar-logo">{logo}</div>
          <aside className="sidebar">
            <SidebarLinks />
            <Socials />
          </aside>
        </div>
      )}
      <header className={`topbar ${home ? 'home-bar' : ''}`} data-hidden={hideTop}>
        <div className="top-left">
          {logo}
          {!home && (
            <button className="search-trigger" onClick={() => setSearch(true)} aria-haspopup="dialog">
              <Search size={16} /> Search...{' '}
              <span className="ml-auto flex gap-0.5">
                <kbd>&#8984;</kbd>
                <kbd>K</kbd>
              </span>
            </button>
          )}
        </div>
        <nav className="top-links" aria-label="Main navigation">
          {site.topNav.map((item) => (
            <SiteLink key={item.link} href={item.link}>
              {item.text}
            </SiteLink>
          ))}
        </nav>
        <div className="mobile-actions">
          {!home && (
            <button aria-label="Search" onClick={() => setSearch(true)}>
              <Search size={16} />
            </button>
          )}
          <Dialog.Root open={menu} onOpenChange={setMenu}>
            <Dialog.Trigger aria-label="Open menu">
              <TextAlignJustify size={16} />
            </Dialog.Trigger>
            <Dialog.Portal>
              <Dialog.Overlay className="overlay" />
              <Dialog.Content className="mobile-drawer" aria-describedby={undefined}>
                <Dialog.Title className="sr-only">Menu</Dialog.Title>
                <div className="drawer-head">
                  <DropdownMenu.Root>
                    <DropdownMenu.Trigger className="drawer-select">
                      {(site.topNav.find((item) => item.link === path) ?? site.topNav[0]).text}
                      <ChevronDown size={16} />
                    </DropdownMenu.Trigger>
                    <DropdownMenu.Portal>
                      <DropdownMenu.Content align="start" sideOffset={4} className="drawer-options">
                        {site.topNav.map((item) => (
                          <DropdownMenu.Item asChild key={item.link}>
                            <SiteLink href={item.link} onClick={() => setMenu(false)}>
                              {item.text}
                            </SiteLink>
                          </DropdownMenu.Item>
                        ))}
                      </DropdownMenu.Content>
                    </DropdownMenu.Portal>
                  </DropdownMenu.Root>
                  <Dialog.Close aria-label="Close menu">
                    <X size={16} />
                  </Dialog.Close>
                </div>
                <div className="drawer-scroll">
                  <SidebarLinks onNavigate={() => setMenu(false)} />
                  <Socials />
                </div>
              </Dialog.Content>
            </Dialog.Portal>
          </Dialog.Root>
        </div>
      </header>
      {search && (
        <Suspense>
          <SearchPanel onClose={() => setSearch(false)} />
        </Suspense>
      )}
    </>
  )
}
