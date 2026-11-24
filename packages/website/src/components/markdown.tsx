import {
  Check,
  ChevronDown,
  CircleAlert,
  Clipboard,
  ClipboardList,
  ExternalLink,
  Info,
  Lightbulb,
  Link as Chain,
  TriangleAlert,
} from 'lucide-react'
import { Tabs } from 'radix-ui'
import {
  Children,
  cloneElement,
  type ComponentProps,
  type CSSProperties,
  isValidElement,
  type ReactNode,
  useEffect,
  useRef,
  useState,
} from 'react'
import { SiteLink } from './navigation.tsx'

// The Markdown pipeline (markdown/plugin.ts) emits these custom tags; everything
// else in a page body is a plain HTML element.
type PageTitleProps = { subtitle?: string; children: ReactNode }
type CalloutProps = {
  type: 'info' | 'note' | 'tip' | 'warning' | 'danger'
  title?: string
  children: ReactNode
}
type ReleaseProps = { version: string; date: string; url: string; children: ReactNode }
// Highlighted code arrives as <pre data-lang="sh" data-title="Homebrew"> holding one <code>;
// data-lang is set only when the fence names a language, data-title only for ```sh [Homebrew].
type PreProps = ComponentProps<'pre'> & { 'data-lang'?: string; 'data-title'?: string }

declare global {
  interface Window {
    rybbit?: { event(name: string, props: Record<string, string>): void | Promise<void> }
  }
}

const installMethods: [string, RegExp][] = [
  ['brew', /^\s*brew install (?:tenequm\/tap\/)?pond(?:\s|$)/m],
  ['nix', /^\s*nix (?:profile (?:add|install)|run|shell) github:tenequm\/pond(?:#pond)?(?:\s|$)/m],
  ['scoop', /^\s*scoop install (?:tenequm\/)?pond(?:\s|$)/m],
  ['cargo', /^\s*cargo install pond-db(?:\s|$)/m],
  ['cargo-binstall', /^\s*cargo binstall pond-db(?:\s|$)/m],
  ['curl', /^\s*curl\s+[^|\n]*pond[^|\n]*\|\s*(?:ba)?sh\b/m],
  ['npm', /^\s*npm (?:install|i|add) (?:-[\w-]+ )*(?:pond(?:-db)?|openclaw-pond|pi-pond)(?:@|\s|$)/m],
]

async function trackCommandCopy(text: string) {
  const method = installMethods.find(([, pattern]) => pattern.test(text))?.[0]
  const page = window.location.pathname
  if (method) await window.rybbit?.event('install_command_copied', { method, page })
  else await window.rybbit?.event('command_copied', { page })
}

export function CopyButton({
  page,
  line,
  read,
}: {
  page?: boolean
  line?: number
  read: () => string | Promise<string>
}) {
  const [state, setState] = useState<'idle' | 'copied' | 'failed'>('idle')
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  useEffect(() => () => clearTimeout(timer.current), [])
  const Icon = state === 'copied' ? Check : Clipboard
  const label =
    state === 'copied'
      ? 'Copied!'
      : state === 'failed'
        ? 'Copy failed'
        : page
          ? 'Copy page for AI'
          : line === undefined
            ? 'Copy code'
            : 'Copy command'
  async function copy() {
    try {
      // Start the clipboard write inside the click gesture even when Markdown needs fetching.
      const value = read()
      if (typeof value === 'string') await navigator.clipboard.writeText(value)
      else {
        // A failed fetch still reports through the write below, and stays handled if the write throws first.
        value.catch(() => {})
        await navigator.clipboard.write([
          new ClipboardItem({
            'text/plain': value.then((text) => new Blob([text], { type: 'text/plain' })),
          }),
        ])
      }
      setState('copied')
      if (!page && typeof value === 'string') void trackCommandCopy(value).catch(() => {})
    } catch {
      setState('failed')
    }
    clearTimeout(timer.current)
    timer.current = setTimeout(() => setState('idle'), 2000)
  }
  return (
    <button
      type="button"
      className={page ? 'copy-page' : line === undefined ? 'copy-code' : 'copy-code copy-command'}
      onClick={copy}
      style={line === undefined ? undefined : ({ '--line': line } as CSSProperties)}
      aria-label={page ? undefined : label}
      title={label}
      data-state={state}
    >
      <Icon size={line === undefined ? 16 : 14} />
      {page && <span>{label}</span>}
    </button>
  )
}

function PageTitle({ subtitle, children }: PageTitleProps) {
  return subtitle ? (
    <hgroup>
      {children}
      <p role="doc-subtitle">{subtitle}</p>
    </hgroup>
  ) : (
    children
  )
}

function Callout({ type, title, children }: CalloutProps) {
  const Icon = {
    info: Info,
    note: ClipboardList,
    tip: Lightbulb,
    warning: TriangleAlert,
    danger: CircleAlert,
  }[type]
  return (
    <aside className="callout" data-type={type}>
      <Icon size={14} aria-hidden />
      {title && (
        <p>
          <strong>{title}</strong>
        </p>
      )}
      {children}
    </aside>
  )
}

function heading(Tag: 'h2' | 'h3' | 'h4' | 'h5' | 'h6') {
  return function SectionHeading({ id, children, ...props }: ComponentProps<'h2'>) {
    const [copied, setCopied] = useState(false)
    return (
      <Tag {...props} id={id}>
        {children}
        {id && (
          <SiteLink
            href={`#${id}`}
            className="heading-anchor"
            aria-label="Copy link and go to this section"
            title="Copy link and go to this section"
            onClick={(event) => {
              void navigator.clipboard?.writeText(event.currentTarget.href).then(
                () => setCopied(true),
                () => {},
              )
            }}
            onBlur={() => setCopied(false)}
            onMouseLeave={() => setCopied(false)}
          >
            {copied ? <Check size="0.75em" /> : <Chain size="0.75em" />}
          </SiteLink>
        )}
      </Tag>
    )
  }
}

function codeText(node: ReactNode): string {
  if (isValidElement<{ children?: ReactNode }>(node)) return codeText(node.props.children)
  if (Array.isArray(node)) return node.map(codeText).join('')
  return typeof node === 'string' ? node : ''
}

export function CodeBlock({ children, 'data-title': title, ...props }: PreProps) {
  const shell = /^(sh|shell|bash|zsh|console|terminal|ps1?|powershell|fish)$/.test(props['data-lang'] ?? '')
  const code = isValidElement<ComponentProps<'code'>>(children) ? children : undefined
  const lines = code ? Children.toArray(code.props.children).filter(isValidElement<ComponentProps<'span'>>) : []
  const text = lines.map(codeText)
  // A leading "# " in shell source is a comment; in a console transcript it can be a root prompt.
  const prompt = /^(console|terminal)$/.test(props['data-lang'] ?? '') ? /^[#$>%] / : /^[$>%] /
  const commands = text.flatMap((line, i) => {
    if (!shell || !prompt.test(line)) return []
    const command = [line.slice(2)]
    for (
      let next = i + 1;
      command.at(-1)?.endsWith('\\') && next < text.length && !prompt.test(text[next]);
      next++
    )
      command.push(text[next])
    return [{ index: i, text: command.join('\n') }]
  })
  const content =
    code && commands.length
      ? cloneElement(
          code,
          {},
          lines.map((line, i) => {
            if (!prompt.test(text[i])) return line
            return cloneElement(
              line,
              {},
              <span className="shell-prompt">{text[i].slice(0, 2)}</span>,
              text[i].slice(2),
            )
          }),
        )
      : children
  return (
    <div className="code-frame">
      {title && <div className="code-title">{title}</div>}
      <pre {...props} tabIndex={0}>
        {content}
        {commands.length ? (
          commands.map((command) => (
            <CopyButton key={command.index} line={command.index} read={() => command.text} />
          ))
        ) : (
          <CopyButton read={() => text.join('\n')} />
        )}
      </pre>
    </div>
  )
}

function CodeGroup({ children }: { children: ReactNode }) {
  const blocks = Children.toArray(children).filter((child) => isValidElement<PreProps>(child))
  if (!blocks.length) return null
  return (
    <Tabs.Root className="code-group" defaultValue="0">
      <Tabs.List className="code-tabs" aria-label="Code group">
        {blocks.map((block, i) => (
          <Tabs.Trigger key={i} value={String(i)}>
            {block.props['data-title']}
          </Tabs.Trigger>
        ))}
      </Tabs.List>
      {blocks.map((block, i) => (
        <Tabs.Content key={i} forceMount value={String(i)} className="code-panel">
          {block}
        </Tabs.Content>
      ))}
    </Tabs.Root>
  )
}

function Release({ version, date, url, children }: ReleaseProps) {
  const body = useRef<HTMLDivElement>(null)
  const [long, setLong] = useState(false)
  const [expanded, setExpanded] = useState(false)
  useEffect(() => {
    const element = body.current!
    const observer = new ResizeObserver(() => setLong(element.scrollHeight > 600))
    observer.observe(element)
    return () => observer.disconnect()
  }, [])
  return (
    <section className="release" id={version}>
      <div className="release-label">
        <div>
          <h2>
            <SiteLink href={url}>
              {version}
              <ExternalLink size={12} />
            </SiteLink>
          </h2>
          <time dateTime={date}>
            {new Date(date).toLocaleDateString('en-US', {
              month: 'long',
              day: 'numeric',
              year: 'numeric',
              timeZone: 'UTC',
            })}
          </time>
        </div>
      </div>
      <div className="release-details">
        <div className="release-clip" data-expanded={expanded}>
          <div ref={body} className="release-body">
            {children}
          </div>
        </div>
        {long && !expanded && (
          <div className="release-expand">
            <button onClick={() => setExpanded(true)}>
              Show more <ChevronDown size={16} />
            </button>
          </div>
        )}
      </div>
    </section>
  )
}

export const markdownComponents = {
  'page-title': PageTitle,
  callout: Callout,
  'code-group': CodeGroup,
  steps: ({ children }: { children: ReactNode }) => <div className="steps">{children}</div>,
  release: Release,
  pre: CodeBlock,
  a: (props: ComponentProps<'a'>) => <SiteLink className="body-link" {...props} />,
  h2: heading('h2'),
  h3: heading('h3'),
  h4: heading('h4'),
  h5: heading('h5'),
  h6: heading('h6'),
  table: (props: ComponentProps<'table'>) => (
    <div className="table-scroll">
      <table {...props} />
    </div>
  ),
}
