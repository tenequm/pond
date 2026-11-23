import {
  Check,
  ChevronDown,
  CircleAlert,
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
  isValidElement,
  type ReactNode,
  useEffect,
  useRef,
  useState,
} from 'react'
import { CopyButton } from './copy'
import { SiteLink } from './navigation'

export type PageTitleProps = { subtitle?: string; children: ReactNode }
export type CalloutProps = {
  type: 'info' | 'note' | 'tip' | 'warning' | 'danger'
  title?: string
  children: ReactNode
}
export type CodeGroupProps = { children: ReactNode }
export type StepsProps = { children: ReactNode }
export type ReleaseProps = { version: string; date: string; url: string; children: ReactNode }
export type PreProps = ComponentProps<'pre'> & { 'data-lang'?: string; 'data-title'?: string }

function PageTitle({ subtitle, children }: PageTitleProps) {
  // The router may try to scroll before the lazy article has mounted its headings.
  useEffect(() => {
    const hash = location.hash.slice(1)
    if (!hash) return
    try {
      document.getElementById(decodeURIComponent(hash))?.scrollIntoView()
    } catch {
      /* A malformed fragment cannot identify a heading. */
    }
  }, [])
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
              void navigator.clipboard
                .writeText(event.currentTarget.href)
                .then(() => setCopied(true))
                .catch(() => {})
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
  const frame = useRef<HTMLPreElement>(null)
  const shell = /^(sh|shell|bash|zsh|console|terminal|ps1?|powershell|fish)$/.test(props['data-lang'] ?? '')
  const code = isValidElement<ComponentProps<'code'>>(children) ? children : undefined
  const lines =
    code && shell ? Children.toArray(code.props.children).filter(isValidElement<ComponentProps<'span'>>) : []
  const text = lines.map(codeText)
  const prompt = /^([#$>%] )/
  const commands = text.flatMap((line, i) => {
    if (!prompt.test(line)) return []
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
      <pre {...props} ref={frame} tabIndex={0}>
        {content}
        {commands.length ? (
          commands.map((command) => (
            <CopyButton key={command.index} line={command.index} read={() => command.text} />
          ))
        ) : (
          <CopyButton
            read={() => {
              const element = frame.current!.querySelector('code')!
              const rows = Array.from(element.querySelectorAll(':scope > .line'), (line) => line.textContent)
              return rows.length ? rows.join('\n') : element.textContent
            }}
          />
        )}
      </pre>
    </div>
  )
}

function CodeGroup({ children }: CodeGroupProps) {
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
      <div className="grid">
        {blocks.map((block, i) => (
          <Tabs.Content key={i} forceMount value={String(i)} className="code-panel">
            {block}
          </Tabs.Content>
        ))}
      </div>
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
          <SiteLink href={url}>
            {version}
            <ExternalLink size={12} />
          </SiteLink>
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
  steps: ({ children }: StepsProps) => <div className="steps">{children}</div>,
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
