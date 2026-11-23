import type { ComponentProps, ReactNode } from 'react'

// The Markdown pipeline (markdown/plugin.ts) emits these custom tags; everything
// else in a page body is a plain HTML element.
export type PageTitleProps = { subtitle?: string; children: ReactNode }
export type CalloutProps = { type: 'info' | 'note' | 'tip' | 'warning' | 'danger'; title?: string; children: ReactNode }
export type CodeGroupProps = { children: ReactNode }
export type StepsProps = { children: ReactNode }
export type ReleaseProps = { version: string; date: string; url: string; children: ReactNode }
// Highlighted code arrives as <pre data-lang="sh" data-title="Homebrew"> holding
// one <code>; data-title is set only for a titled fence (```sh [Homebrew]).
export type PreProps = ComponentProps<'pre'> & { 'data-lang'?: string; 'data-title'?: string }

function PageTitle({ subtitle, children }: PageTitleProps) {
  return (
    <header>
      {children}
      {subtitle ? <p role="doc-subtitle">{subtitle}</p> : null}
    </header>
  )
}

function Callout({ type, title, children }: CalloutProps) {
  return (
    <aside data-callout={type}>
      {title ? <strong>{title}</strong> : null}
      {children}
    </aside>
  )
}

function Release({ version, date, url, children }: ReleaseProps) {
  return (
    <section>
      <h2 id={version}>
        <a href={url}>{version}</a>
      </h2>
      <time dateTime={date}>{date}</time>
      {children}
    </section>
  )
}

export const markdownComponents = {
  'page-title': PageTitle,
  callout: Callout,
  'code-group': ({ children }: CodeGroupProps) => <div data-code-group>{children}</div>,
  steps: ({ children }: StepsProps) => <div data-steps>{children}</div>,
  release: Release,
}
