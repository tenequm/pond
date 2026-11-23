import { Check, Clipboard } from 'lucide-react'
import { type CSSProperties, useEffect, useRef, useState } from 'react'

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
          ? 'Copy page'
          : line === undefined
            ? 'Copy code'
            : 'Copy command'
  async function copy() {
    try {
      // Start the clipboard write inside the click gesture even when Markdown needs fetching.
      const value = read()
      if (typeof value === 'string') await navigator.clipboard.writeText(value)
      else
        await navigator.clipboard.write([
          new ClipboardItem({
            'text/plain': value.then((text) => new Blob([text], { type: 'text/plain' })),
          }),
        ])
      setState('copied')
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
      aria-label={label}
      title={label}
      data-state={state}
    >
      <Icon size={line === undefined ? 16 : 14} />
      {page && <span>{label}</span>}
    </button>
  )
}
