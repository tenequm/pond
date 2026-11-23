declare module 'virtual:pond-search' {
  const docs: import('../markdown/plugin.ts').SearchDoc[]
  export default docs
}

// URL path -> ISO time of the page's last commit, defined by static-files.ts.
declare const __PAGE_DATES__: Record<string, string>
