// The endpoints Rybbit's script requests; nothing else under /mesh/ is relayed to the analytics host.
const analyticsPath =
  /^\/mesh\/(script\.js|replay\.js|track|identify|site\/tracking-config\/[\w-]+|session-replay\/record\/[\w-]+)$/

export default {
  async fetch(request: Request, env: { ASSETS: { fetch: typeof fetch }; RYBBIT_HOST: string }): Promise<Response> {
    const url = new URL(request.url)
    // First-party analytics: the page loads /mesh/script.js, which then posts to
    // /mesh/track. Rybbit serves both under /api/ upstream.
    if (analyticsPath.test(url.pathname)) {
      const upstream = new Request(
        `${env.RYBBIT_HOST}/api${url.pathname.slice('/mesh'.length)}${url.search}`,
        request,
      )
      // X-Real-IP is the header Rybbit's proxy guide prefers; without the
      // visitor IP, events from a datacenter egress get flagged as bot ASN.
      const visitorIp = request.headers.get('CF-Connecting-IP') ?? ''
      upstream.headers.set('X-Real-IP', visitorIp)
      upstream.headers.set('X-Forwarded-For', visitorIp)
      upstream.headers.delete('Cookie')
      return fetch(upstream)
    }
    return env.ASSETS.fetch(request)
  },
}
