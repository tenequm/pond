// The endpoints Rybbit's script requests; nothing else under /mesh/ is relayed to the analytics host.
const analyticsPath =
  /^\/mesh\/(script\.js|replay\.js|track|identify|site\/tracking-config\/[\w-]+|session-replay\/record\/[\w-]+)$/

const crawler =
  /(?<!cu)bot|crawl|spider|fetch|python|curl|\bnode\b|axios|go-http|wget|undici|chatgpt-user|anthropic-ai|claude-(?:user|code|web)|perplexity-user|mistralai-user|manus-user|meta-externalagent|googleother|google-(?:extended|agent)|cohere-ai|youchat|slurp|openai\/|cursor\//i

interface Env {
  ASSETS: { fetch: typeof fetch }
  RYBBIT_HOST: string
  RYBBIT_SITE_ID: string
  RYBBIT_API_KEY?: string
}

function shouldReport(path: string, response: Response, userAgent: string): boolean {
  if (response.status < 200 || response.status >= 400) return false
  if (path === '/llms.txt' || path === '/llms-full.txt') return true
  if (!crawler.test(userAgent)) return false
  if (path === '/robots.txt' || path === '/sitemap.xml') return true
  if (response.headers.get('Content-Type')?.startsWith('text/html')) return true
  // Redirects and conditional page reads can have no Content-Type.
  return response.status >= 300 && (!/\.[^/]*\/?$/.test(path) || /\.html\/?$/i.test(path))
}

export default {
  async fetch(request: Request, env: Env, ctx: { waitUntil(task: Promise<unknown>): void }): Promise<Response> {
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
    const response = url.pathname.startsWith('/mesh/') ? undefined : await env.ASSETS.fetch(request)
    if (!response || response.status === 404) {
      // Served from here because a _headers rule matches by path alone, and would
      // let a browser keep the 404 for a not-yet-deployed /assets file for a year.
      const notFound = await env.ASSETS.fetch(new URL('/404', url))
      return new Response(notFound.body, {
        status: 404,
        headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' },
      })
    }

    const userAgent = request.headers.get('User-Agent') ?? ''
    if (env.RYBBIT_API_KEY && request.method === 'GET' && shouldReport(url.pathname, response, userAgent)) {
      ctx.waitUntil(fetch(`${env.RYBBIT_HOST}/api/track`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${env.RYBBIT_API_KEY}`, 'Content-Type': 'application/json' },
        redirect: 'manual',
        signal: AbortSignal.timeout(2000),
        body: JSON.stringify({
          site_id: env.RYBBIT_SITE_ID,
          type: 'pageview',
          hostname: 'pond.locker',
          pathname: url.pathname.slice(0, 2048),
          querystring: url.search.slice(0, 2048),
          user_agent: userAgent.slice(0, 512),
          ip_address: request.headers.get('CF-Connecting-IP') || undefined,
          referrer: (request.headers.get('Referer') ?? '').slice(0, 2048),
        }),
      }).catch(() => {}))
    }
    return response
  },
}
