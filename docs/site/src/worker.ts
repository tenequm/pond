export default {
  async fetch(request: Request, env: { ASSETS: Fetcher; RYBBIT_HOST: string }): Promise<Response> {
    const url = new URL(request.url)
    if (url.hostname === 'pond.cascade.fyi') {
      url.hostname = 'pond.locker'
      return Response.redirect(url.toString(), 301)
    }
    // First-party analytics: the page loads /mesh/script.js, which then posts to
    // /mesh/track. Rybbit serves both under /api/ upstream.
    if (url.pathname.startsWith('/mesh/')) {
      const upstream = new Request(
        `${env.RYBBIT_HOST}/api${url.pathname.slice('/mesh'.length)}${url.search}`,
        request,
      )
      upstream.headers.set('X-Forwarded-For', request.headers.get('CF-Connecting-IP') ?? '')
      return fetch(upstream)
    }
    const response = await env.ASSETS.fetch(request)
    // Markdown twins of every page (vocs agent support) are emitted under
    // /assets/md/; the friendly /page.md URL needs this runtime rewrite.
    if (response.status === 404 && url.pathname.endsWith('.md')) {
      return env.ASSETS.fetch(new URL(`/assets/md${url.pathname}`, url).toString())
    }
    return response
  },
}
