const BASE = '/api'

async function request(method, path, body) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' },
  }
  if (body !== undefined) opts.body = JSON.stringify(body)
  const res = await fetch(`${BASE}${path}`, opts)
  if (res.status === 204 || res.headers.get('content-length') === '0') return null
  const ct = res.headers.get('content-type') ?? ''
  const data = ct.includes('application/json') ? await res.json() : await res.text()
  if (!res.ok) {
    const msg = typeof data === 'object'
      ? (data?.errors?.[0] ?? res.statusText)
      : (data || res.statusText)
    throw Object.assign(new Error(msg), { status: res.status, data })
  }
  return data
}

// ── Definitions ───────────────────────────────────────────────────────────────

export const api = {
  listDefinitions(limit = 50, offset = 0) {
    return request('GET', `/definitions?limit=${limit}&offset=${offset}`)
  },
  getDefinition(id) {
    return request('GET', `/definitions/${id}`)
  },
  createDefinition(body) {
    return request('POST', '/definitions', body)
  },
  updateDefinition(id, body) {
    return request('PUT', `/definitions/${id}`, body)
  },
  deleteDefinition(id) {
    return request('DELETE', `/definitions/${id}`)
  },
  runDefinition(name, version, payload = {}) {
    return request('POST', `/definitions/${name}/${version}/run`, { payload })
  },

  // ── Executions ──────────────────────────────────────────────────────────────

  listExecutions(params = {}) {
    const q = new URLSearchParams()
    if (params.status) q.set('status', params.status)
    if (params.name) q.set('name', params.name)
    if (params.limit) q.set('limit', params.limit)
    if (params.offset) q.set('offset', params.offset)
    return request('GET', `/executions?${q}`)
  },
  getExecution(id) {
    return request('GET', `/executions/${id}`)
  },
  getExecutionDetail(id) {
    return request('GET', `/executions/${id}/detail`)
  },
  getExecutionSteps(id) {
    return request('GET', `/executions/${id}/steps`)
  },
  retryExecution(id) {
    return request('POST', `/executions/${id}/retry`)
  },
}
