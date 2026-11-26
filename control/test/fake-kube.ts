// An API server in memory, enough of one for the operator's tests: objects by path, lists by label (equality only), create, replace,
// server-side apply (as a whole-object replace that keeps status and the server's metadata), merge patch, status patch, delete.
import { check } from '../src/api/schema'
import { KINDS } from '../src/api/v1alpha1'
import { pluralOf, type Answer, type Kube } from '../src/kube'

const clone = <T>(v: T): T => structuredClone(v)
function merge(into: any, patch: any): any {
  if (patch === null || typeof patch !== 'object' || Array.isArray(patch)) return clone(patch)
  const out = into && typeof into === 'object' && !Array.isArray(into) ? { ...into } : {}
  for (const [k, v] of Object.entries(patch)) { if (v === null) delete out[k]; else out[k] = merge(out[k], v) }
  return out
}

/** `/api/v1/namespaces/a/pods/b` into its parts. */
function parse(path: string): { group: string; ns: string | null; plural: string; name: string | null; sub: string | null } {
  const p = path.replace(/^\/(api|apis)\//, '').split('/')
  const group = path.startsWith('/api/') ? p.splice(0, 1).join('/') : p.splice(0, 2).join('/')
  let ns: string | null = null
  if (p[0] === 'namespaces' && p.length >= 3) { ns = p[1]!; p.splice(0, 2) }
  else if (p[0] === 'namespaces' && p.length === 2) return { group, ns: null, plural: 'namespaces', name: p[1]!, sub: null }
  return { group, ns, plural: p[0]!, name: p[1] ?? null, sub: p[2] ?? null }
}

export class FakeKube implements Kube {
  namespace = 'peat-system'
  readonly objects = new Map<string, any>()
  readonly calls: string[] = []
  private rv = 1
  /** Answers to force, by "METHOD path" (for a refusal or an outage); `forcedOnce` answers once and is gone. */
  readonly forced = new Map<string, Answer>()
  readonly forcedOnce = new Map<string, Answer>()

  private keyOf(group: string, ns: string | null, plural: string, name: string) { return `${group}|${ns ?? ''}|${plural}|${name}` }
  put(o: any) {
    const plural = pluralOf(o.kind)
    const group = o.apiVersion
    const stored = { ...clone(o), metadata: { creationTimestamp: new Date(1_800_000_000_000 + this.rv * 1000).toISOString(), uid: `uid-${this.rv}`, generation: 1, ...clone(o.metadata), resourceVersion: String(this.rv++) } }
    this.objects.set(this.keyOf(group, o.metadata.namespace ?? null, plural, o.metadata.name), stored)
    return stored
  }
  get(apiVersion: string, plural: string, ns: string | null, name: string): any { return this.objects.get(this.keyOf(apiVersion, ns, plural, name)) }
  all(plural: string): any[] { return [...this.objects.entries()].filter(([k]) => k.split('|')[2] === plural).map(([, v]) => v) }

  async request(method: string, path: string, body?: unknown, contentType?: string): Promise<Answer> {
    this.calls.push(`${method} ${path}`)
    const forced = this.forced.get(`${method} ${path.split('?')[0]}`)
    if (forced) return clone(forced)
    const once = this.forcedOnce.get(`${method} ${path.split('?')[0]}`)
    if (once) { this.forcedOnce.delete(`${method} ${path.split('?')[0]}`); return clone(once) }
    const [bare, query] = path.split('?') as [string, string | undefined]
    const q = new URLSearchParams(query ?? '')
    const { group, ns, plural, name, sub } = parse(bare)
    const k = name ? this.keyOf(group, ns, plural, name) : ''
    // a body as the API server receives it: JSON (a field set to undefined is not sent at all)
    const parsed = body === undefined ? undefined : typeof body === 'string' ? JSON.parse(body) : JSON.parse(JSON.stringify(body))
    if (method === 'GET' && !name) {
      const sel = (q.get('labelSelector') ?? '').split(',').filter(Boolean).map((x) => x.split('=') as [string, string])
      const items = [...this.objects.entries()].filter(([key]) => { const [g, n, pl] = key.split('|'); return g === group && pl === plural && (ns === null || n === ns) }).map(([, v]) => v)
        .filter((o) => sel.every(([a, b]) => o.metadata?.labels?.[a] === b))
      return { status: 200, json: { items: clone(items) } }
    }
    const live = this.objects.get(k)
    if (method === 'GET') return live ? { status: 200, json: clone(live) } : { status: 404, json: { message: 'not found' } }
    if (method === 'POST') {
      const o = parsed as any
      const key = this.keyOf(group, ns, plural, o.metadata.name)
      if (this.objects.has(key)) return { status: 409, json: { message: 'exists' } }
      return { status: 201, json: clone(this.put({ ...o, ...(o.stringData ? { data: Object.fromEntries(Object.entries(o.stringData).map(([a, v]) => [a, Buffer.from(String(v)).toString('base64')])), stringData: undefined } : {}) })) }
    }
    if (method === 'PUT') {
      if (!live) return { status: 404, json: { message: 'not found' } }
      const o = parsed as any
      if (o.metadata?.resourceVersion && o.metadata.resourceVersion !== live.metadata.resourceVersion) return { status: 409, json: { message: 'conflict' } }
      return { status: 200, json: clone(this.put({ ...o, metadata: { ...live.metadata, ...o.metadata } })) }
    }
    if (method === 'PATCH' && sub === 'status') {
      if (!live) return { status: 404, json: { message: 'not found' } }
      const next = merge(live.status ?? {}, (parsed as any).status)
      // the API server keeps only the fields a kind's status schema names (it prunes the rest): here a field it would drop is an error, so a test sees it
      const kind = KINDS[live.kind as keyof typeof KINDS]
      if (kind) { const unknown = check(kind.status, next).filter((e) => e.endsWith('is not a field')); if (unknown.length) throw new Error(`the API server would drop these status fields of ${live.kind} ${live.metadata.name}: ${unknown.join(', ')}`) }
      live.status = next
      return { status: 200, json: clone(live) }
    }
    if (method === 'PATCH' && contentType === 'application/apply-patch+yaml') {
      const o = parsed as any
      const data = o.stringData ? { data: Object.fromEntries(Object.entries(o.stringData).map(([a, v]) => [a, Buffer.from(String(v)).toString('base64')])) } : {}
      const next = { ...o, ...data, stringData: undefined, metadata: { ...(live?.metadata ?? {}), ...o.metadata }, ...(live?.status ? { status: live.status } : {}) }
      if (live) next.metadata.generation = (live.metadata.generation ?? 1) + (JSON.stringify(live.spec) === JSON.stringify(o.spec) ? 0 : 1)
      return { status: live ? 200 : 201, json: clone(this.put(next)) }
    }
    if (method === 'PATCH' && contentType === 'application/json-patch+json') {
      if (!live) return { status: 404, json: { message: 'not found' } }
      const next = clone(live)
      for (const op of parsed as { op: string; path: string; value: unknown }[]) {
        const parts = op.path.split('/').slice(1)
        let at: any = next
        for (const part of parts.slice(0, -1)) at = at[part]
        if (op.op !== 'replace' && op.op !== 'add') return { status: 422, json: { message: `${op.op} not handled` } }
        at[parts.at(-1)!] = op.value
      }
      next.metadata.generation = (live.metadata.generation ?? 1) + (JSON.stringify(live.spec) === JSON.stringify(next.spec) ? 0 : 1)
      return { status: 200, json: clone(this.put(next)) }
    }
    if (method === 'PATCH') {
      if (!live) return { status: 404, json: { message: 'not found' } }
      const p = { ...(parsed as any) }
      if (p.stringData) { p.data = { ...(p.data ?? {}), ...Object.fromEntries(Object.entries(p.stringData).map(([a, v]) => [a, Buffer.from(String(v)).toString('base64')])) }; delete p.stringData }
      return { status: 200, json: clone(this.put(merge(live, p))) }
    }
    if (method === 'DELETE') {
      if (!live) return { status: 404, json: { message: 'not found' } }
      // a delete with a UID precondition removes only that very object
      const want = (parsed as any)?.preconditions?.uid
      if (want && want !== live.metadata.uid) return { status: 409, json: { message: 'the UID in the precondition does not match' } }
      this.objects.delete(k)
      return { status: 200, json: {} }
    }
    return { status: 405, json: { message: `${method} not handled` } }
  }
}
