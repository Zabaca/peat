// A Kubernetes API client, small on purpose: requests by path, as a pod reaches the API server (its service account) or as a person does
// (their kubeconfig: a token, a client certificate, or an exec credential plugin, as kubectl runs them). The operator uses the first; the
// `peat` command the second, so the cluster's own permissions decide what its user may do.
import { spawnSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

export interface Answer { status: number; json: any }
export interface Kube {
  request(method: string, path: string, body?: unknown, contentType?: string): Promise<Answer>
  /** The namespace the login points at (a pod's own, or the kubeconfig context's). */
  namespace: string
  /** Where it acts, for a person to read: the kubeconfig context and its server. */
  where?: string
}

const SA = '/var/run/secrets/kubernetes.io/serviceaccount'

interface Conn { server: string; ca?: string; cert?: string; key?: string; insecure?: boolean; token: () => string | undefined; namespace: string; where?: string }

function client(c: Conn): Kube {
  return {
    namespace: c.namespace,
    ...(c.where ? { where: c.where } : {}),
    async request(method, path, body, contentType = 'application/json') {
      const token = c.token()
      const r = await fetch(`${c.server}${path}`, {
        method,
        headers: { ...(token ? { authorization: `Bearer ${token}` } : {}), ...(body !== undefined ? { 'content-type': contentType } : {}), accept: 'application/json' },
        body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
        signal: AbortSignal.timeout(30_000),
        tls: { ...(c.ca ? { ca: c.ca } : {}), ...(c.cert ? { cert: c.cert, key: c.key } : {}), ...(c.insecure ? { rejectUnauthorized: false } : {}) },
      } as RequestInit)
      const text = await r.text()
      let json: any = {}
      try { json = text ? JSON.parse(text) : {} } catch { json = { message: text } }
      return { status: r.status, json }
    },
  }
}

/** The pod's own login: its service account's token (read at every request: the kubelet rotates it) and the cluster's authority. */
export function inCluster(): Kube {
  const host = process.env['KUBERNETES_SERVICE_HOST'] ?? 'kubernetes.default.svc'
  const port = process.env['KUBERNETES_SERVICE_PORT'] ?? '443'
  return client({
    server: `https://${host.includes(':') ? `[${host}]` : host}:${port}`,
    ca: readFileSync(`${SA}/ca.crt`, 'utf8'),
    token: () => readFileSync(`${SA}/token`, 'utf8').trim(),
    namespace: existsSync(`${SA}/namespace`) ? readFileSync(`${SA}/namespace`, 'utf8').trim() : 'default',
  })
}

const b64 = (s: string | undefined) => (s ? Buffer.from(s, 'base64').toString('utf8') : undefined)
const file = (p: string | undefined, base: string) => (p ? readFileSync(p.startsWith('/') ? p : join(base, p), 'utf8') : undefined)

/** The first kubeconfig file of KUBECONFIG (or ~/.kube/config), and the context to use. */
export function fromKubeconfig(opts: { path?: string; context?: string; namespace?: string } = {}): Kube {
  // a list of files joined by ':' is read as kubectl reads it: each name (context, cluster, user) from the first file that has it, the
  // current context from the first file that sets one; a file's relative paths are relative to that file
  const paths = (opts.path ?? (process.env['KUBECONFIG'] || join(homedir(), '.kube', 'config'))).split(':').filter(Boolean)
  const files = paths.filter((p) => existsSync(p)).map((p) => ({ cfg: (Bun.YAML.parse(readFileSync(p, 'utf8')) ?? {}) as any, base: p.replace(/\/[^/]*$/, '') || '.' }))
  if (!files.length) throw new Error(`no kubeconfig at ${paths.join(':')}: set KUBECONFIG, or log in to the cluster as kubectl would`)
  const named = (list: 'contexts' | 'clusters' | 'users', name: string): { value: any; base: string } | undefined => {
    for (const f of files) { const hit = (f.cfg[list] ?? []).find((x: any) => x.name === name); if (hit) return { value: hit[list.slice(0, -1)], base: f.base } }
    return undefined
  }
  const ctxName = opts.context ?? files.map((f) => f.cfg['current-context']).find(Boolean)
  const ctx = named('contexts', ctxName)?.value
  if (!ctx) throw new Error(`the kubeconfig has no context ${JSON.stringify(ctxName)}`)
  const clusterHit = named('clusters', ctx.cluster), userHit = named('users', ctx.user)
  const cluster = clusterHit?.value
  const user = userHit?.value ?? {}
  const base = userHit?.base ?? files[0]!.base, clusterBase = clusterHit?.base ?? files[0]!.base
  if (!cluster?.server) throw new Error(`the kubeconfig's context ${ctxName} names no cluster with a server`)
  let cached: { token: string; until: number } | undefined
  const token = (): string | undefined => {
    if (user.token) return user.token
    if (user.tokenFile) return file(user.tokenFile, base)!.trim()
    if (!user.exec) return undefined
    if (cached && cached.until > Date.now()) return cached.token
    // an exec credential plugin, as kubectl runs it: the command prints an ExecCredential whose status holds a token
    const e = user.exec
    const env = { ...process.env, ...Object.fromEntries((e.env ?? []).map((v: any) => [v.name, v.value])) }
    const run = spawnSync(e.command, e.args ?? [], { env, encoding: 'utf8', timeout: 60_000 })
    if (run.status !== 0) throw new Error(`the kubeconfig's credential command ${e.command} failed: ${(run.stderr || '').trim().slice(0, 300)}`)
    const status = JSON.parse(run.stdout).status ?? {}
    if (!status.token) throw new Error(`the kubeconfig's credential command ${e.command} gave no token`)
    cached = { token: status.token, until: status.expirationTimestamp ? Date.parse(status.expirationTimestamp) - 60_000 : Date.now() + 300_000 }
    return status.token
  }
  return client({
    server: String(cluster.server).replace(/\/$/, ''),
    ca: b64(cluster['certificate-authority-data']) ?? file(cluster['certificate-authority'], clusterBase),
    cert: b64(user['client-certificate-data']) ?? file(user['client-certificate'], base),
    key: b64(user['client-key-data']) ?? file(user['client-key'], base),
    insecure: cluster['insecure-skip-tls-verify'] === true,
    token,
    namespace: opts.namespace ?? ctx.namespace ?? 'default',
    where: `context ${ctxName} at ${String(cluster.server).replace(/\/$/, '')}`,
  })
}

/** An answer that is not a success becomes an error naming what was asked, unless its status is one of `also`. */
export function ok(r: Answer, what: string, also: number[] = []): Answer {
  if (r.status >= 300 && !also.includes(r.status)) throw new Error(`${what}: ${r.status} ${r.json?.message ?? JSON.stringify(r.json).slice(0, 300)}`)
  return r
}

/** The API path of an object or of its collection, from its apiVersion and kind (the plural is the kind's, lower-cased, as for every kind here). */
const PLURALS: Record<string, string> = { NetworkPolicy: 'networkpolicies', ValidatingAdmissionPolicy: 'validatingadmissionpolicies', ValidatingAdmissionPolicyBinding: 'validatingadmissionpolicybindings', Ingress: 'ingresses' }
export const pluralOf = (kind: string): string => PLURALS[kind] ?? `${kind.toLowerCase()}s`
export function pathOf(o: { apiVersion: string; kind: string; metadata: { name?: string; namespace?: string } }, collection = false): string {
  const root = o.apiVersion.includes('/') ? `/apis/${o.apiVersion}` : `/api/${o.apiVersion}`
  const ns = o.metadata.namespace ? `/namespaces/${o.metadata.namespace}` : ''
  return `${root}${ns}/${pluralOf(o.kind)}${collection ? '' : `/${o.metadata.name}`}`
}

/** Server-side apply: the object as the field manager wants it; fields another manager set are left to it, conflicts taken (`force`). */
export async function serverApply(k: Kube, o: { apiVersion: string; kind: string; metadata: { name: string; namespace?: string } }, manager: string): Promise<Answer> {
  return k.request('PATCH', `${pathOf(o)}?fieldManager=${encodeURIComponent(manager)}&force=true`, JSON.stringify(o), 'application/apply-patch+yaml')
}
