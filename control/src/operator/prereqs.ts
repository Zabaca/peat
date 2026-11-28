// What a Platform needs from outside it before its pieces can start, checked by the operator so that a first install says what is missing
// and what to do, instead of waiting in Installing on pods that cannot start: the store's Secrets and their keys, the store's bucket (one
// request to the store, while the platform is not Ready), the records' Postgres Secret, and cert-manager and the issuer when
// certificates.issuer names one. And, for a piece that is not ready, why its pods are not: an image that cannot be pulled, a container
// that cannot be made or keeps failing, a pod that cannot be placed.
import { S3Client } from 'bun'
import { ok, type Kube } from '../kube'
import type { Resolved } from './platform'

export const STORE_KEYS = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'] as const

type SecretRead = { found: false } | { found: true; data: Record<string, string> }
async function readSecret(k: Kube, ns: string, name: string): Promise<SecretRead> {
  const s = ok(await k.request('GET', `/api/v1/namespaces/${ns}/secrets/${name}`), `read the Secret ${ns}/${name}`, [404])
  if (s.status === 404) return { found: false }
  return { found: true, data: Object.fromEntries(Object.entries((s.json.data ?? {}) as Record<string, string>).map(([key, v]) => [key, Buffer.from(v, 'base64').toString('utf8')])) }
}

/** The Secrets the store's pieces read (one, or one per piece, and the WAL trim's), each named once. */
export const storeSecrets = (r: Resolved): string[] => [...new Set([r.store.pageservers.credentialsSecret, r.store.safekeepers.credentialsSecret, r.store.records.credentialsSecret, r.store.walTrim])]

/**
 * What is missing of the Secrets the pieces read: each named with the command that makes it (the operator cannot: only the installer has
 * the store's credentials, and the records' URL). The store's credentials read, by Secret, for the bucket check.
 */
export async function secretProblems(k: Kube, r: Resolved): Promise<{ problems: string[]; creds: Map<string, { id: string; secret: string }> }> {
  const ns = r.names.namespace
  const problems: string[] = [], creds = new Map<string, { id: string; secret: string }>()
  const make = (name: string, keys: string) => `kubectl -n ${ns} create secret generic ${name} ${keys}`
  for (const name of storeSecrets(r)) {
    const s = await readSecret(k, ns, name)
    if (!s.found) { problems.push(`the store's Secret ${ns}/${name} is missing: make it with ${make(name, STORE_KEYS.map((key) => `--from-literal=${key}=<${key === 'AWS_ACCESS_KEY_ID' ? 'key id' : 'secret key'}>`).join(' '))}`); continue }
    const lacking = STORE_KEYS.filter((key) => !s.data[key])
    if (lacking.length) { problems.push(`the store's Secret ${ns}/${name} has no ${lacking.join(' and no ')}: add ${lacking.length > 1 ? 'them' : 'it'} (the store's access key id and secret key)`); continue }
    creds.set(name, { id: s.data['AWS_ACCESS_KEY_ID']!, secret: s.data['AWS_SECRET_ACCESS_KEY']! })
  }
  const pg = r.spec.controller.records === 'postgres' ? r.spec.controller.postgres : undefined
  if (pg) {
    const s = await readSecret(k, ns, pg.urlSecret)
    if (!s.found) problems.push(`controller.postgres: the Secret ${ns}/${pg.urlSecret} is missing: make it with ${make(pg.urlSecret, `--from-literal=${pg.key}=postgresql://<user>:<password>@<host>:5432/<database>`)}`)
    else if (!s.data[pg.key]) problems.push(`controller.postgres: the Secret ${ns}/${pg.urlSecret} has no ${pg.key} (the records' database URL)`)
  }
  return { problems, creds }
}

/** With certificates.issuer: cert-manager served, and the issuer there (an Issuer in the compute namespace, or a ClusterIssuer). */
export async function issuerProblems(k: Kube, r: Resolved): Promise<string[]> {
  const issuer = r.spec.certificates.issuer
  if (!issuer) return []
  const instead = 'or leave certificates.issuer out, and the platform\'s own authority signs the databases\' certificates'
  const served = await k.request('GET', '/apis/cert-manager.io/v1')
  if (served.status === 404) return [`certificates.issuer names ${issuer.kind}/${issuer.name}, but cert-manager is not installed (the cluster serves no cert-manager.io/v1): install cert-manager, ${instead}`]
  ok(served, 'ask whether the cluster serves cert-manager.io/v1')
  const path = issuer.kind === 'ClusterIssuer' ? `/apis/cert-manager.io/v1/clusterissuers/${issuer.name}` : `/apis/cert-manager.io/v1/namespaces/${r.names.computeNamespace}/issuers/${issuer.name}`
  const found = ok(await k.request('GET', path), `read ${issuer.kind} ${issuer.name}`, [404])
  if (found.status === 404) return [`certificates.issuer names ${issuer.kind}/${issuer.name}, which ${issuer.kind === 'ClusterIssuer' ? 'the cluster does not have' : `is not in ${r.names.computeNamespace}`}: make it, ${instead}`]
  return []
}

/** The store's answer to one request of the pieces' kind (a list of at most one key under the piece's folder). */
export type StoreProbe = (p: { endpoint: string; region: string; bucket: string; prefix: string; id: string; secret: string }) => Promise<{ ok: true } | { ok: false; refused: boolean; error: string }>

const PROBE_TIMEOUT_MS = 10_000
/** The store's own error text, the code and message it gave; never anything of the credentials. */
export const bunStoreProbe: StoreProbe = async (p) => {
  const client = new S3Client({ accessKeyId: p.id, secretAccessKey: p.secret, endpoint: p.endpoint, region: p.region, bucket: p.bucket })
  try {
    // a store that does not answer holds the pass no longer than this: it is said as not answered, never as a refusal
    let timer: ReturnType<typeof setTimeout> | undefined
    const late = new Promise<never>((_, no) => { timer = setTimeout(() => no(Object.assign(new Error(`no answer in ${PROBE_TIMEOUT_MS / 1000} s`), { code: 'Timeout' })), PROBE_TIMEOUT_MS) })
    try { await Promise.race([client.list({ prefix: p.prefix, maxKeys: 1 }), late]) } finally { clearTimeout(timer) }
    return { ok: true }
  } catch (e: any) {
    const code = String(e?.code ?? e?.name ?? '')
    // a bucket that is not there, or credentials the store refuses: a fault of the settings; anything else (the store not answering, a
    // 5xx) may pass, and is not a reason to fail the Platform
    const refused = /NoSuchBucket|InvalidAccessKeyId|SignatureDoesNotMatch|AccessDenied|InvalidBucketName|AuthorizationHeaderMalformed|PermanentRedirect/i.test(code)
    const text = `${code}${e?.message ? `: ${String(e.message)}` : ''}`.replaceAll(p.secret, '<secret key>').replaceAll(p.id, '<key id>').slice(0, 300)
    return { ok: false, refused, error: text }
  }
}

/** For each piece's bucket and Secret, one request to the store: a bucket not found or credentials refused, with the store's error. */
export async function storeProblems(r: Resolved, creds: Map<string, { id: string; secret: string }>, probe: StoreProbe): Promise<{ problems: string[]; unanswered: string[] }> {
  const problems: string[] = [], unanswered: string[] = []
  const seen = new Set<string>()
  for (const ref of [r.store.pageservers, r.store.safekeepers, r.store.records]) {
    const c = creds.get(ref.credentialsSecret)
    const once = `${ref.bucket}|${ref.credentialsSecret}`
    if (!c || seen.has(once)) continue
    seen.add(once)
    const a = await probe({ endpoint: r.store.endpoint, region: r.store.region, bucket: ref.bucket, prefix: ref.prefix, id: c.id, secret: c.secret })
    if (a.ok) continue
    const said = `the store at ${r.store.endpoint} answered for the bucket ${ref.bucket} (with the Secret ${r.names.namespace}/${ref.credentialsSecret}): ${a.error}`
    if (a.refused) problems.push(`${said}: make the bucket, or set objectStore's bucket, endpoint or Secret to the store's`)
    else unanswered.push(said)
  }
  return { problems, unanswered }
}

const STUCK = /^(ErrImagePull|ImagePullBackOff|InvalidImageName|ErrImageNeverPull|CreateContainerConfigError|CreateContainerError|CrashLoopBackOff|RunContainerError)$/
/** Why the pods of a workload that is not ready are not: the first stuck container's reason, image and message, or a pod not placed. */
export async function podReasons(k: Kube, ns: string, workload: any): Promise<string | undefined> {
  const labels = workload?.spec?.selector?.matchLabels as Record<string, string> | undefined
  if (!labels || !Object.keys(labels).length) return undefined
  const sel = Object.entries(labels).map(([a, b]) => `${encodeURIComponent(a)}%3D${encodeURIComponent(b)}`).join('%2C')
  const pods = await k.request('GET', `/api/v1/namespaces/${ns}/pods?labelSelector=${sel}`)
  if (pods.status !== 200) return undefined
  for (const pod of (pods.json.items ?? []) as any[]) {
    const statuses = [...(pod.status?.initContainerStatuses ?? []), ...(pod.status?.containerStatuses ?? [])] as any[]
    for (const c of statuses) {
      const w = c.state?.waiting
      if (w && STUCK.test(String(w.reason ?? ''))) {
        const what = /Image|Pull/.test(w.reason) ? `the image ${c.image} cannot be pulled` : w.reason === 'CrashLoopBackOff' ? `${c.name} keeps failing` : `${c.name} cannot be started`
        return `${what} (${w.reason}${w.message ? `: ${String(w.message).slice(0, 200)}` : ''})`
      }
    }
    const unplaced = (pod.status?.conditions ?? []).find((x: any) => x.type === 'PodScheduled' && x.status === 'False')
    if (unplaced) return `not placed (${unplaced.reason ?? 'Unschedulable'}${unplaced.message ? `: ${String(unplaced.message).slice(0, 200)}` : ''})`
  }
  return undefined
}
