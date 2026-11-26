// One pass of the operator over the cluster: the Platform read and checked, the machines chosen (once, then kept), the keys made when the
// install gives none (once, then kept), every piece applied as rendered, what the operator made and no longer renders removed, and the
// Platform's status written. Run every few seconds by main.ts; every pass is whole, so a pass that stops half way is finished by the next.
//
// What it never does: change an object it did not make (one already there, of the same name, is reported and left alone; the Platform's
// `adopt` list is how it takes one over; a namespace the Platform names is the one exception, as a person makes it first to hold the
// store's Secret), delete an object it did not make, delete a namespace, or copy the Platform's labels onto anything.
import { API_VERSION, GROUP, type Platform } from '../api/v1alpha1'
import { ok, pathOf, pluralOf, serverApply, type Kube } from '../kube'
import { keyProblems, makeMissing, AUTH_PRIVATE, AUTH_PUBLIC, CA_CERT, CA_KEY } from './keys'
import { choosePlacements, imagesOf, resolve, specOf, type Images, type Placements, type Resolved } from './platform'
import { storageOf, type Storage } from './storage'
import { decide, imagesFor, stepsOf, type UpgradeState } from './upgrade'
import { runStep } from './upgrade-run'
import { KEYS_SECRET, MANAGED_BY, OPERATOR, PLATFORM_LABEL, render, type Manifest } from './render'

export const FIELD_MANAGER = OPERATOR
/** The operator's record of where each pinned piece runs, in the platform namespace. */
export const PLACEMENTS_CONFIG_MAP = 'peat-placements'
/** Every kind the renderer makes, so a prune lists each. */
export const RENDERED_KINDS: { apiVersion: string; kind: string; namespaced: boolean }[] = [
  { apiVersion: 'v1', kind: 'Namespace', namespaced: false },
  { apiVersion: 'v1', kind: 'ConfigMap', namespaced: true },
  { apiVersion: 'v1', kind: 'Secret', namespaced: true },
  { apiVersion: 'v1', kind: 'Service', namespaced: true },
  { apiVersion: 'v1', kind: 'ServiceAccount', namespaced: true },
  { apiVersion: 'apps/v1', kind: 'Deployment', namespaced: true },
  { apiVersion: 'apps/v1', kind: 'StatefulSet', namespaced: true },
  { apiVersion: 'batch/v1', kind: 'Job', namespaced: true },
  { apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy', namespaced: true },
  { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'Role', namespaced: true },
  { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'RoleBinding', namespaced: true },
  { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRole', namespaced: false },
  { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRoleBinding', namespaced: false },
  { apiVersion: 'admissionregistration.k8s.io/v1', kind: 'ValidatingAdmissionPolicy', namespaced: false },
  { apiVersion: 'admissionregistration.k8s.io/v1', kind: 'ValidatingAdmissionPolicyBinding', namespaced: false },
]

const platformsPath = `/apis/${API_VERSION}/platforms`
const key = (o: { kind: string; metadata: { namespace?: string; name: string } }) => `${o.kind}/${o.metadata.namespace ?? ''}/${o.metadata.name}`
const sel = (labels: Record<string, string>) => encodeURIComponent(Object.entries(labels).map(([k, v]) => `${k}=${v}`).join(','))
const madeHere = (o: any, platform: string) => o?.metadata?.labels?.[MANAGED_BY] === OPERATOR && o?.metadata?.labels?.[PLATFORM_LABEL] === platform

export interface PassResult { platform?: string; phase: string; message: string; applied: number; pruned: number; refused: string[] }

/** The upgrade's optional fields: in a merge patch a field left out is kept, so a cleared one is written as null (which removes it). */
const UPGRADE_CLEARABLE = ['step', 'phase', 'since', 'halted', 'lastCheck', 'retried', 'complete'] as const
async function writeStatus(k: Kube, p: Platform, status: Record<string, unknown>): Promise<void> {
  const u = status['upgrade'] as Record<string, unknown> | undefined
  const upgrade = u ? { ...Object.fromEntries(UPGRADE_CLEARABLE.map((f) => [f, null])), ...Object.fromEntries(Object.entries(u).filter(([, v]) => v !== undefined)) } : undefined
  await k.request('PATCH', `${platformsPath}/${p.metadata.name}/status`, { status: { observedGeneration: p.metadata.generation, ...status, ...(upgrade ? { upgrade } : {}) } }, 'application/merge-patch+json')
}

/** The machines that carry the platform's label. */
async function labelledMachines(k: Kube, r: { key: string; value: string }): Promise<{ names: string[]; nodes: any[] }> {
  const nodes = ok(await k.request('GET', `/api/v1/nodes?labelSelector=${sel({ [r.key]: r.value })}`), 'list the machines').json.items as any[]
  return { names: nodes.map((n) => n.metadata.name as string), nodes }
}

async function readPlacements(k: Kube, ns: string): Promise<Partial<Placements>> {
  const r = await k.request('GET', `/api/v1/namespaces/${ns}/configmaps/${PLACEMENTS_CONFIG_MAP}`)
  if (r.status !== 200) return {}
  try { return JSON.parse(r.json.data?.['placements.json'] ?? '{}') } catch { return {} }
}

/** The keys as kept, with any the install gives in place of the operator's own. */
async function readKeys(k: Kube, r: Resolved): Promise<Record<string, string>> {
  const n = r.names
  const secretData = async (ns: string, name: string, what: string): Promise<Record<string, string> | null> => {
    const s = await k.request('GET', `/api/v1/namespaces/${ns}/secrets/${name}`)
    if (s.status === 404) return null
    ok(s, what)
    return Object.fromEntries(Object.entries((s.json.data ?? {}) as Record<string, string>).map(([key, v]) => [key, Buffer.from(v, 'base64').toString('utf8')]))
  }
  const kept = (await secretData(n.namespace, KEYS_SECRET, 'read the keys')) ?? {}
  if (r.spec.keys.authSecret) {
    const given = await secretData(n.namespace, r.spec.keys.authSecret, `read ${r.spec.keys.authSecret}`)
    if (!given?.[AUTH_PRIVATE]) throw new Error(`keys.authSecret: ${n.namespace}/${r.spec.keys.authSecret} holds no ${AUTH_PRIVATE}`)
    kept[AUTH_PRIVATE] = given[AUTH_PRIVATE]
    if (given[AUTH_PUBLIC]) kept[AUTH_PUBLIC] = given[AUTH_PUBLIC]
  }
  if (r.spec.keys.caSecret) {
    const given = await secretData(n.computeNamespace, r.spec.keys.caSecret, `read ${r.spec.keys.caSecret}`)
    if (!given?.['tls.crt'] || !given?.['tls.key']) throw new Error(`keys.caSecret: ${n.computeNamespace}/${r.spec.keys.caSecret} holds no tls.crt and tls.key`)
    kept[CA_CERT] = given['tls.crt']
    kept[CA_KEY] = given['tls.key']
  }
  return kept
}

async function writeKeys(k: Kube, r: Resolved, kept: Record<string, string>): Promise<void> {
  const body = { apiVersion: 'v1', kind: 'Secret', metadata: { name: KEYS_SECRET, namespace: r.names.namespace, labels: { [MANAGED_BY]: OPERATOR } }, stringData: kept }
  const made = await k.request('POST', `/api/v1/namespaces/${r.names.namespace}/secrets`, body)
  if (made.status === 409) ok(await k.request('PATCH', `/api/v1/namespaces/${r.names.namespace}/secrets/${KEYS_SECRET}`, { stringData: kept }, 'application/merge-patch+json'), 'keep the keys')
  else ok(made, 'make the keys')
}

/** The pieces and whether each is ready, for the status: the Deployments and StatefulSets the operator made, and the safekeepers the control layer made. */
async function pieces(k: Kube, r: Resolved, rendered: Manifest[]): Promise<{ kind: string; namespace: string; name: string; release: string; ready: boolean; message?: string }[]> {
  const out: { kind: string; namespace: string; name: string; release: string; ready: boolean; message?: string }[] = []
  const releaseOf = (image: string) => image.replace(/^.*[:@]/, '')
  const judge = (o: any) => {
    const want = o.spec?.replicas ?? 1
    const ready = (o.status?.readyReplicas ?? 0) >= want && (o.status?.updatedReplicas ?? o.status?.currentReplicas ?? want) >= want
    return { ready, message: ready ? undefined : `${o.status?.readyReplicas ?? 0} of ${want} ready` }
  }
  for (const m of rendered.filter((x) => x.kind === 'Deployment' || x.kind === 'StatefulSet')) {
    const live = await k.request('GET', pathOf(m))
    const image = String(m.spec.template.spec.containers[0].image)
    if (live.status !== 200) { out.push({ kind: m.kind, namespace: m.metadata.namespace!, name: m.metadata.name, release: releaseOf(image), ready: false, message: 'not made yet' }); continue }
    out.push({ kind: m.kind, namespace: m.metadata.namespace!, name: m.metadata.name, release: releaseOf(String(live.json.spec.template.spec.containers[0].image)), ...judge(live.json) })
  }
  const sks = await k.request('GET', `/apis/apps/v1/namespaces/${r.names.namespace}/statefulsets?labelSelector=${sel({ 'peat-sk': 'true' })}`)
  for (const s of (sks.status === 200 ? sks.json.items : []) as any[]) out.push({ kind: 'StatefulSet', namespace: r.names.namespace, name: s.metadata.name, release: releaseOf(String(s.spec.template.spec.containers[0].image)), ...judge(s) })
  if (sks.status === 200 && (sks.json.items as any[]).length < r.spec.safekeepers.count) out.push({ kind: 'StatefulSet', namespace: r.names.namespace, name: 'safekeepers', release: releaseOf(r.images.storage), ready: false, message: `${(sks.json.items as any[]).length} of ${r.spec.safekeepers.count} safekeepers made (the control layer makes them once it runs)` })
  return out
}

export const CA_CONFIG_MAP = 'peat-ca'
/**
 * What a client of each database needs, in the database's own namespace: the authority's public certificate (a ConfigMap `peat-ca`,
 * ca.crt, for sslmode=verify-full) in every namespace that holds a Database, and each made database's login (a Secret `<name>-login`:
 * username, password, the names to connect to and a URL). A branch and a replica use their root's login. The control layer keeps the
 * passwords in the platform namespace; these are copies, made and removed by the operator.
 */
async function published(k: Kube, r: Resolved, keys: Record<string, string>): Promise<{ manifests: Manifest[]; dbs: any[] }> {
  const list = await k.request('GET', `/apis/${API_VERSION}/databases`)
  if (list.status !== 200) return { manifests: [], dbs: [] }
  const dbs = list.json.items as any[]
  const meta = (name: string, namespace: string) => ({ name, namespace, labels: { [MANAGED_BY]: OPERATOR, [PLATFORM_LABEL]: r.name } })
  const out: Manifest[] = []
  for (const ns of [...new Set(dbs.map((d) => d.metadata.namespace as string))].sort()) out.push({ apiVersion: 'v1', kind: 'ConfigMap', metadata: meta(CA_CONFIG_MAP, ns), data: { 'ca.crt': keys['ca.crt'] ?? '' } })
  for (const d of dbs) {
    const rec = d.status?.record
    if (!d.status?.tenant || !rec || d.metadata.deletionTimestamp) continue
    const root = rec.root ? d.metadata.name : rec.parent
    const login = await k.request('GET', `/api/v1/namespaces/${r.names.namespace}/secrets/db-${root}-login`)
    if (login.status !== 200) continue
    const password = Buffer.from(String(login.json.data?.password ?? ''), 'base64').toString()
    const serviceHost = `${d.metadata.name}.${r.names.dbNamespace}.svc`
    const host = r.spec.gateways.domain ? `${d.metadata.name}.${r.spec.gateways.domain}` : serviceHost
    out.push({ apiVersion: 'v1', kind: 'Secret', metadata: meta(`${d.metadata.name}-login`, d.metadata.namespace), type: 'Opaque', stringData: {
      username: 'app', password, dbname: 'app', host, serviceHost, port: '5432',
      url: `postgresql://app:${encodeURIComponent(password)}@${host}:5432/app?sslmode=verify-full`,
    } })
  }
  return { manifests: out, dbs }
}

/** The mark on an object the operator took over from an install made another way. */
export const ADOPTED = `${GROUP}/adopted`
/**
 * How `live` differs from what the operator would make (`want`), as the paths of every field the operator sets whose value is not the live
 * one; fields the operator does not set (defaults the API server filled in, another tool's fields) are not differences. The operator's own
 * labels are not either: taking an object over is what adds them.
 */
export function differences(want: unknown, live: unknown, path = ''): string[] {
  if (path === '.metadata.labels' || path === '.metadata.annotations' || path === '.stringData') return []
  if (want === null || typeof want !== 'object') return JSON.stringify(want) === JSON.stringify(live) ? [] : [`${path || '.'}: ${JSON.stringify(live) ?? 'none'} would be ${JSON.stringify(want)}`.slice(0, 240)]
  if (Array.isArray(want)) {
    if (!Array.isArray(live) || live.length !== want.length) return [`${path}: ${Array.isArray(live) ? `${live.length} items` : JSON.stringify(live) ?? 'none'} would be ${want.length} items`]
    return want.flatMap((w, i) => differences(w, live[i], `${path}[${i}]`))
  }
  if (!live || typeof live !== 'object' || Array.isArray(live)) return [`${path}: none would be set`]
  return Object.entries(want as Record<string, unknown>).flatMap(([k2, v]) => differences(v, (live as Record<string, unknown>)[k2], `${path}.${k2}`))
}

/** One pass. Returns what it did, for the log and the tests. */
export interface Deps { storage?: (r: Resolved, keys: Record<string, string>) => Storage; now?: () => Date }

export async function reconcile(k: Kube, deps: Deps = {}): Promise<PassResult> {
  const now = (deps.now ?? (() => new Date()))()
  const all = (ok(await k.request('GET', platformsPath), 'list the Platforms').json.items ?? []) as Platform[]
  if (all.length === 0) return { phase: 'none', message: 'no Platform object: nothing to do', applied: 0, pruned: 0, refused: [] }
  const ordered = [...all].sort((a, b) => String(a.metadata.creationTimestamp).localeCompare(String(b.metadata.creationTimestamp)) || a.metadata.name.localeCompare(b.metadata.name))
  const p = ordered[0]!
  for (const other of ordered.slice(1)) await writeStatus(k, other, { phase: 'Failed', message: `a cluster holds one platform: ${p.metadata.name} is the one acted on` })
  const done = async (phase: string, message: string, more: Record<string, unknown> = {}, counts = { applied: 0, pruned: 0, refused: [] as string[] }): Promise<PassResult> => {
    await writeStatus(k, p, { phase, message, ...more })
    return { platform: p.metadata.name, phase, message, ...counts }
  }
  if (p.spec?.suspend) return done('Suspended', 'suspend is true: the operator changes nothing')
  const checked = specOf(p)
  if ('errors' in checked) return done('Failed', `the Platform is not valid: ${checked.errors.join('; ')}`)
  const spec = checked.spec
  const machines = await labelledMachines(k, spec.machines.label)
  const kept = await readPlacements(k, spec.namespaces.platform)
  const chosen = choosePlacements(spec, machines.names, kept)
  if ('errors' in chosen) return done('Failed', chosen.errors.join('; '))
  const r = resolve(p, spec, chosen.placements)

  // the namespaces first: the keys and the placements are kept in the platform namespace
  const applied: string[] = []
  const refused: string[] = []
  const adoptList = new Set((spec.adopt ?? []).map((a) => `${a.kind}/${a.namespace ?? ''}/${a.name}`))
  const pending: { kind: string; namespace?: string; name: string; change: string }[] = []
  const applyOne = async (m: Manifest) => {
    const live = await k.request('GET', pathOf(m))
    // an object listed in adopt is taken over only when what the operator would make changes nothing of it; else the difference is
    // reported as a pending change and nothing is applied, until the object is taken out of the list (then it is applied as rendered)
    if (live.status === 200 && adoptList.has(key(m))) {
      const diff = differences(m, live.json)
      if (diff.length) {
        pending.push({ kind: m.kind, ...(m.metadata.namespace ? { namespace: m.metadata.namespace } : {}), name: m.metadata.name, change: diff.slice(0, 8).join('; ') + (diff.length > 8 ? `; and ${diff.length - 8} more` : '') })
        if (!madeHere(live.json, r.name)) ok(await k.request('PATCH', pathOf(m), { metadata: { labels: { [MANAGED_BY]: OPERATOR, [PLATFORM_LABEL]: r.name, [ADOPTED]: 'true' } } }, 'application/merge-patch+json'), `adopt ${key(m)}`)
        return
      }
      ok(await serverApply(k, { ...m, metadata: { ...m.metadata, labels: { ...m.metadata.labels, [ADOPTED]: 'true' } } } as Manifest, FIELD_MANAGER), `adopt ${key(m)}`)
      applied.push(key(m))
      return
    }
    // a namespace the Platform names is the platform's, made by the operator or beforehand by a person (to hold the store's Secret): its labels are applied either way
    if (live.status === 200 && !madeHere(live.json, r.name) && m.kind !== 'Namespace') {
      refused.push(`${key(m)} exists and was not made by the operator: left alone (list it in adopt to take it over)`)
      return
    }
    ok(await serverApply(k, m, FIELD_MANAGER), `apply ${key(m)}`)
    applied.push(key(m))
  }
  const rendered0 = render(r, {})
  for (const m of rendered0.filter((x) => x.kind === 'Namespace')) await applyOne(m)

  let keys = await readKeys(k, r)
  const missing = makeMissing(keys)
  if (Object.keys(missing).length) {
    keys = { ...keys, ...missing }
    await writeKeys(k, r, keys)
  }
  const problems = keyProblems(keys)
  if (problems.length) return done('Failed', `the keys: ${problems.join('; ')}`)
  if (JSON.stringify(kept) !== JSON.stringify(r.placements)) {
    const cm = { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: PLACEMENTS_CONFIG_MAP, namespace: r.names.namespace, labels: { [MANAGED_BY]: OPERATOR } }, data: { 'placements.json': JSON.stringify(r.placements) } }
    ok(await serverApply(k, cm, FIELD_MANAGER), 'keep the placements')
  }

  // the upgrade: decided before anything is applied, so each piece is rendered at the release its step has reached
  const status = (p.status ?? {}) as any
  const running = status.release ? { release: String(status.release), images: (status.images as Images | undefined) ?? imagesOf(spec, String(status.release)) } : undefined
  const sks = await k.request('GET', `/apis/apps/v1/namespaces/${r.names.namespace}/statefulsets?labelSelector=${sel({ 'peat-sk': 'true' })}`)
  const skNames = sks.status === 200 ? (sks.json.items as any[]).map((x) => String(x.metadata.name)) : []
  const decision = decide({ release: spec.release, images: r.images, running, state: status.upgrade as UpgradeState | undefined, steps: stepsOf(r.placements.pageservers.map((x) => x.id), skNames), retry: spec.requests?.retry, quietHours: spec.upgrades.quietHours, now })
  let upgrade: UpgradeState | undefined = decision.kind === 'none' ? undefined : decision.state
  let stepMessage = ''
  if (decision.kind === 'continue') {
    const before = await pieces(k, r, render(r, keys, imagesFor(upgrade, r.images)))
    const storage = (deps.storage ?? ((res, kk) => storageOf(res, kk['auth_private_key.pem']!)))(r, keys)
    const work = await runStep(k, r, storage, upgrade!, now, before.filter((x) => !x.ready).map((x) => x.name))
    upgrade = work.state
    stepMessage = work.message
  }
  const live = upgrade && !upgrade.complete ? upgrade : undefined
  const at = imagesFor(live, r.images)
  const pub = await published(k, r, keys)
  const rendered = [...render(r, keys, at, live ? [live.fromImages.storage, live.toImages.storage] : [r.images.storage]), ...pub.manifests]
  for (const m of rendered.filter((x) => x.kind !== 'Namespace')) await applyOne(m)
  for (const d of pub.dbs) {
    if (!d.status?.tenant || d.metadata.deletionTimestamp || (d.status.loginSecret && d.status.caConfigMap)) continue
    await k.request('PATCH', `/apis/${API_VERSION}/namespaces/${d.metadata.namespace}/databases/${d.metadata.name}/status`, { status: { loginSecret: `${d.metadata.name}-login`, caConfigMap: CA_CONFIG_MAP } }, 'application/merge-patch+json')
  }

  // what the operator made for this platform and no longer renders: removed (the namespaces never are: what is in them may not be its own)
  const wanted = new Set(rendered.map(key))
  let pruned = 0
  for (const kind of RENDERED_KINDS.filter((x) => x.kind !== 'Namespace')) {
    const root = kind.apiVersion.includes('/') ? `/apis/${kind.apiVersion}` : `/api/${kind.apiVersion}`
    const list = await k.request('GET', `${root}/${pluralOf(kind.kind)}?labelSelector=${sel({ [MANAGED_BY]: OPERATOR, [PLATFORM_LABEL]: r.name })}`)
    if (list.status !== 200) continue
    for (const o of list.json.items as any[]) {
      const m = { apiVersion: kind.apiVersion, kind: kind.kind, metadata: { name: o.metadata.name, ...(kind.namespaced ? { namespace: o.metadata.namespace } : {}) } }
      if (wanted.has(key(m)) || !madeHere(o, r.name)) continue
      ok(await k.request('DELETE', pathOf(m)), `remove ${key(m)}`, [404])
      pruned++
    }
  }

  const ps = await pieces(k, r, rendered)
  const notReady = ps.filter((x) => !x.ready)
  const counts = { applied: applied.length, pruned, refused }
  const more = { pieces: ps, survives: r.survives, pendingChanges: pending, ...(upgrade ? { upgrade } : {}) }
  if (live?.halted) return done('Halted', `the upgrade to ${live.direction === 'forward' ? live.to : live.from} halted at ${live.halted.step}: ${live.halted.reason}. peat retry goes on from there; setting ${live.direction === 'forward' ? live.from : live.to} takes back what moved`, more, counts)
  if (decision.kind === 'pause') return done('Paused', `the upgrade to ${live!.direction === 'forward' ? live!.to : live!.from} waits for the quiet hours (${decision.window}) to end: ${live!.moved.length} of ${live!.steps.length} steps moved`, more, counts)
  if (live) return done('Upgrading', `${live.direction === 'forward' ? `${live.from} to ${live.to}` : `back from ${live.to} to ${live.from}`}: ${stepMessage}`, more, counts)
  if (decision.kind === 'finish') return done(notReady.length ? 'Installing' : 'Ready', `the upgrade ${decision.state.direction === 'forward' ? 'to' : 'back to'} ${decision.release} is done: every piece runs it`, { ...more, release: decision.release, images: decision.images }, counts)
  const phase = notReady.length || refused.length ? 'Installing' : 'Ready'
  const message = refused.length ? refused[0]! : notReady.length ? `waiting for ${notReady.map((x) => `${x.name}${x.message ? ` (${x.message})` : ''}`).join(', ')}` : `every piece is ready on ${spec.release}`
  // the release every piece runs, recorded once all are ready on it: what the next upgrade starts from
  return done(phase, message, { ...more, ...(phase === 'Ready' && !running ? { release: spec.release, images: r.images } : {}) }, counts)
}

export { GROUP }
