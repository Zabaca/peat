// One pass of the operator over the cluster: the Platform read and checked, the machines chosen (once, then kept), the keys made when the
// install gives none (once, then kept), every piece applied as rendered, what the operator made and no longer renders removed, and the
// Platform's status written. Run every few seconds by main.ts; a pass that stops half way is finished by the next (what it made is in the
// record of what the operator made, owned.ts, from the moment it is made).
//
// What it never does: change an object it did not make (one already there, of the same name, is reported and left alone; a namespace the
// Platform names is the one exception, as a person makes it first to hold the store's Secret), delete an object it did not make, delete a
// namespace, or copy the Platform's labels onto anything.
import { API_VERSION, GROUP, type Platform } from '../api/v1alpha1'
import { ok, pathOf, serverApply, type Kube } from '../kube'
import { NONCE_ANNOTATION, Owned } from './owned'
import { keyProblems, makeMissing, AUTH_PRIVATE, AUTH_PUBLIC, CA_CERT, CA_KEY } from './keys'
import { choosePlacements, resolve, specOf, type Placements, type Resolved } from './platform'
import { KEYS_SECRET, MANAGED_BY, OPERATOR, PLATFORM_LABEL, RECORDS_GROUP, RELEASE_ANNOTATION, render, type Manifest } from './render'

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
const kindOf = (key: string) => RENDERED_KINDS.find((x) => x.kind === key.split('/')[0])

export interface PassResult { platform?: string; phase: string; message: string; applied: number; pruned: number; refused: string[] }

/** Writes the status, unless it would change nothing (a pass every few seconds over a settled platform writes nothing). */
async function writeStatus(k: Kube, p: Platform, status: Record<string, unknown>): Promise<void> {
  const next = { observedGeneration: p.metadata.generation, ...status }
  const now = (p.status ?? {}) as Record<string, unknown>
  if (Object.entries(next).every(([key2, v]) => JSON.stringify(v) === JSON.stringify(now[key2]))) return
  await k.request('PATCH', `${platformsPath}/${p.metadata.name}/status`, { status: { observedGeneration: p.metadata.generation, ...status } }, 'application/merge-patch+json')
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
    const given = await secretData(n.namespace, r.spec.keys.caSecret, `read ${r.spec.keys.caSecret}`)
    if (!given?.['tls.crt'] || !given?.['tls.key']) throw new Error(`keys.caSecret: ${n.namespace}/${r.spec.keys.caSecret} holds no tls.crt and tls.key`)
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
  // the release a workload was rendered at, as stamped on it; one made before stamps by its image's tag
  const releaseOf = (o: any) => String(o?.metadata?.annotations?.[RELEASE_ANNOTATION] ?? String(o?.spec?.template?.spec?.containers?.[0]?.image ?? '').replace(/^.*[:@]/, ''))
  const judge = (o: any) => {
    const want = o.spec?.replicas ?? 1
    const ready = (o.status?.readyReplicas ?? 0) >= want && (o.status?.updatedReplicas ?? o.status?.currentReplicas ?? want) >= want
    return { ready, message: ready ? undefined : `${o.status?.readyReplicas ?? 0} of ${want} ready` }
  }
  for (const m of rendered.filter((x) => x.kind === 'Deployment' || x.kind === 'StatefulSet')) {
    const live = await k.request('GET', pathOf(m))
    if (live.status !== 200) { out.push({ kind: m.kind, namespace: m.metadata.namespace!, name: m.metadata.name, release: releaseOf(m), ready: false, message: 'not made yet' }); continue }
    out.push({ kind: m.kind, namespace: m.metadata.namespace!, name: m.metadata.name, release: releaseOf(live.json), ...judge(live.json) })
  }
  const sks = await k.request('GET', `/apis/apps/v1/namespaces/${r.names.namespace}/statefulsets?labelSelector=${sel({ 'peat-sk': 'true' })}`)
  for (const s of (sks.status === 200 ? sks.json.items : []) as any[]) out.push({ kind: 'StatefulSet', namespace: r.names.namespace, name: s.metadata.name, release: releaseOf(s), ...judge(s) })
  if (sks.status === 200 && (sks.json.items as any[]).length < r.spec.safekeepers.count) out.push({ kind: 'StatefulSet', namespace: r.names.namespace, name: 'safekeepers', release: r.images.release, ready: false, message: `${(sks.json.items as any[]).length} of ${r.spec.safekeepers.count} safekeepers made (the control layer makes them once it runs)` })
  return out
}

export const CA_CONFIG_MAP = 'peat-ca'
/**
 * What a client of each database needs, in the database's own namespace: the authority's public certificate (a ConfigMap `peat-ca`,
 * ca.crt, for sslmode=verify-full) in every namespace that holds a Database, and each made database's login (a Secret `<name>-login`:
 * username, password, the names to connect to and a URL). A branch and a replica use their root's login. The control layer keeps the
 * passwords in the platform namespace; these are copies the operator makes. Outside the platform's namespaces it deletes nothing: each copy
 * is owned by its Database (a login by its database, the certificate by every Database of its namespace), so Kubernetes removes it with the
 * last of them, and a person's own object of the same name is never the operator's to remove.
 */
async function published(k: Kube, r: Resolved, keys: Record<string, string>, caPublished: boolean): Promise<{ manifests: Manifest[]; dbs: any[]; keep: Set<string> }> {
  // a list that fails stops the pass: an empty answer would read as "no database wants its copies" and prune them
  const list = ok(await k.request('GET', `/apis/${API_VERSION}/databases`), 'list the Databases', [404])
  const dbs = list.status === 404 ? [] : list.json.items as any[]
  const keep = new Set<string>()
  const owner = (d: any) => ({ apiVersion: API_VERSION, kind: 'Database', name: String(d.metadata.name), uid: String(d.metadata.uid) })
  const meta = (name: string, namespace: string, owners: any[]) => ({ name, namespace, labels: { [MANAGED_BY]: OPERATOR, [PLATFORM_LABEL]: r.name }, ...(owners.length ? { ownerReferences: owners.map(owner) } : {}) })
  const out: Manifest[] = []
  // the database namespace has its own (render.ts), owned by the platform, not by its Databases
  if (caPublished) for (const ns of [...new Set(dbs.map((d) => d.metadata.namespace as string))].filter((x) => x !== r.names.dbNamespace).sort()) out.push({ apiVersion: 'v1', kind: 'ConfigMap', metadata: meta(CA_CONFIG_MAP, ns, dbs.filter((d) => d.metadata.namespace === ns && d.metadata.uid)), data: { 'ca.crt': keys['ca.crt'] ?? '' } })
  for (const d of dbs) {
    const rec = d.status?.record
    if (!d.status?.tenant || !rec || d.metadata.deletionTimestamp) continue
    const root = rec.root ? d.metadata.name : rec.parent
    const login = ok(await k.request('GET', `/api/v1/namespaces/${r.names.namespace}/secrets/db-${root}-login`), `read the login of ${root}`, [404])
    // a login not there (a make that has not written it yet): the copy is left as it is, neither written nor pruned
    if (login.status === 404) { keep.add(`Secret/${d.metadata.namespace}/${d.metadata.name}-login`); continue }
    const password = Buffer.from(String(login.json.data?.password ?? ''), 'base64').toString()
    const serviceHost = `${d.metadata.name}.${r.names.dbNamespace}.svc`
    const host = r.spec.gateways.domain ? `${d.metadata.name}.${r.spec.gateways.domain}` : serviceHost
    out.push({ apiVersion: 'v1', kind: 'Secret', metadata: meta(`${d.metadata.name}-login`, d.metadata.namespace, d.metadata.uid ? [d] : []), type: 'Opaque', stringData: {
      username: 'app', password, dbname: 'app', host, serviceHost, port: '5432',
      url: `postgresql://app:${encodeURIComponent(password)}@${host}:5432/app?sslmode=verify-full`,
    } })
  }
  return { manifests: out, dbs, keep }
}

/** One pass. Returns what it did, for the log and the tests. */
export async function reconcile(k: Kube): Promise<PassResult> {
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
  // the namespaces are where the platform's pieces and data are: once recorded they never change (the kind's rule says so too, on a cluster that judges it)
  const recordedNs = (p.status as any)?.namespaces
  const sameNamespaces = (a: Record<string, string>, b: Record<string, string>) => (['platform', 'computes', 'gateways', 'databases'] as const).every((f) => a[f] === b[f])
  if (recordedNs && !sameNamespaces(recordedNs, spec.namespaces)) return done('Failed', `the namespaces cannot change: the platform runs in ${JSON.stringify(recordedNs)}; set them back`)
  // records kubernetes needs the storage controller's record kinds, which come with the storage image that keeps them
  if (spec.controller.records === 'kubernetes') {
    const served = await k.request('GET', `/apis/${RECORDS_GROUP}/v1alpha1`)
    if (served.status !== 200) return done('Failed', `controller.records kubernetes needs the storage controller's record kinds (${RECORDS_GROUP}), which this cluster does not serve: apply an install file and storage images of a release that has them, or set records postgres`)
  }
  const machines = await labelledMachines(k, spec.machines.label)
  const kept = await readPlacements(k, spec.namespaces.platform)
  const zones = Object.fromEntries(machines.nodes.map((n) => [String(n.metadata.name), String(n.metadata?.labels?.[spec.safekeepers.zoneLabel] ?? n.metadata.name)]))
  const chosen = choosePlacements(spec, machines.names, kept, zones)
  if ('errors' in chosen) return done('Failed', chosen.errors.join('; '))
  const r = resolve(p, spec, chosen.placements)

  // the record of what the operator made, in its own namespace; a record that cannot be read stops the pass here
  const owned = await Owned.load(k, k.namespace)
  const applied: string[] = []
  const refused: string[] = []
  const applyOne = async (m: Manifest) => {
    const live = await k.request('GET', pathOf(m))
    if (live.status === 200) {
      // a namespace the Platform names is the platform's, made by the operator or beforehand by a person (to hold the store's Secret)
      if (m.kind !== 'Namespace' && !(await owned.ours(key(m), live.json))) {
        refused.push(`${key(m)} exists and was not made by the operator: left alone`)
        return
      }
      ok(await serverApply(k, m, FIELD_MANAGER), `apply ${key(m)}`)
    } else {
      ok(live, `read ${key(m)}`, [404])
      // the intent first, then the object stamped with its nonce, then its UID: a pass that dies between them is recognised by the next
      const nonce = await owned.intend(key(m))
      const a = ok(await serverApply(k, { ...m, metadata: { ...m.metadata, annotations: { ...(m.metadata.annotations ?? {}), [NONCE_ANNOTATION]: nonce } } } as Manifest, FIELD_MANAGER), `make ${key(m)}`)
      await owned.made(key(m), String(a.json?.metadata?.uid ?? ''))
    }
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

  // the authority that signs the databases' certificates: with an issuer, only the one keys.caSecret names (none is published otherwise)
  const caPublished = !r.spec.certificates.issuer || Boolean(r.spec.keys.caSecret)
  const pub = await published(k, r, keys, caPublished)
  const rendered = [...render(r, keys), ...pub.manifests]
  for (const m of rendered.filter((x) => x.kind !== 'Namespace')) await applyOne(m)
  // a database's status names its copies once they are there: its login written (not a person's own Secret of that name, refused above)
  const loginsWritten = new Set(pub.manifests.filter((m) => m.kind === 'Secret').map(key).filter((k2) => !refused.some((x) => x.startsWith(`${k2} `))))
  for (const d of pub.dbs) {
    if (!loginsWritten.has(`Secret/${d.metadata.namespace}/${d.metadata.name}-login`)) continue
    if (!d.status?.tenant || d.metadata.deletionTimestamp || (d.status.loginSecret && (d.status.caConfigMap || !caPublished))) continue
    await k.request('PATCH', `/apis/${API_VERSION}/namespaces/${d.metadata.namespace}/databases/${d.metadata.name}/status`, { status: { loginSecret: `${d.metadata.name}-login`, ...(caPublished ? { caConfigMap: CA_CONFIG_MAP } : {}) } }, 'application/merge-patch+json')
  }

  // what the operator made and no longer renders, by its record: removed only inside the platform's namespaces, or cluster-scoped and still
  // carrying the operator's labels, and only the very object it made (the delete carries the recorded UID as a precondition). A namespace
  // is never removed: what is in it may not be its own.
  const wanted = new Set([...rendered.map(key), ...pub.keep])
  const platformNamespaces = new Set([r.names.namespace, r.names.computeNamespace, r.names.gatewayNamespace, r.names.dbNamespace])
  let pruned = 0
  for (const k2 of owned.keys()) {
    if (wanted.has(k2)) continue
    const [kind, ns, name] = k2.split('/') as [string, string, string]
    const known = kindOf(k2)
    if (!known || kind === 'Namespace') continue
    // outside the platform's namespaces the operator deletes nothing: what it published there goes with its Database (an owner reference)
    if (known.namespaced && !platformNamespaces.has(ns)) { await owned.forget(k2); continue }
    const m = { apiVersion: known.apiVersion, kind, metadata: { name, ...(known.namespaced ? { namespace: ns } : {}) } }
    const live = await k.request('GET', pathOf(m))
    if (live.status === 404) { await owned.forget(k2); continue }
    ok(live, `read ${k2}`)
    if (!(await owned.ours(k2, live.json))) continue
    if (!known.namespaced && !(live.json.metadata?.labels?.[MANAGED_BY] === OPERATOR && live.json.metadata?.labels?.[PLATFORM_LABEL] === r.name)) continue
    const uid = String(live.json.metadata.uid)
    ok(await k.request('DELETE', pathOf(m), { apiVersion: 'v1', kind: 'DeleteOptions', preconditions: { uid } }), `remove ${k2}`, [404, 409])
    await owned.forget(k2)
    pruned++
  }

  const ps = await pieces(k, r, rendered)
  const notReady = ps.filter((x) => !x.ready)
  const releases = [...new Set(ps.map((x) => x.release))]
  const phase = notReady.length || refused.length ? 'Installing' : 'Ready'
  const message = refused.length ? refused[0]! : notReady.length ? `waiting for ${notReady.map((x) => `${x.name}${x.message ? ` (${x.message})` : ''}`).join(', ')}` : `every piece is ready on ${spec.release}`
  return done(phase, message, { pieces: ps, survives: r.survives, namespaces: spec.namespaces, ...(releases.length === 1 && !notReady.length ? { release: releases[0] } : {}) }, { applied: applied.length, pruned, refused })
}

export { GROUP }
