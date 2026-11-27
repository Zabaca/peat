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
import { issuerCaFindings, keyProblems, makeMissing, AUTH_PRIVATE, AUTH_PUBLIC, CA_CERT, CA_KEY } from './keys'
import { choosePlacements, imagesOf, resolve, specOf, type Images, type Placements, type Resolved } from './platform'
import { storageOf, type Storage } from './storage'
import { decide, imagesFor, sameImages, stepsOf, type UpgradeState } from './upgrade'
import { piecesOf, runStep } from './upgrade-run'
import { COMPUTE_IMAGE_ENV, KEYS_SECRET, MANAGED_BY, OPERATOR, PLATFORM_LABEL, RECORDS_GROUP, RELEASE_ANNOTATION, render, type Manifest } from './render'

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

/** The upgrade's optional fields: in a merge patch a field left out is kept, so a cleared one is written as null (which removes it). */
const UPGRADE_CLEARABLE = ['step', 'phase', 'since', 'halted', 'lastCheck', 'retried', 'complete'] as const
/** Writes the status, unless it would change nothing (a pass every few seconds over a settled platform writes nothing); null removes a field. */
async function writeStatus(k: Kube, p: Platform, status: Record<string, unknown>): Promise<void> {
  const u = status['upgrade'] as Record<string, unknown> | undefined
  const upgrade = u ? { ...Object.fromEntries(UPGRADE_CLEARABLE.map((f) => [f, null])), ...Object.fromEntries(Object.entries(u).filter(([, v]) => v !== undefined)) } : undefined
  const next: Record<string, unknown> = { observedGeneration: p.metadata.generation, ...status, ...(upgrade ? { upgrade } : {}) }
  const now = (p.status ?? {}) as Record<string, unknown>
  const bare = (v: unknown): unknown => (v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.entries(v as Record<string, unknown>).filter(([, x]) => x !== null && x !== undefined).map(([a, x]) => [a, bare(x)])) : v)
  if (Object.entries(next).every(([key2, v]) => JSON.stringify(bare(v) ?? null) === JSON.stringify(bare(now[key2]) ?? null))) return
  await k.request('PATCH', `${platformsPath}/${p.metadata.name}/status`, { status: next }, 'application/merge-patch+json')
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

/**
 * The keys as kept, with any the install gives in place of the operator's own; and, with an issuer, the issuer's certificate alone
 * (`issuer`: issuerCaFindings, keys.ts), which is published and never kept; a Secret that holds the issuer's key fails the pass.
 */
async function readKeys(k: Kube, r: Resolved): Promise<{ kept: Record<string, string>; issuer?: ReturnType<typeof issuerCaFindings> }> {
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
  if (r.spec.keys.caSecret && r.spec.certificates.issuer) {
    const given = await secretData(n.namespace, r.spec.keys.caSecret, `read ${r.spec.keys.caSecret}`)
    return { kept, issuer: issuerCaFindings(given ?? {}) }
  }
  if (r.spec.keys.caSecret) {
    const given = await secretData(n.namespace, r.spec.keys.caSecret, `read ${r.spec.keys.caSecret}`)
    if (!given?.['tls.crt'] || !given?.['tls.key']) throw new Error(`keys.caSecret: ${n.namespace}/${r.spec.keys.caSecret} holds no tls.crt and tls.key`)
    kept[CA_CERT] = given['tls.crt']
    kept[CA_KEY] = given['tls.key']
  }
  return { kept }
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

/** The mark on an object the operator took over from an install made another way. */
export const ADOPTED = `${GROUP}/adopted`
/**
 * How `live` differs from what the operator would make (`want`), as the paths of every field the operator sets whose value is not the live
 * one; fields the operator does not set (defaults the API server filled in, another tool's fields) are not differences. The operator's own
 * labels are not either: taking an object over is what adds them.
 */
export function differences(want: unknown, live: unknown, path = ''): string[] {
  if (path === '.metadata.labels') return []
  // the annotations the operator sets, less its release stamp (which an upgrade moves on its own); another tool's are not its business
  if (path === '.metadata.annotations') return differences(Object.fromEntries(Object.entries((want ?? {}) as Record<string, unknown>).filter(([a]) => a !== RELEASE_ANNOTATION)), live ?? {}, '.metadata.annotations.')
  if (path === '.stringData') return []
  if (want === null || typeof want !== 'object') return JSON.stringify(want) === JSON.stringify(live) ? [] : [`${path || '.'}: ${JSON.stringify(live) ?? 'none'} would be ${JSON.stringify(want)}`.slice(0, 240)]
  // an empty list or object is what an API server does not store (omitempty): absent live, it is no difference, for every kind
  if ((live === undefined || live === null) && (Array.isArray(want) ? want.length === 0 : Object.keys(want).length === 0)) return []
  if (Array.isArray(want)) {
    if (!Array.isArray(live) || live.length !== want.length) return [`${path}: ${Array.isArray(live) ? `${live.length} items` : JSON.stringify(live) ?? 'none'} would be ${want.length} items`]
    return want.flatMap((w, i) => differences(w, live[i], `${path}[${i}]`))
  }
  if (!live || typeof live !== 'object' || Array.isArray(live)) return [`${path}: none would be set`]
  const out = Object.entries(want as Record<string, unknown>).flatMap(([k2, v]) => differences(v, (live as Record<string, unknown>)[k2], path.endsWith('.') ? `${path}${k2}` : `${path}.${k2}`))
  // a Secret's stringData is written as its data: each value compared, base64, with the live data's (never printed: a key's name only)
  if (path === '' && (want as any)?.stringData && typeof (want as any).stringData === 'object') {
    const data = ((live as any).data ?? {}) as Record<string, string>
    for (const [key2, v] of Object.entries((want as any).stringData as Record<string, string>)) if (Buffer.from(String(v)).toString('base64') !== data[key2]) out.push(`.data.${key2}: its value would change`)
  }
  return out
}

/**
 * The images the platform runs, read from its live objects: for a status an earlier operator wrote without them, where a guess from the
 * release's name would miss an install that pins its images. Null when the pieces are not all there to read.
 */
async function liveImages(k: Kube, r: Resolved, release?: string): Promise<Images | null> {
  const dep = async (ns: string, name: string) => { const a = await k.request('GET', `/apis/apps/v1/namespaces/${ns}/deployments/${name}`); return a.status === 200 ? a.json : null }
  const [controller, control] = await Promise.all([dep(r.names.namespace, 'storage-controller'), dep(r.names.namespace, 'peat-control')])
  const image = (o: any) => String(o?.spec?.template?.spec?.containers?.[0]?.image ?? '')
  const env = (name: string) => ((control?.spec?.template?.spec?.containers?.[0]?.env ?? []) as any[]).find((e) => e.name === name)?.value as string | undefined
  const c18 = env(COMPUTE_IMAGE_ENV[18]), c19 = env(COMPUTE_IMAGE_ENV[19])
  if (!image(controller) || !image(control) || !c18 || !c19) return null
  return { release: release ?? String(controller?.metadata?.annotations?.[RELEASE_ANNOTATION] ?? 'unrecorded'), storage: image(controller), control: image(control), compute: { 18: c18, 19: c19 } }
}

/** Fields an object cannot change in place, by kind: a difference there is one an apply would be refused (422). */
const IMMUTABLE: Record<string, RegExp[]> = {
  StatefulSet: [/^\.spec\.selector\b/, /^\.spec\.volumeClaimTemplates\b/, /^\.spec\.serviceName\b/, /^\.spec\.podManagementPolicy\b/],
  Deployment: [/^\.spec\.selector\b/],
  Job: [/^\.spec\.template\b/, /^\.spec\.selector\b/],
  Service: [/^\.spec\.clusterIP\b/, /^\.spec\.clusterIPs\b/],
  RoleBinding: [/^\.roleRef\b/],
  ClusterRoleBinding: [/^\.roleRef\b/],
}
const immutableFor = (kind: string) => (path: string) => (IMMUTABLE[kind] ?? []).some((re) => re.test(path))

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
  // the operator's own namespace holds its record of what it made: none of the platform's may be it
  const mine = Object.entries(spec.namespaces).find(([, ns]) => ns === k.namespace)
  if (mine) return done('Failed', `the Platform is not valid: namespaces.${mine[0]}: ${k.namespace} is the operator's own namespace; the platform needs namespaces of its own`)
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
  // adoption's pending changes are applied when requests.applyPending asks (a value not yet answered)
  const applyPending = Boolean(spec.requests?.applyPending) && spec.requests?.applyPending !== (p.status as any)?.answered?.applyPending
  const adoptList = new Set((spec.adopt ?? []).map((a) => `${a.kind}/${a.namespace ?? ''}/${a.name}`))
  const pending: { kind: string; namespace?: string; name: string; change: string }[] = []
  // the objects the upgrade's current step is applying this pass (set below, once the step is decided)
  let stepApplies = new Set<string>()
  const applyOne = async (m: Manifest) => {
    const live = await k.request('GET', pathOf(m))
    if (live.status === 200) {
      const ours = m.kind === 'Namespace' || (await owned.ours(key(m), live.json))
      // an object listed in adopt that the record does not hold is taken into it (an install made another way, or a record lost or restored
      // with new UIDs), and stays as it was taken over (marked adopted, listed or not): a difference between it and what the operator would
      // make is listed as a pending change and applied only when requests.applyPending asks
      // once in the record with nothing left pending (the adopted mark gone), it is the operator's like any other, listed or not: a later change,
      // an upgrade's included, is applied, never held as pending
      if ((adoptList.has(key(m)) && !ours) || (ours && live.json.metadata?.labels?.[ADOPTED] === 'true')) {
        if (!ours) {
          // the mark first, then the record: a pass that dies between them leaves an object still marked and listed (taken into the record
          // on the next pass, its difference still held), never one in the record without the mark (which would count as ordinary and be
          // applied with every difference)
          ok(await k.request('PATCH', pathOf(m), { metadata: { labels: { [MANAGED_BY]: OPERATOR, [PLATFORM_LABEL]: r.name, [ADOPTED]: 'true' } } }, 'application/merge-patch+json'), `adopt ${key(m)}`)
          await owned.made(key(m), String(live.json.metadata.uid))
        }
        const diff = differences(m, live.json)
        // a difference in a field that cannot change in place is never applied (it would be refused on every pass): said as such
        const fixed = diff.filter((d) => immutableFor(m.kind)(d))
        // the upgrade's own step applies its piece's new images to an adopted object too: that is how an adopted platform moves, in order
        const byStep = stepApplies.has(key(m))
        // a pageserver's pods take a new template only when the ordered upgrade restarts them (OnDelete): applyPending does not change one
        const podsHeld = !byStep && m.kind === 'StatefulSet' && diff.some((d) => d.startsWith('.spec.template'))
        if (diff.length && ((!applyPending && !byStep) || fixed.length || podsHeld)) {
          if (podsHeld && applyPending) diff.unshift('its pods change only through the ordered upgrade: set the release or images it should run')
          pending.push({ kind: m.kind, ...(m.metadata.namespace ? { namespace: m.metadata.namespace } : {}), name: m.metadata.name, change: (fixed.length ? `cannot be applied in place (${fixed.slice(0, 3).join('; ')}): make the object again by hand, or leave it; ` : '') + diff.slice(0, 8).join('; ') + (diff.length > 8 ? `; and ${diff.length - 8} more` : '') })
          return
        }
        ok(await serverApply(k, m, FIELD_MANAGER), `apply ${key(m)}`)
        // taken over with nothing left pending: an object of the operator's like any other
        ok(await k.request('PATCH', pathOf(m), { metadata: { labels: { [ADOPTED]: null } } }, 'application/merge-patch+json'), `adopted ${key(m)}`)
        applied.push(key(m))
        return
      }
      // a namespace the Platform names is the platform's, made by the operator or beforehand by a person (to hold the store's Secret)
      if (!ours) {
        refused.push(`${key(m)} exists and was not made by the operator: left alone (adopt takes an object into the record${owned.fresh ? "; the operator's record was made new on this pass: if the operator made it before, list it under adopt to take it back" : ''})`)
        return
      }
      // a Job's template cannot change (the API server answers 422): a Job the operator made whose template differs is deleted and made again
      if (m.kind === 'Job' && differences((m as any).spec?.template, live.json.spec?.template).length) {
        ok(await k.request('DELETE', pathOf(m), { apiVersion: 'v1', kind: 'DeleteOptions', propagationPolicy: 'Background', preconditions: { uid: String(live.json.metadata.uid) } }), `remove ${key(m)} to make it again`, [404])
        await owned.forget(key(m))
        return applyOne(m)
      }
      ok(await serverApply(k, m, FIELD_MANAGER), `apply ${key(m)}`)
    } else {
      ok(live, `read ${key(m)}`, [404])
      // the intent first, then the object stamped with its nonce, then its UID: a pass that dies between them is recognised by the next
      const nonce = await owned.intend(key(m))
      const a = ok(await serverApply(k, { ...m, metadata: { ...m.metadata, annotations: { ...(m.metadata.annotations ?? {}), [NONCE_ANNOTATION]: nonce } } } as Manifest, FIELD_MANAGER), `make ${key(m)}`)
      // an apply that made nothing answers 200, not 201: someone made the object between the read and the apply, so it is not recorded
      if (a.status === 200) {
        await owned.forget(key(m))
        refused.push(`${key(m)} was made by someone else as the operator made it: applied over once, left out of the record`)
        return
      }
      // an answer without a UID records nothing: the intent stays, and the next pass knows the object by its nonce
      if (a.json?.metadata?.uid) await owned.made(key(m), String(a.json.metadata.uid))
    }
    applied.push(key(m))
  }
  const rendered0 = render(r, {})
  for (const m of rendered0.filter((x) => x.kind === 'Namespace')) await applyOne(m)

  const read = await readKeys(k, r)
  let keys = read.kept
  const missing = makeMissing(keys)
  if (Object.keys(missing).length) {
    keys = { ...keys, ...missing }
    await writeKeys(k, r, keys)
  }
  const problems = keyProblems(keys)
  if (read.issuer) problems.push(...read.issuer.problems)
  if (problems.length) return done('Failed', `the keys: ${problems.join('; ')}`)
  // what is published from here on: the issuer's certificate in peat-ca, in place of the operator's own (which then signs nothing)
  if (read.issuer?.cert) keys = { ...keys, [CA_CERT]: read.issuer.cert }
  if (JSON.stringify(kept) !== JSON.stringify(r.placements)) {
    const cm = { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: PLACEMENTS_CONFIG_MAP, namespace: r.names.namespace, labels: { [MANAGED_BY]: OPERATOR } }, data: { 'placements.json': JSON.stringify(r.placements) } }
    ok(await serverApply(k, cm, FIELD_MANAGER), 'keep the placements')
  }

  // the upgrade: decided before anything is applied, so each piece is rendered at the release its step has reached
  const status = (p.status ?? {}) as any
  // what the platform runs: the release recorded, or, with none recorded (a first install not yet Ready, an adopted platform), the live
  // pieces' images: once any piece runs, a changed release is only ever rendered through the ordered steps
  const seeded = status.release ? null : await liveImages(k, r)
  const running = status.release ? { release: String(status.release), images: (status.images as Images | undefined) ?? (await liveImages(k, r, String(status.release))) ?? imagesOf(spec, String(status.release)) }
    : seeded ? { release: sameImages(seeded, r.images) ? spec.release : seeded.release, images: seeded } : undefined
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
  if (live?.step && !['precheck', 'draining'].includes(String(live.phase))) {
    const names = new Set(piecesOf(live.step))
    stepApplies = new Set(render(r, keys, at).filter((m) => (m.kind === 'Deployment' || m.kind === 'StatefulSet') && names.has(m.metadata.name)).map(key))
  }
  // the authority that signs the databases' certificates: with an issuer, only the one keys.caSecret names (none is published otherwise)
  const caPublished = !r.spec.certificates.issuer || Boolean(r.spec.keys.caSecret)
  const pub = await published(k, r, keys, caPublished)
  const rendered = [...render(r, keys, at, live ? [live.fromImages.storage, live.toImages.storage] : [r.images.storage]), ...pub.manifests]
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
  const counts = { applied: applied.length, pruned, refused }
  const more = { pieces: ps, survives: r.survives, pendingChanges: pending, ...(applyPending ? { answered: { applyPending: spec.requests!.applyPending } } : {}), ...(upgrade ? { upgrade } : {}) }
  // while an upgrade is under way no one release is what every piece runs: status.release is cleared until it ends (the upgrade holds both ends)
  const midway = live ? { release: null, images: null } : {}
  if (live?.halted) return done('Halted', `the upgrade to ${live.direction === 'forward' ? live.to : live.from} halted at ${live.halted.step}: ${live.halted.reason}. peat retry goes on from there; setting ${live.direction === 'forward' ? live.from : live.to} takes back what moved`, { ...more, ...midway }, counts)
  if (decision.kind === 'pause') return done('Paused', `the upgrade to ${live!.direction === 'forward' ? live!.to : live!.from} waits for the quiet hours (${decision.window}) to end: ${live!.moved.length} of ${live!.steps.length} steps moved`, { ...more, ...midway }, counts)
  if (live) return done('Upgrading', `${live.direction === 'forward' ? `${live.from} to ${live.to}` : `back from ${live.to} to ${live.from}`}: ${stepMessage}`, { ...more, ...midway }, counts)
  if (decision.kind === 'finish') return done(notReady.length || pending.length ? 'Installing' : 'Ready', `the upgrade ${decision.state.direction === 'forward' ? 'to' : 'back to'} ${decision.release} is done: every piece runs it`, { ...more, release: decision.release, images: decision.images }, counts)
  // an adopt entry naming nothing the operator makes is said, not silently ignored
  const unmatched = [...adoptList].filter((a) => !rendered.some((m) => key(m) === a))
  const note = unmatched.length ? `; adopt names ${unmatched.join(', ')}, which the platform does not make: nothing is taken over for ${unmatched.length === 1 ? 'it' : 'them'}` : ''
  // a pending change keeps the platform from Ready: what runs is not yet what is declared
  const phase = notReady.length || refused.length || pending.length ? 'Installing' : 'Ready'
  const message = (refused.length ? refused[0]! : notReady.length ? `waiting for ${notReady.map((x) => `${x.name}${x.message ? ` (${x.message})` : ''}`).join(', ')}` : pending.length ? `${pending.length} pending change${pending.length === 1 ? '' : 's'} wait for requests.applyPending (status.pendingChanges)` : `every piece is ready on ${spec.release}`) + note + (read.issuer?.warnings.length ? `; ${read.issuer.warnings.join('; ')}` : '')
  // the release every piece runs, recorded once all are ready on it: what the next upgrade starts from
  // the release every piece runs, recorded once all are ready on it, whatever is pending or refused: what the next upgrade starts from
  const record = !notReady.length && !status.release && decision.kind === 'none'
  return done(phase, message, { ...more, namespaces: spec.namespaces, ...(record ? { release: running?.release ?? spec.release, images: running?.images ?? r.images } : {}) }, counts)
}

export { GROUP }
