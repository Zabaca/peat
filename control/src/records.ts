// A database's record, kept twice: in its Database object's status (what a client and the loops read) and in a ConfigMap `db-<name>` in
// the platform namespace (label peat-record), which only the control layer can write. The ConfigMap is the durable copy: a status wiped
// (a patch of the status subresource, a restore from a backup that leaves status out) is written again from it, a record lost from it is
// written again from the status, and its format is the one the records had before Database objects, so a way back to a control layer of
// that kind reads every database, those made since included. Around the record:
//   - `making-<name>` holds the ids of a database being made before its storage exists, so a make that dies half way is finished with the
//     same tenant and timeline on the next pass, not begun again beside them;
//   - `held-<name>-<timeline>` holds a deleted database's data until its hold is over (one per deletion: the same name deleted twice in a
//     hold keeps both);
//   - a root's login `db-<name>-login` carries its tenant, so the purge of an old database never removes the login of a newer one of the
//     same name;
//   - `port-<n>` claims a gateway port for one database (its owner and timeline).
// Every function takes its API calls as arguments, so the tests run them against an API server in memory.
import { randomBytes } from 'node:crypto'
import type { Answer } from './kube'
import { GROUP } from './names'
import { COPYING, FINALIZER, purgeDue } from './databases'
import { pgVersionOf } from './storage-spec'

export interface Db { name: string; namespace: string; tenant: string; timeline: string; parent: string; from: string; root: boolean; mode: 'primary' | 'replica'; port: number; created: string; pg: number; closed: boolean }

export interface Deps {
  kube(method: string, path: string, body?: unknown, contentType?: string): Promise<Answer>
  storcon(method: string, path: string, body?: unknown): Promise<Answer>
  /** A timeline's ancestor, as its pageserver tells it (null: no answer). */
  ancestorOf?(tenant: string, timeline: string): Promise<string | null>
  /** The platform namespace (the records, the logins, the claims). */
  ns: string
  /** The database namespace: the older records' Database objects are made there. */
  dbNs: string
  /** The domain clients reach the databases under, when the platform has one. */
  domain?: string
  log?(line: string): void
}

export const RECORD_LABEL = 'peat-record'
export const HELD_LABEL = 'peat-held'
export const MAKING_LABEL = 'peat-making'
export const SUPERSEDED_LABEL = 'peat-superseded'
export const PORT_LABEL = 'peat-port'
/** On a root's login: the tenant whose login it is. */
export const TENANT_ANNOTATION = `${GROUP}/tenant`
export const DB_API = `/apis/${GROUP}/v1alpha1`
export const dbObjectPath = (ns: string, name: string) => `${DB_API}/namespaces/${ns}/databases/${name}`
const cms = (d: Deps) => `/api/v1/namespaces/${d.ns}/configmaps`
const secrets = (d: Deps) => `/api/v1/namespaces/${d.ns}/secrets`
export const loginName = (root: string) => `db-${root}-login`
export const heldName = (name: string, timeline: string) => `held-${name}-${timeline}`

export const ok = (r: Answer, what: string, also: number[] = []) => {
  if (r.status >= 300 && !also.includes(r.status)) throw new Error(`${what}: ${r.status} ${JSON.stringify(r.json).slice(0, 300)}`)
  return r
}
/** Create, or replace when it exists. */
async function apply(d: Deps, collection: string, name: string, obj: any): Promise<Answer> {
  const made = await d.kube('POST', collection, obj)
  if (made.status !== 409) return ok(made, `create ${name}`)
  const live = ok(await d.kube('GET', `${collection}/${name}`), `read ${name}`)
  return ok(await d.kube('PUT', `${collection}/${name}`, { ...obj, metadata: { ...obj.metadata, resourceVersion: live.json.metadata.resourceVersion } }), `replace ${name}`)
}

// ---- the record's two forms
/** The record in a Database object's status, or null when the status holds none. */
export function fromStatus(o: any): Db | null {
  const s = o?.status, r = s?.record
  if (!s?.tenant || !r) return null
  return { name: o.metadata.name, namespace: o.metadata.namespace, tenant: s.tenant, timeline: s.timeline, parent: r.parent ?? '', from: r.from ?? '', root: r.root === true, mode: r.mode === 'replica' ? 'replica' : 'primary', port: Number(r.port ?? 0), created: s.createdAt ?? '', pg: pgVersionOf(s.pgVersion === undefined ? undefined : String(s.pgVersion)), closed: s.phase === 'Closed' || s.phase === 'Moving' }
}
/** The record in its ConfigMap. A record from before Database objects has no namespace (its object goes in the database namespace) and no `from`. */
export function fromConfigMap(cm: any, dbNs: string): Db {
  const v = cm.data ?? {}
  return { name: String(cm.metadata.name).slice(3), namespace: v.namespace || dbNs, tenant: v.tenant, timeline: v.timeline, parent: v.parent ?? '', from: v.from ?? '', root: v.root === 'true', mode: v.mode === 'replica' ? 'replica' : 'primary', port: Number(v.port ?? 0), created: v.created || cm.metadata.creationTimestamp || '', pg: pgVersionOf(v.pg), closed: false }
}
const recordData = (r: Db) => ({ tenant: r.tenant, timeline: r.timeline, parent: r.parent, from: r.from, root: String(r.root), mode: r.mode, port: String(r.port), created: r.created, pg: String(r.pg), namespace: r.namespace })

/** A database by name: its object's status (`fromStatuses`), else the platform's record of it, else null (nothing knows it). */
export const knownDb = (name: string, fromStatuses: Map<string, Db>, records: Map<string, { db: Db }>): Db | null => fromStatuses.get(name) ?? records.get(name)?.db ?? null

/** Every record in the platform namespace, by name, with its ConfigMap. */
export async function readRecords(d: Deps): Promise<Map<string, { db: Db; cm: any }>> {
  const items = ok(await d.kube('GET', `${cms(d)}?labelSelector=${RECORD_LABEL}%3Dtrue`), 'list the records').json.items as any[]
  return new Map(items.map((cm) => { const db = fromConfigMap(cm, d.dbNs); return [db.name, { db, cm }] }))
}

/** Writes a database's record: its ConfigMap first (the durable copy), then its object's status (its fields only). */
export async function save(d: Deps, r: Db, more: Record<string, unknown> = {}): Promise<void> {
  await writeRecord(d, r)
  ok(await d.kube('PATCH', `${dbObjectPath(r.namespace, r.name)}/status`, { status: {
    tenant: r.tenant, timeline: r.timeline, pgVersion: r.pg, createdAt: r.created,
    record: { mode: r.mode, root: r.root, parent: r.parent, from: r.from, port: r.port },
    // the address a client connects to: the platform's domain, else the name inside the cluster (what `kubectl get database` shows as Host)
    serviceHost: `${r.name}.${d.dbNs}.svc`, host: d.domain ? `${r.name}.${d.domain}` : `${r.name}.${d.dbNs}.svc`, port: 5432,
    ...more,
  } }, 'application/merge-patch+json'), `record ${r.name}`)
}
/** Writes the record's ConfigMap alone (a record the status holds and the ConfigMaps lost). */
export async function writeRecord(d: Deps, r: Db): Promise<void> {
  const live = await d.kube('GET', `${cms(d)}/db-${r.name}`)
  const labels = { [RECORD_LABEL]: 'true', ...(live.status === 200 ? live.json.metadata?.labels ?? {} : {}) }
  await apply(d, cms(d), `db-${r.name}`, { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: `db-${r.name}`, namespace: d.ns, labels }, data: recordData(r) })
}

// ---- making
export interface Making { tenant: string; timeline: string; port?: number }
/**
 * Claims the name for a database being made in `namespace` and returns the ids to make it with: `ids` when the claim is new, the ids an
 * earlier pass chose when one is there for the same namespace (a make that died half way is finished, not begun again), or `taken` when a
 * make in another namespace holds the name.
 */
export async function beginMaking(d: Deps, name: string, namespace: string, ids: { tenant: string; timeline: string }, kind: { root: boolean; mode: Db['mode']; parent: string; from: string }): Promise<Making | { taken: string }> {
  const made = await d.kube('POST', cms(d), { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: `making-${name}`, namespace: d.ns, labels: { [MAKING_LABEL]: 'true' } }, data: { namespace, ...ids, root: String(kind.root), mode: kind.mode, parent: kind.parent, from: kind.from } })
  if (made.status !== 409) { ok(made, `claim the name ${name}`); return ids }
  const v = ok(await d.kube('GET', `${cms(d)}/making-${name}`), `read the claim on ${name}`).json.data ?? {}
  if (v.namespace !== namespace) return { taken: String(v.namespace) }
  return { tenant: String(v.tenant), timeline: String(v.timeline) }
}
/** The make is recorded: its claim goes. */
export const endMaking = async (d: Deps, name: string) => { ok(await d.kube('DELETE', `${cms(d)}/making-${name}`), `release the claim on ${name}`, [404]) }
/** A make claimed for `namespace` and never finished (its object deleted before it was made), as the held record its data needs. */
export async function unfinishedMake(d: Deps, name: string, namespace: string): Promise<Db | null> {
  const r = await d.kube('GET', `${cms(d)}/making-${name}`)
  if (r.status === 404) return null
  const v = ok(r, `read the claim on ${name}`).json.data ?? {}
  if (v.namespace !== namespace) return null
  return { name, namespace, tenant: v.tenant, timeline: v.timeline, parent: v.parent ?? '', from: v.from ?? '', root: v.root === 'true', mode: v.mode === 'replica' ? 'replica' : 'primary', port: 0, created: '', pg: 0, closed: false }
}

/** A root's login and the tenant it names: `marked` a tenant, `unmarked` a login from before logins carried one, or `none`. */
export async function loginOf(d: Deps, root: string): Promise<{ state: 'marked'; tenant: string; uid: string } | { state: 'unmarked'; uid: string } | { state: 'none' }> {
  const live = await d.kube('GET', `${secrets(d)}/${loginName(root)}`)
  if (live.status === 404) return { state: 'none' }
  ok(live, `read ${loginName(root)}`)
  const t = live.json.metadata?.annotations?.[TENANT_ANNOTATION]
  return t ? { state: 'marked', tenant: String(t), uid: live.json.metadata.uid } : { state: 'unmarked', uid: live.json.metadata.uid }
}
/** Whether the login of `root` is the login of `tenant`: marked with it, or unmarked while no live database of that name has another. */
export const loginIsTenants = (login: Awaited<ReturnType<typeof loginOf>>, root: string, tenant: string, live: Db[]) =>
  login.state === 'marked' ? login.tenant === tenant : login.state === 'unmarked' && !live.some((x) => x.root && x.name === root && x.tenant !== tenant)

/**
 * A root's login: made with its tenant on it, and kept as it is when an earlier pass of the same make wrote it. `adoptUnmarked`: a login
 * from before logins carried their tenant is this tenant's (a record brought back), so it is marked and its password kept.
 */
export async function ensureLogin(d: Deps, root: string, tenant: string, password: () => string, adoptUnmarked = false): Promise<void> {
  const path = `${secrets(d)}/${loginName(root)}`
  const live = await d.kube('GET', path)
  if (live.status === 200 && live.json.metadata?.annotations?.[TENANT_ANNOTATION] === tenant) return
  if (live.status !== 404) ok(live, `read ${loginName(root)}`)
  if (live.status === 200 && adoptUnmarked && !live.json.metadata?.annotations?.[TENANT_ANNOTATION]) {
    ok(await d.kube('PATCH', path, { metadata: { annotations: { [TENANT_ANNOTATION]: tenant } } }, 'application/merge-patch+json'), `mark ${loginName(root)}`)
    return
  }
  await apply(d, secrets(d), loginName(root), { apiVersion: 'v1', kind: 'Secret', metadata: { name: loginName(root), namespace: d.ns, annotations: { [TENANT_ANNOTATION]: tenant } }, data: { password: Buffer.from(password()).toString('base64') } })
}

/**
 * The tenant of an earlier database of `name` whose record is lost and whose data is still in the store: its login names a tenant (not
 * `tenant`, the one this make chose) that no held deletion names and the storage controller still has. A make under the name is refused
 * while it is there: making it again would leave the earlier data named by nothing and replace its password.
 */
export async function orphanedTenant(d: Deps, name: string, tenant: string): Promise<string | null> {
  const login = await loginOf(d, name)
  if (login.state !== 'marked' || login.tenant === tenant) return null
  const held = ok(await d.kube('GET', `${cms(d)}?labelSelector=${HELD_LABEL}%3Dtrue`), 'list held deletions').json.items as any[]
  // only a root, recorded or held, names the tenant its login is for: a branch of it left alone means its root's record was lost too
  if (held.some((h) => h.data?.tenant === login.tenant && h.data?.root === 'true')) return null
  if ([...(await readRecords(d)).values()].some((r) => r.db.tenant === login.tenant && r.db.root)) return null
  const there = await d.storcon('GET', `/v1/tenant/${login.tenant}/timeline`)
  return there.status === 200 ? login.tenant : null
}

/**
 * A gateway port for the database of `timeline`: the one a claim already holds for it (an earlier pass of the same make), else the first
 * free one from `base`. A port is claimed by creating `port-<n>` (the API server refuses a second create), so two control layer pods never
 * give one port to two databases; the records' ports count as claimed too (records from before claims).
 */
export async function claimPort(d: Deps, owner: string, timeline: string, base: number, used: Set<number>): Promise<number> {
  const claims = ok(await d.kube('GET', `${cms(d)}?labelSelector=${PORT_LABEL}%3Dtrue`), 'list the port claims').json.items as any[]
  const mine = claims.find((c) => c.data?.owner === owner && c.data?.timeline === timeline)
  if (mine) return Number(String(mine.metadata.name).slice(5))
  for (let p = base; p < 19999; p++) { // the gateway's own range is 15000 to 19999
    if (used.has(p)) continue
    const made = await d.kube('POST', cms(d), { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: `port-${p}`, namespace: d.ns, labels: { [PORT_LABEL]: 'true' } }, data: { owner, timeline } })
    if (made.status === 409) continue
    ok(made, `claim port ${p}`)
    return p
  }
  throw new Error('no gateway port left')
}

// ---- deleting
/**
 * A port kept for `name`: its claim made (owned by the name and `timeline`), or kept when it is already the name's (a record from before
 * claims has none). False when another database claims it.
 */
async function reservePort(d: Deps, name: string, timeline: string, port: number): Promise<boolean> {
  const made = await d.kube('POST', cms(d), { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: `port-${port}`, namespace: d.ns, labels: { [PORT_LABEL]: 'true' } }, data: { owner: name, timeline } })
  if (made.status !== 409) { ok(made, `claim port ${port}`); return true }
  const claim = ok(await d.kube('GET', `${cms(d)}/port-${port}`), `read the claim of port ${port}`).json
  // the name's claim, whatever timeline it names (a recovery moves a database to a new timeline and keeps its port)
  return claim.data?.owner === name
}

/**
 * A deleted database's data is held: `held-<name>-<timeline>` records it until `after` (null: never; the epoch: at once), its port stays
 * claimed (so no database made during the hold is given it), and its record goes.
 */
export async function hold(d: Deps, r: Db, after: Date | null, now: Date = new Date()): Promise<void> {
  if (r.port > 0 && !(await reservePort(d, r.name, r.timeline, r.port))) d.log?.(`hold: ${r.name}: port ${r.port} is claimed by another database: its held record keeps the number, and bringing it back is refused while the claim stands`)
  await apply(d, cms(d), heldName(r.name, r.timeline), {
    apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: heldName(r.name, r.timeline), namespace: d.ns, labels: { [HELD_LABEL]: 'true' } },
    data: { ...recordData(r), name: r.name, deletedAt: now.toISOString(), deleteAfter: after ? after.toISOString() : 'never' },
  })
  const rec = await d.kube('GET', `${cms(d)}/db-${r.name}`)
  // the record goes only if it is this database's (a record of the same name and another timeline is a newer database's)
  if (rec.status === 200 && rec.json.data?.timeline === r.timeline) ok(await d.kube('DELETE', `${cms(d)}/db-${r.name}`, { preconditions: { uid: rec.json.metadata.uid } }), `delete the record of ${r.name}`, [404, 409])
  await endMaking(d, r.name)
}

/**
 * Held data whose time has come is removed from the store: a branch's timeline (unless a database still uses it or a timeline was made
 * from it), or a root's tenant once no database uses it, with the root's own login (only the login of that tenant). `inUse` is every
 * database that is not being deleted (the records' own list is read here too). One held deletion that fails is logged and the next goes on.
 */
export async function purgeHeld(d: Deps, inUse: Db[], now: Date = new Date()): Promise<string[]> {
  const held = ok(await d.kube('GET', `${cms(d)}?labelSelector=${HELD_LABEL}%3Dtrue`), 'list held deletions').json.items as any[]
  const live = [...(await readRecords(d)).values()].map((x) => x.db).concat(inUse)
  const purged: string[] = []
  for (const h of held) {
    const t = h.data ?? {}
    try {
      const tenantInUse = live.some((x) => x.tenant === t.tenant) || held.some((o) => o !== h && o.data?.tenant === t.tenant && o.data?.root !== 'true')
      if (!purgeDue({ deleteAfter: t.deleteAfter, tenant: t.tenant, root: t.root === 'true' }, now, tenantInUse)) continue
      if (t.mode === 'primary' && t.root === 'true') {
        ok(await d.storcon('DELETE', `/v1/tenant/${t.tenant}`), `delete tenant ${t.tenant}`, [404])
        const login = await d.kube('GET', `${secrets(d)}/${loginName(t.name)}`)
        if (login.status === 200) {
          const of = login.json.metadata?.annotations?.[TENANT_ANNOTATION]
          // a login without a tenant is from before logins carried one: it is this database's only while no database of the name exists
          if (of === t.tenant || (!of && !live.some((x) => x.root && x.name === t.name))) ok(await d.kube('DELETE', `${secrets(d)}/${loginName(t.name)}`, { preconditions: { uid: login.json.metadata.uid } }), `delete ${loginName(t.name)}`, [404, 409])
        }
      } else if (t.mode === 'primary') {
        if (live.some((x) => x.timeline === t.timeline)) { d.log?.(`purge: ${t.name}'s timeline is used by a database: kept`); continue }
        const timelines = await d.storcon('GET', `/v1/tenant/${t.tenant}/timeline`)
        if (timelines.status === 404) { /* the tenant is gone already: nothing to delete but the marks */ }
        else if (timelines.status !== 200) { d.log?.(`purge: ${t.name}: the timelines of its tenant could not be listed (${timelines.status}): kept until they can`); continue }
        else if ((timelines.json as any[]).some((x) => x.ancestor_timeline_id === t.timeline)) { d.log?.(`purge: ${t.name}'s timeline has timelines made from it: kept until they go`); continue }
        ok(await d.storcon('DELETE', `/v1/tenant/${t.tenant}/timeline/${t.timeline}`), `delete timeline ${t.timeline}`, [404])
      }
      // its port claim: the one its record names, or (a make that never finished, whose record named none) the claim of its name and
      // timeline; only a claim of this name that no database holds now (a recovery moves a database to a new timeline and keeps its port)
      const claims = ok(await d.kube('GET', `${cms(d)}?labelSelector=${PORT_LABEL}%3Dtrue`), 'list the port claims').json.items as any[]
      for (const claim of claims.filter((c) => c.data?.owner === t.name && (c.metadata.name === `port-${t.port}` || c.data?.timeline === t.timeline))) {
        if (live.some((x) => `port-${x.port}` === claim.metadata.name)) continue
        ok(await d.kube('DELETE', `${cms(d)}/${claim.metadata.name}`, { preconditions: { uid: claim.metadata.uid } }), `release ${claim.metadata.name}`, [404, 409])
      }
      ok(await d.kube('DELETE', `${cms(d)}/${h.metadata.name}`), `delete ${h.metadata.name}`, [404])
      purged.push(t.name)
      d.log?.(`purge: ${t.name}'s data removed from the store (held since ${t.deletedAt})`)
    } catch (e) { d.log?.(`purge: ${t.name}: ${e}`) }
  }
  return purged
}

// ---- the copy of records into Database objects (`POST /records/copy`)
export interface CopyResult { records: number; copied: string[]; finished: string[]; equal: string[]; differ: { name: string; fields: string[] }[]; missing: string[]; restored: string[]; refused: { name: string; why: string }[]; parentGuessed: string[]; databases: number; countsEqual: boolean }
/** What `?held=` takes: a name (its newest held deletion), or `<name>@<timeline>` (that one). */
export const HELD_SELECTOR = /^[a-z][a-z0-9-]{0,30}(@[0-9a-f]{32})?$/

/**
 * The database a record was made from, for its object's spec: a replica's primary (the record of its timeline), a branch's direct parent
 * (the record of its timeline's ancestor, following a recovery's new timeline for its old one), the root when nothing nearer is known.
 */
async function directParent(d: Deps, r: Db, all: Db[], recoveredTo: Map<string, string>): Promise<{ name: string; guessed: boolean }> {
  if (r.from) return { name: r.from, guessed: false }
  if (r.root) return { name: '', guessed: false }
  const named = (tl: string) => all.find((x) => x.mode === 'primary' && x.tenant === r.tenant && x.timeline === tl && x.name !== r.name)?.name
  if (r.mode === 'replica') { const n = named(r.timeline); return n ? { name: n, guessed: false } : { name: r.parent, guessed: true } }
  let tl: string | null = r.timeline
  for (let i = 0; i < 32 && tl; i++) {
    tl = d.ancestorOf ? await d.ancestorOf(r.tenant, tl) : null
    if (!tl) break
    let to = tl
    while (recoveredTo.has(to)) to = recoveredTo.get(to)!
    const n = named(to) ?? named(tl)
    if (n) return { name: n, guessed: false }
  }
  return { name: r.parent, guessed: true } // nothing nearer is known: the root (listed in the copy's result as parentGuessed)
}

/**
 * Every record without a Database object gets one (in its namespace, its status written from the record, the old ConfigMap labelled
 * superseded); an object a copy left half made is finished; then each record is compared with its object (ids, kind, port, version, when
 * made), with the storage (its timeline is there) and with its login (the root's login names its tenant). `dry` reads and compares only,
 * listing what would be copied as missing. `held` names deleted databases to bring back from their held data, refused when the name is in
 * use now (or by another brought back in the same call), its hold ended and its data was removed, its timeline is not in the store, its
 * namespace is gone, its port is another database's, or (a branch or a replica) its root's login is now another database's. A record that
 * cannot be copied is listed as refused and the rest go on. Nothing is deleted or changed in the old records but their label, and the tenant
 * marked on a login from before logins carried one.
 */
export async function copyRecords(d: Deps, opts: { dry: boolean; held?: string[] }): Promise<CopyResult> {
  const out: CopyResult = { records: 0, copied: [], finished: [], equal: [], differ: [], missing: [], restored: [], refused: [], parentGuessed: [], databases: 0, countsEqual: false }
  const objects = async () => ok(await d.kube('GET', `${DB_API}/databases`), 'list the Database objects').json.items as any[]
  let objs = await objects()
  let recs = await readRecords(d)
  // held databases asked back: the record is written again from the held data and the copy below makes the object
  const restored: Db[] = []
  const liveOf = () => [...recs.values()].map((x) => x.db).concat(objs.map(fromStatus).filter((x): x is Db => x !== null), restored)
  for (const selector of opts.held ?? []) {
    const [name, timeline] = selector.split('@') as [string, string | undefined]
    const refuse = (why: string) => { out.refused.push({ name: selector, why }) }
    let made: Answer | null = null, recorded = false
    try {
      const held = (ok(await d.kube('GET', `${cms(d)}?labelSelector=${HELD_LABEL}%3Dtrue`), 'list held deletions').json.items as any[]).filter((h) => h.data?.name === name && (!timeline || h.data?.timeline === timeline))
      const h = held.sort((a, b) => String(b.data.deletedAt).localeCompare(String(a.data.deletedAt)))[0]
      if (!h) { refuse('no held data of that name (its hold ended and its data was removed, or it was never deleted)'); continue }
      if (recs.has(name) || objs.some((o) => o.metadata.name === name) || restored.some((x) => x.name === name)) { refuse('the name is in use now: delete that database first'); continue }
      const r = fromConfigMap({ metadata: { name: `db-${name}`, creationTimestamp: h.metadata.creationTimestamp }, data: h.data }, d.dbNs)
      const root = r.root ? r.name : r.parent
      const login = await loginOf(d, root)
      if (r.root ? login.state === 'marked' && login.tenant !== r.tenant && liveOf().some((x) => x.root && x.name === root) : !loginIsTenants(login, root, r.tenant, liveOf())) {
        refuse(`its root's login db-${root}-login is now another database's: bring ${root} back first`)
        continue
      }
      const stored = await d.storcon('GET', `/v1/tenant/${r.tenant}/timeline`)
      if (stored.status === 404 || (stored.status === 200 && !(stored.json as any[]).some((x) => x.timeline_id === r.timeline))) { refuse('its timeline is no longer in the store'); continue }
      ok(stored, `list the timelines of ${r.tenant}`)
      if (r.port > 0 && liveOf().some((x) => x.port === r.port)) { refuse(`its port ${r.port} is another database's now`); continue }
      if (timeline === undefined && held.length > 1) d.log?.(`records copy: ${name}: the newest of ${held.length} held deletions (${r.timeline}); name another with ${name}@<timeline>`)
      if (opts.dry) { out.restored.push(`${name}@${r.timeline}`); continue }
      if (r.port > 0 && !(await reservePort(d, name, r.timeline, r.port))) { refuse(`its port ${r.port} is claimed by another database`); continue }
      // its object first, marked as being copied (the loops leave it alone): the namespace must be there, and nothing is released before it is
      const from = await directParent(d, r, liveOf(), new Map())
      if (from.guessed) out.parentGuessed.push(name)
      made = await d.kube('POST', `${DB_API}/namespaces/${r.namespace}/databases`, { apiVersion: `${GROUP}/v1alpha1`, kind: 'Database', metadata: { name, namespace: r.namespace, finalizers: [FINALIZER], annotations: { [COPYING]: 'true' } }, spec: { pgVersion: r.pg, ...(r.root ? {} : r.mode === 'replica' ? { replicaOf: from.name } : { parent: from.name }) } })
      if (made.status === 404) { made = null; refuse(`its namespace ${r.namespace} is gone: make it again first`); continue }
      ok(made, `bring back ${name}`)
      r.from = from.name
      if (r.root) await ensureLogin(d, name, r.tenant, () => randomBytes(12).toString('hex'), true)
      await writeRecord(d, r)
      recorded = true
      ok(await d.kube('DELETE', `${cms(d)}/${h.metadata.name}`), `release ${h.metadata.name}`, [404])
      restored.push(r)
      out.restored.push(`${name}@${r.timeline}`)
    } catch (e) {
      // a restore that failed after its object was made takes back what it wrote: the record (its held data is still held) and the object
      if (made) {
        if (recorded) await d.kube('DELETE', `${cms(d)}/db-${name}`).catch(() => {})
        const uid = made.json?.metadata?.uid
        await d.kube('PATCH', dbObjectPath(made.json?.metadata?.namespace ?? d.dbNs, name), { metadata: { finalizers: null } }, 'application/merge-patch+json').catch(() => {})
        await d.kube('DELETE', dbObjectPath(made.json?.metadata?.namespace ?? d.dbNs, name), uid ? { preconditions: { uid } } : undefined).catch(() => {})
      }
      refuse(String(e).slice(0, 300))
    }
  }
  if (!opts.dry && out.restored.length) objs = await objects()
  if (!opts.dry && out.restored.length) recs = await readRecords(d)
  const all = [...recs.values()].map((x) => x.db)
  const marks = (ok(await d.kube('GET', `${cms(d)}?labelSelector=peat-recovered%3Dtrue`), 'list recoveries').json.items as any[])
  const recoveredTo = new Map(marks.filter((m) => m.data?.from && m.data?.to).map((m) => [String(m.data.from), String(m.data.to)]))
  out.records = recs.size
  const timelinesOf = new Map<string, Promise<Set<string> | null>>()
  const storageHas = async (tenant: string, tl: string) => {
    if (!timelinesOf.has(tenant)) timelinesOf.set(tenant, d.storcon('GET', `/v1/tenant/${tenant}/timeline`).then((x) => x.status === 200 ? new Set((x.json as any[]).map((t) => String(t.timeline_id))) : x.status === 404 ? new Set<string>() : null))
    const got = await timelinesOf.get(tenant)!
    return got === null ? null : got.has(tl)
  }
  for (const { db: r, cm } of recs.values()) {
    try {
      let o = objs.find((x) => x.metadata.name === r.name)
      if (o && o.metadata.namespace !== r.namespace) { out.differ.push({ name: r.name, fields: ['namespace'] }); continue }
      if (!o || o.metadata.annotations?.[COPYING]) {
        if (opts.dry) { out.missing.push(r.name); continue }
        if (!o) {
          const from = await directParent(d, r, all, recoveredTo)
          if (from.guessed) out.parentGuessed.push(r.name)
          const spec = { pgVersion: r.pg, ...(r.root ? {} : r.mode === 'replica' ? { replicaOf: from.name } : { parent: from.name }) }
          ok(await d.kube('POST', `${DB_API}/namespaces/${r.namespace}/databases`, { apiVersion: `${GROUP}/v1alpha1`, kind: 'Database', metadata: { name: r.name, namespace: r.namespace, finalizers: [FINALIZER], annotations: { [COPYING]: 'true' } }, spec }), `copy ${r.name}`, [409])
          r.from = from.name
          out.copied.push(r.name)
        } else out.finished.push(r.name)
        await save(d, r, { phase: 'Ready', message: 'copied from the records' })
        ok(await d.kube('PATCH', dbObjectPath(r.namespace, r.name), { metadata: { annotations: { [COPYING]: null } } }, 'application/merge-patch+json'), `release ${r.name}`)
        if (cm.metadata?.labels?.[SUPERSEDED_LABEL] !== 'true') ok(await d.kube('PATCH', `${cms(d)}/${cm.metadata.name}`, { metadata: { labels: { [SUPERSEDED_LABEL]: 'true' } } }, 'application/merge-patch+json'), `mark ${cm.metadata.name} copied`)
        objs = await objects()
        o = objs.find((x) => x.metadata.name === r.name)
      }
      const s = o ? fromStatus(o) : null
      if (!s) { out.differ.push({ name: r.name, fields: ['status'] }); continue }
      const fields: string[] = (['tenant', 'timeline', 'parent', 'root', 'mode', 'port', 'pg', 'created'] as const).filter((f) => String(s[f]) !== String(r[f]))
      // beyond the object (which a copy writes from the record): the storage holds the timeline, and the root's login is this tenant's
      const has = await storageHas(r.tenant, r.timeline)
      if (has === false) fields.push('storage')
      const root = r.root ? r.name : r.parent
      const login = await loginOf(d, root)
      if (!loginIsTenants(login, root, r.tenant, all)) fields.push('login')
      else if (login.state === 'unmarked' && r.root && !opts.dry) ok(await d.kube('PATCH', `${secrets(d)}/${loginName(root)}`, { metadata: { annotations: { [TENANT_ANNOTATION]: r.tenant } } }, 'application/merge-patch+json'), `mark ${loginName(root)}`)
      if (fields.length) out.differ.push({ name: r.name, fields })
      else out.equal.push(r.name)
    } catch (e) { out.refused.push({ name: r.name, why: String(e).slice(0, 300) }) }
  }
  out.databases = (await objects()).filter((o) => fromStatus(o)).length
  out.countsEqual = out.equal.length === out.records && out.differ.length === 0 && out.missing.length === 0 && out.refused.length === 0 && out.databases === out.records
  d.log?.(`records copy${opts.dry ? ' (dry)' : ''}: ${out.records} records, ${out.copied.length} copied, ${out.finished.length} finished, ${out.equal.length} equal, ${out.differ.length} differ, ${out.missing.length} missing, ${out.databases} Database records`)
  return out
}
