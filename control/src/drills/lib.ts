// What the drills of Peat share: the analysis of a writer's and a reader's logs, the check that the platform is in a known good state, the
// snapshot of what must not move, and the clients (a writer and a reader pod) that work a database through the FRONT DOOR with TLS. Nothing
// here builds a command for a machine or the cluster: every read and every change goes through `guard.ts`.
//
// The clients are pods named `drill-*` in the computes' namespace, on the platform's machines, with a policy of their own (`drill-client`:
// DNS and the gateways' 5432 only). They connect to `<database>.<databases' namespace>.svc` with `sslmode=verify-full` against the platform
// CA's certificate (a ConfigMap `drill-ca` holding the PUBLIC half, read from the CA's Secret), as any client of the platform would.
import { durationMs } from '../api/schema'
import { DRILL_BASE_DATABASE, Guard, egressReachesStore, namespacesOf } from './guard'
import { placement, type Names } from '../names'

/** The drills' database: 200 MB of `filler` (a heap scan reads every page), a counter table the writer commits into, and a bulk table for the alert drills. Made by `setup`. */
export const DB = DRILL_BASE_DATABASE
export const FILLER_ROWS = 200_000
export const FILLER_BYTES = 1000
/** The reader's statement: a HEAP scan of the whole table (200 MB, more than the compute's shared buffers), so that it needs the pageserver for pages. `select count(*)` of a table with a primary key is an index-only scan of a few MB that the compute keeps in memory, so a reader that counts never asks the pageserver, and a drill that cuts one would measure nothing. */
export const READER_SQL = 'select sum(length(payload)) from filler'
export const READER_EXPECT = String(FILLER_ROWS * FILLER_BYTES)

// ---- analysis (pure)
export interface WriterSummary {
  ackedTotal: number; ackedBeforeFault: number; missingAcked: number; missingFirst: number[]
  longestGapS: number; gapBeganMsAfterFault: number; firstOkAfterGapEndedMsAfterFault: number
  errsFromFault: number; errMessages: [string, number][]; duplicates: number[]
}
/** `ok <start> <end> <n>|<ts>` and `err <start> <end> <message>` lines, times in ms on the writer's own clock; `rows` is the table's `n` column, one per line. */
export function analyzeWriter(log: string, rows: string, fault: number): WriterSummary {
  const ok: [number, number, number][] = [], err: [number, number, string][] = []
  for (const l of log.split('\n')) {
    const p = l.trim().split(/\s+/)
    if (p[0] === 'ok' && p.length >= 4 && p[3]!.includes('|')) ok.push([Number(p[1]), Number(p[2]), Number(p[3]!.split('|')[0])])
    else if (p[0] === 'err') err.push([Number(p[1]), Number(p[2]), p.slice(3).join(' ')])
  }
  const table = new Set(rows.split(/\s+/).filter(Boolean).map(Number))
  const acked = new Set(ok.map((o) => o[2]))
  const missing = [...acked].filter((n) => !table.has(n)).sort((a, b) => a - b)
  const before = ok.filter((o) => o[1] < fault - 3000)
  const win = ok.filter((o) => o[1] >= fault - 3000)
  // the last answer BEFORE the window belongs to it, or a gap that begins before the window loses its first part
  if (before.length) win.unshift(before[before.length - 1]!)
  let worst: [number, number, number, number] = [0, 0, 0, 0]
  for (let i = 0; i + 1 < win.length; i++) {
    const g: [number, number, number, number] = [win[i + 1]![1] - win[i]![1], win[i]![1], win[i + 1]![0], win[i + 1]![1]]
    if (g[0] > worst[0]) worst = g
  }
  const msgs = new Map<string, number>()
  for (const [, e, m] of err) if (e >= fault - 1000) msgs.set(m.slice(0, 140), (msgs.get(m.slice(0, 140)) ?? 0) + 1)
  const counts = new Map<number, number>()
  for (const o of ok) counts.set(o[2], (counts.get(o[2]) ?? 0) + 1)
  return {
    ackedTotal: ok.length, ackedBeforeFault: ok.filter((o) => o[1] < fault).length, missingAcked: missing.length, missingFirst: missing.slice(0, 5),
    longestGapS: Math.round(worst[0] / 100) / 10, gapBeganMsAfterFault: worst[1] - fault, firstOkAfterGapEndedMsAfterFault: worst[3] - fault,
    errsFromFault: [...msgs.values()].reduce((a, b) => a + b, 0), errMessages: [...msgs.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4),
    duplicates: [...counts.entries()].filter(([, c]) => c > 1).map(([n]) => n).slice(0, 3),
  }
}
/** The drill's first result line. A table that could not be read is not a table with rows missing: with nothing read, no acknowledged row can be called missing (found 2026-11-27: a drill whose own SQL client died with the machine it cut reported every acknowledged row missing, while the database was readable throughout). */
export function ackedRowsResult(w: WriterSummary, rows: string | null): { value: string; note: string } {
  if (rows === null) return { value: `${w.ackedBeforeFault} / NOT CHECKED`, note: 'THE TABLE COULD NOT BE READ, so whether any acknowledged row is missing is unknown (read it from outside the drill)' }
  return { value: `${w.ackedBeforeFault} / ${w.missingAcked}`, note: w.missingAcked ? `FIRST MISSING ${w.missingFirst.join(',')}` : '' }
}
/** What to do about the drill's SQL client pod: make it, keep it, or make it again. It is made again when it is not Running and Ready (a bare Pod on a machine the drill cut never runs again), or when it sits on another machine than the one the drill chose for its clients (`home`, a machine the drill does not cut). */
export function sqlClientAction(pod: { node: string; ready: boolean } | undefined, home: string | undefined): 'create' | 'keep' | 'recreate' {
  if (!pod) return 'create'
  if (!pod.ready || (home !== undefined && pod.node !== home)) return 'recreate'
  return 'keep'
}
export interface ReaderSummary { answers: number; longestGapS: number; gapBeganMsAfterFault: number; gapEndedMsAfterFault: number; errsFromFault: number; errMessages: [string, number][]; wrongCounts: number }
export function analyzeReader(log: string, fault: number, expect = READER_EXPECT): ReaderSummary {
  const ok: [number, number, string][] = [], err: [number, number, string][] = []
  for (const l of log.split('\n')) {
    const p = l.trim().split(/\s+/)
    if (p[0] === 'ok' && p.length >= 4) ok.push([Number(p[1]), Number(p[2]), p[3]!])
    else if (p[0] === 'err') err.push([Number(p[1]), Number(p[2]), p.slice(3).join(' ')])
  }
  const before = ok.filter((o) => o[1] < fault - 3000)
  const win = ok.filter((o) => o[1] >= fault - 3000)
  if (before.length) win.unshift(before[before.length - 1]!)
  let w: [number, number, number] = [0, 0, 0]
  for (let i = 0; i + 1 < win.length; i++) { const g: [number, number, number] = [win[i + 1]![1] - win[i]![1], win[i]![1], win[i + 1]![1]]; if (g[0] > w[0]) w = g }
  const msgs = new Map<string, number>()
  for (const [, e, m] of err) if (e >= fault - 1000) msgs.set(m.slice(0, 140), (msgs.get(m.slice(0, 140)) ?? 0) + 1)
  return {
    answers: ok.length, longestGapS: Math.round(w[0] / 100) / 10, gapBeganMsAfterFault: w[1] - fault, gapEndedMsAfterFault: w[2] - fault,
    errsFromFault: [...msgs.values()].reduce((a, b) => a + b, 0), errMessages: [...msgs.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4), wrongCounts: ok.filter((o) => o[2] !== expect).length,
  }
}

// ---- the snapshot around each drill (what must not move)

export interface PodRow { ns: string; name: string; uid: string; node: string; restarts: number; job: boolean }
export interface Snapshot { at: string; pods: PodRow[]; firing: string[]; availableGiB: number | null }
/**
 * What moved between two snapshots: pods changed (another pod of the name, a restart, another machine), added and removed (a Job's pods are left out), and of those the ones of the namespaces a drill must leave
 * alone: `watched` (the install's list), or, when it gives none, every namespace that is not one of `platform` or kube-system.
 */
export function diffSnapshots(a: Snapshot, b: Snapshot, o: { platform: readonly string[]; watched?: readonly string[] | null }): { changed: string[]; added: string[]; removed: string[]; flagged: string[] } {
  const A = new Map(a.pods.map((p) => [`${p.ns}/${p.name}`, p])), B = new Map(b.pods.map((p) => [`${p.ns}/${p.name}`, p]))
  const changed: string[] = [], added: string[] = [], removed: string[] = []
  for (const [k, p] of A) {
    const q = B.get(k)
    if (!q) { if (!p.job) removed.push(k); continue }
    if (q.uid !== p.uid) changed.push(`${k}: a different pod (uid ${p.uid.slice(0, 8)} to ${q.uid.slice(0, 8)})`)
    else if (q.restarts > p.restarts) changed.push(`${k}: restarts ${p.restarts} to ${q.restarts}`)
    else if (q.node !== p.node) changed.push(`${k}: node ${p.node} to ${q.node}`)
  }
  for (const [k, q] of B) if (!A.has(k) && !q.job) added.push(k)
  const ns = (l: string) => l.split(':')[0]!.split('/')[0]!
  const watched = (n: string) => (o.watched ? o.watched.includes(n) : !o.platform.includes(n) && n !== 'kube-system')
  const flagged = [...changed, ...added, ...removed].filter((l) => watched(ns(l)))
  return { changed, added, removed, flagged }
}

// ---- the context a drill runs in

export interface Placement { attached: number; secondary: number[] }
export interface PodInfo { name: string; node: string; ip: string; ready: boolean }
export interface Ctx {
  readonly g: Guard
  readonly name: string
  readonly runNo: number
  note(line: string): void
  notes: string[]
  sleep(seconds: number): Promise<void>
  /** The known-good-state check: every platform pod Ready, every workload at its count, the controller's nodes Active, a write through the front door, each safekeeper and the pageserver reachable. */
  state(): Promise<{ ok: boolean; lines: string[] }>
  waitState(maxSeconds: number): Promise<boolean>
  snapshot(): Promise<Snapshot>
  dbInfo(db: string): Promise<{ tenant: string; timeline: string }>
  placement(tenant: string): Promise<Placement>
  pods(ns: string, selector: string): Promise<PodInfo[]>
  controlLeader(): Promise<{ pod: string; node: string }>
  /** The safekeepers the controller lists as Active, each with the machine its pod is on: the ids are whatever the control layer has made them (a replaced member has a new id). */
  safekeepers(): Promise<{ id: number; node: string }[]>
  createDb(db: string): Promise<void>
  /** A short checkpoint timeout on one drill database's tenant, so a write is flushed and uploaded within about a minute. */
  setCheckpoint(db: string, seconds: number): Promise<void>
  /** A client pod `drill-*` running `command` as the database's `app` login, with the environment overridden by `env` (PGHOSTADDR to pick a gateway, PGSSLMODE and PGHOST to dial a compute's address). */
  startClientPod(name: string, node: string, db: string, command: string, env?: Record<string, string>): Promise<void>
  startWriter(node: string, db?: string): Promise<void>
  startReader(node: string, db?: string): Promise<void>
  stopClients(): Promise<void>
  /** The writer pod's clock in ms: every time in the clients' logs and every fault time of a drill is on this clock. */
  clientClock(): Promise<number>
  clientLog(pod: string): Promise<string>
  /** The machine the SQL client pod `drill-sql` runs on from now on: one the drill does not cut. A pod already running elsewhere is made again there at the next statement. */
  sqlClientOn(node: string): void
  /** One SQL statement against a database through the front door (a client pod `drill-sql` runs psql, made again when it is not Running or not where `sqlClientOn` said). */
  sql(db: string, text: string, seconds?: number): Promise<string>
  /** Waits until a database answers a query through the front door, up to `seconds`; the table's rows are read this way. */
  sqlRetry(db: string, text: string, seconds: number): Promise<string | null>
  /** Has the attached pageserver uploaded everything of the database's timeline (remote_consistent_lsn = last_record_lsn)? */
  flushed(db: string): Promise<boolean>
  waitReady(ns: string, selector: string, seconds: number): Promise<boolean>
  /** Alerts firing or pending now: names with their state. */
  alerts(): Promise<{ name: string; state: string }[]>
  /** The drill's own result rows, written to its record. */
  record(what: string, value: string, note?: string): void
  results: { what: string; value: string; note: string }[]
}


// ---- the live context

const clientPolicy = (n: Names) => ({
  apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy', metadata: { name: 'drill-client', namespace: n.computeNamespace },
  spec: {
    podSelector: { matchLabels: { app: 'drill-client' } }, policyTypes: ['Ingress', 'Egress'], ingress: [],
    egress: [
      { to: [{ namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': 'kube-system' } }, podSelector: { matchLabels: { 'k8s-app': 'kube-dns' } } }], ports: [{ protocol: 'UDP', port: 53 }, { protocol: 'TCP', port: 53 }] },
      { to: [{ namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': n.gatewayNamespace } }, podSelector: { matchLabels: { app: 'peat-gateway' } } }], ports: [{ protocol: 'TCP', port: 5432 }] },
    ],
  },
})
const loop = (label: string, sql: string, timeout: number) => `while true; do t=$(date +%s%3N); out=$(timeout ${timeout} psql -qAtX -c "${sql}" 2>&1); rc=$?; t2=$(date +%s%3N); if [ $rc -eq 0 ]; then echo "ok $t $t2 $out"; else echo "err $t $t2 rc=$rc $(echo $out | tr '\\n' ' ' | cut -c1-200)"; fi; sleep 1; done`
function clientPod(n: Names, image: string, name: string, node: string, db: string, password: string, command: string, extra: Record<string, string> = {}): any {
  const p = placement(n)
  return {
    apiVersion: 'v1', kind: 'Pod', metadata: { name, namespace: n.computeNamespace, labels: { app: 'drill-client' } },
    spec: {
      restartPolicy: 'Never', terminationGracePeriodSeconds: 1, automountServiceAccountToken: false, enableServiceLinks: false,
      nodeSelector: { ...p.nodeSelector, 'kubernetes.io/hostname': node },
      tolerations: p.tolerations,
      containers: [{
        name: 'client', image, command: ['sh', '-c', command],
        env: Object.entries({
          PGHOST: `${db}.${n.dbNamespace}.svc`, PGUSER: 'app', PGDATABASE: 'app', PGSSLMODE: 'verify-full', PGSSLROOTCERT: '/ca/ca.crt', PGCONNECT_TIMEOUT: '10', PGPASSWORD: password, ...extra,
        }).map(([name, value]) => ({ name, value })),
        resources: { requests: { cpu: '10m', memory: '32Mi' }, limits: { memory: '256Mi' } },
        volumeMounts: [{ name: 'ca', mountPath: '/ca', readOnly: true }],
      }],
      volumes: [{ name: 'ca', configMap: { name: 'drill-ca' } }],
    },
  }
}

export class LiveCtx implements Ctx {
  notes: string[] = []
  results: { what: string; value: string; note: string }[] = []
  /** The machine the drill's clients run on (the first client pod's): the SQL client goes there too, never to the acting control layer's machine, which a drill may be cutting (found 2026-11-20: a table could not be read for 5 minutes because its client waited for the cut machine). */
  private clientHome: string | undefined
  constructor(readonly g: Guard, readonly name: string, readonly runNo: number) {}
  private get n() { return this.g.names }
  note(line: string) { this.notes.push(line); this.g.log(`note: ${line}`) }
  record(what: string, value: string, note = '') { this.results.push({ what, value, note }); this.g.log(`RESULT ${this.name} run ${this.runNo}: ${what} = ${value} ${note}`) }
  sleep(seconds: number) { return this.g.sleep(seconds * 1000) }

  async dbInfo(db: string) { const r = await this.g.control('GET', `/db/${db}`); if (r.status !== 200) throw new Error(`no database ${db}: ${r.status}`); return { tenant: r.json.tenant as string, timeline: r.json.timeline as string } }
  async placement(tenant: string): Promise<Placement> {
    const j = await this.g.storage('admin', `http://storage-controller.${this.n.namespace}.svc:1234/control/v1/tenant/${tenant}`)
    const s = j.shards[0]
    return { attached: Number(s.node_attached), secondary: (s.node_secondary ?? []).map(Number) }
  }
  async pods(ns: string, selector: string): Promise<PodInfo[]> {
    const j = await this.g.kjson<any>(['-n', ns, 'get', 'pods', '-l', selector])
    return (j.items as any[]).filter((p) => !p.metadata.deletionTimestamp).map((p) => ({ name: p.metadata.name, node: p.spec.nodeName ?? '', ip: p.status.podIP ?? '', ready: (p.status.conditions ?? []).some((c: any) => c.type === 'Ready' && c.status === 'True') }))
  }
  async controlLeader() {
    const lease = await this.g.kjson<any>(['-n', this.n.namespace, 'get', 'lease', 'peat-control-leader'])
    const pod = lease.spec.holderIdentity as string
    const p = await this.g.kjson<any>(['-n', this.n.namespace, 'get', 'pod', pod])
    return { pod, node: p.spec.nodeName as string }
  }
  async setCheckpoint(db: string, seconds: number) { await this.g.setTenantCheckpoint(db, seconds) }
  async safekeepers() {
    const list = (await this.g.storage('admin', `http://storage-controller.${this.n.namespace}.svc:1234/control/v1/safekeeper`)) as any[]
    const pods = await this.pods(this.n.namespace, 'app=safekeeper')
    return list.filter((x) => x.scheduling_policy === 'Active').map((x) => ({ id: Number(x.id), node: pods.find((p) => p.name === `safekeeper-w${x.id}-0`)?.node ?? '' })).filter((x) => x.node)
  }
  async createDb(db: string) {
    const r = await this.g.control('POST', '/db', { name: db })
    if (r.status !== 200) throw new Error(`create ${db}: ${r.status} ${JSON.stringify(r.json).slice(0, 200)}`)
  }
  private async password(db: string): Promise<string> {
    const ns = this.n.namespace
    const j = await this.g.kjson<any>(['-n', ns, 'get', 'secret', `db-${db}-login`]).catch(async (e) => {
      // a branch has no login of its own: it carries its parent's (the control layer's record names the parent)
      const parent = ((await this.g.control('GET', `/db/${db}`)).json as { parent?: string } | null)?.parent
      if (!parent) throw e
      return this.g.kjson<any>(['-n', ns, 'get', 'secret', `db-${parent}-login`])
    })
    return Buffer.from(j.data.password, 'base64').toString()
  }
  /** The public half of the platform CA, as a ConfigMap the clients mount, copied from the one the platform publishes in the database namespace (which holds no key). */
  private async ensureCa() {
    const cns = this.n.computeNamespace
    // looked for each time, not remembered: the undo log deletes it at the end of a drill, and the check that follows a drill needs a client again (found 2026-11-19: a remembered flag left a pod waiting for a deleted ConfigMap)
    const have = await this.g.kread(['-n', cns, 'get', 'configmap', 'drill-ca', '-o', 'name']).then(() => true).catch(() => false)
    if (have && (await this.g.kread(['-n', cns, 'get', 'networkpolicy', 'drill-client', '-o', 'name']).then(() => true).catch(() => false))) return
    const j = await this.g.kjson<any>(['-n', this.n.dbNamespace, 'get', 'configmap', this.g.settings.caConfigMap])
    await this.g.apply(cns, { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'drill-ca', namespace: cns }, data: { 'ca.crt': String(j.data['ca.crt']) } })
    await this.g.apply(cns, clientPolicy(this.n))
  }
  async startClientPod(pod: string, node: string, db: string, command: string, env: Record<string, string> = {}) {
    await this.ensureCa()
    if (pod !== 'drill-sql' && !this.clientHome) this.clientHome = node
    const image = (await this.g.platform()).computeImage
    if (!image) throw new Error('the compute image could not be read from the control layer (COMPUTE_IMAGE): set PEAT_DRILL_CLIENT_IMAGE')
    await this.g.apply(this.n.computeNamespace, clientPod(this.n, image, pod, node, db, await this.password(db), command, env))
    for (let i = 0; i < 60; i++) { const p = (await this.pods(this.n.computeNamespace, 'app=drill-client')).find((x) => x.name === pod); if (p?.ready) return; await this.g.sleep(2000) }
    throw new Error(`${pod} did not start`)
  }
  async startWriter(node: string, db = DB) { await this.startClientPod('drill-writer', node, db, loop('writer', 'insert into proof_rows default values returning n, ts', 40)) }
  async startReader(node: string, db = DB) { await this.startClientPod('drill-reader', node, db, loop('reader', READER_SQL, 60)) }
  async stopClients() { for (const p of ['drill-writer', 'drill-reader', 'drill-sql', 'drill-session', 'drill-fence-old', 'drill-fence-new']) await this.g.deleteObject(this.n.computeNamespace, 'Pod', p) }
  async clientClock() { return Number((await this.g.clientClock('drill-writer')).trim()) }
  async clientLog(pod: string) { return this.g.kread(['-n', this.n.computeNamespace, 'logs', pod]) }
  sqlClientOn(node: string) { this.clientHome = node }
  async sql(db: string, text: string, seconds = 60) {
    const sqlPod = (await this.pods(this.n.computeNamespace, 'app=drill-client')).find((p) => p.name === 'drill-sql')
    const action = sqlClientAction(sqlPod, this.clientHome)
    if (action !== 'keep') {
      // forced, so that a pod left on a machine that is down does not keep the name
      if (action === 'recreate') await this.g.killPod(this.n.computeNamespace, 'drill-sql')
      await this.startClientPod('drill-sql', this.clientHome ?? (await this.controlLeader()).node, db, 'sleep 86400')
    }
    return this.g.clientPsql('drill-sql', { host: `${db}.${this.n.dbNamespace}.svc`, password: await this.password(db), seconds }, text)
  }
  async sqlRetry(db: string, text: string, seconds: number) {
    const end = this.g.now() + seconds * 1000
    for (;;) { try { return await this.sql(db, text, 20) } catch { if (this.g.now() > end) return null; await this.g.sleep(5000) } }
  }
  async flushed(db: string) {
    const { tenant, timeline } = await this.dbInfo(db)
    const p = await this.placement(tenant)
    const j = await this.g.storage('pageserverapi', `http://pageserver-w${p.attached}-0.pageserver.${this.n.namespace}.svc:9898/v1/tenant/${tenant}/timeline/${timeline}`)
    return j.remote_consistent_lsn === j.last_record_lsn
  }
  async waitReady(ns: string, selector: string, seconds: number) {
    const end = this.g.now() + seconds * 1000
    for (;;) { const p = await this.pods(ns, selector).catch(() => []); if (p.length > 0 && p.every((x) => x.ready)) return true; if (this.g.now() > end) return false; await this.g.sleep(3000) }
  }
  async alerts() {
    if (!this.g.settings.prometheus) return []
    const j = await this.g.monitoringGet('prometheus', '/api/v1/alerts')
    return (j.data.alerts as any[]).map((a) => ({ name: a.labels.alertname as string, state: a.state as string }))
  }
  async snapshot(): Promise<Snapshot> {
    const j = await this.g.kjson<any>(['get', 'pods', '-A'])
    const platform = namespacesOf(this.n)
    const pods: PodRow[] = (j.items as any[]).filter((p) => !platform.includes(p.metadata.namespace)).map((p) => ({
      ns: p.metadata.namespace, name: p.metadata.name, uid: p.metadata.uid, node: p.spec.nodeName ?? '', job: (p.metadata.ownerReferences ?? []).some((o: any) => o.kind === 'Job'),
      restarts: (p.status.containerStatuses ?? []).reduce((a: number, c: any) => a + c.restartCount, 0),
    }))
    const al = await this.alerts().catch(() => [])
    return { at: new Date(this.g.now()).toISOString(), pods, firing: al.filter((a) => a.state === 'firing').map((a) => a.name).sort(), availableGiB: null }
  }
  async state() {
    const lines: string[] = []
    let ok = true
    const ns = this.n.namespace
    const cdb = this.g.settings.controllerDatabase
    const pods = await this.g.kjson<any>(['-n', ns, 'get', 'pods'])
    const skip = (name: string) => name.startsWith('drill-') || (cdb !== null && /-(prep|root)/.test(name) && name.startsWith(cdb.cluster))
    const odd = (pods.items as any[]).filter((p) => !skip(p.metadata.name) && p.status.phase !== 'Succeeded').filter((p) => p.status.phase !== 'Running' || !(p.status.conditions ?? []).some((c: any) => c.type === 'Ready' && c.status === 'True') || p.metadata.deletionTimestamp).map((p) => p.metadata.name)
    lines.push(odd.length ? `pods NOT ready: ${odd.join(' ')}` : 'pods: all Ready'); if (odd.length) ok = false
    // what a drill leaves behind and a pod check cannot see: a storage policy without its rule to the store, a workload held at zero, an environment still patched
    for (const name of ['pageserver', 'safekeeper']) {
      const p = await this.g.kjson<any>(['-n', ns, 'get', 'networkpolicy', name]).catch(() => null)
      if (p && !egressReachesStore(p.spec.egress)) { lines.push(`networkpolicy ${name}: its egress has no rule to the store (a store drill's leftover: recover)`); ok = false }
    }
    for (const kind of ['statefulsets', 'deployments']) {
      const list = await this.g.kjson<any>(['-n', ns, 'get', kind]).catch(() => ({ items: [] }))
      for (const w of (list.items ?? []) as any[]) {
        const want = Number(w.spec?.replicas ?? 1), ready = Number(w.status?.readyReplicas ?? 0)
        if (want === 0) { lines.push(`${kind.slice(0, -1)} ${w.metadata.name} is held at zero (an interrupted drill?)`); ok = false }
        else if (ready < want) { lines.push(`${kind.slice(0, -1)} ${w.metadata.name}: ${ready} of ${want} ready`); ok = false }
      }
    }
    const env = async (n: string, name: string, key: string) => (await this.g.kjson<any>(['-n', n, 'get', 'deployment', name]).catch(() => null))?.spec?.template?.spec?.containers?.[0]?.env?.find((e: any) => e.name === key)
    const want = String(Math.max(1, Math.round(durationMs(this.g.settings.idleAfter) / 60_000)))
    const idle = [await env(ns, 'peat-control', 'IDLE_MINUTES'), await env(this.n.gatewayNamespace, 'peat-gateway', 'IDLE_MINUTES')]
    if (idle.some((e) => e !== undefined && e?.value !== want)) { lines.push(`IDLE_MINUTES is not ${want} on the control layer and the gateways (a sleep drill's leftover: recover)`); ok = false }
    if (cdb) {
      const dbUrl = await env(ns, 'storage-controller', 'DATABASE_URL')
      if (dbUrl?.valueFrom?.secretKeyRef?.name !== `${cdb.cluster}-app`) { lines.push(`the storage controller does not read ${cdb.cluster}-app (a restore drill's leftover: recover)`); ok = false }
    }
    const gw = await this.pods(this.n.gatewayNamespace, 'app=peat-gateway')
    lines.push(`gateways: ${gw.map((g) => `${g.name}${g.ready ? '' : ' NOT READY'}`).join(' ')}`); if (gw.length < 1 || gw.some((g) => !g.ready)) ok = false
    try {
      const nodes = await this.g.storage('admin', `http://storage-controller.${ns}.svc:1234/control/v1/node`)
      lines.push(`storage controller nodes: ${(nodes as any[]).map((n) => `${n.id}:${n.availability}/${n.scheduling}`).join(' ')}`)
      if ((nodes as any[]).some((n) => n.availability !== 'Active' || n.scheduling !== 'Active')) ok = false
    } catch (e) { lines.push(`storage controller: UNREACHABLE (${e})`); ok = false }
    try {
      const { tenant, timeline } = await this.dbInfo(DB)
      const pl = await this.placement(tenant)
      lines.push(`tenant ${DB} attached ${pl.attached}, secondary ${pl.secondary.join(',') || 'none'}`); if (pl.secondary.length === 0) ok = false
      // the safekeepers are the ones the controller lists as Active (a replaced one has a new id)
      const sks = await this.g.storage('admin', `http://storage-controller.${ns}.svc:1234/control/v1/safekeeper`).catch(() => [] as any[])
      const active = (sks as any[]).filter((x) => x.scheduling_policy === 'Active')
      lines.push(`safekeepers Active in the controller: ${active.map((x) => x.id).join(',') || 'none'}`); if (active.length < 3) ok = false
      for (const x of active) {
        try { const t = await this.g.storage('safekeeperdata', `http://safekeeper-w${x.id}-0.safekeeper.${ns}.svc:7676/v1/tenant/${tenant}/timeline/${timeline}`); lines.push(`safekeeper ${x.id}: commit ${t.commit_lsn} remote_consistent ${t.remote_consistent_lsn}`) } catch { lines.push(`safekeeper ${x.id}: unreachable or not a member of this timeline`) }
      }
      const w = await this.sqlRetry(DB, 'insert into proof_rows default values returning n', 60)
      lines.push(w === null ? 'write through the front door: FAILED' : 'write through the front door: ok'); if (w === null) ok = false
    } catch (e) { lines.push(`database ${DB}: ${e}`); ok = false }
    return { ok, lines }
  }
  async waitState(maxSeconds: number) {
    const end = this.g.now() + maxSeconds * 1000
    for (;;) { const s = await this.state().catch(() => ({ ok: false, lines: [] as string[] })); if (s.ok) return true; if (this.g.now() > end) return false; await this.g.sleep(10_000) }
  }
}
