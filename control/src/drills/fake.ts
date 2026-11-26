// A fake world for the drills: what `plan` prints and what the tests run every drill against. The drill's own code runs unchanged against
// `FakeCtx`, whose reads are canned and whose clock jumps, with a REAL Guard in dry mode: every cutting step the drill takes is judged by
// guard.ts's own rules and recorded, and nothing reaches a cluster. `PlanExec` answers the reads the guard and the drills make directly: three
// platform machines, `machine-1` to `machine-3`, a pageserver on each of the first two, the storage controller on the third.
import { DEFAULT_NAMES } from '../names'
import { Guard, drillSettings, type Clock, type DrillSettings, type Exec, type Result } from './guard'
import { DB, type Ctx, type Placement, type PodInfo, type Snapshot } from './lib'

const TENANT = 'a'.repeat(32), TIMELINE = 'b'.repeat(32)
const ok = (out: string): Result => ({ code: 0, out, err: '' })
export const FAKE_MACHINES = ['machine-1', 'machine-2', 'machine-3'] as const
const LABEL = DEFAULT_NAMES.machineLabel
const node = (name: string) => ({ metadata: { name, labels: { [LABEL.key]: LABEL.value } }, status: { conditions: [{ type: 'Ready', status: 'True' }] } })
const pod = (name: string, app: string, machine: string) => ({ metadata: { name, labels: { app } }, spec: { nodeName: machine, containers: [{ image: 'registry.example/peat/storage:fake' }] }, status: { phase: 'Running', podIP: '192.0.2.20' } })

/** The settings of the fake world: the installer's command (an example of one), an Alertmanager and a Prometheus, and a controller database, so that every drill has what it needs. No quiet hours. */
export function fakeSettings(over: Partial<DrillSettings> = {}): DrillSettings {
  return {
    ...drillSettings({}),
    machineCommand: ['machinectl', '{action}', '{machine}'],
    alertmanager: { namespace: 'monitoring', name: 'alertmanager' },
    prometheus: { namespace: 'monitoring', service: 'prometheus:80' },
    controllerDatabase: { cluster: 'storcon-db', machine: 'machine-3', image: 'postgres:19', storageClass: 'local', objectStore: 'controller-backup', archive: 'storcon-db', database: 'storage_controller' },
    ...over,
  }
}

/** Answers what a dry run reads: nodes, the platform's pods, a NetworkPolicy, StatefulSets, the Lease, the control layer's Deployment, and the control pod's exec calls by the URL or route in their script. */
export class PlanExec implements Exec {
  readonly argvs: string[][] = []
  constructor(private readonly machines: readonly string[] = FAKE_MACHINES) {}
  async run(argv: string[]): Promise<Result> {
    this.argvs.push(argv)
    const j = argv.join(' ')
    if (argv[0] !== 'kubectl') return ok('')
    if (argv.includes('--raw') && /prometheus:80\/proxy\/api\/v1\/query/.test(j)) return ok(JSON.stringify({ data: { result: [] } }))
    if (argv.includes('--raw') && /prometheus:80\/proxy\/api\/v1\/alerts/.test(j)) return ok(JSON.stringify({ data: { alerts: [] } }))
    if (argv.includes('--raw') && /alertmanager/.test(j)) return ok('[]')
    if (/get nodes -l /.test(j)) return ok(JSON.stringify({ items: this.machines.map(node) }))
    const g = /get node (\S+)/.exec(j)
    if (g) return ok(JSON.stringify(node(g[1]!)))
    if (/get networkpolicy (pageserver|safekeeper)/.test(j)) return ok(JSON.stringify({ spec: { egress: [{ to: [{ podSelector: {} }] }, { to: [{ ipBlock: { cidr: '0.0.0.0/0' } }] }] } }))
    if (/get statefulsets -l peat-sk=true/.test(j)) return ok(JSON.stringify({ items: [{ status: { readyReplicas: 1 } }] }))
    if (/get lease/.test(j)) return ok(JSON.stringify({ spec: { holderIdentity: 'peat-control-1' } }))
    if (/get deployment peat-control/.test(j)) return ok(JSON.stringify({ spec: { template: { spec: { containers: [{ env: [{ name: 'COMPUTE_IMAGE_PG19', value: 'registry.example/peat/compute-pg19:fake' }] }] } } } }))
    if (new RegExp(`-n ${DEFAULT_NAMES.namespace} get pods -o json`).test(j)) return ok(JSON.stringify({ items: [pod('pageserver-w1-0', 'pageserver', 'machine-1'), pod('pageserver-w2-0', 'pageserver', 'machine-2'), pod('storage-controller-x', 'storage-controller', 'machine-3'), pod('safekeeper-w3-0', 'safekeeper', 'machine-3')] }))
    if (/get pods/.test(j)) return ok(JSON.stringify({ items: [] }))
    if (argv.includes('exec') && argv.includes('deploy/peat-control')) {
      const script = argv[argv.length - 1]!
      let body: unknown = {}
      if (script.includes('/control/v1/node')) body = [{ id: 1, availability: 'Active', scheduling: 'Active' }, { id: 2, availability: 'Active', scheduling: 'Active' }]
      else if (script.includes('/control/v1/tenant/')) body = { shards: [{ node_attached: 1, node_secondary: [2] }] }
      else if (script.includes('/timeline/')) body = { remote_consistent_lsn: '0/1', last_record_lsn: '0/1', commit_lsn: '0/1' }
      else if (script.includes('/v1/tenant/')) body = { state: { slug: 'Active' } }
      else if (script.includes('/sk-replace')) body = { current: null, history: [{ lost: 2, endedAt: 1 }] }
      else if (/localhost:8080\/dbs'/.test(script)) body = [{ name: DB }]
      else body = { name: DB, tenant: TENANT, timeline: TIMELINE }
      return ok(JSON.stringify({ status: 200, json: body }))
    }
    return ok('{}')
  }
  runSync(argv: string[]): Result { this.argvs.push(argv); return ok('') }
}
export class FakeClock implements Clock {
  t = Date.UTC(2026, 10, 20, 12, 0, 0)
  now() { return this.t }
  async sleep(ms: number) { this.t += ms }
}

export class FakeCtx implements Ctx {
  notes: string[] = []
  results: { what: string; value: string; note: string }[] = []
  readonly started: string[] = []
  constructor(readonly g: Guard, readonly name: string, readonly runNo = 1, private readonly computeNode = 'machine-1', private readonly gateways = ['machine-1', 'machine-2']) {}
  note(l: string) { this.notes.push(l) }
  record(what: string, value: string, note = '') { this.results.push({ what, value, note }) }
  sleep(s: number) { return this.g.sleep(s * 1000) }
  async state() { return { ok: true, lines: ['fake: all good'] } }
  async waitState() { return true }
  async snapshot(): Promise<Snapshot> { return { at: new Date(this.g.now()).toISOString(), pods: [], firing: [], availableGiB: null } }
  async dbInfo() { return { tenant: TENANT, timeline: TIMELINE } }
  async placement(): Promise<Placement> { return { attached: 1, secondary: [2] } }
  async pods(ns: string, selector: string): Promise<PodInfo[]> {
    const n = this.g.names
    if (ns === n.gatewayNamespace) return this.gateways.map((m, i) => ({ name: `peat-gateway-${i}`, node: m, ip: `192.0.2.${i + 1}`, ready: true }))
    if (ns === n.computeNamespace && /compute=/.test(selector)) return [{ name: 'compute-drill-par200-x', node: this.computeNode, ip: '192.0.2.11', ready: true }]
    if (ns === n.computeNamespace) return []
    if (selector.includes('pageserver')) return [{ name: 'pageserver-w1-0', node: 'machine-1', ip: '192.0.2.21', ready: true }, { name: 'pageserver-w2-0', node: 'machine-2', ip: '192.0.2.22', ready: true }]
    return [{ name: selector.includes('controller') ? 'storage-controller-x' : selector.includes('broker') ? 'storage-broker-x' : 'safekeeper-w1-0', node: 'machine-3', ip: '192.0.2.23', ready: true }]
  }
  async controlLeader() { return { pod: 'peat-control-1', node: 'machine-2' } }
  /** The Active members: ids 3, 5 and 6 on the platform's machines; for the recovery drill the live set (one on a machine outside the platform's), and three new ones once a member has been decommissioned. */
  async safekeepers() {
    if (this.g.actions.some((a) => a.what.startsWith('control POST /sk/'))) return [{ id: 11, node: 'machine-1' }, { id: 12, node: 'machine-2' }, { id: 13, node: 'machine-3' }]
    return this.name === 'recover-name' ? [{ id: 7, node: 'machine-1' }, { id: 8, node: 'machine-2' }, { id: 10, node: 'machine-3' }] : [{ id: 3, node: 'machine-3' }, { id: 5, node: 'machine-1' }, { id: 6, node: 'machine-2' }]
  }
  async createDb() {}
  async setCheckpoint(db: string, seconds: number) { await this.g.setTenantCheckpoint(db, seconds) }
  async startClientPod(name: string) { this.started.push(name) }
  async startWriter() { this.started.push('drill-writer') }
  async startReader() { this.started.push('drill-reader') }
  async stopClients() {}
  async clientClock() { return this.g.now() }
  async clientLog() { return '' }
  async sql(_db: string, text: string) { return /count\(\*\)/i.test(text) && !/md5/.test(text) ? '1' : text.includes('md5') ? '1|1|x' : '1' }
  async sqlRetry(db: string, text: string) { return this.sql(db, text) }
  async flushed() { return true }
  async waitReady() { return true }
  async alerts() { return [] }
}
