import { afterEach, describe, expect, test } from 'bun:test'
import { FFIType, dlopen, ptr } from 'bun:ffi'
import { createHash, randomBytes } from 'node:crypto'
import { copyFileSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { type Server, Socket, connect, createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { ANNOTATIONS, gatewaySettings } from '../src/gateway/settings'
import { type Api, type BackupRow, type ClusterRow, type Names, type Options, type PodRow, type ScheduleRow, type ServiceRow, type Stage, MAX_LOGINS, classify, createGateway, fenceOf, kubeApi, readAnswer, readStartup } from '../src/gateway/gateway'

/**
 * The Postgres gateway (src/gateway/gateway.ts), at the two seams its header names: a stand-in for the calls it makes of the cluster's API, and real sockets on this machine, with a
 * stand-in database that listens or refuses. What a client sees through it is what is asserted, never how it is built. The platform's own pods run it over the control layer
 * (main.ts, neon-api.ts); the last tests here start that entry.
 */

// The names the platform's gateway pods give the program (settings.ts), so the tests run it as a pod does.
const PG_ROUTE_ANNOTATION = ANNOTATIONS.route
const SLEEP_ENABLED_ANNOTATION = ANNOTATIONS.sleepEnabled
const SLEEP_IDLE_MINUTES_ANNOTATION = ANNOTATIONS.sleepIdleMinutes
const SLEEP_FULL_MINUTES_ANNOTATION = ANNOTATIONS.sleepFullMinutes
const SLEEP_NEVER = 'never'
const HIBERNATION_ANNOTATION = ANNOTATIONS.hibernation
const FENCED_INSTANCES_ANNOTATION = ANNOTATIONS.fencedInstances
const FENCE_ALL = '["*"]'
const GATEWAY_IDLE_SINCE_ANNOTATION = ANNOTATIONS.idleSince
const SUSPENDED_BY_ANNOTATION = ANNOTATIONS.suspendedBy

const DB = 'apps/orders'
const names: Names = {
  namespace: 'peat-gateway',
  routeAnnotation: PG_ROUTE_ANNOTATION,
  sleepEnabledAnnotation: SLEEP_ENABLED_ANNOTATION,
  sleepIdleMinutesAnnotation: SLEEP_IDLE_MINUTES_ANNOTATION,
  sleepFullMinutesAnnotation: SLEEP_FULL_MINUTES_ANNOTATION,
  sleepNever: SLEEP_NEVER,
  hibernationAnnotation: HIBERNATION_ANNOTATION,
  fencedInstancesAnnotation: FENCED_INSTANCES_ANNOTATION,
  fenceAll: FENCE_ALL,
  idleSinceAnnotation: GATEWAY_IDLE_SINCE_ANNOTATION,
  suspendedByAnnotation: SUSPENDED_BY_ANNOTATION,
  portRanges: [{ from: 1024, to: 65535 }],
  // the ports a test route gets are never in this range, so a test route is a branch's unless it says it is a permanent database's
  permanentPorts: { from: 15000, to: 15999 },
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))
/** Waits until `ok()` holds, at most `ms`: a test of something that happens on a timer asserts the result, not how fast a loaded machine ran it. */
const until = async (ok: () => boolean, ms = 2000) => { const end = Date.now() + ms; while (!ok() && Date.now() < end) await sleep(5) }

/** Ports handed out in this file: the operating system may give a port it has just been given back, and a route on the stand-in database's own port is a gateway dialing itself. */
const handedOut = new Set<number>()
async function freePort(): Promise<number> {
  for (;;) {
    const server = createServer()
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const port = (server.address() as { port: number }).port
    await new Promise<void>((resolve) => server.close(() => resolve()))
    if (handedOut.has(port)) continue
    handedOut.add(port)
    return port
  }
}

/** A database that is not there until it listens: it echoes what it is sent, and counts the connections made to it. */
class StandIn {
  connections = 0
  private server?: Server
  private sockets = new Set<Socket>()
  constructor(readonly port: number) {}
  static async make() {
    return new StandIn(await freePort())
  }
  async listen(echo: (socket: Socket) => void = (socket) => void socket.pipe(socket)) {
    this.server = createServer((socket) => {
      this.connections += 1
      this.sockets.add(socket)
      socket.on('error', () => {})
      socket.on('close', () => this.sockets.delete(socket))
      echo(socket)
    })
    await new Promise<void>((resolve) => this.server!.listen(this.port, '127.0.0.1', resolve))
  }
  /** Goes away: nothing listens, so a dial is refused at once, and the connections it had are ended. */
  async close() {
    for (const s of this.sockets) s.destroy()
    await new Promise<void>((resolve) => (this.server ? this.server.close(() => resolve()) : resolve()))
    this.server = undefined
  }
}

/** A Map that numbers every `set`: the Cluster's `resourceVersion`, so that a change made by anyone (a test standing in for the writer of the Cluster's declaration, the operator or the gateway) is seen by a patch that names the version it was decided on. */
class Versioned<V> extends Map<string, V> {
  versions = new Map<string, number>()
  private counter = 0
  override set(key: string, value: V) {
    this.versions.set(key, ++this.counter)
    return super.set(key, value)
  }
  override delete(key: string) {
    this.versions.delete(key)
    return super.delete(key)
  }
}

/** The cluster's API, in memory: the Services and Clusters it holds, and every call made of it, in order. */
class FakeApi implements Api {
  services: ServiceRow[] = []
  clusters = new Versioned<ClusterRow | 'throw'>()
  calls: string[] = []
  failServices = false
  failWake = false
  /** How long a read of a Cluster and a turning of the mark take (the call goes out, and the answer is not at once). */
  getDelayMs = 0
  wakeDelayMs = 0
  onWake: () => void = () => {}
  /** The ScheduledBackups of each Cluster, by `<namespace>/<cluster>`, as the cluster holds them; a suspend or a resume changes them, as the patch would. */
  schedules = new Map<string, ScheduleRow[]>()
  failSchedules = false
  async listSchedules(namespace: string, cluster: string) {
    this.calls.push(`list schedules ${namespace}/${cluster}`)
    if (this.failSchedules) throw new Error('forbidden')
    return (this.schedules.get(`${namespace}/${cluster}`) ?? []).map((s) => structuredClone(s))
  }
  private schedule(namespace: string, name: string): ScheduleRow {
    for (const [key, rows] of this.schedules) if (key.startsWith(`${namespace}/`)) for (const row of rows) if (row.metadata.name === name) return row
    throw new Error(`no schedule ${name}`)
  }
  async suspendSchedule(namespace: string, name: string) {
    this.calls.push(`suspend ${namespace}/${name}`)
    if (this.failSchedules) throw new Error('forbidden')
    const row = this.schedule(namespace, name)
    row.spec = { ...row.spec, suspend: true }
    row.metadata.annotations = { ...row.metadata.annotations, [SUSPENDED_BY_ANNOTATION]: 'pg-gateway' }
  }
  async resumeSchedule(namespace: string, name: string) {
    this.calls.push(`resume ${namespace}/${name}`)
    if (this.failSchedules) throw new Error('forbidden')
    const row = this.schedule(namespace, name)
    row.spec = { ...row.spec, suspend: false }
    const { [SUSPENDED_BY_ANNOTATION]: _gone, ...rest } = row.metadata.annotations ?? {}
    row.metadata.annotations = rest
  }
  async listServices() {
    this.calls.push('listServices')
    if (this.failServices) throw new Error('the API is down')
    return this.services
  }
  /** The pods it holds by `<namespace>/<name>`: a missing one is a 404 (null), 'throw' is a read that fails (the Role lacks `get` on pods, or the API is down). */
  pods = new Map<string, PodRow | 'throw'>()
  async getPod(namespace: string, name: string) {
    this.calls.push(`getPod ${namespace}/${name}`)
    const pod = this.pods.get(`${namespace}/${name}`)
    if (pod === 'throw') throw new Error('forbidden')
    return pod ? structuredClone(pod) : null
  }
  async getCluster(namespace: string, name: string) {
    this.calls.push(`get ${namespace}/${name}`)
    if (this.getDelayMs) await sleep(this.getDelayMs)
    const cluster = this.clusters.get(`${namespace}/${name}`)
    if (cluster === 'throw') throw new Error('the API is down')
    return cluster ? this.versioned(`${namespace}/${name}`, cluster) : null
  }
  private versioned(key: string, cluster: ClusterRow): ClusterRow {
    return structuredClone({ ...cluster, metadata: { ...cluster.metadata, resourceVersion: String(this.clusters.versions.get(key)) } })
  }
  /** A patch that names the version it was decided on is refused (409) when the Cluster has changed since, as the API server does. */
  private precondition(namespace: string, name: string, version: string) {
    if (String(this.clusters.versions.get(`${namespace}/${name}`)) !== version) throw new Error('409 the object has been modified')
  }
  /** Held by a test: a gate every sleep patch waits at before it is made, to put a caller in the window while it is in flight. */
  patchGate?: Promise<void>
  patchStarted = 0
  /** The stage each wake patch carried, in order. */
  wakeStages: Stage[] = []
  /** The Clusters' own change as the operator would make it: the patches below change the stored Cluster, so the next read sees them. A wake does not unless `realistic` (the older tests keep the Cluster as it was). */
  realistic = false
  /** Every list call, in order (`list clusters <ns>`, `list backups <ns>`): kept apart from `calls`, which is the reads and writes of one database. */
  lists: string[] = []
  backups = new Map<string, BackupRow[]>()
  failBackups = false
  failList = false
  private change(namespace: string, name: string, changes: Record<string, string | null>) {
    const key = `${namespace}/${name}`
    const cluster = this.clusters.get(key)
    if (!cluster || cluster === 'throw') return
    const annotations = { ...(cluster.metadata?.annotations ?? {}) }
    for (const [k, v] of Object.entries(changes)) {
      if (v === null) delete annotations[k]
      else annotations[k] = v
    }
    this.clusters.set(key, { ...cluster, metadata: { ...cluster.metadata, annotations } })
  }
  async listClusters(namespace: string) {
    this.lists.push(`list clusters ${namespace}`)
    if (this.failList) throw new Error('the API is down')
    const rows: ClusterRow[] = []
    for (const [key, cluster] of this.clusters) {
      if (!key.startsWith(`${namespace}/`) || cluster === 'throw') continue
      rows.push(this.versioned(key, { ...cluster, metadata: { ...cluster.metadata, name: key.slice(namespace.length + 1) } }))
    }
    return rows
  }
  async listBackups(namespace: string) {
    this.lists.push(`list backups ${namespace}`)
    if (this.failBackups) throw new Error('forbidden')
    return [...this.backups].filter(([key]) => key.startsWith(`${namespace}/`)).flatMap(([, rows]) => structuredClone(rows))
  }
  async wake(namespace: string, name: string, stage: Stage = 'full') {
    this.calls.push(`wake ${namespace}/${name}`)
    this.wakeStages.push(stage)
    if (this.wakeDelayMs) await sleep(this.wakeDelayMs)
    if (this.failWake) throw new Error('forbidden')
    if (this.realistic) this.change(namespace, name, { ...(stage === 'full' ? { [HIBERNATION_ANNOTATION]: 'off' } : {}), [FENCED_INSTANCES_ANNOTATION]: null, [GATEWAY_IDLE_SINCE_ANNOTATION]: null })
    this.onWake()
  }
  async fence(namespace: string, name: string, idleSince: string, version: string) {
    this.calls.push(`fence ${namespace}/${name}`)
    this.patchStarted += 1
    await this.patchGate
    this.precondition(namespace, name, version)
    this.change(namespace, name, { [FENCED_INSTANCES_ANNOTATION]: FENCE_ALL, [GATEWAY_IDLE_SINCE_ANNOTATION]: idleSince })
  }
  async hibernate(namespace: string, name: string, version: string) {
    this.calls.push(`hibernate ${namespace}/${name}`)
    this.patchStarted += 1
    await this.patchGate
    this.precondition(namespace, name, version)
    this.change(namespace, name, { [HIBERNATION_ANNOTATION]: 'on' })
  }
  async unfence(namespace: string, name: string, version: string) {
    this.calls.push(`unfence ${namespace}/${name}`)
    this.patchStarted += 1
    await this.patchGate
    this.precondition(namespace, name, version)
    this.change(namespace, name, { [FENCED_INSTANCES_ANNOTATION]: null, [GATEWAY_IDLE_SINCE_ANNOTATION]: null })
  }
  count(prefix: string) {
    return this.calls.filter((c) => c.startsWith(prefix)).length
  }
}

const route = (name: string, port: number, database: string = DB): ServiceRow => ({
  metadata: { name, annotations: { [PG_ROUTE_ANNOTATION]: database } },
  spec: { ports: [{ port: 5432, targetPort: port }] },
})

/** Awake: no mark and no condition. What the Cluster says of its servers is not read, so a fixture carries none of it. */
const awake = (): ClusterRow => ({ metadata: { annotations: {} }, status: { conditions: [{ type: 'cnpg.io/hibernation', status: 'False' }] } })
/** Asleep as CloudNativePG leaves it: the mark on, the condition true, and the phase still reading healthy (measured 2026-11-14). */
const asleep = (declared: boolean): ClusterRow => ({
  metadata: { annotations: { [HIBERNATION_ANNOTATION]: 'on', ...(declared ? { [SLEEP_ENABLED_ANNOTATION]: 'true' } : {}) } },
  status: { conditions: [{ type: 'cnpg.io/hibernation', status: 'True' }] },
})

/** A client of the gateway: what it was sent, whether it was closed, and a way to wait for either. */
class Client {
  private chunks: Buffer[] = []
  length = 0
  closed = false
  errored = false
  ended = false
  readonly socket: Socket
  constructor(port: number) {
    this.socket = connect({ host: '127.0.0.1', port })
    this.socket.on('data', (chunk: Buffer) => {
      this.chunks.push(chunk)
      this.length += chunk.length
    })
    this.socket.on('close', () => (this.closed = true))
    this.socket.on('error', () => (this.errored = true))
    this.socket.on('end', () => (this.ended = true))
  }
  /** Under Bun a pause is dropped on a socket that has not connected yet: a test that holds a client back waits for this first. */
  connected() {
    return new Promise<void>((resolve) => (this.socket.connecting ? this.socket.once('connect', () => resolve()) : resolve()))
  }
  /** Everything received so far, as one buffer (joined when asked, not at each chunk). */
  get received(): Buffer {
    if (this.chunks.length > 1) this.chunks = [Buffer.concat(this.chunks)]
    return this.chunks[0] ?? Buffer.alloc(0)
  }
  write(data: string | Buffer) {
    this.socket.write(data)
  }
  async until(condition: () => boolean, ms = 4000) {
    const end = Date.now() + ms
    while (!condition()) {
      if (Date.now() > end) throw new Error('timed out waiting')
      await sleep(5)
    }
  }
  async hears(bytes: number, ms = 4000) {
    await this.until(() => this.length >= bytes, ms)
    return this.received
  }
  /** The connection is over as far as this end can tell: under Bun a socket with unsent bytes reports `end` and never `close`, so a test that asks whether the gateway let go reads either. */
  get over() {
    return this.closed || this.errored || this.ended
  }
  async isClosed(ms = 4000) {
    await this.until(() => this.closed, ms)
  }
  destroy() {
    this.socket.destroy()
  }
}

const cleanups: (() => Promise<void> | void)[] = []
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) await c()
})

async function setup(over: Partial<Options> = {}, withRoute = true) {
  const api = new FakeApi()
  const db = await StandIn.make()
  const port = await freePort()
  if (withRoute) api.services = [route('orders', port)]
  const seen: string[] = []
  const gateway = createGateway({
    api,
    names,
    target: (database) => (seen.push(database), { host: '127.0.0.1', port: db.port }),
    holdLimitMs: 3000,
    awakeHoldMs: 1000,
    retryMs: 15,
    wakeRetryMs: 15,
    dialTimeoutMs: 500,
    listenHost: '127.0.0.1',
    log: () => {},
    ...over,
  })
  cleanups.push(() => gateway.stop(), () => db.close())
  await gateway.refresh()
  const client = () => {
    const c = new Client(port)
    cleanups.push(() => c.destroy())
    return c
  }
  return { api, db, port, gateway, client, seen }
}

/** One sample out of the exposition, as Prometheus reads it. */
const sample = (gateway: { metrics(): string }, name: string, labels = `database="${DB}"`): number => {
  const prefix = labels === '' ? `${name} ` : `${name}{${labels}} `
  const line = gateway.metrics().split('\n').find((l) => l.startsWith(prefix))
  if (line === undefined) throw new Error(`no sample ${prefix}`)
  return Number(line.slice(prefix.length))
}
const wakes = (gateway: { metrics(): string }, result: string, stage: Stage = 'full') => sample(gateway, 'pg_gateway_wakes_total', `database="${DB}",stage="${stage}",result="${result}"`)
const sleeps = (gateway: { metrics(): string }, stage: Stage) => sample(gateway, 'pg_gateway_sleeps_total', `database="${DB}",stage="${stage}"`)
/** The wake time histogram of a stage (default the full one, which is what the older tests wake). */
const seconds = (gateway: { metrics(): string }, name: 'count' | 'sum' | `bucket,${string}`, stage: Stage = 'full') =>
  name.startsWith('bucket,') ? sample(gateway, 'pg_gateway_wake_seconds_bucket', `database="${DB}",stage="${stage}",le="${name.slice(7)}"`) : sample(gateway, `pg_gateway_wake_seconds_${name}`, `database="${DB}",stage="${stage}"`)
const holds = (gateway: { metrics(): string }, result: string) => sample(gateway, 'pg_gateway_holds_total', `database="${DB}",result="${result}"`)

describe('an awake database: the bytes pass both ways, untouched', () => {
  test('what a client sends arrives, and what the database answers comes back, with nothing asked of the cluster but its Services', async () => {
    const { api, db, client } = await setup()
    await db.listen()
    const c = client()
    c.write('SSLRequest then the startup message')
    expect((await c.hears(35)).toString()).toBe('SSLRequest then the startup message')
    expect(api.calls.filter((call) => call !== 'listServices')).toEqual([])
    expect(db.connections).toBe(1)
  })

  test('a large stream passes unchanged, both ways (32 MiB out and back, by checksum)', async () => {
    const { db, client } = await setup()
    await db.listen()
    const payload = randomBytes(32 * 1024 * 1024)
    const c = client()
    c.write(payload)
    const got = await c.hears(payload.length, 30_000)
    expect(got.length).toBe(payload.length)
    expect(createHash('sha256').update(got).digest('hex')).toBe(createHash('sha256').update(payload).digest('hex'))
  }, 60_000)

  test('a database that streams on its own passes a 64 MiB read to a client that sends one line', async () => {
    const { db, client } = await setup()
    const payload = randomBytes(64 * 1024 * 1024)
    await db.listen((socket) => socket.once('data', () => socket.end(payload)))
    const c = client()
    c.write('read it all')
    await c.isClosed(30_000)
    expect(createHash('sha256').update(c.received).digest('hex')).toBe(createHash('sha256').update(payload).digest('hex'))
  }, 60_000)

  test('a client that sends a great deal at once, before any database has the connection, is held back and loses nothing (8 MiB while the database wakes)', async () => {
    const { api, db, client } = await setup()
    api.clusters.set(DB, asleep(true))
    api.onWake = () => void sleep(300).then(() => db.listen())
    const payload = randomBytes(8 * 1024 * 1024)
    const c = client()
    c.write(payload)
    const got = await c.hears(payload.length, 20_000)
    expect(createHash('sha256').update(got).digest('hex')).toBe(createHash('sha256').update(payload).digest('hex'))
  }, 30_000)

  test('a session is counted while it is open and not after, and each connection is counted', async () => {
    const { db, gateway, client } = await setup()
    await db.listen()
    const a = client()
    const b = client()
    a.write('x')
    b.write('y')
    await a.hears(1)
    await b.hears(1)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(2)
    expect(sample(gateway, 'pg_gateway_connections_total')).toBe(2)
    a.destroy()
    b.destroy()
    await sleep(100)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(0)
    expect(sample(gateway, 'pg_gateway_connections_total')).toBe(2)
  })

  test('the database ending the session ends the client`s', async () => {
    const { db, client } = await setup()
    await db.listen((socket) => socket.end('bye'))
    const c = client()
    c.write('hi')
    await c.isClosed()
    expect(c.received.toString()).toBe('bye')
  })
})

describe('a database asleep and declared to sleep: held, woken, passed through', () => {
  test('turns only the mark off, holds the client, and passes it through, with what it sent while it waited, once the database listens', async () => {
    const { api, db, client } = await setup()
    api.clusters.set(DB, asleep(true))
    api.onWake = () => void sleep(250).then(() => db.listen())
    const c = client()
    c.write('hello while asleep')
    await sleep(120)
    // held: nothing came back and it is not closed
    expect(c.length).toBe(0)
    expect(c.closed).toBe(false)
    expect(api.count('wake')).toBe(1)
    await c.hears(18)
    expect(c.received.toString()).toBe('hello while asleep')
    // the whole of what it asked of the cluster: the Services, one read of the Cluster, one wake
    expect(api.calls.filter((call) => call !== 'listServices')).toEqual([`get ${DB}`, `wake ${DB}`])
    expect(db.connections).toBe(1)
  })

  test('counts the wake, and how long it took (the number the faster wake starts from)', async () => {
    const { api, db, gateway, client } = await setup()
    api.clusters.set(DB, asleep(true))
    api.onWake = () => void sleep(300).then(() => db.listen())
    const c = client()
    c.write('x')
    await c.hears(1)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(wakes(gateway, 'timed_out')).toBe(0)
    expect(wakes(gateway, 'refused')).toBe(0)
    expect(seconds(gateway, 'count')).toBe(1)
    const took = seconds(gateway, 'sum')
    expect(took).toBeGreaterThanOrEqual(0.28)
    expect(took).toBeLessThan(2)
    expect(seconds(gateway, 'bucket,1')).toBe(1)
    expect(seconds(gateway, 'bucket,0.25')).toBe(0)
    expect(seconds(gateway, 'bucket,+Inf')).toBe(1)
    // the light stage's histogram is its own, and empty here
    expect(seconds(gateway, 'count', 'light')).toBe(0)
  })

  test('ten callers share one wake: one read of the Cluster, one turning of the mark, ten connections through', async () => {
    const { api, db, gateway, client } = await setup()
    api.clusters.set(DB, asleep(true))
    api.onWake = () => void sleep(300).then(() => db.listen())
    const clients = Array.from({ length: 10 }, (_, i) => {
      const c = client()
      c.write(`caller-${i}`)
      return c
    })
    await Promise.all(clients.map((c, i) => c.hears(`caller-${i}`.length)))
    clients.forEach((c, i) => expect(c.received.toString()).toBe(`caller-${i}`))
    expect(api.count('wake')).toBe(1)
    expect(api.count('get')).toBe(1)
    expect(db.connections).toBe(10)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(sample(gateway, 'pg_gateway_connections_total')).toBe(10)
    expect(seconds(gateway, 'count')).toBe(1)
  })

  test('a caller that arrives while the wake is under way joins it: no second read, no second wake', async () => {
    const { api, db, client } = await setup()
    api.clusters.set(DB, asleep(true))
    api.onWake = () => void sleep(400).then(() => db.listen())
    const first = client()
    first.write('one')
    await sleep(150)
    const second = client()
    second.write('two')
    await first.hears(3)
    await second.hears(3)
    expect(api.count('wake')).toBe(1)
    expect(api.count('get')).toBe(1)
  })

  test('a caller that leaves while the Cluster is read and the mark turned does not drop the wake under the others: one read, one wake, a caller arriving after it is on it too', async () => {
    const { api, db, gateway, client } = await setup()
    api.clusters.set(DB, asleep(true))
    api.getDelayMs = 150
    api.wakeDelayMs = 100
    api.onWake = () => void sleep(500).then(() => db.listen())
    const first = client()
    first.write('one')
    await sleep(40)
    const second = client()
    second.write('two')
    await sleep(40)
    first.destroy()
    // the wake is made at about 250 ms; the first caller notices it has gone then, with the second still waiting on it
    await sleep(300)
    const third = client()
    third.write('three')
    await second.hears(3)
    await third.hears(5)
    expect(api.count('get')).toBe(1)
    expect(api.count('wake')).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(db.connections).toBe(2)
  })

  test('the hold limit closes the client and counts one failed wake', async () => {
    const { api, gateway, client } = await setup({ holdLimitMs: 400 })
    api.clusters.set(DB, asleep(true))
    // the database never answers
    const started = Date.now()
    const a = client()
    const b = client()
    a.write('x')
    b.write('y')
    await a.isClosed()
    await b.isClosed()
    const waited = Date.now() - started
    expect(waited).toBeGreaterThanOrEqual(380)
    expect(waited).toBeLessThan(2500)
    expect(a.length).toBe(0)
    expect(api.count('wake')).toBe(1)
    expect(wakes(gateway, 'timed_out')).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(0)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(0)
  })

  test('a wake it cannot make (the mark refuses) closes the caller and counts the wake as given up on', async () => {
    const { api, gateway, client } = await setup()
    api.clusters.set(DB, asleep(true))
    api.failWake = true
    const c = client()
    c.write('x')
    await c.isClosed()
    expect(wakes(gateway, 'timed_out')).toBe(1)
    expect(sample(gateway, 'pg_gateway_api_errors_total', '')).toBe(1)
  })

  test('a database that answers after the first wake is dialed afresh by the next caller, with no second wake', async () => {
    const { api, db, client } = await setup()
    api.clusters.set(DB, asleep(true))
    api.onWake = () => void sleep(100).then(() => db.listen())
    const first = client()
    first.write('a')
    await first.hears(1)
    const second = client()
    second.write('b')
    await second.hears(1)
    expect(api.count('wake')).toBe(1)
    expect(db.connections).toBe(2)
  })
})

describe('a database asleep by hand is refused, and nothing is woken', () => {
  test('a Cluster whose mark is on and that is not declared to sleep: the client is closed at once, the mark is not touched, the refusal is counted', async () => {
    const { api, db, gateway, client } = await setup()
    api.clusters.set(DB, asleep(false))
    const started = Date.now()
    const c = client()
    c.write('x')
    await c.isClosed()
    expect(Date.now() - started).toBeLessThan(1000)
    expect(api.count('wake')).toBe(0)
    expect(wakes(gateway, 'refused')).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(0)
    // and a server that comes up afterwards is not reached by that client
    await db.listen()
    expect(db.connections).toBe(0)
  })

  test('ten callers are each refused and none wakes it', async () => {
    const { api, gateway, client } = await setup()
    api.clusters.set(DB, asleep(false))
    const clients = Array.from({ length: 10 }, () => client())
    await Promise.all(clients.map((c) => c.isClosed()))
    expect(api.count('wake')).toBe(0)
    expect(wakes(gateway, 'refused')).toBe(10)
  })

  test('asleep by the condition alone (the mark absent) is asleep too, and declared or not decides the same way', async () => {
    const { api, gateway, client } = await setup()
    api.clusters.set(DB, { metadata: { annotations: {} }, status: { conditions: [{ type: 'cnpg.io/hibernation', status: 'True' }] } })
    const c = client()
    await c.isClosed()
    expect(api.count('wake')).toBe(0)
    expect(wakes(gateway, 'refused')).toBe(1)
  })

  test('the phase alone never says asleep: a Cluster that reads healthy with no mark and no condition is not woken', async () => {
    const { api, client } = await setup({ awakeHoldMs: 200 })
    api.clusters.set(DB, { metadata: { annotations: {} }, status: { phase: 'Cluster in healthy state' } } as ClusterRow)
    const c = client()
    await c.isClosed()
    expect(api.count('wake')).toBe(0)
  })
})

describe('a database that is not asleep and refuses: held for a short limit of its own, then closed', () => {
  test('a server that starts within the limit is reached: the caller is held, nothing is woken, and it is a hold, not a wake', async () => {
    const { api, db, gateway, client } = await setup()
    api.clusters.set(DB, awake())
    setTimeout(() => void db.listen(), 250)
    const c = client()
    c.write('hello')
    await sleep(100)
    expect(c.closed).toBe(false)
    await c.hears(5)
    expect(api.count('wake')).toBe(0)
    expect(holds(gateway, 'reached')).toBe(1)
    expect(holds(gateway, 'gave_up')).toBe(0)
    expect(wakes(gateway, 'woken') + wakes(gateway, 'timed_out') + wakes(gateway, 'refused')).toBe(0)
    expect(seconds(gateway, 'count')).toBe(0)
    expect(seconds(gateway, 'sum')).toBe(0)
  })

  test('a server that never answers closes the caller at the awake limit, not the wake limit, and counts a hold that gave up, no wake', async () => {
    const { api, gateway, client } = await setup({ awakeHoldMs: 300, holdLimitMs: 5000 })
    api.clusters.set(DB, awake())
    const started = Date.now()
    const c = client()
    c.write('x')
    await c.isClosed()
    const waited = Date.now() - started
    expect(waited).toBeGreaterThanOrEqual(280)
    expect(waited).toBeLessThan(2000)
    expect(api.count('wake')).toBe(0)
    expect(holds(gateway, 'gave_up')).toBe(1)
    expect(holds(gateway, 'reached')).toBe(0)
    expect(wakes(gateway, 'timed_out')).toBe(0)
  })

  test('the limit is each caller`s own: one that arrives later is held its whole limit, and each is counted', async () => {
    const { api, gateway, client } = await setup({ awakeHoldMs: 400 })
    api.clusters.set(DB, awake())
    const first = client()
    first.write('a')
    await sleep(250)
    const second = client()
    second.write('b')
    await first.isClosed()
    await sleep(100)
    // the first is gone at its limit; the second has had 150 ms of its own and is still held
    expect(second.closed).toBe(false)
    await second.isClosed()
    expect(holds(gateway, 'gave_up')).toBe(2)
    expect(api.count('wake')).toBe(0)
  })

  test('a route whose Cluster does not exist closes the client', async () => {
    const { client } = await setup()
    const c = client()
    await c.isClosed()
  })

  test('a Cluster that cannot be read wakes nothing and is counted as an API failure; the caller is held as for a database that is not asleep, and closed at that limit when nothing answers', async () => {
    const { api, gateway, client } = await setup({ awakeHoldMs: 300 })
    api.clusters.set(DB, 'throw')
    const c = client()
    await c.isClosed()
    expect(api.count('wake')).toBe(0)
    expect(sample(gateway, 'pg_gateway_api_errors_total', '')).toBe(1)
    expect(holds(gateway, 'gave_up')).toBe(1)
  })

  test('a caller of an awake database whose first dial was slow and whose Cluster cannot be read is served by the hold, not closed (the API is down and the database takes longer than the short first dial)', async () => {
    const { api, db, gateway, client } = await setup({ firstDialMs: 80 })
    api.clusters.set(DB, 'throw')
    setTimeout(() => void db.listen(), 200)
    const c = client()
    c.write('still served')
    expect((await c.hears(12)).toString()).toBe('still served')
    expect(api.count('wake')).toBe(0)
    expect(holds(gateway, 'reached')).toBe(1)
    expect(sample(gateway, 'pg_gateway_api_errors_total', '')).toBe(1)
  })

  test('a wake the gateway began keeps the wake limit however short the awake limit is, and is counted as a wake and not as a hold', async () => {
    const { api, db, gateway, client } = await setup({ awakeHoldMs: 100 })
    api.clusters.set(DB, asleep(true))
    api.onWake = () => void sleep(500).then(() => db.listen())
    const c = client()
    c.write('slept')
    await c.hears(5)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(holds(gateway, 'reached') + holds(gateway, 'gave_up')).toBe(0)
  })
})

describe('the routes are the Services, read while it runs', () => {
  test('a Service added is served without a restart, and one removed stops being listened on while its open sessions run on', async () => {
    const { api, db, gateway, port, client } = await setup({}, false)
    await db.listen()
    expect(gateway.ports()).toEqual([])
    const before = new Client(port)
    cleanups.push(() => before.destroy())
    await before.until(() => before.errored || before.closed)
    api.services = [route('orders', port)]
    await gateway.refresh()
    expect(gateway.ports()).toEqual([port])
    const c = client()
    c.write('served')
    await c.hears(6)
    // removed: the next connection is refused, the open one still works
    api.services = []
    await gateway.refresh()
    expect(gateway.ports()).toEqual([])
    const late = new Client(port)
    cleanups.push(() => late.destroy())
    await late.until(() => late.errored || late.closed)
    expect(late.length).toBe(0)
    c.write('still')
    await c.hears(11)
    expect(c.received.toString()).toBe('servedstill')
  })

  test('a route that is removed closes the callers still waiting on it at once, not at their deadline', async () => {
    const { api, gateway, client } = await setup({ holdLimitMs: 30_000 })
    api.clusters.set(DB, asleep(true))
    const waiting = client()
    waiting.write('x')
    await sleep(100)
    expect(waiting.closed).toBe(false)
    api.services = []
    await gateway.refresh()
    const removed = Date.now()
    await waiting.isClosed(2000)
    expect(Date.now() - removed).toBeLessThan(1500)
  })

  test('a fault that goes on is said once, and again only when it changes or has come back; the count still counts every time', async () => {
    const lines: string[] = []
    const { api, db, gateway, port } = await setup({ log: (line) => lines.push(line) })
    await db.listen()
    api.failServices = true
    for (let i = 0; i < 4; i++) await gateway.refresh()
    expect(lines.filter((l) => l.includes('listing the Services failed'))).toHaveLength(1)
    expect(sample(gateway, 'pg_gateway_api_errors_total', '')).toBe(4)
    api.failServices = false
    await gateway.refresh()
    await gateway.refresh()
    expect(lines.filter((l) => l.includes('works again'))).toHaveLength(1)
    api.failServices = true
    await gateway.refresh()
    expect(lines.filter((l) => l.includes('listing the Services failed'))).toHaveLength(2)
    api.failServices = false
    // a port someone else holds: said once however often the Services are read, and the route is made when the port is free
    const other = await freePort()
    const blocker = createServer()
    await new Promise<void>((resolve) => blocker.listen(other, '127.0.0.1', resolve))
    cleanups.push(() => void blocker.close())
    api.services = [route('orders', port), route('held', other, 'apps/held')]
    for (let i = 0; i < 4; i++) await gateway.refresh()
    expect(lines.filter((l) => l.includes(`cannot listen on ${other}`))).toHaveLength(1)
    expect(gateway.ports()).toEqual([port])
    await new Promise<void>((resolve) => blocker.close(() => resolve()))
    await gateway.refresh()
    expect(gateway.ports().sort((a, b) => a - b)).toEqual([port, other].sort((a, b) => a - b))
  })

  test('start reads the Services at once and again every refresh interval', async () => {
    const { api, db, gateway, port, client } = await setup({ refreshMs: 20 }, false)
    await db.listen()
    gateway.start()
    await sleep(60)
    api.services = [route('orders', port)]
    await sleep(120)
    expect(gateway.ports()).toEqual([port])
    const c = client()
    c.write('hi')
    await c.hears(2)
    expect(api.count('listServices')).toBeGreaterThan(3)
  })

  test('a Service that is not a route, one with a malformed route, one with a port outside the ranges and a second on a taken port are not listened on', async () => {
    const port = await freePort()
    const other = await freePort()
    const { api, gateway } = await setup({ names: { ...names, portRanges: [{ from: port, to: port }] } }, false)
    api.services = [
      { metadata: { name: 'metrics' }, spec: { ports: [{ port: 9090, targetPort: 9090 }] } },
      route('a-bad-route', other, 'no-slash'),
      route('b-outside', other),
      route('c-first', port, 'apps/first'),
      route('d-second', port, 'apps/second'),
    ]
    await gateway.refresh()
    expect(gateway.ports()).toEqual([port])
    expect(gateway.metrics()).toContain('database="apps/first"')
    expect(gateway.metrics()).not.toContain('database="apps/second"')
  })

  test('a failed read of the Services keeps the routes it has, and says so in the count', async () => {
    const { api, db, gateway, port, client } = await setup()
    await db.listen()
    api.failServices = true
    await gateway.refresh()
    expect(gateway.ports()).toEqual([port])
    expect(sample(gateway, 'pg_gateway_api_errors_total', '')).toBe(1)
    const c = client()
    c.write('ok')
    await c.hears(2)
  })

  test('a route that moves to another database sends its next connection there', async () => {
    const { api, db, gateway, port, client, seen } = await setup({}, false)
    await db.listen()
    api.services = [route('orders', port, 'apps/one')]
    await gateway.refresh()
    const a = client()
    a.write('1')
    await a.hears(1)
    api.services = [route('orders', port, 'apps/two')]
    await gateway.refresh()
    const b = client()
    b.write('2')
    await b.hears(1)
    expect(seen).toEqual(['apps/one', 'apps/two'])
  })

  test('a database whose route is gone and whose sessions have ended leaves the metrics; every series of a live route is there at zero from the start', async () => {
    const { api, gateway } = await setup()
    expect(sample(gateway, 'pg_gateway_connections_total')).toBe(0)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(0)
    for (const r of ['woken', 'timed_out', 'refused']) expect(wakes(gateway, r)).toBe(0)
    expect(seconds(gateway, 'count')).toBe(0)
    api.services = []
    await gateway.refresh()
    expect(gateway.metrics()).not.toContain('database="')
  })
})

const sha = (data: Buffer) => createHash('sha256').update(data).digest('hex')
const MiB = 1024 * 1024

describe('a side that reads slowly holds the other back: nothing is piled up in the gateway, and nothing is lost', () => {
  // What shows it from outside: a writer that has not been able to hand over what it has to write (its `drain` has not come, its write has not finished) while the reader sits still.
  // Take the pause out of the gateway and the writer finishes at once, because the gateway reads it all into its own memory.
  test('a client that does not read: the database`s 48 MiB write is not taken in, and arrives whole when the client reads', async () => {
    const { db, client } = await setup()
    const payload = randomBytes(48 * MiB)
    let written = false
    await db.listen((socket) => socket.once('data', () => socket.write(payload, () => (written = true))))
    const c = client()
    await c.connected()
    c.socket.pause()
    c.write('send it')
    await sleep(800)
    expect(written).toBe(false)
    c.socket.resume()
    await c.hears(payload.length, 30_000)
    expect(sha(c.received)).toBe(sha(payload))
    expect(written).toBe(true)
  }, 60_000)

  test('a database that does not read: the client`s 48 MiB write is not taken in, and arrives whole when the database reads', async () => {
    const { db, client } = await setup()
    const payload = randomBytes(48 * MiB)
    const hash = createHash('sha256')
    let got = 0
    let slow = true
    const held: Socket[] = []
    // paused from its first chunk (under Bun a server-side socket is reading from the moment it is accepted, and a pause made before that is dropped)
    await db.listen((socket) => {
      held.push(socket)
      socket.on('data', (chunk: Buffer) => {
        hash.update(chunk)
        got += chunk.length
        if (slow) socket.pause()
      })
    })
    const c = client()
    let drained = false
    c.socket.once('drain', () => (drained = true))
    c.socket.write(payload)
    await sleep(800)
    expect(drained).toBe(false)
    expect(c.socket.writableLength).toBeGreaterThan(16 * MiB)
    expect(got).toBeLessThan(16 * MiB)
    slow = false
    held[0]!.resume()
    for (let i = 0; i < 600 && got < payload.length; i++) await sleep(50)
    expect(got).toBe(payload.length)
    expect(hash.digest('hex')).toBe(sha(payload))
  }, 60_000)

  test('a client that sends a great deal before any database has the connection is paused at about 1 MiB, and what it sent arrives whole once the database is up', async () => {
    const { api, db, gateway, client } = await setup()
    api.clusters.set(DB, asleep(true))
    // nothing answers yet
    const payload = randomBytes(48 * MiB)
    const c = client()
    let drained = false
    c.socket.once('drain', () => (drained = true))
    c.socket.write(payload)
    await sleep(800)
    expect(drained).toBe(false)
    expect(c.socket.writableLength).toBeGreaterThan(16 * MiB)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(0)
    const hash = createHash('sha256')
    let got = 0
    await db.listen((socket) =>
      socket.on('data', (chunk: Buffer) => {
        hash.update(chunk)
        got += chunk.length
      }),
    )
    for (let i = 0; i < 600 && got < payload.length; i++) await sleep(50)
    expect(got).toBe(payload.length)
    expect(hash.digest('hex')).toBe(sha(payload))
  }, 60_000)
})

describe('a reset, and a client that closes while held', () => {
  test('a database that resets the connection mid-stream ends the client`s, the session is not left counted, and the next caller is served', async () => {
    const { db, gateway, client } = await setup()
    await db.listen((socket) =>
      socket.once('data', () => {
        socket.write(randomBytes(64 * 1024))
        setTimeout(() => socket.resetAndDestroy(), 50)
      }),
    )
    const a = client()
    a.write('go')
    await a.isClosed()
    expect(a.length).toBeLessThanOrEqual(64 * 1024)
    await sleep(100)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(0)
    const b = client()
    b.write('again')
    await b.isClosed()
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(0)
  })

  test('a client that resets the connection mid-stream ends the database`s, and the session is not left counted', async () => {
    const { db, gateway, client } = await setup()
    const ended: Promise<void>[] = []
    await db.listen((socket) => {
      ended.push(new Promise((resolve) => socket.once('close', () => resolve())))
      socket.pipe(socket)
    })
    const c = client()
    c.write(randomBytes(256 * 1024))
    await c.hears(1)
    c.socket.resetAndDestroy()
    await ended[0]
    await sleep(100)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(0)
  })

  test('a client that closes while it is held for a database that is not asleep: no session, no count either way, no dialing after, and the others are served', async () => {
    const { api, db, gateway, client } = await setup({ awakeHoldMs: 2000 })
    api.clusters.set(DB, awake())
    const leaving = client()
    leaving.write('x')
    const staying = client()
    staying.write('y')
    await sleep(150)
    leaving.destroy()
    await sleep(100)
    await db.listen()
    await staying.hears(1)
    await sleep(200)
    // only the one that stayed reached the database
    expect(db.connections).toBe(1)
    expect(holds(gateway, 'reached')).toBe(1)
    expect(holds(gateway, 'gave_up')).toBe(0)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(1)
    staying.destroy()
    await sleep(100)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(0)
  })

  test('a client that closes while it is held for a wake leaves the wake to the others and the sessions at zero', async () => {
    const { api, db, gateway, client } = await setup()
    api.clusters.set(DB, asleep(true))
    api.onWake = () => void sleep(400).then(() => db.listen())
    const leaving = client()
    leaving.write('x')
    const staying = client()
    staying.write('yy')
    await sleep(100)
    leaving.destroy()
    await staying.hears(2)
    expect(api.count('wake')).toBe(1)
    expect(db.connections).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(wakes(gateway, 'timed_out')).toBe(0)
  })
})

/**
 * A side that goes away while the other is paused (read live 2026-11-15, Bun 1.3.14 on Linux, a client killed mid-way through a 100 MB read: the session stayed open for minutes, the
 * database's backend stayed `active` on `ClientWrite`, and a database that counts open sessions as use never slept). Reproduced on macOS, same Bun: a socket whose buffered writes cannot
 * be sent reports `end` and never `close`, so a pair that waited for `close` was never ended. These pin every way out of a pair, from each side, with the other side paused.
 */
describe('a side that goes while the other is paused ends the pair', () => {
  const GRACE_MS = 400
  const leaves: [string, (c: Client) => void][] = [
    ['resets the connection', (c) => c.socket.resetAndDestroy()],
    ['closes it', (c) => c.destroy()],
    ['half-closes it', (c) => c.socket.end()],
  ]
  /** The end of a stand-in database's connection as it can tell: the gateway's FIN or reset (`end`, `error`), or a `close` (which Bun does not report on a socket with bytes it could not send). */
  const goes = (socket: Socket) =>
    new Promise<void>((resolve) => {
      for (const event of ['end', 'error', 'close']) socket.once(event, () => resolve())
    })
  /** A database that streams 64 MiB at the first byte, and the promise of the end of its connection. */
  const streaming = async (db: StandIn) => {
    const ended: Promise<void>[] = []
    await db.listen((socket) => {
      ended.push(goes(socket))
      socket.once('data', () => socket.write(Buffer.alloc(64 * MiB, 'x')))
    })
    return ended
  }
  const sessions = (gateway: { metrics(): string }) => sample(gateway, 'pg_gateway_sessions_open')
  const within = async (what: string, condition: () => boolean, ms = 4000) => {
    const end = Date.now() + ms
    while (!condition()) {
      if (Date.now() > end) throw new Error(`timed out waiting: ${what}`)
      await sleep(10)
    }
  }

  for (const [how, leave] of leaves) {
    test(`a client that stops reading and then ${how}: the database's connection is ended and the session is not left counted`, async () => {
      const { db, gateway, client } = await setup({ closeGraceMs: GRACE_MS })
      const ended = await streaming(db)
      const c = client()
      await c.connected()
      c.write('read it all')
      await c.hears(1)
      c.socket.pause()
      // the client's side is full: the gateway has paused the database's
      await sleep(500)
      expect(sessions(gateway)).toBe(1)
      leave(c)
      let over = false
      void ended[0]!.then(() => (over = true))
      await within('the database`s connection ended', () => over)
      await within('the session not counted', () => sessions(gateway) === 0)
    })

    test(`a database that stops reading while the client sends a great deal, and then the client ${how}: the database's connection is ended and the session is not left counted`, async () => {
      const { db, gateway, client } = await setup({ closeGraceMs: GRACE_MS })
      const ended: Promise<void>[] = []
      const held: Socket[] = []
      await db.listen((socket) => {
        held.push(socket)
        ended.push(goes(socket))
        socket.once('data', () => socket.pause())
      })
      const c = client()
      c.socket.write(Buffer.alloc(48 * MiB, 'y'))
      await sleep(500)
      expect(sessions(gateway)).toBe(1)
      leave(c)
      // a paused socket does not see the other end go: the database reads again, as a backend does when it has a moment, and finds it
      await sleep(GRACE_MS * 3)
      held[0]!.resume()
      let over = false
      void ended[0]!.then(() => (over = true))
      await within('the database`s connection ended', () => over)
      await within('the session not counted', () => sessions(gateway) === 0)
    })
  }

  const databaseGoes: [string, (socket: Socket) => void][] = [
    ['resets the connection', (socket) => socket.resetAndDestroy()],
    ['closes it', (socket) => socket.destroy()],
    ['half-closes it', (socket) => socket.end()],
  ]
  for (const [how, leave] of databaseGoes) {
    test(`a database that stops reading while the client sends a great deal, and then ${how}: the client's connection is ended and the session is not left counted`, async () => {
      const { db, gateway, client } = await setup({ closeGraceMs: GRACE_MS })
      const held: Socket[] = []
      await db.listen((socket) => {
        held.push(socket)
        socket.once('data', () => socket.pause())
      })
      const c = client()
      c.socket.write(Buffer.alloc(48 * MiB, 'y'))
      await sleep(500)
      expect(sessions(gateway)).toBe(1)
      leave(held[0]!)
      await within('the client`s connection ended', () => c.over)
      await within('the session not counted', () => sessions(gateway) === 0)
    })

    test(`a database that streams to a client that stopped reading, and then ${how}: once the client reads again the session is not left counted and the client's connection is ended`, async () => {
      const { db, gateway, client } = await setup({ closeGraceMs: GRACE_MS })
      const held: Socket[] = []
      await db.listen((socket) => {
        held.push(socket)
        socket.once('data', () => socket.write(Buffer.alloc(64 * MiB, 'x')))
      })
      const c = client()
      await c.connected()
      c.write('read it all')
      await c.hears(1)
      c.socket.pause()
      await sleep(500)
      expect(sessions(gateway)).toBe(1)
      leave(held[0]!)
      // The gateway's read of the database is paused while the client's side is full, and a FIN or a reset behind unread bytes is not seen until they are read: so the end is seen when
      // the client reads again (what the database had already sent goes on to it first), and then the pair is ended, not left open for good.
      c.socket.resume()
      await within('the session not counted', () => sessions(gateway) === 0, 8000)
      await within('the client`s connection ended', () => c.over)
    })
  }

  test('the pair ended is not counted twice, and a session opened afterwards is counted from zero', async () => {
    const { db, gateway, client } = await setup({ closeGraceMs: GRACE_MS })
    const ended = await streaming(db)
    const a = client()
    await a.connected()
    a.write('read it all')
    await a.hears(1)
    a.socket.pause()
    await sleep(300)
    a.socket.resetAndDestroy()
    await ended[0]
    await within('the session not counted', () => sessions(gateway) === 0)
    await sleep(GRACE_MS + 200)
    expect(sessions(gateway)).toBe(0)
    const b = client()
    b.write('hello')
    await b.hears(1)
    expect(sessions(gateway)).toBe(1)
  })

  // Before any database has the connection the caller is on its own: nothing of it is passed, so a caller that went is closed at once and nothing dials for it.
  for (const [how, leave] of leaves) {
    for (const [what, state] of [
      ['a wake', () => asleep(true)],
      ['a database that is not asleep', () => awake()],
    ] as const) {
      test(`a client held for ${what} that ${how}: it is closed at once, nothing dials for it after, and no session is made when the database comes`, async () => {
        const { api, db, gateway, client, seen } = await setup({ closeGraceMs: GRACE_MS, awakeHoldMs: 5000 })
        api.clusters.set(DB, state())
        const c = client()
        c.write('startup message')
        await sleep(200)
        leave(c)
        await sleep(200)
        const dials = seen.length
        await sleep(300)
        expect(seen.length).toBe(dials)
        await db.listen()
        await sleep(300)
        expect(db.connections).toBe(0)
        expect(sessions(gateway)).toBe(0)
      })
    }
  }

  // A socket that is paused is not read, and a FIN behind unread bytes is not seen until they are read: a caller that sent more than the gateway keeps (about 1 MiB) and went is found out
  // when its bytes are read, which is when a database has the connection (`splice` resumes it) or its hold ends. What must hold is that the pair is then ended, and that nothing is left counted.
  // A reset is the one that differs by kernel: macOS keeps it behind the unread bytes, so the same holds; Linux drops the unread bytes and tells even a paused socket at once, so the gateway
  // closes the caller as it does for one that leaves before it has sent much, and nothing dials for it. Either is right, and each is pinned: what is not right is a database's connection
  // that is made and left open, or a session counted.
  // The caller writes far more than the kernel can hold between the two sockets (Linux takes about 10 MiB into its send and receive buffers on loopback, so 8 MiB was sometimes all taken, and
  // nothing was left unread to hide the departure; the tests above write 48 MiB for the same reason).
  for (const [how, leave] of leaves) {
    test(`a client held for a wake that sent more than the gateway keeps, and then ${how}: when the database comes, its connection is ended (or never made, for a reset the kernel reports at once) and no session is left counted`, async () => {
      const { api, db, gateway, client } = await setup({ closeGraceMs: GRACE_MS })
      api.clusters.set(DB, asleep(true))
      const ended: Promise<void>[] = []
      const c = client()
      c.socket.write(Buffer.alloc(48 * MiB, 'z'))
      await sleep(500)
      expect(c.socket.writableLength).toBeGreaterThan(MiB)
      leave(c)
      await sleep(200)
      await db.listen((socket) => {
        ended.push(goes(socket))
        socket.resume()
      })
      const made = await within('the database`s connection made', () => ended.length === 1, 3000).then(
        () => true,
        () => false,
      )
      if (made) {
        let over = false
        void ended[0]!.then(() => (over = true))
        await within('the database`s connection ended', () => over)
      } else {
        // Only a reset may be found out before a database has the connection; then nothing dials for the caller after, and none is made while the database listens.
        expect(how).toBe('resets the connection')
        await sleep(300)
        expect(db.connections).toBe(0)
      }
      await within('the session not counted', () => sessions(gateway) === 0)
    }, 10_000)
  }
})

/**
 * A peer that vanishes without a word (read live 2026-11-15: the database's pod was deleted, which sends no FIN and no reset, and the gateway's socket to it, which nobody read or wrote,
 * stayed ESTABLISHED and counted as a session) is found by the kernel's keepalive probes. What is pinned is what the kernel holds for each socket of a pair, read back with
 * getsockopt from the socket's descriptor: probing on, after 30 s idle. (Bun passes the idle time on, and sets the probe interval to 1 s and the count to 10.) That a probe unanswered ends
 * the pair is the kernel's and Bun's, and is the live run's to show; `over` is what the other tests pin once an error or an end comes.
 */
describe('a peer that vanishes without a word is probed for', () => {
  const posix = process.platform === 'darwin' || process.platform === 'linux'
  const mac = process.platform === 'darwin'
  const getsockopt = posix
    ? dlopen(mac ? '/usr/lib/libSystem.B.dylib' : 'libc.so.6', {
        getsockopt: { args: [FFIType.i32, FFIType.i32, FFIType.i32, FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
      }).symbols.getsockopt
    : undefined
  const option = (fd: number, level: number, name: number): number => {
    const value = new Int32Array(1)
    const length = new Int32Array([4])
    if (getsockopt!(fd, level, name, ptr(value), ptr(length)) !== 0) throw new Error(`getsockopt ${level}/${name} failed`)
    return value[0]!
  }
  const SOL_SOCKET = mac ? 0xffff : 1
  const SO_KEEPALIVE = mac ? 0x8 : 9
  const IPPROTO_TCP = 6
  const TCP_KEEPIDLE = mac ? 0x10 : 4

  test.skipIf(!posix)('both sockets of a pair are set to be probed after 30 s idle, by what the kernel holds for each', async () => {
    const probed: { socket: Socket; enable: boolean; delay: number }[] = []
    const original = Socket.prototype.setKeepAlive
    Socket.prototype.setKeepAlive = function (this: Socket, enable?: boolean, delay?: number) {
      probed.push({ socket: this, enable: enable ?? false, delay: delay ?? 0 })
      return original.call(this, enable, delay)
    } as typeof original
    cleanups.push(() => void (Socket.prototype.setKeepAlive = original))
    const { db, client } = await setup()
    await db.listen()
    const c = client()
    c.write('x')
    await c.hears(1)
    // the one the gateway accepted and the one it dialed (the test's own client and the stand-in's accepted socket are not set, so these are the gateway's)
    expect(probed.length).toBe(2)
    for (const { socket, enable, delay } of probed) {
      expect(enable).toBe(true)
      expect(delay).toBe(30_000)
      const fd = (socket as unknown as { _handle: { fd: number } })._handle.fd
      expect(option(fd, SOL_SOCKET, SO_KEEPALIVE)).not.toBe(0)
      expect(option(fd, IPPROTO_TCP, TCP_KEEPIDLE)).toBe(30)
    }
  })

  test('a session idle for longer than the probe time is not cut: no idle timeout is built', async () => {
    const { db, gateway, client } = await setup({ keepAliveMs: 100 })
    await db.listen()
    const c = client()
    c.write('x')
    await c.hears(1)
    await sleep(1500)
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(1)
    c.write('y')
    expect((await c.hears(2)).toString()).toBe('xy')
  })
})

describe('it never opens a session by itself', () => {
  test('with routes, sleeping and awake, and no caller, the database sees no connection and the cluster is asked for nothing but the Services', async () => {
    for (const cluster of [asleep(true), awake()]) {
      const { api, db, gateway } = await setup({ refreshMs: 15 })
      api.clusters.set(DB, cluster)
      await db.listen()
      gateway.start()
      await sleep(250)
      expect(db.connections).toBe(0)
      expect(api.calls.filter((c) => c !== 'listServices')).toEqual([])
      await gateway.stop()
    }
  })

  test('a caller that leaves while the database is asleep ends the dialing: the database, when it comes up, sees no connection', async () => {
    const { api, db, client } = await setup()
    api.clusters.set(DB, asleep(true))
    const c = client()
    c.write('x')
    await sleep(80)
    expect(api.count('wake')).toBe(1)
    c.destroy()
    await sleep(80)
    await db.listen()
    await sleep(200)
    expect(db.connections).toBe(0)
  })

  test('a wake nobody is waiting on at its deadline is not counted as a failed wake: nothing dialed, so nothing is known', async () => {
    const { api, gateway, client } = await setup({ holdLimitMs: 250 })
    api.clusters.set(DB, asleep(true))
    const c = client()
    c.write('x')
    await sleep(60)
    c.destroy()
    await sleep(400)
    expect(wakes(gateway, 'timed_out')).toBe(0)
    expect(wakes(gateway, 'woken')).toBe(0)
  })
})

/**
 * A write that makes no progress (found as a fault: there was no limit on a write that makes no progress). A peer that drops off the network in the middle of a large read leaves bytes waiting that
 * nothing will accept, and keepalive does not probe a socket with data queued, so only TCP's retransmission limit (about 15 minutes) found it and the database was held awake that long. The
 * limit is each side's: bytes waiting to be written and none accepted for it. A session with nothing waiting is never cut, and a reader that is slow but moving is not.
 */
describe('a write that makes no progress for the limit ends the pair', () => {
  const STALL_MS = 600
  const GRACE_MS = 300
  const goes = (socket: Socket) =>
    new Promise<void>((resolve) => {
      for (const event of ['end', 'error', 'close']) socket.once(event, () => resolve())
    })
  const sessions = (gateway: { metrics(): string }) => sample(gateway, 'pg_gateway_sessions_open')
  const within = async (what: string, condition: () => boolean, ms = 6000) => {
    const end = Date.now() + ms
    while (!condition()) {
      if (Date.now() > end) throw new Error(`timed out waiting: ${what}`)
      await sleep(10)
    }
  }
  /** A database that streams 64 MiB at the first byte, and the promise of the end of each connection made to it. */
  const streaming = async (db: StandIn) => {
    const ended: Promise<void>[] = []
    await db.listen((socket) => {
      ended.push(goes(socket))
      socket.once('data', () => socket.write(Buffer.alloc(64 * MiB, 'x')))
    })
    return ended
  }

  test('a client that stops reading in the middle of a large read and does not leave: the pair is ended at the limit and the database`s connection with it, with no one closing anything', async () => {
    const { db, gateway, client } = await setup({ stallMs: STALL_MS, closeGraceMs: GRACE_MS })
    const ended = await streaming(db)
    const c = client()
    await c.connected()
    c.write('read it all')
    await c.hears(1)
    c.socket.pause()
    await sleep(200)
    // the client's side is full, and has been for less than the limit: the session is still there
    expect(sessions(gateway)).toBe(1)
    let over = false
    void ended[0]!.then(() => (over = true))
    await within('the database`s connection ended', () => over)
    await within('the session not counted', () => sessions(gateway) === 0)
  })

  test('a database that stops reading while the client sends a great deal is never cut, whatever the limit: bytes waiting toward the database wait as on a direct connection (a COPY FROM STDIN whose statement waits on a lock)', async () => {
    const { db, gateway, client } = await setup({ stallMs: STALL_MS, closeGraceMs: GRACE_MS })
    const held: Socket[] = []
    await db.listen((socket) => {
      held.push(socket)
      socket.once('data', () => socket.pause())
    })
    const c = client()
    c.socket.write(Buffer.alloc(48 * MiB, 'y'))
    await sleep(200)
    expect(sessions(gateway)).toBe(1)
    // the client's bytes wait for the database for four times the limit and the grace after it
    await sleep(STALL_MS * 4 + GRACE_MS)
    expect(sessions(gateway)).toBe(1)
    expect(c.over).toBe(false)
    expect(held[0]!.destroyed).toBe(false)
    expect(c.socket.writableLength).toBeGreaterThan(16 * MiB)
  }, 15_000)

  test('the same stalled client is not cut within a few seconds at the default limit (300 s): the limit is what ends it', async () => {
    const { db, gateway, client } = await setup({ closeGraceMs: GRACE_MS })
    const ended = await streaming(db)
    const c = client()
    await c.connected()
    c.write('read it all')
    await c.hears(1)
    c.socket.pause()
    await sleep(STALL_MS * 4)
    expect(sessions(gateway)).toBe(1)
    let over = false
    void ended[0]!.then(() => (over = true))
    await sleep(100)
    expect(over).toBe(false)
  })

  test('a session with nothing waiting is never cut, however long it is idle: a healthy session that says nothing for four times the limit still works', async () => {
    const { db, gateway, client } = await setup({ stallMs: STALL_MS, closeGraceMs: GRACE_MS })
    await db.listen()
    const c = client()
    c.write('x')
    await c.hears(1)
    await sleep(STALL_MS * 4)
    expect(sessions(gateway)).toBe(1)
    c.write('y')
    expect((await c.hears(2)).toString()).toBe('xy')
    expect(c.over).toBe(false)
  })

  test('a reader that is slow but moving is never cut: bytes wait the whole time, some are accepted every so often, and the pair is still there after several times the limit', async () => {
    const { db, gateway, client } = await setup({ stallMs: STALL_MS, closeGraceMs: GRACE_MS })
    // a database with no end to what it sends (a stream the reader can never finish), so that bytes wait however much is read
    const ended: Promise<void>[] = []
    await db.listen((socket) => {
      ended.push(goes(socket))
      const chunk = Buffer.alloc(64 * 1024, 'x')
      const pump = () => {
        while (!socket.destroyed) if (!socket.write(chunk)) return void socket.once('drain', pump)
      }
      socket.once('data', pump)
    })
    const c = client()
    await c.connected()
    c.write('read it all')
    await c.hears(1)
    c.socket.pause()
    let over = false
    void ended[0]!.then(() => (over = true))
    const before = c.length
    // takes 8 MiB and then stops reading for 340 ms, over and over, for five times the limit (a loopback socket reads at gigabytes a second, so a slow reader is made by counting what it
    // takes; the kernel makes room for the gateway's writes in bursts of about a third of the socket's buffer, so what a reader takes has to be of that order to be seen): bytes wait for
    // the gateway to hand over the whole time, and the gap between two takings (about 400 ms) is less than the limit (600 ms), so it is the bytes accepted that keep the pair, not an empty buffer
    const take = (bytes: number) =>
      new Promise<void>((resolve) => {
        let got = 0
        const on = (chunk: Buffer) => {
          got += chunk.length
          if (got < bytes) return
          c.socket.pause()
          c.socket.off('data', on)
          resolve()
        }
        c.socket.on('data', on)
        c.socket.resume()
      })
    for (let i = 0; i < (STALL_MS * 5) / 400; i++) {
      await take(8 * MiB)
      await sleep(340)
    }
    expect(over).toBe(false)
    expect(sessions(gateway)).toBe(1)
    expect(c.length).toBeGreaterThan(before)
    expect(c.over).toBe(false)
  }, 20_000)
})

/**
 * The count of a wake that timed out (a fault found: a dial still in flight at the wake limit could pass its caller through while the wake was counted `timed_out`): an alert
 * reads it, so a caller that was served must never be in it.
 */
describe('the count of a wake at its limit is what the callers saw', () => {
  /** The first dial is refused (the database is asleep); every later one takes `ms` to end, and then reaches the stand-in database (its port is set once `setup` has made it) or is refused. */
  async function slowDials(ms: number, reachable: boolean) {
    const refused = await freePort()
    const state = { port: 0, dials: 0 }
    const options: Partial<Options> = {
      holdLimitMs: 300,
      wakeDialMs: 3000,
      connect: (t) => {
        state.dials += 1
        if (state.dials === 1) return connect({ host: t.host, port: refused })
        const socket = new Socket()
        setTimeout(() => socket.connect(reachable ? state.port : refused, t.host), ms)
        return socket
      },
    }
    return { options, state }
  }

  test('a dial that is in flight at the limit and is answered: the caller is served, the wake is counted woken, and nothing is counted timed out', async () => {
    const slow = await slowDials(450, true)
    const { api, db, gateway, client } = await setup(slow.options)
    slow.state.port = db.port
    api.clusters.set(DB, asleep(true))
    await db.listen()
    const c = client()
    c.write('hello')
    expect((await c.hears(5, 4000)).toString()).toBe('hello')
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(wakes(gateway, 'timed_out')).toBe(0)
    expect(slow.state.dials).toBe(2)
  })

  test('a dial that is in flight at the limit and is refused: the caller is closed and the wake is counted timed out, once', async () => {
    const slow = await slowDials(450, false)
    const { api, gateway, client } = await setup(slow.options)
    api.clusters.set(DB, asleep(true))
    const c = client()
    c.write('hello')
    await c.isClosed()
    expect(wakes(gateway, 'timed_out')).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(0)
  })

  test('ten callers on one wake whose dials are in flight at the limit: one count, whichever way it falls', async () => {
    const slow = await slowDials(450, false)
    const { api, gateway, client } = await setup(slow.options)
    api.clusters.set(DB, asleep(true))
    const clients = Array.from({ length: 10 }, () => client())
    for (const c of clients) c.write('x')
    for (const c of clients) await c.isClosed()
    expect(wakes(gateway, 'timed_out')).toBe(1)
  })
})

/**
 * On this cluster a dial to a Service with no ready server does not come back refused: it hangs (measured 2026-11-15, 14.7 s for a wake of which 3 s were the first dial). A hanging dial
 * is a socket that never connects, made here by the `connect` seam; the limits are shortened so that what is asserted is which limit applies, by how long the caller waits.
 */
describe('a dial that hangs is abandoned at a short limit, and the limit that applies is the one for where the caller is', () => {
  /** A socket that is never connected: its dial neither answers nor is refused. */
  const hanging = (made: Socket[]) => {
    const socket = new Socket()
    made.push(socket)
    return socket
  }

  test('a first dial that hangs leads to the read of the Cluster after the short limit, not after the dial limit of the hold', async () => {
    const made: Socket[] = []
    const state = { port: 0, dials: 0 }
    const { api, db, client } = await setup({
      firstDialMs: 100,
      dialTimeoutMs: 3000,
      holdLimitMs: 5000,
      connect: (t) => {
        state.dials += 1
        return state.dials === 1 ? hanging(made) : connect({ host: t.host, port: state.port })
      },
    })
    state.port = db.port
    api.clusters.set(DB, asleep(true))
    api.onWake = () => void sleep(50).then(() => db.listen())
    const started = Date.now()
    const c = client()
    c.write('hello')
    expect((await c.hears(5, 4000)).toString()).toBe('hello')
    // 100 ms of the first dial, the read, the mark and the server: nowhere near the 3 s that a dial of the hold would have been given
    expect(Date.now() - started).toBeLessThan(1500)
    expect(api.calls.filter((call) => call !== 'listServices')).toEqual([`get ${DB}`, `wake ${DB}`])
    expect(made[0]!.destroyed).toBe(true)
  })

  test('while a wake is under way a dial that hangs is abandoned at the short limit, and the next one is through once the server answers', async () => {
    const made: Socket[] = []
    const refused = await freePort()
    const state = { port: 0, dials: 0, up: false }
    const { api, db, gateway, client } = await setup({
      wakeDialMs: 100,
      // the limit of the hold is long: a dial that waited for it would be 3 s late
      dialTimeoutMs: 3000,
      holdLimitMs: 5000,
      connect: (t) => {
        state.dials += 1
        if (state.dials === 1) return connect({ host: t.host, port: refused })
        return state.up ? connect({ host: t.host, port: state.port }) : hanging(made)
      },
    })
    state.port = db.port
    api.clusters.set(DB, asleep(true))
    const c = client()
    c.write('hello')
    // 350 ms of dials that hang: each is abandoned at 100 ms and the next made, so there are several and none is still waiting
    await sleep(350)
    expect(made.length).toBeGreaterThanOrEqual(3)
    expect(made.slice(0, -1).every((socket) => socket.destroyed)).toBe(true)
    expect(c.closed).toBe(false)
    await db.listen()
    state.up = true
    const up = Date.now()
    expect((await c.hears(5, 4000)).toString()).toBe('hello')
    // the dial in flight when the server came up is abandoned within 100 ms and the next one is through (one retry gap of 15 ms between)
    expect(Date.now() - up).toBeLessThan(600)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(wakes(gateway, 'timed_out')).toBe(0)
    expect(api.count('wake')).toBe(1)
  })

  test('an awake database whose first dial is slower than the short limit is still served: the read finds it awake and the hold dials with the longer limit', async () => {
    const made: Socket[] = []
    const state = { port: 0, dials: 0 }
    const { api, db, gateway, client } = await setup({
      firstDialMs: 100,
      dialTimeoutMs: 1000,
      connect: (t) => {
        state.dials += 1
        if (state.dials === 1) return hanging(made)
        // slower than the first dial's limit, well inside the hold's: it is answered, late
        const socket = new Socket()
        setTimeout(() => socket.connect(state.port, t.host), 250)
        return socket
      },
    })
    state.port = db.port
    api.clusters.set(DB, awake())
    await db.listen()
    const c = client()
    c.write('slow but there')
    expect((await c.hears(14, 4000)).toString()).toBe('slow but there')
    expect(made[0]!.destroyed).toBe(true)
    expect(api.count('get')).toBe(1)
    expect(api.count('wake')).toBe(0)
    expect(holds(gateway, 'reached')).toBe(1)
    expect(holds(gateway, 'gave_up')).toBe(0)
    expect(wakes(gateway, 'woken') + wakes(gateway, 'timed_out') + wakes(gateway, 'refused')).toBe(0)
  })
})

/**
 * A permanent route whose Cluster declares no sleep at all (a database that must never sleep, behind the same gateway). The gateway must carry its traffic at the
 * address every route's default is (`<cluster>-rw.<namespace>.svc:5432`) and never act on the Cluster, however long it idles and however many sessions come and go: no fence, no hibernation, no wake, and no schedule suspended or resumed (the
 * Backups and the schedules are listed, which its Role must allow, and nothing is patched).
 */
describe('a permanent route whose Cluster declares no sleep: passed through at its own Service, never acted on', () => {
  const LEDGER = 'ledger/ledger'
  const ledgerSample = (gateway: { metrics(): string }, name: string, labels = '') => sample(gateway, name, labels === '' ? `database="${LEDGER}"` : `database="${LEDGER}",${labels}`)
  /** The ledger as the operator leaves it: no annotation of ours, one ready instance, its primary named. */
  const ledgerRow = (): ClusterRow => ({ metadata: { annotations: {} }, status: { readyInstances: 1, currentPrimary: 'ledger-1', conditions: [{ type: 'cnpg.io/hibernation', status: 'False' }] } })
  const schedule = (name: string): ScheduleRow => ({ metadata: { name, annotations: {} }, spec: { cluster: { name: 'ledger' } } })

  async function ledgerWorld() {
    let skew = 0
    const api = new FakeApi()
    const db = await StandIn.make()
    const port = await freePort()
    const dialed: { host: string; port: number }[] = []
    // no `target`: the gateway's own default decides where it dials, and the stand-in only stands where that address leads
    const gateway = createGateway({
      api,
      names: { ...names, permanentPorts: { from: 1024, to: 65535 } },
      connect: (to) => (dialed.push(to), connect({ host: '127.0.0.1', port: db.port })),
      now: () => Date.now() + skew,
      holdLimitMs: 3000,
      awakeHoldMs: 1000,
      listenHost: '127.0.0.1',
      log: () => {},
    })
    cleanups.push(() => gateway.stop(), () => db.close())
    api.services = [route('ledger', port, LEDGER)]
    api.clusters.set(LEDGER, ledgerRow())
    api.schedules.set(LEDGER, [schedule('ledger-daily'), schedule('ledger-hourly')])
    await db.listen()
    await gateway.refresh()
    const client = () => {
      const c = new Client(port)
      cleanups.push(() => c.destroy())
      return c
    }
    return { api, db, gateway, client, dialed, advance: (minutes: number) => void (skew += minutes * 60_000) }
  }
  const acted = (api: FakeApi) => api.calls.filter((c) => /^(fence|hibernate|unfence|wake|suspend|resume) /.test(c))

  test('a caller is dialed at the Cluster`s read-write Service, `ledger-rw.ledger.svc` on 5432, and its bytes pass untouched; the Cluster is not even read for an awake database', async () => {
    const { api, gateway, client, dialed } = await ledgerWorld()
    const c = client()
    c.write('the startup message')
    expect((await c.hears(19)).toString()).toBe('the startup message')
    expect(dialed).toEqual([{ host: 'ledger-rw.ledger.svc', port: 5432 }])
    expect(api.count('get')).toBe(0)
    expect(ledgerSample(gateway, 'pg_gateway_sessions_open')).toBe(1)
    expect(ledgerSample(gateway, 'pg_gateway_connections_total')).toBe(1)
  })

  test('over three days idle and sessions coming and going, nothing is fenced, hibernated, woken, suspended or resumed, and the schedules and the Backups are listed and never patched', async () => {
    const { api, gateway, client, advance } = await ledgerWorld()
    for (let day = 0; day < 3; day++) {
      const c = client()
      c.write('x')
      await c.hears(1)
      await gateway.tick()
      c.destroy()
      await sleep(80)
      advance(24 * 60)
      await gateway.tick()
      await gateway.tick()
    }
    expect(acted(api)).toEqual([])
    expect(api.lists.filter((l) => l.startsWith('list clusters'))).toContain('list clusters ledger')
    expect(api.calls).toContain('list schedules ledger/ledger')
    expect(api.lists.filter((l) => l.startsWith('list backups'))).toEqual([])
    for (const stage of ['light', 'full']) {
      expect(ledgerSample(gateway, 'pg_gateway_sleeps_total', `stage="${stage}"`)).toBe(0)
      for (const result of ['woken', 'timed_out', 'refused']) expect(ledgerSample(gateway, 'pg_gateway_wakes_total', `stage="${stage}",result="${result}"`)).toBe(0)
    }
    // the Cluster is as the operator left it: no annotation of ours was ever written
    expect((api.clusters.get(LEDGER) as ClusterRow).metadata?.annotations).toEqual({})
    expect(api.schedules.get(LEDGER)!.map((s) => s.spec?.suspend)).toEqual([undefined, undefined])
  })

  test('stopped by hand (hibernated, and not declared to sleep): the caller is closed at once and nothing is woken; the schedules are suspended and marked by the gateway, and resumed only once the Cluster is awake with a ready instance', async () => {
    const { api, db, gateway, client } = await ledgerWorld()
    // nothing answers the dial, so the caller is read against the Cluster
    await db.close()
    api.clusters.set(LEDGER, { metadata: { annotations: { [HIBERNATION_ANNOTATION]: 'on' } }, status: { readyInstances: 0, conditions: [{ type: 'cnpg.io/hibernation', status: 'True' }] } })
    await gateway.tick()
    const c = client()
    c.write('x')
    await c.isClosed()
    expect(api.count('wake')).toBe(0)
    expect(['light', 'full'].reduce((n, stage) => n + ledgerSample(gateway, 'pg_gateway_wakes_total', `stage="${stage}",result="refused"`), 0)).toBe(1)
    expect(acted(api)).toEqual(['suspend ledger/ledger-daily', 'suspend ledger/ledger-hourly'])
    for (const s of api.schedules.get(LEDGER)!) {
      expect(s.spec?.suspend).toBe(true)
      expect(s.metadata.annotations![SUSPENDED_BY_ANNOTATION]).toBe('pg-gateway')
    }
    // the stop is lifted by hand, and the server is not ready yet: the schedules stay suspended
    api.clusters.set(LEDGER, { metadata: { annotations: {} }, status: { readyInstances: 0, conditions: [{ type: 'cnpg.io/hibernation', status: 'False' }] } })
    await gateway.tick()
    await gateway.tick()
    expect(acted(api)).toEqual(['suspend ledger/ledger-daily', 'suspend ledger/ledger-hourly'])
    // ready: the two it marked are resumed, and nothing else is written
    api.clusters.set(LEDGER, ledgerRow())
    await gateway.tick()
    expect(acted(api)).toEqual(['suspend ledger/ledger-daily', 'suspend ledger/ledger-hourly', 'resume ledger/ledger-daily', 'resume ledger/ledger-hourly'])
    expect(api.schedules.get(LEDGER)!.map((s) => s.spec?.suspend)).toEqual([false, false])
    expect(api.count('fence') + api.count('hibernate') + api.count('unfence')).toBe(0)
  })

  test('a schedule somebody suspended by hand is left as it is, and the gateway resumes only what it marked itself (nothing, here)', async () => {
    const { api, gateway } = await ledgerWorld()
    api.schedules.set(LEDGER, [schedule('ledger-daily'), { ...schedule('ledger-hourly'), spec: { cluster: { name: 'ledger' }, suspend: true } }])
    await gateway.tick()
    await gateway.tick()
    expect(acted(api)).toEqual([])
    expect(api.schedules.get(LEDGER)![1]!.spec?.suspend).toBe(true)
  })
})

/**
 * A sleeping permanent database's backup schedules. A permanent database is a route whose port is in the permanent range; here every test route is made
 * one. The gateway reads the Cluster and its schedules and never dials the database for this.
 */
describe('the schedules of a sleeping permanent database are kept', () => {
  const PERMANENT = { ...names, permanentPorts: { from: 1024, to: 65535 } }
  const schedule = (name: string, over: Partial<{ suspend: boolean; marked: boolean; cluster: string }> = {}): ScheduleRow => ({
    metadata: { name, annotations: over.marked ? { [SUSPENDED_BY_ANNOTATION]: 'pg-gateway' } : {} },
    spec: { cluster: { name: over.cluster ?? 'orders' }, ...(over.suspend === undefined ? {} : { suspend: over.suspend }) },
  })
  const withServer = (cluster: ClusterRow, readyInstances = 1): ClusterRow => ({ ...cluster, status: { ...cluster.status, readyInstances } })
  const rowsOf = (api: FakeApi) => api.schedules.get(DB)!
  const find = (api: FakeApi, name: string) => rowsOf(api).find((r) => r.metadata.name === name)!
  const writes = (api: FakeApi) => api.calls.filter((c) => c.startsWith('suspend ') || c.startsWith('resume '))

  async function permanent(over: Partial<Options> = {}) {
    const made = await setup({ names: PERMANENT, ...over })
    made.api.schedules.set(DB, [schedule('orders-daily'), schedule('orders-hourly')])
    return made
  }

  test('asleep: each schedule that is not suspended is suspended and marked as the gateway`s; one somebody suspended by hand is left as it is, unmarked', async () => {
    const { api, gateway } = await permanent()
    api.clusters.set(DB, asleep(true))
    api.schedules.set(DB, [schedule('orders-daily'), schedule('orders-hourly', { suspend: true })])
    await gateway.tick()
    expect(writes(api)).toEqual(['suspend apps/orders-daily'])
    expect(find(api, 'orders-daily').spec!.suspend).toBe(true)
    expect(find(api, 'orders-daily').metadata.annotations![SUSPENDED_BY_ANNOTATION]).toBe('pg-gateway')
    expect(find(api, 'orders-hourly').metadata.annotations).toEqual({})
  })

  test('asleep by hand (not declared to sleep) is asleep too: a backup of a hibernated Cluster fails either way', async () => {
    const { api, gateway } = await permanent()
    api.clusters.set(DB, asleep(false))
    await gateway.tick()
    expect(writes(api).sort()).toEqual(['suspend apps/orders-daily', 'suspend apps/orders-hourly'])
  })

  test('a second look at a database that is still asleep writes nothing: the schedules are suspended already', async () => {
    const { api, gateway } = await permanent()
    api.clusters.set(DB, asleep(true))
    await gateway.tick()
    api.calls.length = 0
    await gateway.tick()
    await gateway.tick()
    expect(writes(api)).toEqual([])
  })

  test('once the database is awake and has a server that is ready, only the schedules the gateway marked are resumed: the one suspended by hand stays suspended', async () => {
    const { api, gateway } = await permanent()
    api.clusters.set(DB, asleep(true))
    api.schedules.set(DB, [schedule('orders-daily'), schedule('orders-hourly', { suspend: true })])
    await gateway.tick()
    api.calls.length = 0
    api.clusters.set(DB, withServer(awake()))
    await gateway.tick()
    expect(writes(api)).toEqual(['resume apps/orders-daily'])
    expect(find(api, 'orders-daily').spec!.suspend).toBe(false)
    expect(find(api, 'orders-daily').metadata.annotations).toEqual({})
    expect(find(api, 'orders-hourly').spec!.suspend).toBe(true)
    api.calls.length = 0
    await gateway.tick()
    expect(writes(api)).toEqual([])
  })

  test('the mark off is not the database answering: while the server is not ready the schedules stay suspended, and are resumed at the first look after it is', async () => {
    const { api, gateway } = await permanent()
    api.clusters.set(DB, asleep(true))
    await gateway.tick()
    // the mark turned off by a wake: awake by the mark, no server yet
    api.clusters.set(DB, withServer(awake(), 0))
    await gateway.tick()
    expect(writes(api)).toEqual(['suspend apps/orders-daily', 'suspend apps/orders-hourly'])
    api.clusters.set(DB, withServer(awake(), 1))
    await gateway.tick()
    expect(writes(api).slice(2).sort()).toEqual(['resume apps/orders-daily', 'resume apps/orders-hourly'])
  })

  test('a database that never slept has its schedules read at most and none written, even one that is suspended by hand', async () => {
    const { api, gateway } = await permanent()
    api.clusters.set(DB, withServer(awake()))
    api.schedules.set(DB, [schedule('orders-daily', { suspend: true }), schedule('orders-hourly')])
    await gateway.tick()
    expect(writes(api)).toEqual([])
    expect(find(api, 'orders-daily').spec!.suspend).toBe(true)
  })

  test('a branch has no schedules: a route outside the permanent range is not looked at, and the Cluster is not even read', async () => {
    const { api, gateway } = await setup({ names: { ...names, permanentPorts: { from: 15000, to: 15999 } } })
    api.clusters.set(DB, asleep(true))
    api.schedules.set(DB, [schedule('orders-daily')])
    await gateway.tick()
    expect(api.calls.filter((c) => c !== 'listServices')).toEqual([])
  })

  test('only the schedules of the database are touched: another Cluster`s are never listed into the change', async () => {
    const { api, gateway } = await permanent()
    api.clusters.set(DB, asleep(true))
    api.schedules.set(DB, [schedule('orders-daily')])
    api.schedules.set('apps/other', [schedule('other-daily', { cluster: 'other' })])
    await gateway.tick()
    expect(writes(api)).toEqual(['suspend apps/orders-daily'])
    expect(api.calls.some((c) => c.includes('other'))).toBe(false)
  })

  test('a Cluster that is not there, or whose namespace cannot be listed, or whose schedules cannot be listed or changed: nothing is written, the failure is counted and said once, and the next look tries again', async () => {
    const lines: string[] = []
    const { api, gateway } = await permanent({ log: (line) => void lines.push(line) })
    api.clusters.delete(DB)
    await gateway.tick()
    expect(writes(api)).toEqual([])
    api.failList = true
    await gateway.tick()
    await gateway.tick()
    expect(lines.filter((l) => l.includes('listing the Clusters of apps failed') || l.includes('listing the Clusters of apps:'))).toHaveLength(1)
    api.failList = false
    api.clusters.set(DB, asleep(true))
    api.failSchedules = true
    await gateway.tick()
    await gateway.tick()
    expect(writes(api)).toEqual([])
    // two failed lists and two failed schedule reads
    expect(sample(gateway, 'pg_gateway_api_errors_total', '')).toBe(4)
    expect(lines.filter((l) => l.includes('keeping the schedules'))).toHaveLength(1)
    api.failSchedules = false
    await gateway.tick()
    expect(writes(api).sort()).toEqual(['suspend apps/orders-daily', 'suspend apps/orders-hourly'])
    expect(lines.some((l) => l.includes('works again'))).toBe(true)
  })

  test('start keeps them every look, with the routes: a database that goes to sleep is suspended within a refresh', async () => {
    const { api, gateway } = await permanent({ refreshMs: 15 })
    api.clusters.set(DB, withServer(awake()))
    gateway.start()
    await sleep(100)
    expect(writes(api)).toEqual([])
    api.clusters.set(DB, asleep(true))
    await sleep(150)
    expect(writes(api).sort()).toEqual(['suspend apps/orders-daily', 'suspend apps/orders-hourly'])
  })

  test('it opens no session by keeping them: the database sees no connection', async () => {
    const { api, db, gateway } = await permanent()
    await db.listen()
    for (const cluster of [asleep(true), withServer(awake())]) {
      api.clusters.set(DB, cluster)
      await gateway.tick()
    }
    expect(db.connections).toBe(0)
  })

  test('a wake and the schedules together: a caller wakes the database while its schedules are kept, and they are resumed once its server is ready', async () => {
    const { api, db, gateway, client } = await permanent()
    api.clusters.set(DB, asleep(true))
    await gateway.tick()
    api.onWake = () => {
      // what the operator does: the mark goes off, the server starts and is ready
      setTimeout(() => api.clusters.set(DB, withServer(awake())), 60)
      setTimeout(() => void db.listen(), 60)
    }
    const c = client()
    c.write('hi')
    expect((await c.hears(2)).toString()).toBe('hi')
    await gateway.tick()
    expect(writes(api).filter((w) => w.startsWith('resume')).sort()).toEqual(['resume apps/orders-daily', 'resume apps/orders-hourly'])
  })
})

/**
 * The direct dial: a wake reads the pod the Cluster names as its primary, asks the
 * server at that address whether it takes logins, and passes the callers there instead of waiting for the read-write Service to lead to it. The stand-in Service here is a port nothing listens
 * on (a dial of it is refused), so a caller that is served was served by the pod's own address; the stand-in database speaks as much of Postgres's startup as the question needs.
 */
const errorResponse = (sqlstate: string, message: string) => {
  const fields = Buffer.from(`SFATAL\0VFATAL\0C${sqlstate}\0M${message}\0\0`)
  const head = Buffer.alloc(5)
  head.write('E')
  head.writeInt32BE(4 + fields.length, 1)
  return Buffer.concat([head, fields])
}
/** AuthenticationSASL, what a server that takes logins answers a startup message with under scram-sha-256. */
const authRequest = () => {
  const mechanisms = Buffer.from('SCRAM-SHA-256\0\0')
  const head = Buffer.alloc(9)
  head.write('R')
  head.writeInt32BE(8 + mechanisms.length, 1)
  head.writeInt32BE(10, 5)
  return Buffer.concat([head, mechanisms])
}

/** A Postgres as far as the gateway meets it: a startup message for the probe role is answered "starting up" `startingFor` times and then with an authentication request; any other connection is an echo. */
class Postgres {
  /** Questions asked, how many were answered "starting up", and bytes any of them sent after its one startup message (a password, or anything else: must stay 0). */
  asked = 0
  answeredStarting = 0
  sentAfterStartup = 0
  startups: Buffer[] = []
  /** Connections that were not the question: the callers that were passed through. */
  callers = 0
  /** The answers to the first questions, in order (`open` or `starting`), before `startingFor` and the default: a server that is still up for a moment and then stops and starts again. */
  script: ('open' | 'starting')[] = []
  /** How many callers had been passed when each question was asked. */
  callersAtAsk: number[] = []
  constructor(
    readonly db: StandIn,
    public startingFor = 0,
  ) {}
  listen() {
    return this.db.listen((socket) => {
      let first = true
      let asking = false
      socket.on('data', (chunk: Buffer) => {
        if (first) {
          first = false
          if (chunk.includes('pg_gateway_probe')) {
            asking = true
            this.asked += 1
            this.startups.push(Buffer.from(chunk))
            this.callersAtAsk.push(this.callers)
            const scripted = this.script.shift()
            if (scripted === 'open') {
              socket.write(authRequest())
            } else if (scripted === 'starting' || this.startingFor > 0) {
              if (scripted === undefined) this.startingFor -= 1
              this.answeredStarting += 1
              socket.end(errorResponse('57P03', 'the database system is starting up'))
            } else socket.write(authRequest())
            return
          }
          this.callers += 1
        }
        if (asking) {
          this.sentAfterStartup += chunk.length
          return
        }
        socket.write(chunk)
      })
    })
  }
}

const WAKING = (primary: string | null = 'orders-1'): ClusterRow => {
  const cluster = asleep(true)
  return { ...cluster, status: { ...cluster.status, ...(primary ? { currentPrimary: primary } : {}) } }
}
/** Kubernetes stamps a pod's creation in whole seconds. */
const wholeSecond = (ms: number) => new Date(Math.floor(ms / 1000) * 1000).toISOString()
const podRow = (over: { name?: string; uid?: string; ip?: string; ready?: boolean; created?: string; cluster?: string; deleting?: boolean; phase?: string } = {}): PodRow => ({
  metadata: {
    name: over.name ?? 'orders-1',
    uid: over.uid ?? 'uid-1',
    creationTimestamp: over.created ?? wholeSecond(Date.now()),
    labels: { 'cnpg.io/cluster': over.cluster ?? 'orders' },
    ...(over.deleting ? { deletionTimestamp: new Date().toISOString() } : {}),
  },
  status: { phase: over.phase ?? 'Running', podIP: over.ip ?? '192.0.2.7', conditions: [{ type: 'Ready', status: over.ready ? 'True' : 'False' }] },
})
const POD_KEY = 'apps/orders-1'

describe('a wake dials the woken server by its own address once it takes logins', () => {
  /** A gateway whose Service leads nowhere and whose pods are at `ips` (an address to the port of a stand-in database). */
  async function direct(over: Partial<Options> = {}, startingFor = 0) {
    const dead = await freePort()
    const lines: string[] = []
    const ips = new Map<string, number>()
    /** Where the Service leads: nowhere until a test says it leads to the server (`service.port = ...`). */
    const service = { port: dead }
    const made = await setup({
      target: () => ({ host: '127.0.0.1', port: service.port }),
      podTarget: (ip) => ({ host: '127.0.0.1', port: ips.get(ip) ?? dead }),
      askRetryMs: 10,
      locateRetryMs: 30,
      log: (line) => lines.push(line),
      ...over,
    })
    const pg = new Postgres(made.db, startingFor)
    ips.set('192.0.2.7', made.db.port)
    made.api.clusters.set(DB, WAKING())
    return { ...made, pg, lines, ips, dead, service }
  }
  /** What the operator does after the mark goes off: the pod is made `podAfterMs` later and Postgres listens `listenAfterMs` after the wake. */
  const comes = (made: { api: FakeApi; pg: Postgres }, podAfterMs: number, listenAfterMs: number, pod: PodRow = podRow()) => {
    made.api.onWake = () => {
      setTimeout(() => made.api.pods.set(POD_KEY, pod), podAfterMs)
      setTimeout(() => void made.pg.listen(), listenAfterMs)
    }
  }

  test('the caller is passed to the pod`s own address, with what it sent while it waited, though the Service leads nowhere; the wake is counted and its log line says how it came', async () => {
    const made = await direct()
    const { api, gateway, client, pg, lines } = made
    comes(made, 40, 100)
    const c = client()
    c.write('hello while asleep')
    expect((await c.hears(18)).toString()).toBe('hello while asleep')
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(wakes(gateway, 'timed_out')).toBe(0)
    expect(pg.callers).toBe(1)
    // one read of the Cluster (the wake's own), one turning of the mark, and the pod read by the name the Cluster gave
    expect(api.calls.filter((call) => call !== 'listServices' && !call.startsWith('getPod'))).toEqual([`get ${DB}`, `wake ${DB}`])
    expect(api.calls.filter((call) => call.startsWith('getPod')).every((call) => call === `getPod ${POD_KEY}`)).toBe(true)
    expect(lines.some((l) => /answered [\d.]+ s after the full wake began, by the server's own address \(orders-1 at 192\.0\.2\.7;/.test(l))).toBe(true)
  })

  test('a server that listens before it takes logins is asked until it does: the caller is passed only then, and the "starting up" answers are counted and never answered', async () => {
    const made = await direct({}, 4)
    const { gateway, client, pg, lines } = made
    comes(made, 20, 40)
    const c = client()
    c.write('x')
    // while it answers "starting up" the caller is held with nothing back, and nothing has been passed through
    await sleep(25)
    await c.hears(1)
    expect(pg.answeredStarting).toBe(4)
    expect(pg.asked).toBe(5)
    expect(pg.callers).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(lines.some((l) => l.includes('4 "starting up"'))).toBe(true)
  })

  test('the question is one startup message of a role that is never a login, and nothing after it: no password, no database, no session, and it is not counted as one', async () => {
    const made = await direct({}, 3)
    const { gateway, client, pg } = made
    comes(made, 20, 40)
    const c = client()
    c.write('x')
    await c.hears(1)
    expect(pg.asked).toBe(4)
    expect(pg.sentAfterStartup).toBe(0)
    for (const startup of pg.startups) {
      expect(startup.readInt32BE(0)).toBe(startup.length)
      expect(startup.readInt32BE(4)).toBe(196608)
      expect(startup.subarray(8).toString()).toBe('user\0pg_gateway_probe\0\0')
    }
    // the asks are no session: the one session is the caller's, and the connections counted are the callers'
    expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(1)
    expect(sample(gateway, 'pg_gateway_connections_total')).toBe(1)
    expect(pg.callers).toBe(1)
  })

  test('the caller is not passed while the pod has no address, whatever the Service would say: the pod is read until it has one', async () => {
    const made = await direct()
    const { api, client, pg } = made
    api.onWake = () => {
      void pg.listen()
      // an address-less pod first (Pending), then the same pod with its address
      api.pods.set(POD_KEY, { ...podRow(), status: { conditions: [] } })
      setTimeout(() => api.pods.set(POD_KEY, podRow()), 150)
    }
    const c = client()
    c.write('x')
    await sleep(100)
    expect(pg.asked).toBe(0)
    expect(c.length).toBe(0)
    await c.hears(1)
    expect(pg.callers).toBe(1)
    expect(api.count(`getPod ${POD_KEY}`)).toBeGreaterThanOrEqual(3)
  })

  test('ten callers share one wake and one asking: one read of the Cluster, one patch, every caller passed to the pod`s address', async () => {
    const made = await direct({}, 2)
    const { api, gateway, client, pg } = made
    comes(made, 20, 60)
    const clients = Array.from({ length: 10 }, (_, i) => {
      const c = client()
      c.write(`caller-${i}`)
      return c
    })
    await Promise.all(clients.map((c, i) => c.hears(`caller-${i}`.length)))
    expect(api.count('wake')).toBe(1)
    expect(api.count('get ')).toBe(1)
    expect(pg.callers).toBe(10)
    expect(wakes(gateway, 'woken')).toBe(1)
    // one asking for all ten: three asks (two "starting up" and the yes), never ten at a time
    expect(pg.asked).toBeLessThanOrEqual(4)
  })

  test('the asking is for a caller that waits: when the last caller leaves it stops, and the server sees no further question', async () => {
    const made = await direct({}, 1_000_000)
    const { client, pg } = made
    comes(made, 20, 20)
    const c = client()
    c.write('x')
    // the asking runs on a 20 ms timer: it has asked more than twice by 150 ms on an idle machine, and later on a loaded one (seen once in
    // ten full runs, 2026-11-26), so the test waits for the third question instead of a fixed time
    await until(() => pg.asked > 2)
    expect(pg.asked).toBeGreaterThan(2)
    c.destroy()
    await sleep(60)
    const asked = pg.asked
    await sleep(200)
    expect(pg.asked).toBe(asked)
    expect(pg.callers).toBe(0)
  })

  test('a caller that arrives after the wake`s callers have all left joins the wake that is still going: no second read of the Cluster, no second patch, and it is passed as soon as the server takes logins', async () => {
    const made = await direct()
    const { api, gateway, client, pg } = made
    // the pod is made after the wake begins, as the operator makes it; nothing listens yet: the first caller leaves, the wake stays
    api.onWake = () => void api.pods.set(POD_KEY, podRow())
    const first = client()
    first.write('one')
    await sleep(120)
    first.destroy()
    await sleep(120)
    expect(api.count('wake')).toBe(1)
    await pg.listen()
    const second = client()
    second.write('two')
    expect((await second.hears(3)).toString()).toBe('two')
    expect(api.count('wake')).toBe(1)
    expect(api.count('get ')).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(pg.callers).toBe(1)
  })

  test('an address found while the last caller was leaving is not used by a caller that joins later: it reads the pod and asks for itself, and is never passed to another database that took the address', async () => {
    const made = await direct()
    const { api, client, pg, ips, gateway } = made
    // the read that follows the server's yes is slow, and the only caller leaves during it: the address is found with nobody waiting
    const read = api.getPod.bind(api)
    let slowed = false
    api.getPod = async (namespace: string, name: string) => {
      if (pg.asked >= 1 && !slowed) {
        slowed = true
        await sleep(200)
      }
      return read(namespace, name)
    }
    api.onWake = () => {
      api.pods.set(POD_KEY, podRow())
      void pg.listen()
    }
    const first = client()
    first.write('one')
    await first.until(() => pg.asked >= 1)
    first.destroy()
    await sleep(500)
    expect(pg.callers).toBe(0)
    // the pod is replaced and its address, 192.0.2.7, is now another database's pod's
    api.pods.delete(POD_KEY)
    const other = await StandIn.make()
    cleanups.push(() => other.close())
    const otherPg = new Postgres(other)
    await otherPg.listen()
    ips.set('192.0.2.7', other.port)
    const readsBefore = api.count('getPod')
    const second = client()
    second.write('two')
    await sleep(250)
    // not passed anywhere, and not asked at the old address: the pod it reads is gone
    expect(otherPg.callers).toBe(0)
    expect(otherPg.asked).toBe(0)
    expect(second.length).toBe(0)
    expect(second.closed).toBe(false)
    expect(api.count('getPod')).toBeGreaterThan(readsBefore)
    // the replacement, at an address of its own, is the one it is passed to
    ips.set('192.0.2.9', made.db.port)
    api.pods.set(POD_KEY, podRow({ uid: 'uid-2', ip: '192.0.2.9' }))
    expect((await second.hears(3)).toString()).toBe('two')
    expect(otherPg.callers).toBe(0)
    expect(pg.callers).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(1)
  })

  test('a caller that joins a wake nobody was waiting on is no wake time: it is counted woken and gives no sample to the histogram', async () => {
    const made = await direct()
    const { api, client, pg, gateway } = made
    api.onWake = () => void api.pods.set(POD_KEY, podRow())
    const first = client()
    first.write('one')
    await sleep(80)
    first.destroy()
    await sleep(200)
    await pg.listen()
    const second = client()
    second.write('two')
    await second.hears(3)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(seconds(gateway, 'count')).toBe(0)
    expect(seconds(gateway, 'sum')).toBe(0)
    expect(seconds(gateway, 'bucket,+Inf')).toBe(0)
  })

  test('a caller that joins such a wake near its deadline is given at least the awake hold, as main would give it: served after the wake`s deadline, and the wake that never answered in its limit is counted failed once, not woken as well', async () => {
    const made = await direct({ holdLimitMs: 300, awakeHoldMs: 1000 })
    const { api, client, pg, gateway } = made
    api.onWake = () => {
      setTimeout(() => api.pods.set(POD_KEY, podRow()), 450)
      setTimeout(() => void pg.listen(), 450)
    }
    const first = client()
    first.write('one')
    await sleep(60)
    first.destroy()
    await sleep(190)
    // 250 ms in, 50 ms from the wake's deadline; the server takes logins at 450 ms, after the deadline
    const second = client()
    second.write('two')
    expect((await second.hears(3)).toString()).toBe('two')
    expect(wakes(gateway, 'timed_out')).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(0)
    expect(seconds(gateway, 'count')).toBe(0)
  })

  test('a wake nothing ever answered, with a late joiner waiting at its deadline, is counted timed out once (so PostgresWakeFailed can fire); the joiner is closed at its own limit as a hold that gave up', async () => {
    const made = await direct({ holdLimitMs: 200, awakeHoldMs: 300 })
    const { client, gateway } = made
    const first = client()
    first.write('one')
    await sleep(60)
    first.destroy()
    await sleep(60)
    const started = Date.now()
    const second = client()
    second.write('two')
    await second.isClosed()
    expect(Date.now() - started).toBeGreaterThanOrEqual(280)
    expect(holds(gateway, 'gave_up')).toBe(1)
    expect(wakes(gateway, 'timed_out')).toBe(1)
  })

  test('a wake whose server answered the question (it took logins) is never counted failed at its deadline, though no caller was passed (the dials of the pod were refused)', async () => {
    let asks = 0
    const ports = { live: 0, dead: 0 }
    const made = await direct({ holdLimitMs: 300, awakeHoldMs: 600, podTarget: (ip) => ({ host: '127.0.0.1', port: ip === '192.0.2.7' && ++asks === 1 ? ports.live : ports.dead }) })
    const { api, client, pg, gateway } = made
    ports.live = made.db.port
    ports.dead = made.dead
    api.onWake = () => {
      api.pods.set(POD_KEY, podRow())
      void pg.listen()
    }
    const first = client()
    first.write('one')
    await sleep(100)
    first.destroy()
    await sleep(60)
    expect(pg.asked).toBe(1)
    const second = client()
    second.write('two')
    await second.isClosed()
    expect(wakes(gateway, 'timed_out')).toBe(0)
    expect(holds(gateway, 'gave_up')).toBe(1)
  })

  test('a wake kept for late joiners past its deadline is still the database`s one wake until the last of them leaves: a new caller joins it, no second read of the Cluster and no second patch; the wake then goes with the last joiner', async () => {
    const made = await direct({ holdLimitMs: 200, awakeHoldMs: 700 })
    const { api, client, pg } = made
    api.onWake = () => void api.pods.set(POD_KEY, podRow())
    const first = client()
    first.write('one')
    await sleep(40)
    first.destroy()
    await sleep(40)
    const late = client()
    late.write('late')
    // past the wake's 200 ms deadline, with the late joiner still waiting
    await sleep(300)
    const readsBefore = api.count('get ')
    const another = client()
    another.write('another')
    await pg.listen()
    expect((await another.hears(7)).toString()).toBe('another')
    expect((await late.hears(4)).toString()).toBe('late')
    expect(api.count('get ')).toBe(readsBefore)
    expect(api.count('wake')).toBe(1)
  })

  test('a failed read of the pod that goes right again says so once: the fault and its recovery are one place, for the wake and for the hold', async () => {
    const made = await direct()
    const { api, client, pg, lines } = made
    api.pods.set(POD_KEY, 'throw')
    api.onWake = () => {
      setTimeout(() => api.pods.set(POD_KEY, podRow()), 120)
      void pg.listen()
    }
    const c = client()
    c.write('x')
    await c.hears(1)
    expect(lines.filter((l) => l.includes('reading the server of') && l.includes('forbidden'))).toHaveLength(1)
    expect(lines.filter((l) => l.includes('reading the server of') && l.includes('works again'))).toHaveLength(1)
  })

  test('an unsettled wake nobody joins is dropped at its deadline uncounted, and the next caller after it reads the Cluster and wakes again', async () => {
    const made = await direct({ holdLimitMs: 300 })
    const { api, gateway, client } = made
    const first = client()
    first.write('one')
    await sleep(60)
    first.destroy()
    await sleep(500)
    expect(wakes(gateway, 'timed_out')).toBe(0)
    const second = client()
    second.write('two')
    await sleep(100)
    expect(api.count('wake')).toBe(2)
    expect(api.count('get ')).toBe(2)
  })

  test('the Service is the way in when the pod cannot be read (the Role lacks get on pods, or the API is down): the wake goes on as it did, the failure is counted and said once, and no address is asked', async () => {
    const made = await direct()
    const { api, db, gateway, client, lines, pg, service } = made
    service.port = db.port
    api.pods.set(POD_KEY, 'throw')
    api.onWake = () => void sleep(150).then(() => pg.listen())
    const c = client()
    c.write('x')
    await c.hears(1)
    expect(pg.callers).toBe(1)
    expect(pg.asked).toBe(0)
    expect(wakes(gateway, 'woken')).toBe(1)
    expect(lines.filter((l) => l.includes('reading the server of') && l.includes('forbidden'))).toHaveLength(1)
    expect(sample(gateway, 'pg_gateway_api_errors_total', '')).toBeGreaterThanOrEqual(2)
    expect(lines.some((l) => /after the full wake began, through the Service/.test(l))).toBe(true)
  })

  test('a Cluster that names no primary is a wake through the Service: no pod is read', async () => {
    const made = await direct()
    const { api, db, gateway, client, pg, service } = made
    service.port = db.port
    api.clusters.set(DB, WAKING(null))
    api.pods.set(POD_KEY, podRow())
    api.onWake = () => void sleep(100).then(() => pg.listen())
    const c = client()
    c.write('x')
    await c.hears(1)
    expect(api.calls.some((call) => call.startsWith('getPod'))).toBe(false)
    expect(pg.asked).toBe(0)
    expect(wakes(gateway, 'woken')).toBe(1)
  })

  // each pod is made when the wake begins (as the operator makes one), so that what refuses it is its own fault and not that it was made at file load, before the wake, when its creation time is older than the wake's
  const notThisWakes: [string, () => PodRow][] = [
    ['older than the wake (a server that was already there is not the one this wake made)', () => podRow({ created: wholeSecond(Date.now() - 3_600_000) })],
    ['on its way out', () => podRow({ deleting: true })],
    ['that is Failed (it can keep an address the network has freed)', () => podRow({ phase: 'Failed' })],
    ['that is Pending', () => podRow({ phase: 'Pending' })],
    ['of another Cluster (branches of one parent share its login)', () => podRow({ cluster: 'other' })],
    ['one with no creation time', () => ({ ...podRow(), metadata: { ...podRow().metadata, creationTimestamp: undefined } })],
  ]
  for (const [what, make] of notThisWakes) {
    test(`a pod ${what} is never asked or dialed: the wake is through the Service`, async () => {
      const made = await direct()
      const { api, db, gateway, client, pg, service } = made
      service.port = db.port
      api.onWake = () => {
        api.pods.set(POD_KEY, make())
        void sleep(150).then(() => pg.listen())
      }
      const c = client()
      c.write('x')
      await c.hears(1)
      // the Service's server was the stand-in itself: what matters is that the question was never asked
      expect(pg.asked).toBe(0)
      expect(api.count(`getPod ${POD_KEY}`)).toBeGreaterThan(0)
      expect(wakes(gateway, 'woken')).toBe(1)
    })
  }

  test('an address is never reused from an earlier wake: the second wake reads the pod afresh, passes over the old pod that is on its way out and dials the new one, and the old address sees nothing', async () => {
    const made = await direct()
    const { api, client, pg, ips, gateway } = made
    comes(made, 20, 40)
    const first = client()
    first.write('one')
    await first.hears(3)
    expect(pg.callers).toBe(1)
    first.destroy()
    await sleep(60)
    // asleep again; the old pod is still there on its way out when the next wake begins, and a new pod takes its place a moment later
    const next = await StandIn.make()
    cleanups.push(() => next.close())
    const fresh = new Postgres(next)
    ips.set('192.0.2.8', next.port)
    api.pods.set(POD_KEY, podRow({ deleting: true }))
    api.clusters.set(DB, WAKING())
    const askedBefore = pg.asked
    const callersBefore = pg.callers
    api.onWake = () => {
      void fresh.listen()
      setTimeout(() => api.pods.set(POD_KEY, podRow({ uid: 'uid-2', ip: '192.0.2.8' })), 120)
    }
    const second = client()
    second.write('two')
    expect((await second.hears(3)).toString()).toBe('two')
    expect(fresh.callers).toBe(1)
    expect(pg.asked).toBe(askedBefore)
    expect(pg.callers).toBe(callersBefore)
    expect(wakes(gateway, 'woken')).toBe(2)
  })

  test('a pod that is replaced while it is asked starts the finding again from the read: the address asked next is the new pod`s', async () => {
    const made = await direct({}, 1_000_000)
    const { api, client, ips, pg } = made
    const next = await StandIn.make()
    cleanups.push(() => next.close())
    const fresh = new Postgres(next)
    ips.set('192.0.2.8', next.port)
    api.onWake = () => {
      api.pods.set(POD_KEY, podRow())
      void pg.listen()
      void fresh.listen()
      // the first pod only ever says "starting up" (it is the one that goes), the replacement takes logins
      setTimeout(() => api.pods.set(POD_KEY, podRow({ uid: 'uid-2', ip: '192.0.2.8' })), 120)
    }
    const c = client()
    c.write('x')
    await c.hears(1)
    expect(pg.asked).toBeGreaterThan(2)
    expect(pg.callers).toBe(0)
    expect(fresh.callers).toBe(1)
  })

  test('a server that took logins a moment ago and refuses the dial (the pod went) is found again: the address is cleared and read afresh, and the caller is served by the next pod', async () => {
    const next = await StandIn.make()
    cleanups.push(() => next.close())
    // the first pod's address answers the question once and then refuses every dial; the replacement takes its place a moment later
    let asks = 0
    const ports = { live: 0, next: next.port, dead: 0 }
    const made = await direct({ podTarget: (ip) => ({ host: '127.0.0.1', port: ip === '192.0.2.7' ? (++asks === 1 ? ports.live : ports.dead) : ports.next }) })
    const { api, client, pg, gateway } = made
    ports.live = made.db.port
    ports.dead = made.dead
    const fresh = new Postgres(next)
    api.onWake = () => {
      api.pods.set(POD_KEY, podRow())
      void pg.listen()
      void fresh.listen()
      setTimeout(() => api.pods.set(POD_KEY, podRow({ uid: 'uid-2', ip: '192.0.2.8' })), 80)
    }
    const c = client()
    c.write('x')
    await c.hears(1)
    expect(pg.asked).toBe(1)
    expect(pg.callers).toBe(0)
    expect(fresh.callers).toBe(1)
    expect(wakes(gateway, 'woken')).toBe(1)
  })

  describe('a caller that arrives after the wake settled and before the same pod is Ready', () => {
    const awakeWithPrimary = (): ClusterRow => ({ metadata: { annotations: {} }, status: { currentPrimary: 'orders-1', conditions: [{ type: 'cnpg.io/hibernation', status: 'False' }] } })
    /** A wake that has settled through the pod's address, with the pod still not Ready. */
    async function settled(over: Partial<Options> = {}) {
      const made = await direct({ awakeHoldMs: 600, ...over })
      comes(made, 20, 40)
      const c = made.client()
      c.write('one')
      await c.hears(3)
      c.destroy()
      await sleep(40)
      made.api.clusters.set(DB, awakeWithPrimary())
      return made
    }

    test('it is asked and dialed at the pod`s own address (the Service does not lead there yet), counted as a hold that reached', async () => {
      const { client, pg, gateway } = await settled()
      const c = client()
      c.write('two')
      expect((await c.hears(3)).toString()).toBe('two')
      expect(pg.callers).toBe(2)
      expect(holds(gateway, 'reached')).toBe(1)
      expect(wakes(gateway, 'woken')).toBe(1)
    })

    test('once the pod is Ready the Service is the way in, and the address is not used again', async () => {
      const made = await settled()
      made.api.pods.set(POD_KEY, podRow({ ready: true }))
      const c = made.client()
      c.write('two')
      await c.isClosed()
      expect(made.pg.callers).toBe(1)
      expect(holds(made.gateway, 'gave_up')).toBe(1)
    })

    test('a pod that is another one (its UID differs) is not used: the address of an earlier reading is never dialed', async () => {
      const made = await settled()
      made.api.pods.set(POD_KEY, podRow({ uid: 'uid-other' }))
      const c = made.client()
      c.write('two')
      await c.isClosed()
      expect(made.pg.callers).toBe(1)
    })

    test('a pod that cannot be read is no address: the hold goes on through the Service', async () => {
      const made = await settled()
      made.api.pods.set(POD_KEY, 'throw')
      const c = made.client()
      c.write('two')
      await c.isClosed()
      expect(made.pg.callers).toBe(1)
    })
  })

  test('with the wake`s mark patch refused nothing is asked: no pod is read for a wake that was not made', async () => {
    const made = await direct()
    const { api, client } = made
    api.failWake = true
    const c = client()
    c.write('x')
    await c.isClosed()
    expect(api.calls.some((call) => call.startsWith('getPod'))).toBe(false)
  })
})

describe('what a server says to the question', () => {
  const error = (state: string) => errorResponse(state, 'x')
  test('an authentication request of any kind, or any error but "cannot connect now", is a server that takes logins; 57P03 is one that is starting or stopping', () => {
    expect(readAnswer(authRequest())).toBe('open')
    expect(readAnswer(Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0]))).toBe('open')
    expect(readAnswer(error('57P03'))).toBe('starting')
    expect(readAnswer(error('28000'))).toBe('open')
    expect(readAnswer(error('28P01'))).toBe('open')
    expect(readAnswer(error('53300'))).toBe('open')
  })
  test('it waits for the whole of an error before it reads its state, and gives no answer to what is not a Postgres', () => {
    const whole = error('57P03')
    expect(readAnswer(whole.subarray(0, 3))).toBeUndefined()
    expect(readAnswer(whole.subarray(0, whole.length - 3))).toBeUndefined()
    expect(readAnswer(Buffer.from('HTTP/1.1 400 Bad Request\r\n'))).toBe('unreachable')
    expect(readAnswer(Buffer.from([0x45, 0, 0, 0, 2]))).toBe('unreachable')
    expect(readAnswer(Buffer.from([0x45, 0, 0, 0, 6, 0, 0]))).toBe('unreachable')
  })
})


/**
 * The two stages of sleep. The clock is the real one plus a skew the test moves, so that minutes pass at once; a
 * test that holds a caller or a wake gives it limits so long that a skew cannot trip them. The Cluster is the in-memory one: the gateway's patches change it as the operator would.
 */
const MIN = 60_000
const clockOf = () => {
  let skew = 0
  return { now: () => Date.now() + skew, advance: (minutes: number) => void (skew += minutes * MIN) }
}
const isoMinutesAgo = (clock: { now(): number }, minutes: number) => new Date(clock.now() - minutes * MIN).toISOString()

/** A Cluster declared to sleep: `light` minutes (the idle minutes annotation), `full` minutes, `'never'` or `null` for none declared. */
const sleeper = (o: { light?: number; full?: number | 'never' | null; fence?: string; idleSince?: string; hibernated?: boolean; condition?: boolean; declared?: boolean; ready?: number } = {}): ClusterRow => {
  const annotations: Record<string, string> = {}
  if (o.declared !== false) annotations[SLEEP_ENABLED_ANNOTATION] = 'true'
  annotations[SLEEP_IDLE_MINUTES_ANNOTATION] = String(o.light ?? 30)
  if (o.full !== null) annotations[SLEEP_FULL_MINUTES_ANNOTATION] = String(o.full ?? 360)
  if (o.fence !== undefined) annotations[FENCED_INSTANCES_ANNOTATION] = o.fence
  if (o.idleSince !== undefined) annotations[GATEWAY_IDLE_SINCE_ANNOTATION] = o.idleSince
  if (o.hibernated) annotations[HIBERNATION_ANNOTATION] = 'on'
  return {
    metadata: { annotations },
    status: { readyInstances: o.ready ?? 1, currentPrimary: 'orders-1', conditions: [{ type: 'cnpg.io/hibernation', status: o.condition ? 'True' : 'False' }] },
  }
}
/** A sleeper in light sleep since `minutesAgo` (the fence and the gateway's mark). */
const lightSleeper = (clock: { now(): number }, minutesAgo: number, o: Parameters<typeof sleeper>[0] = {}) => sleeper({ ...o, fence: FENCE_ALL, idleSince: isoMinutesAgo(clock, minutesAgo) })

describe('the two stages of sleep, decided by the gateway', () => {
  /** A gateway of a permanent database (so it has Backups) whose Service leads nowhere and whose pod's address is `192.0.2.7`; the Cluster changes as the operator would change it. */
  async function world(over: Partial<Options> = {}, forBranch = false) {
    const dead = await freePort()
    const lines: string[] = []
    const ips = new Map<string, number>()
    /** Where the Service leads: the stand-in database (as the Service of an awake database does) unless a test says otherwise. */
    const service = { port: dead }
    const clock = clockOf()
    const made = await setup({
      names: forBranch ? names : { ...names, permanentPorts: { from: 1024, to: 65535 } },
      now: clock.now,
      target: () => ({ host: '127.0.0.1', port: service.port }),
      podTarget: (ip) => ({ host: '127.0.0.1', port: ips.get(ip) ?? dead }),
      askRetryMs: 10,
      locateRetryMs: 30,
      log: (line) => lines.push(line),
      ...over,
    })
    const pg = new Postgres(made.db)
    ips.set('192.0.2.7', made.db.port)
    service.port = made.db.port
    made.api.realistic = true
    /** A server the Service can be made to lead to: its connections are the dials of the Service that got through. */
    const svc = await StandIn.make()
    cleanups.push(() => svc.close())
    return { ...made, pg, lines, ips, service, clock, dead, svc }
  }
  const changes = (api: FakeApi) => api.calls.filter((c) => /^(fence|hibernate|unfence|wake) /.test(c))
  const annotationsOf = (api: FakeApi) => (api.clusters.get(DB) as ClusterRow).metadata!.annotations!
  const aMinute = { light: 1 }

  describe('into light sleep', () => {
    test('after the light time with no session: one patch sets the fence on every instance and the gateway`s mark, the moment the last session ended (here, when it first met the database); not a minute before', async () => {
      const { api, clock, gateway } = await world()
      const met = clock.now()
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(29)
      await gateway.tick()
      expect(changes(api)).toEqual([])
      clock.advance(2)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
      const annotations = annotationsOf(api)
      expect(annotations[FENCED_INSTANCES_ANNOTATION]).toBe(FENCE_ALL)
      expect(Math.abs(Date.parse(annotations[GATEWAY_IDLE_SINCE_ANNOTATION]!) - met)).toBeLessThan(5000)
      // the mark is an RFC 3339 time that Go's parser takes (kube-state-metrics reads it as a number, and a mark that does not parse pages): UTC with the Z, as `toISOString` writes it
      expect(annotations[GATEWAY_IDLE_SINCE_ANNOTATION]).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/)
      // nothing else of the Cluster is touched, and it is not fenced twice
      expect(annotations[HIBERNATION_ANNOTATION]).toBeUndefined()
      clock.advance(5)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
      expect(sleeps(gateway, 'light')).toBe(1)
      expect(sleeps(gateway, 'full')).toBe(0)
    })

    test('the idle time runs from the moment the last session ended, not from when the gateway met the database: a session of 40 minutes is not idle time', async () => {
      const { api, db, clock, gateway, client } = await world()
      api.clusters.set(DB, sleeper())
      await db.listen()
      await gateway.tick()
      const c = client()
      c.write('x')
      await c.hears(1)
      clock.advance(40)
      await gateway.tick()
      // a session is open: idle time does not run
      expect(changes(api)).toEqual([])
      c.destroy()
      await sleep(80)
      clock.advance(29)
      await gateway.tick()
      expect(changes(api)).toEqual([])
      clock.advance(2)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
    })

    test('not while a caller waits on the database, however long: a caller on its way in is use', async () => {
      const { api, clock, gateway, client } = await world({ awakeHoldMs: 2_000_000_000, holdLimitMs: 2_000_000_000 })
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      const c = client()
      c.write('x')
      await sleep(150)
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([])
      // it goes: the idle time starts again from there
      c.destroy()
      await sleep(900)
      await gateway.tick()
      expect(changes(api)).toEqual([])
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
    })

    test('not while a wake is under way or unsettled, and a woken server stays up its whole light time (the idle time starts at the wake)', async () => {
      const { api, clock, gateway, client } = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000 })
      api.clusters.set(DB, sleeper({ hibernated: true, condition: true }))
      await gateway.tick()
      const c = client()
      c.write('x')
      await sleep(150)
      expect(api.count('wake')).toBe(1)
      c.destroy()
      await sleep(900)
      // the server never came: the wake is unsettled and kept
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([`wake ${DB}`])
    })

    test('a database that was asleep and is awake now, whoever woke it, starts its idle time at the look that sees it awake', async () => {
      const { api, clock, gateway } = await world()
      api.clusters.set(DB, sleeper({ hibernated: true, condition: true }))
      await gateway.tick()
      clock.advance(100)
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(29)
      await gateway.tick()
      expect(changes(api)).toEqual([])
      clock.advance(2)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
    })

    test('not while a Backup of its Cluster is unfinished, and a Backup of another Cluster or a finished one does not hold it', async () => {
      const { api, clock, gateway, lines } = await world()
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(31)
      api.backups.set('apps/b', [{ spec: { cluster: { name: 'orders' } }, status: { phase: 'running' } }])
      await gateway.tick()
      expect(changes(api)).toEqual([])
      api.backups.set('apps/b', [{ spec: { cluster: { name: 'orders' } } }])
      await gateway.tick()
      expect(changes(api)).toEqual([])
      api.backups.set('apps/b', [
        { spec: { cluster: { name: 'other' } }, status: { phase: 'running' } },
        { spec: { cluster: { name: 'orders' } }, status: { phase: 'completed' } },
        { spec: { cluster: { name: 'orders' } }, status: { phase: 'failed' } },
      ])
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
      expect(lines.length).toBeGreaterThan(0)
    })

    test('a list of Backups that cannot be read is an unfinished Backup: nothing sleeps on a guess, and the fault is counted and said once', async () => {
      const { api, clock, gateway, lines } = await world()
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(31)
      api.failBackups = true
      await gateway.tick()
      await gateway.tick()
      expect(changes(api)).toEqual([])
      expect(lines.filter((l) => l.includes('listing the Backups of apps'))).toHaveLength(1)
      api.failBackups = false
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
    })

    test('a branch has no Backups, so none is listed for it', async () => {
      const { api, clock, gateway } = await world({}, true)
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
      expect(api.lists.filter((l) => l.startsWith('list backups'))).toEqual([])
    })

    test('only a database declared to sleep with a light time is put to sleep; one stopped by hand is left as it is', async () => {
      const { api, clock, gateway } = await world()
      for (const row of [
        sleeper({ declared: false }),
        { ...sleeper(), metadata: { annotations: { [SLEEP_ENABLED_ANNOTATION]: 'true' } } } as ClusterRow,
        sleeper({ fence: '["orders-1"]' }),
        sleeper({ fence: FENCE_ALL }),
        sleeper({ hibernated: true, condition: true, declared: false }),
      ]) {
        api.clusters.set(DB, row)
        await gateway.tick()
        clock.advance(1000)
        await gateway.tick()
      }
      expect(changes(api)).toEqual([])
    })

    test('a database with no route is never touched, whatever its Cluster says', async () => {
      const { api, clock, gateway } = await world()
      api.clusters.set('ledger/ledger', sleeper())
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(1000)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
      expect(api.calls.some((c) => c.includes('ledger/'))).toBe(false)
      expect(api.lists).not.toContain('list clusters ledger')
    })

    test('a failed list of Clusters changes nothing, is said once and counted each time; the next look goes on', async () => {
      const { api, clock, gateway, lines } = await world()
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(31)
      api.failList = true
      await gateway.tick()
      await gateway.tick()
      expect(changes(api)).toEqual([])
      expect(lines.filter((l) => l.includes('listing the Clusters of apps'))).toHaveLength(1)
      expect(sample(gateway, 'pg_gateway_api_errors_total', '')).toBe(2)
      api.failList = false
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
      expect(lines.some((l) => l.includes('listing the Clusters of apps works again'))).toBe(true)
    })

    test('the Clusters of a namespace are listed once per look, not once per database', async () => {
      const { api, gateway, port } = await world()
      const other = await freePort()
      api.services = [route('orders', port), route('second', other, 'apps/second')]
      await gateway.refresh()
      api.clusters.set(DB, sleeper())
      api.clusters.set('apps/second', sleeper())
      await gateway.tick()
      expect(api.lists.filter((l) => l === 'list clusters apps')).toHaveLength(1)
    })
  })

  describe('into full sleep', () => {
    test('from light sleep, once the full time has passed counted from the gateway`s mark: the hibernation mark is set and the fence and the mark are kept; they come off only once the pod is gone (the condition is true)', async () => {
      const { api, clock, gateway } = await world()
      api.clusters.set(DB, lightSleeper(clock, 359))
      await gateway.tick()
      expect(changes(api)).toEqual([])
      clock.advance(2)
      await gateway.tick()
      expect(changes(api)).toEqual([`hibernate ${DB}`])
      expect(annotationsOf(api)[HIBERNATION_ANNOTATION]).toBe('on')
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBe(FENCE_ALL)
      expect(sleeps(gateway, 'full')).toBe(1)
      // the pod is still there: the fence stays (lifted now it would start Postgres for a moment)
      await gateway.tick()
      await gateway.tick()
      expect(changes(api)).toEqual([`hibernate ${DB}`])
      // the pod is gone
      const row = api.clusters.get(DB) as ClusterRow
      api.clusters.set(DB, { ...row, status: { ...row.status, conditions: [{ type: 'cnpg.io/hibernation', status: 'True' }] } })
      await gateway.tick()
      expect(changes(api)).toEqual([`hibernate ${DB}`, `unfence ${DB}`])
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
      expect(annotationsOf(api)[GATEWAY_IDLE_SINCE_ANNOTATION]).toBeUndefined()
      expect(annotationsOf(api)[HIBERNATION_ANNOTATION]).toBe('on')
      await gateway.tick()
      expect(changes(api)).toEqual([`hibernate ${DB}`, `unfence ${DB}`])
    })

    test('the fence and the gateway`s mark are never apart: through light sleep, the way on to full sleep, the full sleep itself and both wakes, every state the Cluster is left in has both or neither (a fence without its mark reads as stopped by hand and pages)', async () => {
      const { api, clock, gateway, client, pg } = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000 })
      const together = () => {
        const a = annotationsOf(api)
        expect(a[FENCED_INSTANCES_ANNOTATION] === undefined, JSON.stringify(a)).toBe(a[GATEWAY_IDLE_SINCE_ANNOTATION] === undefined)
      }
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      together()
      clock.advance(31)
      await gateway.tick()
      together()
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBe(FENCE_ALL)
      clock.advance(360)
      await gateway.tick()
      together()
      expect(annotationsOf(api)[HIBERNATION_ANNOTATION]).toBe('on')
      const row = api.clusters.get(DB) as ClusterRow
      api.clusters.set(DB, { ...row, status: { ...row.status, conditions: [{ type: 'cnpg.io/hibernation', status: 'True' }] } })
      await gateway.tick()
      together()
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
      // a full wake, then a light sleep and a light wake
      let listening = false
      api.onWake = () => {
        // the operator makes the pod and clears the hibernation condition
        const woken = api.clusters.get(DB) as ClusterRow
        api.clusters.set(DB, { ...woken, status: { ...woken.status, conditions: [{ type: 'cnpg.io/hibernation', status: 'False' }] } })
        setTimeout(() => api.pods.set(POD_KEY, podRow()), 20)
        if (!listening) {
          listening = true
          setTimeout(() => void pg.listen(), 40)
        }
      }
      const c = client()
      c.write('x')
      await c.hears(1)
      together()
      c.destroy()
      await sleep(100)
      clock.advance(31)
      await gateway.tick()
      together()
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBe(FENCE_ALL)
      const d = client()
      d.write('y')
      await d.hears(1)
      together()
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
    })

    test('a restart of the gateway starts every idle time again but keeps the full time from the mark: a database in light sleep for 359 minutes is hibernated 2 minutes later, and an awake one is not fenced for its whole light time', async () => {
      const first = await world()
      first.api.clusters.set(DB, lightSleeper(first.clock, 359))
      await first.gateway.tick()
      expect(changes(first.api)).toEqual([])
      const awake = await world()
      awake.api.clusters.set(DB, sleeper())
      // it may have been idle for hours as far as the Cluster can say, but this gateway has only just met it: its idle time runs from here
      await awake.gateway.tick()
      expect(changes(awake.api)).toEqual([])
      awake.clock.advance(29)
      await awake.gateway.tick()
      expect(changes(awake.api)).toEqual([])
      awake.clock.advance(2)
      await awake.gateway.tick()
      expect(changes(awake.api)).toEqual([`fence ${DB}`])
    })

    test('from awake, when the light time is not shorter than the full time, or none is declared (the Cluster before the two stages): the hibernation alone, at the light time, no fence', async () => {
      for (const row of [sleeper({ light: 30, full: 30 }), sleeper({ light: 30, full: 10 }), sleeper({ light: 30, full: null })]) {
        const { api, clock, gateway } = await world()
        api.clusters.set(DB, row)
        await gateway.tick()
        clock.advance(29)
        await gateway.tick()
        expect(changes(api)).toEqual([])
        clock.advance(2)
        await gateway.tick()
        expect(changes(api)).toEqual([`hibernate ${DB}`])
        expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
        expect(annotationsOf(api)[GATEWAY_IDLE_SINCE_ANNOTATION]).toBeUndefined()
      }
    })

    test('a full time of `never` keeps a database in light sleep for good; and it is fenced, not hibernated, at its light time', async () => {
      const kept = await world()
      kept.api.clusters.set(DB, lightSleeper(kept.clock, 10, { full: 'never' }))
      await kept.gateway.tick()
      kept.clock.advance(100_000)
      await kept.gateway.tick()
      expect(changes(kept.api)).toEqual([])
      const fresh = await world()
      fresh.api.clusters.set(DB, sleeper({ full: 'never' }))
      await fresh.gateway.tick()
      fresh.clock.advance(31)
      await fresh.gateway.tick()
      expect(changes(fresh.api)).toEqual([`fence ${DB}`])
    })

    test('not while a caller waits, a wake is under way or a Backup is unfinished; a full time that is no number is no full time', async () => {
      const { api, clock, gateway } = await world()
      api.clusters.set(DB, lightSleeper(clock, 400))
      api.backups.set('apps/b', [{ spec: { cluster: { name: 'orders' } }, status: { phase: 'started' } }])
      await gateway.tick()
      expect(changes(api)).toEqual([])
      api.backups.set('apps/b', [])
      const bad = await world()
      bad.api.clusters.set(DB, lightSleeper(bad.clock, 4000, { full: 'soon' as unknown as number }))
      await bad.gateway.tick()
      expect(changes(bad.api)).toEqual([])
      await gateway.tick()
      expect(changes(api)).toEqual([`hibernate ${DB}`])
    })
  })

  describe('the state callers meet, and the wakes', () => {
    /** A light sleeper whose pod is there, Postgres restarting after the fence is lifted: "starting up" `startingFor` times, then it takes logins, 50 ms after the patch. */
    async function lightWorld(startingFor = 3, over: Partial<Options> = {}) {
      const made = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000, ...over })
      // the Service leads nowhere: what a light wake does is read from the pod's own address
      made.service.port = made.dead
      made.pg.startingFor = startingFor
      made.api.clusters.set(DB, lightSleeper(made.clock, 40))
      made.api.pods.set(POD_KEY, podRow({ created: wholeSecond(Date.now() - 3_600_000) }))
      made.api.onWake = () => setTimeout(() => void made.pg.listen(), 50)
      return made
    }

    test('a caller of a database in light sleep is woken at once: the fence and the gateway`s mark come off in one patch, and the caller is passed to the pod`s own address only once the server takes logins, with "starting up" answered by asking again and never by a login', async () => {
      const { api, client, gateway, pg, lines } = await lightWorld(3)
      await gateway.tick()
      const c = client()
      c.write('hello')
      expect((await c.hears(5)).toString()).toBe('hello')
      expect(api.wakeStages).toEqual(['light'])
      expect(changes(api)).toEqual([`wake ${DB}`])
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
      expect(annotationsOf(api)[GATEWAY_IDLE_SINCE_ANNOTATION]).toBeUndefined()
      // "starting up" three times, never answered, then the yes; the caller is the only connection that was not a question
      expect(pg.answeredStarting).toBe(3)
      expect(pg.sentAfterStartup).toBe(0)
      expect(pg.callers).toBe(1)
      expect(wakes(gateway, 'woken', 'light')).toBe(1)
      expect(wakes(gateway, 'woken', 'full')).toBe(0)
      expect(seconds(gateway, 'count', 'light')).toBe(1)
      expect(seconds(gateway, 'sum', 'light')).toBeLessThan(1)
      expect(lines.some((l) => l.includes("light wake began, by the server's own address"))).toBe(true)
    })

    test('the Service is not dialed for a database last read as asleep (the first dial is skipped: the Cluster is known), nor by a light wake while it looks for the pod`s address: the fenced pod is still an endpoint, and a dial would connect before Postgres takes logins', async () => {
      const made = await lightWorld(2)
      const { api, client, gateway, svc } = made
      await svc.listen()
      made.service.port = svc.port
      await gateway.tick()
      const c = client()
      c.write('x')
      await c.hears(1)
      expect(svc.connections).toBe(0)
      expect(api.count('wake')).toBe(1)
      expect(wakes(gateway, 'woken', 'light')).toBe(1)
    })

    test('a light wake whose pod cannot be read goes through the Service after the grace: slower, never stuck', async () => {
      const made = await lightWorld(0, { lightDirectGraceMs: 150 })
      const { api, client, gateway, svc } = made
      svc.listen()
      await sleep(10)
      made.service.port = svc.port
      api.pods.set(POD_KEY, 'throw')
      await gateway.tick()
      const started = Date.now()
      const c = client()
      c.write('x')
      await c.hears(1)
      expect(Date.now() - started).toBeGreaterThanOrEqual(140)
      expect(svc.connections).toBe(1)
      expect(wakes(gateway, 'woken', 'light')).toBe(1)
    })

    test('an awake database is dialed first, as before, and a database last read as asleep is not: the Service answers the one and never sees the other', async () => {
      const made = await world({ awakeHoldMs: 800 })
      const { api, client, gateway, svc } = made
      await svc.listen()
      made.service.port = svc.port
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      const a = client()
      a.write('x')
      await a.hears(1)
      expect(svc.connections).toBe(1)
      expect(api.count('wake')).toBe(0)
      a.destroy()
      api.clusters.set(DB, sleeper({ hibernated: true, condition: true }))
      await gateway.tick()
      let atWake = -1
      api.onWake = () => (atWake = svc.connections)
      const b = client()
      b.write('y')
      await sleep(150)
      expect(atWake).toBe(1)
      expect(api.count('wake')).toBe(1)
    })

    test('a database in full sleep carries its stage: a full wake removes any fence and mark left from the way down in the same patch as it turns the hibernation off, and is counted and timed as full', async () => {
      const { api, clock, client, gateway, pg } = await world({ holdLimitMs: 2_000_000_000 })
      api.clusters.set(DB, sleeper({ hibernated: true, condition: true, fence: FENCE_ALL, idleSince: isoMinutesAgo(clock, 400) }))
      pg.startingFor = 1
      api.onWake = () => {
        setTimeout(() => api.pods.set(POD_KEY, podRow()), 30)
        setTimeout(() => void pg.listen(), 60)
      }
      await gateway.tick()
      const c = client()
      c.write('x')
      await c.hears(1)
      expect(api.wakeStages).toEqual(['full'])
      expect(annotationsOf(api)[HIBERNATION_ANNOTATION]).toBe('off')
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
      expect(annotationsOf(api)[GATEWAY_IDLE_SINCE_ANNOTATION]).toBeUndefined()
      expect(wakes(gateway, 'woken', 'full')).toBe(1)
      expect(wakes(gateway, 'woken', 'light')).toBe(0)
      expect(seconds(gateway, 'count', 'full')).toBe(1)
      expect(seconds(gateway, 'count', 'light')).toBe(0)
    })

    test('a fence that is not the gateway`s (no mark, or not the whole-Cluster fence) is a stop by hand even on a database declared to sleep: the caller is refused at once, nothing is woken and the fence is not lifted; counted refused, in the light stage', async () => {
      for (const row of [sleeper({ fence: FENCE_ALL }), sleeper({ fence: '["orders-1"]', idleSince: new Date().toISOString() })]) {
        const { api, client, gateway } = await world()
        api.clusters.set(DB, row)
        await gateway.tick()
        const started = Date.now()
        const c = client()
        c.write('x')
        await c.isClosed()
        expect(Date.now() - started).toBeLessThan(1000)
        expect(api.count('wake')).toBe(0)
        expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBe(row.metadata!.annotations![FENCED_INSTANCES_ANNOTATION])
        expect(wakes(gateway, 'refused', 'light')).toBe(1)
        expect(wakes(gateway, 'refused', 'full')).toBe(0)
      }
    })

    test('a hibernation by hand on a database not declared to sleep is still refused, in the full stage', async () => {
      const { api, client, gateway } = await world()
      api.clusters.set(DB, sleeper({ declared: false, hibernated: true, condition: true }))
      await gateway.tick()
      const c = client()
      await c.isClosed()
      expect(api.count('wake')).toBe(0)
      expect(wakes(gateway, 'refused', 'full')).toBe(1)
    })

    test('an empty fence is awake: the caller is passed through and nothing is woken', async () => {
      const { api, db, client, gateway } = await world()
      api.clusters.set(DB, sleeper({ fence: '[]' }))
      await db.listen()
      await gateway.tick()
      const c = client()
      c.write('x')
      await c.hears(1)
      expect(api.count('wake')).toBe(0)
    })
  })

  describe('one change at a time, and only on the Cluster that was read (review fix round)', () => {
    /** A gate every sleep patch waits at, until `release()`: the window in which the patch is in flight. */
    const gated = (api: FakeApi) => {
      let release!: () => void
      api.patchGate = new Promise<void>((resolve) => (release = resolve))
      return release
    }
    const inFlight = async (api: FakeApi) => {
      while (api.patchStarted === 0) await sleep(2)
    }
    const oldPod = () => podRow({ created: wholeSecond(Date.now() - 3_600_000) })

    test('a caller that arrives while the fence is in flight is held, not dialed: no session is opened for the fence to end, and it is served by a light wake once the fence has landed', async () => {
      const { api, db, clock, gateway, client, pg } = await world({ stoppedWaitMs: 150, holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000 })
      api.clusters.set(DB, sleeper())
      api.pods.set(POD_KEY, oldPod())
      await db.listen()
      await gateway.tick()
      clock.advance(31)
      const release = gated(api)
      const tick = gateway.tick()
      await inFlight(api)
      const c = client()
      c.write('hello')
      await sleep(150)
      // the Service would have answered (the stand-in listens) and Postgres is still up: nothing was passed
      expect(pg.callers).toBe(0)
      expect(sample(gateway, 'pg_gateway_sessions_open')).toBe(0)
      expect(c.closed).toBe(false)
      release()
      await tick
      expect(changes(api)).toContain(`fence ${DB}`)
      expect((await c.hears(5)).toString()).toBe('hello')
      expect(api.wakeStages).toEqual(['light'])
      expect(wakes(gateway, 'woken', 'light')).toBe(1)
    })

    test('the same for the hibernation from awake (the light time not shorter than the full time): the caller is held, and woken after it as a full wake', async () => {
      const { api, clock, gateway, client, pg } = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000 })
      api.clusters.set(DB, sleeper({ full: 30 }))
      pg.startingFor = 0
      api.onWake = () => {
        setTimeout(() => api.pods.set(POD_KEY, podRow()), 20)
        setTimeout(() => void pg.listen(), 40)
      }
      await gateway.tick()
      clock.advance(31)
      const release = gated(api)
      const tick = gateway.tick()
      await inFlight(api)
      const c = client()
      c.write('hello')
      await sleep(100)
      expect(pg.callers).toBe(0)
      release()
      await tick
      expect((await c.hears(5)).toString()).toBe('hello')
      expect(api.wakeStages).toEqual(['full'])
      expect(annotationsOf(api)[HIBERNATION_ANNOTATION]).toBe('off')
    })

    test('a caller that arrives while the hibernation from light sleep is in flight is woken as the full wake it has become: the wake reads the Cluster after the patch settled, so the Cluster does not end hibernated while the caller waits on a light wake that can never find its pod, and no light wake is counted timed out', async () => {
      const { api, clock, gateway, client, pg } = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000 })
      api.clusters.set(DB, lightSleeper(clock, 359))
      api.pods.set(POD_KEY, oldPod())
      api.onWake = () => {
        setTimeout(() => api.pods.set(POD_KEY, podRow()), 20)
        setTimeout(() => void pg.listen(), 40)
      }
      await gateway.tick()
      clock.advance(2)
      const release = gated(api)
      const tick = gateway.tick()
      await inFlight(api)
      const c = client()
      c.write('x')
      await sleep(100)
      // the wake has not read the Cluster yet: nothing is decided until the patch has settled
      expect(api.wakeStages).toEqual([])
      release()
      await tick
      expect((await c.hears(1)).toString()).toBe('x')
      expect(api.wakeStages).toEqual(['full'])
      expect(annotationsOf(api)[HIBERNATION_ANNOTATION]).toBe('off')
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
      expect(wakes(gateway, 'timed_out', 'light')).toBe(0)
      expect(wakes(gateway, 'woken', 'full')).toBe(1)
    })

    test('a patch the API refuses (409) or that fails leaves the database as it was and its callers as they were: it is decided again at the next look', async () => {
      const { api, db, clock, gateway, client, lines } = await world()
      api.clusters.set(DB, sleeper())
      await db.listen()
      await gateway.tick()
      clock.advance(31)
      const release = gated(api)
      const tick = gateway.tick()
      await inFlight(api)
      // the Cluster changes under the patch (the operator, the declaration's writer): the patch names a version that is gone
      api.clusters.set(DB, { ...sleeper(), metadata: { annotations: { ...sleeper().metadata!.annotations, 'a-label': 'x' } } })
      const c = client()
      c.write('x')
      release()
      await tick
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
      expect(lines.some((l) => l.includes('changed since it was read'))).toBe(true)
      // the caller was held for the sleep, found the database awake, and was served by the hold
      expect((await c.hears(1)).toString()).toBe('x')
      expect(sample(gateway, 'pg_gateway_api_errors_total', '')).toBe(0)
    })

    test('the writer of the declaration takes the sleep declaration off while the look lists the Backups: the fence names the version the decision was made on, is refused, and a Cluster that is not declared to sleep is never fenced', async () => {
      const { api, clock, gateway } = await world()
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(31)
      let release!: () => void
      const gate = new Promise<void>((resolve) => (release = resolve))
      let started = false
      api.listBackups = async () => {
        started = true
        await gate
        return []
      }
      const tick = gateway.tick()
      while (!started) await sleep(2)
      // a declaration of `sleep: never`: the three annotations off
      api.clusters.set(DB, { ...sleeper(), metadata: { annotations: {} } })
      release()
      await tick
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
      expect(classify(api.clusters.get(DB) as ClusterRow, names)).toBe('awake')
      // and at the next look, with the declaration gone, nothing is done either
      clock.advance(31)
      await gateway.tick()
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
    })

    test('a row listed before a light wake is not taken in after it: the old mark is not taken back, the database that was used moments ago is not hibernated, and its state is not set back to light sleep', async () => {
      const { api, db, clock, gateway, client } = await world({ holdLimitMs: 20_000, awakeHoldMs: 20_000 })
      const pg = new Postgres(db)
      api.clusters.set(DB, lightSleeper(clock, 400))
      api.pods.set(POD_KEY, oldPod())
      api.onWake = () => setTimeout(() => void pg.listen(), 20)
      let release!: () => void
      const gate = new Promise<void>((resolve) => (release = resolve))
      const list = api.listClusters.bind(api)
      let listed = false
      api.listClusters = async (namespace: string) => {
        const rows = await list(namespace)
        listed = true
        await gate
        return rows
      }
      const tick = gateway.tick()
      while (!listed) await sleep(2)
      const c = client()
      c.write('x')
      await c.hears(1)
      c.destroy()
      await sleep(150)
      release()
      await tick
      expect(api.calls.filter((call) => call.startsWith('hibernate '))).toEqual([])
      expect(annotationsOf(api)[HIBERNATION_ANNOTATION]).toBeUndefined()
      // and the next look, on a fresh list, leaves a database used a moment ago alone
      api.listClusters = list
      await gateway.tick()
      expect(api.calls.filter((call) => call.startsWith('hibernate '))).toEqual([])
    })

    test('a gateway never patches a Cluster whose row carries no version: a decision is not applied to a Cluster that may no longer be the one it was made about', async () => {
      const { api, clock, gateway } = await world()
      api.clusters.set(DB, sleeper())
      const list = api.listClusters.bind(api)
      api.listClusters = async (namespace: string) => (await list(namespace)).map((r) => ({ ...r, metadata: { ...r.metadata, resourceVersion: undefined } }))
      await gateway.tick()
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([])
    })

    test('a light wake begun right after the gateway`s own fence takes a yes only after an ask has seen the server stopped: the Postgres that has not begun to stop is not passed a caller', async () => {
      const { api, clock, gateway, client, pg } = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000, stoppedWaitMs: 2_000 })
      api.clusters.set(DB, sleeper())
      api.pods.set(POD_KEY, oldPod())
      // up for two more questions, then stopped, then up again
      pg.script = ['open', 'open', 'starting', 'starting']
      await pg.listen()
      await gateway.tick()
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
      const c = client()
      c.write('x')
      expect((await c.hears(1)).toString()).toBe('x')
      expect(pg.asked).toBeGreaterThanOrEqual(5)
      // no question was answered with a caller already passed, and the caller came after the server had been seen stopped
      expect(pg.answeredStarting).toBe(2)
      expect(pg.callersAtAsk.every((n) => n === 0)).toBe(true)
      expect(pg.callers).toBe(1)
    })

    test('and when the server is never stopped (the fence was lifted before the instance manager acted) the caller is passed after the bound, not held for good', async () => {
      const { api, clock, gateway, client, pg } = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000, stoppedWaitMs: 400 })
      api.clusters.set(DB, sleeper())
      api.pods.set(POD_KEY, oldPod())
      await pg.listen()
      await gateway.tick()
      clock.advance(31)
      await gateway.tick()
      const started = Date.now()
      const c = client()
      c.write('x')
      await c.hears(1)
      expect(Date.now() - started).toBeGreaterThanOrEqual(350)
      expect(Date.now() - started).toBeLessThan(2_000)
    })

    test('a light wake long after the fence is not held to see the server stopped', async () => {
      const made = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000, stoppedWaitMs: 2_000 })
      const { api, clock, gateway, client, pg } = made
      made.service.port = made.dead
      api.clusters.set(DB, lightSleeper(clock, 40))
      api.pods.set(POD_KEY, oldPod())
      await pg.listen()
      await gateway.tick()
      const started = Date.now()
      const c = client()
      c.write('x')
      await c.hears(1)
      expect(Date.now() - started).toBeLessThan(500)
    })

    test('the schedules are suspended first, then the Backups looked at again, then the Cluster fenced: no Backup can be made between the look and the fence; a running Backup is seen first, so the schedules are not suspended at every look for as long as it runs', async () => {
      const { api, clock, gateway } = await world()
      const order: string[] = []
      const backups = api.listBackups.bind(api)
      api.listBackups = async (ns: string) => (order.push('backups'), backups(ns))
      const suspend = api.suspendSchedule.bind(api)
      api.suspendSchedule = async (ns: string, name: string) => (order.push(`suspend ${name}`), suspend(ns, name))
      const fence = api.fence.bind(api)
      api.fence = async (ns: string, name: string, since: string, version: string) => (order.push('fence'), fence(ns, name, since, version))
      api.schedules.set(DB, [{ metadata: { name: 'orders-daily', annotations: {} }, spec: { cluster: { name: 'orders' } } }])
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(31)
      api.backups.set('apps/b', [{ spec: { cluster: { name: 'orders' } }, status: { phase: 'running' } }])
      await gateway.tick()
      await gateway.tick()
      expect(order.filter((o) => o.startsWith('suspend'))).toEqual([])
      order.length = 0
      api.backups.set('apps/b', [])
      await gateway.tick()
      expect(order).toEqual(['backups', 'suspend orders-daily', 'backups', 'fence'])
    })

    test('a Cluster that disappears under a route and is made again has its whole idle time: it is not fenced at the first look after it reappears', async () => {
      const { api, clock, gateway } = await world()
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(31)
      api.clusters.delete(DB)
      await gateway.tick()
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      expect(changes(api)).toEqual([])
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
    })

    test('the move from light to full sleep waits for a session that is open and for a caller that waits', async () => {
      const open = await world()
      open.api.clusters.set(DB, sleeper())
      await open.db.listen()
      await open.gateway.tick()
      const c = open.client()
      c.write('x')
      await c.hears(1)
      // the Cluster reads light sleep for 400 minutes while a session is open through the gateway
      open.api.clusters.set(DB, lightSleeper(open.clock, 400))
      open.clock.advance(5)
      await open.gateway.tick()
      expect(open.api.calls.filter((call) => call.startsWith('hibernate '))).toEqual([])
      // the session ends: nothing holds the move any longer, and the mark says how long it has been light
      c.destroy()
      await sleep(80)
      await open.gateway.tick()
      expect(open.api.calls.filter((call) => call.startsWith('hibernate '))).toHaveLength(1)

      const waiting = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000 })
      waiting.api.realistic = false
      waiting.api.clusters.set(DB, lightSleeper(waiting.clock, 300))
      waiting.api.pods.set(POD_KEY, oldPod())
      await waiting.gateway.tick()
      const d = waiting.client()
      d.write('y')
      await sleep(150)
      expect(waiting.api.count('wake')).toBe(1)
      // 400 minutes of light sleep by the mark, with a caller waiting on its wake
      waiting.clock.advance(100)
      await waiting.gateway.tick()
      expect(waiting.api.calls.filter((call) => call.startsWith('hibernate '))).toEqual([])
    })
  })

  describe('the schedules, in either stage', () => {
    const schedule = (name: string) => ({ metadata: { name, annotations: {} }, spec: { cluster: { name: 'orders' } } }) as ScheduleRow
    const suspends = (api: FakeApi) => api.calls.filter((c) => c.startsWith('suspend ') || c.startsWith('resume '))

    test('suspended while the database is in light sleep or full sleep, and resumed only once a server is ready after the wake', async () => {
      const { api, clock, gateway } = await world()
      api.schedules.set(DB, [schedule('orders-daily'), schedule('orders-hourly')])
      api.clusters.set(DB, lightSleeper(clock, 40, { ready: 0 }))
      await gateway.tick()
      expect(suspends(api).sort()).toEqual(['suspend apps/orders-daily', 'suspend apps/orders-hourly'])
      // hibernated (the way on from light): they stay suspended
      api.clusters.set(DB, sleeper({ hibernated: true, condition: true, ready: 0 }))
      await gateway.tick()
      expect(suspends(api)).toHaveLength(2)
      // awake, the server not ready yet: still suspended; ready: resumed
      api.clusters.set(DB, sleeper({ ready: 0 }))
      await gateway.tick()
      expect(suspends(api)).toHaveLength(2)
      api.clusters.set(DB, sleeper({ ready: 1 }))
      await gateway.tick()
      expect(suspends(api).slice(2).sort()).toEqual(['resume apps/orders-daily', 'resume apps/orders-hourly'])
    })

    test('the ledger, a permanent database in a namespace of its own and declared to sleep: `ledger-daily` and `ledger-hourly` are suspended in the look that fences it, stay suspended through full sleep and a wake that is not ready, and are resumed only once a server is ready; the throwaway`s namespace is not read for it', async () => {
      const { api, clock, gateway, port } = await world()
      const LEDGER = 'ledger/ledger'
      const ledgerSchedule = (name: string) => ({ metadata: { name, annotations: {} }, spec: { cluster: { name: 'ledger' } } }) as ScheduleRow
      api.services = [route('ledger', port, LEDGER)]
      await gateway.refresh()
      api.schedules.set(LEDGER, [ledgerSchedule('ledger-daily'), ledgerSchedule('ledger-hourly')])
      // the three sleep annotations and nothing else of a sleeper's: 30 minutes, 360 minutes, one ready instance
      api.clusters.set(LEDGER, sleeper({ light: 30, full: 360 }))
      await gateway.tick()
      expect(suspends(api)).toEqual([])
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${LEDGER}`])
      expect(suspends(api).sort()).toEqual(['suspend ledger/ledger-daily', 'suspend ledger/ledger-hourly'])
      expect(api.lists).toContain('list clusters ledger')
      expect(api.lists).not.toContain('list clusters apps')
      for (const s of api.schedules.get(LEDGER)!) expect(s.metadata.annotations![SUSPENDED_BY_ANNOTATION]).toBe('pg-gateway')
      // full sleep: they stay suspended
      api.clusters.set(LEDGER, sleeper({ hibernated: true, condition: true, ready: 0 }))
      await gateway.tick()
      expect(suspends(api)).toHaveLength(2)
      // woken, the server not ready yet: still suspended; ready: the two it marked are resumed
      api.clusters.set(LEDGER, sleeper({ ready: 0 }))
      await gateway.tick()
      expect(suspends(api)).toHaveLength(2)
      api.clusters.set(LEDGER, sleeper({ ready: 1 }))
      await gateway.tick()
      expect(suspends(api).slice(2).sort()).toEqual(['resume ledger/ledger-daily', 'resume ledger/ledger-hourly'])
      expect(api.schedules.get(LEDGER)!.map((s) => s.spec?.suspend)).toEqual([false, false])
    })

    test('the schedules are suspended in the same look that fences the database: a fenced Cluster is not left to the next one', async () => {
      const { api, clock, gateway } = await world()
      api.schedules.set(DB, [schedule('orders-daily')])
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
      expect(suspends(api)).toEqual(['suspend apps/orders-daily'])
    })
  })

  describe('real data: a refused sleep is counted, no sleep for a server that is not ready, and who woke it', () => {
    const refusedOf = (gateway: { metrics(): string }, stage: Stage) => sample(gateway, 'pg_gateway_sleeps_refused_total', `database="${DB}",stage="${stage}"`)
    const gated = (api: FakeApi) => {
      let release!: () => void
      api.patchGate = new Promise<void>((resolve) => (release = resolve))
      return release
    }
    const inFlight = async (api: FakeApi) => {
      while (api.patchStarted === 0) await sleep(2)
    }
    /** The Cluster changes under the patch that is in flight, as the declaration's writer or the operator would change it. */
    const changeUnder = (api: FakeApi) => {
      const row = api.clusters.get(DB) as ClusterRow
      api.clusters.set(DB, { ...row, metadata: { ...row.metadata, annotations: { ...row.metadata!.annotations, 'a-label': 'x' } } })
    }

    test('both series are there at zero from the start, for both stages', async () => {
      const { gateway } = await world()
      for (const stage of ['light', 'full'] as const) expect(refusedOf(gateway, stage)).toBe(0)
    })

    test('a light sleep the API server refused (the Cluster changed since it was read) is counted in the light stage, is not counted as a sleep, and is decided again at the next look, which makes it', async () => {
      const { api, clock, gateway, lines } = await world()
      api.clusters.set(DB, sleeper())
      await gateway.tick()
      clock.advance(31)
      const release = gated(api)
      const tick = gateway.tick()
      await inFlight(api)
      changeUnder(api)
      release()
      await tick
      expect(refusedOf(gateway, 'light')).toBe(1)
      expect(refusedOf(gateway, 'full')).toBe(0)
      expect(sleeps(gateway, 'light')).toBe(0)
      expect(lines.some((l) => l.includes('changed since it was read'))).toBe(true)
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBeUndefined()
      await gateway.tick()
      expect(annotationsOf(api)[FENCED_INSTANCES_ANNOTATION]).toBe(FENCE_ALL)
      expect(sleeps(gateway, 'light')).toBe(1)
      // the count stays: it is what happened, not what is true now
      expect(refusedOf(gateway, 'light')).toBe(1)
    })

    test('a hibernation refused is counted in the full stage, whether it is from awake (the light time not shorter) or from light sleep', async () => {
      const straight = await world()
      straight.api.clusters.set(DB, sleeper({ full: 30 }))
      await straight.gateway.tick()
      straight.clock.advance(31)
      const first = gated(straight.api)
      const tick = straight.gateway.tick()
      await inFlight(straight.api)
      changeUnder(straight.api)
      first()
      await tick
      expect(refusedOf(straight.gateway, 'full')).toBe(1)
      expect(refusedOf(straight.gateway, 'light')).toBe(0)
      expect(sleeps(straight.gateway, 'full')).toBe(0)

      const fromLight = await world()
      fromLight.api.clusters.set(DB, lightSleeper(fromLight.clock, 359))
      await fromLight.gateway.tick()
      fromLight.clock.advance(2)
      const second = gated(fromLight.api)
      const again = fromLight.gateway.tick()
      await inFlight(fromLight.api)
      changeUnder(fromLight.api)
      second()
      await again
      expect(refusedOf(fromLight.gateway, 'full')).toBe(1)
      expect(refusedOf(fromLight.gateway, 'light')).toBe(0)
    })

    test('the removal of the fence and mark at the end of a full sleep is no sleep: a refusal of it is not counted', async () => {
      const { api, gateway } = await world()
      api.clusters.set(DB, sleeper({ hibernated: true, condition: true, fence: FENCE_ALL, idleSince: new Date().toISOString() }))
      const release = gated(api)
      const tick = gateway.tick()
      await inFlight(api)
      changeUnder(api)
      release()
      await tick
      expect(api.calls.filter((c) => c.startsWith('unfence '))).toEqual([`unfence ${DB}`])
      expect(refusedOf(gateway, 'light')).toBe(0)
      expect(refusedOf(gateway, 'full')).toBe(0)
    })

    test('an awake database whose Cluster reports no ready instance is not put to sleep, nor are its schedules suspended; at the first look that reports one it is, at once and not after another light time', async () => {
      const { api, clock, gateway, lines } = await world()
      api.schedules.set(DB, [{ metadata: { name: 'orders-daily', annotations: {} }, spec: { cluster: { name: 'orders' } } } as ScheduleRow])
      api.clusters.set(DB, sleeper({ ready: 0 }))
      await gateway.tick()
      clock.advance(31)
      await gateway.tick()
      clock.advance(300)
      await gateway.tick()
      await gateway.tick()
      expect(changes(api)).toEqual([])
      expect(api.calls.filter((c) => c.startsWith('suspend '))).toEqual([])
      expect(api.lists.filter((l) => l.startsWith('list backups'))).toEqual([])
      // said once, however many looks
      expect(lines.filter((l) => l.includes('no ready instance'))).toHaveLength(1)
      api.clusters.set(DB, sleeper({ ready: 1 }))
      await gateway.tick()
      expect(changes(api)).toEqual([`fence ${DB}`])
      expect(lines.some((l) => l.includes('reports a ready instance again'))).toBe(true)
    })

    test('the same for the hibernation from awake (the light time not shorter than the full time), and for a Cluster whose status says nothing of its instances', async () => {
      const { api, clock, gateway } = await world()
      api.clusters.set(DB, { ...sleeper({ full: 30 }), status: { conditions: [{ type: 'cnpg.io/hibernation', status: 'False' }] } })
      await gateway.tick()
      clock.advance(31)
      await gateway.tick()
      expect(changes(api)).toEqual([])
      api.clusters.set(DB, sleeper({ full: 30, ready: 1 }))
      await gateway.tick()
      expect(changes(api)).toEqual([`hibernate ${DB}`])
    })

    test('a database in light sleep goes on to full sleep though its Cluster reports no ready instance (a fenced server reports none): the rule is for an awake database', async () => {
      const { api, clock, gateway } = await world()
      api.clusters.set(DB, lightSleeper(clock, 361, { ready: 0 }))
      await gateway.tick()
      expect(changes(api)).toEqual([`hibernate ${DB}`])
    })

    // ── who woke it ──
    const startup = (user: string | undefined, database?: string, extra: Record<string, string> = {}) => {
      const params = [...(user !== undefined ? ['user', user] : []), ...(database !== undefined ? ['database', database] : []), ...Object.entries(extra).flat()]
      const body = Buffer.concat([Buffer.from([0, 3, 0, 0]), Buffer.from(params.length ? `${params.join('\0')}\0\0` : '\0')])
      const length = Buffer.alloc(4)
      length.writeInt32BE(body.length + 4)
      return Buffer.concat([length, body])
    }
    const sslRequest = () => {
      const b = Buffer.alloc(8)
      b.writeInt32BE(8, 0)
      b.writeInt32BE(80877103, 4)
      return b
    }
    const callerLines = (gateway: { metrics(): string }, stage = 'light') => gateway.metrics().split('\n').filter((l) => l.startsWith('pg_gateway_wake_callers_total{') && l.includes(`stage="${stage}"`))
    /** What the counter says of a login in a stage; 0 for a login with no series (which is what a counter that does not exist yet means). */
    const callersOf = (gateway: { metrics(): string }, login: string, stage: Stage = 'light') => {
      const line = callerLines(gateway, stage).find((l) => l.startsWith(`pg_gateway_wake_callers_total{database="${DB}",stage="${stage}",login="${login}"} `))
      return line === undefined ? 0 : Number(line.slice(line.lastIndexOf(' ') + 1))
    }
    const hasSeries = (gateway: { metrics(): string }, login: string, stage = 'light') => callerLines(gateway, stage).some((l) => l.includes(`login="${login}"`))
    /** A light sleeper whose server is up behind it, so that each caller that wakes it is passed through (and each wake is one wake). */
    async function sleeping(over: Partial<Options> = {}) {
      const made = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000, ...over })
      made.service.port = made.dead
      made.api.pods.set(POD_KEY, podRow({ created: wholeSecond(Date.now() - 3_600_000) }))
      await made.pg.listen()
      const sleepAgain = async () => {
        made.api.clusters.set(DB, lightSleeper(made.clock, 40))
        await made.gateway.tick()
      }
      await sleepAgain()
      /** One caller that sends `first`, is passed through and leaves: the wake it began is over when this returns. */
      const wakeBy = async (first: Buffer) => {
        const c = made.client()
        c.write(first)
        await c.hears(first.length)
        c.destroy()
        await sleep(40)
        await sleepAgain()
      }
      return { ...made, sleepAgain, wakeBy }
    }

    test('the caller that begins a wake is named by the login of its startup message: the wake`s log line, and a counter by database, stage and login; the caller is passed through with every byte it sent, the startup message included', async () => {
      const { api, gateway, client, lines } = await sleeping()
      // a read of the Cluster that takes a little, as it does against the real API server: the caller's first message is there when the wake begins
      api.getDelayMs = 40
      const message = startup('app_reader', 'app_reader', { application_name: 'a-secret-looking-name', options: '-c search_path=hidden' })
      const c = client()
      c.write(message)
      expect(Buffer.compare(await c.hears(message.length), message)).toBe(0)
      expect(callersOf(gateway, 'app_reader')).toBe(1)
      expect(callersOf(gateway, 'app_reader', 'full')).toBe(0)
      expect(lines.find((l) => l.startsWith(`pg-gateway: waking ${DB}`))!).toContain('(light sleep), begun by login app_reader on database app_reader')
      expect(lines.find((l) => l.includes('answered') && l.includes('wake began'))!).toContain('; begun by login app_reader on database app_reader')
      // nothing else of the message is logged or kept
      for (const text of [...lines, gateway.metrics()]) {
        expect(text).not.toContain('a-secret-looking-name')
        expect(text).not.toContain('search_path')
      }
    })

    test('a wake of the full stage is counted by the full stage', async () => {
      const { api, clock, client, gateway, pg } = await world({ holdLimitMs: 2_000_000_000 })
      api.clusters.set(DB, sleeper({ hibernated: true, condition: true, fence: FENCE_ALL, idleSince: isoMinutesAgo(clock, 400) }))
      api.getDelayMs = 40
      api.onWake = () => {
        setTimeout(() => api.pods.set(POD_KEY, podRow()), 30)
        setTimeout(() => void pg.listen(), 60)
      }
      await gateway.tick()
      const c = client()
      const message = startup('app_writer', 'app_writer')
      c.write(message)
      await c.hears(message.length)
      expect(callersOf(gateway, 'app_writer', 'full')).toBe(1)
      expect(hasSeries(gateway, 'app_writer', 'light')).toBe(false)
    })

    test('a first message that is not whole when the wake begins is waited for in the background and never holds the wake up: the login is counted once the message is whole, in a line of its own', async () => {
      const { gateway, client, lines, pg } = await sleeping()
      // the server answers "starting up" for a while, so the wake lasts long enough for the rest of the message to come
      pg.startingFor = 30
      const message = startup('app_batch', 'app_batch')
      const c = client()
      c.write(message.subarray(0, 6))
      await sleep(80)
      expect(callersOf(gateway, 'app_batch')).toBe(0)
      c.write(message.subarray(6))
      await c.hears(message.length)
      await sleep(60)
      expect(callersOf(gateway, 'app_batch')).toBe(1)
      expect(callersOf(gateway, 'other')).toBe(0)
      expect(lines.some((l) => l.includes('the light wake was begun by login app_batch on database app_batch'))).toBe(true)
    })

    test('a first message that never becomes whole counts as other after the wait, and the caller was passed through all the same', async () => {
      const { gateway, client } = await sleeping({ firstMessageMs: 150 })
      const c = client()
      c.write(Buffer.from([0, 0, 0, 80]))
      expect((await c.hears(4)).length).toBe(4)
      expect(callersOf(gateway, 'other')).toBe(0)
      await sleep(250)
      expect(callersOf(gateway, 'other')).toBe(1)
    })

    test('a caller that asks for encryption first reads as encrypted, and the bytes after the request are not read', async () => {
      const { wakeBy, gateway, lines } = await sleeping()
      await wakeBy(Buffer.concat([sslRequest(), Buffer.from('anything that follows is inside the handshake: user=hidden')]))
      await sleep(40)
      expect(callersOf(gateway, 'encrypted')).toBe(1)
      expect(lines.some((l) => l.includes('begun by login encrypted'))).toBe(true)
      expect(lines.join('\n')).not.toContain('hidden')
    })

    test('a name that is not a well-formed role name is never a label or a word of the log; a first message that is not a startup message, or has no login, reads as other; and the two labels that are not names cannot be taken as logins', async () => {
      const { wakeBy, gateway, lines } = await sleeping()
      const bad = ['quo"te', 'new\nline-feed', 'back\\slash', 'with space', '9starts-with-digit', 'x'.repeat(64), '', 'encrypted', 'other']
      for (const name of bad) await wakeBy(startup(name, 'db'))
      await wakeBy(startup(undefined, 'db'))
      await wakeBy(Buffer.from('GET / HTTP/1.1\r\nHost: x\r\n\r\n'))
      // a cancel request, which has no login
      const cancel = Buffer.alloc(16)
      cancel.writeInt32BE(16, 0)
      cancel.writeInt32BE(80877102, 4)
      await wakeBy(cancel)
      await sleep(60)
      expect(callersOf(gateway, 'other')).toBe(bad.length + 3)
      expect(callersOf(gateway, 'encrypted')).toBe(0)
      // only the two fixed labels exist
      expect(callerLines(gateway)).toHaveLength(2)
      const text = [...lines, gateway.metrics()].join('\n')
      for (const raw of ['quo"te', 'feed', 'back\\slash', 'with space', '9starts']) expect(text).not.toContain(raw)
    })

    test('a database name that is not well-formed is left out of the log line, and the login is still counted', async () => {
      const { api, gateway, client, lines } = await sleeping()
      api.getDelayMs = 40
      const c = client()
      const message = startup('app_reader', 'bad name\nforged log line')
      c.write(message)
      await c.hears(message.length)
      expect(callersOf(gateway, 'app_reader')).toBe(1)
      expect(lines.find((l) => l.startsWith(`pg-gateway: waking ${DB}`))!).toBe(`pg-gateway: waking ${DB} (light sleep), begun by login app_reader`)
      expect(lines.some((l) => l.includes('forged'))).toBe(false)
    })

    test('a login has a label of its own only up to sixteen per database: the seventeenth distinct one is other, and one that has a label is counted by it for good', async () => {
      const { wakeBy, gateway } = await sleeping()
      for (let i = 1; i <= MAX_LOGINS; i += 1) await wakeBy(startup(`role_${i}`, 'db'))
      await wakeBy(startup('role_17', 'db'))
      await wakeBy(startup('role_18', 'db'))
      await wakeBy(startup('role_1', 'db'))
      await sleep(60)
      expect(callersOf(gateway, 'other')).toBe(2)
      expect(callersOf(gateway, 'role_1')).toBe(2)
      expect(callersOf(gateway, 'role_16')).toBe(1)
      expect(hasSeries(gateway, 'role_17')).toBe(false)
      // sixteen names and the two fixed labels, in each stage
      expect(callerLines(gateway)).toHaveLength(MAX_LOGINS + 2)
    }, 30_000)
  })

  describe('the metrics', () => {
    test('every series of a route is there at zero from the start, for both stages: the wakes by result, the histogram with buckets under a second, and the sleeps', async () => {
      const { gateway } = await world()
      for (const stage of ['light', 'full'] as const) {
        for (const result of ['woken', 'timed_out', 'refused']) expect(wakes(gateway, result, stage)).toBe(0)
        expect(sleeps(gateway, stage)).toBe(0)
        expect(seconds(gateway, 'count', stage)).toBe(0)
        for (const le of ['0.05', '0.1', '0.25', '0.5', '1', '2', '3', '120', '+Inf']) expect(seconds(gateway, `bucket,${le}` as `bucket,${string}`, stage)).toBe(0)
      }
    })

    test('a light wake falls into the sub-second buckets of the light histogram, and the full histogram is untouched', async () => {
      const made = await world({ holdLimitMs: 2_000_000_000, awakeHoldMs: 2_000_000_000 })
      made.api.clusters.set(DB, lightSleeper(made.clock, 40))
      made.api.pods.set(POD_KEY, podRow({ created: wholeSecond(Date.now() - 3_600_000) }))
      made.api.onWake = () => setTimeout(() => void made.pg.listen(), 30)
      await made.gateway.tick()
      const c = made.client()
      c.write('x')
      await c.hears(1)
      expect(seconds(made.gateway, 'bucket,0.5', 'light')).toBe(1)
      expect(seconds(made.gateway, 'bucket,+Inf', 'light')).toBe(1)
      expect(seconds(made.gateway, 'bucket,+Inf', 'full')).toBe(0)
    })
  })
})


describe('what a caller`s first message tells (readStartup)', () => {
  const message = (...pairs: string[]) => {
    const body = Buffer.concat([Buffer.from([0, 3, 0, 0]), Buffer.from(`${pairs.join('\0')}\0\0`)])
    const length = Buffer.alloc(4)
    length.writeInt32BE(body.length + 4)
    return Buffer.concat([length, body])
  }
  const request = (code: number) => {
    const b = Buffer.alloc(8)
    b.writeInt32BE(8, 0)
    b.writeInt32BE(code, 4)
    return b
  }

  test('a plain startup message gives its login and database and nothing else', () => {
    expect(readStartup(message('user', 'app_reader', 'database', 'app_reader', 'application_name', 'duckdb'))).toEqual({ kind: 'startup', user: 'app_reader', database: 'app_reader' })
    expect(readStartup(message('database', 'x', 'user', 'u'))).toEqual({ kind: 'startup', user: 'u', database: 'x' })
    expect(readStartup(message('user', 'only'))).toEqual({ kind: 'startup', user: 'only' })
  })

  test('a request for encryption, SSL or GSS, is encrypted whatever follows it', () => {
    expect(readStartup(request(80877103))).toEqual({ kind: 'encrypted' })
    expect(readStartup(request(80877104))).toEqual({ kind: 'encrypted' })
    expect(readStartup(Buffer.concat([request(80877103), Buffer.from('user\0hidden\0\0')]))).toEqual({ kind: 'encrypted' })
  })

  test('fewer bytes than the message needs say "more", at any cut, and the whole message says what it is', () => {
    const whole = message('user', 'app_reader', 'database', 'app_reader')
    for (let cut = 0; cut < whole.length; cut += 1) {
      const got = readStartup(whole.subarray(0, cut))
      // an SSL request needs eight bytes, a startup message its whole length
      expect(got.kind, `cut at ${cut}`).toBe('more')
    }
    expect(readStartup(whole).kind).toBe('startup')
  })

  test('anything else is other: a cancel request, another protocol version, a length that is not one, a message that does not end in a zero, an HTTP request', () => {
    expect(readStartup(request(80877102))).toEqual({ kind: 'other' })
    const old = message('user', 'u')
    old.writeInt32BE(131072, 4)
    expect(readStartup(old)).toEqual({ kind: 'other' })
    const short = message('user', 'u')
    short.writeInt32BE(4, 0)
    expect(readStartup(short)).toEqual({ kind: 'other' })
    const huge = message('user', 'u')
    huge.writeInt32BE(2_000_000_000, 0)
    expect(readStartup(huge)).toEqual({ kind: 'other' })
    const unended = message('user', 'u')
    expect(readStartup(unended.subarray(0, unended.length - 1)).kind).toBe('more')
    const noZero = Buffer.concat([unended.subarray(0, unended.length - 1), Buffer.from('x')])
    expect(readStartup(noZero)).toEqual({ kind: 'other' })
    expect(readStartup(Buffer.from('GET / HTTP/1.1\r\n\r\n'))).toEqual({ kind: 'other' })
  })

  test('a message with no parameters has no login', () => {
    const empty = Buffer.concat([Buffer.from([0, 0, 0, 9, 0, 3, 0, 0]), Buffer.from([0])])
    expect(readStartup(empty)).toEqual({ kind: 'startup' })
  })
})


describe('what it reads of a Cluster', () => {
  const read = (cluster: ClusterRow) => classify(cluster, names)
  const withAnnotations = (annotations: Record<string, string>, status: ClusterRow['status'] = {}): ClusterRow => ({ metadata: { annotations }, status })
  const NOW = '2026-11-15T22:00:00Z'
  test('full sleep is the hibernation mark or the condition on a database declared to sleep (the annotation "true"); the same on one that is not is a stop by hand', () => {
    expect(read(asleep(true))).toBe('full-sleep')
    expect(read(asleep(false))).toBe('by-hand')
    expect(read({ metadata: { annotations: { [HIBERNATION_ANNOTATION]: 'on', [SLEEP_ENABLED_ANNOTATION]: 'false' } } })).toBe('by-hand')
    expect(read({ metadata: { annotations: { [SLEEP_ENABLED_ANNOTATION]: 'true' } }, status: { conditions: [{ type: 'cnpg.io/hibernation', status: 'True' }] } })).toBe('full-sleep')
  })
  test('light sleep is the whole-Cluster fence with the gateway`s mark on a database declared to sleep; a fence without the mark, another fence, an unreadable one, or the same on a database not declared to sleep is a stop by hand', () => {
    const declared = { [SLEEP_ENABLED_ANNOTATION]: 'true' }
    expect(read(withAnnotations({ ...declared, [FENCED_INSTANCES_ANNOTATION]: FENCE_ALL, [GATEWAY_IDLE_SINCE_ANNOTATION]: NOW }))).toBe('light-sleep')
    // the fence is the gateway's only with its mark, and the mark must be a time
    expect(read(withAnnotations({ ...declared, [FENCED_INSTANCES_ANNOTATION]: FENCE_ALL }))).toBe('by-hand')
    expect(read(withAnnotations({ ...declared, [FENCED_INSTANCES_ANNOTATION]: FENCE_ALL, [GATEWAY_IDLE_SINCE_ANNOTATION]: 'not a time' }))).toBe('by-hand')
    expect(read(withAnnotations({ ...declared, [FENCED_INSTANCES_ANNOTATION]: '["orders-1"]', [GATEWAY_IDLE_SINCE_ANNOTATION]: NOW }))).toBe('by-hand')
    expect(read(withAnnotations({ ...declared, [FENCED_INSTANCES_ANNOTATION]: 'not json', [GATEWAY_IDLE_SINCE_ANNOTATION]: NOW }))).toBe('by-hand')
    expect(read(withAnnotations({ [FENCED_INSTANCES_ANNOTATION]: FENCE_ALL, [GATEWAY_IDLE_SINCE_ANNOTATION]: NOW }))).toBe('by-hand')
    // a fence by hand wins over everything, even a hibernation on a database declared to sleep
    expect(read(withAnnotations({ ...declared, [HIBERNATION_ANNOTATION]: 'on', [FENCED_INSTANCES_ANNOTATION]: '["orders-1"]' }))).toBe('by-hand')
  })
  test('the way from light to full sleep (the hibernation on, the fence and the gateway`s mark kept) is full sleep; an empty fence is awake', () => {
    const declared = { [SLEEP_ENABLED_ANNOTATION]: 'true' }
    expect(read(withAnnotations({ ...declared, [HIBERNATION_ANNOTATION]: 'on', [FENCED_INSTANCES_ANNOTATION]: FENCE_ALL, [GATEWAY_IDLE_SINCE_ANNOTATION]: NOW }))).toBe('full-sleep')
    expect(read(withAnnotations({ ...declared, [FENCED_INSTANCES_ANNOTATION]: '[]' }))).toBe('awake')
    expect(read(withAnnotations({ ...declared, [FENCED_INSTANCES_ANNOTATION]: '' }))).toBe('awake')
    expect(fenceOf(withAnnotations({ [FENCED_INSTANCES_ANNOTATION]: '[]' }), names)).toBe('none')
    expect(fenceOf(withAnnotations({ [FENCED_INSTANCES_ANNOTATION]: FENCE_ALL, [GATEWAY_IDLE_SINCE_ANNOTATION]: NOW }), names)).toBe('ours')
  })
  test('awake: the mark off, or the condition false, or neither; the phase is of no account and nothing of the servers is read', () => {
    expect(read({ metadata: { annotations: { [HIBERNATION_ANNOTATION]: 'off' } }, status: { conditions: [{ type: 'cnpg.io/hibernation', status: 'False' }] } })).toBe('awake')
    expect(read(awake())).toBe('awake')
    expect(read({ status: { phase: 'Cluster in healthy state' } } as ClusterRow)).toBe('awake')
    expect(read({})).toBe('awake')
  })
})

describe('the cluster calls it makes', () => {
  const dir = mkdtempSync(join(tmpdir(), 'pg-gateway-api-'))
  const tokenFile = join(dir, 'token')
  writeFileSync(tokenFile, 'the-token\n')
  const calls: { url: string; init: RequestInit }[] = []
  const answer = (status: number, body: unknown) =>
    (async (url: string, init: RequestInit) => {
      calls.push({ url, init })
      return new Response(JSON.stringify(body), { status })
    }) as unknown as typeof fetch
  const api = (fetchFn: typeof fetch) => kubeApi({ namespace: 'peat-gateway', hibernationAnnotation: HIBERNATION_ANNOTATION, fencedInstancesAnnotation: FENCED_INSTANCES_ANNOTATION, fenceAll: FENCE_ALL, idleSinceAnnotation: GATEWAY_IDLE_SINCE_ANNOTATION, suspendedByAnnotation: SUSPENDED_BY_ANNOTATION, host: '192.0.2.1', port: '443', tokenFile, caFile: '', fetchFn })

  test('lists the Services of its own namespace with its own token', async () => {
    calls.length = 0
    const rows = await api(answer(200, { items: [route('a', 16000)] })).listServices()
    expect(rows).toHaveLength(1)
    expect(calls[0]!.url).toBe('https://192.0.2.1:443/api/v1/namespaces/peat-gateway/services')
    expect(calls[0]!.init.method).toBe('GET')
    expect((calls[0]!.init.headers as Record<string, string>).authorization).toBe('Bearer the-token')
  })

  test('reads a Cluster, and a missing one is null, not an error', async () => {
    calls.length = 0
    expect(await api(answer(200, { status: { phase: 'x' } })).getCluster('apps', 'orders')).toEqual({ status: { phase: 'x' } } as never)
    expect(calls[0]!.url).toBe('https://192.0.2.1:443/apis/postgresql.cnpg.io/v1/namespaces/apps/clusters/orders')
    expect(await api(answer(404, {})).getCluster('apps', 'gone')).toBeNull()
    await expect(api(answer(500, {})).getCluster('apps', 'x')).rejects.toThrow('500')
  })

  const CLUSTER_URL = 'https://192.0.2.1:443/apis/postgresql.cnpg.io/v1/namespaces/apps/clusters/orders'
  const sent = (n = 0) => {
    expect(calls[n]!.url).toBe(CLUSTER_URL)
    expect(calls[n]!.init.method).toBe('PATCH')
    expect((calls[n]!.init.headers as Record<string, string>)['content-type']).toBe('application/merge-patch+json')
    return JSON.parse(calls[n]!.init.body as string)
  }

  test('a full wake is one merge patch of annotations that turns the hibernation mark off and removes any fence and the gateway`s mark left from the way down, and nothing else', async () => {
    calls.length = 0
    await api(answer(200, {})).wake('apps', 'orders', 'full')
    expect(calls).toHaveLength(1)
    expect(sent()).toEqual({ metadata: { annotations: { [HIBERNATION_ANNOTATION]: 'off', [FENCED_INSTANCES_ANNOTATION]: null, [GATEWAY_IDLE_SINCE_ANNOTATION]: null } } })
    await expect(api(answer(403, {})).wake('apps', 'orders', 'full')).rejects.toThrow('403')
  })

  test('a light wake is one merge patch that removes the fence and the gateway`s mark, and does not touch the hibernation mark', async () => {
    calls.length = 0
    await api(answer(200, {})).wake('apps', 'orders', 'light')
    expect(calls).toHaveLength(1)
    expect(sent()).toEqual({ metadata: { annotations: { [FENCED_INSTANCES_ANNOTATION]: null, [GATEWAY_IDLE_SINCE_ANNOTATION]: null } } })
  })

  test('light sleep is one merge patch of the fence on every instance and the gateway`s mark; full sleep is the hibernation mark on and nothing else; the fence and mark come off in one patch', async () => {
    calls.length = 0
    await api(answer(200, {})).fence('apps', 'orders', '2026-11-15T22:00:00.000Z', '')
    await api(answer(200, {})).hibernate('apps', 'orders', '')
    await api(answer(200, {})).unfence('apps', 'orders', '')
    expect(calls).toHaveLength(3)
    expect(sent(0)).toEqual({ metadata: { annotations: { [FENCED_INSTANCES_ANNOTATION]: '["*"]', [GATEWAY_IDLE_SINCE_ANNOTATION]: '2026-11-15T22:00:00.000Z' } } })
    expect(sent(1)).toEqual({ metadata: { annotations: { [HIBERNATION_ANNOTATION]: 'on' } } })
    expect(sent(2)).toEqual({ metadata: { annotations: { [FENCED_INSTANCES_ANNOTATION]: null, [GATEWAY_IDLE_SINCE_ANNOTATION]: null } } })
    await expect(api(answer(403, {})).fence('apps', 'orders', 'x', '')).rejects.toThrow('403')
  })

  test('lists the Clusters and the Backups of a namespace, each in one call', async () => {
    calls.length = 0
    expect(await api(answer(200, { items: [{ metadata: { name: 'orders' } }] })).listClusters('apps')).toEqual([{ metadata: { name: 'orders' } }])
    expect(await api(answer(200, { items: [{ metadata: { name: 'b' }, status: { phase: 'completed' } }] })).listBackups('apps')).toHaveLength(1)
    expect(calls.map((c) => c.url)).toEqual(['https://192.0.2.1:443/apis/postgresql.cnpg.io/v1/namespaces/apps/clusters', 'https://192.0.2.1:443/apis/postgresql.cnpg.io/v1/namespaces/apps/backups'])
    expect(calls.every((c) => c.init.method === 'GET')).toBe(true)
    await expect(api(answer(403, {})).listBackups('ledger')).rejects.toThrow('403')
  })

  test('reads one pod by name in its namespace, and a missing one is null, not an error', async () => {
    calls.length = 0
    expect(await api(answer(200, { metadata: { name: 'orders-1' }, status: { podIP: '192.0.2.23' } })).getPod('apps', 'orders-1')).toEqual({ metadata: { name: 'orders-1' }, status: { podIP: '192.0.2.23' } })
    expect(calls[0]!.url).toBe('https://192.0.2.1:443/api/v1/namespaces/apps/pods/orders-1')
    expect(calls[0]!.init.method).toBe('GET')
    expect(await api(answer(404, {})).getPod('apps', 'gone')).toBeNull()
    await expect(api(answer(403, {})).getPod('apps', 'x')).rejects.toThrow('403')
  })

  test('lists the ScheduledBackups of a namespace and keeps those of the one Cluster', async () => {
    calls.length = 0
    const rows = await api(
      answer(200, {
        items: [
          { metadata: { name: 'orders-daily' }, spec: { cluster: { name: 'orders' } } },
          { metadata: { name: 'orders-hourly' }, spec: { cluster: { name: 'orders' } } },
          { metadata: { name: 'other-daily' }, spec: { cluster: { name: 'other' } } },
        ],
      }),
    ).listSchedules('apps', 'orders')
    expect(rows.map((r) => r.metadata.name)).toEqual(['orders-daily', 'orders-hourly'])
    expect(calls[0]!.url).toBe('https://192.0.2.1:443/apis/postgresql.cnpg.io/v1/namespaces/apps/scheduledbackups')
    expect(calls[0]!.init.method).toBe('GET')
    await expect(api(answer(403, {})).listSchedules('apps', 'orders')).rejects.toThrow('403')
  })

  test('suspends a schedule by a merge patch of spec.suspend and the one annotation, and resumes it by the same two fields back, the annotation removed by a null', async () => {
    calls.length = 0
    await api(answer(200, {})).suspendSchedule('apps', 'orders-daily')
    await api(answer(200, {})).resumeSchedule('apps', 'orders-daily')
    expect(calls.map((c) => c.url)).toEqual(Array(2).fill('https://192.0.2.1:443/apis/postgresql.cnpg.io/v1/namespaces/apps/scheduledbackups/orders-daily'))
    for (const c of calls) {
      expect(c.init.method).toBe('PATCH')
      expect((c.init.headers as Record<string, string>)['content-type']).toBe('application/merge-patch+json')
    }
    expect(JSON.parse(calls[0]!.init.body as string)).toEqual({ metadata: { annotations: { [SUSPENDED_BY_ANNOTATION]: 'pg-gateway' } }, spec: { suspend: true } })
    expect(JSON.parse(calls[1]!.init.body as string)).toEqual({ metadata: { annotations: { [SUSPENDED_BY_ANNOTATION]: null } }, spec: { suspend: false } })
    await expect(api(answer(403, {})).suspendSchedule('apps', 'x')).rejects.toThrow('403')
  })

  test('makes no other write: the only methods in the program are GET and PATCH, and the two PATCHes are the mark of a Cluster and the two fields of a ScheduledBackup', () => {
    const source = readFileSync(join(import.meta.dir, '../src/gateway/gateway.ts'), 'utf8')
    expect([...source.matchAll(/method: '([A-Z]+)'/g)].map((m) => m[1]).sort()).toEqual(['GET', 'GET', 'GET', 'GET', 'GET', 'GET', 'PATCH', 'PATCH'])
    expect([...source.matchAll(/cluster\(namespace, name\), \{\s+method: 'PATCH'/g)]).toHaveLength(1)
    expect([...source.matchAll(/schedules\(namespace\)\}\/\$\{encodeURIComponent\(name\)\}`, \{\s+method: 'PATCH'/g)]).toHaveLength(1)
  })
})

describe('the program on its own, and the platform pod that runs it', () => {
  const file = join(import.meta.dir, '../src/gateway/gateway.ts')

  test('the program imports nothing but Node`s and Bun`s own modules: what it shares comes from its caller', () => {
    const text = readFileSync(file, 'utf8')
    for (const m of text.matchAll(/(?:import|from)\s+(?:[^'"]*?from\s+)?["']([^"']+)["']/g)) expect(m[1]!.startsWith('node:') || m[1]!.startsWith('bun:'), m[1]).toBe(true)
  })

  test('a pod`s settings: the defaults under the platform`s group, the namespaces from the names, and a value that is set and wrong refused', () => {
    const d = gatewaySettings({})
    expect(d.names.namespace).toBe('peat-gateway')
    expect(d.names.routeAnnotation).toBe('peat.zabaca.com/route')
    expect([d.names.idleSinceAnnotation, d.names.sleepFullMinutesAnnotation, d.names.suspendedByAnnotation]).toEqual(['peat.zabaca.com/idle-since', 'peat.zabaca.com/full-sleep-minutes', 'peat.zabaca.com/suspended-by'])
    expect([d.names.sleepEnabledAnnotation, d.names.hibernationAnnotation]).toEqual(['xata.io/scale-to-zero-enabled', 'cnpg.io/hibernation'])
    expect([d.controlUrl, d.controlNamespace, d.frontPort, d.metricsPort, d.idleMs]).toEqual(['http://peat-control.peat.svc:8080', 'peat', 5432, 9090, 30 * 60_000])
    const set = gatewaySettings({ NAMESPACE: 'pg-store', GATEWAY_NAMESPACE: 'pg-doors', IDLE_MINUTES: '5', FRONT_PORT: '6432', METRICS_PORT: '9191', POD_NAME: 'gw-a' })
    expect([set.names.namespace, set.controlUrl, set.controlNamespace, set.frontPort, set.metricsPort, set.idleMs, set.pod]).toEqual(['pg-doors', 'http://peat-control.pg-store.svc:8080', 'pg-store', 6432, 9191, 5 * 60_000, 'gw-a'])
    expect(gatewaySettings({ CONTROL_URL: 'http://192.0.2.5:8080' }).controlUrl).toBe('http://192.0.2.5:8080')
    expect(() => gatewaySettings({ FRONT_PORT: 'x' })).toThrow('FRONT_PORT')
    expect(() => gatewaySettings({ IDLE_MINUTES: '0' })).toThrow('IDLE_MINUTES')
  })

  test('the pod`s entry, started against a stand-in control layer, serves its route, answers /health and /activity, and its front door answers an SSLRequest with S', async () => {
    const control = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch: (req) => {
        const path = new URL(req.url).pathname
        if (path === '/compute-addrs') return Response.json({})
        if (path === '/services') return Response.json([{ metadata: { name: 'orders', annotations: { [PG_ROUTE_ANNOTATION]: 'peat/orders' } }, spec: { ports: [{ port: 5432, targetPort: 15010 }] } }])
        if (path === '/clusters' || path === '/cluster/orders') {
          const row = { metadata: { name: 'orders', resourceVersion: 'none:0:0', annotations: { [SLEEP_ENABLED_ANNOTATION]: 'true', [SLEEP_IDLE_MINUTES_ANNOTATION]: '30', [HIBERNATION_ANNOTATION]: 'on' } }, status: { readyInstances: 0 } }
          return Response.json(path === '/clusters' ? [row] : row)
        }
        if (path === '/route/orders') return Response.json({ port: 15010 })
        return Response.json({ error: 'not found' }, { status: 404 })
      },
    })
    cleanups.push(() => void control.stop(true))
    const [frontPort, metricsPort] = [await freePort(), await freePort()]
    const child = Bun.spawn(['bun', join(import.meta.dir, '../src/gateway/main.ts')], {
      env: { PATH: process.env['PATH']!, HOME: tmpdir(), CONTROL_URL: `http://127.0.0.1:${control.port}`, FRONT_PORT: String(frontPort), METRICS_PORT: String(metricsPort), POD_NAME: 'gw-test' },
      stdout: 'pipe',
      stderr: 'pipe',
    })
    cleanups.push(() => child.kill())
    let body = ''
    for (let i = 0; i < 100 && !body.includes('database="peat/orders"'); i++) {
      body = await fetch(`http://127.0.0.1:${metricsPort}/metrics`).then((r) => r.text()).catch(() => '')
      if (!body.includes('database="peat/orders"')) await sleep(50)
    }
    expect(body).toContain('database="peat/orders"')
    expect((await fetch(`http://127.0.0.1:${metricsPort}/health`)).status).toBe(200)
    expect(await (await fetch(`http://127.0.0.1:${metricsPort}/activity`)).json()).toEqual({})
    // the front door: an SSLRequest is answered with S by the door itself, before any server name has arrived
    const socket = connect({ host: '127.0.0.1', port: frontPort })
    const answer = await new Promise<string>((resolve, reject) => {
      socket.on('error', reject)
      socket.once('data', (d) => resolve(d.toString('latin1')))
      socket.on('connect', () => { const m = Buffer.alloc(8); m.writeUInt32BE(8, 0); m.writeUInt32BE(80877103, 4); socket.write(m) })
    })
    socket.destroy()
    expect(answer).toBe('S')
  }, 20_000)
})

describe('the folder`s own text', () => {
  test('the gateway`s files have no em dash or en dash', () => {
    for (const f of ['gateway.ts', 'front.ts', 'neon-api.ts', 'main.ts', 'settings.ts']) {
      const text = readFileSync(join(import.meta.dir, '../src/gateway', f), 'utf8')
      expect([f, [0x2013, 0x2014].some((code) => text.includes(String.fromCharCode(code)))]).toEqual([f, false])
    }
  })
})
