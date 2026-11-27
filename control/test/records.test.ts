import { beforeEach, describe, expect, test } from 'bun:test'
import { COPYING } from '../src/databases'
import { TENANT_ANNOTATION, beginMaking, claimPort, copyRecords, endMaking, ensureLogin, fromConfigMap, fromStatus, hold, purgeHeld, readRecords, save, unfinishedMake, type Db, type Deps } from '../src/records'
import { FakeKube } from './fake-kube'

// A database's record kept twice (its object's status and a ConfigMap in the platform namespace), the hold of a deleted one's data, its
// purge, and the copy of records into Database objects: against an API server and a storage controller in memory.

/** A storage controller in memory: tenants, their timelines and each timeline's ancestor. */
class FakeStorcon {
  readonly tenants = new Map<string, Map<string, string | null>>()
  readonly calls: string[] = []
  /** Timelines whose delete is refused (a 412, as for a timeline with timelines made from it). */
  readonly refuse = new Set<string>()
  add(tenant: string, timeline: string, ancestor: string | null = null) {
    if (!this.tenants.has(tenant)) this.tenants.set(tenant, new Map())
    this.tenants.get(tenant)!.set(timeline, ancestor)
  }
  async request(method: string, path: string): Promise<{ status: number; json: any }> {
    this.calls.push(`${method} ${path}`)
    const m = path.match(/^\/v1\/tenant\/([0-9a-f]+)(?:\/timeline(?:\/([0-9a-f]+))?)?$/)
    if (!m) return { status: 404, json: {} }
    const [, tenant, timeline] = m
    const t = this.tenants.get(tenant!)
    if (!t) return { status: 404, json: {} }
    if (method === 'DELETE' && !path.includes('/timeline')) { this.tenants.delete(tenant!); return { status: 200, json: {} } }
    if (method === 'GET' && !timeline) return { status: 200, json: [...t].map(([id, ancestor]) => ({ timeline_id: id, ancestor_timeline_id: ancestor })) }
    if (method === 'DELETE' && timeline) {
      if (this.refuse.has(timeline)) return { status: 412, json: { msg: 'has children' } }
      if (!t.delete(timeline)) return { status: 404, json: {} }
      return { status: 200, json: {} }
    }
    return { status: 405, json: {} }
  }
}

const hex = (c: string) => c.repeat(32)
let k: FakeKube, sc: FakeStorcon, deps: Deps, lines: string[]
beforeEach(() => {
  k = new FakeKube()
  sc = new FakeStorcon()
  lines = []
  deps = {
    kube: (m, p, b, ct) => k.request(m, p, b, ct), storcon: (m, p) => sc.request(m, p), ns: 'peat', dbNs: 'peat-db', domain: 'db.example.com', log: (l) => lines.push(l),
    ancestorOf: async (tenant, timeline) => sc.tenants.get(tenant)?.get(timeline) ?? null,
  }
})
const object = (name: string, namespace: string, spec: Record<string, unknown> = {}, more: Record<string, unknown> = {}) => k.put({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Database', metadata: { name, namespace, ...more }, spec })
const root = (name: string, tenant: string, more: Partial<Db> = {}): Db => ({ name, namespace: 'shop', tenant, timeline: hex('b'), parent: '', from: '', root: true, mode: 'primary', port: 15010, created: '2026-11-01T00:00:00.000Z', pg: 19, closed: false, ...more })
const cm = (name: string) => k.get('v1', 'configmaps', 'peat', name)
const secret = (name: string) => k.get('v1', 'secrets', 'peat', name)
const heldNames = () => k.all('configmaps').filter((c) => c.metadata.labels?.['peat-held'] === 'true').map((c) => c.metadata.name).sort()

describe('the record, in the status and in the platform namespace', () => {
  test('save writes both, in the format the records had before Database objects (with the namespace and the parent it was made from)', async () => {
    object('orders', 'shop')
    const d = root('orders', hex('a'))
    await save(deps, d, { phase: 'Ready' })
    expect(cm('db-orders').metadata.labels).toEqual({ 'peat-record': 'true' })
    expect(cm('db-orders').data).toEqual({ tenant: hex('a'), timeline: hex('b'), parent: '', from: '', root: 'true', mode: 'primary', port: '15010', created: d.created, pg: '19', namespace: 'shop' })
    const o = k.get('peat.zabaca.com/v1alpha1', 'databases', 'shop', 'orders')
    expect(fromStatus(o)).toEqual(d)
    expect(o.status).toMatchObject({ phase: 'Ready', host: 'orders.db.example.com', serviceHost: 'orders.peat-db.svc', port: 5432 })
    expect((await readRecords(deps)).get('orders')!.db).toEqual(d)
  })
  test('a record from before Database objects reads as one of the database namespace, with no parent it was made from', () => {
    const old = fromConfigMap({ metadata: { name: 'db-lake', creationTimestamp: '2026-10-01T00:00:00Z' }, data: { tenant: hex('a'), timeline: hex('b'), parent: '', root: 'true', mode: 'primary', port: '15011', pg: '18' } }, 'peat-db')
    expect(old).toMatchObject({ name: 'lake', namespace: 'peat-db', from: '', created: '2026-10-01T00:00:00Z', pg: 18, port: 15011 })
  })
})

describe('making', () => {
  test('a make claims its name and ids first: a second pass for the same namespace gets the same ids, another namespace is refused', async () => {
    expect(await beginMaking(deps, 'orders', 'shop', { tenant: hex('a'), timeline: hex('b') }, { root: true, mode: 'primary', parent: '', from: '' })).toEqual({ tenant: hex('a'), timeline: hex('b') })
    expect(await beginMaking(deps, 'orders', 'shop', { tenant: hex('c'), timeline: hex('d') }, { root: true, mode: 'primary', parent: '', from: '' })).toEqual({ tenant: hex('a'), timeline: hex('b') })
    expect(await beginMaking(deps, 'orders', 'other', { tenant: hex('c'), timeline: hex('d') }, { root: true, mode: 'primary', parent: '', from: '' })).toEqual({ taken: 'shop' })
    expect(await unfinishedMake(deps, 'orders', 'shop')).toMatchObject({ tenant: hex('a'), timeline: hex('b'), root: true, mode: 'primary' })
    expect(await unfinishedMake(deps, 'orders', 'other')).toBeNull()
    await endMaking(deps, 'orders')
    expect(cm('making-orders')).toBeUndefined()
  })
  test('a login carries its tenant and is kept by a second pass of the same make; a newer database of the name gets its own', async () => {
    let n = 0
    const pw = () => `pw-${++n}`
    await ensureLogin(deps, 'orders', hex('a'), pw)
    await ensureLogin(deps, 'orders', hex('a'), pw)
    expect(secret('db-orders-login').metadata.annotations[TENANT_ANNOTATION]).toBe(hex('a'))
    expect(Buffer.from(secret('db-orders-login').data.password, 'base64').toString()).toBe('pw-1')
    await ensureLogin(deps, 'orders', hex('c'), pw)
    expect(secret('db-orders-login').metadata.annotations[TENANT_ANNOTATION]).toBe(hex('c'))
    expect(Buffer.from(secret('db-orders-login').data.password, 'base64').toString()).toBe('pw-2')
  })
  test('a port: the claim a second pass of the same make already holds, else the first one free', async () => {
    const p = await claimPort(deps, 'orders', hex('b'), 15010, new Set([15010]))
    expect(p).toBe(15011)
    expect(await claimPort(deps, 'orders', hex('b'), 15010, new Set([15010]))).toBe(15011)
    expect(await claimPort(deps, 'orders-dev', hex('c'), 15010, new Set([15010]))).toBe(15012)
    expect(cm('port-15011').data).toEqual({ owner: 'orders', timeline: hex('b') })
  })
})

describe('deleted, held and purged', () => {
  const made = async (d: Db) => {
    object(d.name, d.namespace)
    sc.add(d.tenant, d.timeline)
    await ensureLogin(deps, d.root ? d.name : d.parent, d.tenant, () => 'pw')
    await claimPort(deps, d.name, d.timeline, d.port, new Set())
    await save(deps, d)
  }
  test('a name made again during the hold: the purge of the old one removes its tenant and leaves the new one\'s login, record and port', async () => {
    const old = root('orders', hex('1'), { timeline: hex('2') })
    await made(old)
    await hold(deps, old, new Date('2026-12-08T00:00:00Z'), new Date('2026-12-01T00:00:00Z'))
    expect(cm('db-orders')).toBeUndefined()
    const fresh = root('orders', hex('3'), { timeline: hex('4'), port: 15011 })
    await made(fresh)
    expect(await purgeHeld(deps, [fresh], new Date('2026-12-05T00:00:00Z'))).toEqual([]) // not due yet
    expect(await purgeHeld(deps, [fresh], new Date('2026-12-09T00:00:00Z'))).toEqual(['orders'])
    expect(sc.tenants.has(hex('1'))).toBe(false)
    expect(sc.tenants.has(hex('3'))).toBe(true)
    expect(secret('db-orders-login').metadata.annotations[TENANT_ANNOTATION]).toBe(hex('3'))
    expect(cm('db-orders').data.tenant).toBe(hex('3'))
    expect(cm('port-15010')).toBeUndefined()
    expect(cm('port-15011').data.owner).toBe('orders')
    expect(heldNames()).toEqual([])
  })
  test('the same name deleted twice in one hold keeps both held; a hold of never keeps the data', async () => {
    const a = root('orders', hex('1'), { timeline: hex('2') }), b = root('orders', hex('3'), { timeline: hex('4'), port: 15011 })
    await made(a)
    await hold(deps, a, new Date('2026-12-08T00:00:00Z'))
    await made(b)
    await hold(deps, b, null)
    expect(heldNames()).toEqual([`held-orders-${hex('2')}`, `held-orders-${hex('4')}`])
    expect(await purgeHeld(deps, [], new Date('2027-06-01T00:00:00Z'))).toEqual(['orders'])
    expect(sc.tenants.has(hex('1'))).toBe(false)
    expect(sc.tenants.has(hex('3'))).toBe(true)
    expect(heldNames()).toEqual([`held-orders-${hex('4')}`])
  })
  test('a drill\'s database goes at once: its record is gone before the purge reads what is in use', async () => {
    const d = root('drill-sleep', hex('5'), { timeline: hex('6') })
    await made(d)
    await hold(deps, d, new Date(0))
    // the object still exists (its finalizer is released after the purge); what is in use is every database not being deleted
    expect(await purgeHeld(deps, [])).toEqual(['drill-sleep'])
    expect(sc.tenants.has(hex('5'))).toBe(false)
    expect(secret('db-drill-sleep-login')).toBeUndefined()
    expect(cm('port-15010')).toBeUndefined()
  })
  test('a root\'s tenant waits for its branches; a branch\'s timeline waits for timelines made from it; one refusal does not stop the rest', async () => {
    const r = root('orders', hex('1'), { timeline: hex('2') })
    const dev = root('orders-dev', hex('1'), { timeline: hex('3'), root: false, parent: 'orders', from: 'orders', port: 15011 })
    const sub = root('orders-sub', hex('1'), { timeline: hex('4'), root: false, parent: 'orders', from: 'orders-dev', port: 15012 })
    await made(r); await made(dev); await made(sub)
    sc.add(hex('1'), hex('3'), hex('2')); sc.add(hex('1'), hex('4'), hex('3'))
    await hold(deps, dev, new Date(0))
    await hold(deps, r, new Date(0))
    expect(await purgeHeld(deps, [sub])).toEqual([])
    expect(lines.some((l) => l.includes("orders-dev's timeline has timelines made from it"))).toBe(true)
    expect(sc.tenants.get(hex('1'))!.has(hex('3'))).toBe(true)
    const other = root('lake', hex('7'), { timeline: hex('8'), port: 15013 })
    await made(other)
    sc.refuse.add(hex('4'))
    await hold(deps, sub, new Date(0))
    await hold(deps, other, new Date(0))
    const purged = await purgeHeld(deps, [])
    expect(purged).toContain('lake') // the refused one is logged and the next goes on
    expect(lines.some((l) => l.startsWith('purge: orders-sub:') && l.includes('412'))).toBe(true)
  })
  test('a held branch\'s timeline is kept while a database still uses it (a replica of it)', async () => {
    const dev = root('orders-dev', hex('1'), { timeline: hex('3'), root: false, parent: 'orders', from: 'orders' })
    const ro = root('orders-ro', hex('1'), { timeline: hex('3'), root: false, parent: 'orders', from: 'orders-dev', mode: 'replica', port: 0 })
    await made(dev)
    await hold(deps, dev, new Date(0))
    expect(await purgeHeld(deps, [ro])).toEqual([])
    expect(sc.tenants.get(hex('1'))!.has(hex('3'))).toBe(true)
  })
  test('a login from before logins carried their tenant goes with its root only while no database of the name exists', async () => {
    const old = root('orders', hex('1'), { timeline: hex('2') })
    await made(old)
    k.put({ ...secret('db-orders-login'), metadata: { name: 'db-orders-login', namespace: 'peat' } })
    await hold(deps, old, new Date(0))
    const fresh = root('orders', hex('3'), { timeline: hex('4'), port: 15011 })
    expect(await purgeHeld(deps, [fresh])).toEqual(['orders'])
    expect(secret('db-orders-login')).toBeDefined()
  })
})

describe('the copy of records into Database objects', () => {
  const oldRecord = (name: string, data: Record<string, string>) => k.put({ apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: `db-${name}`, namespace: 'peat', labels: { 'peat-record': 'true' } }, data: { created: '2026-10-01T00:00:00Z', pg: '18', ...data } })
  const login = (root: string) => k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: `db-${root}-login`, namespace: 'peat' }, data: { password: Buffer.from('pw').toString('base64') } })
  const setUp = () => {
    oldRecord('lake', { tenant: hex('1'), timeline: hex('2'), parent: '', root: 'true', mode: 'primary', port: '15010' })
    oldRecord('lake-dev', { tenant: hex('1'), timeline: hex('3'), parent: 'lake', root: 'false', mode: 'primary', port: '15011' })
    oldRecord('lake-sub', { tenant: hex('1'), timeline: hex('4'), parent: 'lake', root: 'false', mode: 'primary', port: '15012' }) // a branch of lake-dev: the old record names the root
    oldRecord('lake-ro', { tenant: hex('1'), timeline: hex('3'), parent: 'lake', root: 'false', mode: 'replica', port: '0' }) // a replica of lake-dev
    sc.add(hex('1'), hex('2')); sc.add(hex('1'), hex('3'), hex('2')); sc.add(hex('1'), hex('4'), hex('3'))
    login('lake')
  }
  const obj = (name: string) => k.get('peat.zabaca.com/v1alpha1', 'databases', 'peat-db', name)
  test('a dry run lists what it would copy and writes nothing', async () => {
    setUp()
    const r = await copyRecords(deps, { dry: true })
    expect(r.missing.sort()).toEqual(['lake', 'lake-dev', 'lake-ro', 'lake-sub'])
    expect(r.countsEqual).toBe(false)
    expect(k.all('databases')).toEqual([])
  })
  test('each record becomes an object whose spec names its direct parent; the counts and fields compare equal; a rerun changes nothing', async () => {
    setUp()
    const r = await copyRecords(deps, { dry: false })
    expect(r.copied.sort()).toEqual(['lake', 'lake-dev', 'lake-ro', 'lake-sub'])
    expect(r.equal.sort()).toEqual(['lake', 'lake-dev', 'lake-ro', 'lake-sub'])
    expect(r).toMatchObject({ records: 4, databases: 4, differ: [], missing: [], countsEqual: true })
    expect(obj('lake-sub').spec).toEqual({ pgVersion: 18, parent: 'lake-dev' })
    expect(obj('lake-ro').spec).toEqual({ pgVersion: 18, replicaOf: 'lake-dev' })
    expect(obj('lake-dev').spec).toEqual({ pgVersion: 18, parent: 'lake' })
    expect(obj('lake').metadata.annotations?.[COPYING]).toBeUndefined()
    expect(fromStatus(obj('lake-sub'))).toMatchObject({ tenant: hex('1'), timeline: hex('4'), parent: 'lake', from: 'lake-dev', port: 15012 })
    expect(cm('db-lake').metadata.labels['peat-superseded']).toBe('true')
    expect(cm('db-lake-sub').data).toMatchObject({ tenant: hex('1'), timeline: hex('4'), parent: 'lake', from: 'lake-dev', namespace: 'peat-db' })
    const again = await copyRecords(deps, { dry: false })
    expect(again).toMatchObject({ copied: [], finished: [], countsEqual: true })
  })
  test('a copy that died half way is finished by the rerun: the mark goes and the old record is labelled', async () => {
    setUp()
    object('lake', 'peat-db', { pgVersion: 18 }, { annotations: { [COPYING]: 'true' } })
    const r = await copyRecords(deps, { dry: false })
    expect(r.finished).toEqual(['lake'])
    expect(obj('lake').metadata.annotations?.[COPYING]).toBeUndefined()
    expect(cm('db-lake').metadata.labels['peat-superseded']).toBe('true')
    expect(r.countsEqual).toBe(true)
  })
  test('a root without its login, an object without a record, or a field that differs: not equal', async () => {
    setUp()
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|secrets|db-lake-login'))!)
    object('extra', 'peat-db')
    await save(deps, root('extra', hex('9'), { namespace: 'peat-db' }))
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|configmaps|db-extra'))!)
    const r = await copyRecords(deps, { dry: false })
    expect(r.differ.find((x) => x.name === 'lake')!.fields).toEqual(['login'])
    expect(r.databases).toBe(5)
    expect(r.countsEqual).toBe(false)
  })
  test('a held database is brought back by name while its data is there, and refused when its name is in use', async () => {
    const d = root('orders', hex('5'), { timeline: hex('6'), namespace: 'peat-db' })
    object('orders', 'peat-db')
    await ensureLogin(deps, 'orders', d.tenant, () => 'pw')
    await save(deps, d)
    await hold(deps, d, new Date('2026-12-08T00:00:00Z'))
    expect((await copyRecords(deps, { dry: false, held: ['orders'] })).refused).toEqual([{ name: 'orders', why: 'the name is in use now: delete that database first' }])
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|databases|orders'))!)
    const r = await copyRecords(deps, { dry: false, held: ['orders', 'nothing'] })
    expect(r.restored).toEqual(['orders'])
    expect(r.refused.map((x) => x.name)).toEqual(['nothing'])
    expect(fromStatus(obj('orders'))).toMatchObject({ tenant: hex('5'), timeline: hex('6') })
    expect(heldNames()).toEqual([])
    expect(r.equal).toEqual(['orders'])
  })
})
