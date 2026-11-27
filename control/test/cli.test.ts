import { beforeEach, describe, expect, test } from 'bun:test'
import { checkName, parseArgs, parseAt, run } from '../src/cli/peat'
import { FakeKube } from './fake-kube'

// The command as a user types it: each verb is a change to a Database (or the Platform) through the user's own login, read back until it is
// answered. The API server is fake-kube's; the control layer's answers are written into it by the tests.

let k: FakeKube
let lines: string[]
const out = { line: (s: string) => lines.push(s) }
const peat = (...argv: string[]) => run(argv, { kube: k, out, everyMs: 5, now: () => Date.parse('2026-12-01T12:00:00Z') })
const DB = 'peat.zabaca.com/v1alpha1'
const made = (name: string, ns = 'shop', more: Record<string, unknown> = {}) => k.put({ apiVersion: DB, kind: 'Database', metadata: { name, namespace: ns }, spec: {}, status: { tenant: 'a'.repeat(32), timeline: 'b'.repeat(32), pgVersion: 19, phase: 'Ready', host: `${name}.db.example.com`, port: 5432, serviceHost: `${name}.peat-db.svc`, loginSecret: `${name}-login`, caConfigMap: 'peat-ca', ...more } })
beforeEach(() => { k = new FakeKube(); k.namespace = 'shop'; lines = [] })

describe('the words a user types', () => {
  test('a name, a moment and options are checked before anything is asked of the cluster', () => {
    expect(() => checkName('Orders')).toThrow('lowercase')
    expect(() => checkName('drill-a')).toThrow("drills")
    expect(parseAt('2026-12-01T09:30:00Z', Date.parse('2026-12-02T00:00:00Z'))).toBe('2026-12-01T09:30:00.000Z')
    expect(parseAt('16/b374d848')).toBe('16/B374D848')
    expect(() => parseAt('2027-01-01T00:00:00Z', Date.parse('2026-12-01T00:00:00Z'))).toThrow('in the future')
    expect(() => parseAt('yesterday')).toThrow('log position')
    expect(parseArgs(['make', 'orders', '--pg', '18', '-n', 'shop', '--no-wait'])).toEqual({ words: ['make', 'orders'], opts: { pg: '18', n: 'shop', namespace: 'shop', 'no-wait': true } })
  })
  test('no verb, or one that is not a verb, prints the usage and exits 2', async () => {
    expect(await peat()).toBe(2)
    expect(await peat('frobnicate')).toBe(2)
    expect(lines[0]).toContain('usage: peat')
  })
})

describe('making databases', () => {
  test('make writes a Database in the login\'s namespace and waits until it is made', async () => {
    setTimeout(() => { const o = k.get(DB, 'databases', 'shop', 'orders'); o.status = { tenant: 'a'.repeat(32), phase: 'Ready', pgVersion: 18, host: 'orders.db.example.com', port: 5432, loginSecret: 'orders-login' } }, 30)
    expect(await peat('make', 'orders', '--pg', '18')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders').spec).toEqual({ pgVersion: 18 })
    expect(lines.join('\n')).toContain('shop/orders: Ready, Postgres 18')
    expect(lines.join('\n')).toContain('connect to orders.db.example.com:5432')
  })
  test('a branch, a restore and a replica go into their parent\'s namespace, found by name', async () => {
    made('orders', 'shop')
    expect(await peat('branch', 'orders', 'orders-dev', '--no-wait')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders-dev').spec).toEqual({ parent: 'orders' })
    expect(await peat('restore', 'orders', 'orders-tue', '--at', '2026-12-01T09:30:00Z', '--no-wait')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders-tue').spec).toEqual({ parent: 'orders', at: '2026-12-01T09:30:00.000Z' })
    expect(await peat('replica', 'orders', 'orders-ro', '--no-wait')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders-ro').spec).toEqual({ replicaOf: 'orders' })
  })
  test('a name that exists, a parent that does not, and a failed make are said plainly and exit 1', async () => {
    made('orders')
    expect(await peat('make', 'orders', '--no-wait')).toBe(1)
    expect(lines.at(-1)).toBe('peat: orders exists in shop')
    expect(await peat('branch', 'nothing', 'x')).toBe(1)
    expect(lines.at(-1)).toBe('peat: no database nothing in shop (-n names another namespace; -A looks in every one)')
    setTimeout(() => { k.get(DB, 'databases', 'shop', 'taken').status = { phase: 'Failed', message: 'the name taken is taken by the database in other: a name is unique across the platform' } }, 20)
    expect(await peat('make', 'taken')).toBe(1)
    expect(lines.at(-1)).toContain('a name is unique across the platform')
  })
})

describe('keeping them', () => {
  test('stop asks once and waits for the answer', async () => {
    made('orders')
    setTimeout(() => { const o = k.get(DB, 'databases', 'shop', 'orders'); o.status.answered = { stop: o.spec.requests.stop } }, 20)
    // answered is not enough: a status written again from the record marks a request answered whatever the compute does
    setTimeout(() => { k.get(DB, 'databases', 'shop', 'orders').status.awake = false }, 60)
    expect(await peat('stop', 'orders')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders').spec.requests.stop).toBe('2026-12-01T12:00:00.000Z')
  })
  test('close and open change the spec and wait for the phase', async () => {
    made('orders')
    setTimeout(() => { k.get(DB, 'databases', 'shop', 'orders').status.phase = 'Closed' }, 20)
    expect(await peat('close', 'orders')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders').spec.closed).toBe(true)
    setTimeout(() => { k.get(DB, 'databases', 'shop', 'orders').status.phase = 'Ready' }, 20)
    expect(await peat('open', 'orders')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders').spec.closed).toBe(false)
  })
  test('delete needs --yes', async () => {
    made('orders')
    expect(await peat('delete', 'orders')).toBe(1)
    expect(k.get(DB, 'databases', 'shop', 'orders')).toBeDefined()
    expect(await peat('delete', 'orders', '--yes')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders')).toBeUndefined()
  })
  test('move raises the version and follows the move to its end; a rehearsal names its version; a retry and the way back are requests', async () => {
    made('orders', 'shop', { pgVersion: 18 })
    const answer = (move: Record<string, unknown>, from: 'rehearseMove' | 'retryMove' | null = null) => setTimeout(() => { const o = k.get(DB, 'databases', 'shop', 'orders'); o.status.move = { ...move, ...(from ? { request: o.spec.requests?.[from] } : {}) } }, 20)
    answer({ to: 19, rehearsal: true, state: 'done', message: 'rehearsed: copied in 3 s and compared equal' }, 'rehearseMove')
    expect(await peat('move', 'orders', '--pg', '19', '--rehearse')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders').spec.requests.rehearseMove).toBe('19@2026-12-01T12:00:00.000Z')
    expect(lines.at(-1)).toBe('shop/orders: done: rehearsed: copied in 3 s and compared equal')
    expect(k.get(DB, 'databases', 'shop', 'orders').spec.pgVersion).toBeUndefined()
    answer({ to: 19, state: 'done', message: 'moved to Postgres 19 under its own name' })
    expect(await peat('move', 'orders', '--pg', '19')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders').spec.pgVersion).toBe(19)
    answer({ to: 19, state: 'failed', message: 'the copy is not the same' }, 'retryMove')
    expect(await peat('move', 'orders', '--retry')).toBe(1)
    expect(k.get(DB, 'databases', 'shop', 'orders').spec.requests.retryMove).toBe('2026-12-01T12:00:00.000Z')
    expect(await peat('move', 'orders', '--back', '--no-wait')).toBe(0)
    expect(k.get(DB, 'databases', 'shop', 'orders').spec.requests.moveBack).toBe('2026-12-01T12:00:00.000Z')
  })
  test('a move to a version not above the one it runs is refused before anything is asked', async () => {
    made('orders', 'shop', { pgVersion: 19 })
    expect(await peat('move', 'orders', '--pg', '19')).toBe(1)
    expect(lines.at(-1)).toBe('peat: orders runs Postgres 19: a move goes to a newer version')
    expect(k.get(DB, 'databases', 'shop', 'orders').spec).toEqual({})
  })

})

describe('which database a name means', () => {
  test('the context\'s namespace (or -n), as make and list use; another namespace\'s database of the name is never touched', async () => {
    made('orders', 'prod')
    expect(await peat('delete', 'orders', '--yes')).toBe(1)
    expect(lines.at(-1)).toBe('peat: no database orders in shop (-n names another namespace; -A looks in every one)')
    expect(k.get(DB, 'databases', 'prod', 'orders')).toBeDefined()
    expect(await peat('close', 'orders', '--no-wait')).toBe(1)
    expect(k.get(DB, 'databases', 'prod', 'orders').spec.closed).toBeUndefined()
    expect(await peat('show', 'orders', '-n', 'prod')).toBe(0)
  })
  test('-A finds the one of that name in any namespace, and refuses a name more than one namespace holds', async () => {
    made('orders', 'prod')
    expect(await peat('show', 'orders', '-A')).toBe(0)
    expect(lines.at(-1)).toContain('prod/orders: Ready')
    made('orders', 'staging')
    expect(await peat('delete', 'orders', '-A', '--yes')).toBe(1)
    expect(lines.at(-1)).toBe('peat: orders is in more than one namespace (prod, staging): name one with -n')
    expect(k.get(DB, 'databases', 'prod', 'orders')).toBeDefined()
  })
  test('the delete prompt and outcome, and a close, name where they act', async () => {
    made('orders')
    const here = Object.assign(k, { where: 'context staging at https://cluster.example.com' })
    const at = (...argv: string[]) => run(argv, { kube: here, out, everyMs: 5 })
    expect(await at('delete', 'orders')).toBe(1)
    expect(lines.at(-1)).toContain('deleting shop/orders (context staging at https://cluster.example.com) removes its compute')
    expect(await at('delete', 'orders', '--yes')).toBe(0)
    expect(lines.at(-1)).toBe("shop/orders (context staging at https://cluster.example.com): deleted (its compute goes now, its data after the platform's hold)")
  })
  test('--timeout needs a number; peat help prints the commands', async () => {
    expect(await peat('show', 'orders', '--timeout', 'abc')).toBe(2)
    expect(await peat('help')).toBe(0)
    expect(lines.join('\n')).toContain('peat url <db>')
  })
  test('retry asks the one Platform, or the one named; never the first of several', async () => {
    k.put({ apiVersion: DB, kind: 'Platform', metadata: { name: 'a' }, spec: {} })
    k.put({ apiVersion: DB, kind: 'Platform', metadata: { name: 'b' }, spec: {} })
    expect(await peat('retry')).toBe(1)
    expect(lines.at(-1)).toBe('peat: more than one Platform (a, b): name one')
    expect(await peat('retry', 'b')).toBe(0)
    expect(k.get(DB, 'platforms', null, 'b').spec.requests.retry).toBeDefined()
    expect(k.get(DB, 'platforms', null, 'a').spec.requests).toBeUndefined()
  })
  test('ca <db> reads the ConfigMap its status names, in its namespace', async () => {
    made('orders', 'shop', { caConfigMap: 'corp-ca' })
    k.put({ apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'corp-ca', namespace: 'shop' }, data: { 'ca.crt': 'the corp one' } })
    expect(await peat('ca', 'orders')).toBe(0)
    expect(lines.at(-1)).toBe('the corp one')
  })
})

describe('the options a user types', () => {
  test('an unknown option is refused, never ignored; -n<value> is read as kubectl reads it; an empty namespace is refused', async () => {
    made('orders', 'prod')
    expect(await peat('delete', 'orders', '--namespcae', 'prod', '--yes')).toBe(2)
    expect(lines.at(-1)).toBe('unknown option --namespcae (peat help lists them)')
    expect(await peat('delete', 'orders', '-nprod', '--yes')).toBe(0)
    expect(k.get(DB, 'databases', 'prod', 'orders')).toBeUndefined()
    expect(await peat('list', '-n', '')).toBe(2)
    expect(lines.at(-1)).toBe('-n needs a namespace, not an empty one')
    expect(await peat('show', 'x', '--yes=1')).toBe(2)
  })
  test('list -A -n prod lists prod only, as the other verbs take -n over -A', async () => {
    made('orders', 'prod'); made('lake', 'staging')
    expect(await peat('list', '-A', '-n', 'prod')).toBe(0)
    expect(lines).toEqual(['prod/orders  Ready  pg19  asleep  orders.db.example.com'])
  })
  test('a kubeconfig that cannot be read is said in one line', async () => {
    expect(await run(['list', '--kubeconfig', '/nowhere/at/all'], { out, everyMs: 5 })).toBe(1)
    expect(lines).toEqual(['peat: no kubeconfig at /nowhere/at/all: set KUBECONFIG, or log in to the cluster as kubectl would'])
  })
})

describe('reading', () => {
  test('list, show, url and ca', async () => {
    made('orders')
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'orders-login', namespace: 'shop' }, data: { url: Buffer.from('postgresql://app:pw@orders.db.example.com:5432/app?sslmode=verify-full').toString('base64') } })
    k.put({ apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'peat-ca', namespace: 'shop' }, data: { 'ca.crt': '-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n' } })
    expect(await peat('list')).toBe(0)
    expect(lines.at(-1)).toBe('shop/orders  Ready  pg19  asleep  orders.db.example.com')
    expect(await peat('show', 'orders')).toBe(0)
    expect(lines.at(-1)).toContain('login: Secret orders-login')
    expect(await peat('url', 'orders')).toBe(0)
    expect(lines.at(-1)).toBe('postgresql://app:pw@orders.db.example.com:5432/app?sslmode=verify-full')
    expect(await peat('ca')).toBe(0)
    expect(lines.at(-1)).toBe('-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----')
  })
  test('status and retry, on the Platform', async () => {
    k.put({ apiVersion: DB, kind: 'Platform', metadata: { name: 'peat' }, spec: { release: 'release-2026-12-15' }, status: { phase: 'Halted', release: 'release-2026-12-01', survives: "one machine's loss", message: 'halted', pieces: [{ kind: 'Deployment', namespace: 'peat', name: 'storage-controller', release: 'release-2026-12-15', ready: true }], upgrade: { halted: { step: 'pageserver-w1', reason: 'a secondary is not warm' } } } })
    expect(await peat('status')).toBe(0)
    expect(lines.join('\n')).toContain('peat: Halted, release release-2026-12-15 (running release-2026-12-01)')
    expect(lines.join('\n')).toContain('halted at pageserver-w1: a secondary is not warm')
    expect(await peat('retry')).toBe(0)
    expect(k.get(DB, 'platforms', null, 'peat').spec.requests.retry).toBe('2026-12-01T12:00:00.000Z')
  })
})
