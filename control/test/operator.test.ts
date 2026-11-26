import { beforeEach, describe, expect, test } from 'bun:test'
import { X509Certificate } from 'node:crypto'
import { ENTRIES, verifiedScope } from '../src/operator/keys'
import { PLACEMENTS_CONFIG_MAP, reconcile } from '../src/operator/reconcile'
import { KEYS_SECRET, MANAGED_BY } from '../src/operator/render'
import { installManifests } from '../src/install/install'
import { FakeKube } from './fake-kube'

// The operator as an installer meets it: a Platform declared on a cluster of labelled machines, one pass (or a few), and what the cluster
// then holds and what the Platform's status says. The API server is the in-memory one of fake-kube.ts.

const LABEL = { 'peat.zabaca.com/machine': 'true' }
const node = (name: string, labelled = true) => ({ apiVersion: 'v1', kind: 'Node', metadata: { name, labels: labelled ? { ...LABEL, 'kubernetes.io/hostname': name } : { 'kubernetes.io/hostname': name } }, status: { conditions: [{ type: 'Ready', status: 'True' }] } })
const platform = (spec: Record<string, unknown> = {}, name = 'peat', labels: Record<string, string> = { 'example.com/instance': 'platform' }) => ({
  apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Platform', metadata: { name, labels },
  spec: { release: 'release-2026-12-01', objectStore: { endpoint: 'https://s3.example.com', bucket: 'peat-data', credentialsSecret: 'peat-store' }, ...spec },
})
const decode = (o: any, key: string) => Buffer.from(String(o.data[key]), 'base64').toString()
const env = (dep: any) => Object.fromEntries((dep.spec.template.spec.containers[0].env as any[]).filter((e) => 'value' in e).map((e) => [e.name, e.value]))

let k: FakeKube
beforeEach(() => {
  k = new FakeKube()
  for (const n of ['machine-1', 'machine-2', 'machine-3']) k.put(node(n))
  k.put(node('other-work', false))
})
const statusOf = (name = 'peat') => k.get('peat.zabaca.com/v1alpha1', 'platforms', null, name).status

describe('a first install from one Platform', () => {
  test('every piece is made: the namespaces, the storage pinned by the rules, the control layer, the gateways, the guards', async () => {
    k.put(platform())
    const r = await reconcile(k)
    expect(r.phase).toBe('Installing')
    for (const ns of ['peat', 'peat-compute', 'peat-gateway', 'peat-db']) expect(k.get('v1', 'namespaces', null, ns)).toBeDefined()
    const ps = k.all('statefulsets').filter((s) => s.metadata.name.startsWith('pageserver-w'))
    expect(ps.map((s) => s.metadata.name).sort()).toEqual(['pageserver-w1', 'pageserver-w2'])
    const psNodes = ps.map((s) => s.spec.template.spec.nodeSelector['kubernetes.io/hostname'])
    expect(new Set(psNodes).size).toBe(2)
    const ctl = k.get('apps/v1', 'deployments', 'peat', 'storage-controller')
    const ctlNode = ctl.spec.template.spec.nodeSelector['kubernetes.io/hostname']
    expect(psNodes).not.toContain(ctlNode) // never beside a pageserver
    expect(k.get('apps/v1', 'deployments', 'peat', 'storage-broker').spec.template.spec.nodeSelector['kubernetes.io/hostname']).toBe(ctlNode)
    expect(k.get('apps/v1', 'deployments', 'peat', 'peat-control').spec.replicas).toBe(2)
    expect(k.get('apps/v1', 'deployments', 'peat-gateway', 'peat-gateway').spec.replicas).toBe(2)
    expect(k.get('v1', 'services', 'peat-gateway', 'pg-front')).toBeDefined()
    expect(k.all('validatingadmissionpolicies').map((p) => p.metadata.name).sort()).toEqual(['peat-control-keeps-to-peat-nodes', 'peat-control-services-stay-inside', 'peat-control-statefulsets-keep-to-the-safekeeper'])
    // the machine not labelled is never used
    expect(JSON.stringify([...k.objects.values()].filter((o) => o.kind !== 'Node'))).not.toContain('other-work')
  })

  test('the safekeepers are the control layer\'s to make: it is told three members on three distinct machines', async () => {
    k.put(platform())
    await reconcile(k)
    const seed = String(env(k.get('apps/v1', 'deployments', 'peat', 'peat-control'))['SAFEKEEPER_SEED'])
    const members = seed.split(',').map((x) => x.split('='))
    expect(members.map((m) => m[0])).toEqual(['1', '2', '3'])
    expect(new Set(members.map((m) => m[1])).size).toBe(3)
    expect(k.all('statefulsets').filter((s) => s.metadata.name.startsWith('safekeeper'))).toEqual([])
  })

  test('keys none were given: made once, whole, kept in the operator\'s record and given to each piece; a second pass makes none', async () => {
    k.put(platform())
    await reconcile(k)
    const rec = k.get('v1', 'secrets', 'peat', KEYS_SECRET)
    expect(Object.keys(rec.data).sort()).toEqual([...ENTRIES].sort())
    const pub = decode(rec, 'auth_public_key.pem')
    expect(decode(k.get('v1', 'secrets', 'peat', 'peat-auth-public'), 'auth_public_key.pem')).toBe(pub)
    expect(verifiedScope(decode(k.get('v1', 'secrets', 'peat', 'peat-storcon-tokens'), 'storcon_to_pageservers.jwt'), pub)).toBe('pageserverapi')
    expect(new X509Certificate(decode(k.get('v1', 'secrets', 'peat-compute', 'peat-ca'), 'tls.crt')).ca).toBe(true)
    // the control layer signs databases' certificates: the authority's key reaches it in its own namespace
    expect(decode(k.get('v1', 'secrets', 'peat', 'peat-ca-key'), 'tls.key')).toContain('PRIVATE KEY')
    const before = JSON.stringify(rec.data)
    await reconcile(k)
    expect(JSON.stringify(k.get('v1', 'secrets', 'peat', KEYS_SECRET).data)).toBe(before)
  })

  test('the machines chosen are kept: a pass after a new machine is labelled moves nothing', async () => {
    k.put(platform())
    await reconcile(k)
    const placed = k.get('v1', 'configmaps', 'peat', PLACEMENTS_CONFIG_MAP).data['placements.json']
    k.put(node('machine-0'))
    await reconcile(k)
    expect(k.get('v1', 'configmaps', 'peat', PLACEMENTS_CONFIG_MAP).data['placements.json']).toBe(placed)
  })

  test('nothing the operator makes carries a label of the Platform object', async () => {
    k.put(platform({}, 'peat', { 'example.com/instance': 'platform' }))
    await reconcile(k)
    const made = [...k.objects.values()].filter((o) => o.metadata?.labels?.[MANAGED_BY] === 'peat-operator')
    expect(made.length).toBeGreaterThan(30)
    for (const o of made) expect(o.metadata.labels['example.com/instance']).toBeUndefined()
  })

  test('the status names every piece and its release, and says Ready once each is', async () => {
    k.put(platform())
    await reconcile(k)
    expect(statusOf().phase).toBe('Installing')
    expect(statusOf().survives).toBe("one machine's loss")
    for (const o of [...k.all('deployments'), ...k.all('statefulsets')]) o.status = { readyReplicas: o.spec.replicas, updatedReplicas: o.spec.replicas }
    for (const [i, m] of ['machine-1', 'machine-2', 'machine-3'].entries()) k.put({ apiVersion: 'apps/v1', kind: 'StatefulSet', metadata: { name: `safekeeper-w${i + 1}`, namespace: 'peat', labels: { 'peat-sk': 'true' } }, spec: { replicas: 1, template: { spec: { nodeSelector: { 'kubernetes.io/hostname': m }, containers: [{ image: 'ghcr.io/zabaca/peat/storage:release-2026-12-01' }] } } }, status: { readyReplicas: 1, updatedReplicas: 1 } })
    const r = await reconcile(k)
    expect(r.message).toBe('every piece is ready on release-2026-12-01')
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-2026-12-01' })
    expect((statusOf().pieces as any[]).length).toBe(2 + 2 + 2 + 3) // the controller and broker, two pageservers, the control layer and gateways, three safekeepers
  })
})

describe('what the operator refuses or leaves alone', () => {
  test('an object of a piece\'s name that it did not make is left as it is and named in the status', async () => {
    k.put({ apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: 'storage-broker', namespace: 'peat' }, spec: { replicas: 1, template: { spec: { containers: [{ image: 'someone-else' }] } } } })
    k.put(platform())
    const r = await reconcile(k)
    expect(k.get('apps/v1', 'deployments', 'peat', 'storage-broker').spec.template.spec.containers[0].image).toBe('someone-else')
    expect(r.refused[0]).toContain('Deployment/peat/storage-broker exists and was not made by the operator')
    expect(statusOf().phase).toBe('Installing')
  })
  test('a Platform that is not valid is refused with every fault, and nothing is made', async () => {
    k.put(platform({ spread: 'zones', safekeepers: { count: 4 } }))
    const r = await reconcile(k)
    expect(r.phase).toBe('Failed')
    expect(r.message).toContain('spread: is one of machines, none')
    expect(r.message).toContain('safekeepers.count: is one of 3, 5')
    expect(k.all('deployments')).toEqual([])
  })
  test('spread machines with two machines: refused with the rule, nothing made', async () => {
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|machine-3'))!)
    k.put(platform())
    const r = await reconcile(k)
    expect(r.phase).toBe('Failed')
    expect(r.message).toContain('the controller needs a machine with no pageserver')
    expect(k.all('deployments')).toEqual([])
  })
  test('a second Platform is refused: a cluster holds one', async () => {
    k.put(platform())
    k.put(platform({}, 'another'))
    await reconcile(k)
    expect(statusOf('another')).toMatchObject({ phase: 'Failed', message: 'a cluster holds one platform: peat is the one acted on' })
  })
  test('suspend: nothing is changed', async () => {
    k.put(platform({ suspend: true }))
    const r = await reconcile(k)
    expect(r.phase).toBe('Suspended')
    expect(k.all('deployments')).toEqual([])
  })
  test('a pageserver count lowered is refused: a pageserver holds data', async () => {
    k.put(platform({ pageservers: { count: 2 } }))
    await reconcile(k)
    k.put(platform({ pageservers: { count: 1 } }))
    const r = await reconcile(k)
    expect(r.phase).toBe('Failed')
    expect(r.message).toContain('pageservers.count: 1 is fewer than the 2')
    expect(k.get('apps/v1', 'statefulsets', 'peat', 'pageserver-w2')).toBeDefined()
  })
})

describe('keeping it as declared', () => {
  test('what the operator made and no longer renders is removed; what it did not make never is', async () => {
    k.put(platform({ safekeepers: { outside: { machines: ['outside-1'] } } }))
    await reconcile(k)
    expect(k.get('batch/v1', 'jobs', 'peat', 'keeper-prep')).toBeDefined()
    k.put({ apiVersion: 'batch/v1', kind: 'Job', metadata: { name: 'someone-elses', namespace: 'peat' }, spec: {} })
    k.put(platform())
    const r = await reconcile(k)
    expect(r.pruned).toBe(1)
    expect(k.get('batch/v1', 'jobs', 'peat', 'keeper-prep')).toBeUndefined()
    expect(k.get('batch/v1', 'jobs', 'peat', 'someone-elses')).toBeDefined()
  })
  test('a release changed reaches every piece the operator makes (the ordered upgrade replaces this in a later change)', async () => {
    k.put(platform())
    await reconcile(k)
    k.put(platform({ release: 'release-2026-12-15' }))
    await reconcile(k)
    expect(k.get('apps/v1', 'deployments', 'peat', 'storage-controller').spec.template.spec.containers[0].image).toBe('ghcr.io/zabaca/peat/storage:release-2026-12-15')
    expect(k.get('apps/v1', 'deployments', 'peat', 'peat-control').spec.template.spec.containers[0].image).toBe('ghcr.io/zabaca/peat/control:release-2026-12-15')
  })
})

describe('what a client of a database needs, in its own namespace', () => {
  test('the authority\'s certificate in every namespace that holds a Database, each made database\'s login (a branch uses its root\'s), named in its status; removed with the database', async () => {
    k.put(platform({ gateways: { domain: 'db.example.com' } }))
    await reconcile(k)
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'db-orders-login', namespace: 'peat' }, data: { password: Buffer.from('s3cret word').toString('base64') } })
    const rec = (parent = '', root = true) => ({ tenant: 'a'.repeat(32), timeline: 'b'.repeat(32), pgVersion: 19, phase: 'Ready', record: { mode: 'primary', root, parent, port: 15010 } })
    k.put({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Database', metadata: { name: 'orders', namespace: 'shop' }, spec: {}, status: rec() })
    k.put({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Database', metadata: { name: 'orders-dev', namespace: 'shop' }, spec: { parent: 'orders' }, status: rec('orders', false) })
    k.put({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Database', metadata: { name: 'pending', namespace: 'other' }, spec: {} })
    await reconcile(k)
    expect(k.get('v1', 'configmaps', 'shop', 'peat-ca').data['ca.crt']).toContain('BEGIN CERTIFICATE')
    expect(k.get('v1', 'configmaps', 'other', 'peat-ca')).toBeDefined()
    const login = k.get('v1', 'secrets', 'shop', 'orders-login')
    expect(decode(login, 'password')).toBe('s3cret word')
    expect(decode(login, 'host')).toBe('orders.db.example.com')
    expect(decode(login, 'url')).toBe('postgresql://app:s3cret%20word@orders.db.example.com:5432/app?sslmode=verify-full')
    expect(decode(k.get('v1', 'secrets', 'shop', 'orders-dev-login'), 'password')).toBe('s3cret word')
    expect(k.get('v1', 'secrets', 'other', 'pending-login')).toBeUndefined()
    expect(k.get('peat.zabaca.com/v1alpha1', 'databases', 'shop', 'orders').status).toMatchObject({ loginSecret: 'orders-login', caConfigMap: 'peat-ca' })
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|databases|orders-dev'))!)
    await reconcile(k)
    expect(k.get('v1', 'secrets', 'shop', 'orders-dev-login')).toBeUndefined()
  })
})

describe('a one-machine trial', () => {
  test('spread none: every piece on the one machine, one replica each, the safekeepers zones of their own, and the status says what it survives', async () => {
    for (const n of ['machine-2', 'machine-3']) k.objects.delete([...k.objects.keys()].find((x) => x.endsWith(`|${n}`))!)
    k.put(platform({ spread: 'none', pageservers: { count: 1 }, gateways: { count: 1 }, control: { replicas: 1 } }))
    const r = await reconcile(k)
    expect(r.phase).toBe('Installing')
    const nodes = new Set([...k.all('deployments'), ...k.all('statefulsets')].map((o) => o.spec.template.spec.nodeSelector?.['kubernetes.io/hostname']).filter(Boolean))
    expect([...nodes]).toEqual(['machine-1'])
    const control = k.get('apps/v1', 'deployments', 'peat', 'peat-control')
    expect(control.spec.template.spec.affinity).toBeUndefined()
    expect(env(control)['SPREAD']).toBe('none')
    expect(env(control)['SAFEKEEPER_SEED']).toBe('1=machine-1,2=machine-1,3=machine-1')
    expect(statusOf().survives).toContain("no machine's loss")
  })
})

describe('the install file', () => {
  test('the kinds, the operator\'s namespace, its account and its Deployment of the image given', () => {
    const m = installManifests('ghcr.io/zabaca/peat/control:release-2026-12-01') as any[]
    expect(m.filter((x) => x.kind === 'CustomResourceDefinition').map((x) => x.metadata.name)).toContain('platforms.peat.zabaca.com')
    const dep = m.find((x) => x.kind === 'Deployment')
    expect(dep.metadata.namespace).toBe('peat-system')
    expect(dep.spec.template.spec.containers[0]).toMatchObject({ image: 'ghcr.io/zabaca/peat/control:release-2026-12-01', args: ['operator'] })
    const role = m.find((x) => x.kind === 'ClusterRole')
    expect(role.rules.find((r: any) => r.apiGroups.includes('rbac.authorization.k8s.io')).verbs).toContain('escalate')
  })
})
