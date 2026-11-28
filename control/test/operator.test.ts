import { beforeEach, describe, expect, test } from 'bun:test'
import { X509Certificate } from 'node:crypto'
import { ENTRIES, verifiedScope } from '../src/operator/keys'
import { PLACEMENTS_CONFIG_MAP, reconcile } from '../src/operator/reconcile'
import { KEYS_SECRET, MANAGED_BY } from '../src/operator/render'
import { EXEC_ROLE, OPERATOR_EXEC_POLICY, OPERATOR_POLICIES, SECRETS_ROLE, installManifests } from '../src/install/install'
import { RELEASE_IMAGES_ENV, imagesOf, parseReleaseImages, specOf, type ReleaseImages } from '../src/operator/platform'
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
    expect(k.all('validatingadmissionpolicies').map((p) => p.metadata.name).sort()).toEqual(['peat-control-computes-of-the-compute-images', 'peat-control-exec-only-psql-in-a-compute', 'peat-control-keeps-to-peat-nodes', 'peat-control-services-stay-inside', 'peat-control-statefulsets-keep-to-the-safekeeper'])
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
    // clients get the authority's certificate and nothing else: no Secret of it outside the platform namespace, and its key only in peat-ca-key
    expect(new X509Certificate(k.get('v1', 'configmaps', 'peat-db', 'peat-ca').data['ca.crt']).ca).toBe(true)
    expect([...k.objects.values()].filter((o) => o.kind === 'Secret' && o.metadata.namespace !== 'peat')).toEqual([])
    expect([...k.objects.values()].filter((o) => o.kind === 'Secret' && Object.keys(o.data ?? {}).some((x) => x === 'tls.key' || x === 'ca.key')).map((o) => o.metadata.name).sort()).toEqual(['peat-ca-key', KEYS_SECRET])
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

describe('the review\'s rules', () => {
  test('spread none: one pageserver by default, two refused', async () => {
    for (const n of ['machine-2', 'machine-3']) k.objects.delete([...k.objects.keys()].find((x) => x.endsWith(`|${n}`))!)
    k.put(platform({ spread: 'none' }))
    await reconcile(k)
    expect(k.all('statefulsets').filter((x) => x.metadata.name.startsWith('pageserver')).map((x) => x.metadata.name)).toEqual(['pageserver-w1'])
    k.put(platform({ spread: 'none', pageservers: { count: 2 } }, 'peat'))
    expect((await reconcile(k)).message).toContain('spread none runs one pageserver, not 2')
  })
  test('the first safekeepers go to machines of distinct zones; too few zones are refused with the rule', async () => {
    const zone = (n: string, z: string) => { const o = k.get('v1', 'nodes', null, n); o.metadata.labels['topology.kubernetes.io/zone'] = z }
    zone('machine-1', 'a'); zone('machine-2', 'a'); zone('machine-3', 'b')
    k.put(platform({ safekeepers: { zoneLabel: 'topology.kubernetes.io/zone' } }))
    const r = await reconcile(k)
    expect(r.phase).toBe('Failed')
    expect(r.message).toContain('3 safekeepers need 3 zones of topology.kubernetes.io/zone, and their machines are in 2')
  })
  test('a release is reported as stamped on each piece, also when its images are pinned by digest', async () => {
    k.put(platform({ images: { storage: 'ghcr.io/zabaca/peat/storage@sha256:' + 'a'.repeat(64), control: 'ghcr.io/zabaca/peat/control@sha256:' + 'b'.repeat(64) } }))
    await reconcile(k)
    expect((statusOf().pieces as any[]).find((x) => x.name === 'storage-controller').release).toBe('release-2026-12-01')
    expect(k.get('apps/v1', 'deployments', 'peat', 'peat-control').metadata.annotations['peat.zabaca.com/release']).toBe('release-2026-12-01')
  })
  test('records kubernetes on a cluster that does not serve the record kinds: refused with why', async () => {
    k.forced.set('GET /apis/storagecontroller.peat.zabaca.com/v1alpha1', { status: 404, json: {} })
    k.put(platform())
    const r = await reconcile(k)
    expect(r.phase).toBe('Failed')
    expect(r.message).toContain("needs the storage controller's record kinds")
    expect(k.all('deployments')).toEqual([])
  })
  test('the namespaces: one of the cluster\'s own refused; once recorded, a change refused', async () => {
    k.put(platform({ namespaces: { platform: 'kube-system' } }))
    expect((await reconcile(k)).message).toContain("namespaces.platform: kube-system is the cluster's own")
    k.put(platform())
    await reconcile(k)
    const o = k.get('peat.zabaca.com/v1alpha1', 'platforms', null, 'peat')
    o.spec = { ...o.spec, namespaces: { platform: 'elsewhere' } }
    expect((await reconcile(k)).message).toContain('the namespaces cannot change')
  })
})

describe('a given authority, and an issuer', () => {
  test('keys.caSecret is read from the platform namespace, and peat-ca carries its certificate', async () => {
    const { newCa } = await import('../src/operator/keys')
    const ca = newCa()
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'my-ca', namespace: 'peat' }, data: { 'tls.crt': Buffer.from(ca.certPem).toString('base64'), 'tls.key': Buffer.from(ca.keyPem).toString('base64') } })
    k.put(platform({ keys: { caSecret: 'my-ca' } }))
    await reconcile(k)
    expect(k.get('v1', 'configmaps', 'peat-db', 'peat-ca').data['ca.crt']).toBe(ca.certPem)
  })
  test('with an issuer and no authority named, no peat-ca is published (the platform\'s own would sign nothing)', async () => {
    k.put(platform({ certificates: { issuer: { name: 'corp-ca', kind: 'ClusterIssuer' } } }))
    await reconcile(k)
    expect(k.get('v1', 'configmaps', 'peat-db', 'peat-ca')).toBeUndefined()
  })
  test('with an issuer, keys.caSecret needs the issuer\'s certificate alone: peat-ca carries it, and its key is never kept', async () => {
    const { newCa } = await import('../src/operator/keys')
    const issuer = newCa()
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'corp-ca', namespace: 'peat' }, data: { 'ca.crt': Buffer.from(issuer.certPem).toString('base64') } })
    k.put(platform({ certificates: { issuer: { name: 'corp-ca', kind: 'ClusterIssuer' } }, keys: { caSecret: 'corp-ca' } }))
    const r = await reconcile(k)
    expect(r.phase).not.toBe('Failed')
    expect(k.get('v1', 'configmaps', 'peat-db', 'peat-ca').data['ca.crt']).toBe(issuer.certPem)
    const kept = k.get('v1', 'secrets', 'peat', KEYS_SECRET).data
    expect(Buffer.from(kept['ca.crt'], 'base64').toString()).not.toBe(issuer.certPem) // the operator's own, which signs nothing here
    // given the key too: refused, and the key is copied nowhere
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'corp-ca', namespace: 'peat' }, data: { 'tls.crt': Buffer.from(issuer.certPem).toString('base64'), 'tls.key': Buffer.from(issuer.keyPem).toString('base64') } })
    const refused = await reconcile(k)
    expect(refused.phase).toBe('Failed')
    expect(refused.message).toContain("holds tls.key; with certificates.issuer give the issuer's certificate alone")
    // an issuer's certificate in its last 30 days: the status says so
    const soon = newCa(new Date(), 10)
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'corp-ca', namespace: 'peat' }, data: { 'ca.crt': Buffer.from(soon.certPem).toString('base64') } })
    expect((await reconcile(k)).message).toContain("keys.caSecret: the issuer's certificate expires on")
    expect([...k.objects.values()].filter((o) => o.kind === 'Secret' && o.metadata.name !== 'corp-ca' && Object.values(o.data ?? {}).some((v: any) => Buffer.from(v, 'base64').toString() === issuer.keyPem))).toEqual([])
  })
})

describe('what is the operator\'s: its record of the objects it made, never a label', () => {
  test('an object with the operator\'s labels that it did not make is neither applied over nor deleted, here or in another namespace', async () => {
    const labels = { [MANAGED_BY]: 'peat-operator', 'peat.zabaca.com/platform': 'peat' }
    k.put({ apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: 'storage-broker', namespace: 'peat', labels }, spec: { replicas: 1, template: { spec: { containers: [{ image: 'someone-else' }] } } } })
    k.put({ apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'peat-ca', namespace: 'shop', labels }, data: { 'ca.crt': 'a copy' } })
    k.put({ apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'stale', namespace: 'peat', labels }, data: {} })
    k.put(platform())
    const r = await reconcile(k)
    expect(k.get('apps/v1', 'deployments', 'peat', 'storage-broker').spec.template.spec.containers[0].image).toBe('someone-else')
    expect(r.refused[0]).toContain('Deployment/peat/storage-broker exists and was not made by the operator')
    expect(k.get('v1', 'configmaps', 'shop', 'peat-ca').data['ca.crt']).toBe('a copy')
    expect(k.get('v1', 'configmaps', 'peat', 'stale')).toBeDefined()
    expect(r.pruned).toBe(0)
  })
  const record = () => k.get('v1', 'configmaps', 'peat-system', 'peat-owned').data as Record<string, string>
  test('the record, in the operator\'s own namespace, names each object made by its UID', async () => {
    k.put(platform())
    await reconcile(k)
    const ctl = k.get('apps/v1', 'deployments', 'peat', 'storage-controller')
    expect(record()['Deployment.peat.storage-controller']).toBe(`uid:${ctl.metadata.uid}`)
    expect(Object.keys(record()).length).toBeGreaterThan(30)
    expect(k.get('v1', 'configmaps', 'peat', 'peat-owned')).toBeUndefined()
  })
  test('a pass that fails half way loses nothing it made: the next owns all of it', async () => {
    k.put(platform())
    k.forcedOnce.set('PATCH /apis/apps/v1/namespaces/peat-gateway/deployments/peat-gateway', { status: 500, json: { message: 'injected' } })
    await expect(reconcile(k)).rejects.toThrow('injected')
    const r = await reconcile(k)
    expect(r.refused).toEqual([])
    expect(k.get('apps/v1', 'deployments', 'peat-gateway', 'peat-gateway')).toBeDefined()
  })
  test('a pass that dies between the create and the record: the object is found by its nonce and recorded', async () => {
    k.put(platform())
    await reconcile(k)
    const cm = k.get('v1', 'configmaps', 'peat-system', 'peat-owned')
    const live = k.get('apps/v1', 'deployments', 'peat', 'storage-broker')
    cm.data['Deployment.peat.storage-broker'] = 'nonce:abc123'
    live.metadata.annotations = { ...live.metadata.annotations, 'peat.zabaca.com/owner-nonce': 'abc123' }
    const r = await reconcile(k)
    expect(r.refused).toEqual([])
    expect(record()['Deployment.peat.storage-broker']).toBe(`uid:${live.metadata.uid}`)
  })
  test('a record that cannot be read stops the pass and is never overwritten', async () => {
    k.put(platform())
    await reconcile(k)
    const before = { ...record() }
    k.forced.set('GET /api/v1/namespaces/peat-system/configmaps/peat-owned', { status: 500, json: { message: 'down' } })
    await expect(reconcile(k)).rejects.toThrow('read the record')
    k.forced.clear()
    k.get('v1', 'configmaps', 'peat-system', 'peat-owned').data['Job.peat.x'] = 'not a value'
    await expect(reconcile(k)).rejects.toThrow('cannot read')
    delete k.get('v1', 'configmaps', 'peat-system', 'peat-owned').data['Job.peat.x']
    expect(record()).toEqual(before)
  })
  test('cleanup deletes only the very object recorded (a UID precondition), and a cluster-scoped one only with the operator\'s labels', async () => {
    k.put(platform({ safekeepers: { outside: { machines: ['outside-1'] } } }))
    await reconcile(k)
    // the Job made again by someone else under the same name: the record's UID is not its UID
    const job = k.get('batch/v1', 'jobs', 'peat', 'keeper-prep')
    job.metadata.uid = 'someone-elses'
    // and a cluster-scoped object the record names, no longer rendered, without the operator's labels
    const cr = k.put({ apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRole', metadata: { name: 'leftover' }, rules: [] })
    k.get('v1', 'configmaps', 'peat-system', 'peat-owned').data['ClusterRole..leftover'] = `uid:${cr.metadata.uid}`
    k.put(platform())
    const r = await reconcile(k)
    expect(k.get('batch/v1', 'jobs', 'peat', 'keeper-prep')).toBeDefined()
    expect(k.get('rbac.authorization.k8s.io/v1', 'clusterroles', null, 'leftover')).toBeDefined()
    expect(r.pruned).toBe(0)
    // with the operator's labels it is the operator's to remove
    k.get('rbac.authorization.k8s.io/v1', 'clusterroles', null, 'leftover').metadata.labels = { [MANAGED_BY]: 'peat-operator', 'peat.zabaca.com/platform': 'peat' }
    expect((await reconcile(k)).pruned).toBe(1)
    expect(k.get('rbac.authorization.k8s.io/v1', 'clusterroles', null, 'leftover')).toBeUndefined()
  })
  test('the API server refuses a delete whose UID does not match (the fake honours the precondition)', async () => {
    k.put({ apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'x', namespace: 'peat' }, data: {} })
    expect((await k.request('DELETE', '/api/v1/namespaces/peat/configmaps/x', { preconditions: { uid: 'other' } })).status).toBe(409)
    expect(k.get('v1', 'configmaps', 'peat', 'x')).toBeDefined()
  })
  test('an object someone else makes between the read and the apply is not recorded; an answer without a UID records nothing', async () => {
    k.put(platform())
    const path = '/apis/apps/v1/namespaces/peat/deployments/storage-broker'
    // the read finds nothing; by the apply someone has made it (the apply answers 200, not 201)
    k.forcedOnce.set(`GET ${path}`, { status: 404, json: { message: 'not found' } })
    k.put({ apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: 'storage-broker', namespace: 'peat' }, spec: { template: { spec: { containers: [{ image: 'someone-else' }] } } } })
    const r = await reconcile(k)
    expect(r.refused).toContain('Deployment/peat/storage-broker was made by someone else as the operator made it: applied over once, left out of the record')
    expect(record()['Deployment.peat.storage-broker']).toBeUndefined()
    const control = 'PATCH /apis/apps/v1/namespaces/peat/deployments/peat-control'
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|deployments|peat-control'))!)
    delete k.get('v1', 'configmaps', 'peat-system', 'peat-owned').data['Deployment.peat.peat-control']
    k.forcedOnce.set(control, { status: 201, json: {} })
    await reconcile(k)
    expect(record()['Deployment.peat.peat-control']).toMatch(/^nonce:/)
  })
  test('a record made new while the platform\'s objects exist says so: lost, with no way back in this release but by hand; a record another pass made first is read, not taken as empty', async () => {
    k.put(platform())
    await reconcile(k)
    const before = { ...record() }
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|configmaps|peat-owned'))!)
    const r = await reconcile(k)
    expect(r.refused[0]).toContain('the record is lost, and this release has no way to take its objects back but by hand')
    k.put({ apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'peat-owned', namespace: 'peat-system' }, data: before })
    k.forcedOnce.set('GET /api/v1/namespaces/peat-system/configmaps/peat-owned', { status: 404, json: { message: 'not found' } })
    const again = await reconcile(k)
    expect(again.refused).toEqual([])
  })
  test('none of the platform\'s namespaces may be the operator\'s own', async () => {
    k.put(platform({ namespaces: { platform: 'peat-system' } }))
    expect((await reconcile(k)).message).toContain("peat-system is the operator's own namespace")
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
  test('a release changed before the platform was ever Ready still goes through the ordered upgrade: the pieces that run are where it starts', async () => {
    k.put(platform())
    await reconcile(k)
    // not Ready (nothing rolled out in this API server), so no release was recorded; the live pieces say what runs
    expect(k.get('peat.zabaca.com/v1alpha1', 'platforms', null, 'peat').status.release).toBeUndefined()
    k.put(platform({ release: 'release-2026-12-15' }))
    const r = await reconcile(k)
    expect(r.phase).toBe('Upgrading')
    expect(k.get('apps/v1', 'deployments', 'peat', 'peat-control').spec.template.spec.containers[0].image).toBe('ghcr.io/zabaca/peat/control:release-2026-12-01')
  })
})

describe('a move\'s exec, granted per move', () => {
  const compute = (name: string, db: string, more: Record<string, string> = {}) => ({ apiVersion: 'v1', kind: 'Pod', metadata: { name, namespace: 'peat-compute', labels: { app: 'peat-compute', compute: db, ...more } } })
  const moving = (state: string, pods: string[] | null) => ({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Database', metadata: { name: 'orders', namespace: 'shop' }, spec: {},
    status: { tenant: 'a'.repeat(32), timeline: 'b'.repeat(32), pgVersion: 18, phase: 'Moving', record: { mode: 'primary', root: true, parent: '', port: 15010 }, move: { from: 18, to: 19, state, target: 'mv-0123456789ab', oldDatabase: 'orders-pg18', pods } } })
  test('a Role of the move\'s own pods while it execs, another database\'s pod left out; taken back after, by a pass that stops early too', async () => {
    k.put(platform())
    await reconcile(k)
    for (const p of [compute('src-1', 'orders'), compute('tgt-1', 'mv-0123456789ab', { 'peat-move': 'target' }), compute('other-1', 'shop')]) k.put(p)
    k.put(moving('copying', ['src-1', 'tgt-1', 'other-1']))
    await reconcile(k)
    const role = k.get('rbac.authorization.k8s.io/v1', 'roles', 'peat-compute', 'peat-move-orders')
    expect(role.rules).toEqual([{ apiGroups: [''], resources: ['pods/exec'], resourceNames: ['src-1', 'tgt-1'], verbs: ['create', 'get'] }])
    expect(k.get('rbac.authorization.k8s.io/v1', 'rolebindings', 'peat-compute', 'peat-move-orders').subjects).toEqual([{ kind: 'ServiceAccount', name: 'peat-control', namespace: 'peat' }])
    // the move failed (its source still runs under the same name) and the next pass stops early: the grant goes all the same
    k.put({ ...moving('failed', ['src-1']), status: { ...moving('failed', ['src-1']).status, phase: 'Ready' } })
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|machine-3'))!)
    expect((await reconcile(k)).phase).toBe('Failed')
    expect(k.get('rbac.authorization.k8s.io/v1', 'roles', 'peat-compute', 'peat-move-orders')).toBeUndefined()
    expect(k.get('rbac.authorization.k8s.io/v1', 'rolebindings', 'peat-compute', 'peat-move-orders')).toBeUndefined()
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
    // outside the platform's namespaces the operator deletes nothing: each copy is owned by its Database, which Kubernetes removes it with
    const dev = k.get('peat.zabaca.com/v1alpha1', 'databases', 'shop', 'orders-dev')
    expect(k.get('v1', 'secrets', 'shop', 'orders-dev-login').metadata.ownerReferences).toEqual([{ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Database', name: 'orders-dev', uid: dev.metadata.uid }])
    expect(k.get('v1', 'configmaps', 'shop', 'peat-ca').metadata.ownerReferences.map((o: any) => o.name).sort()).toEqual(['orders', 'orders-dev'])
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|databases|orders-dev'))!)
    const r = await reconcile(k)
    expect(r.pruned).toBe(0)
    expect(k.get('v1', 'secrets', 'shop', 'orders-dev-login')).toBeDefined() // the API server's garbage collector removes it, not the operator
  })
  test('a failed list of the Databases stops the pass, and a login not there yet leaves its copy as it is: neither prunes a published copy', async () => {
    k.put(platform())
    await reconcile(k)
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'db-orders-login', namespace: 'peat' }, data: { password: Buffer.from('pw').toString('base64') } })
    const rec = { tenant: 'a'.repeat(32), timeline: 'b'.repeat(32), pgVersion: 19, phase: 'Ready', record: { mode: 'primary', root: true, parent: '', port: 15010 } }
    k.put({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Database', metadata: { name: 'orders', namespace: 'peat-db' }, spec: {}, status: rec })
    await reconcile(k)
    expect(k.get('v1', 'secrets', 'peat-db', 'orders-login')).toBeDefined()
    k.forcedOnce.set('GET /apis/peat.zabaca.com/v1alpha1/databases', { status: 503, json: { message: 'unavailable' } })
    await expect(reconcile(k)).rejects.toThrow('list the Databases: 503')
    expect(k.get('v1', 'secrets', 'peat-db', 'orders-login')).toBeDefined()
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|secrets|db-orders-login'))!)
    const r = await reconcile(k)
    expect(r.pruned).toBe(0)
    expect(k.get('v1', 'secrets', 'peat-db', 'orders-login')).toBeDefined()
  })
  test('a database whose status is wiped for a pass keeps its published login the operator\'s', async () => {
    k.put(platform())
    await reconcile(k)
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'db-orders-login', namespace: 'peat' }, data: { password: Buffer.from('pw').toString('base64') } })
    const rec = { tenant: 'a'.repeat(32), timeline: 'b'.repeat(32), pgVersion: 19, phase: 'Ready', record: { mode: 'primary', root: true, parent: '', port: 15010 } }
    k.put({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Database', metadata: { name: 'orders', namespace: 'shop' }, spec: {}, status: rec })
    await reconcile(k)
    k.get('peat.zabaca.com/v1alpha1', 'databases', 'shop', 'orders').status = {}
    await reconcile(k)
    k.get('peat.zabaca.com/v1alpha1', 'databases', 'shop', 'orders').status = rec
    const r = await reconcile(k)
    expect(r.refused).toEqual([])
  })
  test('a person\'s own Secret of the login\'s name is left alone, and the status does not name it as the login', async () => {
    k.put(platform())
    await reconcile(k)
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'db-orders-login', namespace: 'peat' }, data: { password: Buffer.from('pw').toString('base64') } })
    k.put({ apiVersion: 'v1', kind: 'Secret', metadata: { name: 'orders-login', namespace: 'shop' }, data: { mine: Buffer.from('x').toString('base64') } })
    k.put({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Database', metadata: { name: 'orders', namespace: 'shop' }, spec: {}, status: { tenant: 'a'.repeat(32), timeline: 'b'.repeat(32), pgVersion: 19, phase: 'Ready', record: { mode: 'primary', root: true, parent: '', port: 15010 } } })
    const r = await reconcile(k)
    expect(r.refused.some((x) => x.startsWith('Secret/shop/orders-login '))).toBe(true)
    expect(decode(k.get('v1', 'secrets', 'shop', 'orders-login'), 'mine')).toBe('x')
    expect(k.get('peat.zabaca.com/v1alpha1', 'databases', 'shop', 'orders').status.loginSecret).toBeUndefined()
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
  test('a Platform of namespaces the install did not bind the operator\'s rights in is refused with the flag to install with, and nothing is made', async () => {
    k.noSecretsIn.add('shop-db')
    k.put(platform({ namespaces: { databases: 'shop-db' } }))
    const r = await reconcile(k)
    expect(r.phase).toBe('Failed')
    expect(statusOf()).toMatchObject({ phase: 'Failed', message: "the install's --namespaces do not cover shop-db: install with --namespaces peat,peat-compute,peat-gateway,shop-db" })
    expect(k.all('statefulsets')).toEqual([])
    expect(k.all('roles')).toEqual([])
    // the same Platform once the install covers it
    k.noSecretsIn.delete('shop-db')
    expect((await reconcile(k)).phase).toBe('Installing')
  })
  test('the operator holds every right it grants the control layer (the API server refuses to grant one it does not hold)', async () => {
    const m = installManifests('img') as any[]
    const clusterRole = (name: string) => m.find((x) => x.kind === 'ClusterRole' && x.metadata.name === name)
    // what it holds everywhere (its ClusterRole, bound cluster-wide), and what it holds in a namespace besides (ClusterRoles bound there)
    const everywhere = m.find((x) => x.kind === 'ClusterRole' && x.metadata.name === 'peat-operator').rules as any[]
    const inNamespace = (ns: string) => m.filter((x) => x.kind === 'RoleBinding' && x.metadata.namespace === ns).flatMap((b) => clusterRole(b.roleRef.name).rules as any[])
    const unnamed = (r: any) => !r.resourceNames
    const holds = (rules: any[], group: string, resource: string, verb: string) => rules.filter(unnamed).some((r: any) => r.apiGroups.includes(group) && (r.resources.includes(resource) || r.resources.includes('*')) && (r.verbs.includes(verb) || r.verbs.includes('*')))
    k.put(platform())
    await reconcile(k)
    const missing: string[] = []
    // a Role's rules are held in its namespace; a ClusterRole's, everywhere
    for (const x of [...k.all('roles'), ...k.all('clusterroles')].filter((y) => !String(y.metadata.name).startsWith('peat-operator'))) {
      const rules = x.kind === 'Role' ? [...everywhere, ...inNamespace(x.metadata.namespace)] : everywhere
      for (const r of (x.rules ?? []) as any[]) for (const g of r.apiGroups) for (const res of r.resources) for (const v of r.verbs) if (!holds(rules, g, res, v)) missing.push(`${x.kind} ${x.metadata.namespace ?? ''}/${x.metadata.name}: ${g}/${res} ${v}`)
    }
    expect(missing).toEqual([])
  })
  test('the kinds, the operator\'s namespace, its account and its Deployment of the image given', () => {
    const m = installManifests('ghcr.io/zabaca/peat/control:release-2026-12-01') as any[]
    expect(m.filter((x) => x.kind === 'CustomResourceDefinition').map((x) => x.metadata.name)).toContain('platforms.peat.zabaca.com')
    const dep = m.find((x) => x.kind === 'Deployment')
    expect(dep.metadata.namespace).toBe('peat-system')
    expect(dep.spec.template.spec.containers[0]).toMatchObject({ image: 'ghcr.io/zabaca/peat/control:release-2026-12-01', args: ['operator'] })
    const role = m.find((x) => x.kind === 'ClusterRole')
    // it grants nothing it does not hold: no bind, no escalate; and it makes no pod
    for (const r of role.rules as any[]) { expect(r.verbs).not.toContain('bind'); expect(r.verbs).not.toContain('escalate') }
    expect(role.rules.find((r: any) => r.resources.includes('pods')).verbs).not.toContain('create')
    // narrowed: no namespace is deleted; Secrets anywhere only read, made and patched; exec held nowhere cluster-wide; policies only its own, by name
    expect(role.rules.find((r: any) => r.resources.includes('namespaces')).verbs).not.toContain('delete')
    expect(role.rules.find((r: any) => r.resources.includes('secrets')).verbs).toEqual(['get', 'create', 'patch'])
    expect(JSON.stringify(role.rules)).not.toContain('pods/exec')
    const policies = (role.rules as any[]).filter((r) => r.resources.includes('validatingadmissionpolicies'))
    expect(policies).toEqual([
      { apiGroups: ['admissionregistration.k8s.io'], resources: ['validatingadmissionpolicies', 'validatingadmissionpolicybindings'], verbs: ['create'] },
      { apiGroups: ['admissionregistration.k8s.io'], resources: ['validatingadmissionpolicies', 'validatingadmissionpolicybindings'], resourceNames: OPERATOR_POLICIES, verbs: ['get', 'update', 'patch', 'delete'] },
    ])
    // every right on Secrets in the platform's four namespaces, exec in the compute namespace only, by RoleBindings; the namespaces made first
    expect(m.filter((x) => x.kind === 'RoleBinding' && x.roleRef.name === SECRETS_ROLE).map((x) => x.metadata.namespace).sort()).toEqual(['peat', 'peat-compute', 'peat-db', 'peat-gateway'])
    expect(m.filter((x) => x.kind === 'RoleBinding' && x.roleRef.name === EXEC_ROLE).map((x) => x.metadata.namespace)).toEqual(['peat-compute'])
    for (const n of ['peat', 'peat-compute', 'peat-db', 'peat-gateway']) expect(m.some((x) => x.kind === 'Namespace' && x.metadata.name === n)).toBe(true)
    // a Platform that names its own namespaces: the install names them too
    const own = installManifests('img', { platform: 'db', computes: 'db-run', gateways: 'db-in', databases: 'db-names' }) as any[]
    expect(own.filter((x) => x.kind === 'RoleBinding' && x.roleRef.name === EXEC_ROLE).map((x) => x.metadata.namespace)).toEqual(['db-run'])
    // the exec it holds to grant is refused it by an admission rule of its own, whatever its RBAC
    const noExec = m.find((x) => x.kind === 'ValidatingAdmissionPolicy' && x.metadata.name === OPERATOR_EXEC_POLICY)
    expect(noExec.spec.matchConstraints.resourceRules[0]).toMatchObject({ operations: ['CONNECT'], resources: ['pods/exec', 'pods/attach'] })
    expect(noExec.spec.matchConditions).toEqual([{ name: 'by-the-operator', expression: "request.userInfo.username == 'system:serviceaccount:peat-system:peat-operator'" }])
    expect(noExec.spec.validations[0].expression).toBe('false')
    expect(m.find((x) => x.kind === 'ValidatingAdmissionPolicyBinding' && x.metadata.name === OPERATOR_EXEC_POLICY).spec).toEqual({ policyName: OPERATOR_EXEC_POLICY, validationActions: ['Deny'] })
  })
})

describe('the release\'s image map: every piece by digest', () => {
  const D = (c: string) => `sha256:${c.repeat(64)}`
  const R = 'ghcr.io/zabaca/peat'
  const MAP: ReleaseImages = {
    release: 'release-2026-12-01', registry: R,
    storage: `${R}/storage:release-2026-12-01@${D('a')}`, control: `${R}/control:release-2026-12-01@${D('b')}`,
    compute: { '18': `${R}/compute-pg18:release-2026-12-01@${D('c')}`, '19': `${R}/compute-pg19:release-2026-12-01@${D('d')}` },
  }
  const ready = () => {
    for (const o of [...k.all('deployments'), ...k.all('statefulsets')]) o.status = { readyReplicas: o.spec.replicas, updatedReplicas: o.spec.replicas }
    for (const [i] of ['machine-1', 'machine-2', 'machine-3'].entries()) {
      const sk = k.get('apps/v1', 'statefulsets', 'peat', `safekeeper-w${i + 1}`)
      if (!sk) k.put({ apiVersion: 'apps/v1', kind: 'StatefulSet', metadata: { name: `safekeeper-w${i + 1}`, namespace: 'peat', labels: { 'peat-sk': 'true' } }, spec: { replicas: 1, template: { spec: { containers: [{ name: 'safekeeper', image: env(k.get('apps/v1', 'deployments', 'peat', 'peat-control'))['STORAGE_IMAGE'] }] } } }, status: { readyReplicas: 1, updatedReplicas: 1 } })
    }
  }

  test('a map is read only when every image it names is a reference by digest', () => {
    expect(parseReleaseImages(JSON.stringify(MAP))).toEqual(MAP)
    expect(parseReleaseImages(JSON.stringify({ release: 'ci', registry: `${R}/`, storage: MAP.storage }))).toEqual({ release: 'ci', registry: R, storage: MAP.storage })
    expect(() => parseReleaseImages(JSON.stringify({ ...MAP, storage: `${R}/storage:release-2026-12-01` }))).toThrow('not a reference by digest')
    expect(() => parseReleaseImages(JSON.stringify({ ...MAP, compute: { '18': `${R}/compute-pg18@sha256:abc` } }))).toThrow('compute 18 image is not a reference by digest')
    expect(() => parseReleaseImages(JSON.stringify({ ...MAP, compute: { '17': MAP.compute!['18'] } }))).toThrow('versions other than 18 and 19')
    expect(() => parseReleaseImages(JSON.stringify({ ...MAP, release: '' }))).toThrow('names no release')
    expect(() => parseReleaseImages(JSON.stringify({ ...MAP, images: {} }))).toThrow('fields it should not: images')
    expect(() => parseReleaseImages('{')).toThrow('not JSON')
    expect(() => parseReleaseImages('[]')).toThrow('not an object')
  })

  test('the map names the images of its release on its registry; what the Platform pins comes first; another release or registry keeps its tags', () => {
    const spec = (extra: Record<string, unknown> = {}) => { const s = specOf(platform(extra) as any); if ('errors' in s) throw new Error(s.errors.join('; ')); return s.spec }
    expect(imagesOf(spec(), 'release-2026-12-01', MAP)).toEqual({ release: 'release-2026-12-01', storage: MAP.storage!, control: MAP.control!, compute: { 18: MAP.compute!['18']!, 19: MAP.compute!['19']! } })
    const pinned = imagesOf(spec({ images: { control: 'example.com/own/control:mine' } }), 'release-2026-12-01', MAP)
    expect(pinned.control).toBe('example.com/own/control:mine')
    expect(pinned.storage).toBe(MAP.storage!)
    expect(imagesOf(spec({ release: 'release-2026-12-15' }), 'release-2026-12-15', MAP).storage).toBe(`${R}/storage:release-2026-12-15`)
    expect(imagesOf(spec({ images: { registry: 'mirror.example.com/peat' } }), 'release-2026-12-01', MAP).storage).toBe('mirror.example.com/peat/storage:release-2026-12-01')
    // a map that names fewer images: the rest are the registry's tags
    expect(imagesOf(spec(), 'release-2026-12-01', { release: 'release-2026-12-01', registry: R, storage: MAP.storage }).control).toBe(`${R}/control:release-2026-12-01`)
  })

  test('an install of the map\'s release runs every piece by digest, and records the digests as what it runs', async () => {
    k.put(platform())
    await reconcile(k, { releaseImages: MAP })
    expect(k.get('apps/v1', 'deployments', 'peat', 'storage-controller').spec.template.spec.containers[0].image).toBe(MAP.storage)
    expect(k.get('apps/v1', 'statefulsets', 'peat', 'pageserver-w1').spec.template.spec.containers.map((c: any) => c.image)).toEqual([MAP.storage])
    const control = k.get('apps/v1', 'deployments', 'peat', 'peat-control')
    expect(control.spec.template.spec.containers[0].image).toBe(MAP.control)
    expect(env(control)).toMatchObject({ STORAGE_IMAGE: MAP.storage, COMPUTE_IMAGE: MAP.compute!['18'], COMPUTE_IMAGE_PG19: MAP.compute!['19'] })
    expect(k.get('apps/v1', 'deployments', 'peat-gateway', 'peat-gateway').spec.template.spec.containers[0].image).toBe(MAP.control)
    // the guards admit the digests: the control layer makes the computes and the safekeepers of exactly these images
    const guards = JSON.stringify(k.all('validatingadmissionpolicies'))
    for (const i of [MAP.storage, MAP.compute!['18'], MAP.compute!['19']]) expect(guards).toContain(`'${i}'`)
    ready()
    await reconcile(k, { releaseImages: MAP })
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-2026-12-01', images: { storage: MAP.storage, control: MAP.control } })
  })

  // the three orders a release's install file and its release can land in: no piece is restarted twice for one release's tags and digests
  test('a platform on the release by tags, handed the same release\'s digests, stays on its tags: no upgrade, no piece restarted', async () => {
    k.put(platform())
    await reconcile(k)
    ready()
    await reconcile(k)
    const tag = `${R}/storage:release-2026-12-01`
    expect(statusOf()).toMatchObject({ phase: 'Ready', images: { storage: tag } })
    const before = JSON.stringify([...k.all('deployments'), ...k.all('statefulsets')].map((o) => o.spec.template))
    const r = await reconcile(k, { releaseImages: MAP })
    expect(r.phase).toBe('Ready')
    expect(statusOf().upgrade).toBeUndefined()
    expect(JSON.stringify([...k.all('deployments'), ...k.all('statefulsets')].map((o) => o.spec.template))).toBe(before)
    // the control layer may still make a safekeeper of the image the platform runs (and of the digest, for the next upgrade)
    const sts = k.all('validatingadmissionpolicies').find((p) => p.metadata.name === 'peat-control-statefulsets-keep-to-the-safekeeper')
    expect(JSON.stringify(sts)).toContain(`'${tag}'`)
  })
  test('the install file first, then the release: one upgrade, to the digests', async () => {
    k.put(platform({ release: 'release-2026-11-15' }))
    await reconcile(k)
    ready()
    await reconcile(k)
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-2026-11-15' })
    expect((await reconcile(k, { releaseImages: MAP })).phase).toBe('Ready')
    expect(statusOf().message).toContain("the operator's image map is of release release-2026-12-01")
    k.put(platform())
    const r = await reconcile(k, { releaseImages: MAP })
    expect(r.phase).toBe('Upgrading')
    expect(statusOf().upgrade).toMatchObject({ from: 'release-2026-11-15', to: 'release-2026-12-01', direction: 'forward', toImages: { storage: MAP.storage } })
  })
  test('the install file landing mid-upgrade to the same release by tags: the upgrade goes on forward on its tags, never back and again', async () => {
    k.put(platform({ release: 'release-2026-11-15' }))
    await reconcile(k)
    ready()
    await reconcile(k)
    k.put(platform())
    expect((await reconcile(k)).phase).toBe('Upgrading')
    const tags = statusOf().upgrade.toImages
    expect(tags.storage).toBe(`${R}/storage:release-2026-12-01`)
    await reconcile(k, { releaseImages: MAP })
    expect(statusOf().upgrade).toMatchObject({ to: 'release-2026-12-01', direction: 'forward', toImages: { storage: tags.storage } })
    expect(statusOf().upgrade.halted).toBeUndefined()
  })

  test('the install file made from a map runs its control image and hands the operator the map; --image names another', () => {
    const dep = (m: any[]) => m.find((x) => x.kind === 'Deployment')
    const withMap = dep(installManifests(MAP.control!, undefined, MAP) as any[])
    expect(withMap.spec.template.spec.containers[0].image).toBe(MAP.control)
    expect(parseReleaseImages(env(withMap)[RELEASE_IMAGES_ENV] as string)).toEqual(MAP)
    expect(env(dep(installManifests('img') as any[]))[RELEASE_IMAGES_ENV]).toBeUndefined()
  })
})
