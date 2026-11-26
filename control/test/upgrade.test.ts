import { beforeEach, describe, expect, test } from 'bun:test'
import { reconcile, differences } from '../src/operator/reconcile'
import type { Storage } from '../src/operator/storage'
import { decide, imagesFor, nextPhase, stepsOf, type UpgradeState } from '../src/operator/upgrade'
import { FakeKube } from './fake-kube'

// The ordered upgrade, first as decisions, then as passes of the operator over an API server in memory with a stand-in storage: the order,
// the checks between steps, a halt that waits for a person, a retry, going back over only what moved, quiet hours, and adoption.

const img = (rel: string) => ({ storage: `s:${rel}`, control: `c:${rel}`, compute: { 18: `p18:${rel}`, 19: `p19:${rel}` } })
const NOW = new Date('2026-12-01T12:00:00Z')
const steps = stepsOf([2, 1], ['safekeeper-w3', 'safekeeper-w1', 'safekeeper-w2'])

describe('the decisions', () => {
  test('the order: the controller, each pageserver, each safekeeper, the gateways, the control layer', () => {
    expect(steps).toEqual(['controller', 'pageserver-w1', 'pageserver-w2', 'safekeeper-w1', 'safekeeper-w2', 'safekeeper-w3', 'gateways', 'control'])
    expect(nextPhase('pageserver-w1', 'precheck', 2)).toBe('draining')
    expect(nextPhase('pageserver-w1', 'precheck', 1)).toBe('applying') // one pageserver: nowhere to drain to
    expect(nextPhase('controller', 'postcheck', 2)).toBe('done')
  })
  test('nothing to do on a first install, or when the release is the one running; a changed release starts one', () => {
    expect(decide({ release: 'b', images: img('b'), steps, quietHours: [], now: NOW }).kind).toBe('none')
    expect(decide({ release: 'a', images: img('a'), running: { release: 'a', images: img('a') }, steps, quietHours: [], now: NOW }).kind).toBe('none')
    const d = decide({ release: 'b', images: img('b'), running: { release: 'a', images: img('a') }, steps, quietHours: [], now: NOW })
    expect(d.kind).toBe('continue')
    expect((d as any).state).toMatchObject({ from: 'a', to: 'b', direction: 'forward', step: 'controller', phase: 'precheck', moved: [] })
  })
  const mid: UpgradeState = { from: 'a', to: 'b', fromImages: img('a'), toImages: img('b'), direction: 'forward', steps, moved: ['controller'], step: 'pageserver-w1', phase: 'restarting', since: NOW.toISOString(), startedAt: NOW.toISOString() }
  test('mid-upgrade, each piece is rendered at the release its step has reached', () => {
    const at = imagesFor(mid, img('b'))
    expect(at('controller').storage).toBe('s:b')
    expect(at('pageserver-w1').storage).toBe('s:b') // applied, restarting
    expect(at('pageserver-w2').storage).toBe('s:a')
    expect(at('control').control).toBe('c:a')
    expect(imagesFor({ ...mid, phase: 'draining' }, img('b'))('pageserver-w1').storage).toBe('s:a') // not applied yet
  })
  test('a halt waits for a person: it stays halted until a new retry is asked, then goes on', () => {
    const halted = { ...mid, halted: { at: NOW.toISOString(), step: 'pageserver-w1', reason: 'x' } }
    expect(decide({ release: 'b', images: img('b'), state: halted, steps, quietHours: [], now: NOW }).kind).toBe('halt')
    const retried = decide({ release: 'b', images: img('b'), state: halted, steps, retry: 'r1', quietHours: [], now: NOW })
    expect(retried.kind).toBe('continue')
    expect((retried as any).state).toMatchObject({ halted: undefined, retried: 'r1', step: 'pageserver-w1' })
    expect(decide({ release: 'b', images: img('b'), state: { ...halted, retried: 'r1' }, steps, retry: 'r1', quietHours: [], now: NOW }).kind).toBe('halt')
  })
  test('the earlier release set back: only what moved goes back, the step under way included once applied, in reverse order', () => {
    const d = decide({ release: 'a', images: img('a'), state: mid, steps, quietHours: [], now: NOW }) as any
    expect(d.kind).toBe('continue')
    expect(d.state).toMatchObject({ direction: 'back', moved: ['controller', 'pageserver-w1'], step: 'pageserver-w1', phase: 'precheck' })
    const notApplied = decide({ release: 'a', images: img('a'), state: { ...mid, phase: 'draining' }, steps, quietHours: [], now: NOW }) as any
    expect(notApplied.state).toMatchObject({ direction: 'back', moved: ['controller'], step: 'controller' })
    const back = decide({ release: 'a', images: img('a'), state: { ...mid, direction: 'back', moved: [], step: undefined, phase: undefined }, steps, quietHours: [], now: NOW }) as any
    expect(back).toMatchObject({ kind: 'finish', release: 'a' })
  })
  test('a third release halts it, and every step done finishes it', () => {
    expect(decide({ release: 'c', images: img('c'), state: mid, steps, quietHours: [], now: NOW }).kind).toBe('halt')
    expect(decide({ release: 'b', images: img('b'), state: { ...mid, moved: steps, step: undefined, phase: undefined }, steps, quietHours: [], now: NOW })).toMatchObject({ kind: 'finish', release: 'b' })
  })
  test('quiet hours pause it at a step boundary, never inside a step', () => {
    const q = [{ start: '11:30', end: '12:30' }]
    expect(decide({ release: 'b', images: img('b'), state: { ...mid, step: undefined, phase: undefined }, steps, quietHours: q, now: NOW }).kind).toBe('pause')
    expect(decide({ release: 'b', images: img('b'), state: mid, steps, quietHours: q, now: NOW }).kind).toBe('continue')
    expect(decide({ release: 'b', images: img('b'), running: { release: 'a', images: img('a') }, steps, quietHours: q, now: NOW }).kind).toBe('pause')
  })
  test('adoption compares only what the operator sets: defaults the API server filled are no difference', () => {
    const want = { spec: { replicas: 1, template: { spec: { containers: [{ name: 'x', image: 'i:2' }] } } }, metadata: { name: 'a', labels: { mine: 'yes' } } }
    const live = { spec: { replicas: 1, revisionHistoryLimit: 10, template: { spec: { containers: [{ name: 'x', image: 'i:1', imagePullPolicy: 'IfNotPresent' }] } } }, metadata: { name: 'a' } }
    expect(differences(want, live)).toEqual(['.spec.template.spec.containers[0].image: "i:1" would be "i:2"'])
    expect(differences({ ...want, spec: { ...want.spec, template: { spec: { containers: [{ name: 'x', image: 'i:1' }] } } } }, live)).toEqual([])
  })
})

// ---- whole passes

const LABEL = { 'peat.zabaca.com/machine': 'true' }
let k: FakeKube
let clock: Date
let unsettled: string[]
const calls: string[] = []
const storage: Storage = {
  problems: async () => [...unsettled],
  drain: async (id) => { calls.push(`drain ${id}`) },
  drained: async () => ({ done: true, why: '' }),
  fill: async (id) => { calls.push(`fill ${id}`) },
  filled: async () => ({ done: true, why: '' }),
}
const platform = (release: string, more: Record<string, unknown> = {}) => ({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Platform', metadata: { name: 'peat' }, spec: { release, objectStore: { endpoint: 'https://s3.example.com', bucket: 'b', credentialsSecret: 's' }, upgrades: { checkSeconds: 60 }, ...more } })
const statusOf = () => k.get('peat.zabaca.com/v1alpha1', 'platforms', null, 'peat').status
/** A change to the Platform's spec, as kubectl apply makes one: the status stays. */
const respec = (release: string, more: Record<string, unknown> = {}) => { const o = k.get('peat.zabaca.com/v1alpha1', 'platforms', null, 'peat'); o.spec = platform(release, more).spec; o.metadata.generation = (o.metadata.generation ?? 1) + 1 }
const ready = () => {
  // the cluster's controllers, played here: every workload rolled out on its template, every pod made again from its template
  for (const o of [...k.all('deployments'), ...k.all('statefulsets')]) o.status = { readyReplicas: o.spec.replicas ?? 1, updatedReplicas: o.spec.replicas ?? 1, replicas: o.spec.replicas ?? 1, observedGeneration: o.metadata.generation ?? 1 }
  for (const s of k.all('statefulsets')) if (!k.get('v1', 'pods', 'peat', `${s.metadata.name}-0`)) k.put({ apiVersion: 'v1', kind: 'Pod', metadata: { name: `${s.metadata.name}-0`, namespace: 'peat' }, spec: { containers: [{ image: s.spec.template.spec.containers[0].image }] }, status: { conditions: [{ type: 'Ready', status: 'True' }] } })
}
const pass = async () => { const r = await reconcile(k, { storage: () => storage, now: () => clock }); ready(); clock = new Date(clock.getTime() + 10_000); return r }
const image = (kind: string, ns: string, name: string) => k.get('apps/v1', kind, ns, name).spec.template.spec.containers[0].image as string

beforeEach(async () => {
  k = new FakeKube()
  for (const n of ['machine-1', 'machine-2', 'machine-3']) k.put({ apiVersion: 'v1', kind: 'Node', metadata: { name: n, labels: LABEL } })
  clock = new Date('2026-12-01T12:00:00Z')
  unsettled = []
  calls.length = 0
  k.put(platform('release-a'))
  await pass()
  for (const [i, m] of ['machine-1', 'machine-2', 'machine-3'].entries()) k.put({ apiVersion: 'apps/v1', kind: 'StatefulSet', metadata: { name: `safekeeper-w${i + 1}`, namespace: 'peat', labels: { 'peat-sk': 'true' } }, spec: { replicas: 1, template: { spec: { nodeSelector: { 'kubernetes.io/hostname': m }, containers: [{ image: 'ghcr.io/zabaca/peat/storage:release-a' }] } } } })
  ready()
  await pass()
  expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-a' })
})

describe('an upgrade, pass by pass', () => {
  test('one changed line moves every piece in order, each checked before and after, a pageserver drained and filled; then Ready on the new release', async () => {
    respec('release-b')
    const order: string[] = []
    for (let i = 0; i < 80 && (i === 0 || statusOf().phase !== 'Ready'); i++) {
      await pass()
      const u = statusOf().upgrade
      if (u?.step && order.at(-1) !== u.step) order.push(u.step)
    }
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-b' })
    expect(order).toEqual(['controller', 'pageserver-w1', 'pageserver-w2', 'safekeeper-w1', 'safekeeper-w2', 'safekeeper-w3', 'gateways', 'control'])
    expect(calls).toEqual(['drain 1', 'fill 1', 'drain 2', 'fill 2'])
    expect(image('deployments', 'peat', 'storage-controller')).toBe('ghcr.io/zabaca/peat/storage:release-b')
    expect(image('statefulsets', 'peat', 'pageserver-w2')).toBe('ghcr.io/zabaca/peat/storage:release-b')
    expect(image('statefulsets', 'peat', 'safekeeper-w3')).toBe('ghcr.io/zabaca/peat/storage:release-b')
    expect(image('deployments', 'peat-gateway', 'peat-gateway')).toBe('ghcr.io/zabaca/peat/control:release-b')
    expect(image('deployments', 'peat', 'peat-control')).toBe('ghcr.io/zabaca/peat/control:release-b')
  })
  test('mid-upgrade the pieces run two releases, and the control layer\'s account may make a safekeeper of either', async () => {
    respec('release-b')
    for (let i = 0; i < 6; i++) await pass()
    expect(image('deployments', 'peat', 'storage-controller')).toBe('ghcr.io/zabaca/peat/storage:release-b')
    expect(image('deployments', 'peat', 'peat-control')).toBe('ghcr.io/zabaca/peat/control:release-a')
    const guard = JSON.stringify(k.get('admissionregistration.k8s.io/v1', 'validatingadmissionpolicies', null, 'peat-control-statefulsets-keep-to-the-safekeeper'))
    expect(guard).toContain('storage:release-a')
    expect(guard).toContain('storage:release-b')
  })
  test('a failed check halts it at that step with why; it stays halted after the check passes again; a retry goes on', async () => {
    respec('release-b')
    await pass()
    unsettled = ['a secondary is not warm']
    for (let i = 0; i < 10; i++) await pass()
    expect(statusOf().phase).toBe('Halted')
    expect(statusOf().upgrade.halted.reason).toContain('a secondary is not warm')
    unsettled = []
    for (let i = 0; i < 3; i++) await pass()
    expect(statusOf().phase).toBe('Halted')
    respec('release-b', { requests: { retry: 'one' } })
    for (let i = 0; i < 80 && (i === 0 || statusOf().phase !== 'Ready'); i++) await pass()
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-b' })
  })
  test('the earlier release set back moves back only what had moved, and the platform runs it again', async () => {
    respec('release-b')
    for (let i = 0; i < 12; i++) await pass()
    const u0 = statusOf().upgrade
    const movedBefore = [...u0.moved]
    expect(movedBefore.length).toBeGreaterThan(0)
    // what goes back: every step moved, and the one under way if anything of it was applied, last first
    const expectedBack = [...movedBefore, ...(u0.step && !['precheck', 'draining'].includes(u0.phase) ? [u0.step] : [])].reverse()
    expect(image('deployments', 'peat', 'peat-control')).toBe('ghcr.io/zabaca/peat/control:release-a')
    respec('release-a')
    const backOrder: string[] = []
    for (let i = 0; i < 60 && statusOf().release !== 'release-a' || (i < 60 && statusOf().phase !== 'Ready'); i++) {
      await pass()
      const u = statusOf().upgrade
      if (u?.direction === 'back' && u.step && backOrder.at(-1) !== u.step) backOrder.push(u.step)
    }
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-a' })
    expect(backOrder).toEqual(expectedBack)
    expect(image('deployments', 'peat', 'storage-controller')).toBe('ghcr.io/zabaca/peat/storage:release-a')
  })
  test('quiet hours: no step starts inside them; the upgrade waits Paused and goes on after', async () => {
    respec('release-b', { upgrades: { checkSeconds: 60, quietHours: [{ start: '12:00', end: '12:30' }] } })
    await pass()
    expect(statusOf().phase).toBe('Paused')
    expect(image('deployments', 'peat', 'storage-controller')).toBe('ghcr.io/zabaca/peat/storage:release-a')
    clock = new Date('2026-12-01T12:31:00Z')
    for (let i = 0; i < 80 && (i === 0 || statusOf().phase !== 'Ready'); i++) await pass()
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-b' })
  })
})

describe('adoption', () => {
  test('an object of a piece\'s name made another way is taken over only when nothing would change; a difference waits as a pending change until it leaves the list', async () => {
    k = new FakeKube()
    for (const n of ['machine-1', 'machine-2', 'machine-3']) k.put({ apiVersion: 'v1', kind: 'Node', metadata: { name: n, labels: LABEL } })
    k.put({ apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: 'storage-broker', namespace: 'peat' }, spec: { replicas: 1, strategy: { type: 'Recreate' }, selector: { matchLabels: { app: 'storage-broker' } }, template: { metadata: { labels: { app: 'storage-broker' } }, spec: { containers: [{ name: 'storage-broker', image: 'ghcr.io/zabaca/peat/storage:older' }] } } } })
    const adopt = [{ kind: 'Deployment', namespace: 'peat', name: 'storage-broker' }]
    k.put(platform('release-a', { adopt }))
    await pass()
    expect(image('deployments', 'peat', 'storage-broker')).toBe('ghcr.io/zabaca/peat/storage:older')
    const pendingChange = (statusOf().pendingChanges as any[]).find((x) => x.name === 'storage-broker')
    expect(pendingChange.change).toContain('.spec.template.spec.containers[0].image: "ghcr.io/zabaca/peat/storage:older" would be "ghcr.io/zabaca/peat/storage:release-a"')
    expect(k.get('apps/v1', 'deployments', 'peat', 'storage-broker').metadata.labels['peat.zabaca.com/adopted']).toBe('true')
    respec('release-a')
    await pass()
    expect(image('deployments', 'peat', 'storage-broker')).toBe('ghcr.io/zabaca/peat/storage:release-a')
    expect(statusOf().pendingChanges).toEqual([])
  })
})
