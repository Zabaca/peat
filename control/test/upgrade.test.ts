import { afterEach, beforeEach, describe, expect, test } from 'bun:test'
import { reconcile } from '../src/operator/reconcile'
import { differences } from '../src/operator/adoption'
import type { Storage } from '../src/operator/storage'
import { decide, imagesFor, nextPhase, stepsOf, type UpgradeState } from '../src/operator/upgrade'
import { FakeKube } from './fake-kube'

// The ordered upgrade, first as decisions, then as passes of the operator over an API server in memory with a stand-in storage: the order,
// the checks between steps, a halt that waits for a person, a retry, going back over only what moved, quiet hours, and adoption.

const img = (rel: string) => ({ release: rel, storage: `s:${rel}`, control: `c:${rel}`, compute: { 18: `p18:${rel}`, 19: `p19:${rel}` } })
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
/** The storage controller's nodes, played here: a drain asked puts a node to PauseForRestart (done), a fill puts it back to Active. */
const scheduling = new Map<number, string>()
const storage: Storage = {
  problems: async () => [...unsettled],
  node: async (id) => ({ availability: 'Active', scheduling: scheduling.get(id) ?? 'Active' }),
  drain: async (id) => { calls.push(`drain ${id}`); scheduling.set(id, 'PauseForRestart') },
  drained: async (id) => (scheduling.get(id) === 'PauseForRestart' ? { done: true, why: '' } : { done: false, why: `node ${id} scheduling is ${scheduling.get(id) ?? 'Active'}` }),
  fill: async (id) => { calls.push(`fill ${id}`); scheduling.set(id, 'Active') },
  filled: async (id) => (scheduling.get(id) === 'PauseForRestart' ? { done: false, why: 'paused' } : { done: true, why: '' }),
}
const platform = (release: string, more: Record<string, unknown> = {}) => ({ apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Platform', metadata: { name: 'peat' }, spec: { release, objectStore: { endpoint: 'https://s3.example.com', bucket: 'b', credentialsSecret: 's' }, upgrades: { checkTimeout: '1m' }, ...more } })
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
  scheduling.clear()
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
    respec('release-b', { upgrades: { checkTimeout: '1m', quietHours: [{ start: '12:00', end: '12:30' }] } })
    await pass()
    expect(statusOf().phase).toBe('Paused')
    expect(image('deployments', 'peat', 'storage-controller')).toBe('ghcr.io/zabaca/peat/storage:release-a')
    clock = new Date('2026-12-01T12:31:00Z')
    for (let i = 0; i < 80 && (i === 0 || statusOf().phase !== 'Ready'); i++) await pass()
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-b' })
  })
})

describe('adoption', () => {
  const fresh = () => {
    k = new FakeKube()
    for (const n of ['machine-1', 'machine-2', 'machine-3']) k.put({ apiVersion: 'v1', kind: 'Node', metadata: { name: n, labels: LABEL } })
  }
  test('an object of a piece\'s name made another way is taken over as it is; its difference waits as a pending change, even off the list, until requests.applyPending asks', async () => {
    fresh()
    k.put({ apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: 'storage-broker', namespace: 'peat' }, spec: { replicas: 1, strategy: { type: 'Recreate' }, selector: { matchLabels: { app: 'storage-broker' } }, template: { metadata: { labels: { app: 'storage-broker' } }, spec: { containers: [{ name: 'storage-broker', image: 'ghcr.io/zabaca/peat/storage:older' }] } } } })
    const adopt = [{ kind: 'Deployment', namespace: 'peat', name: 'storage-broker' }]
    k.put(platform('release-a', { adopt }))
    await pass()
    expect(image('deployments', 'peat', 'storage-broker')).toBe('ghcr.io/zabaca/peat/storage:older')
    const pendingChange = (statusOf().pendingChanges as any[]).find((x) => x.name === 'storage-broker')
    expect(pendingChange.change).toContain('.spec.template.spec.containers[0].image: "ghcr.io/zabaca/peat/storage:older" would be "ghcr.io/zabaca/peat/storage:release-a"')
    expect(k.get('apps/v1', 'deployments', 'peat', 'storage-broker').metadata.labels['peat.zabaca.com/adopted']).toBe('true')
    // what runs is not yet what is declared: not Ready
    expect(statusOf().phase).toBe('Installing')
    // off the list it is still the operator's, still as it was: nothing restarts by a change of the list
    respec('release-a')
    await pass()
    expect(image('deployments', 'peat', 'storage-broker')).toBe('ghcr.io/zabaca/peat/storage:older')
    // asked for: applied, nothing left pending, the request answered
    respec('release-a', { requests: { applyPending: 'now' } })
    await pass()
    expect(image('deployments', 'peat', 'storage-broker')).toBe('ghcr.io/zabaca/peat/storage:release-a')
    expect(statusOf().pendingChanges).toEqual([])
    expect(statusOf().answered).toEqual({ applyPending: 'now' })
    expect(k.get('apps/v1', 'deployments', 'peat', 'storage-broker').metadata.labels['peat.zabaca.com/adopted']).toBeUndefined()
  })
  test('a lost record is taken back by adoption: what the operator would not change is recorded again and applied, nothing restarts', async () => {
    fresh()
    k.put(platform('release-a'))
    await pass()
    const ctl = k.get('apps/v1', 'deployments', 'peat', 'storage-controller')
    // the record lost (or a cluster restored with new UIDs): every piece is refused
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|configmaps|peat-owned'))!)
    const refused = await reconcile(k, { storage: () => storage, now: () => clock })
    expect(refused.refused.some((x) => x.includes('Deployment/peat/storage-controller'))).toBe(true)
    // listed in adopt, it is taken back into the record as it is
    respec('release-a', { adopt: [{ kind: 'Deployment', namespace: 'peat', name: 'storage-controller' }] })
    const r = await reconcile(k, { storage: () => storage, now: () => clock })
    expect(r.refused.some((x) => x.includes('Deployment/peat/storage-controller'))).toBe(false)
    expect(k.get('v1', 'configmaps', 'peat-system', 'peat-owned').data['Deployment.peat.storage-controller']).toBe(`uid:${ctl.metadata.uid}`)
    expect(statusOf().pendingChanges.find((x: any) => x.name === 'storage-controller')).toBeUndefined()
  })
})

describe('the review\'s faults, each played', () => {
  test('going back past a pageserver that never becomes ready on the new image: the way back mends it, it does not wait for it', async () => {
    respec('release-b')
    // pageserver-w1 on release-b never becomes ready
    const broken = () => { for (const o of k.all('statefulsets').filter((x) => x.metadata.name === 'pageserver-w1' && x.spec.template.spec.containers[0].image.endsWith(':release-b'))) o.status = { readyReplicas: 0, replicas: 1 }; const pod = k.get('v1', 'pods', 'peat', 'pageserver-w1-0'); if (pod?.spec.containers[0].image.endsWith(':release-b')) pod.status = { conditions: [] } }
    for (let i = 0; i < 30 && statusOf().phase !== 'Halted'; i++) { await pass(); broken() }
    expect(statusOf().phase).toBe('Halted')
    expect(statusOf().upgrade.step).toBe('pageserver-w1')
    respec('release-a')
    for (let i = 0; i < 40 && !(statusOf().phase === 'Ready' && statusOf().release === 'release-a'); i++) { await pass(); broken() }
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-a' })
    expect(image('statefulsets', 'peat', 'pageserver-w1')).toBe('ghcr.io/zabaca/peat/storage:release-a')
  })
  test('the drain is asked only once the status says draining; a drain the controller ended is asked again', async () => {
    respec('release-b')
    for (let i = 0; i < 30 && statusOf().upgrade?.step !== 'pageserver-w1'; i++) await pass()
    // the pass whose check passed recorded draining and asked nothing yet
    expect(statusOf().upgrade.phase).toBe('draining')
    expect(calls).toEqual([])
    // the drain asked, then ended by the controller (its own restart): the node reads Active again, and the drain is asked again
    storage.drained = async () => ({ done: false, why: 'shards still attached' })
    await pass()
    expect(calls).toEqual(['drain 1'])
    scheduling.set(1, 'Active')
    await pass()
    expect(calls).toEqual(['drain 1', 'drain 1'])
    storage.drained = async (id) => (scheduling.get(id) === 'PauseForRestart' ? { done: true, why: '' } : { done: false, why: 'draining' })
  })
  test('a node left Draining or PauseForRestart by an earlier attempt at the step does not fail its own check', async () => {
    respec('release-b')
    for (let i = 0; i < 30 && !(statusOf().upgrade?.moved ?? []).includes('controller'); i++) await pass()
    expect(statusOf().upgrade.step).toBeUndefined()
    // the next pass begins pageserver-w1 with its check: its own node's scheduling is no problem there, another node's is
    unsettled = ['node 1 scheduling is "PauseForRestart"']
    await pass()
    expect(statusOf().upgrade).toMatchObject({ step: 'pageserver-w1', phase: 'draining' })
    unsettled = ['node 2 scheduling is "Draining"']
    respec('release-b')
    const u = statusOf().upgrade
    k.get('peat.zabaca.com/v1alpha1', 'platforms', null, 'peat').status.upgrade = { ...u, phase: 'precheck' }
    await pass()
    expect(statusOf().upgrade.phase).toBe('precheck')
    expect(statusOf().upgrade.lastCheck.message).toContain('node 2 scheduling')
    unsettled = []
  })
  test('an object adopted cleanly and still listed is the operator\'s: the upgrade\'s change is applied, never held as pending', async () => {
    respec('release-a', { adopt: [{ kind: 'StatefulSet', namespace: 'peat', name: 'pageserver-w1' }] })
    await pass(); await pass()
    expect(statusOf().pendingChanges).toEqual([])
    respec('release-b', { adopt: [{ kind: 'StatefulSet', namespace: 'peat', name: 'pageserver-w1' }] })
    for (let i = 0; i < 80 && (i === 0 || statusOf().phase !== 'Ready'); i++) await pass()
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-b', pendingChanges: [] })
    expect(image('statefulsets', 'peat', 'pageserver-w1')).toBe('ghcr.io/zabaca/peat/storage:release-b')
  })
  test('a pod is deleted for a restart only once its template carries the new image', async () => {
    respec('release-b')
    for (let i = 0; i < 30 && !(statusOf().upgrade?.step === 'pageserver-w1' && statusOf().upgrade?.phase === 'restarting'); i++) await pass()
    // the template held back (as a pending change would hold it)
    const sts = k.get('apps/v1', 'statefulsets', 'peat', 'pageserver-w1')
    sts.spec.template.spec.containers[0].image = 'ghcr.io/zabaca/peat/storage:release-a'
    const pod = k.get('v1', 'pods', 'peat', 'pageserver-w1-0')
    const r = await reconcile(k, { storage: () => storage, now: () => clock })
    expect(k.calls.filter((c) => c === 'DELETE /api/v1/namespaces/peat/pods/pageserver-w1-0')).toEqual([])
    expect(k.get('v1', 'pods', 'peat', 'pageserver-w1-0').metadata.uid).toBe(pod.metadata.uid)
    expect(r.phase).toBe('Upgrading')
  })
  test('with safekeepers outside, the keeper-prep Job is made again on the new image (a Job\'s template cannot change)', async () => {
    respec('release-a', { safekeepers: { outside: { machines: ['machine-3'] } } })
    await pass()
    const before = k.get('batch/v1', 'jobs', 'peat', 'keeper-prep')
    expect(before).toBeDefined()
    respec('release-b', { safekeepers: { outside: { machines: ['machine-3'] } } })
    for (let i = 0; i < 80 && (i === 0 || statusOf().phase !== 'Ready'); i++) await pass()
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-b' })
    const after = k.get('batch/v1', 'jobs', 'peat', 'keeper-prep')
    expect(after.metadata.uid).not.toBe(before.metadata.uid)
    expect(after.spec.template.spec.containers[0].image).toBe('ghcr.io/zabaca/peat/storage:release-b')
  })
  test('adoption compares the annotations the operator sets and a Secret\'s values: a difference there is pending, not written', () => {
    expect(differences({ metadata: { annotations: { 'lb.example/ip': '192.0.2.6', 'peat.zabaca.com/release': 'b' } } }, { metadata: { annotations: { 'lb.example/ip': '192.0.2.5', 'peat.zabaca.com/release': 'a', 'other.example/x': '1' } } }))
      .toEqual(['.metadata.annotations.lb.example/ip: "192.0.2.5" would be "192.0.2.6"'])
    expect(differences({ stringData: { 'auth_public_key.pem': 'mine' } }, { data: { 'auth_public_key.pem': Buffer.from('theirs').toString('base64') } })).toEqual(['.data.auth_public_key.pem: its value would change'])
    expect(differences({ stringData: { k: 'same' } }, { data: { k: Buffer.from('same').toString('base64') } })).toEqual([])
  })
})

describe('the review\'s smaller points', () => {
  test('a keeper waits while a keeper replacement is open, and carries the release it now runs', async () => {
    respec('release-b')
    for (let i = 0; i < 40 && !(statusOf().upgrade?.moved ?? []).includes('pageserver-w2'); i++) await pass()
    k.put({ apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'sk-replacement-2', namespace: 'peat', labels: { 'peat-sk-open': 'true' } }, data: {} })
    for (let i = 0; i < 3; i++) await pass()
    expect(statusOf().upgrade).toMatchObject({ step: 'safekeeper-w1', phase: 'precheck' })
    expect(statusOf().upgrade.lastCheck.message).toContain('a keeper replacement is open: sk-replacement-2')
    k.objects.delete([...k.objects.keys()].find((x) => x.endsWith('|configmaps|sk-replacement-2'))!)
    for (let i = 0; i < 80 && statusOf().phase !== 'Ready'; i++) await pass()
    expect(k.get('apps/v1', 'statefulsets', 'peat', 'safekeeper-w1').metadata.annotations['peat.zabaca.com/release']).toBe('release-b')
  })
  test('the same release name with other images starts an upgrade too', async () => {
    respec('release-a', { images: { storage: 'registry.example.com/storage:pinned' } })
    await pass()
    expect(statusOf().phase).toBe('Upgrading')
  })
  test('a third release halts with how to go on: set it again and retry', async () => {
    respec('release-b')
    await pass()
    respec('release-c')
    await pass()
    const halted = statusOf().upgrade.halted
    expect(halted.reason).toContain('set release-b again and run peat retry to go on, or set release-a to go back')
    await pass()
    expect(statusOf().upgrade.halted.at).toBe(halted.at)
  })
  test('pending changes keep the platform from Ready; an adopt entry naming nothing is said', async () => {
    respec('release-a', { adopt: [{ kind: 'Deployment', namespace: 'peat', name: 'nothing-like-it' }] })
    await pass()
    expect(statusOf().message).toContain('adopt names Deployment/peat/nothing-like-it, which the platform does not make')
  })
})

describe('the second review\'s faults, with their siblings', () => {
  // a piece broken on release-b: its workload never ready there, and the storage readings a broken piece of that kind gives
  const breakOn = (piece: string) => {
    const isB = (o: any) => String(o?.spec?.template?.spec?.containers?.[0]?.image ?? '').endsWith(':release-b')
    const broken = () => {
      for (const o of [...k.all('deployments'), ...k.all('statefulsets')]) if (o.metadata.name === piece && isB(o)) o.status = { readyReplicas: 0, replicas: 1, observedGeneration: o.metadata.generation ?? 1 }
      const pod = k.get('v1', 'pods', 'peat', `${piece}-0`)
      if (pod?.spec.containers[0].image.endsWith(':release-b')) pod.status = { conditions: [] }
    }
    const brokenNow = () => [...k.all('deployments'), ...k.all('statefulsets')].some((o) => o.metadata.name === piece && isB(o))
    storage.problems = async () => {
      if (piece === 'storage-controller' && brokenNow()) throw new Error('fetch failed')
      const m = /^safekeeper-w(\d)$/.exec(piece)
      if (m && brokenNow()) return [`safekeeper ${m[1]} does not answer`]
      return [...unsettled]
    }
    return broken
  }
  afterEach(() => { storage.problems = async () => [...unsettled] })
  for (const piece of ['storage-controller', 'storage-broker', 'pageserver-w1', 'pageserver-w2', 'safekeeper-w1', 'safekeeper-w2', 'safekeeper-w3', 'peat-gateway', 'peat-control']) {
    test(`going back past a broken ${piece}: the way back mends it, it does not wait for it`, async () => {
      const broken = breakOn(piece)
      respec('release-b')
      for (let i = 0; i < 80 && statusOf().phase !== 'Halted'; i++) { await pass(); broken() }
      expect(statusOf().phase).toBe('Halted')
      respec('release-a')
      for (let i = 0; i < 80 && !(statusOf().phase === 'Ready' && statusOf().release === 'release-a'); i++) { await pass(); broken() }
      expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-a' })
    })
  }
  test('while one keeper is mended on the way back the others must still answer', async () => {
    const broken = breakOn('safekeeper-w1')
    respec('release-b')
    for (let i = 0; i < 80 && statusOf().phase !== 'Halted'; i++) { await pass(); broken() }
    const own = storage.problems
    storage.problems = async (x) => [...(await own(x)), 'safekeeper 2 does not answer']
    respec('release-a')
    for (let i = 0; i < 5; i++) { await pass(); broken() }
    expect(statusOf().upgrade.lastCheck.message).toContain('safekeeper 2 does not answer')
    expect(statusOf().upgrade.lastCheck.message).not.toContain('safekeeper 1 does not answer')
  })
})

describe('adoption written in the right order', () => {
  const broker = () => k.put({ apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: 'storage-broker', namespace: 'peat' }, spec: { replicas: 1, strategy: { type: 'Recreate' }, selector: { matchLabels: { app: 'storage-broker' } }, template: { metadata: { labels: { app: 'storage-broker' } }, spec: { containers: [{ name: 'storage-broker', image: 'ghcr.io/zabaca/peat/storage:older' }] } } } })
  const fresh = () => {
    k = new FakeKube()
    for (const n of ['machine-1', 'machine-2', 'machine-3']) k.put({ apiVersion: 'v1', kind: 'Node', metadata: { name: n, labels: LABEL } })
  }
  const adopt = [{ kind: 'Deployment', namespace: 'peat', name: 'storage-broker' }]
  for (const failing of ['the mark', 'the record'] as const) {
    test(`a pass that fails writing ${failing}: the next holds the difference, never applies it`, async () => {
      fresh(); broker()
      k.put(platform('release-a', { adopt }))
      if (failing === 'the mark') k.forcedOnce.set('PATCH /apis/apps/v1/namespaces/peat/deployments/storage-broker', { status: 500, json: { message: 'down' } })
      else k.forcedOnce.set('PATCH /api/v1/namespaces/peat-system/configmaps/peat-owned', { status: 500, json: { message: 'down' } })
      await reconcile(k, { storage: () => storage, now: () => clock }).catch(() => undefined)
      for (let i = 0; i < 3; i++) await pass()
      expect(image('deployments', 'peat', 'storage-broker')).toBe('ghcr.io/zabaca/peat/storage:older')
      expect((statusOf().pendingChanges as any[]).some((x) => x.name === 'storage-broker')).toBe(true)
      // and taken off the list it is still held
      respec('release-a')
      await pass()
      expect(image('deployments', 'peat', 'storage-broker')).toBe('ghcr.io/zabaca/peat/storage:older')
    })
  }
  test('a release changed while an adoption is pending goes through the ordered steps, from what the live pieces run', async () => {
    fresh(); broker()
    k.put(platform('release-a', { adopt }))
    for (let i = 0; i < 4; i++) await pass()
    // never recorded Ready on release-a? the release is recorded once every piece is ready, pending or not
    respec('release-b', { adopt })
    await pass()
    // the adopted broker runs another image than the controller: nothing starts from pieces that disagree, and nothing moves at once
    expect(statusOf().phase).toBe('Installing')
    expect(statusOf().message).toContain('the pieces do not all run the same images (storage-broker runs ghcr.io/zabaca/peat/storage:older')
    expect(image('deployments', 'peat', 'peat-control')).toBe('ghcr.io/zabaca/peat/control:release-a')
    expect(image('deployments', 'peat-gateway', 'peat-gateway')).toBe('ghcr.io/zabaca/peat/control:release-a')
    expect(image('statefulsets', 'peat', 'pageserver-w2')).toBe('ghcr.io/zabaca/peat/storage:release-a')
  })
})

describe('the keeper-prep Job, steady', () => {
  test('with safekeepers outside and no tolerations, keeper-prep is made once and left alone pass after pass', async () => {
    respec('release-a', { safekeepers: { outside: { machines: ['machine-3'] } } })
    await pass()
    const uid = k.get('batch/v1', 'jobs', 'peat', 'keeper-prep').metadata.uid
    k.calls.length = 0
    for (let i = 0; i < 5; i++) await pass()
    expect(k.get('batch/v1', 'jobs', 'peat', 'keeper-prep').metadata.uid).toBe(uid)
    expect(k.calls.filter((c) => c.startsWith('DELETE /apis/batch/v1/namespaces/peat/jobs/keeper-prep'))).toEqual([])
  })
})

describe('a drain the controller refuses', () => {
  test('is a wait bounded by checkTimeout: the upgrade halts there, every pass completes', async () => {
    const drain = storage.drain, node = storage.node
    storage.drain = async () => { throw new Error('drain of pageserver 1: 412 no other node to drain to') }
    respec('release-b')
    for (let i = 0; i < 40 && statusOf().phase !== 'Halted'; i++) await pass()
    expect(statusOf().phase).toBe('Halted')
    expect(statusOf().upgrade.halted.reason).toContain('412 no other node to drain to')
    storage.node = async () => { throw new Error('the storage controller: fetch failed') }
    const r = await reconcile(k, { storage: () => storage, now: () => clock })
    expect(r.phase).toBe('Halted')
    storage.drain = drain; storage.node = node
  })
})

describe('the third review\'s faults, with their siblings', () => {
  const pinned = (storage: string, control: string) => ({ images: { storage, control, compute: { '18': 'ghcr.io/zabaca/peat/compute-pg18:release-a', '19': 'ghcr.io/zabaca/peat/compute-pg19:release-a' } } })
  const strip = () => {
    // as a platform installed another way: no release stamp on any piece, images pinned by digest, no release recorded
    for (const o of [...k.all('deployments'), ...k.all('statefulsets')]) {
      if (o.metadata.annotations) delete o.metadata.annotations['peat.zabaca.com/release']
      for (const c of o.spec.template.spec.containers ?? []) c.image = c.image.replace(/storage:release-a$/, 'storage@sha256:aaa').replace(/control:release-a$/, 'control@sha256:bbb')
    }
    const p = k.get('peat.zabaca.com/v1alpha1', 'platforms', null, 'peat')
    delete p.status.release; delete p.status.images; delete p.status.upgrade
  }
  test('a platform whose images no release records: nothing starts, nothing moves; pinning spec.images to what runs records the release under the spec\'s name', async () => {
    strip()
    respec('release-b')
    for (let i = 0; i < 3; i++) await pass()
    expect(statusOf().phase).toBe('Installing')
    expect(statusOf().message).toContain('the pieces run images no release records (storage ghcr.io/zabaca/peat/storage@sha256:aaa')
    expect(statusOf().upgrade).toBeUndefined()
    expect(statusOf().release).toBeUndefined()
    expect(image('deployments', 'peat', 'storage-controller')).toBe('ghcr.io/zabaca/peat/storage@sha256:aaa')
    expect(image('deployments', 'peat', 'peat-control')).toBe('ghcr.io/zabaca/peat/control@sha256:bbb')
    respec('release-b', pinned('ghcr.io/zabaca/peat/storage@sha256:aaa', 'ghcr.io/zabaca/peat/control@sha256:bbb'))
    for (let i = 0; i < 3; i++) await pass()
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-b' })
    // and from there an ordinary upgrade
    respec('release-c')
    for (let i = 0; i < 80 && (i === 0 || statusOf().phase !== 'Ready'); i++) await pass()
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-c' })
    expect(image('statefulsets', 'peat', 'pageserver-w2')).toBe('ghcr.io/zabaca/peat/storage:release-c')
  })
  test('a halted upgrade\'s images replaced by a new spec: what moved goes back, then the upgrade runs to the new images', async () => {
    respec('release-b')
    const isBad = (o: any) => String(o?.spec?.template?.spec?.containers?.[0]?.image ?? '').endsWith(':release-b')
    const broken = () => { for (const o of k.all('statefulsets').filter((x) => x.metadata.name === 'pageserver-w1' && isBad(x))) o.status = { readyReplicas: 0, replicas: 1 }; const pod = k.get('v1', 'pods', 'peat', 'pageserver-w1-0'); if (pod && isBad(pod)) pod.status = { conditions: [] } }
    for (let i = 0; i < 40 && statusOf().phase !== 'Halted'; i++) { await pass(); broken() }
    expect(statusOf().phase).toBe('Halted')
    respec('release-b', pinned('ghcr.io/zabaca/peat/storage:release-b-fixed', 'ghcr.io/zabaca/peat/control:release-b'))
    for (let i = 0; i < 160 && !(statusOf().phase === 'Ready' && statusOf().release === 'release-b'); i++) { await pass(); broken() }
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-b' })
    for (const n of ['storage-controller']) expect(image('deployments', 'peat', n)).toBe('ghcr.io/zabaca/peat/storage:release-b-fixed')
    expect(image('statefulsets', 'peat', 'pageserver-w1')).toBe('ghcr.io/zabaca/peat/storage:release-b-fixed')
  })
  test('the way back finishes at the images it started from, and renders them', async () => {
    respec('release-b')
    for (let i = 0; i < 6; i++) await pass()
    respec('release-a')
    for (let i = 0; i < 60 && !(statusOf().phase === 'Ready' && statusOf().release === 'release-a'); i++) await pass()
    expect(statusOf().images.storage).toBe('ghcr.io/zabaca/peat/storage:release-a')
    expect(image('deployments', 'peat', 'storage-controller')).toBe('ghcr.io/zabaca/peat/storage:release-a')
  })
  test('on the way back the controller\'s step skips every storage reading: a controller that answers wrongly is mended too', async () => {
    const isB = () => image('deployments', 'peat', 'storage-controller').endsWith(':release-b')
    storage.problems = async () => (isB() ? ['node 1 availability is "Offline"', 'node 2 availability is "Offline"'] : [...unsettled])
    respec('release-b')
    for (let i = 0; i < 40 && statusOf().phase !== 'Halted'; i++) await pass()
    expect(statusOf().phase).toBe('Halted')
    respec('release-a')
    for (let i = 0; i < 40 && !(statusOf().phase === 'Ready' && statusOf().release === 'release-a'); i++) await pass()
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-a' })
    storage.problems = async () => [...unsettled]
  })
  test('a way back set while a pageserver drains forward fills its node again before anything else', async () => {
    const drained = storage.drained
    storage.drained = async () => ({ done: false, why: 'shards still attached' })
    respec('release-b')
    for (let i = 0; i < 40 && !(statusOf().upgrade?.step === 'pageserver-w1' && statusOf().upgrade?.phase === 'draining' && calls.includes('drain 1')); i++) await pass()
    expect(scheduling.get(1)).toBe('PauseForRestart')
    respec('release-a')
    for (let i = 0; i < 40 && !(statusOf().phase === 'Ready' && statusOf().release === 'release-a'); i++) await pass()
    expect(calls).toContain('fill 1')
    expect(scheduling.get(1)).toBe('Active')
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-a' })
    storage.drained = drained
  })
  test('a reading that fails while filling is a wait, never an aborted pass', async () => {
    const filled = storage.filled
    storage.filled = async () => { throw new Error('the storage controller: fetch failed') }
    respec('release-b')
    for (let i = 0; i < 60 && statusOf().phase !== 'Halted'; i++) await pass()
    expect(statusOf().phase).toBe('Halted')
    expect(statusOf().upgrade.halted.reason).toContain('filling: Error: the storage controller: fetch failed')
    storage.filled = filled
  })
})

describe('adoption in an upgrade\'s steps', () => {
  const fresh = () => {
    k = new FakeKube()
    for (const n of ['machine-1', 'machine-2', 'machine-3']) k.put({ apiVersion: 'v1', kind: 'Node', metadata: { name: n, labels: LABEL } })
  }
  const adopt = [{ kind: 'Deployment', namespace: 'peat', name: 'storage-broker' }]
  // a broker made another way on the release's own image, with a command of its own
  const broker = () => k.put({ apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: 'storage-broker', namespace: 'peat' }, spec: { replicas: 1, strategy: { type: 'Recreate' }, selector: { matchLabels: { app: 'storage-broker' } }, template: { metadata: { labels: { app: 'storage-broker' } }, spec: { containers: [{ name: 'storage-broker', image: 'ghcr.io/zabaca/peat/storage:release-a', command: ['storage_broker', '--theirs'] }] } } } })
  test('a step whose adopted piece holds differences beyond its image waits for applyPending, naming them; once asked, the step applies the whole render', async () => {
    fresh(); broker()
    k.put(platform('release-a', { adopt }))
    await pass()
    // the control layer's safekeepers, as the main suite's setup makes them
    for (const [i, m] of ['machine-1', 'machine-2', 'machine-3'].entries()) k.put({ apiVersion: 'apps/v1', kind: 'StatefulSet', metadata: { name: `safekeeper-w${i + 1}`, namespace: 'peat', labels: { 'peat-sk': 'true' } }, spec: { replicas: 1, template: { spec: { nodeSelector: { 'kubernetes.io/hostname': m }, containers: [{ image: 'ghcr.io/zabaca/peat/storage:release-a' }] } } } })
    for (let i = 0; i < 3; i++) await pass()
    expect(statusOf().release).toBe('release-a') // recorded although a change is pending
    respec('release-b', { adopt })
    for (let i = 0; i < 3; i++) await pass()
    expect(statusOf().upgrade).toMatchObject({ step: 'controller', phase: 'precheck' })
    expect(statusOf().upgrade.lastCheck.message).toContain('storage-broker was adopted with differences held (')
    expect((statusOf().pendingChanges as any[]).find((x) => x.name === 'storage-broker').change).toContain('.spec.template.spec.containers[0].command')
    expect(image('deployments', 'peat', 'storage-broker')).toBe('ghcr.io/zabaca/peat/storage:release-a')
    respec('release-b', { adopt, requests: { applyPending: 'go' } })
    for (let i = 0; i < 80 && statusOf().phase !== 'Ready'; i++) await pass()
    expect(statusOf()).toMatchObject({ phase: 'Ready', release: 'release-b' })
    const b = k.get('apps/v1', 'deployments', 'peat', 'storage-broker')
    expect(b.spec.template.spec.containers[0].image).toBe('ghcr.io/zabaca/peat/storage:release-b')
    expect(b.spec.template.spec.containers[0].command).not.toContain('--theirs')
  })
  test('no upgrade starts on the pass that adopts', async () => {
    const owned = k.get('v1', 'configmaps', 'peat-system', 'peat-owned')
    delete owned.data['Deployment.peat.storage-broker'] // the broker no longer in the record, as one made another way would be
    respec('release-b', { adopt })
    await pass()
    expect(statusOf().upgrade).toBeUndefined()
    await pass()
    expect(statusOf().upgrade?.direction).toBe('forward')
  })
  test('an empty wanted object is not a subset of a live one: a selector of every namespace is a difference', () => {
    expect(differences({ ingress: [{ from: [{ namespaceSelector: {} }] }] }, { ingress: [{ from: [{ podSelector: { matchLabels: { a: 'b' } } }] }] })).toEqual(['.ingress[0].from[0].namespaceSelector: none would be set'])
    expect(differences({ namespaceSelector: {} }, { namespaceSelector: { matchLabels: { a: 'b' } } })).toEqual(['.namespaceSelector: {"matchLabels":{"a":"b"}} would be {}'])
    expect(differences({ tolerations: [] }, {})).toEqual([])
  })
})
