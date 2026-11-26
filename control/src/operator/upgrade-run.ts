// One pass of an upgrade's current step: the work its phase does to the cluster and what it waits for, bounded by the Platform's
// upgrades.checkSeconds (past it the upgrade halts there, with why). The check before and after each step is the whole platform's: every
// piece ready and the storage settled (storage.ts).
import { ok, type Kube } from '../kube'
import type { Resolved } from './platform'
import type { Storage } from './storage'
import { nextPhase, overdue, type Phase, type UpgradeState } from './upgrade'

export interface StepWork { state: UpgradeState; message: string }

const imageOf = (o: any): string => String(o?.spec?.template?.spec?.containers?.[0]?.image ?? o?.spec?.containers?.[0]?.image ?? '')
const deploymentDone = (o: any, image: string) => o && imageOf(o) === image && (o.status?.observedGeneration ?? 0) >= (o.metadata?.generation ?? 0) && (o.status?.updatedReplicas ?? 0) >= (o.spec?.replicas ?? 1) && (o.status?.readyReplicas ?? 0) >= (o.spec?.replicas ?? 1) && (o.status?.replicas ?? 0) <= (o.spec?.replicas ?? 1)
const statefulSetDone = (o: any, image: string) => o && imageOf(o) === image && (o.status?.readyReplicas ?? 0) >= (o.spec?.replicas ?? 1) && (!o.status?.updateRevision || o.status.updateRevision === o.status.currentRevision)
const podReady = (p: any) => !!p?.status?.conditions?.some((c: any) => c.type === 'Ready' && c.status === 'True')

export async function runStep(k: Kube, r: Resolved, storage: Storage, u: UpgradeState, now: Date, notReady: string[]): Promise<StepWork> {
  const step = u.step!
  const phase = u.phase!
  const target = u.direction === 'forward' ? u.toImages : u.fromImages
  const ns = r.names.namespace
  const psIds = r.placements.pageservers.map((p) => p.id)
  const count = psIds.length
  const at = now.toISOString()
  const advance = (msg: string, extra: Partial<UpgradeState> = {}): StepWork => {
    const next = nextPhase(step, phase, count)
    if (next === 'done') {
      const moved = u.direction === 'forward' ? [...u.moved, step] : u.moved.filter((x) => x !== step)
      return { state: { ...u, ...extra, moved, step: undefined, phase: undefined, since: undefined }, message: `${step} ${u.direction === 'forward' ? `moved to ${u.to}` : `moved back to ${u.from}`}` }
    }
    return { state: { ...u, ...extra, phase: next as Phase, since: at }, message: msg }
  }
  const wait = (why: string, extra: Partial<UpgradeState> = {}): StepWork => {
    if (overdue(u, now, r.spec.upgrades.checkSeconds)) return { state: { ...u, ...extra, halted: { at, step, reason: `${phase}: ${why} (for more than ${r.spec.upgrades.checkSeconds} s)` } }, message: `halted at ${step}: ${why}` }
    return { state: { ...u, ...extra }, message: `${step}, ${phase}: ${why}` }
  }
  const pageserverId = step.startsWith('pageserver-w') ? Number(step.slice('pageserver-w'.length)) : 0

  if (phase === 'precheck' || phase === 'postcheck') {
    const problems = [...notReady.map((x) => `${x} is not ready`), ...(await storage.problems(psIds).catch((e) => [String(e)]))]
    const lastCheck = { at, passed: problems.length === 0, message: problems.length ? problems.slice(0, 5).join('; ') : 'every piece ready, the storage settled' }
    if (problems.length) return wait(`the platform is not settled: ${lastCheck.message}`, { lastCheck })
    if (phase === 'precheck' && step.startsWith('pageserver-w') && count > 1) await storage.drain(pageserverId)
    return advance(`${step}: the check passed`, { lastCheck })
  }
  if (phase === 'draining') {
    const d = await storage.drained(pageserverId)
    return d.done ? advance(`pageserver ${pageserverId} drained`) : wait(`draining: ${d.why}`)
  }
  if (phase === 'applying') {
    // the operator's own pieces were rendered at the step's images in this pass; a safekeeper is the control layer's StatefulSet, changed here
    if (step.startsWith('safekeeper-w')) {
      const path = `/apis/apps/v1/namespaces/${ns}/statefulsets/${step}`
      const live = ok(await k.request('GET', path), `read ${step}`).json
      const ops = [
        ...((live.spec.template.spec.containers ?? []) as any[]).map((_: unknown, i: number) => ({ op: 'replace', path: `/spec/template/spec/containers/${i}/image`, value: target.storage })),
        ...((live.spec.template.spec.initContainers ?? []) as any[]).map((_: unknown, i: number) => ({ op: 'replace', path: `/spec/template/spec/initContainers/${i}/image`, value: target.storage })),
      ]
      ok(await k.request('PATCH', path, ops, 'application/json-patch+json'), `move ${step}`)
    }
    return advance(`${step}: its new image applied`)
  }
  if (phase === 'restarting') {
    const podPath = `/api/v1/namespaces/${ns}/pods/${step}-0`
    const pod = await k.request('GET', podPath)
    if (pod.status === 200 && imageOf(pod.json) !== target.storage && !pod.json.metadata?.deletionTimestamp) {
      await k.request('DELETE', podPath)
      return wait('its pod restarting on the new image')
    }
    if (pod.status !== 200 || imageOf(pod.json) !== target.storage || !podReady(pod.json)) return wait('its pod not ready on the new image yet')
    if (count > 1) await storage.fill(pageserverId).catch(() => undefined)
    return advance(`${step}: restarted on the new image`)
  }
  if (phase === 'filling') {
    const f = await storage.filled(pageserverId)
    if (!f.done) { await storage.fill(pageserverId).catch(() => undefined); return wait(`filling: ${f.why}`) }
    return advance(`pageserver ${pageserverId} filled`)
  }
  // waiting: the step's workloads rolled out on the new image
  const read = async (kind: 'deployments' | 'statefulsets', namespace: string, name: string) => { const a = await k.request('GET', `/apis/apps/v1/namespaces/${namespace}/${kind}/${name}`); return a.status === 200 ? a.json : null }
  if (step === 'controller') {
    const [c, b] = await Promise.all([read('deployments', ns, 'storage-controller'), read('deployments', ns, 'storage-broker')])
    return deploymentDone(c, target.storage) && deploymentDone(b, target.storage) ? advance('the controller and the broker on the new image') : wait('the controller and the broker rolling out')
  }
  if (step === 'gateways') return deploymentDone(await read('deployments', r.names.gatewayNamespace, 'peat-gateway'), target.control) ? advance('the gateways on the new image') : wait('the gateways rolling out, one pod at a time')
  if (step === 'control') return deploymentDone(await read('deployments', ns, 'peat-control'), target.control) ? advance('the control layer on the new image') : wait('the control layer rolling out, one pod at a time')
  if (step.startsWith('safekeeper-w')) return statefulSetDone(await read('statefulsets', ns, step), target.storage) ? advance(`${step} on the new image`) : wait(`${step} restarting`)
  return wait(`no work is known for ${step}`)
}
