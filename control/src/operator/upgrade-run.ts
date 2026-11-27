// One pass of an upgrade's current step: the work its phase does to the cluster and what it waits for, bounded by the Platform's
// upgrades.checkTimeout (past it the upgrade halts there, with why). The check before and after each step is the whole platform's: every
// piece ready and the storage settled (storage.ts).
import { durationMs } from '../api/schema'
import { ok, type Kube } from '../kube'
import type { Resolved } from './platform'
import type { Storage } from './storage'
import { RELEASE_ANNOTATION } from './render'
import { nextPhase, overdue, pageserverOf, type Phase, type UpgradeState } from './upgrade'

export interface StepWork { state: UpgradeState; message: string }

/** The pieces (as the status names them) a step moves. */
export function piecesOf(step: string): string[] {
  if (step === 'controller') return ['storage-controller', 'storage-broker']
  if (step === 'gateways') return ['peat-gateway']
  if (step === 'control') return ['peat-control']
  return [step]
}

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
    const bound = Math.round(durationMs(r.spec.upgrades.checkTimeout) / 1000)
    if (overdue(u, now, bound)) return { state: { ...u, ...extra, halted: { at, step, reason: `${phase}: ${why} (for more than ${r.spec.upgrades.checkTimeout})` } }, message: `halted at ${step}: ${why}` }
    return { state: { ...u, ...extra }, message: `${step}, ${phase}: ${why}` }
  }
  const pageserverId = pageserverOf(step)
  const ownNode = new RegExp(`\\bnode ${pageserverId}\\b`)

  if (phase === 'precheck' || phase === 'postcheck') {
    // the step's own piece is left out of the check before it: on the way back it is the broken piece the way back mends (so it can never
    // be ready first), and its node may still read Draining or PauseForRestart from an earlier attempt at this step
    const own = (name: string) => piecesOf(step).includes(name)
    const back = phase === 'precheck' && u.direction === 'back'
    const storageProblems = (await storage.problems(back && pageserverId ? psIds.filter((x) => x !== pageserverId) : psIds).catch((e) => [String(e)]))
      .filter((x) => !(phase === 'precheck' && pageserverId && (back ? ownNode.test(x) : /^node \d+ scheduling is/.test(x) && ownNode.test(x))))
    const problems = [...notReady.filter((x) => !(back && own(x))).map((x) => `${x} is not ready`), ...storageProblems]
    // a keeper is restarted only with the others answering (a write needs two of three) and no keeper replacement open (the control layer
    // is moving a member)
    if (step.startsWith('safekeeper-w') && phase === 'precheck') {
      const open = await k.request('GET', `/api/v1/namespaces/${ns}/configmaps?labelSelector=peat-sk-open%3Dtrue`)
      if (open.status !== 200) problems.push(`the keeper replacements could not be read: ${open.status}`)
      else if ((open.json.items as any[]).length) problems.push(`a keeper replacement is open: ${(open.json.items as any[]).map((x) => x.metadata.name).join(', ')}`)
    }
    const lastCheck = { at, passed: problems.length === 0, message: problems.length ? problems.slice(0, 5).join('; ') : 'every piece ready, the storage settled' }
    if (problems.length) return wait(`the platform is not settled: ${lastCheck.message}`, { lastCheck })
    // the drain is asked in the draining phase, once the status says the step is there (a pass that dies between leaves nothing unrecorded)
    return advance(`${step}: the check passed`, { lastCheck })
  }
  if (phase === 'draining') {
    const n = await storage.node(pageserverId)
    // a pageserver that is not available has nothing attached to drain (the controller moved its shards when it went): on the way back the
    // broken one is restarted at once
    if (n && n.availability !== 'Active' && u.direction === 'back') return advance(`pageserver ${pageserverId} is not available: nothing to drain`)
    const d = await storage.drained(pageserverId)
    if (d.done) return advance(`pageserver ${pageserverId} drained`)
    // asked again whenever the node reads Active: a drain the controller ended (its own restart, a cancel) is started again
    if (n && n.scheduling === 'Active') await storage.drain(pageserverId)
    return wait(`draining: ${d.why}`)
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
      // its release stamp too, so the status reads the release it now runs
      ok(await k.request('PATCH', path, { metadata: { annotations: { [RELEASE_ANNOTATION]: target.release } } }, 'application/merge-patch+json'), `mark ${step}'s release`)
    }
    return advance(`${step}: its new image applied`)
  }
  if (phase === 'restarting') {
    // the pod is deleted only once its StatefulSet's template carries the new image: a pod made again from an older template comes back on
    // the old image (a template held back, for example as an adopted object's pending change)
    const sts = await k.request('GET', `/apis/apps/v1/namespaces/${ns}/statefulsets/${step}`)
    if (sts.status !== 200 || imageOf(sts.json) !== target.storage) return wait(`its StatefulSet's template does not carry ${target.storage}${sts.status === 200 ? ` (it has ${imageOf(sts.json)}: a pending change? requests.applyPending applies it)` : ''}`)
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
