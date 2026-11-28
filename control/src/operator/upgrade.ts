// The ordered upgrade: a changed release moves the platform one piece at a time, in the safe order, the platform checked before and after
// every step; a failed check halts it until a person asks for a retry or sets the earlier release back; quiet hours pause it at the next
// step boundary. Its state is kept in the Platform's status (status.upgrade), so an operator that restarts goes on where the last one was.
//
// The order: the storage controller and the broker; each pageserver, drained first (its attached shards moved to the others), restarted on
// the new image, filled again and its copies warm; each safekeeper, restarted in place (a write still needs two of three); the gateways, one
// pod at a time; the control layer. The computes take the new image at their next start, or in the declared restart hours.
//
// Going back: setting the release the upgrade started from moves back only what had moved (the step under way included, once anything of
// it was applied), in the reverse order, with the same checks. A third release set while one is under way halts it.
import type { Window } from '../quiet-hours'
import { describeWindow, windowAt } from '../quiet-hours'
import type { Images } from './platform'
import { pageserverPiece } from './render'

export type Phase = 'precheck' | 'draining' | 'applying' | 'restarting' | 'waiting' | 'filling' | 'postcheck'
export interface UpgradeState {
  from: string
  to: string
  fromImages: Images
  toImages: Images
  direction: 'forward' | 'back'
  steps: string[]
  /** The pieces on `to`, in the order they moved. */
  moved: string[]
  step?: string
  phase?: Phase
  /** When the current phase began: its wait is bounded by the Platform's upgrades.checkTimeout. */
  since?: string
  startedAt: string
  halted?: { at: string; step: string; reason: string }
  retried?: string
  lastCheck?: { at: string; passed: boolean; message: string }
  complete?: boolean
  /** A pageserver a dropped draining step left paused: the next step's check fills it again before anything else. */
  refill?: number
  /** Why the way back was taken without the earlier release being set (new target images while the upgrade was under way). */
  reason?: string
}

/** A release no record names: a platform installed another way, whose pieces carry no release of ours. Never an upgrade's end. */
export const UNRECORDED = 'unrecorded'

/** The pageserver a step is of (0: not a pageserver's): the step is its piece, as render.ts names it. */
export const pageserverOf = (step: string): number => { const id = Number(step.replace(/^\D+/, '')); return Number.isInteger(id) && id > 0 && pageserverPiece(id) === step ? id : 0 }

/** The steps of an upgrade, in order. Safekeepers are the control layer's StatefulSets, named here as the cluster has them. */
export function stepsOf(pageservers: number[], safekeepers: string[]): string[] {
  const sk = [...safekeepers].sort((a, b) => Number(a.replace(/\D/g, '')) - Number(b.replace(/\D/g, '')))
  return ['controller', ...[...pageservers].sort((a, b) => a - b).map(pageserverPiece), ...sk, 'gateways', 'control']
}
/**
 * One image under two names: the same reference, or `x:tag` and `x:tag@sha256:d` (a release's tag and the digest its image map names
 * for it, as a release's install file has it). Two digests are two images, whatever their tags say (an image pinned anew to mend one).
 */
export function sameImage(a: string, b: string): boolean {
  if (a === b) return true
  const digest = /@sha256:[0-9a-f]{64}$/
  if (digest.test(a) === digest.test(b)) return false
  const [byDigest, byTag] = digest.test(a) ? [a, b] : [b, a]
  const named = byDigest.replace(digest, '')
  // a tag: a colon after the last slash (a registry's port is before it)
  return named === byTag && named.slice(named.lastIndexOf('/') + 1).includes(':')
}
/**
 * The same images, field by field (a status read back from the API server need not keep the order its fields were written in), each by
 * sameImage: a platform on a release's tags and the same release's digests is on one release, so moving from one to the other is never an
 * upgrade, and an upgrade by tags under way is not undone by an install file that names the same release by digest.
 */
export const sameImages = (a: Images, b: Images) => sameImage(a.storage, b.storage) && sameImage(a.control, b.control) && sameImage(String(a.compute?.[18]), String(b.compute?.[18])) && sameImage(String(a.compute?.[19]), String(b.compute?.[19]))
/** The very same references: what decides whether a piece's template would change. */
export const identicalImages = (a: Images, b: Images) => a.storage === b.storage && a.control === b.control && String(a.compute?.[18]) === String(b.compute?.[18]) && String(a.compute?.[19]) === String(b.compute?.[19])

/** Whether the current step's own images have been applied (so it renders at its target). */
const applied = (p: Phase | undefined) => p !== undefined && p !== 'precheck' && p !== 'draining'

/** The images a piece is rendered at, given the upgrade under way (or none). */
export function imagesFor(u: UpgradeState | undefined, current: Images): (piece: string) => Images {
  if (!u || u.complete) return () => current
  return (piece) => {
    if (u.step === piece && applied(u.phase)) return u.direction === 'forward' ? u.toImages : u.fromImages
    return u.moved.includes(piece) ? u.toImages : u.fromImages
  }
}

export type Decision =
  | { kind: 'none' }
  | { kind: 'continue'; state: UpgradeState }
  | { kind: 'finish'; release: string; images: Images; state: UpgradeState }
  | { kind: 'halt'; state: UpgradeState }
  | { kind: 'pause'; state: UpgradeState; window: string }

/**
 * What the upgrade does this pass, before any step's work: start one (the release changed from the one every piece last ran), flip it back
 * (the release it started from set again), finish it (every step done in its direction), halt it (a third release), answer a retry, or
 * pause it in quiet hours at a step boundary.
 */
export function decide(o: {
  release: string
  images: Images
  running?: { release: string; images: Images }
  state?: UpgradeState
  steps: string[]
  retry?: string
  quietHours: Window[]
  now: Date
}): Decision {
  const now = o.now.toISOString()
  let u = o.state && !o.state.complete ? { ...o.state } : undefined
  if (!u) {
    // a changed release, or the same release name with other images (an install that pins them); never from a release no record names
    if (!o.running || (o.running.release === o.release && sameImages(o.running.images, o.images)) || o.running.release === UNRECORDED) return { kind: 'none' }
    u = { from: o.running.release, to: o.release, fromImages: o.running.images, toImages: o.images, direction: 'forward', steps: o.steps, moved: [], startedAt: now }
  }
  if (o.release !== u.to && o.release !== u.from) {
    const reason = `the release ${o.release} was set while the upgrade from ${u.from} to ${u.to} is under way: set ${u.to} again and run peat retry to go on, or set ${u.from} to go back`
    // halted once for it: the time it halted is kept, not written again every pass
    return { kind: 'halt', state: { ...u, halted: u.halted?.reason === reason ? u.halted : { at: now, step: u.step ?? '(between steps)', reason } } }
  }
  // the target's images replaced (the release kept, its images pinned anew, to mend a bad one): what moved goes back to where it started, in
  // order, and the next upgrade then starts to the new images; the frozen images of the one under way are never mixed with the new
  const replaced = o.release === u.to && !sameImages(o.images, u.toImages)
  const wanted = o.release === u.to && !replaced ? 'forward' : 'back'
  if (wanted !== u.direction) {
    // the step under way counts where its images are: once anything of it was applied it is on the release it was going to
    let moved = u.moved
    if (u.step && applied(u.phase)) {
      if (u.direction === 'forward' && !moved.includes(u.step)) moved = [...moved, u.step]
      if (u.direction === 'back') moved = moved.filter((x) => x !== u!.step)
    }
    // a pageserver step dropped while it drained (nothing of it applied) leaves its node paused: the next step's check fills it again
    const refill = u.step && !applied(u.phase) && u.phase === 'draining' && pageserverOf(u.step) ? pageserverOf(u.step) : u.refill
    u = { ...u, direction: wanted, moved, step: undefined, phase: undefined, since: undefined, halted: undefined, ...(refill ? { refill } : {}), ...(replaced ? { reason: 'the target images were replaced while it was under way: what moved goes back, then the upgrade starts again to the new images' } : {}) }
  }
  if (u.halted) {
    if (o.retry && o.retry !== u.retried) u = { ...u, halted: undefined, retried: o.retry, since: now }
    else return { kind: 'halt', state: u }
  }
  if (!u.step) {
    const left = u.direction === 'forward' ? u.steps.filter((s) => !u!.moved.includes(s)) : [...u.moved].reverse()
    if (left.length === 0) return { kind: 'finish', release: u.direction === 'forward' ? u.to : u.from, images: u.direction === 'forward' ? u.toImages : u.fromImages, state: { ...u, complete: true } }
    const w = windowAt(o.quietHours, o.now)
    if (w) return { kind: 'pause', state: u, window: describeWindow(w) }
    u = { ...u, step: left[0], phase: 'precheck', since: now }
  }
  return { kind: 'continue', state: u }
}

/** What a step is made of, by its piece: the phases it goes through, in order. */
export function phasesOf(step: string, pageserverCount: number): Phase[] {
  if (pageserverOf(step)) return pageserverCount > 1 ? ['precheck', 'draining', 'applying', 'restarting', 'filling', 'postcheck'] : ['precheck', 'applying', 'restarting', 'postcheck']
  return ['precheck', 'applying', 'waiting', 'postcheck']
}
export function nextPhase(step: string, phase: Phase, pageserverCount: number): Phase | 'done' {
  const all = phasesOf(step, pageserverCount)
  const i = all.indexOf(phase)
  return i < 0 || i === all.length - 1 ? 'done' : all[i + 1]!
}

/** The step's phase is over its bound: the upgrade halts there, with why. */
export function overdue(u: UpgradeState, now: Date, timeoutSeconds: number): boolean {
  return Boolean(u.since) && now.getTime() - Date.parse(u.since!) > timeoutSeconds * 1000
}
