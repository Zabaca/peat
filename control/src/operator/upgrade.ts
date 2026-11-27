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
}

/** A pageserver's step is its piece (render.ts names it); the pageserver a step is of (0: not a pageserver's). */
export const pageserverStep = pageserverPiece
export const pageserverOf = (step: string): number => (/^pageserver-w\d+$/.test(step) ? Number(step.slice('pageserver-w'.length)) : 0)

/** The steps of an upgrade, in order. Safekeepers are the control layer's StatefulSets, named here as the cluster has them. */
export function stepsOf(pageservers: number[], safekeepers: string[]): string[] {
  const sk = [...safekeepers].sort((a, b) => Number(a.replace(/\D/g, '')) - Number(b.replace(/\D/g, '')))
  return ['controller', ...[...pageservers].sort((a, b) => a - b).map(pageserverStep), ...sk, 'gateways', 'control']
}
/** The same images, field by field (a status read back from the API server need not keep the order its fields were written in). */
const sameImages = (a: Images, b: Images) => a.storage === b.storage && a.control === b.control && String(a.compute?.[18]) === String(b.compute?.[18]) && String(a.compute?.[19]) === String(b.compute?.[19])

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
    // a changed release, or the same release name with other images (an install that pins them)
    if (!o.running || (o.running.release === o.release && sameImages(o.running.images, o.images))) return { kind: 'none' }
    u = { from: o.running.release, to: o.release, fromImages: o.running.images, toImages: o.images, direction: 'forward', steps: o.steps, moved: [], startedAt: now }
  }
  if (o.release !== u.to && o.release !== u.from) {
    const reason = `the release ${o.release} was set while the upgrade from ${u.from} to ${u.to} is under way: set ${u.to} again and run peat retry to go on, or set ${u.from} to go back`
    // halted once for it: the time it halted is kept, not written again every pass
    return { kind: 'halt', state: { ...u, halted: u.halted?.reason === reason ? u.halted : { at: now, step: u.step ?? '(between steps)', reason } } }
  }
  const wanted = o.release === u.to ? 'forward' : 'back'
  if (wanted !== u.direction) {
    // the step under way counts where its images are: once anything of it was applied it is on the release it was going to
    let moved = u.moved
    if (u.step && applied(u.phase)) {
      if (u.direction === 'forward' && !moved.includes(u.step)) moved = [...moved, u.step]
      if (u.direction === 'back') moved = moved.filter((x) => x !== u!.step)
    }
    u = { ...u, direction: wanted, moved, step: undefined, phase: undefined, since: undefined, halted: undefined }
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
export function overdue(u: UpgradeState, now: Date, checkSeconds: number): boolean {
  return Boolean(u.since) && now.getTime() - Date.parse(u.since!) > checkSeconds * 1000
}
