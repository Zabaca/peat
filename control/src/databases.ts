// What the control layer does about one Database object, decided from the object and the others: pure, so a test can judge it; control.ts
// does what it says. A Database is the database's one record: the command and the API write the object, and the control layer makes the
// storage behind it, keeps its record in the object's status, and takes it apart when the object is deleted.
import { durationMs } from './api/schema'
import { GROUP } from './names'

/** The finalizer that keeps a deleted Database until its compute is gone and its data's hold is recorded. */
export const FINALIZER = `${GROUP}/storage`
/** On a Database the copy of older records writes: the control layer leaves it alone until the copy has written its record. */
export const COPYING = `${GROUP}/copying`
/** On a drill's Database: the control layer gives it a name beginning `drill`, and deletes its data at once (no hold). */
export const DRILL = `${GROUP}/drill`
export const DB_NAME = /^[a-z][a-z0-9-]{0,30}$/

export interface DbObject {
  metadata: { name: string; namespace: string; creationTimestamp?: string; deletionTimestamp?: string; finalizers?: string[]; annotations?: Record<string, string> }
  spec?: { pgVersion?: number; parent?: string; at?: string; replicaOf?: string; closed?: boolean; requests?: { stop?: string; rehearseMove?: string } }
  status?: { tenant?: string; timeline?: string; pgVersion?: number; phase?: string; message?: string; answered?: { stop?: string; rehearseMove?: string } }
}

export type Action =
  | { do: 'finalize' }
  | { do: 'make'; kind: 'root' | 'branch' | 'replica'; from?: string; at?: string; pg?: number }
  | { do: 'delete'; holdData: boolean }
  | { do: 'close' }
  | { do: 'open' }
  | { do: 'stop'; request: string }
  | { do: 'wait'; why: string }
  | { do: 'refuse'; why: string }
  | { do: 'none' }

const made = (o: DbObject) => Boolean(o.status?.tenant)
const older = (a: DbObject, b: DbObject) => String(a.metadata.creationTimestamp ?? '') < String(b.metadata.creationTimestamp ?? '') || (a.metadata.creationTimestamp === b.metadata.creationTimestamp && a.metadata.namespace < b.metadata.namespace)

/**
 * The next thing to do about `o`, given every Database (`all`) and whether a safekeeper replacement is open (no database is made while one
 * is: the controller could put a new timeline on the member being replaced). In order: a deleted object is taken apart (a root waits for its
 * branches and replicas to go first); an object without the finalizer gets it; an object not made yet is made, unless its name is taken
 * (names are unique across namespaces: the oldest object keeps it), malformed, a drill's, or its parent is not made yet; a made one is
 * closed, opened or stopped as its spec asks. A version raised or lowered is refused here (the move to a newer version is its own step).
 */
export function databaseAction(o: DbObject, all: DbObject[], ctx: { replacementOpen: string | null }): Action {
  const name = o.metadata.name
  const others = all.filter((x) => x !== o && !(x.metadata.name === name && x.metadata.namespace === o.metadata.namespace))
  if (o.metadata.deletionTimestamp) {
    if (!(o.metadata.finalizers ?? []).includes(FINALIZER)) return { do: 'none' }
    if (made(o)) {
      const children = others.filter((x) => (x.spec?.parent === name || x.spec?.replicaOf === name) && x.metadata.namespace === o.metadata.namespace && !x.metadata.deletionTimestamp)
      if (children.length) return { do: 'wait', why: `deleted, waiting for ${children.map((x) => x.metadata.name).join(', ')} (made from it) to be deleted first` }
    }
    return { do: 'delete', holdData: made(o) && o.metadata.annotations?.[DRILL] !== 'true' }
  }
  if (o.metadata.annotations?.[COPYING]) return { do: 'none' }
  if (!(o.metadata.finalizers ?? []).includes(FINALIZER)) return { do: 'finalize' }
  if (!made(o)) {
    if (!DB_NAME.test(name)) return { do: 'refuse', why: `a database's name is 1 to 31 lowercase letters, digits or hyphens, starting with a letter, not ${JSON.stringify(name)}` }
    if (name.startsWith('drill') && o.metadata.annotations?.[DRILL] !== 'true') return { do: 'refuse', why: 'names that begin with drill belong to the platform\'s drills' }
    const holder = others.find((x) => x.metadata.name === name && (made(x) || older(x, o)))
    if (holder) return { do: 'refuse', why: `the name ${name} is taken by the database in ${holder.metadata.namespace}: a name is unique across the platform` }
    if (o.spec?.parent && o.spec?.replicaOf) return { do: 'refuse', why: 'a database is made from a parent or is a replica of one, not both' }
    const from = o.spec?.parent ?? o.spec?.replicaOf
    if (from) {
      const p = others.find((x) => x.metadata.name === from && x.metadata.namespace === o.metadata.namespace)
      if (!p) return { do: 'wait', why: `no database ${from} in ${o.metadata.namespace} to make it from` }
      if (!made(p)) return { do: 'wait', why: `${from} is not made yet` }
      if (o.spec?.pgVersion && p.status?.pgVersion && o.spec.pgVersion !== p.status.pgVersion) return { do: 'refuse', why: `a branch or a replica is of its parent's version (${p.status.pgVersion}), not ${o.spec.pgVersion}` }
    }
    if (o.spec?.at && !o.spec.parent) return { do: 'refuse', why: 'at is the moment of a parent: name the parent' }
    if (ctx.replacementOpen) return { do: 'wait', why: `new databases wait while a safekeeper replacement is open: ${ctx.replacementOpen}` }
    if (o.spec?.replicaOf) return { do: 'make', kind: 'replica', from: o.spec.replicaOf }
    if (o.spec?.parent) return { do: 'make', kind: 'branch', from: o.spec.parent, ...(o.spec.at ? { at: o.spec.at } : {}) }
    return { do: 'make', kind: 'root', ...(o.spec?.pgVersion ? { pg: o.spec.pgVersion } : {}) }
  }
  if (o.spec?.closed && o.status?.phase !== 'Closed') return { do: 'close' }
  if (!o.spec?.closed && o.status?.phase === 'Closed') return { do: 'open' }
  const stop = o.spec?.requests?.stop
  if (stop && stop !== o.status?.answered?.stop) return { do: 'stop', request: stop }
  if (o.spec?.pgVersion && o.status?.pgVersion && o.spec.pgVersion !== o.status.pgVersion && o.status.phase !== 'Failed') {
    if (o.spec.pgVersion < o.status.pgVersion) return { do: 'refuse', why: `Postgres ${o.status.pgVersion} cannot be moved back to ${o.spec.pgVersion}: make a new database and copy into it` }
    return { do: 'refuse', why: `the move from Postgres ${o.status.pgVersion} to ${o.spec.pgVersion} is not in this release: the database stays on ${o.status.pgVersion}` }
  }
  return { do: 'none' }
}

/** When a deleted database's data goes: `hold` is the Platform's deleteHold (`7d`, `36h`, `never`); null is never. */
export function deleteAfter(hold: string, now: Date = new Date()): Date | null {
  if (hold === 'never') return null
  return new Date(now.getTime() + durationMs(hold)) // a duration, as every one of the kinds (7d, 36h)
}

/** A held deletion's data may be removed now: its time has come, and no database still uses its tenant (a root's data goes with its last timeline). */
export function purgeDue(t: { deleteAfter: string; tenant: string; root: boolean }, now: Date, tenantInUse: boolean): boolean {
  if (t.deleteAfter === 'never') return false
  if (Date.parse(t.deleteAfter) > now.getTime()) return false
  return !(t.root && tenantInUse)
}
