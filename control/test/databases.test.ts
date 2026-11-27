import { describe, expect, test } from 'bun:test'
import { COPYING, DRILL, FINALIZER, databaseAction, deleteAfter, purgeDue, type DbObject } from '../src/databases'

// What the control layer does about one Database object: made as its spec says, closed, opened, stopped, deleted (a root after its branches).

let n = 0
const db = (name: string, more: Partial<DbObject> & { ns?: string; made?: boolean; pg?: number } = {}): DbObject => ({
  metadata: { name, namespace: more.ns ?? 'shop', creationTimestamp: new Date(1_800_000_000_000 + n++ * 1000).toISOString(), finalizers: [FINALIZER], ...more.metadata },
  spec: more.spec ?? {},
  ...(more.made ? { status: { tenant: 'a'.repeat(32), timeline: 'b'.repeat(32), pgVersion: more.pg ?? 19, phase: 'Ready', ...more.status } } : more.status ? { status: more.status } : {}),
})
const act = (o: DbObject, all: DbObject[] = [o], open: string | null = null) => databaseAction(o, all, { replacementOpen: open })

describe('made as its spec says', () => {
  test('first the finalizer, so a delete waits for the storage to be taken apart', () => {
    expect(act(db('orders', { metadata: { name: 'orders', namespace: 'shop', finalizers: [] } }))).toEqual({ do: 'finalize' })
  })
  test('a new database, at the version asked or the platform\'s default', () => {
    expect(act(db('orders'))).toEqual({ do: 'make', kind: 'root' })
    expect(act(db('orders', { spec: { pgVersion: 18 } }))).toEqual({ do: 'make', kind: 'root', pg: 18 })
  })
  test('a branch, a restore to a moment and a replica, once the parent is made; the parent must be in the same namespace', () => {
    const parent = db('orders', { made: true })
    expect(act(db('orders-dev', { spec: { parent: 'orders' } }), [parent, db('orders-dev', { spec: { parent: 'orders' } })])).toMatchObject({ do: 'make', kind: 'branch', from: 'orders' })
    const restore = db('orders-tue', { spec: { parent: 'orders', at: '2026-12-01T09:30:00Z' } })
    expect(act(restore, [parent, restore])).toEqual({ do: 'make', kind: 'branch', from: 'orders', at: '2026-12-01T09:30:00Z' })
    const replica = db('orders-ro', { spec: { replicaOf: 'orders' } })
    expect(act(replica, [parent, replica])).toEqual({ do: 'make', kind: 'replica', from: 'orders' })
    const unmade = db('orders')
    const child = db('orders-dev', { spec: { parent: 'orders' } })
    expect(act(child, [unmade, child])).toEqual({ do: 'wait', why: 'orders is not made yet' })
    const elsewhere = db('x', { spec: { parent: 'orders' }, ns: 'other' })
    expect(act(elsewhere, [parent, elsewhere])).toEqual({ do: 'wait', why: 'no database orders in other to make it from' })
  })
  test('a name is unique across the platform: the oldest object keeps it, a later one in another namespace is refused', () => {
    const first = db('orders', { ns: 'a' }), second = db('orders', { ns: 'b' })
    expect(act(first, [first, second])).toMatchObject({ do: 'make' })
    expect(act(second, [first, second])).toEqual({ do: 'refuse', why: 'the name orders is taken by the database in a: a name is unique across the platform' })
  })
  test('a status that lost its record is written again from the platform\'s record, never made again; a record in another namespace holds the name', () => {
    const records = new Map([['orders', { namespace: 'shop' }]])
    expect(databaseAction(db('orders'), [db('orders')], { replacementOpen: null, records })).toEqual({ do: 'rebuild' })
    const other = db('orders', { ns: 'elsewhere' })
    expect(databaseAction(other, [other], { replacementOpen: null, records })).toEqual({ do: 'refuse', why: 'the name orders is taken by the database in shop: a name is unique across the platform' })
    // a deleted object whose status lost its record still waits for what was made from it
    const root = { ...db('orders'), metadata: { ...db('orders').metadata, deletionTimestamp: '2026-12-01T00:00:00Z' } }
    const branch = db('orders-dev', { made: true, spec: { parent: 'orders' } })
    expect(databaseAction(root, [root, branch], { replacementOpen: null, records })).toMatchObject({ do: 'wait' })
  })
  test('refused: a malformed name, a drill\'s name, a parent and a replica at once, a moment with no parent, a branch of another version', () => {
    expect(act(db('Orders'))).toMatchObject({ do: 'refuse' })
    expect(act(db('drill-x'))).toMatchObject({ do: 'refuse', why: expect.stringContaining('drills') })
    expect(act(db('drill-x', { metadata: { name: 'drill-x', namespace: 'shop', finalizers: [FINALIZER], annotations: { [DRILL]: 'true' } } }))).toMatchObject({ do: 'make' })
    expect(act(db('x', { spec: { parent: 'a', replicaOf: 'a' } }))).toMatchObject({ do: 'refuse', why: expect.stringContaining('not both') })
    expect(act(db('x', { spec: { at: '2026-12-01T00:00:00Z' } }))).toMatchObject({ do: 'refuse', why: expect.stringContaining('name the parent') })
    const p = db('orders', { made: true, pg: 18 }), c = db('c', { spec: { parent: 'orders', pgVersion: 19 } })
    expect(act(c, [p, c])).toMatchObject({ do: 'refuse', why: expect.stringContaining('of its parent\'s version (18)') })
  })
  test('nothing is made while a safekeeper replacement is open', () => {
    expect(act(db('orders'), undefined, 'safekeeper 2 is being replaced')).toEqual({ do: 'wait', why: 'new databases wait while a safekeeper replacement is open: safekeeper 2 is being replaced' })
  })
  test('an object the copy of older records is writing is left alone', () => {
    expect(act(db('orders', { metadata: { name: 'orders', namespace: 'shop', annotations: { [COPYING]: 'true' } } }))).toEqual({ do: 'none' })
  })
})

describe('kept as its spec says', () => {
  test('closed, then opened', () => {
    expect(act(db('orders', { made: true, spec: { closed: true } }))).toEqual({ do: 'close' })
    expect(act(db('orders', { made: true, spec: { closed: true }, status: { phase: 'Closed' } }))).toEqual({ do: 'none' })
    expect(act(db('orders', { made: true, spec: { closed: false }, status: { phase: 'Closed' } }))).toEqual({ do: 'open' })
  })
  test('a stop request is answered once', () => {
    expect(act(db('orders', { made: true, spec: { requests: { stop: '1' } } }))).toEqual({ do: 'stop', request: '1' })
    expect(act(db('orders', { made: true, spec: { requests: { stop: '1' } }, status: { answered: { stop: '1' } } }))).toEqual({ do: 'none' })
  })
  test('a version lowered is refused; raised, the database moves; a move under way goes on before anything else', () => {
    expect(act(db('orders', { made: true, pg: 19, spec: { pgVersion: 18 } }))).toMatchObject({ do: 'refuse', why: expect.stringContaining('cannot be moved back') })
    expect(act(db('orders', { made: true, pg: 18, spec: { pgVersion: 19 } }))).toEqual({ do: 'move' })
    expect(act(db('orders', { made: true, pg: 18, spec: { pgVersion: 19, closed: true }, status: { move: { state: 'copying' } } }))).toEqual({ do: 'move' })
    expect(act(db('orders', { made: true, pg: 18, spec: { requests: { rehearseMove: '19@now' } } }))).toEqual({ do: 'move' })
    expect(act(db('orders', { made: true, pg: 18, spec: { requests: { moveBack: 'b1' } } }))).toEqual({ do: 'moveBack', request: 'b1' })
  })
  test('a replica does not move, nor a primary with replicas on its timeline', () => {
    const primary = db('orders', { made: true, pg: 18, spec: { pgVersion: 19 } }), ro = db('orders-ro', { made: true, pg: 18, spec: { replicaOf: 'orders', pgVersion: 19 } })
    expect(act(primary, [primary, ro])).toMatchObject({ do: 'refuse', why: 'orders-ro is a replica on its timeline: delete it before the move, and make it again after' })
    expect(act(ro, [primary, ro])).toMatchObject({ do: 'refuse', why: expect.stringContaining('a replica runs its primary') })
  })
  test('a restore to a moment before its parent moved is refused, naming the old copy that holds that history', () => {
    const parent = db('orders', { made: true, pg: 19, status: { move: { state: 'done', to: 19, switchedAt: '2026-12-01T12:00:00Z', oldDatabase: 'orders-pg18' } } })
    const before = db('orders-then', { spec: { parent: 'orders', at: '2026-12-01T11:00:00Z' } })
    expect(act(before, [parent, before])).toEqual({ do: 'refuse', why: '2026-12-01T11:00:00Z is before orders moved to Postgres 19 (2026-12-01T12:00:00Z): restore from its old copy orders-pg18, which holds that history' })
    const after = db('orders-later', { spec: { parent: 'orders', at: '2026-12-01T13:00:00Z' } })
    expect(act(after, [parent, after])).toMatchObject({ do: 'make', kind: 'branch' })
  })
})

describe('deleted', () => {
  const gone = (o: DbObject) => ({ ...o, metadata: { ...o.metadata, deletionTimestamp: '2026-12-01T00:00:00Z' } })
  test('its data is held; a drill\'s goes at once; one never made has nothing to hold', () => {
    expect(act(gone(db('orders', { made: true })))).toEqual({ do: 'delete', holdData: true })
    const drill = db('drill-x', { made: true, metadata: { name: 'drill-x', namespace: 'shop', finalizers: [FINALIZER], annotations: { [DRILL]: 'true' } } })
    expect(act(gone(drill))).toEqual({ do: 'delete', holdData: false })
    expect(act(gone(db('orders')))).toEqual({ do: 'delete', holdData: false })
    // the annotation alone is not a drill: a person who adds it to their own database still gets the hold
    const marked = db('orders', { made: true, metadata: { name: 'orders', namespace: 'shop', finalizers: [FINALIZER], annotations: { [DRILL]: 'true' } } })
    expect(act(gone(marked))).toEqual({ do: 'delete', holdData: true })
    // a status that lost its record, with the platform's record there: held all the same
    expect(databaseAction(gone(db('orders')), [], { replacementOpen: null, records: new Map([['orders', { namespace: 'shop' }]]) })).toEqual({ do: 'delete', holdData: true })
  })
  test('a root waits for what was made from it', () => {
    const root = gone(db('orders', { made: true })), branch = db('orders-dev', { made: true, spec: { parent: 'orders' } })
    expect(act(root, [root, branch])).toEqual({ do: 'wait', why: 'deleted, waiting for orders-dev (made from it) to be deleted first' })
    expect(act(root, [root, gone(branch)])).toEqual({ do: 'delete', holdData: true })
  })
  test('without the finalizer, nothing is left to do', () => {
    const o = gone(db('orders', { made: true }))
    o.metadata.finalizers = []
    expect(act(o)).toEqual({ do: 'none' })
  })
})

describe('the hold', () => {
  const now = new Date('2026-12-01T00:00:00Z')
  test('days, hours or never', () => {
    expect(deleteAfter('7d', now)?.toISOString()).toBe('2026-12-08T00:00:00.000Z')
    expect(deleteAfter('36h', now)?.toISOString()).toBe('2026-12-02T12:00:00.000Z')
    expect(deleteAfter('never', now)).toBeNull()
    expect(() => deleteAfter('a week', now)).toThrow('duration')
  })
  test('the data goes when its time has come, a root\'s only once nothing uses its tenant, and never when the hold is never', () => {
    expect(purgeDue({ deleteAfter: '2026-11-30T00:00:00Z', tenant: 't', root: false }, now, true)).toBe(true)
    expect(purgeDue({ deleteAfter: '2026-12-02T00:00:00Z', tenant: 't', root: false }, now, false)).toBe(false)
    expect(purgeDue({ deleteAfter: '2026-11-30T00:00:00Z', tenant: 't', root: true }, now, true)).toBe(false)
    expect(purgeDue({ deleteAfter: '2026-11-30T00:00:00Z', tenant: 't', root: true }, now, false)).toBe(true)
    expect(purgeDue({ deleteAfter: 'never', tenant: 't', root: false }, now, false)).toBe(false)
  })
})

describe('closed', () => {
  test('closed once, and closed again when a compute started as it closed (a start that read the database just before)', () => {
    expect(act(db('orders', { made: true, spec: { closed: true } }))).toEqual({ do: 'close' })
    expect(act(db('orders', { made: true, spec: { closed: true }, status: { phase: 'Closed' } }))).toEqual({ do: 'none' })
    expect(act(db('orders', { made: true, spec: { closed: true }, status: { phase: 'Closed', awake: true } }))).toEqual({ do: 'close' })
    expect(act(db('orders', { made: true, spec: { closed: false }, status: { phase: 'Closed' } }))).toEqual({ do: 'open' })
  })
})
