import { describe, expect, test } from 'bun:test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { ancestorFor, olderTimelineFor, ownMemberProbe, recoverDecision, recoverStep, recoveryPoint, recoveryScope, timelineOwnMembers, unfinishedRepoint, type MemberProbe, type RecoveryMark, type TimelineProbe } from '../src/storage-spec'

/**
 * A database recovered under its own name after every safekeeper lost its data (`POST /db/<name>/recover`): the decisions are pure and judged here with literal rows; the calls (the storage controller, the pageserver,
 * the records) run only against a cluster and are for the hand test, so what is pinned for them is what the source shows (their order, and that nothing is deleted).
 */
const control = readFileSync(join(import.meta.dir, '../src/control.ts'), 'utf8')
const probe = (id: number, p: TimelineProbe, active = true): MemberProbe => ({ id, active, probe: p })
const three = [probe(7, 'unknown'), probe(8, 'unknown'), probe(9, 'unknown')]
const base = { mode: 'primary', probes: three, replacementOpen: null }

describe('when a database may be recovered', () => {
  test('only when no member knows the timeline, every member answered, and three Active ones are there', () => {
    expect(recoverDecision(base)).toMatchObject({ allow: true })
  })

  test('it refuses when it is not needed: a member that knows the timeline means the database starts as it is', () => {
    const needed = recoverDecision({ ...base, probes: [probe(7, 'knows'), probe(8, 'unknown'), probe(9, 'unknown')] })
    expect(needed.allow).toBe(false)
    expect(needed.why).toContain('safekeeper 7 still knows')
    // a Paused member that knows it counts too
    expect(recoverDecision({ ...base, probes: [...three, probe(4, 'knows', false)] }).allow).toBe(false)
  })

  test('a member that does not answer may hold the timeline, so nothing is recovered while one is silent', () => {
    expect(recoverDecision({ ...base, probes: [probe(7, 'silent'), probe(8, 'unknown'), probe(9, 'unknown')] }).why).toContain('does not answer')
    expect(recoverDecision({ ...base, probes: [...three, probe(4, 'silent', false)] }).allow).toBe(false)
  })

  test('a new timeline needs three Active members that answer (the seeding after a total loss makes them)', () => {
    expect(recoverDecision({ ...base, probes: [probe(7, 'unknown'), probe(8, 'unknown')] }).why).toContain('2 Active')
    expect(recoverDecision({ ...base, probes: [probe(7, 'unknown'), probe(8, 'unknown'), probe(9, 'unknown', false)] }).allow).toBe(false)
    expect(recoverDecision({ ...base, probes: [] }).allow).toBe(false)
  })

  test('a replica follows its primary, and an open replacement could give the new timeline a dead member', () => {
    expect(recoverDecision({ ...base, mode: 'replica' }).why).toContain('replica')
    expect(recoverDecision({ ...base, replacementOpen: 'the replacement of safekeeper 3 is not closed' }).why).toContain('replacement is open')
  })
})

describe('where the new timeline branches off', () => {
  test('the pageserver`s last uploaded record, never what it holds only in memory', () => {
    expect(recoveryPoint({ remote_consistent_lsn: '0/1000000', remote_consistent_lsn_visible: '0/1000000', last_record_lsn: '0/1100000' })).toEqual({ ok: true, lsn: '0/1000000', unuploadedBytes: 0x100000 })
  })

  test('an idle timeline reads 0/0 in remote_consistent_lsn: the visible one is used, and either alone when the other is missing', () => {
    expect(recoveryPoint({ remote_consistent_lsn: '0/0', remote_consistent_lsn_visible: '1/2000', last_record_lsn: '1/2000' })).toEqual({ ok: true, lsn: '1/2000', unuploadedBytes: 0 })
    expect(recoveryPoint({ remote_consistent_lsn: '1/4000', last_record_lsn: '1/4000' })).toMatchObject({ ok: true, lsn: '1/4000' })
  })

  test('positions are compared as numbers, not as text: 9/0 is behind 10/0', () => {
    expect(recoveryPoint({ remote_consistent_lsn_visible: '9/0', last_record_lsn: '10/0' })).toEqual({ ok: true, lsn: '9/0', unuploadedBytes: 0x100000000 * 7 })
  })

  test('nothing uploaded, or unreadable: refused, never a guess', () => {
    expect(recoveryPoint({ remote_consistent_lsn: '0/0', remote_consistent_lsn_visible: '0/0', last_record_lsn: '0/5000' }).ok).toBe(false)
    expect(recoveryPoint({}).ok).toBe(false)
    expect(recoveryPoint({ remote_consistent_lsn: 'garbage', last_record_lsn: '0/5000' }).ok).toBe(false)
  })

  test('not behind the timeline`s own branch point (the pageserver refuses a branch there)', () => {
    expect(recoveryPoint({ remote_consistent_lsn: '0/1000', last_record_lsn: '0/9000', ancestor_lsn: '0/2000' }).ok).toBe(false)
    expect(recoveryPoint({ remote_consistent_lsn: '0/2000', last_record_lsn: '0/9000', ancestor_lsn: '0/2000' }).ok).toBe(true)
  })
})

describe('what moves and what is left', () => {
  const rec = (name: string, tenant: string, timeline: string, mode = 'primary') => ({ name, tenant, timeline, mode })
  const records = [rec('app', 't1', 'a'), rec('app-replica', 't1', 'a', 'replica'), rec('app-child', 't1', 'c'), rec('app-sib', 't1', 'd'), rec('other', 't2', 'a'), rec('child-replica', 't1', 'c', 'replica')]

  test('the database and its replicas move together; the tenant`s other databases (its child branches among them) keep their own timelines and need their own calls', () => {
    expect(recoveryScope(records, records[0]!)).toEqual({ repoint: ['app', 'app-replica'], others: ['app-child', 'app-sib'] })
    expect(recoveryScope(records, records[2]!)).toEqual({ repoint: ['app-child', 'child-replica'], others: ['app', 'app-sib'] })
  })

  test('a database of another tenant with the same timeline id is never touched, and a replica listed first still moves after its primary', () => {
    expect(recoveryScope(records, records[0]!).repoint).not.toContain('other')
    expect(recoveryScope([records[1]!, records[0]!], records[0]!).repoint).toEqual(['app', 'app-replica'])
  })

  test('a rerun after a stop half way adopts the new timeline it finds and never makes it twice', () => {
    expect(recoverStep('absent')).toEqual({ do: 'create' })
    expect(recoverStep('ours')).toEqual({ do: 'adopt' })
    expect(recoverStep('foreign').do).toBe('refuse')
  })
})

describe('the route in the control layer', () => {
  const fn = control.slice(control.indexOf('async function recoverDatabase'), control.indexOf('// ---- what the gateway copy reads'))

  test('it is an operator route: no caller class lists it, so only the pod`s own loopback reaches it', () => {
    const routes = control.slice(control.indexOf('const CALLER_ROUTES'), control.indexOf('let callerCache'))
    expect(routes).not.toContain('recover')
    expect(control).toContain('const rec = path.match(/^\\/db\\/([a-z][a-z0-9-]{0,30})\\/recover$/)')
  })

  test('it asks the decision before it changes anything, and a dry run changes nothing', () => {
    expect(fn.indexOf('recoverDecision(')).toBeGreaterThan(0)
    // (the one stop before the decision is the finishing of a call that stopped half way, which a dry run does not reach)
    const stopped = fn.indexOf('for (const n of scope.repoint) await stopCompute(')
    expect(stopped).toBeGreaterThan(0)
    expect(fn.indexOf('recoverDecision(')).toBeLessThan(stopped)
    expect(fn.indexOf('if (dry) return { status: 200, body: { dry: true, ...preview')).toBeLessThan(stopped)
    expect(fn.indexOf('if (dry) return { status: 200, body: { dry: true, finishing')).toBeLessThan(fn.indexOf('await stopCompute(n, true); const r'))
  })

  test('the new timeline is a branch of the old one at the uploaded record, and the records move only after it exists', () => {
    expect(fn).toContain('ancestor_timeline_id: d.timeline, ancestor_start_lsn: lsn')
    expect(fn.indexOf("'create the recovered timeline'")).toBeGreaterThan(0)
    expect(fn.indexOf("'create the recovered timeline'")).toBeLessThan(fn.indexOf('saveDb({ ...r, timeline: newTimeline })'))
    // the record keeps its name, port and login (the route, the certificate and the Service are keyed by them): only the timeline changes
    expect(fn).toContain('saveDb({ ...r, timeline: newTimeline })')
  })

  test('it deletes nothing: no delete call, no database delete, no store client', () => {
    expect(fn).not.toMatch(/\bdel\(|'DELETE'|deleteDb|s3\(\)/)
  })

  test('its decisions come from the shared definitions the control layer already imports from', () => {
    expect(control).toContain("import { ancestorFor, olderTimelineFor, ownMemberProbe, recoverDecision, recoverStep, recoveryPoint, recoveryScope, timelineOwnMembers, unfinishedRepoint, type MemberProbe, type RecoveryMark } from './storage-spec'")
  })

  test('a real call is for the acting control layer only (409 naming the lease holder, as a move does), a dry run is not', () => {
    const at = control.indexOf('const rec = path.match')
    const route = control.slice(at, at + 900)
    expect(route).toContain("if (!isLeader && url.searchParams.get('dry') !== '1')")
    expect(route).toContain('peat-control-leader')
    expect(route).toContain('holderIdentity')
    expect(route.indexOf('isLeader')).toBeLessThan(route.indexOf('recoverDatabase('))
  })

  test('a mark made by another call decides the timeline and the position, so one recovery never makes two timelines', () => {
    expect(fn).toContain('if (made.status === 409)')
    expect(fn.indexOf('if (made.status === 409)')).toBeLessThan(fn.indexOf("'create the recovered timeline'"))
  })

  test('a stop half way is finished before anything is decided, and the timeline`s own members are asked as well as the listed ones', () => {
    expect(fn.indexOf('unfinishedRepoint(')).toBeGreaterThan(0)
    expect(fn.indexOf('unfinishedRepoint(')).toBeLessThan(fn.indexOf('recoverDecision('))
    expect(fn.indexOf('timelineOwnMembers(')).toBeGreaterThan(0)
    expect(fn.indexOf('timelineOwnMembers(')).toBeLessThan(fn.indexOf('recoverDecision('))
    expect(fn).toContain('ownMemberProbe(')
  })

  test('a branch or restore through the name from before the recovery point is taken from the timeline the recovery kept', () => {
    const branch = control.slice(control.indexOf('async function makeBranch'), control.indexOf('async function makeReplica'))
    expect(branch).toContain('ancestorFor(recoveries, parent.timeline, at)')
    expect(branch).toContain('olderTimelineFor(recoveries, ancestor, j.kind)')
    expect(branch).toContain('ancestor_timeline_id: ancestor')
    expect(branch).not.toContain('ancestor_timeline_id: parent.timeline')
  })
})

describe('the timeline`s own members', () => {
  const spec = ['safekeeper-w3-0.safekeeper.peat.svc.cluster.local:5454', 'safekeeper-w5-0.safekeeper.peat.svc.cluster.local:5454', 'safekeeper-w6-0.safekeeper.peat.svc.cluster.local:5454']
  test('those the list of members that are not decommissioned does not hold are asked too', () => {
    expect(timelineOwnMembers(spec, ['safekeeper-w7-0.safekeeper.peat.svc.cluster.local', 'safekeeper-w8-0.safekeeper.peat.svc.cluster.local'])).toEqual([
      { id: 3, host: 'safekeeper-w3-0.safekeeper.peat.svc.cluster.local' }, { id: 5, host: 'safekeeper-w5-0.safekeeper.peat.svc.cluster.local' }, { id: 6, host: 'safekeeper-w6-0.safekeeper.peat.svc.cluster.local' },
    ])
  })
  test('a member already listed is not asked twice, a host that is not a safekeeper`s is ignored, and an empty spec asks nobody', () => {
    expect(timelineOwnMembers([...spec, 'elsewhere.example:5454'], ['safekeeper-w3-0.safekeeper.peat.svc.cluster.local', 'safekeeper-w5-0.safekeeper.peat.svc.cluster.local:7676']).map((m) => m.id)).toEqual([6])
    expect(timelineOwnMembers([], [])).toEqual([])
  })
  test('one that answers decides, one with a pod that is silent blocks, one whose pod is gone cannot be asked (every old member after a total loss)', () => {
    expect(ownMemberProbe({ status: 200, podExists: true })).toBe('knows')
    expect(ownMemberProbe({ status: 404, podExists: true })).toBe('unknown')
    expect(ownMemberProbe({ status: 500, podExists: true })).toBe('silent')
    expect(ownMemberProbe({ status: null, podExists: true })).toBe('silent')
    expect(ownMemberProbe({ status: null, podExists: false })).toBe('unknown')
    // and the decision refuses on the first two kinds
    const base = { mode: 'primary', replacementOpen: null }
    const three = [probe(7, 'unknown'), probe(8, 'unknown'), probe(9, 'unknown')]
    expect(recoverDecision({ ...base, probes: [...three, { id: 3, active: false, probe: ownMemberProbe({ status: 200, podExists: true }) }] }).allow).toBe(false)
    expect(recoverDecision({ ...base, probes: [...three, { id: 3, active: false, probe: ownMemberProbe({ status: null, podExists: true }) }] }).allow).toBe(false)
    expect(recoverDecision({ ...base, probes: [...three, { id: 3, active: false, probe: ownMemberProbe({ status: null, podExists: false }) }] }).allow).toBe(true)
  })
})

describe('a recovery that stopped half way, and a restore from before it', () => {
  const mark = (to: string, from: string, lsn: string, tenant = 't1'): RecoveryMark => ({ name: `recovered-${from}`, tenant, from, to, lsn, state: 'started' })
  const rec = (name: string, timeline: string, mode = 'primary', tenant = 't1') => ({ name, tenant, timeline, mode })

  test('the primary already on the new timeline and a replica still on the old one: the mark finishes it', () => {
    const records = [rec('app', 'new'), rec('app-replica', 'old', 'replica'), rec('app-child', 'child')]
    expect(unfinishedRepoint([mark('new', 'old', '0/1000')], records, records[0]!)).toEqual({ mark: mark('new', 'old', '0/1000'), names: ['app-replica'] })
    // asked about the replica that was left behind, its own timeline is the old one: nothing names it as a `to`
    expect(unfinishedRepoint([mark('new', 'old', '0/1000')], records, records[1]!)).toBeNull()
  })
  test('nothing is finished when every record is already on the new timeline, for another tenant`s mark, or with no mark', () => {
    const done = [rec('app', 'new'), rec('app-replica', 'new', 'replica')]
    expect(unfinishedRepoint([mark('new', 'old', '0/1000')], done, done[0]!)).toBeNull()
    expect(unfinishedRepoint([mark('new', 'old', '0/1000', 't2')], [rec('app', 'new'), rec('r', 'old', 'replica')], rec('app', 'new'))).toBeNull()
    expect(unfinishedRepoint([], done, done[0]!)).toBeNull()
  })

  test('a position before the recovery point is taken from the old timeline, at or after it from the new one, and back along two recoveries', () => {
    const marks = [mark('c', 'b', '0/3000'), mark('b', 'a', '0/2000')]
    expect(ancestorFor(marks, 'c', '0/3000')).toBe('c')
    expect(ancestorFor(marks, 'c', '0/4000')).toBe('c')
    expect(ancestorFor(marks, 'c', '0/2800')).toBe('b')
    expect(ancestorFor(marks, 'c', '0/2000')).toBe('b')
    expect(ancestorFor(marks, 'c', '0/1000')).toBe('a')
    expect(ancestorFor([], 'c', '0/1000')).toBe('c')
    expect(ancestorFor(marks, 'c', 'garbage')).toBe('c')
    expect(ancestorFor(marks, 'x', '0/1000')).toBe('x')
  })
  test('a moment before a timeline`s records (past, or nodata) is looked for on the older timeline; any other answer is not', () => {
    const marks = [mark('c', 'b', '0/3000')]
    expect(olderTimelineFor(marks, 'c', 'past')).toBe('b')
    expect(olderTimelineFor(marks, 'c', 'nodata')).toBe('b')
    expect(olderTimelineFor(marks, 'c', 'future')).toBeNull()
    expect(olderTimelineFor(marks, 'c', 'present')).toBeNull()
    expect(olderTimelineFor(marks, 'b', 'past')).toBeNull()
  })
})
