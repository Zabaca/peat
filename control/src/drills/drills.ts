// The drills of Peat, each ONE run. A drill is a function of a `Ctx`: it reads through the context, changes things only through `ctx.g`
// (the guard), and records what it saw. Nothing here names a command, a machine of its own choosing outside the platform's, or a path:
// `guard.ts` is the door. The clients connect through the front door with TLS (`verify-full`), the reader is a HEAP scan (`select count(*)`
// is an index-only scan the compute keeps in memory and never asks the pageserver), "the store unreachable" is a NetworkPolicy taken off
// the storage pods' egress (the store itself is never touched), a machine cut is the installer's own command (as a power cut does) and is
// started again by the guard's undo log, and a lost safekeeper is replaced by the control layer's own automation (`/sk-replace`).
import { DB, FILLER_BYTES, FILLER_ROWS, READER_EXPECT, analyzeReader, analyzeWriter, type Ctx } from './lib'
import { RESTORED_CLUSTER } from './guard'

export interface Opts { want?: 'with-gateway' | 'without-gateway'; gb?: number }
export interface DrillSpec {
  name: string
  group: 'setup' | 'pod' | 'machine' | 'store' | 'wipe' | 'restore' | 'alert' | 'uplink'
  title: string
  /** Wall-clock minutes the drill takes (an estimate, from earlier runs and the holds below). */
  minutes: number
  /** Cuts a machine, the store or loads the uplink: must be clear of the install's quiet hours. */
  clear: boolean
  machineCut: boolean
  /** The alerts this drill is testing (an alert drill: they are left out of the silence, seen active in Alertmanager and, unless `notify`, silenced then) and whether the tested alert is let through to the install's receivers. */
  alerts?: { tested: string[]; notify: boolean }
  run(c: Ctx, o: Opts): Promise<void>
}

/** The wipes and the restore wipe whole volumes, losing whatever has not reached the store, of EVERY database on the platform: they refuse while the control layer lists one that is not a drill's own. (The drills delete their own databases through the control layer, and the storage then deletes those timelines' objects in the store; no other database's object is touched.) */
async function onlyDrillDatabases(c: Ctx): Promise<void> {
  const dbs = ((await c.g.control('GET', '/dbs')).json ?? []) as { name: string }[]
  const others = dbs.map((d) => d.name).filter((n) => !n.startsWith('drill'))
  if (others.length) throw new Error(`the platform holds databases that are not the drills' own (${others.join(', ')}): this drill wipes volumes and would lose what has not reached the store. Nothing was changed.`)
}
/** The recovery drill loses every safekeeper's copy of EVERY database's timeline, so it runs only while everything on the platform can be recreated: the drills' own, and the names the install declares recreatable (`PEAT_DRILL_RECREATABLE`). */
async function onlyRecreatable(c: Ctx): Promise<string[]> {
  const dbs = ((await c.g.control('GET', '/dbs')).json ?? []) as { name: string }[]
  const others = dbs.map((d) => d.name).filter((n) => !c.g.settings.recreatable.test(n))
  if (others.length) throw new Error(`the platform holds databases that are not known to be recreatable (${others.join(', ')}): this drill loses every safekeeper's copy of every timeline. Nothing was changed.`)
  return dbs.map((d) => d.name)
}
const ps = (n: number) => `pageserver-w${n}-0`
const sk = (n: number) => `safekeeper-w${n}-0`

/** Where everything a drill cuts is now: the database's tenant and its pageservers (by id, and the machine of the attached one), its compute, the acting control layer, the gateways, the storage controller and the platform's machines. */
interface Info { tenant: string; timeline: string; attached: number; secondary: number[]; attachedMachine: string; computeNode: string; leaderNode: string; gatewayNodes: string[]; controllerNode: string; machines: string[] }
async function gather(c: Ctx, db = DB): Promise<Info> {
  const n = c.g.names
  const shape = await c.g.platform()
  const { tenant, timeline } = await c.dbInfo(db)
  const p = await c.placement(tenant)
  const comp = (await c.pods(n.computeNamespace, `app=peat-compute,compute=${db}`))[0]
  const leader = await c.controlLeader()
  const gws = await c.pods(n.gatewayNamespace, 'app=peat-gateway')
  const ctl = (await c.pods(n.namespace, 'app=storage-controller'))[0]
  const attachedMachine = shape.pageservers.find((x) => x.id === p.attached)?.machine ?? ''
  return { tenant, timeline, attached: p.attached, secondary: p.secondary, attachedMachine, computeNode: comp?.node ?? 'none', leaderNode: leader.node, gatewayNodes: gws.map((g) => g.node), controllerNode: ctl?.node ?? '', machines: shape.machines }
}
/** The machine the drill's clients run on: the storage controller's (which holds no pageserver where the platform keeps them apart), else the first platform machine. */
const clientMachine = (i: Info): string => (i.controllerNode && i.machines.includes(i.controllerNode) ? i.controllerNode : i.machines[0]!)
/** A machine for the clients that is not the one being cut: the first of `prefer` that is a platform machine, then any other. */
const notCut = (i: Info, cut: string, ...prefer: string[]): string => {
  const m = [...prefer, ...i.machines].find((x) => x && x !== cut && i.machines.includes(x))
  if (!m) throw new Error(`no platform machine but ${cut} for the clients: a machine cut needs at least two machines`)
  return m
}

/** One failure drill: a writer committing a row a second and a reader scanning the heap each second through the front door, one fault, then the numbers from their own logs and the table. */
async function failureDrill(c: Ctx, o: { clientNode: (i: Info) => string; fault: (i: Info, faultAt: number) => Promise<void>; settle?: number; note?: (i: Info) => string }): Promise<void> {
  const i = await gather(c)
  const client = o.clientNode(i)
  c.note(`attached pageserver ${i.attached}, secondary ${i.secondary.join(',') || 'none'}; compute on ${i.computeNode}; acting control layer on ${i.leaderNode}; gateways on ${i.gatewayNodes.join(',')}; clients on ${client}${o.note ? `; ${o.note(i)}` : ''}`)
  await c.startWriter(client)
  await c.startReader(client)
  await c.sleep(25)
  const faultAt = await c.clientClock()
  c.note(`fault at ${faultAt} (the writer's clock)`)
  await o.fault(i, faultAt)
  c.note(`released; settling ${o.settle ?? 45} s`)
  await c.sleep(o.settle ?? 45)
  const wlog = await c.clientLog('drill-writer'), rlog = await c.clientLog('drill-reader')
  const rows = await c.sqlRetry(DB, 'select n from proof_rows order by n', 300)
  await c.stopClients()
  if (rows === null) c.note('THE TABLE COULD NOT BE READ AFTER 5 MINUTES')
  const w = analyzeWriter(wlog, rows ?? '', faultAt), r = analyzeReader(rlog, faultAt)
  c.record('acknowledged commits before the fault / acknowledged rows missing at the end', `${w.ackedBeforeFault} / ${w.missingAcked}`, w.missingAcked ? `FIRST MISSING ${w.missingFirst.join(',')}` : '')
  c.record('writer: longest gap between acknowledged commits (s); began / ended after the fault (ms)', `${w.longestGapS}; ${w.gapBeganMsAfterFault} / ${w.firstOkAfterGapEndedMsAfterFault}`, `${w.errsFromFault} errors ${JSON.stringify(w.errMessages)}${w.duplicates.length ? ` DUPLICATES ${w.duplicates}` : ''}`)
  c.record('reader (heap scan): longest gap (s), errors, wrong counts', `${r.longestGapS}; ${r.errsFromFault} errors; ${r.wrongCounts} wrong`, `gap began ${r.gapBeganMsAfterFault} ms after the fault, ended ${r.gapEndedMsAfterFault} ${JSON.stringify(r.errMessages)}`)
}

// ---- pod cuts

const podDrills: DrillSpec[] = [
  {
    name: 'ps-quick', group: 'pod', title: 'the attached pageserver killed (no grace period); its StatefulSet brings it back', minutes: 5, clear: false, machineCut: false,
    run: (c) => failureDrill(c, { clientNode: clientMachine, fault: async (i) => { await c.g.killPod(c.g.names.namespace, ps(i.attached)) } }),
  },
  {
    name: 'ps-hold', group: 'pod', title: 'the attached pageserver killed and held down 240 s: does the controller fail over to the secondary, and how', minutes: 9, clear: false, machineCut: false,
    run: (c) => failureDrill(c, { clientNode: clientMachine, fault: async (i) => { await c.g.killPod(c.g.names.namespace, ps(i.attached)); await c.g.holdDown(c.g.names.namespace, 'statefulset', `pageserver-w${i.attached}`, 240); c.note(`attached/secondary at the end of the hold: ${JSON.stringify(await c.placement(i.tenant))}`) } }),
  },
  {
    name: 'ps-warm', group: 'alert', title: 'the attached pageserver killed, killed again 20 s after (it is warming up) and held 240 s: the 300 s grace; PeatNodeWarmingUpTooLong', minutes: 10, clear: false, machineCut: false,
    alerts: { tested: ['PeatNodeWarmingUpTooLong'], notify: false },
    run: (c) => failureDrill(c, {
      clientNode: clientMachine,
      fault: async (i) => {
        await c.g.killPod(c.g.names.namespace, ps(i.attached)); await c.sleep(20)
        c.note(`second kill at ${await c.clientClock()} (the writer's clock), 20 s after the first`)
        await c.g.killPod(c.g.names.namespace, ps(i.attached)); await c.g.holdDown(c.g.names.namespace, 'statefulset', `pageserver-w${i.attached}`, 240)
        c.note(`attached/secondary at the end of the hold: ${JSON.stringify(await c.placement(i.tenant))}`)
      },
    }),
  },
  {
    name: 'ps-cold', group: 'pod', title: 'the attached pageserver killed and held 240 s AND the compute stopped 3 s later: a cold start through the gateway needs a pageserver', minutes: 9, clear: false, machineCut: false,
    run: (c) => failureDrill(c, {
      clientNode: clientMachine,
      fault: async (i) => { await c.g.killPod(c.g.names.namespace, ps(i.attached)); await c.sleep(3); await c.g.control('POST', `/db/${DB}/stop`); await c.g.holdDown(c.g.names.namespace, 'statefulset', `pageserver-w${i.attached}`, 240); c.note(`attached/secondary at the end of the hold: ${JSON.stringify(await c.placement(i.tenant))}`) },
    }),
  },
  {
    name: 'sk1', group: 'pod', title: 'one safekeeper killed and held 60 s', minutes: 5, clear: false, machineCut: false,
    run: (c) => failureDrill(c, { clientNode: clientMachine, fault: async () => { const x = (await c.safekeepers())[1]!; await c.g.killPod(c.g.names.namespace, sk(x.id)); await c.g.holdDown(c.g.names.namespace, 'statefulset', `safekeeper-w${x.id}`, 60) } }),
  },
  {
    name: 'sk2', group: 'pod', title: 'two safekeepers killed and held 60 s (no quorum)', minutes: 5, clear: false, machineCut: false,
    run: (c) => failureDrill(c, {
      clientNode: clientMachine,
      fault: async () => {
        const sks = await c.safekeepers(); const a = sks[0]!, b = sks[1]!
        await c.g.killPod(c.g.names.namespace, sk(a.id)); await c.g.killPod(c.g.names.namespace, sk(b.id))
        await c.g.scale(c.g.names.namespace, 'statefulset', `safekeeper-w${a.id}`, 0); await c.g.scale(c.g.names.namespace, 'statefulset', `safekeeper-w${b.id}`, 0)
        await c.sleep(60)
        await c.g.release(c.g.names.namespace, 'statefulset', `safekeeper-w${a.id}`); await c.g.release(c.g.names.namespace, 'statefulset', `safekeeper-w${b.id}`)
      },
    }),
  },
  {
    name: 'ctl', group: 'pod', title: 'the storage controller killed; its Deployment brings it back', minutes: 4, clear: false, machineCut: false,
    run: (c) => failureDrill(c, { clientNode: clientMachine, fault: async () => { const p = (await c.pods(c.g.names.namespace, 'app=storage-controller'))[0]!; await c.g.killPod(c.g.names.namespace, p.name) } }),
  },
  {
    name: 'broker', group: 'pod', title: 'the storage broker killed; its Deployment brings it back', minutes: 4, clear: false, machineCut: false,
    run: (c) => failureDrill(c, { clientNode: clientMachine, fault: async () => { const p = (await c.pods(c.g.names.namespace, 'app=storage-broker'))[0]!; await c.g.killPod(c.g.names.namespace, p.name) } }),
  },
  {
    name: 'gw-kill', group: 'pod', title: 'one gateway killed while the writer commits through the front door', minutes: 4, clear: false, machineCut: false,
    run: (c) => failureDrill(c, { clientNode: clientMachine, settle: 40, fault: async () => { const g = (await c.pods(c.g.names.gatewayNamespace, 'app=peat-gateway'))[0]!; c.note(`killing ${g.name} on ${g.node}`); await c.g.killPod(c.g.names.gatewayNamespace, g.name) } }),
  },
  {
    name: 'crash', group: 'pod', title: 'a plain crash: the database\'s compute pod killed without warning', minutes: 4, clear: false, machineCut: false,
    run: (c) => failureDrill(c, { clientNode: clientMachine, fault: async () => { const p = (await c.pods(c.g.names.computeNamespace, `app=peat-compute,compute=${DB}`))[0]!; await c.g.killPod(c.g.names.computeNamespace, p.name) } }),
  },
]

// ---- sleep with two gateways, and a compute that is cut off but not dead

/** The full-sleep time of a database, shortened for these two drills: both the control layer and the gateways read IDLE_MINUTES; the guard restores it at the end. */
async function shortIdle(c: Ctx, minutes: string): Promise<void> {
  await c.g.patchEnv(c.g.names.namespace, 'deployment', 'peat-control', { name: 'IDLE_MINUTES', value: minutes })
  await c.g.patchEnv(c.g.names.gatewayNamespace, 'deployment', 'peat-gateway', { name: 'IDLE_MINUTES', value: minutes })
  await c.g.rolloutStatus(c.g.names.namespace, 'deployment', 'peat-control'); await c.g.rolloutStatus(c.g.names.gatewayNamespace, 'deployment', 'peat-gateway')
  await c.sleep(20)
}
async function awakeComputes(c: Ctx, db: string): Promise<number> { return (await c.pods(c.g.names.computeNamespace, `app=peat-compute,compute=${db}`)).length }

const sleepDrills: DrillSpec[] = [
  {
    name: 'gw-sleep', group: 'pod', title: 'with two gateways an idle database is put to sleep by one gateway only, and woken by a connection (idle time set to 2 minutes for the drill)', minutes: 15, clear: false, machineCut: false,
    async run(c) {
      const D = 'drill-sleep'
      await shortIdle(c, '2')
      await c.createDb(D); await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${D}`, 120)
      await c.sql(D, "select 'awake', count(*) from pg_class")
      const t0 = c.g.now()
      while ((await awakeComputes(c, D)) > 0) { if (c.g.now() - t0 > 7 * 60_000) { c.record('asleep after idle', 'NO: still awake after 7 minutes'); break } await c.sleep(5) }
      if ((await awakeComputes(c, D)) === 0) c.record('asleep after an idle time of 2 min (s)', String(Math.round((c.g.now() - t0) / 1000)))
      for (const g of await c.pods(c.g.names.gatewayNamespace, 'app=peat-gateway')) c.note(`${g.name}: ${(await c.g.kread(['-n', c.g.names.gatewayNamespace, 'logs', g.name, '--tail=200'])).split('\n').filter((l) => /full sleep|refused|409/.test(l)).slice(-3).join(' | ')}`)
      const w0 = c.g.now()
      const woke = await c.sqlRetry(D, "select 'woken', count(*) from pg_class", 120)
      c.record('woken by a connection (s)', woke === null ? 'NOT WOKEN' : String(Math.round((c.g.now() - w0) / 100) / 10), `computes awake now ${await awakeComputes(c, D)}`)
      await c.g.control('DELETE', `/db/${D}`)
    },
  },
  {
    name: 'gw-sleep-b', group: 'pod', title: 'a session open on the gateway that does NOT hold the sleep lease keeps the database awake against the other\'s decision', minutes: 10, clear: false, machineCut: false,
    async run(c) {
      const D = 'drill-sleep'
      await shortIdle(c, '2')
      await c.createDb(D); await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${D}`, 120)
      await c.sql(D, 'select 1')
      const lease = await c.g.kjson<any>(['-n', c.g.names.namespace, 'get', 'lease', 'peat-gateway-sleep']).catch(() => null)
      const holder = lease?.spec?.holderIdentity as string | undefined
      const gws = await c.pods(c.g.names.gatewayNamespace, 'app=peat-gateway')
      if (gws.length < 2) throw new Error(`${gws.length} gateway(s): this drill needs two`)
      const other = gws.find((g) => g.name !== holder) ?? gws[1]!
      c.note(`the sleep lease is held by ${holder ?? 'nobody'}; the session goes to ${other.name} (${other.ip})`)
      await c.startClientPod('drill-session', clientMachine(await gather(c)), D, `psql -qAtX -c "select 'session on the other gateway survived', pg_sleep(270)"`, { PGHOSTADDR: other.ip, PGCONNECT_TIMEOUT: '60' })
      await c.sleep(285)
      const n = await awakeComputes(c, D)
      c.record('computes awake after 285 s with the session open on the other gateway', String(n), n === 1 ? 'AWAKE as it must be' : 'ASLEEP: the session did not keep it awake')
      c.note(`session pod log: ${(await c.clientLog('drill-session')).trim().slice(0, 200)}`)
      await c.stopClients(); await c.g.control('DELETE', `/db/${D}`)
    },
  },
  {
    name: 'fence', group: 'pod', title: 'a compute that is cut off but not dead: its pod is released from its Deployment, a second compute is made, and both are written to', minutes: 6, clear: false, machineCut: false,
    async run(c) {
      await c.sql(DB, 'create table if not exists fence_rows (id bigserial primary key, src text not null, seq int not null)'); await c.sql(DB, 'delete from fence_rows')
      const old = (await c.pods(c.g.names.computeNamespace, `app=peat-compute,compute=${DB}`))[0]!
      const clients = clientMachine(await gather(c))
      // the two writers: one through the front door, one straight at the old compute's address (a policy admits the drill's clients to the computes' 5432 for this drill alone)
      const p = (name: string, from: string, to: string) => ({ apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy', metadata: { name, namespace: c.g.names.computeNamespace }, spec: { podSelector: { matchLabels: { app: from } }, policyTypes: [to === 'in' ? 'Ingress' : 'Egress'], ...(to === 'in' ? { ingress: [{ from: [{ podSelector: { matchLabels: { app: 'drill-client' } } }], ports: [{ protocol: 'TCP', port: 5432 }] }] } : { egress: [{ to: [{ podSelector: { matchLabels: { app: 'peat-compute' } } }], ports: [{ protocol: 'TCP', port: 5432 }] }] }) } })
      await c.g.apply(c.g.names.computeNamespace, p('drill-fence-into-computes', 'peat-compute', 'in')); await c.g.apply(c.g.names.computeNamespace, p('drill-fence-from-clients', 'drill-client', 'out'))
      const cmd = (src: string) => `i=0; while true; do i=$((i + 1)); t=$(date +%s%3N); out=$(timeout 20 psql -qAtX -c "insert into fence_rows(src, seq) values ('${src}', $i) returning id" 2>&1); rc=$?; t2=$(date +%s%3N); if [ $rc -eq 0 ]; then echo "ok $t $t2 ${src} $i $out"; else echo "err $t $t2 ${src} $i rc=$rc $(echo $out | tr '\\n' ' ' | cut -c1-160)"; fi; sleep 1; done`
      await c.startClientPod('drill-fence-old', clients, DB, cmd('old'), { PGHOST: old.ip, PGSSLMODE: 'require' })
      await c.startClientPod('drill-fence-new', clients, DB, cmd('new'))
      await c.sleep(15)
      const t0 = Number((await c.g.clientClock('drill-fence-new')).trim())
      c.note(`orphaning the running compute ${old.name} (${old.ip}) at ${t0}: it keeps running, the Deployment makes a second`)
      await c.g.relabelPod(c.g.names.computeNamespace, old.name, { app: 'drill-orphan', compute: 'orphan' })
      await c.sleep(75)
      const rows = (await c.sql(DB, 'select src, seq, id from fence_rows order by id')).split('\n').filter(Boolean)
      const counts = new Map<string, number>(); for (const r of rows) { const [s, q] = r.split('|'); const k = `${s}:${q}`; counts.set(k, (counts.get(k) ?? 0) + 1) }
      for (const who of ['old', 'new'] as const) {
        const log = await c.clientLog(`drill-fence-${who}`)
        const acked = log.split('\n').filter((l) => l.startsWith('ok ')).map((l) => l.split(' ')), errs = log.split('\n').filter((l) => l.startsWith('err '))
        const missing = acked.filter((a) => counts.get(`${who}:${a[4]}`) !== 1).length
        const after = acked.filter((a) => Number(a[2]) > t0).length
        const lastOk = Math.max(0, ...acked.map((a) => Number(a[2])))
        c.record(`${who}: acknowledged (after the orphaning), errors, last acknowledgement after it (s), acknowledged rows missing or doubled`, `${acked.length} (${after}); ${errs.length}; ${Math.round((lastOk - t0) / 100) / 10}; ${missing}`, errs[0]?.slice(0, 160) ?? '')
      }
      c.record('rows in the table, doubled', `${rows.length}; ${[...counts.values()].filter((v) => v > 1).length}`)
      c.note(`the old compute's log: ${(await c.g.kread(['-n', c.g.names.computeNamespace, 'logs', old.name, '--tail=400'])).split('\n').filter((l) => /WP\]|walproposer|term|FATAL|terminat/.test(l)).slice(-6).join(' | ').slice(0, 600)}`)
      await c.stopClients(); await c.g.killPod(c.g.names.computeNamespace, old.name)
    },
  },
]

// ---- machine cuts

async function waitNodeReady(c: Ctx, node: string, seconds: number): Promise<number | null> {
  const t0 = c.g.now()
  while (c.g.now() - t0 < seconds * 1000) {
    const n = await c.g.kjson<any>(['get', 'node', node]).catch(() => null)
    if (n && (n.status.conditions ?? []).some((x: any) => x.type === 'Ready' && x.status === 'True')) return Math.round((c.g.now() - t0) / 1000)
    await c.sleep(3)
  }
  return null
}
/** A power cut: the machine stopped by the installer's command, held 150 s (under the cluster's 5 minute NodeNotReady), started again, waited for. The trap starts it again whatever happens. */
async function cutAndRestore(c: Ctx, machine: string, i: Info): Promise<void> {
  await c.g.silence({ machine, skipPeat: true, comment: `${c.name}: ${machine} cut` })
  await c.g.cutMachine(machine)
  const cutAt = c.g.now()
  c.note(`${machine} cut`)
  await c.sleep(150)
  await c.g.startMachine(machine)
  c.note(`${machine} started again`)
  const r = await waitNodeReady(c, machine, 360)
  c.note(`node Ready again ${r === null ? 'NOT within 6 minutes' : `${Math.round((c.g.now() - cutAt) / 1000)} s from the cut`}; placement at the Ready: ${JSON.stringify(await c.placement(i.tenant).catch(() => 'unreachable'))}`)
}
const machineDrills: DrillSpec[] = [
  {
    name: 'worker', group: 'machine', title: 'the machine of the attached pageserver cut like a power cut, held 150 s, started again', minutes: 12, clear: true, machineCut: true,
    run: (c) => failureDrill(c, { clientNode: (i) => notCut(i, i.attachedMachine, clientMachine(i)), fault: (i) => cutAndRestore(c, i.attachedMachine, i) }),
  },
  {
    name: 'worker-comp', group: 'machine', title: 'the machine the compute runs on cut (run with --want with-gateway and again without-gateway: the gateway on it, or not)', minutes: 12, clear: true, machineCut: true,
    async run(c, o) {
      const i = await gather(c)
      const here = i.gatewayNodes.includes(i.computeNode)
      if (o.want && (o.want === 'with-gateway') !== here) throw new Error(`the compute is on ${i.computeNode}, ${here ? 'which has' : 'which has no'} a gateway; this run wants ${o.want}. Nothing was cut. Stop the database's compute (POST /db/${DB}/stop) and let a connection start it again until it lands where wanted.`)
      if (i.computeNode === 'none') throw new Error('the database has no compute running')
      await failureDrill(c, { clientNode: () => notCut(i, i.computeNode, clientMachine(i)), note: () => `gateway on the compute's machine: ${here}`, fault: (j) => cutAndRestore(c, i.computeNode, j) })
    },
  },
  {
    name: 'worker-ctl', group: 'machine', title: 'the machine of the acting control layer cut', minutes: 12, clear: true, machineCut: true,
    run: (c) => failureDrill(c, { clientNode: (i) => notCut(i, i.leaderNode, clientMachine(i)), fault: (i) => cutAndRestore(c, i.leaderNode, i) }),
  },
  {
    name: 'worker-w3', group: 'machine', title: 'the storage controller\'s machine cut: no failover can happen while it is down', minutes: 12, clear: true, machineCut: true,
    run: (c) => failureDrill(c, { clientNode: (i) => notCut(i, i.controllerNode), fault: (i) => cutAndRestore(c, i.controllerNode, i) }),
  },
]

// ---- the store unreachable

const INTERNET_RULE = (r: any) => (r.to ?? []).some((t: any) => t.ipBlock)
/** The storage pods' egress to the internet (the store) taken away for `seconds`, put back by the guard. The store itself is not touched. Connections already open may live on until they time out (not verified: the first run says). */
async function storeDown(c: Ctx, seconds: number): Promise<void> {
  for (const name of ['pageserver', 'safekeeper']) {
    const cur = await c.g.kjson<any>(['-n', c.g.names.namespace, 'get', 'networkpolicy', name])
    await c.g.setPolicyEgress(c.g.names.namespace, name, (cur.spec.egress as any[]).filter((r) => !INTERNET_RULE(r)))
  }
  c.note(`the store made unreachable for the pageservers and safekeepers (their egress to the internet removed) for ${seconds} s`)
  await c.sleep(seconds)
}
const storeDrills: DrillSpec[] = [
  {
    name: 'store-down', group: 'store', title: 'the store unreachable 300 s (a NetworkPolicy takes the storage pods\' egress to the internet away; the store is not touched)', minutes: 12, clear: true, machineCut: false,
    run: (c) => failureDrill(c, { clientNode: clientMachine, settle: 60, fault: async () => { const m = c.g.undoMark(); await storeDown(c, 300); await c.g.undoSince(m) } }),
  },
]

// ---- volumes wiped

const sum = (db: string) => `select count(*), sum(length(payload)), md5(string_agg(id::text, ',' order by id)) from ${db}`
const wipeDrills: DrillSpec[] = [
  {
    name: 'disk-loss', group: 'wipe', title: 'a pageserver\'s whole volume wiped (the controller\'s disk-loss handling on, the store holding the data): the tenant comes back by itself and a compute reads every row', minutes: 12, clear: false, machineCut: false,
    async run(c) {
      await onlyDrillDatabases(c)
      const i = await gather(c)
      const before = await c.sql(DB, sum('alerts_bulk'))
      c.note(`before: ${before}; attached on ${i.attached}`)
      if (!(await c.flushed(DB))) { c.note('uploads not finished: waiting up to 90 s'); for (let k = 0; k < 30 && !(await c.flushed(DB)); k++) await c.sleep(3) }
      await c.g.control('POST', `/db/${DB}/stop`); await c.sleep(25)
      const t0 = c.g.now()
      await c.g.scale(c.g.names.namespace, 'statefulset', `pageserver-w${i.attached}`, 0)
      await c.sleep(10)
      await c.g.wipeVolume(i.attachedMachine, c.g.settings.volumes.pageserver)
      await c.g.release(c.g.names.namespace, 'statefulset', `pageserver-w${i.attached}`)
      let active = false
      for (let k = 0; k < 180 && !active; k++) { active = await c.g.storage('pageserverapi', `http://${ps(i.attached)}.pageserver.${c.g.names.namespace}.svc:9898/v1/tenant/${i.tenant}`).then((j) => j.state?.slug === 'Active').catch(() => false); if (!active) await c.sleep(2) }
      c.record('the tenant Active again (s after the wipe)', active ? String(Math.round((c.g.now() - t0) / 1000)) : 'NOT ACTIVE after 6 minutes')
      await c.g.control('POST', `/db/${DB}/start`); await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${DB}`, 300)
      c.record('the compute Ready (s after the wipe)', String(Math.round((c.g.now() - t0) / 1000)))
      const after = await c.sqlRetry(DB, sum('alerts_bulk'), 300)
      c.record('full read done (s after the wipe); before and after', `${Math.round((c.g.now() - t0) / 1000)}; ${before === after ? 'IDENTICAL' : 'DIFFERENT'}`, `after: ${after}`)
    },
  },
  {
    name: 'partial-loss', group: 'alert', title: 'ONE tenant lost from the pageserver it is attached to, the controller not told: the control layer\'s own recovery (60 s) is held off for the drill so that the tenant stays missing past the alert\'s 5 minutes; PeatAttachedTenantMissing (critical, let through to the install\'s receivers)', minutes: 20, clear: false, machineCut: false,
    alerts: { tested: ['PeatAttachedTenantMissing'], notify: true },
    async run(c) {
      const i = await gather(c)
      const before = await c.sql(DB, sum('alerts_bulk'))
      c.note(`before: ${before} (attached on pageserver ${i.attached})`)
      await c.g.control('POST', `/db/${DB}/stop`); await c.sleep(25)
      // The control layer attaches a tenant that is missing from its pageserver again after RECOVER_AFTER_S (60 s), which is quicker than the alert's 5 minutes: with it, the alert (rightly) never fires. For the drill the recovery is held off
      // (the environment of the control layer patched, put back by the undo log), so the stuck state the alert is for lasts; the first run showed the recovery doing its work in 71 s and the alert quiet.
      const mark = c.g.undoMark()
      await c.g.patchEnv(c.g.names.namespace, 'deployment', 'peat-control', { name: 'RECOVER_AFTER_S', value: '3600' })
      await c.g.rolloutStatus(c.g.names.namespace, 'deployment', 'peat-control'); await c.sleep(30)
      const t0 = c.g.now()
      await c.g.detachTenant(i.attached, i.tenant)
      c.note(`the pageserver was told to detach it (the controller was not): the controller still counts it attached on ${i.attached}: ${JSON.stringify(await c.placement(i.tenant))}`)
      // held until the alert has fired and Alertmanager has had its 30 s group wait to send it, up to 9 minutes
      let firedAt: number | null = null
      while (c.g.now() - t0 < 9 * 60_000) {
        if (!firedAt && (await c.alerts()).some((a) => a.name === 'PeatAttachedTenantMissing' && a.state === 'firing')) firedAt = c.g.now()
        if (firedAt && c.g.now() - firedAt > 75_000) break
        await c.sleep(5)
      }
      c.record('PeatAttachedTenantMissing firing in Prometheus (s after the loss)', firedAt ? String(Math.round((firedAt - t0) / 1000)) : 'NEVER within 9 minutes')
      await c.g.undoSince(mark)
      await c.g.rolloutStatus(c.g.names.namespace, 'deployment', 'peat-control')
      let active = false
      for (let k = 0; k < 150 && !active; k++) { active = await c.g.storage('pageserverapi', `http://${ps(i.attached)}.pageserver.${c.g.names.namespace}.svc:9898/v1/tenant/${i.tenant}`).then((j) => j.state?.slug === 'Active').catch(() => false); if (!active) await c.sleep(4) }
      c.record('the tenant Active again (s after the loss), by the control layer\'s own recovery once it was put back', active ? String(Math.round((c.g.now() - t0) / 1000)) : 'NOT ACTIVE after 10 more minutes', `now attached ${JSON.stringify(await c.placement(i.tenant))}`)
      await c.g.control('POST', `/db/${DB}/start`); await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${DB}`, 300)
      const after = await c.sqlRetry(DB, sum('alerts_bulk'), 300)
      c.record('read back through the front door', before === after ? 'IDENTICAL' : 'DIFFERENT', `after: ${after}`)
    },
  },
  {
    name: 'sk-replace', group: 'wipe', title: 'a safekeeper\'s data wiped under a writer: it is a registered member on an empty disk, so it is treated as lost and the control layer replaces it by itself', minutes: 14, clear: false, machineCut: false,
    run: async (c) => { await onlyDrillDatabases(c); await failureDrill(c, {
      clientNode: clientMachine, settle: 60,
      fault: async () => {
        const victim = (await c.safekeepers())[0]!
        const id = victim.id
        await c.g.scale(c.g.names.namespace, 'statefulset', `safekeeper-w${id}`, 0); await c.sleep(10)
        await c.g.wipeVolume(victim.node, `${c.g.settings.volumes.safekeeper}/sk-${id}`)
        await c.g.release(c.g.names.namespace, 'statefulset', `safekeeper-w${id}`)
        c.note(`safekeeper ${id}'s directory wiped; it starts empty and asks the control layer, which marks it lost`)
        const t0 = c.g.now()
        let done = false
        for (let k = 0; k < 240 && !done; k++) {
          // a replacement is a StatefulSet the control layer made (labelled peat-sk=true) with its pod Ready; the replaced one's id is decommissioned
          const sts = await c.g.kjson<any>(['-n', c.g.names.namespace, 'get', 'statefulsets', '-l', 'peat-sk=true']).catch(() => ({ items: [] }))
          done = (sts.items as any[]).some((x) => (x.status?.readyReplicas ?? 0) >= 1)
          if (!done) await c.sleep(5)
        }
        c.record('safekeeper replaced by the automation (s from the wipe)', done ? String(Math.round((c.g.now() - t0) / 1000)) : 'NOT DONE after 20 minutes', JSON.stringify((await c.g.control('GET', '/sk-replace').catch(() => ({ json: null }))).json).slice(0, 400))
      },
    }) },
  },
]

// ---- the controller's database restored beside and switched to (an install whose controller keeps its records on a CloudNativePG Postgres)

/** The install's controller database, or a refusal: this drill restores a CloudNativePG Cluster from its archive, so it runs only where one is named (`PEAT_DRILL_CONTROLLER_DB`). */
function controllerDatabase(c: Ctx) {
  const cdb = c.g.settings.controllerDatabase
  if (!cdb || !cdb.machine || !cdb.image || !cdb.storageClass || !cdb.objectStore || !cdb.archive) throw new Error('the controller\'s records are not on a CloudNativePG database named for the drills (PEAT_DRILL_CONTROLLER_DB with cluster, machine, image, storageClass, objectStore and archive): nothing was changed')
  return cdb
}
const restoredCluster = (c: Ctx) => {
  const cdb = controllerDatabase(c)
  const p = { nodeSelector: { [c.g.names.machineLabel.key]: c.g.names.machineLabel.value, 'kubernetes.io/hostname': cdb.machine }, ...(c.g.names.machineTaint ? { tolerations: [{ key: c.g.names.machineTaint.key, operator: 'Equal', value: c.g.names.machineTaint.value, effect: c.g.names.machineTaint.effect }] } : {}) }
  return {
    apiVersion: 'postgresql.cnpg.io/v1', kind: 'Cluster', metadata: { name: RESTORED_CLUSTER, namespace: c.g.names.namespace },
    spec: {
      // the server's own image and its live archive folder (both change together at a major move): a copy restored from a folder of another major than its image cannot start, and the operator's default image may be an older major
      instances: 1, enablePDB: false, enableSuperuserAccess: false, imageName: cdb.image,
      affinity: p,
      storage: { size: '4Gi', storageClass: cdb.storageClass },
      resources: { requests: { cpu: '100m', memory: '256Mi' }, limits: { memory: '512Mi' } },
      bootstrap: { recovery: { source: 'origin', database: cdb.database, owner: 'storcon' } },
      externalClusters: [{ name: 'origin', plugin: { name: 'barman-cloud.cloudnative-pg.io', parameters: { barmanObjectName: cdb.objectStore, serverName: cdb.archive } } }],
      plugins: [{ name: 'barman-cloud.cloudnative-pg.io', isWALArchiver: true, parameters: { barmanObjectName: cdb.objectStore, serverName: RESTORED_CLUSTER } }],
    },
  }
}
const restoredVolume = (c: Ctx) => {
  const cdb = controllerDatabase(c)
  return {
    apiVersion: 'v1', kind: 'PersistentVolume', metadata: { name: RESTORED_CLUSTER },
    spec: {
      capacity: { storage: '4Gi' }, accessModes: ['ReadWriteOnce'], persistentVolumeReclaimPolicy: 'Retain', storageClassName: cdb.storageClass, claimRef: { namespace: c.g.names.namespace, name: `${RESTORED_CLUSTER}-1` },
      hostPath: { path: `${c.g.settings.volumes.controllerDatabase}/restored`, type: 'Directory' },
      nodeAffinity: { required: { nodeSelectorTerms: [{ matchExpressions: [{ key: 'kubernetes.io/hostname', operator: 'In', values: [cdb.machine] }] }] } },
    },
  }
}
const counts = async (c: Ctx, which: 'live' | 'restored') => {
  const out: string[] = []
  for (const t of (await c.g.storconSql(which, "select tablename from pg_tables where schemaname = 'public' order by 1")).split('\n').filter(Boolean)) out.push(`${t} ${await c.g.storconSql(which, `select count(*) from "${t}"`)}`)
  return out.join('\n')
}
const restoreDrills: DrillSpec[] = [
  {
    name: 'storcon-restore', group: 'restore', title: 'the controller\'s database restored from its archive in the store into a copy beside it and the controller switched to it (never two controllers at once), then switched back', minutes: 20, clear: false, machineCut: false,
    async run(c) {
      const cdb = controllerDatabase(c)
      const ns = c.g.names.namespace
      c.note('1. the live database\'s counts, then a change after the base backup')
      const liveBefore = await counts(c, 'live')
      await c.g.control('POST', '/db', { name: 'drillpostbackup' })
      await c.g.storconSql('live', 'select pg_switch_wal()')
      await c.sleep(30)
      c.note(`last archived WAL: ${await c.g.storconSql('live', 'select last_archived_wal || \' \' || last_archived_time from pg_stat_archiver')}`)
      const expected = await counts(c, 'live')
      c.note('2. the controller stopped; the database restored from the store')
      await c.g.scale(ns, 'deployment', 'storage-controller', 0)
      await c.sleep(10)
      const t0 = c.g.now()
      await c.g.makeVolumeDir(cdb.machine, `${c.g.settings.volumes.controllerDatabase}/restored`)
      await c.g.apply(null, restoredVolume(c))
      await c.g.apply(ns, restoredCluster(c))
      // the instance pod itself, not the recovery Job's pod that carries the same cluster label before it
      let ready = false
      for (const end = c.g.now() + 540_000; !ready && c.g.now() < end; ) {
        ready = (await c.pods(ns, `cnpg.io/cluster=${RESTORED_CLUSTER}`).catch(() => [])).some((x) => x.name === `${RESTORED_CLUSTER}-1` && x.ready)
        if (!ready) await c.sleep(3)
      }
      c.record('the restore took (s from the cluster\'s creation to Ready)', ready ? String(Math.round((c.g.now() - t0) / 1000)) : 'NOT READY in 9 minutes')
      const restored = await counts(c, 'restored')
      c.record('the restored database\'s row counts equal the live database\'s (every table)', restored === expected ? 'PASS' : 'FAIL', restored === expected ? '' : `live-before vs restored differ: ${liveBefore === restored}`)
      c.note('3. the controller pointed at the restored database')
      await c.g.patchEnv(ns, 'deployment', 'storage-controller', { name: 'DATABASE_URL', secretName: `${RESTORED_CLUSTER}-app`, secretKey: 'uri' })
      await c.g.scale(ns, 'deployment', 'storage-controller', 1)
      await c.g.rolloutStatus(ns, 'deployment', 'storage-controller', 300)
      c.record('the controller was down (s in all, the restore included)', String(Math.round((c.g.now() - t0) / 1000)))
      await c.sleep(20)
      const nodes = await c.g.storage('admin', `http://storage-controller.${ns}.svc:1234/control/v1/node`)
      c.note(`nodes: ${(nodes as any[]).map((n) => `${n.id}:${n.availability}/${n.scheduling}`).join(' ')}`)
      const t = await c.dbInfo(DB); c.note(`tenant of ${DB} ${t.tenant} attached/secondary: ${JSON.stringify(await c.placement(t.tenant))}`)
      c.note(`a running database answers: ${await c.sqlRetry(DB, "select 'a running database answers', count(*) from proof_rows", 120)}`)
      await c.g.control('POST', '/db', { name: 'drillafterrestore' })
      await c.sleep(7)
      c.note(`a database made through the restored controller answers: ${await c.sqlRetry('drillafterrestore', "select 'afterrestore answers'", 120)}`)
      await c.g.control('DELETE', '/db/drillafterrestore'); await c.g.control('DELETE', '/db/drillpostbackup')
      c.note('4. switched back to the original database (the undo log), the copy deleted')
      await c.g.scale(ns, 'deployment', 'storage-controller', 0)
      await c.g.undoAll()
      await c.g.rolloutStatus(ns, 'deployment', 'storage-controller', 300)
      await c.sleep(30) // the copy's pod is gone before its directory is wiped
      await c.g.wipeVolume(cdb.machine, `${c.g.settings.volumes.controllerDatabase}/restored`)
    },
  },
  {
    name: 'restore-store', group: 'restore', title: 'restore from the store alone: every pageserver volume, then every safekeeper volume, wiped after the uploads have finished', minutes: 40, clear: false, machineCut: false,
    async run(c) {
      await onlyDrillDatabases(c)
      const before = await c.sql(DB, sum('alerts_bulk')), beforeRows = await c.sql(DB, 'select count(*), max(n) from proof_rows')
      c.note(`before: ${before}; proof_rows ${beforeRows}`)
      await c.g.control('POST', `/db/${DB}/stop`)
      for (let k = 0; k < 60 && !(await c.flushed(DB)); k++) await c.sleep(3)
      if (!(await c.flushed(DB))) throw new Error('the uploads did not finish in 3 minutes: nothing wiped')
      c.note('uploads finished (the pageserver\'s remote_consistent_lsn equals its last record)')
      // A: every pageserver
      const a0 = c.g.now()
      const pss = (await c.g.platform()).pageservers
      for (const p of pss) await c.g.scale(c.g.names.namespace, 'statefulset', `pageserver-w${p.id}`, 0)
      await c.sleep(10)
      for (const p of pss) await c.g.wipeVolume(p.machine, c.g.settings.volumes.pageserver)
      for (const p of pss) await c.g.release(c.g.names.namespace, 'statefulset', `pageserver-w${p.id}`)
      const t = await c.dbInfo(DB)
      let active = false
      for (let k = 0; k < 300 && !active; k++) { const p = await c.placement(t.tenant).catch(() => null); active = p ? await c.g.storage('pageserverapi', `http://${ps(p.attached)}.pageserver.${c.g.names.namespace}.svc:9898/v1/tenant/${t.tenant}`).then((j) => j.state?.slug === 'Active').catch(() => false) : false; if (!active) await c.sleep(3) }
      c.record('A: every pageserver wiped: the tenant Active again (s)', active ? String(Math.round((c.g.now() - a0) / 1000)) : 'NOT ACTIVE after 15 minutes')
      await c.g.control('POST', `/db/${DB}/start`); await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${DB}`, 300)
      const afterA = await c.sqlRetry(DB, sum('alerts_bulk'), 600)
      c.record('A: read back from the store alone', before === afterA ? 'IDENTICAL' : 'DIFFERENT', `after: ${afterA}`)
      // B: every safekeeper
      await c.g.control('POST', `/db/${DB}/stop`); await c.sleep(20)
      const b0 = c.g.now()
      // the safekeepers are the ones the control layer has made live (ids and machines read, not assumed)
      const sks = await c.safekeepers()
      if (sks.length < 3) throw new Error(`only ${sks.length} Active safekeepers: nothing wiped`)
      for (const x of sks) await c.g.scale(c.g.names.namespace, 'statefulset', `safekeeper-w${x.id}`, 0)
      await c.sleep(10)
      for (const x of sks) await c.g.wipeVolume(x.node, `${c.g.settings.volumes.safekeeper}/sk-${x.id}`)
      for (const x of sks) await c.g.release(c.g.names.namespace, 'statefulset', `safekeeper-w${x.id}`)
      c.note('every safekeeper\'s directory wiped and the pods started again: each is a registered member on an empty disk')
      await c.sleep(120)
      c.note(`safekeeper pods after 2 minutes: ${(await c.pods(c.g.names.namespace, 'app=safekeeper')).map((p) => `${p.name}:${p.ready ? 'ready' : 'not ready'}`).join(' ')}`)
      c.note(`control layer's view: ${JSON.stringify((await c.g.control('GET', '/sk-replace').catch(() => ({ json: null }))).json).slice(0, 500)}`)
      await c.g.control('POST', `/db/${DB}/start`).catch((e) => c.note(`start after the safekeepers' wipe: ${e}`))
      const ok = await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${DB}`, 300)
      const afterB = ok ? await c.sqlRetry(DB, sum('alerts_bulk'), 300) : null
      c.record('B: every safekeeper wiped: the database reads back (s)', afterB === null ? 'DOES NOT START' : `${Math.round((c.g.now() - b0) / 1000)}; ${before === afterB ? 'IDENTICAL' : 'DIFFERENT'}`)
      const w = ok ? await c.sqlRetry(DB, 'insert into proof_rows default values returning n', 120) : null
      c.record('B: a write after the safekeepers\' wipe', w === null ? 'FAILS' : 'ok')
    },
  },
  {
    name: 'recover-name', group: 'restore', title: 'every safekeeper lost, a member outside the platform\'s machines too: each database recovered under its own name (`POST /db/<name>/recover`): the same connection string reconnects, the rows are identical, a write works, a restore to a moment before the recovery point works', minutes: 45, clear: false, machineCut: false,
    async run(c) {
      const names = await onlyRecreatable(c)
      const CHILD = 'drill-recbr', PAST = 'drill-recpast'
      const mine = (t: string) => `select count(*), md5(string_agg(n::text || batch, ',' order by n)) from ${t}`
      await c.g.control('POST', `/db/${DB}/start`); await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${DB}`, 300)
      // the data: two batches 20 s apart in the parent (the restore's moment sits between them), a child branch of it with rows of its own
      await c.sql(DB, 'drop table if exists rec_rows'); await c.sql(DB, 'create table rec_rows (n serial primary key, batch text)')
      await c.sql(DB, "insert into rec_rows(batch) select 'a' from generate_series(1, 500)")
      const tA = (await c.sql(DB, `select to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')`)).trim()
      await c.sleep(20)
      await c.sql(DB, "insert into rec_rows(batch) select 'b' from generate_series(1, 500)")
      const moment = new Date(Date.parse(tA) + 10_000).toISOString()
      const made = await c.g.control('POST', '/branch', { from: DB, name: CHILD })
      if (made.status !== 200) throw new Error(`the child branch: ${made.status} ${JSON.stringify(made.json).slice(0, 200)}`)
      await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${CHILD}`, 300)
      // the front door learns a new database's route within a few seconds: the first call is retried
      if ((await c.sqlRetry(CHILD, 'create table rec_child (n serial primary key, batch text)', 120)) === null) throw new Error(`${CHILD} did not answer in 2 minutes`)
      await c.sql(CHILD, "insert into rec_child(batch) select 'c' from generate_series(1, 300)")
      const before = { parent: await c.sql(DB, mine('rec_rows')), bulk: await c.sql(DB, sum('alerts_bulk')), child: await c.sql(CHILD, mine('rec_child')), childInherited: await c.sql(CHILD, mine('rec_rows')) }
      c.note(`before: parent ${before.parent}; alerts_bulk ${before.bulk}; child ${before.child}, inherited ${before.childInherited}; the restore's moment ${moment} (between the batches)`)
      const record0 = async (n: string) => { const r = await c.g.control('GET', `/db/${n}`); return r.json as { tenant: string; timeline: string; port: number } }
      const were = { [DB]: await record0(DB), [CHILD]: await record0(CHILD) }
      for (const n of [DB, CHILD]) await c.g.control('POST', `/db/${n}/stop`)
      await c.sleep(20)
      // every upload finished, for every database: what the pageservers hold is all there will be
      const unflushed: string[] = []
      for (const n of names) { let f = false; for (let k = 0; k < 60 && !f; k++) { f = await c.flushed(n).catch(() => false); if (!f) await c.sleep(3) } if (!f) unflushed.push(n) }
      if (unflushed.some((n) => n.startsWith('drill'))) throw new Error(`the uploads of ${unflushed.join(', ')} did not finish in 3 minutes: nothing wiped`)
      c.note(`uploads finished for ${names.length - unflushed.length} of ${names.length} databases${unflushed.length ? `; NOT for ${unflushed.join(', ')} (recreatable: what they hold past their last upload is lost)` : ''}`)
      // the loss: every member's scale to zero, its directory wiped, then started again (one on an outside machine too)
      const old = await c.safekeepers()
      if (old.length < 3) throw new Error(`only ${old.length} Active safekeepers: nothing wiped`)
      c.note(`members lost: ${old.map((x) => `${x.id} on ${x.node}`).join(', ')}`)
      const t0 = c.g.now()
      for (const x of old) await c.g.scale(c.g.names.namespace, 'statefulset', `safekeeper-w${x.id}`, 0)
      await c.sleep(10)
      for (const x of old) await c.g.wipeVolume(x.node, `${c.g.settings.volumes.safekeeper}/sk-${x.id}`)
      for (const x of old) await c.g.release(c.g.names.namespace, 'statefulset', `safekeeper-w${x.id}`)
      await c.sleep(60)
      c.note(`safekeeper pods a minute after: ${(await c.pods(c.g.names.namespace, 'app=safekeeper')).map((p) => `${p.name}:${p.ready ? 'ready' : 'not ready'}`).join(' ')}`)
      // each lost member is retired (the control layer then makes three new ones)
      for (const x of old) {
        let r = await c.g.control('POST', `/sk/${x.id}/decommission`)
        if (r.status !== 200) { c.note(`decommission ${x.id}: ${r.status} ${JSON.stringify(r.json).slice(0, 300)}; asked again in a minute`); await c.sleep(60); r = await c.g.control('POST', `/sk/${x.id}/decommission`) }
        c.note(`decommission ${x.id}: ${r.status} ${JSON.stringify(r.json).slice(0, 200)}`)
        if (r.status !== 200) throw new Error(`decommission ${x.id} was refused: not forced; the way on is by hand`)
      }
      const oldIds = new Set(old.map((x) => x.id))
      let fresh: { id: number; node: string }[] = []
      for (let k = 0; k < 200; k++) {
        fresh = (await c.safekeepers()).filter((x) => !oldIds.has(x.id))
        if (fresh.length >= 3 && (await c.waitReady(c.g.names.namespace, 'app=safekeeper', 3))) break
        await c.sleep(3)
      }
      if (fresh.length < 3) throw new Error(`the control layer made ${fresh.length} new members in 10 minutes`)
      c.record('the new members made and registered (s after the loss)', String(Math.round((c.g.now() - t0) / 1000)), fresh.map((x) => `${x.id} on ${x.node}`).join(', '))
      // each database, under its own name: the dry call, the call, the same string, the same rows, a write
      const recover = async (n: string, dry: boolean) => {
        const r = await c.g.control('POST', `/db/${n}/recover${dry ? '?dry=1' : ''}`)
        c.note(`recover ${n}${dry ? ' (dry)' : ''}: ${r.status} ${JSON.stringify(r.json).slice(0, 600)}`)
        if (r.status !== 200) throw new Error(`recover ${n}${dry ? ' (dry)' : ''}: ${r.status} ${JSON.stringify(r.json).slice(0, 300)}`)
      }
      await recover(DB, true); await recover(DB, false)
      const afterDb = await c.sqlRetry(DB, mine('rec_rows'), 300)
      const bulkAfter = await c.sqlRetry(DB, sum('alerts_bulk'), 60)
      const w = await c.sqlRetry(DB, "insert into rec_rows(batch) values ('after') returning n", 120)
      c.record(`${DB}: wipe to first write under the original name (s)`, w === null ? 'NO WRITE' : String(Math.round((c.g.now() - t0) / 1000)))
      c.record(`${DB}: rows after the recovery`, afterDb === before.parent && bulkAfter === before.bulk ? 'IDENTICAL' : 'DIFFERENT', `${afterDb}; ${bulkAfter}`)
      const nowDb = await record0(DB)
      c.record(`${DB}: the record`, `tenant ${nowDb.tenant === were[DB].tenant ? 'same' : 'CHANGED'}, port ${nowDb.port === were[DB].port ? 'same' : 'CHANGED'}, timeline ${nowDb.timeline === were[DB].timeline ? 'SAME' : 'new'}`)
      await recover(CHILD, true); await recover(CHILD, false)
      const afterChild = await c.sqlRetry(CHILD, mine('rec_child'), 300), inherited = await c.sqlRetry(CHILD, mine('rec_rows'), 60)
      const wc = await c.sqlRetry(CHILD, "insert into rec_child(batch) values ('after') returning n", 120)
      c.record(`${CHILD}: rows after the recovery, a write`, afterChild === before.child && inherited === before.childInherited ? 'IDENTICAL' : 'DIFFERENT', `${afterChild}; ${inherited}; write ${wc === null ? 'FAILS' : 'ok'}`)
      // a restore to a moment from before the recovery point, taken through the recovered name
      const r = await c.g.control('POST', '/branch', { from: DB, name: PAST, at: moment })
      if (r.status === 200) await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${PAST}`, 300)
      const past = r.status === 200 ? await c.sqlRetry(PAST, "select count(*), count(*) filter (where batch = 'a') from rec_rows", 300) : null
      c.record(`restore of ${DB} to ${moment} (before the recovery point): rows (expected 500|500)`, past ?? `FAILS: ${r.status} ${JSON.stringify(r.json).slice(0, 200)}`)
      for (const n of [PAST, CHILD]) await c.g.control('POST', `/db/${n}/stop`).catch(() => {})
      c.note(`the other databases (${names.filter((n) => !n.startsWith('drill')).join(', ')}) are left unrecovered: by hand, after this run`)
    },
  },
]

// ---- alerts: the object store goes away for long enough that every stuck-state rule's `for` passes

const alertDrills: DrillSpec[] = [
  {
    name: 'alerts-store', group: 'alert', title: 'the store unreachable for 22 minutes while a database takes 150 MB of writes, a branch is made and deleted and a pageserver restarts: PeatSafekeeperTrimBehind, PeatUploadsLagging, PeatDeletionFailures and PeatAttachNotActive (the rules\' `for` is 5 to 15 minutes)', minutes: 50, clear: true, machineCut: false,
    alerts: { tested: ['PeatSafekeeperTrimBehind', 'PeatUploadsLagging', 'PeatDeletionFailures', 'PeatAttachNotActive'], notify: false },
    async run(c) {
      await c.sql(DB, 'create table if not exists alerts_bulk (id int, payload text)')
      const rows0 = await c.sql(DB, 'select count(*) from alerts_bulk')
      const mark = c.g.undoMark()
      for (const name of ['pageserver', 'safekeeper']) {
        const cur = await c.g.kjson<any>(['-n', c.g.names.namespace, 'get', 'networkpolicy', name])
        await c.g.setPolicyEgress(c.g.names.namespace, name, (cur.spec.egress as any[]).filter((r) => !INTERNET_RULE(r)))
      }
      c.note('store unreachable; writing 150 MB')
      await c.sql(DB, "insert into alerts_bulk select g, repeat('x', 240) from generate_series(1, 600000) g", 300)
      c.note(`rows now: ${await c.sql(DB, 'select count(*) from alerts_bulk')}`)
      await c.g.control('POST', '/branch', { from: DB, name: 'drillbr', start: false }).catch((e) => c.note(`branch: ${e}`)); await c.sleep(3); await c.g.control('DELETE', '/db/drillbr').catch(() => {})
      const i = await gather(c)
      const other = i.secondary[0] ?? i.attached
      c.note(`pageserver ${other} restarted (its tenants attach with the store down)`); await c.g.killPod(c.g.names.namespace, ps(other))
      await c.sleep(22 * 60)
      await c.g.undoSince(mark)
      c.note('store back; waiting for the alerts to resolve')
      await c.sleep(10 * 60)
      c.record('rows after (before the writes; after them 600000 more)', `${await c.sql(DB, 'select count(*) from alerts_bulk')} (${rows0})`)
    },
  },
]

// ---- the uplink

/**
 * A large load into a platform database, through the store, chunk by chunk: how fast the platform's link to its store carries it. An install whose other work shares that link names a PromQL expression that has a
 * series while that work is behind (`PEAT_DRILL_UPLINK_BEHIND`) and its alert (`PEAT_DRILL_UPLINK_ALERT`); the load stops the moment the expression has one. Without it the load stops only when a chunk takes
 * more than 15 minutes to reach the store.
 */
const uplinkDrills: DrillSpec[] = [
  {
    name: 'uplink', group: 'uplink', title: 'a 5 GB load into a platform database, the next chunk only after the last reached the store; it stops if the work the install named as sharing the link falls behind', minutes: 120, clear: true, machineCut: false,
    async run(c, o) {
      const D = 'drill-uplink', gb = o.gb ?? 5
      const guard = c.g.settings.uplink
      const behind = async (): Promise<string> => {
        if (!guard) return ''
        const series = await c.g.prom(guard.behind)
        const al = guard.alert ? (await c.alerts()).find((a) => a.name === guard.alert) : undefined
        return series.length ? `the work sharing the link is behind (${guard.behind} has ${series.length} series, the first ${Math.round(series[0]!.value)})` : al ? `${guard.alert} is ${al.state}` : ''
      }
      c.note(guard ? `the load stops the moment ${guard.behind} has a series${guard.alert ? ` or ${guard.alert} is active` : ''}` : 'no work is named as sharing the link (PEAT_DRILL_UPLINK_BEHIND): the load stops only on a chunk that takes more than 15 minutes')
      const why = await behind()
      if (why) throw new Error(`not starting: ${why}`)
      await c.createDb(D); await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${D}`, 120)
      // this database's tenant flushes every 30 s (the platform's default is 10 minutes and stays), so a chunk is flushed and uploaded within about a minute
      await c.setCheckpoint(D, 30)
      await c.sql(D, 'create table if not exists uplink_load (id bigserial primary key, payload text)')
      // The load goes in chunks of about 100 MB, the next only after the previous is FLUSHED to the store (remote_consistent_lsn equals the last record), so the upload queue never exceeds a chunk (under about 200 MB)
      // and a stop is a stop: nothing is left uploading behind the check.
      const chunkRows = 100_000
      const t0 = c.g.now(); let loaded = 0, stopped = ''
      while (loaded < gb * 1000 && !stopped) {
        await c.sql(D, `insert into uplink_load(payload) select repeat(md5(random()::text), 31) from generate_series(1, ${chunkRows})`, 300)
        loaded += 100
        const f0 = c.g.now()
        while (!(await c.flushed(D))) {
          stopped = await behind()
          if (stopped || c.g.now() - f0 > 15 * 60_000) { stopped = stopped || `a chunk of 100 MB did not reach the store in 15 minutes (the link is slower than 0.1 MB/s)`; break }
          await c.sleep(10)
        }
        if (!stopped) stopped = await behind()
        c.note(`${loaded} MB loaded and flushed (${Math.round((c.g.now() - f0) / 1000)} s for the last chunk)`)
      }
      const secs = Math.max(1, (c.g.now() - t0) / 1000)
      c.record('loaded (MB), seconds, average rate through the store (MB/s)', `${loaded}; ${Math.round(secs)}; ${Math.round((loaded / secs) * 10) / 10}`, stopped ? `STOPPED: ${stopped}` : 'the load completed')
      if (guard?.alert) c.record(guard.alert, (await c.alerts()).some((a) => a.name === guard.alert) ? 'FIRED or pending' : 'quiet')
      await c.g.control('DELETE', `/db/${D}`)
    },
  },
]

// ---- setup: the drills' database

const setup: DrillSpec = {
  name: 'setup', group: 'setup', title: `makes the drills' database ${DB} (a counter table, ${FILLER_ROWS} rows of ${FILLER_BYTES} bytes of filler, a bulk table) through the control layer and waits for its uploads; changes nothing else`, minutes: 8, clear: false, machineCut: false,
  async run(c) {
    const have = await c.dbInfo(DB).then(() => true).catch(() => false)
    if (!have) { await c.createDb(DB); await c.waitReady(c.g.names.computeNamespace, `app=peat-compute,compute=${DB}`, 180) }
    // the drills' own database flushes every 30 s (the platform's default, 10 minutes, is left alone): the drills that wait for "flushed" would otherwise wait up to 11 minutes
    await c.setCheckpoint(DB, 30)
    await c.sql(DB, 'create table if not exists proof_rows (n bigserial primary key, ts timestamptz default now())')
    await c.sql(DB, 'create table if not exists filler (id int primary key, payload text)')
    if (Number(await c.sql(DB, 'select count(*) from filler')) === 0) await c.sql(DB, `insert into filler select g, left(repeat(md5(g::text), ${Math.ceil(FILLER_BYTES / 32)}), ${FILLER_BYTES}) from generate_series(1, ${FILLER_ROWS}) g`, 300)
    await c.sql(DB, 'create table if not exists alerts_bulk (id int, payload text)')
    if (Number(await c.sql(DB, 'select count(*) from alerts_bulk')) === 0) await c.sql(DB, "insert into alerts_bulk select g, repeat('y', 240) from generate_series(1, 20000) g", 120)
    for (let k = 0; k < 90 && !(await c.flushed(DB)); k++) await c.sleep(3)
    c.record('ready', `filler ${await c.sql(DB, 'select count(*), sum(length(payload)) from filler')}`, `reader expects ${READER_EXPECT} (a heap scan: sum of length)`)
  },
}

export const DRILLS: DrillSpec[] = [setup, ...podDrills, ...sleepDrills, ...machineDrills, ...storeDrills, ...wipeDrills, ...restoreDrills, ...alertDrills, ...uplinkDrills]
export const byName = (n: string) => DRILLS.find((d) => d.name === n)
