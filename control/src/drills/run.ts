// The drills' runner. Nothing here cuts anything by itself: every change goes through `guard.ts`.
//
//   bun src/drills/run.ts list                                     # the drills, their groups and durations
//   bun src/drills/run.ts plan <drill> [--want with-gateway]       # every guarded step the drill would take, against a fake world: changes nothing, reads nothing from the cluster
//   bun src/drills/run.ts run <drill> --yes [--run N] [--want ...] [--gb N] [--kubeconfig <file>]
//   bun src/drills/run.ts recover [--kubeconfig <file>]           # after a run that was killed or could not undo: replays the pending file, sweeps the drills' objects, starts stopped platform machines
//
// What the drills are told about the install (environment values; see guard.ts, `drillSettings`): the platform's names (NAMESPACE, MACHINE_LABEL and the rest of src/names.ts), the machine command
// (PEAT_DRILL_MACHINE_COMMAND, e.g. `["my-power", "{action}", "{machine}"]`), the quiet hours (PEAT_QUIET_HOURS, the JSON of the Platform's spec.upgrades.quietHours), Alertmanager and Prometheus
// (PEAT_DRILL_ALERTMANAGER `monitoring/alertmanager`, PEAT_DRILL_PROMETHEUS `monitoring/prometheus:80`), the volumes (PEAT_DRILL_VOLUMES), the controller's database (PEAT_DRILL_CONTROLLER_DB), the databases
// that may be lost (PEAT_DRILL_RECREATABLE), the namespaces to watch (PEAT_DRILL_WATCHED) and the work the uplink drill must not slow (PEAT_DRILL_UPLINK_BEHIND, PEAT_DRILL_UPLINK_ALERT).
//
// Each change's way back is written to the pending file BEFORE the change; `run` refuses while it holds anything and `recover` clears it. `run` refuses to start unless the platform is in a known good state,
// is clear of the quiet hours when the drill cuts a machine or the store or loads the uplink (no argument overrides that), takes the snapshot of what must not move, holds the Alertmanager silence, runs the
// drill, undoes everything the guard changed, waits for the platform to be good again, takes the snapshot again and compares the pods of the namespaces it watches. The record of the run is written to the
// state directory.
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { DRILLS, byName, type DrillSpec, type Opts } from './drills'
import { FakeClock, FakeCtx, PlanExec, fakeSettings } from './fake'
import { Guard, RealExec, Refusal, assertNoPending, drillSettings, namespacesOf } from './guard'
import { LiveCtx, diffSnapshots, type Ctx } from './lib'

const argv = process.argv.slice(2)
const flag = (n: string) => { const i = argv.indexOf(`--${n}`); return i >= 0 ? argv[i + 1] : undefined }
const has = (n: string) => argv.includes(`--${n}`)
const opts = (): Opts => ({ want: flag('want') as Opts['want'], gb: flag('gb') ? Number(flag('gb')) : undefined })
const settings = () => { const s = drillSettings(); const k = flag('kubeconfig'); return k ? { ...s, kubeconfig: k } : s }

/** The alerts of an alert drill: seen in Prometheus and in Alertmanager (active, not silenced) with the time of first sight; a tested alert is silenced when Alertmanager first shows it active unless the drill lets it through to the install's receivers. */
function watchAlerts(c: Ctx, spec: DrillSpec): { stop: () => Promise<void>; seen: Map<string, { prometheus?: string; alertmanager?: string; silencedAt?: string }>; sent: { before?: { asked: number; failed: number }; sentAt?: string; after?: { asked: number; failed: number } } } {
  const seen = new Map<string, { prometheus?: string; alertmanager?: string; silencedAt?: string }>()
  const sent: { before?: { asked: number; failed: number }; sentAt?: string; after?: { asked: number; failed: number } } = {}
  let on = true
  const tested = spec.alerts!.tested
  const loop = (async () => {
    if (spec.alerts!.notify) sent.before = await c.g.notificationCounters().catch(() => undefined)
    while (on) {
      try {
        // the one alert let through: the moment Alertmanager's own count of notifications first moves is the moment it went to the receivers
        if (spec.alerts!.notify && sent.before && !sent.sentAt) { const b = await c.g.notificationCounters(); if (b.asked > sent.before.asked) { sent.sentAt = new Date(c.g.now()).toISOString(); sent.after = b } }
        const now = new Date(c.g.now()).toISOString().slice(11, 19)
        for (const a of await c.alerts()) if (tested.includes(a.name) && a.state === 'firing') seen.set(a.name, { ...seen.get(a.name), prometheus: seen.get(a.name)?.prometheus ?? now })
        const am = (await c.g.monitoringGet('alertmanager', '/api/v2/alerts?active=true&silenced=false&inhibited=false')) as any[]
        for (const a of am) {
          const n = a.labels.alertname as string
          if (!tested.includes(n)) continue
          const cur = seen.get(n) ?? {}
          if (!cur.alertmanager) { cur.alertmanager = now; seen.set(n, cur); if (!spec.alerts!.notify) { await c.g.silenceAlert(n, `${spec.name}: seen active in Alertmanager`); cur.silencedAt = now } }
        }
      } catch (e) { c.note(`alert watch: ${e}`) }
      await c.sleep(3) // under Alertmanager's 30 s group wait, so a tested warning is silenced before it is sent
    }
  })()
  return { seen, sent, stop: async () => { on = false; await loop } }
}

async function runRecover(): Promise<void> {
  const s = settings()
  const g = new Guard(new RealExec(s.kubeconfig), { settings: s })
  g.installTrap()
  const r = await g.recover()
  g.log(`recover: pending left ${r.pendingLeft.length ? r.pendingLeft.join('; ') : 'none'}; swept ${r.swept.join(', ') || 'nothing'}; machines started ${r.started.join(', ') || 'none'}; the platform's pieces are kept as declared by the operator`)
  const c = new LiveCtx(g, 'recover', 0)
  const st = await c.state()
  for (const l of st.lines) g.log(`state: ${l}`)
  g.log(st.ok && r.pendingLeft.length === 0 ? 'the platform is in a known good state and nothing is pending' : 'NOT clean: look at the lines above')
  if (!st.ok || r.pendingLeft.length) process.exitCode = 1
}
async function runLive(spec: DrillSpec): Promise<void> {
  const s = settings()
  assertNoPending(s.stateDir)
  if (spec.machineCut && !s.machineCommand) throw new Refusal(`${spec.name} cuts a machine and no machine command is set (PEAT_DRILL_MACHINE_COMMAND, with {action} and {machine})`)
  if (spec.alerts && (!s.alertmanager || !s.prometheus)) throw new Refusal(`${spec.name} tests alerts and needs Prometheus and Alertmanager named (PEAT_DRILL_PROMETHEUS, PEAT_DRILL_ALERTMANAGER)`)
  const runNo = Number(flag('run') ?? '1')
  const g = new Guard(new RealExec(s.kubeconfig), { settings: s })
  g.installTrap()
  if (spec.clear) g.assertClearOfQuietHours(spec.minutes)
  const c = new LiveCtx(g, spec.name, runNo)
  const started = new Date().toISOString()
  g.log(`== ${spec.name} run ${runNo}: ${spec.title}`)
  if (spec.group !== 'setup') {
    const s0 = await c.state()
    for (const l of s0.lines) g.log(`state: ${l}`)
    if (!s0.ok) throw new Error('the platform is not in a known good state: nothing was changed')
  }
  const snap0 = await c.snapshot()
  await g.silence({ exclude: spec.alerts?.tested, comment: `${spec.name} run ${runNo}` })
  const watcher = spec.alerts ? watchAlerts(c, spec) : null
  let failure: string | null = null
  try { await spec.run(c, opts()) } catch (e) { failure = String(e); g.log(`DRILL FAILED: ${e}`) }
  finally {
    if (watcher) await watcher.stop()
    await g.undoAll()
    await c.stopClients().catch(() => {})
  }
  g.log('waiting up to 5 minutes for the platform to be good again')
  const good = await c.waitState(300)
  // the check above may have started a client of its own: it and what it made are taken away again before the record is written
  await c.stopClients().catch(() => {})
  await g.undoAll()
  const snap1 = await c.snapshot()
  const d = diffSnapshots(snap0, snap1, { platform: namespacesOf(g.names), watched: s.watched })
  g.log(`known good state after: ${good ? 'yes' : 'NO: hand repair needed'}`)
  g.log(`the watched namespaces' pods compared: ${d.flagged.length ? `CHANGED: ${d.flagged.join('; ')}` : 'unchanged (same pods, no restarts)'}`)
  if (d.changed.length + d.added.length + d.removed.length) g.log(`all differences: ${[...d.changed, ...d.added.map((x) => `+${x}`), ...d.removed.map((x) => `-${x}`)].join('; ')}`)
  let delivered = ''
  if (spec.alerts?.notify && watcher) {
    const after = await g.notificationCounters().catch(() => undefined)
    const b = watcher.sent
    const asked = after && b.before ? after.asked - b.before.asked : NaN, failed = after && b.before ? after.failed - b.before.failed : NaN
    delivered = `Alertmanager tried ${asked} notification(s) and ${failed} failed during the drill; the first at ${b.sentAt ?? 'NEVER'} (UTC, this workstation's clock, polled every 3 s)`
    g.log(`notifications: ${delivered}`)
  }
  mkdirSync(s.stateDir, { recursive: true })
  const file = join(s.stateDir, `${spec.name}-${runNo}-${started.replace(/[:.]/g, '')}.json`)
  writeFileSync(file, JSON.stringify({ drill: spec.name, run: runNo, started, ended: new Date().toISOString(), failure, goodAfter: good, results: c.results, notes: c.notes, snapshotDiff: d, alerts: watcher ? Object.fromEntries(watcher.seen) : undefined, notifications: delivered, actions: g.actions.map((a) => a.what) }, null, 2))
  g.log(`record written to ${file}`)
  for (const r of c.results) console.log(`RESULT ${r.what} = ${r.value} ${r.note}`)
  if (failure || !good) process.exitCode = 1
}

/** Every guarded step of a drill against the fake world (three machines, every setting a drill needs, no quiet hours): prints them, changes nothing, reads nothing from a cluster. */
async function plan(spec: DrillSpec): Promise<FakeCtx> {
  const clock = new FakeClock()
  const lines: string[] = []
  const g = new Guard(new PlanExec(), { dry: true, clock, log: (l) => lines.push(l), settings: fakeSettings() })
  const c = new FakeCtx(g, spec.name, 1)
  if (spec.clear) g.assertClearOfQuietHours(spec.minutes)
  await g.silence({ exclude: spec.alerts?.tested, comment: `${spec.name} plan` })
  await spec.run(c, { want: undefined, ...opts() })
  await g.undoAll()
  for (const l of lines) console.log(l)
  return c
}

if (import.meta.main) {
  const [cmd, name] = argv
  try {
    if (cmd === 'list' || cmd === undefined) {
      for (const d of DRILLS) console.log(`${d.name.padEnd(16)} ${d.group.padEnd(8)} ~${String(d.minutes).padStart(3)} min ${d.clear ? 'clear-of-quiet-hours ' : ''}${d.machineCut ? 'machine-cut ' : ''}${d.alerts ? `alerts:${d.alerts.tested.join(',')}${d.alerts.notify ? ' (let through)' : ''} ` : ''}\n    ${d.title}`)
    } else if (cmd === 'recover') {
      await runRecover()
    } else if (cmd === 'plan' || cmd === 'run') {
      const spec = name ? byName(name) : undefined
      if (!spec) throw new Error(`no drill ${JSON.stringify(name)}; bun run.ts list`)
      if (cmd === 'plan') await plan(spec)
      else if (!has('yes')) { console.log(`${spec.name}: ${spec.title}\n~${spec.minutes} minutes. Add --yes to run it. \`plan\` shows every guarded step first.`); process.exitCode = 2 }
      else await runLive(spec)
    } else throw new Error('usage: run.ts list | plan <drill> | run <drill> --yes | recover')
  } catch (e) {
    console.error(e instanceof Refusal ? e.message : String(e))
    process.exitCode = e instanceof Refusal ? 3 : 1
  }
}

export { plan }
