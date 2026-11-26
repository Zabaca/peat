// The one door every cutting step of Peat's drills goes through. A drill is a script that breaks something on purpose, on a cluster that
// usually runs other work beside the platform: the guard is what makes "on purpose" mean "only the platform's". Nothing outside this file
// builds a command for a machine or the cluster (a test reads every other file of this folder for `Bun.spawn`, `child_process` and
// `kubectl`), so a drill's argument can choose WHICH platform piece to cut and never WHAT KIND of thing: there is no path from an argument to
// a command that names another machine, namespace, path or alert.
//
// What it accepts, whole (an exact string match, never a prefix, never case-folded, never trimmed):
//   - a machine cut: a node that carries the platform's machine label (read from the cluster, and read back again before the cut), cut by
//     the command the installer supplies (`PEAT_DRILL_MACHINE_COMMAND`, with `{action}` and `{machine}` in it), run as an argument list with
//     no shell. A node without the label, a name that only begins like a platform machine, and every machine when no command is set are refused
//     before any command runs.
//   - a pod, workload, volume, NetworkPolicy or object cut: only in the platform's four namespaces (src/names.ts).
//   - a volume wipe: only a path under one of the platform's volume roots, on a machine that holds that volume (the pageservers' volume where a
//     pageserver runs, the safekeepers' where a safekeeper may run, the controller database's where it runs), made by a Job the guard writes
//     (never a path from outside, never `..`, never an absolute path of its own).
//   - an object a drill makes: a Pod, ConfigMap or NetworkPolicy named `drill-*` in the platform's namespaces, the one restored database
//     Cluster `drill-records-restored` and the PersistentVolume of the same name on the controller database's volume; and nothing else.
//   - a silence: Alertmanager, when one is named (`PEAT_DRILL_ALERTMANAGER`), 30 minutes at most, built from the fixed matchers below and
//     never from an argument's text.
// Every change a drill makes is undone by an UNDO LOG the guard keeps: a machine cut is started again, a scale is put back, a patched object
// is restored, a policy that was changed is put back, what the drill made is deleted. The log runs on a normal end, on an error, on SIGINT and
// SIGTERM and at process exit (`installTrap`), so an interrupted drill does not leave a machine down past the cluster's NodeNotReady.
//
// The Exec, the clock and the settings are injected: the tests run every drill against a recording Exec and a fake world, so that every step
// of every drill is checked by this file's own rules without a cluster.
import { existsSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { fromEnv, placement, type Names, type Toleration } from '../names'
import { describeWindow, meets, parseWindows, type Window } from '../quiet-hours'

export class Refusal extends Error {
  constructor(message: string) {
    super(`REFUSED by the drills' guard: ${message}`)
  }
}

/** The drills' standing database (made by `setup`, kept between drills): `recover` leaves it alone and deletes only the databases a drill makes for itself. */
export const DRILL_BASE_DATABASE = 'drill-par200'
/** The controller database's copy the restore drill makes beside the live one (a CloudNativePG Cluster), and the PersistentVolume it lives on. */
export const RESTORED_CLUSTER = 'drill-records-restored'

// ---- settings: what an install tells the drills about itself (environment values; each unset is the neutral default)

/** The controller's records on a CloudNativePG Postgres, for the restore drill: the Cluster, the machine that holds its volume, its image, the storage class of its volume, the barman ObjectStore it archives to and the live archive folder in it, the database's name. */
export interface ControllerDatabase { cluster: string; machine: string; image: string; storageClass: string; objectStore: string; archive: string; database: string }
export interface DrillSettings {
  names: Names
  /** The installer's machine command as an argument list, with `{action}` (stop or start) and `{machine}` in it; null: no machine is cut. */
  machineCommand: string[] | null
  /** The windows in which nothing that cuts a machine or the store, or loads the uplink, may run (the Platform's quiet hours). */
  quietHours: Window[]
  /** Alertmanager's namespace and name (its Deployment and its Service, port 9093); null: nothing is silenced and no alert is read there. */
  alertmanager: { namespace: string; name: string } | null
  /** Prometheus's namespace and Service with its port (`monitoring/prometheus:80`); null: no alert or metric is read. */
  prometheus: { namespace: string; service: string } | null
  /** The platform's Prometheus rule file; null: the platform's own alert names (`PEAT_ALERTS`). */
  rulesFile: string | null
  /** The machines' host directories each storage piece keeps its data in (a wipe is accepted only below one of them). */
  volumes: { pageserver: string; safekeeper: string; controllerDatabase: string }
  /** The Secret, in the computes' namespace, holding the platform CA's certificate (`tls.crt`; its key is never read). */
  /** The ConfigMap, in the database namespace, holding the authority's public certificate (ca.crt): what a client verifies the databases with. */
  caConfigMap: string
  /** The full-sleep time the control layer and the gateways run with (the sleep drills shorten it and the check after a drill wants it back). */
  idleMinutes: string
  controllerDatabase: ControllerDatabase | null
  /** Names of databases that may be lost by the drills that lose every safekeeper's copy, beside the drills' own (`drill*`). */
  recreatable: RegExp
  /** The namespaces whose pods a drill must not disturb; null: every namespace but the platform's and kube-system. */
  watched: string[] | null
  /** A PromQL expression that has a series when the work the uplink drill must not slow down is behind (empty: only the drill's own 15 minute rule stops it), and the alert of that work. */
  uplink: { behind: string; alert: string } | null
  stateDir: string
  /** The kubeconfig the drills' commands use; null: the environment's (KUBECONFIG, or kubectl's default). */
  kubeconfig: string | null
}

/** The Platform kinds' own defaults (spec.pageservers.storage, spec.safekeepers.storage), and the controller database's beside them. */
export const DEFAULT_VOLUMES = { pageserver: '/var/lib/peat/pageserver', safekeeper: '/var/lib/peat/safekeeper', controllerDatabase: '/var/lib/peat/storcon-db' } as const

/** `namespace/name`, plain names both. */
function nsName(text: string, what: string): { namespace: string; name: string } {
  const m = /^([a-z0-9][-a-z0-9]*)\/([a-z0-9][-a-z0-9.:]*)$/.exec(text)
  if (!m) throw new Error(`${what} is namespace/name, not ${JSON.stringify(text)}`)
  return { namespace: m[1]!, name: m[2]! }
}

/** The installer's command: a JSON array of strings, or words separated by spaces. It must name both `{action}` and `{machine}`. */
export function parseMachineCommand(text: string | undefined): string[] | null {
  if (!text || text.trim() === '') return null
  const argv = text.trim().startsWith('[') ? (JSON.parse(text) as unknown[]) : text.trim().split(/\s+/)
  if (!Array.isArray(argv) || argv.length === 0 || argv.some((a) => typeof a !== 'string' || a === '')) throw new Error('PEAT_DRILL_MACHINE_COMMAND is a list of words')
  const all = argv.join(' ')
  if (!all.includes('{machine}') || !all.includes('{action}')) throw new Error('PEAT_DRILL_MACHINE_COMMAND names both {action} and {machine}')
  return argv as string[]
}
/** The command for one machine and action: each placeholder replaced in place, nothing else changed, no shell. */
export function machineCommand(template: readonly string[], action: 'stop' | 'start', machine: string): string[] {
  return template.map((a) => a.split('{action}').join(action).split('{machine}').join(machine))
}

export function drillSettings(env: Record<string, string | undefined> = process.env): DrillSettings {
  const home = env['HOME'] ?? ''
  const volumes = { ...DEFAULT_VOLUMES, ...(env['PEAT_DRILL_VOLUMES'] ? (JSON.parse(env['PEAT_DRILL_VOLUMES']) as Partial<DrillSettings['volumes']>) : {}) }
  const prom = env['PEAT_DRILL_PROMETHEUS'] ? nsName(env['PEAT_DRILL_PROMETHEUS'], 'PEAT_DRILL_PROMETHEUS') : null
  const cdb = env['PEAT_DRILL_CONTROLLER_DB'] ? (JSON.parse(env['PEAT_DRILL_CONTROLLER_DB']) as Partial<ControllerDatabase>) : null
  const uplink = env['PEAT_DRILL_UPLINK_BEHIND'] ? { behind: env['PEAT_DRILL_UPLINK_BEHIND'], alert: env['PEAT_DRILL_UPLINK_ALERT'] ?? '' } : null
  return {
    names: fromEnv(env),
    machineCommand: parseMachineCommand(env['PEAT_DRILL_MACHINE_COMMAND']),
    quietHours: parseWindows(env['PEAT_QUIET_HOURS']),
    alertmanager: env['PEAT_DRILL_ALERTMANAGER'] ? nsName(env['PEAT_DRILL_ALERTMANAGER'], 'PEAT_DRILL_ALERTMANAGER') : null,
    prometheus: prom ? { namespace: prom.namespace, service: prom.name.includes(':') ? prom.name : `${prom.name}:80` } : null,
    rulesFile: env['PEAT_DRILL_RULES'] || null,
    volumes,
    caConfigMap: env['PEAT_CA_CONFIGMAP'] || 'peat-ca',
    idleMinutes: env['IDLE_MINUTES'] || '30',
    controllerDatabase: cdb ? { cluster: 'storcon-db', image: '', storageClass: '', objectStore: '', archive: cdb.cluster ?? 'storcon-db', database: 'storage_controller', machine: '', ...cdb } as ControllerDatabase : null,
    recreatable: new RegExp(env['PEAT_DRILL_RECREATABLE'] ? `^drill|${env['PEAT_DRILL_RECREATABLE']}` : '^drill'),
    watched: env['PEAT_DRILL_WATCHED'] ? env['PEAT_DRILL_WATCHED'].split(',').map((s) => s.trim()).filter(Boolean) : null,
    uplink,
    stateDir: join(env['XDG_STATE_HOME'] || join(home, '.local/state'), 'peat-drills'),
    kubeconfig: env['KUBECONFIG'] || null,
  }
}

export const namespacesOf = (n: Names): string[] => [n.namespace, n.computeNamespace, n.gatewayNamespace, n.dbNamespace]

// ---- pure rules (tested alone)

export function assertMachine(name: unknown, machines: readonly string[]): asserts name is string {
  if (typeof name !== 'string' || !machines.includes(name)) throw new Refusal(`${JSON.stringify(name)} is not one of the platform's machines (${machines.join(', ') || 'none found'}), matched whole`)
}
export function assertNamespace(ns: unknown, namespaces: readonly string[]): asserts ns is string {
  if (typeof ns !== 'string' || !namespaces.includes(ns)) throw new Refusal(`namespace ${JSON.stringify(ns)} is not one of the platform's (${namespaces.join(', ')})`)
}
const DNS_NAME = /^[a-z0-9]([-a-z0-9]{0,251}[a-z0-9])?$/
export function assertName(name: unknown, what = 'name'): asserts name is string {
  if (typeof name !== 'string' || !DNS_NAME.test(name)) throw new Refusal(`${what} ${JSON.stringify(name)} is not a plain object name`)
}

/**
 * Where the platform's volumes are, as the guard read it from the cluster: the platform's machines (nodes with the machine label), which of
 * them hold which volume, and the machines outside the platform's that hold a safekeeper (a member placed apart, by its own selector and
 * tolerations): such a machine is never cut, but its member's directory may be wiped like any member's (a total loss means every member's).
 */
export interface Shape {
  machines: string[]
  /** volume root -> the machines that hold it */
  volumeMachines: Record<string, string[]>
  /** an outside machine -> the tolerations its safekeeper's pod carries (a wipe Job there carries the same) */
  outside: Record<string, Toleration[]>
  /** the ids of the platform's pageservers (`pageserver-w<id>-0`) and the machine each runs on */
  pageservers: { id: number; machine: string }[]
  /** the storage image the platform runs (a wipe Job runs it), and the compute image (the drills' clients run its psql) */
  storageImage: string
  computeImage: string
}

/** What a wipe may name below a volume's root: letters, digits, `.`, `_`, `-`, `/` and `*`, no empty or dot-only segment (so no `..`, no `//`, no leading `/`). Empty means the volume's whole contents. */
const SUBPATH = /^[A-Za-z0-9_*][A-Za-z0-9._*-]*(\/[A-Za-z0-9_*][A-Za-z0-9._*-]*)*$/
export function volumeTarget(machine: unknown, path: unknown, shape: Pick<Shape, 'machines' | 'volumeMachines' | 'outside'>, volumes: DrillSettings['volumes']): { root: string; sub: string } {
  const outside = typeof machine === 'string' && Object.hasOwn(shape.outside, machine)
  if (!outside) assertMachine(machine, shape.machines)
  if (typeof path !== 'string') throw new Refusal('a volume path is a string')
  const roots = Object.values(volumes)
  const root = roots.find((r) => path === r || path.startsWith(`${r}/`))
  if (!root) throw new Refusal(`${JSON.stringify(path)} is not under a platform volume (${roots.join(', ')})`)
  if (outside) { if (root !== volumes.safekeeper) throw new Refusal(`${machine as string} is outside the platform's machines and holds only the safekeepers' volume (${volumes.safekeeper}), not ${root}`) }
  else if (!(shape.volumeMachines[root] ?? []).includes(machine as string)) throw new Refusal(`${machine as string} has no volume at ${root} (it is on ${(shape.volumeMachines[root] ?? []).join(', ') || 'no machine'})`)
  const sub = path === root ? '' : path.slice(root.length + 1)
  if (path !== root && sub === '') throw new Refusal(`${JSON.stringify(path)} ends in a slash: name the volume's root or a plain path below it`)
  if (sub !== '' && !SUBPATH.test(sub)) throw new Refusal(`${JSON.stringify(sub)} is not a plain path below ${root} (letters, digits, . _ - * and / between names; no .. and no empty name)`)
  return { root, sub }
}

/** The platform's own alerts: the stuck states its drills found, and the two of the safekeepers' replacement. A silence may leave out only these (an alert drill leaves out what it tests). */
export const PEAT_ALERTS = ['PeatAttachNotActive', 'PeatAttachedTenantMissing', 'PeatDeletionFailures', 'PeatNodeWarmingUpTooLong', 'PeatSafekeeperReplacementBlocked', 'PeatSafekeeperReplacementOpen', 'PeatSafekeeperTrimBehind', 'PeatUploadsLagging'] as const
/** The names of the platform's alerts, read from its rule file when one is given, else the platform's own list. */
export function peatAlertNames(file: string | null = null): string[] {
  if (!file) return [...PEAT_ALERTS]
  return [...readFileSync(file, 'utf8').matchAll(/^\s*- alert: (Peat\w+)\s*$/gm)].map((m) => m[1]!)
}
/** The generic workload alerts a drill's own cuts and rolls can raise about the platform's pods. Silenced only with a `namespace` of the platform's four. */
export const PLATFORM_WORKLOAD_ALERTS = ['WorkloadNotAvailable', 'ScrapeTargetDown', 'StatefulSetNotReady', 'PodRestartingInALoop', 'PodCrashLoopBackOff'] as const
export interface Matcher { name: string; value: string; isRegex: boolean; isEqual: boolean }
/**
 * The only matchers a silence of a drill ever carries: `alertname=~"Peat.*"` (less the alerts a drill is testing: `alertname!="X"` each), the platform's workload alerts by namespace, and for a machine cut that machine's
 * node alerts, `node="<machine>"`, as a silence of its own (the matchers of one silence are ANDed). The machine is checked as the cut checks it, and an excluded name must be one of the platform's alerts.
 */
export function silenceMatchers(opts: { machine?: string; exclude?: readonly string[]; known?: readonly string[]; skipPeat?: boolean; machines: readonly string[]; namespaces: readonly string[] }): Matcher[][] {
  const known = opts.known ?? peatAlertNames()
  for (const e of opts.exclude ?? []) if (!known.includes(e)) throw new Refusal(`${JSON.stringify(e)} is not one of the platform's alerts, so a silence may not leave it out`)
  const out: Matcher[][] = opts.skipPeat ? [] : [
    [
      { name: 'alertname', value: 'Peat.*', isRegex: true, isEqual: true },
      ...(opts.exclude ?? []).map((e) => ({ name: 'alertname', value: e, isRegex: false, isEqual: false })),
    ],
  ]
  // The platform's own workloads' alerts, by NAMESPACE (the four of the platform and nothing wider): a drill that rolls or cuts these pods makes them pend, and a slow roll must not page anyone. Other work's alerts carry other namespaces and are not matched.
  if (!opts.skipPeat) out.push([
    { name: 'alertname', value: PLATFORM_WORKLOAD_ALERTS.join('|'), isRegex: true, isEqual: true },
    { name: 'namespace', value: opts.namespaces.join('|'), isRegex: true, isEqual: true },
  ])
  if (opts.machine !== undefined) {
    assertMachine(opts.machine, opts.machines)
    // `node` is what the kubelet and cadvisor targets and kube-state-metrics' node series carry (NodeNotReady, ScrapeTargetDown, the pressure alerts); the targets' `instance` is an address, so no matcher is made for it
    out.push([{ name: 'node', value: opts.machine, isRegex: false, isEqual: true }])
  }
  return out
}
export const SILENCE_MINUTES = 30

/**
 * A drill that cuts a machine, cuts the store or loads the uplink is refused if its window, padded by `CLEAR_MINUTES` before and after,
 * would meet one of the install's quiet hours (src/quiet-hours.ts: the windows in which the install's own scheduled work runs, declared in
 * its Platform). A window's end is where that work is declared to be done, so no estimate of its length is added. No window declared: no
 * refusal on the clock.
 */
export const CLEAR_MINUTES = 30
export function quietConflict(now: Date, minutes: number, windows: Window[]): string | null {
  const from = new Date(Math.floor(now.getTime() / 60_000) * 60_000 - CLEAR_MINUTES * 60_000)
  const hit = meets(windows, from, minutes + 2 * CLEAR_MINUTES)
  if (!hit) return null
  const off = Math.round((hit.at.getTime() - now.getTime()) / 60_000)
  return off > 0
    ? `the quiet hours ${describeWindow(hit.window)} begin ${off} minutes into a ${minutes} minute window (the window must end ${CLEAR_MINUTES} minutes before them)`
    : `the quiet hours ${describeWindow(hit.window)} ${off === 0 ? 'are now' : `were on ${-off} minutes ago`} (a window must start ${CLEAR_MINUTES} minutes after they end)`
}

// ---- the doors to the outside

export interface Result { code: number; out: string; err: string }
export interface Exec {
  run(argv: string[], o?: { stdin?: string; timeoutMs?: number; cwd?: string }): Promise<Result>
  runSync(argv: string[]): Result
}
export class RealExec implements Exec {
  private env: Record<string, string | undefined>
  /** `kubeconfig`: the file every command uses (the drills' `--kubeconfig`); unset, the shell's KUBECONFIG. */
  constructor(kubeconfig: string | null = null) { this.env = { ...process.env, ...(kubeconfig ? { KUBECONFIG: kubeconfig } : {}) } }
  async run(argv: string[], o: { stdin?: string; timeoutMs?: number; cwd?: string } = {}): Promise<Result> {
    const p = Bun.spawn(argv, { env: this.env, cwd: o.cwd, stdin: o.stdin !== undefined ? new TextEncoder().encode(o.stdin) : 'ignore', stdout: 'pipe', stderr: 'pipe', timeout: o.timeoutMs ?? 120_000 })
    const [out, err] = await Promise.all([new Response(p.stdout).text(), new Response(p.stderr).text()])
    return { code: (await p.exited) ?? 1, out, err }
  }
  runSync(argv: string[]): Result {
    const p = Bun.spawnSync(argv, { env: this.env, stdout: 'pipe', stderr: 'pipe' })
    return { code: p.exitCode ?? 1, out: new TextDecoder().decode(p.stdout), err: new TextDecoder().decode(p.stderr) }
  }
}
export interface Clock { now(): number; sleep(ms: number): Promise<void> }
export const realClock: Clock = { now: () => Date.now(), sleep: (ms) => new Promise((r) => setTimeout(r, ms)) }

/** kubectl's read-only verbs: a read is anything else refused (a drill reads through these and through the control layer's own API, never by `exec` of its own words). */
const READ_VERBS = ['get', 'logs', 'top', 'describe', 'version', 'api-resources']

/**
 * THE PENDING FILE (`pending.json` in the state directory): each change's undo commands are written here BEFORE the change and removed when the undo succeeded, so a run that is killed (SIGKILL), whose
 * workstation slept, or whose connection dropped while an undo ran leaves a record. `run` refuses to start while it holds anything, and `recover` replays it. A command in the file is judged again by `assertReplayable`
 * before it is run, so a file that was edited cannot make the guard run anything but an undo of the kinds below.
 */
export interface UndoEntry { what: string; cmds: string[][]; soft?: boolean }
export const pendingPath = (dir: string) => join(dir, 'pending.json')
export function readPending(dir: string): UndoEntry[] {
  const f = pendingPath(dir)
  if (!existsSync(f)) return []
  try { return JSON.parse(readFileSync(f, 'utf8')) as UndoEntry[] } catch { throw new Refusal(`${f} cannot be read: look at it by hand, then remove it`) }
}
export function assertNoPending(dir: string): void {
  const p = readPending(dir)
  if (p.length > 0) throw new Refusal(`${p.length} change(s) of an earlier run were not undone (${p.map((e) => e.what).join('; ')}): run the drills' \`recover\` first`)
}
const AM_URL = 'http://localhost:9093'
const KUBE_KINDS = ['pod', 'configmap', 'networkpolicy', 'cluster', 'persistentvolume', 'job']
/** What a replayed command is judged against: the machines, the installer's command, the platform's namespaces and Alertmanager. */
export interface Replay { machines: readonly string[]; machineCommand: readonly string[] | null; namespaces: readonly string[]; alertmanager: { namespace: string; name: string } | null }
export function assertReplayable(cmd: string[], r: Replay): void {
  const bad = () => { throw new Refusal(`${JSON.stringify(cmd).slice(0, 200)} is not an undo the guard makes`) }
  if (!Array.isArray(cmd) || cmd.some((c) => typeof c !== 'string')) bad()
  if (cmd[0] !== 'kubectl') {
    // the one other undo: a machine started again by the installer's command, for one of the platform's machines
    if (!r.machineCommand || !r.machines.some((m) => JSON.stringify(machineCommand(r.machineCommand!, 'start', m)) === JSON.stringify(cmd))) bad()
    return
  }
  if (r.alertmanager && cmd[1] === '-n' && cmd[2] === r.alertmanager.namespace && cmd[3] === 'exec') {
    const head = ['kubectl', '-n', r.alertmanager.namespace, 'exec', `deploy/${r.alertmanager.name}`, '--', 'amtool', `--alertmanager.url=${AM_URL}`, 'silence', 'expire']
    if (cmd.length !== 11 || head.some((h, k) => cmd[k] !== h) || !/^[0-9a-f-]{8,40}$/.test(cmd[10]!)) bad()
    return
  }
  let i = 1
  let ns: string | null = null
  if (cmd[1] === '-n') { ns = cmd[2]!; assertNamespace(ns, r.namespaces); i = 3 }
  const verb = cmd[i]
  if (verb === 'scale') {
    if (!ns || cmd.length !== i + 3 || !/^(statefulset|deployment)\/[a-z0-9-]+$/.test(cmd[i + 1]!) || !/^--replicas=[0-3]$/.test(cmd[i + 2]!)) bad()
    return
  }
  if (verb === 'patch') {
    const ok = ns && cmd.length === i + 6 && ['statefulset', 'deployment', 'networkpolicy'].includes(cmd[i + 1]!) && DNS_NAME.test(cmd[i + 2]!) && cmd[i + 3] === '--type=json' && cmd[i + 4] === '-p'
    if (!ok) bad()
    let ops: any
    try { ops = JSON.parse(cmd[i + 5]!) } catch { bad() }
    if (!Array.isArray(ops) || ops.length !== 1 || !['replace', 'add'].includes(ops[0].op) || !['/spec/template/spec/containers/0/env', '/spec/egress'].includes(ops[0].path)) bad()
    return
  }
  if (verb === 'delete') {
    const kind = cmd[i + 1], name = cmd[i + 2], flags = cmd.slice(i + 3)
    if (!KUBE_KINDS.includes(kind!) || !DNS_NAME.test(name ?? '') || flags.some((f) => !['--ignore-not-found', '--wait=false'].includes(f))) bad()
    if (kind === 'persistentvolume') { if (ns !== null || name !== RESTORED_CLUSTER) bad(); return }
    if (!ns) bad()
    if (kind === 'cluster') { if (name !== RESTORED_CLUSTER) bad(); return }
    if (!name!.startsWith('drill-')) bad()
    return
  }
  bad()
}

/** Whether a storage piece's live egress still lets it reach the store: it holds the rule with an address block (the internet). The store drills take that rule out; a policy without it is their leftover. */
export function egressReachesStore(live: any[] | undefined): boolean {
  return (live ?? []).some((r) => (r?.to ?? []).some((t: any) => t?.ipBlock))
}

interface Action { what: string; argv?: string[] }
export interface GuardOptions { dry?: boolean; clock?: Clock; log?: (line: string) => void; stateDir?: string | null; settings?: DrillSettings }

export class Guard {
  readonly actions: Action[] = []
  private undo: UndoEntry[] = []
  private readonly clock: Clock
  private readonly dry: boolean
  private readonly say: (l: string) => void
  readonly settings: DrillSettings
  readonly names: Names
  readonly namespaces: string[]
  /** Where the pending file lives; null keeps the undo log in memory only (dry runs and tests that do not look at it). A live run always has one. */
  readonly stateDir: string | null
  private shapeRead: Shape | null = null
  constructor(private readonly x: Exec, o: GuardOptions = {}) {
    this.clock = o.clock ?? realClock
    this.dry = o.dry ?? false
    this.say = o.log ?? ((l) => console.log(l))
    this.settings = o.settings ?? drillSettings()
    this.names = this.settings.names
    this.namespaces = namespacesOf(this.names)
    this.stateDir = this.dry ? null : o.stateDir === undefined ? this.settings.stateDir : o.stateDir
  }
  now() { return this.clock.now() }
  sleep(ms: number) { return this.clock.sleep(ms) }
  log(l: string) { this.say(`${new Date(this.clock.now()).toISOString().slice(11, 19)} ${l}`) }
  private get ns() { return this.names.namespace }
  /** What a replayed command is judged against, from what the guard knows now (the machines it read, or none yet). */
  private replay(): Replay { return { machines: this.shapeRead?.machines ?? [], machineCommand: this.settings.machineCommand, namespaces: this.namespaces, alertmanager: this.settings.alertmanager } }

  // ---- reads

  /** A kubectl read (get, logs, top, describe): returns stdout, or throws with stderr. */
  async kread(args: string[], stdin?: string): Promise<string> {
    const verbAt = args[0] === '-n' ? 2 : 0
    if (!READ_VERBS.includes(args[verbAt] ?? '')) throw new Refusal(`kubectl ${args.slice(0, 3).join(' ')} is not a read`)
    if (args.some((a) => a === '--raw') && !args.some((a) => a.startsWith('/api'))) throw new Refusal('a raw read names an /api path')
    const r = await this.x.run(['kubectl', ...args], { stdin })
    if (r.code !== 0) throw new Error(`kubectl ${args.join(' ').slice(0, 160)}: ${r.err.trim().slice(0, 300)}`)
    return r.out
  }
  async kjson<T = any>(args: string[]): Promise<T> { return JSON.parse(await this.kread([...args, '-o', 'json'])) as T }

  /**
   * The platform as it is now, read once and kept for the guard's life (`fresh` reads it again): its machines (the nodes with the machine label), which hold which volume, the outside machines that hold a
   * safekeeper, the pageservers and the images. Read before any machine or volume is judged, so the rules are the cluster's, never a list in a file.
   */
  async platform(fresh = false): Promise<Shape> {
    if (this.shapeRead && !fresh) return this.shapeRead
    const label = this.names.machineLabel
    const nodes = await this.kjson<any>(['get', 'nodes', '-l', `${label.key}=${label.value}`])
    const machines = ((nodes.items ?? []) as any[]).filter((n) => n?.metadata?.labels?.[label.key] === label.value).map((n) => String(n.metadata.name)).sort()
    const pods = ((await this.kjson<any>(['-n', this.ns, 'get', 'pods'])).items ?? []) as any[]
    const where = (p: any): string => p?.spec?.nodeName || p?.spec?.nodeSelector?.['kubernetes.io/hostname'] || ''
    const app = (p: any) => p?.metadata?.labels?.app
    const pageservers = pods.filter((p) => app(p) === 'pageserver').map((p) => ({ id: Number(/^pageserver-w(\d+)-0$/.exec(String(p.metadata.name))?.[1]), machine: where(p) })).filter((p) => Number.isInteger(p.id) && p.id > 0 && p.machine).sort((a, b) => a.id - b.id)
    const outside: Record<string, Toleration[]> = {}
    for (const p of pods.filter((p) => app(p) === 'safekeeper')) { const m = where(p); if (m && !machines.includes(m)) outside[m] = [...(p.spec?.tolerations ?? [])].filter((t: any) => !String(t.key ?? '').startsWith('node.kubernetes.io/')) }
    const v = this.settings.volumes
    const cdb = this.settings.controllerDatabase
    const volumeMachines: Record<string, string[]> = {
      [v.pageserver]: [...new Set(pageservers.map((p) => p.machine))].filter((m) => machines.includes(m)).sort(),
      [v.safekeeper]: machines,
      [v.controllerDatabase]: cdb && machines.includes(cdb.machine) ? [cdb.machine] : [],
    }
    const storagePod = pods.find((p) => ['pageserver', 'safekeeper', 'storage-controller'].includes(app(p)))
    const storageImage = String(storagePod?.spec?.containers?.[0]?.image ?? '')
    const control = await this.kjson<any>(['-n', this.ns, 'get', 'deployment', 'peat-control']).catch(() => null)
    const env = (control?.spec?.template?.spec?.containers?.[0]?.env ?? []) as { name: string; value?: string }[]
    const computeImage = process.env['PEAT_DRILL_CLIENT_IMAGE'] || env.find((e) => e.name === 'COMPUTE_IMAGE_PG19')?.value || env.find((e) => e.name === 'COMPUTE_IMAGE')?.value || ''
    this.shapeRead = { machines, volumeMachines, outside, pageservers, storageImage, computeImage }
    return this.shapeRead
  }

  /** A GET through the API server's proxy to a Service of the monitoring (Prometheus, Alertmanager): read only, and only when the install named it. */
  async monitoringGet(which: 'prometheus' | 'alertmanager', path: string): Promise<any> {
    if (!path.startsWith('/') || path.includes('..')) throw new Refusal('a monitoring path is absolute and plain')
    const s = which === 'prometheus' ? this.settings.prometheus : this.settings.alertmanager ? { namespace: this.settings.alertmanager.namespace, service: `${this.settings.alertmanager.name}:9093` } : null
    if (!s) throw new Error(`no ${which} is named for the drills (PEAT_DRILL_${which.toUpperCase()})`)
    return JSON.parse(await this.kread(['get', '--raw', `/api/v1/namespaces/${s.namespace}/services/${s.service}/proxy${path}`]))
  }
  /** Alertmanager's own counters of the notifications it tried to send, and of those that failed: read from its /metrics through the API server's proxy. */
  async notificationCounters(): Promise<{ asked: number; failed: number }> {
    const am = this.settings.alertmanager
    if (!am) throw new Error('no Alertmanager is named for the drills (PEAT_DRILL_ALERTMANAGER)')
    const t = await this.kread(['get', '--raw', `/api/v1/namespaces/${am.namespace}/services/${am.name}:9093/proxy/metrics`])
    const sum = (name: string) => { let n = 0, seen = false; for (const m of t.matchAll(new RegExp(`^${name}(?:\\{[^}]*\\})? ([0-9.e+]+)$`, 'gm'))) { n += Number(m[1]); seen = true } return seen ? n : NaN }
    const asked = sum('alertmanager_notifications_total'), failed = sum('alertmanager_notifications_failed_total')
    if (Number.isNaN(asked) || Number.isNaN(failed)) throw new Error("Alertmanager's notification counters could not be read")
    return { asked, failed }
  }
  async prom(query: string): Promise<{ metric: Record<string, string>; value: number }[]> {
    const j = await this.monitoringGet('prometheus', `/api/v1/query?query=${encodeURIComponent(query)}`)
    return (j.data.result as any[]).map((r) => ({ metric: r.metric, value: Number(r.value[1]) }))
  }

  /**
   * A call to the control layer's API from the control pod's own loopback (not narrowed there), by a fixed script: GET anything of the routes below, POST the database routes, DELETE only a drill's database. Returns the
   * response's status and body. `peat-control` answers from the leader or the follower alike (every call is served by both).
   */
  async control(method: 'GET' | 'POST' | 'DELETE', path: string, body?: unknown): Promise<{ status: number; json: any }> {
    const ok =
      (method === 'GET' && /^\/(dbs|clusters|compute-addrs|reconcile|sk-audit|sk-replace|healthz|metrics|db\/[a-z][a-z0-9-]{0,30}|compute\/current\?name=[a-z0-9-]+&pod=[a-z0-9-]+)$/.test(path)) ||
      (method === 'POST' && /^\/(db|branch|gateway\/lease|db\/drill[a-z0-9-]{0,26}\/(start|stop)|db\/drill[a-z0-9-]{0,26}\/recover(\?dry=1)?|sk\/[0-9]{1,4}\/decommission(\?force=1)?)$/.test(path)) ||
      (method === 'DELETE' && /^\/db\/drill[a-z0-9-]{0,26}$/.test(path))
    if (!ok) throw new Refusal(`${method} ${path} is not a call a drill may make to the control layer`)
    // a drill makes only databases with the reserved prefix (the control layer refuses that prefix to anyone who does not send the drills' header, which the script below does)
    if (method === 'POST' && (path === '/db' || path === '/branch') && !/^drill[a-z0-9-]{0,26}$/.test(String((body as any)?.name ?? ''))) throw new Refusal(`a drill makes only databases named drill*, not ${JSON.stringify((body as any)?.name)}`)
    if (method !== 'GET') { this.actions.push({ what: `control ${method} ${path}` }); this.log(`CONTROL ${method} ${path}`); if (this.dry) return { status: 200, json: { tenant: 'a'.repeat(32), timeline: 'b'.repeat(32) } } }
    const script = `const r = await fetch('http://localhost:8080${path}', { method: ${JSON.stringify(method)}, headers: { 'x-peat-drill': '1', 'content-type': 'application/json' }, ${body !== undefined ? `body: ${JSON.stringify(JSON.stringify(body))}, ` : ''}signal: AbortSignal.timeout(110000) }); let j; const t = await r.text(); try { j = JSON.parse(t) } catch { j = t } console.log(JSON.stringify({ status: r.status, json: j }))`
    // a recovery and a decommission run on the acting control layer only (the other pod answers 409 naming the lease's holder); a dry recovery runs on either
    const leaderOnly = method === 'POST' && /^\/(db\/[a-z0-9-]+\/recover|sk\/[0-9]+\/decommission)(\?force=1)?$/.test(path)
    const target = leaderOnly ? `pod/${(await this.kread(['-n', this.ns, 'get', 'lease', 'peat-control-leader', '-o', 'jsonpath={.spec.holderIdentity}'])).trim()}` : 'deploy/peat-control'
    const r = await this.x.run(['kubectl', '-n', this.ns, 'exec', target, '--', 'bun', '-e', script], { timeoutMs: 130_000 })
    if (r.code !== 0) throw new Error(`control ${method} ${path}: ${r.err.trim().slice(0, 300)}`)
    return JSON.parse(r.out.trim().split('\n').pop()!)
  }

  /** The script the control pod runs to call a storage piece with a token minted from its own key (the scope named): one request, its status and body printed. */
  private storageScript(scope: string, url: string, method = 'GET', body?: unknown): string {
    const send = method === 'GET' ? `{headers:{authorization:'Bearer '+tok},signal:AbortSignal.timeout(15000)}` : `{method:${JSON.stringify(method)},headers:{authorization:'Bearer '+tok,'content-type':'application/json'},body:${JSON.stringify(JSON.stringify(body))},signal:AbortSignal.timeout(60000)}`
    return `import {createPrivateKey,sign} from 'node:crypto'; import {readFileSync} from 'node:fs'; const k=createPrivateKey(readFileSync('/keys/auth_private_key.pem','utf8')); const b=(x)=>Buffer.from(x).toString('base64url'); const h=b(JSON.stringify({alg:'EdDSA',typ:'JWT'})),c=b(JSON.stringify({scope:${JSON.stringify(scope)}})); const tok=h+'.'+c+'.'+b(sign(null,Buffer.from(h+'.'+c),k)); const r=await fetch(${JSON.stringify(url)},${send}); const t=await r.text(); let j; try{j=JSON.parse(t)}catch{j=t} console.log(JSON.stringify({status:r.status,json:j}))`
  }
  /**
   * A GET to a storage piece inside the cluster, by the control pod with a token minted from its own key (the scope named): the storage controller's `/control/v1/...`, a pageserver's or a safekeeper's HTTP API. The host must
   * be one of the platform's Services in its namespace; only GET, so a read can never be a cut.
   */
  async storage(scope: 'admin' | 'pageserverapi' | 'safekeeperdata', url: string): Promise<any> {
    const ns = this.ns.replace(/[.-]/g, (c) => `\\${c}`)
    if (!new RegExp(`^http://[a-z0-9.-]+\\.${ns}\\.svc(\\.cluster\\.local)?:\\d+/[A-Za-z0-9_./?=&%-]*$`).test(url) || url.includes('..')) throw new Refusal(`${JSON.stringify(url)} is not a read of a platform piece in ${this.ns}`)
    const r = await this.x.run(['kubectl', '-n', this.ns, 'exec', 'deploy/peat-control', '--', 'bun', '-e', this.storageScript(scope, url)], { timeoutMs: 30_000 })
    if (r.code !== 0) throw new Error(`storage GET ${url}: ${r.err.trim().slice(0, 200)}`)
    const j = JSON.parse(r.out.trim().split('\n').pop()!)
    if (j.status >= 300) throw new Error(`storage GET ${url}: ${j.status} ${JSON.stringify(j.json).slice(0, 200)}`)
    return j.json
  }

  /**
   * The two things a drill runs INSIDE one of its own client pods (`drill-*` in the computes' namespace): the pod's clock, and one `psql` statement against a platform database through the front door (the host must be a
   * `<name>.<databases' namespace>.svc`). The statement runs as the database's own `app` login in that one database; it cannot name a machine, a namespace or a volume.
   */
  async clientClock(pod: string): Promise<string> {
    assertName(pod, 'pod'); if (!pod.startsWith('drill-')) throw new Refusal('only a drill client pod')
    const r = await this.x.run(['kubectl', '-n', this.names.computeNamespace, 'exec', pod, '--', 'date', '+%s%3N'], { timeoutMs: 20_000 })
    if (r.code !== 0) throw new Error(`clock of ${pod}: ${r.err.trim().slice(0, 200)}`)
    return r.out
  }
  async clientPsql(pod: string, c: { host: string; password: string; seconds: number; hostaddr?: string }, sql: string): Promise<string> {
    assertName(pod, 'pod'); if (!pod.startsWith('drill-')) throw new Refusal('only a drill client pod')
    const host = /^([a-z][a-z0-9-]{0,30})\.([a-z0-9-]+)\.svc$/.exec(c.host)
    if (!host || host[2] !== this.names.dbNamespace) throw new Refusal(`${c.host} is not a platform database's name in ${this.names.dbNamespace}`)
    if (!Number.isInteger(c.seconds) || c.seconds < 1 || c.seconds > 600) throw new Refusal('a plain timeout')
    if (c.hostaddr !== undefined) {
      const gws = await this.kjson<any>(['-n', this.names.gatewayNamespace, 'get', 'pods'])
      if (!(gws.items as any[]).some((p) => p.status.podIP === c.hostaddr)) throw new Refusal(`${c.hostaddr} is not the address of a gateway pod`)
    }
    // the password goes in on stdin (one line, read by the pod's shell), never in an argument that `ps` on the workstation would show
    const script = c.hostaddr !== undefined ? 'read -r PGPASSWORD; export PGPASSWORD; exec env PGHOST="$0" PGCONNECT_TIMEOUT="$1" PGHOSTADDR="$2" psql -qAtX -c "$3"' : 'read -r PGPASSWORD; export PGPASSWORD; exec env PGHOST="$0" PGCONNECT_TIMEOUT="$1" psql -qAtX -c "$2"'
    const args = c.hostaddr !== undefined ? [c.host, String(c.seconds), c.hostaddr, sql] : [c.host, String(c.seconds), sql]
    const r = await this.x.run(['kubectl', '-n', this.names.computeNamespace, 'exec', '-i', pod, '--', 'sh', '-c', script, ...args], { stdin: `${c.password}\n`, timeoutMs: (c.seconds + 30) * 1000 })
    if (r.code !== 0) throw new Error(`psql ${c.host}: ${r.err.trim().slice(0, 300)}`)
    return r.out.trim()
  }

  /**
   * A pageserver told to detach one tenant (which removes its local data) WITHOUT telling the storage controller: the partial-loss drill. Only one of the platform's pageservers (by the id its pod carries), only a 32 hex
   * tenant, the one PUT of the guard's storage calls, by the control pod with a token of scope `pageserverapi`.
   */
  async detachTenant(node: number, tenant: string): Promise<void> {
    const shape = await this.platform()
    if (!Number.isInteger(node) || !shape.pageservers.some((p) => p.id === node)) throw new Refusal(`pageserver ${node} is not one of the platform's pageservers (${shape.pageservers.map((p) => p.id).join(', ')})`)
    if (!/^[0-9a-f]{32}$/.test(tenant)) throw new Refusal('a tenant id is 32 hex digits')
    const url = `http://pageserver-w${node}-0.pageserver.${this.ns}.svc:9898/v1/tenant/${tenant}-0000/location_config`
    this.actions.push({ what: `DETACH tenant ${tenant} on pageserver ${node}` })
    this.log(`DETACH tenant ${tenant} on pageserver ${node} (the controller is not told)`)
    if (this.dry) return
    const r = await this.x.run(['kubectl', '-n', this.ns, 'exec', 'deploy/peat-control', '--', 'bun', '-e', this.storageScript('pageserverapi', url, 'PUT', { mode: 'Detached', tenant_conf: {} })], { timeoutMs: 90_000 })
    const j = r.code === 0 ? JSON.parse(r.out.trim().split('\n').pop() || '{}') : null
    if (!j || !(j.status >= 200 && j.status < 300)) throw new Error(`detach: ${r.out.trim().slice(0, 200)} ${r.err.trim().slice(0, 200)}`)
  }
  /** A silence on ONE of the platform's alerts (30 minutes), made when an alert drill sees it active in Alertmanager so that it is proven to arrive and not sent. */
  async silenceAlert(name: string, comment: string): Promise<string> {
    if (!peatAlertNames(this.settings.rulesFile).includes(name)) throw new Refusal(`${JSON.stringify(name)} is not one of the platform's alerts`)
    return this.amtoolSilence([{ name: 'alertname', value: name, isRegex: false, isEqual: true }], comment)
  }

  /**
   * A short `checkpoint_timeout` on ONE drill database's tenant (the platform's default, 10 minutes, is left alone): the storage controller's tenant config. Only a database named drill*, only 10 to 120 seconds. The
   * pageserver then flushes and uploads a write within about a minute, so "flushed" follows a load; the setting goes with the tenant when the database is deleted.
   */
  async setTenantCheckpoint(db: string, seconds: number): Promise<void> {
    if (!/^drill[a-z0-9-]{0,26}$/.test(db)) throw new Refusal(`only a drill database's tenant: ${JSON.stringify(db)}`)
    if (!Number.isInteger(seconds) || seconds < 10 || seconds > 120) throw new Refusal('a checkpoint timeout of 10 to 120 seconds')
    const info = await this.control('GET', `/db/${db}`)
    const tenant = info.json?.tenant as string | undefined
    if (info.status !== 200 || !/^[0-9a-f]{32}$/.test(tenant ?? '')) throw new Error(`no database ${db}`)
    this.actions.push({ what: `tenant config of ${db}: checkpoint_timeout ${seconds}s` })
    this.log(`TENANT CONFIG ${db} (${tenant}): checkpoint_timeout ${seconds}s`)
    if (this.dry) return
    const url = `http://storage-controller.${this.ns}.svc:1234/v1/tenant/config`
    const r = await this.x.run(['kubectl', '-n', this.ns, 'exec', 'deploy/peat-control', '--', 'bun', '-e', this.storageScript('pageserverapi', url, 'PUT', { tenant_id: tenant, checkpoint_timeout: `${seconds}s` })], { timeoutMs: 90_000 })
    const j = r.code === 0 ? JSON.parse(r.out.trim().split('\n').pop() || '{}') : null
    if (!j || !(j.status >= 200 && j.status < 300)) throw new Error(`tenant config: ${r.out.trim().slice(0, 200)} ${r.err.trim().slice(0, 200)}`)
  }

  // ---- the undo log

  /** The undo entry goes into the log AND the pending file before the change it undoes is made. */
  private pushUndo(what: string, cmds: string[][], soft = false) {
    for (const c of cmds) assertReplayable(c, this.replay())
    this.undo.push({ what, cmds, soft })
    this.persist()
  }
  private persist() {
    if (!this.stateDir) return
    mkdirSync(this.stateDir, { recursive: true, mode: 0o700 })
    const f = pendingPath(this.stateDir)
    if (this.undo.length === 0) { if (existsSync(f)) unlinkSync(f) } else writeFileSync(f, JSON.stringify(this.undo, null, 1), { mode: 0o600 })
  }
  /** Runs one entry's commands, each tried up to three times (5 s apart); true when all succeeded. `already running` and a missing object count as done. */
  private async runEntry(u: UndoEntry): Promise<boolean> {
    for (const cmd of u.cmds) {
      assertReplayable(cmd, this.replay())
      this.actions.push({ what: `undo ${u.what}`, argv: cmd })
      if (this.dry) continue
      let done = false
      for (let attempt = 1; attempt <= 3 && !done; attempt++) {
        const r = await this.x.run(cmd).catch((e) => ({ code: 1, out: '', err: String(e) }))
        done = r.code === 0 || /already running|not ?found/i.test(r.err)
        if (!done && attempt < 3) await this.clock.sleep(5000)
      }
      if (!done) return false
    }
    return true
  }
  /** Everything the drill changed, undone in reverse order. An entry whose undo fails three times STAYS in the log and the pending file, and `recover` replays it. */
  async undoAll(): Promise<void> { await this.undoSince(0) }
  /** A place in the undo log: `undoSince(mark)` undoes only what was changed after it (the store back, without taking the drill's clients away). */
  undoMark(): number { return this.undo.length }
  async undoSince(mark: number): Promise<void> {
    const stuck: UndoEntry[] = []
    while (this.undo.length > mark) {
      const u = this.undo.pop()!
      this.log(`undo: ${u.what}`)
      if (await this.runEntry(u)) this.persistWith(stuck)
      else if (u.soft) this.log(`undo (soft) failed, dropped: ${u.what}`)
      else { this.log(`undo FAILED, kept in the pending file: ${u.what}`); stuck.push(u) }
    }
    for (const u of stuck.reverse()) this.undo.push(u)
    this.persist()
  }
  private persistWith(stuck: UndoEntry[]) {
    const keep = this.undo
    this.undo = [...keep, ...stuck.slice().reverse()]
    this.persist()
    this.undo = keep
  }
  /** The synchronous form, for process exit: each entry's commands run once, blocking; what could not be done stays in the pending file for `recover`. */
  undoSync(): void {
    const stuck: UndoEntry[] = []
    while (this.undo.length > 0) {
      const u = this.undo.pop()!
      let ok = true
      for (const cmd of u.cmds) { try { assertReplayable(cmd, this.replay()); const r = this.x.runSync(cmd); if (r.code !== 0 && !/already running|not ?found/i.test(r.err)) ok = false } catch { ok = false } }
      if (!ok && !u.soft) stuck.push(u)
    }
    this.undo = stuck.reverse()
    try { this.persist() } catch { /* exit */ }
  }
  get pending(): string[] { return this.undo.map((u) => u.what) }
  // ---- recovery

  /** Loads what an earlier run left in the pending file into the undo log (each command is judged again, against the machines read now, when it runs). */
  async loadPending(): Promise<UndoEntry[]> {
    await this.platform()
    const p = readPending(this.stateDir ?? this.settings.stateDir)
    for (const u of p) for (const c of u.cmds) assertReplayable(c, this.replay())
    this.undo = [...p, ...this.undo]
    return p
  }
  /** Deletes what a drill makes and an interrupted one may have left: `drill-*` pods, ConfigMaps, NetworkPolicies and Jobs in the platform's namespaces, the restored Cluster and its PersistentVolume and directory, and the drills' databases. */
  async sweepDrillObjects(): Promise<string[]> {
    const found: string[] = []
    const kinds: [string, 'Pod' | 'ConfigMap' | 'NetworkPolicy' | 'Job'][] = [['pods', 'Pod'], ['configmaps', 'ConfigMap'], ['networkpolicies', 'NetworkPolicy'], ['jobs', 'Job']]
    for (const ns of this.namespaces) for (const [plural, kind] of kinds) {
      const j = await this.kjson<any>(['-n', ns, 'get', plural]).catch(() => ({ items: [] }))
      for (const it of (j.items ?? []) as any[]) if (String(it.metadata.name).startsWith('drill-')) { await this.deleteObject(ns, kind, it.metadata.name); found.push(`${ns}/${kind}/${it.metadata.name}`) }
    }
    const clusters = await this.kjson<any>(['-n', this.ns, 'get', 'clusters.postgresql.cnpg.io']).catch(() => ({ items: [] }))
    const pvs = await this.kjson<any>(['get', 'persistentvolumes']).catch(() => ({ items: [] }))
    const restored = ((clusters.items ?? []) as any[]).some((c) => c.metadata.name === RESTORED_CLUSTER), drillPv = ((pvs.items ?? []) as any[]).filter((v) => String(v.metadata.name) === RESTORED_CLUSTER)
    if (restored) { await this.deleteObject(this.ns, 'Cluster', RESTORED_CLUSTER); found.push(`${this.ns}/Cluster/${RESTORED_CLUSTER}`) }
    for (const v of drillPv) { await this.deleteObject(null, 'PersistentVolume', v.metadata.name); found.push(`PersistentVolume/${v.metadata.name}`) }
    const cdb = this.settings.controllerDatabase
    if ((restored || drillPv.length) && cdb) { await this.sleep(30_000); await this.wipeVolume(cdb.machine, `${this.settings.volumes.controllerDatabase}/restored`) }
    const dbs = await this.control('GET', '/dbs').catch(() => null)
    for (const d of ((dbs?.json ?? []) as any[]).filter((x) => String(x.name).startsWith('drill') && x.name !== DRILL_BASE_DATABASE)) { await this.control('DELETE', `/db/${d.name}`); found.push(`database ${d.name}`) }
    return found
  }
  /** Starts every platform machine whose node is not Ready, by the installer's command (`start` of a machine that runs is the command's to accept). Nothing when no command is set. */
  async startStoppedMachines(): Promise<string[]> {
    if (!this.settings.machineCommand) return []
    const shape = await this.platform(true)
    const started: string[] = []
    for (const m of shape.machines) {
      const n = await this.kjson<any>(['get', 'node', m]).catch(() => null)
      if (n && (n.status?.conditions ?? []).some((c: any) => c.type === 'Ready' && c.status === 'True')) continue
      this.log(`START machine ${m} (its node is not Ready)`)
      await this.startMachineNow(m)
      started.push(m)
    }
    return started
  }
  /** Waits (reads only) until the cluster's node of that platform machine is Ready; false after `seconds`. */
  async waitNodeReady(name: string, seconds: number): Promise<boolean> {
    assertMachine(name, (await this.platform()).machines)
    if (this.dry) return true
    const end = this.clock.now() + seconds * 1000
    for (;;) {
      const n = await this.kjson<any>(['get', 'node', name]).catch(() => null)
      if (n && (n.status?.conditions ?? []).some((c: any) => c.type === 'Ready' && c.status === 'True')) return true
      if (this.clock.now() > end) { this.log(`node ${name} is not Ready after ${seconds} s`); return false }
      await this.clock.sleep(5000)
    }
  }
  /**
   * The one command that puts everything back: the pending file replayed, the drill objects swept, stopped machines started and waited for. Nothing of the platform's pieces is applied again: the operator keeps them as
   * declared, so a piece a drill left changed is put back by it.
   */
  async recover(): Promise<{ pendingLeft: string[]; swept: string[]; started: string[] }> {
    const pending = await this.loadPending()
    this.log(`pending: ${pending.length ? pending.map((p) => p.what).join('; ') : 'nothing'}`)
    await this.undoSince(0)
    const swept = await this.sweepDrillObjects()
    const started = await this.startStoppedMachines()
    for (const m of started) await this.waitNodeReady(m, 360)
    this.log('the platform\'s pieces are the operator\'s to keep as declared: nothing is applied again here')
    return { pendingLeft: this.pending, swept, started }
  }

  /** On SIGINT, SIGTERM, an uncaught error and exit: the undo log runs (a cut machine is started again by the script itself). */
  installTrap(): void {
    const sync = () => this.undoSync()
    process.on('exit', sync)
    for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP'] as const) process.on(sig, () => { this.undoSync(); process.exit(130) })
    process.on('uncaughtException', (e) => { console.error(e); this.undoSync(); process.exit(1) })
  }

  // ---- machines

  private command(action: 'stop' | 'start', machine: string): string[] {
    const t = this.settings.machineCommand
    if (!t) throw new Refusal('no machine command is set (PEAT_DRILL_MACHINE_COMMAND, with {action} and {machine}): a drill that cuts a machine cannot run')
    return machineCommand(t, action, machine)
  }
  /** Reads back, before anything is cut, that the command is set, that the name is one of the platform's machines, and that the cluster's node of that name carries the machine label and is Ready. */
  async verifyMachine(name: string): Promise<void> {
    if (!this.settings.machineCommand) throw new Refusal('no machine command is set (PEAT_DRILL_MACHINE_COMMAND, with {action} and {machine}): a drill that cuts a machine cannot run')
    if (typeof name !== 'string' || !DNS_NAME.test(name)) throw new Refusal(`${JSON.stringify(name)} is not a plain machine name`)
    assertMachine(name, (await this.platform()).machines)
    const label = this.names.machineLabel
    const node = JSON.parse(await this.kread(['get', 'node', name, '-o', 'json'])) as { metadata: { name: string; labels?: Record<string, string> }; status?: { conditions?: { type: string; status: string }[] } }
    if (node.metadata.name !== name || node.metadata.labels?.[label.key] !== label.value) throw new Refusal(`the cluster's node ${name} does not carry ${label.key}=${label.value}`)
    if (!(node.status?.conditions ?? []).some((c) => c.type === 'Ready' && c.status === 'True')) throw new Refusal(`the node ${name} is not Ready: nothing to cut`)
  }
  /** A power cut: the installer's command with `stop`, started again by the undo log (its `start`) whatever happens. */
  async cutMachine(name: string): Promise<void> {
    await this.verifyMachine(name)
    this.pushUndo(`start ${name} again`, [this.command('start', name)])
    const argv = this.command('stop', name)
    this.actions.push({ what: `cut ${name}`, argv })
    this.log(`CUT machine ${name} (${argv.join(' ')})`)
    if (!this.dry) {
      const r = await this.x.run(argv)
      if (r.code !== 0) throw new Error(`machine stop ${name}: ${r.err.trim().slice(0, 200)}`)
    }
  }
  private async startMachineNow(name: string): Promise<void> {
    assertMachine(name, (await this.platform()).machines)
    const argv = this.command('start', name)
    this.actions.push({ what: `start ${name}`, argv })
    if (this.dry) return
    const r = await this.x.run(argv)
    if (r.code !== 0 && !/already running/i.test(r.err)) throw new Error(`machine start ${name}: ${r.err.trim().slice(0, 200)}`)
  }
  /** Starts the machine now (the drill's own release); the undo entry for it is then a start of a running machine, which the installer's command must accept. */
  async startMachine(name: string): Promise<void> { this.log(`START machine ${name}`); await this.startMachineNow(name) }

  // ---- pods and workloads

  async killPod(ns: string, pod: string): Promise<void> {
    assertNamespace(ns, this.namespaces); assertName(pod, 'pod')
    const argv = ['kubectl', '-n', ns, 'delete', 'pod', pod, '--grace-period=0', '--force', '--wait=false']
    this.actions.push({ what: `kill ${ns}/${pod}`, argv })
    this.log(`KILL pod ${ns}/${pod}`)
    if (!this.dry) await this.x.run(argv)
  }
  /** Scales a StatefulSet or Deployment of the platform; the previous replica count is put back by the undo log. */
  async scale(ns: string, kind: 'statefulset' | 'deployment', name: string, replicas: number): Promise<void> {
    assertNamespace(ns, this.namespaces); assertName(name, kind)
    if (!['statefulset', 'deployment'].includes(kind) || !Number.isInteger(replicas) || replicas < 0 || replicas > 3) throw new Refusal(`cannot scale ${kind} to ${replicas}`)
    const cur = this.dry ? 1 : Number((await this.kread(['-n', ns, 'get', kind, name, '-o', 'jsonpath={.spec.replicas}'])).trim() || '1')
    const cmd = (n: number) => ['kubectl', '-n', ns, 'scale', `${kind}/${name}`, `--replicas=${n}`]
    if (!this.undo.some((u) => u.what === `scale ${ns}/${kind}/${name} back to ${cur}`)) this.pushUndo(`scale ${ns}/${kind}/${name} back to ${cur}`, [cmd(cur)])
    this.log(`SCALE ${ns}/${kind}/${name} to ${replicas} (was ${cur})`)
    this.actions.push({ what: `scale ${ns}/${kind}/${name} to ${replicas}` })
    if (!this.dry) { const r = await this.x.run(cmd(replicas)); if (r.code !== 0) throw new Error(`scale: ${r.err.trim().slice(0, 200)}`) }
  }
  /** A workload held at zero for a while after its pod was killed, then released (the undo log releases it too if the drill is interrupted). */
  async holdDown(ns: string, kind: 'statefulset' | 'deployment', name: string, seconds: number): Promise<void> {
    await this.scale(ns, kind, name, 0)
    await this.sleep(seconds * 1000)
    await this.release(ns, kind, name)
  }
  /** Puts back what `scale` of this workload changed (and takes its undo entry off). */
  async release(ns: string, kind: 'statefulset' | 'deployment', name: string): Promise<void> {
    const i = this.undo.findIndex((u) => u.what.startsWith(`scale ${ns}/${kind}/${name} back to`))
    if (i < 0) return
    const [u] = this.undo.splice(i, 1)
    this.log(`RELEASE ${ns}/${kind}/${name}`)
    if (await this.runEntry(u!)) this.persist()
    else { this.undo.splice(i, 0, u!); this.persist(); throw new Error(`could not put ${ns}/${kind}/${name} back: kept in the pending file`) }
  }
  /** A pod's labels changed with nothing killed (it is released from its Deployment and keeps running): the fence drill. */
  async relabelPod(ns: string, pod: string, labels: Record<string, string>): Promise<void> {
    assertNamespace(ns, this.namespaces); assertName(pod, 'pod')
    for (const [k, v] of Object.entries(labels)) if (!/^[a-z0-9-]+$/.test(k) || !/^[a-z0-9-]+$/.test(v)) throw new Refusal('a label is plain words')
    const argv = ['kubectl', '-n', ns, 'label', 'pod', pod, ...Object.entries(labels).map(([k, v]) => `${k}=${v}`), '--overwrite']
    this.actions.push({ what: `relabel ${ns}/${pod}`, argv })
    this.log(`RELABEL ${ns}/${pod} ${JSON.stringify(labels)}`)
    if (!this.dry) await this.x.run(argv)
  }

  // ---- objects a drill makes

  private allowedObject(ns: string | null, m: any): void {
    const kind = m?.kind as string, name = m?.metadata?.name as string
    assertName(name, `${kind} name`)
    const cdbRoot = this.settings.volumes.controllerDatabase
    if (kind === 'PersistentVolume') {
      if (name !== RESTORED_CLUSTER) throw new Refusal(`a PersistentVolume of a drill is named ${RESTORED_CLUSTER}, not ${name}`)
      const p = m.spec?.hostPath?.path
      if (typeof p !== 'string' || !(p === cdbRoot || p.startsWith(`${cdbRoot}/`)) || p.includes('..')) throw new Refusal('a drill PersistentVolume is a hostPath on the controller database\'s volume')
      if (m.spec?.hostPath?.type !== 'Directory') throw new Refusal('a hostPath is of type Directory')
      return
    }
    assertNamespace(ns, this.namespaces)
    if (m.metadata?.namespace !== ns) throw new Refusal('the object names another namespace than the one it is applied in')
    if (['Pod', 'ConfigMap', 'NetworkPolicy'].includes(kind)) {
      if (!name.startsWith('drill-')) throw new Refusal(`a ${kind} of a drill is named drill-*, not ${name}`)
      if (kind === 'Pod') {
        const sel = m.spec?.nodeSelector ?? {}
        const label = this.names.machineLabel
        if (sel[label.key] !== label.value) throw new Refusal('a drill pod selects the platform machines')
        if (JSON.stringify(m.spec).includes('hostPath') || m.spec?.hostNetwork || m.spec?.containers?.some((c: any) => c.securityContext?.privileged)) throw new Refusal('a drill pod has no hostPath, host network or privilege')
      }
      return
    }
    if (kind === 'Cluster' && name === RESTORED_CLUSTER && ns === this.ns) return
    throw new Refusal(`a drill may not make a ${kind} named ${name} in ${ns}`)
  }
  /** Creates (or replaces) one object a drill needs; the undo log deletes it. */
  async apply(ns: string | null, m: any, o: { keep?: boolean } = {}): Promise<void> {
    this.allowedObject(ns, m)
    const kind = (m.kind as string).toLowerCase(), name = m.metadata.name as string
    const nsArgs = ns ? ['-n', ns] : []
    this.actions.push({ what: `apply ${ns ?? '-'}/${m.kind}/${name}` })
    this.log(`APPLY ${m.kind} ${ns ?? ''}/${name}`)
    if (!o.keep) this.pushUndo(`delete ${m.kind} ${ns ?? ''}/${name}`, [['kubectl', ...nsArgs, 'delete', kind, name, '--ignore-not-found', '--wait=false']])
    if (!this.dry) { const r = await this.x.run(['kubectl', ...nsArgs, 'apply', '-f', '-'], { stdin: JSON.stringify(m) }); if (r.code !== 0) throw new Error(`apply ${m.kind}/${name}: ${r.err.trim().slice(0, 300)}`) }
  }
  async deleteObject(ns: string | null, kind: 'Pod' | 'ConfigMap' | 'NetworkPolicy' | 'Cluster' | 'PersistentVolume' | 'Job', name: string): Promise<void> {
    assertName(name, 'name')
    if (kind === 'PersistentVolume') { if (name !== RESTORED_CLUSTER) throw new Refusal('only the drill PersistentVolume') } else {
      assertNamespace(ns, this.namespaces)
      if (kind === 'Cluster') { if (name !== RESTORED_CLUSTER) throw new Refusal('only the restored Cluster') } else if (!name.startsWith('drill-')) throw new Refusal('only a drill object')
    }
    const argv = ['kubectl', ...(ns ? ['-n', ns] : []), 'delete', kind.toLowerCase(), name, '--ignore-not-found', '--wait=false']
    this.actions.push({ what: `delete ${kind} ${name}`, argv })
    this.log(`DELETE ${kind} ${ns ?? ''}/${name}`)
    if (!this.dry) await this.x.run(argv)
  }

  /** An environment variable of a platform workload's first container changed; the object's own value (or its absence) is put back by the undo log. */
  async patchEnv(ns: string, kind: 'statefulset' | 'deployment', name: string, env: { name: string; value?: string; secretName?: string; secretKey?: string }): Promise<void> {
    assertNamespace(ns, this.namespaces); assertName(name, kind)
    if (!/^[A-Z][A-Z0-9_]*$/.test(env.name)) throw new Refusal('an environment name is upper case words')
    const obj = this.dry ? { spec: { template: { spec: { containers: [{ env: [{ name: env.name, value: 'x' }] }] } } } } : await this.kjson<any>(['-n', ns, 'get', kind, name])
    const list = (obj.spec.template.spec.containers[0].env ?? []) as any[]
    const i = list.findIndex((e) => e.name === env.name)
    const before = i >= 0 ? list[i] : null
    const entry = env.secretName ? { name: env.name, valueFrom: { secretKeyRef: { name: env.secretName, key: env.secretKey ?? '' } } } : { name: env.name, value: env.value ?? '' }
    const hasEnv = Boolean(obj.spec.template.spec.containers[0].env)
    const patchCmd = (e: any | null, op: 'add' | 'replace') => {
      const next = [...list.filter((x) => x.name !== env.name), ...(e ? [e] : [])]
      return ['kubectl', '-n', ns, 'patch', kind, name, '--type=json', '-p', JSON.stringify([{ op, path: '/spec/template/spec/containers/0/env', value: next }])]
    }
    // the way back (the object's own value, or its absence) is written down before the change is made; after the change the array exists, so it is a replace
    this.pushUndo(`restore env ${env.name} of ${ns}/${kind}/${name}`, [patchCmd(before, 'replace')])
    this.log(`PATCH env ${env.name} of ${ns}/${kind}/${name}`)
    this.actions.push({ what: `patch env ${env.name} of ${ns}/${kind}/${name}` })
    if (!this.dry) { const r = await this.x.run(patchCmd(entry, hasEnv ? 'replace' : 'add')); if (r.code !== 0) throw new Error(`patch: ${r.err.trim().slice(0, 300)}`) }
  }
  /** The egress of one platform NetworkPolicy replaced (the store unreachable: the rule that lets the storage pods reach the internet taken out); the saved original is put back by the undo log. */
  async setPolicyEgress(ns: string, name: string, egress: unknown[]): Promise<void> {
    assertNamespace(ns, this.namespaces); assertName(name, 'policy')
    const cur = this.dry ? { spec: { egress: [] as unknown[] } } : await this.kjson<any>(['-n', ns, 'get', 'networkpolicy', name])
    const saved = cur.spec.egress ?? []
    // a policy that cannot reach the store (an earlier drill left it stripped) is never saved as "the original": run `recover` first
    if (!this.dry && !egressReachesStore(saved)) throw new Refusal(`networkpolicy ${ns}/${name} is not as declared (its egress has no rule to the store): run the drills' \`recover\` before any drill saves it as the original`)
    const cmd = (e: unknown[]) => ['kubectl', '-n', ns, 'patch', 'networkpolicy', name, '--type=json', '-p', JSON.stringify([{ op: 'replace', path: '/spec/egress', value: e }])]
    this.pushUndo(`restore egress of networkpolicy ${ns}/${name}`, [cmd(saved)])
    this.log(`SET egress of networkpolicy ${ns}/${name} (${egress.length} rules, was ${saved.length})`)
    this.actions.push({ what: `set egress of networkpolicy ${ns}/${name}` })
    if (!this.dry) { const r = await this.x.run(cmd(egress)); if (r.code !== 0) throw new Error(`patch policy: ${r.err.trim().slice(0, 300)}`) }
  }

  // ---- volumes

  /** The placement of a Job pinned to one machine: the platform's (its label and taint) on a platform machine, the safekeeper's own tolerations on an outside one. */
  private pinTo(shape: Shape, machine: string): { nodeSelector: Record<string, string>; tolerations: Toleration[] } {
    if (Object.hasOwn(shape.outside, machine)) return { nodeSelector: { 'kubernetes.io/hostname': machine }, tolerations: [...shape.outside[machine]!] }
    const p = placement(this.names)
    return { nodeSelector: { ...p.nodeSelector, 'kubernetes.io/hostname': machine }, tolerations: p.tolerations }
  }
  /** One root Job, in the platform's namespace, on one machine, with only one volume's root mounted, waited for and deleted. */
  private async volumeJob(job: string, machine: string, root: string, cmd: string, waitS: number): Promise<string> {
    const shape = await this.platform()
    const manifest = {
      apiVersion: 'batch/v1', kind: 'Job', metadata: { name: job, namespace: this.ns },
      spec: {
        backoffLimit: 0, ttlSecondsAfterFinished: 600,
        template: {
          metadata: { labels: { app: job } },
          spec: {
            restartPolicy: 'Never', automountServiceAccountToken: false, enableServiceLinks: false,
            ...this.pinTo(shape, machine),
            containers: [{ name: 'run', image: shape.storageImage, securityContext: { runAsUser: 0 }, command: ['/bin/sh', '-c', cmd], volumeMounts: [{ name: 'vol', mountPath: '/vol' }] }],
            volumes: [{ name: 'vol', hostPath: { path: root, type: 'Directory' } }],
          },
        },
      },
    }
    await this.x.run(['kubectl', '-n', this.ns, 'delete', 'job', job, '--ignore-not-found'])
    const r = await this.x.run(['kubectl', '-n', this.ns, 'apply', '-f', '-'], { stdin: JSON.stringify(manifest) })
    if (r.code !== 0) throw new Error(`Job ${job}: ${r.err.trim().slice(0, 300)}`)
    const w = await this.x.run(['kubectl', '-n', this.ns, 'wait', '--for=condition=complete', `job/${job}`, `--timeout=${waitS}s`], { timeoutMs: (waitS + 20) * 1000 })
    if (w.code !== 0) throw new Error(`the Job ${job} did not complete: ${w.err.trim().slice(0, 300)}`)
    const out = (await this.x.run(['kubectl', '-n', this.ns, 'logs', `job/${job}`])).out.trim()
    await this.x.run(['kubectl', '-n', this.ns, 'delete', 'job', job, '--ignore-not-found', '--wait=false'])
    return out
  }
  /**
   * A path of a platform machine's volume wiped by a Job the guard writes: root, in the platform's namespace, on that machine, with only that volume's root mounted. An empty `sub` is the volume's whole contents (the
   * directory itself stays). The Job is waited for and deleted. The pod that mounts the volume must be down (a drill scales it to zero first; the undo log brings it back).
   */
  async wipeVolume(machine: string, path: string, name = 'drill-wipe'): Promise<void> {
    const shape = await this.platform()
    const { root, sub } = volumeTarget(machine, path, shape, this.settings.volumes)
    const job = `${name}-${machine}-${root.split('/').pop()}`
    assertName(job, 'job')
    const cmd = sub === '' ? 'find /vol -mindepth 1 -maxdepth 1 -exec rm -rf {} + ; echo wiped everything under /vol' : `rm -rf /vol/${sub} ; echo wiped /vol/${sub}`
    this.actions.push({ what: `WIPE ${machine}:${path}` })
    this.log(`WIPE ${machine}:${path} (Job ${job})`)
    if (this.dry) return
    this.log(`wiped: ${await this.volumeJob(job, machine, root, cmd, 180)}`)
  }

  /** A directory made in the controller database's volume, owned by Postgres (uid 26): where the restore drill's copy of the database lives. Only `<that volume>/<plain name>` on the machine that has it. */
  async makeVolumeDir(machine: string, path: string): Promise<void> {
    const shape = await this.platform()
    const { root, sub } = volumeTarget(machine, path, shape, this.settings.volumes)
    if (root !== this.settings.volumes.controllerDatabase || sub === '' || sub.includes('*') || sub.includes('/')) throw new Refusal('a drill makes one plain directory directly under the controller database\'s volume')
    const job = `drill-mkdir-${sub}`
    assertName(job, 'job')
    this.actions.push({ what: `MKDIR ${machine}:${path}` })
    this.log(`MKDIR ${machine}:${path} (Job ${job})`)
    if (this.dry) return
    await this.volumeJob(job, machine, root, `mkdir -p /vol/${sub} && chown 26:26 /vol/${sub} && chmod 700 /vol/${sub}`, 120)
  }
  /** One SELECT in the controller's database or its restored copy: a read, by `psql` as postgres in that pod. Only when the install named its controller database (`PEAT_DRILL_CONTROLLER_DB`). */
  async storconSql(which: 'live' | 'restored', sql: string): Promise<string> {
    const cdb = this.settings.controllerDatabase
    if (!cdb) throw new Refusal('no controller database is named (PEAT_DRILL_CONTROLLER_DB)')
    if (!/^select [^;]*$/i.test(sql)) throw new Refusal('one SELECT, no semicolon')
    const pod = `${which === 'live' ? cdb.cluster : RESTORED_CLUSTER}-1`
    const r = await this.x.run(['kubectl', '-n', this.ns, 'exec', pod, '-c', 'postgres', '--', 'psql', '-U', 'postgres', '-d', cdb.database, '-Atc', sql], { timeoutMs: 60_000 })
    if (r.code !== 0) throw new Error(`controller database sql: ${r.err.trim().slice(0, 200)}`)
    return r.out.trim()
  }
  /** `kubectl rollout status` of one platform workload: a read that waits. */
  async rolloutStatus(ns: string, kind: 'deployment' | 'statefulset', name: string, seconds = 240): Promise<boolean> {
    assertNamespace(ns, this.namespaces); assertName(name, kind)
    if (this.dry) return true
    const r = await this.x.run(['kubectl', '-n', ns, 'rollout', 'status', `${kind}/${name}`, `--timeout=${seconds}s`], { timeoutMs: (seconds + 20) * 1000 })
    return r.code === 0
  }

  // ---- alerts

  /**
   * An Alertmanager silence of the fixed shape (`silenceMatchers`), 30 minutes, expiring by itself; the ids are kept and expired by the undo log at the drill's end. `machine` adds that machine's node alerts;
   * `exclude` leaves out the alerts a drill is testing. No Alertmanager named: nothing is silenced (an empty list).
   */
  async silence(opts: { machine?: string; exclude?: readonly string[]; skipPeat?: boolean; comment: string }): Promise<string[]> {
    const sets = silenceMatchers({ ...opts, known: peatAlertNames(this.settings.rulesFile), machines: (await this.platform()).machines, namespaces: this.namespaces })
    if (!this.settings.alertmanager) { this.log('no Alertmanager is named: nothing is silenced'); return [] }
    const ids: string[] = []
    for (const matchers of sets) ids.push(await this.amtoolSilence(matchers, opts.comment))
    return ids
  }
  /** One silence by `amtool` in the Alertmanager pod (the API server's proxy refuses a POST of this shape): the matchers are the fixed ones, the comment plain text, 30 minutes. The id is kept and expired by the undo log. */
  private async amtoolSilence(matchers: Matcher[], comment: string): Promise<string> {
    const am = this.settings.alertmanager
    if (!am) throw new Refusal('no Alertmanager is named (PEAT_DRILL_ALERTMANAGER)')
    const m = matchers.map((x) => `${x.name}${x.isRegex ? (x.isEqual ? '=~' : '!~') : x.isEqual ? '=' : '!='}${JSON.stringify(x.value)}`)
    const text = `peat drill: ${comment}`.replace(/[^A-Za-z0-9 .,:_-]/g, ' ').slice(0, 120)
    const head = ['kubectl', '-n', am.namespace, 'exec', `deploy/${am.name}`, '--', 'amtool', `--alertmanager.url=${AM_URL}`, 'silence']
    const argv = [...head, 'add', ...m, `--duration=${SILENCE_MINUTES}m`, '--author=peat-drills', `--comment=${text}`]
    this.actions.push({ what: `silence ${m.join(' ')}`, argv })
    this.log(`SILENCE ${m.join(' ')} for ${SILENCE_MINUTES} min`)
    if (this.dry) return 'dry'
    const r = await this.x.run(argv)
    const id = r.out.trim().split('\n').pop()!.trim()
    if (r.code !== 0 || !/^[0-9a-f-]{8,40}$/.test(id)) throw new Error(`silence: ${(r.err || r.out).trim().slice(0, 300)}`)
    this.pushUndo(`expire silence ${id}`, [[...head, 'expire', id]], true)
    return id
  }
  /** Refuses a drill whose window, padded, meets the install's quiet hours (see `quietConflict`). */
  assertClearOfQuietHours(minutes: number, now = new Date(this.clock.now())): void {
    const c = quietConflict(now, minutes, this.settings.quietHours)
    if (c) throw new Refusal(`not now: ${c}`)
  }
}
