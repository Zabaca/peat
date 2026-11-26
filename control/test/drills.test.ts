import { describe, expect, test } from 'bun:test'
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DEFAULT_NAMES } from '../src/names'
import { DRILLS } from '../src/drills/drills'
import { FAKE_MACHINES, FakeClock, FakeCtx, PlanExec, fakeSettings } from '../src/drills/fake'
import { CLEAR_MINUTES, DEFAULT_VOLUMES, Guard, PEAT_ALERTS, RESTORED_CLUSTER, Refusal, SILENCE_MINUTES, assertMachine, assertNamespace, assertNoPending, assertReplayable, drillSettings, egressReachesStore, machineCommand, namespacesOf, parseMachineCommand, peatAlertNames, pendingPath, quietConflict, readPending, silenceMatchers, volumeTarget, type DrillSettings, type Exec, type Replay, type Result, type Shape } from '../src/drills/guard'
import { DB, READER_EXPECT, READER_SQL, FILLER_BYTES, FILLER_ROWS, analyzeReader, analyzeWriter, diffSnapshots, type Snapshot } from '../src/drills/lib'
import { plan } from '../src/drills/run'

/**
 * The guard of Peat's drills: every cutting step of every drill goes through `guard.ts`, which accepts only the platform's machines (the nodes with its label, read from the cluster), namespaces, volumes and
 * objects, cuts a machine only by the installer's own command, and undoes what it changed. What these pin: the refusals (a node without the label, names that only begin like a platform machine, other
 * namespaces) with NO command reaching a machine or the cluster, the accepted shapes, the undo log (a cut machine started again by the trap), the silence's fixed matchers, the quiet hours, that every drill's
 * steps pass the guard in a dry run, that no other file builds a command, and the analysis of a writer's and a reader's logs. Nothing here touches a cluster or a machine.
 */
const drillsDir = join(import.meta.dir, '../src/drills')
const read = (f: string) => readFileSync(join(drillsDir, f), 'utf8')
const LABEL = DEFAULT_NAMES.machineLabel
const MACHINES: string[] = [...FAKE_MACHINES]
const NAMESPACES = namespacesOf(DEFAULT_NAMES)
const COMMAND = ['machinectl', '{action}', '{machine}']

class Recorder implements Exec {
  argvs: string[][] = []
  stdins: (string | undefined)[] = []
  syncs: string[][] = []
  constructor(private readonly reply: (argv: string[]) => Result = () => ({ code: 0, out: '{}', err: '' })) {}
  async run(argv: string[], o?: { stdin?: string }) { this.argvs.push(argv); this.stdins.push(o?.stdin); return this.reply(argv) }
  runSync(argv: string[]) { this.syncs.push(argv); return this.reply(argv) }
}
const nodeJson = (name: string, labeled = true, ready = true) => ({ metadata: { name, labels: labeled ? { [LABEL.key]: LABEL.value } : {} }, status: { conditions: [{ type: 'Ready', status: ready ? 'True' : 'False' }] } })
const podJson = (name: string, app: string, machine: string, tolerations?: unknown[]) => ({ metadata: { name, labels: { app } }, spec: { nodeName: machine, ...(tolerations ? { tolerations } : {}), containers: [{ image: 'registry.example/peat/storage:test' }] } })
/** A cluster that answers the guard's reads: the labelled machines, the platform's pods (a pageserver on machine-1 and machine-2, the controller and a safekeeper on machine-3, and one safekeeper on `outside-1`, a machine without the label), every node of `nodes`. */
const world = (o: { machines?: string[]; labeled?: boolean; ready?: boolean; also?: (argv: string[]) => Result | undefined } = {}) => {
  const machines = o.machines ?? MACHINES
  return new Recorder((argv) => {
    const extra = o.also?.(argv)
    if (extra) return extra
    const j = argv.join(' ')
    if (/get nodes -l /.test(j)) return { code: 0, out: JSON.stringify({ items: (o.labeled ?? true) ? machines.map((m) => nodeJson(m)) : [] }), err: '' }
    const g = /get node (\S+) -o json/.exec(j)
    if (g) return { code: 0, out: JSON.stringify(nodeJson(g[1]!, o.labeled ?? true, o.ready ?? true)), err: '' }
    if (/-n peat get pods -o json/.test(j)) return { code: 0, out: JSON.stringify({ items: [podJson('pageserver-w1-0', 'pageserver', 'machine-1'), podJson('pageserver-w2-0', 'pageserver', 'machine-2'), podJson('storage-controller-a', 'storage-controller', 'machine-3'), podJson('safekeeper-w3-0', 'safekeeper', 'machine-3'), podJson('safekeeper-w10-0', 'safekeeper', 'outside-1', [{ key: 'example.com/outside', operator: 'Equal', value: 'true', effect: 'NoSchedule' }])] }), err: '' }
    return { code: 0, out: '{}', err: '' }
  })
}
const settings = (over: Partial<DrillSettings> = {}) => fakeSettings(over)
const guard = (x: Exec, dry = false, stateDir: string | null = null, over: Partial<DrillSettings> = {}) => new Guard(x, { dry, clock: new FakeClock(), log: () => {}, stateDir, settings: settings(over) })
const replay: Replay = { machines: MACHINES, machineCommand: COMMAND, namespaces: NAMESPACES, alertmanager: { namespace: 'monitoring', name: 'alertmanager' } }
const SHAPE: Pick<Shape, 'machines' | 'volumeMachines' | 'outside'> = {
  machines: MACHINES,
  volumeMachines: { [DEFAULT_VOLUMES.pageserver]: ['machine-1', 'machine-2'], [DEFAULT_VOLUMES.safekeeper]: MACHINES, [DEFAULT_VOLUMES.controllerDatabase]: ['machine-3'] },
  outside: { 'outside-1': [] },
}

describe('the settings', () => {
  test('every setting unset is a neutral default: no machine command, no quiet hours, no monitoring, the platform\'s own namespaces and alerts', () => {
    const s = drillSettings({ HOME: '/home/example' })
    expect(s.machineCommand).toBeNull()
    expect(s.quietHours).toEqual([])
    expect(s.alertmanager).toBeNull()
    expect(s.prometheus).toBeNull()
    expect(s.controllerDatabase).toBeNull()
    expect(namespacesOf(s.names)).toEqual(['peat', 'peat-compute', 'peat-gateway', 'peat-db'])
    expect(s.stateDir).toBe('/home/example/.local/state/peat-drills')
    expect(drillSettings({ HOME: '/h', XDG_STATE_HOME: '/state' }).stateDir).toBe('/state/peat-drills')
    expect(s.kubeconfig).toBeNull()
    expect(drillSettings({ KUBECONFIG: '/k' }).kubeconfig).toBe('/k')
    expect(peatAlertNames()).toEqual([...PEAT_ALERTS])
  })
  test('the names come from the platform\'s own settings (src/names.ts), so another install\'s namespaces and label are the drills\' too', () => {
    const s = drillSettings({ NAMESPACE: 'pt', COMPUTE_NAMESPACE: 'pt-c', GATEWAY_NAMESPACE: 'pt-g', DB_NAMESPACE: 'pt-d', MACHINE_LABEL: 'example.com/db-machine=yes' })
    expect(namespacesOf(s.names)).toEqual(['pt', 'pt-c', 'pt-g', 'pt-d'])
    expect(s.names.machineLabel).toEqual({ key: 'example.com/db-machine', value: 'yes' })
  })
  test('the machine command: a JSON list or words, with {action} and {machine} both, each replaced in place with no shell', () => {
    expect(parseMachineCommand(undefined)).toBeNull()
    expect(parseMachineCommand('  ')).toBeNull()
    expect(parseMachineCommand('power {action} {machine}')).toEqual(['power', '{action}', '{machine}'])
    expect(parseMachineCommand('["ssh", "host.example", "vm {action} --force {machine}"]')).toEqual(['ssh', 'host.example', 'vm {action} --force {machine}'])
    expect(() => parseMachineCommand('power stop {machine}')).toThrow(/\{action\}/)
    expect(() => parseMachineCommand('power {action}')).toThrow(/\{machine\}/)
    expect(() => parseMachineCommand('["power", 1]')).toThrow()
    expect(machineCommand(['ssh', 'host.example', 'vm {action} --force {machine}'], 'stop', 'machine-2')).toEqual(['ssh', 'host.example', 'vm stop --force machine-2'])
    expect(machineCommand(['power', '{action}', 'pool/{machine}'], 'start', 'machine-1')).toEqual(['power', 'start', 'pool/machine-1'])
  })
})

describe('a machine cut', () => {
  const refused = ['machine-10', 'machine-1-db', 'machine-11', 'machine-0', 'machine-4', 'machine-', 'Machine-1', 'MACHINE-1', ' machine-1', 'machine-1 ', 'machine-1\n', 'machine-1;', 'machine-1 machine-2', 'machine-*', '', 'x_machine-1', 'host:machine-1', 'outside-1', 'control-plane', '$(id)']
  for (const name of refused)
    test(`${JSON.stringify(name)} is refused before any command reaches a machine`, async () => {
      const x = world()
      await expect(guard(x).cutMachine(name)).rejects.toBeInstanceOf(Refusal)
      expect(x.argvs.filter((a) => a[0] !== 'kubectl')).toEqual([])
      expect(x.syncs).toEqual([])
    })
  test('a value that is not a string is refused', async () => {
    const x = world()
    for (const v of [undefined, null, 1, ['machine-1'], { toString: () => 'machine-1' }]) await expect(guard(x).cutMachine(v as any)).rejects.toBeInstanceOf(Refusal)
    expect(x.argvs.filter((a) => a[0] !== 'kubectl')).toEqual([])
  })
  test('the platform\'s machines are the nodes the cluster lists with the machine label, matched whole', async () => {
    const g = guard(world())
    expect((await g.platform()).machines).toEqual(MACHINES)
    for (const m of MACHINES) expect(() => assertMachine(m, MACHINES)).not.toThrow()
    expect(() => assertMachine('machine-1', [])).toThrow(Refusal)
  })
  test('with no machine command set, a cut is refused with the reason and nothing runs', async () => {
    const x = world()
    await expect(guard(x, false, null, { machineCommand: null }).cutMachine('machine-1')).rejects.toThrow(/no machine command is set \(PEAT_DRILL_MACHINE_COMMAND/)
    expect(x.argvs).toEqual([])
  })
  test('the cluster\'s node must carry the machine label and be Ready, read back first: otherwise nothing is stopped', async () => {
    const unlabelled = world({ also: (a) => (/get node machine-1 -o json/.test(a.join(' ')) ? { code: 0, out: JSON.stringify(nodeJson('machine-1', false)), err: '' } : undefined) })
    await expect(guard(unlabelled).cutMachine('machine-1')).rejects.toThrow(new RegExp(`does not carry ${LABEL.key.replace(/\./g, '\\.')}=true`))
    const down = world({ ready: false })
    await expect(guard(down).cutMachine('machine-1')).rejects.toThrow(/not Ready: nothing to cut/)
    for (const r of [unlabelled, down]) expect(r.argvs.some((a) => a[0] === 'machinectl')).toBe(false)
  })
  test('an accepted cut is the installer\'s command with stop, and registers its start, which the trap runs', async () => {
    const x = world()
    const g = guard(x)
    await g.cutMachine('machine-2')
    expect(x.argvs.find((a) => a[0] === 'machinectl')).toEqual(['machinectl', 'stop', 'machine-2'])
    expect(g.pending).toEqual(['start machine-2 again'])
    g.undoSync() // what the exit, SIGINT and SIGTERM handlers call
    expect(x.syncs).toEqual([['machinectl', 'start', 'machine-2']])
    expect(g.pending).toEqual([])
  })
  test('an interrupted drill: the undo log starts the machine again, and so does an error', async () => {
    const x = world()
    const g = guard(x)
    await g.cutMachine('machine-3')
    await g.undoAll()
    expect(x.argvs.filter((a) => a[0] === 'machinectl' && a.includes('start'))).toEqual([['machinectl', 'start', 'machine-3']])
  })
})

describe('pods, workloads and objects', () => {
  const foreign = ['app-db', 'default', 'kube-system', 'monitoring', 'cert-manager', 'peat-compute-2', 'peat2', 'Peat', ' peat', 'peat ', '', 'peat,app-db']
  for (const ns of foreign)
    test(`namespace ${JSON.stringify(ns)} is refused for a pod kill, a scale, a patch, a policy, an object and a delete, with no command issued`, async () => {
      const x = world()
      const g = guard(x)
      await expect(g.killPod(ns, 'x')).rejects.toBeInstanceOf(Refusal)
      await expect(g.scale(ns, 'statefulset', 'x', 0)).rejects.toBeInstanceOf(Refusal)
      await expect(g.patchEnv(ns, 'deployment', 'x', { name: 'A', value: '1' })).rejects.toBeInstanceOf(Refusal)
      await expect(g.setPolicyEgress(ns, 'x', [])).rejects.toBeInstanceOf(Refusal)
      await expect(g.relabelPod(ns, 'x', { a: 'b' })).rejects.toBeInstanceOf(Refusal)
      await expect(g.deleteObject(ns, 'Pod', 'drill-x')).rejects.toBeInstanceOf(Refusal)
      await expect(g.apply(ns, { kind: 'Pod', metadata: { name: 'drill-x', namespace: ns }, spec: { nodeSelector: { [LABEL.key]: LABEL.value } } })).rejects.toBeInstanceOf(Refusal)
      expect(x.argvs).toEqual([])
    })
  test('the four platform namespaces are accepted', () => {
    expect(NAMESPACES).toEqual(['peat', 'peat-compute', 'peat-gateway', 'peat-db'])
    for (const ns of NAMESPACES) expect(() => assertNamespace(ns, NAMESPACES)).not.toThrow()
  })
  test('a pod kill is a forced delete with no grace period of one named pod', async () => {
    const x = world(); const g = guard(x)
    await g.killPod('peat', 'pageserver-w1-0')
    expect(x.argvs).toEqual([['kubectl', '-n', 'peat', 'delete', 'pod', 'pageserver-w1-0', '--grace-period=0', '--force', '--wait=false']])
    for (const bad of ['', 'a b', 'a;b', '../x', 'A', '-a', '$(x)', 'a/b', '--all', '-l x']) await expect(g.killPod('peat', bad)).rejects.toBeInstanceOf(Refusal)
  })
  test('only a StatefulSet or a Deployment may be scaled, to 0 to 3, and the old count is put back by the undo log', async () => {
    const x = new Recorder((a) => ({ code: 0, out: a.includes('jsonpath={.spec.replicas}') ? '1' : '{}', err: '' }))
    const g = guard(x)
    await expect(g.scale('peat', 'job' as any, 'x', 0)).rejects.toBeInstanceOf(Refusal)
    await expect(g.scale('peat', 'statefulset', 'x', 9)).rejects.toBeInstanceOf(Refusal)
    await expect(g.scale('peat', 'statefulset', 'x', -1)).rejects.toBeInstanceOf(Refusal)
    await g.scale('peat', 'statefulset', 'pageserver-w1', 0)
    expect(g.pending).toEqual(['scale peat/statefulset/pageserver-w1 back to 1'])
    await g.undoAll()
    expect(x.argvs.filter((a) => a.includes('scale')).map((a) => a.slice(-1)[0])).toEqual(['--replicas=0', '--replicas=1'])
  })
  test('what a drill makes: a Pod, ConfigMap or NetworkPolicy named drill-* in the platform, the restored Cluster and its PersistentVolume on the controller database\'s volume; nothing else, and the pod selects the platform machines with no hostPath', async () => {
    const x = world(); const g = guard(x, true)
    const pod = (name: string, over: any = {}) => ({ kind: 'Pod', metadata: { name, namespace: 'peat-compute' }, spec: { nodeSelector: { [LABEL.key]: LABEL.value }, containers: [{ name: 'a' }], ...over } })
    await g.apply('peat-compute', pod('drill-writer'))
    await g.apply('peat-compute', { kind: 'NetworkPolicy', metadata: { name: 'drill-client', namespace: 'peat-compute' } })
    await g.apply('peat', { kind: 'Cluster', metadata: { name: RESTORED_CLUSTER, namespace: 'peat' } })
    await g.apply(null, { kind: 'PersistentVolume', metadata: { name: RESTORED_CLUSTER }, spec: { hostPath: { path: `${DEFAULT_VOLUMES.controllerDatabase}/restored`, type: 'Directory' } } })
    for (const bad of [
      [ 'peat-compute', pod('writer') ],
      [ 'peat-compute', pod('drill-x', { nodeSelector: {} }) ],
      [ 'peat-compute', pod('drill-x', { volumes: [{ name: 'h', hostPath: { path: '/' } }] }) ],
      [ 'peat-compute', pod('drill-x', { hostNetwork: true }) ],
      [ 'peat-compute', pod('drill-x', { containers: [{ name: 'a', securityContext: { privileged: true } }] }) ],
      [ 'peat', { kind: 'Secret', metadata: { name: 'drill-s', namespace: 'peat' } } ],
      [ 'peat', { kind: 'Deployment', metadata: { name: 'drill-d', namespace: 'peat' } } ],
      [ 'peat', { kind: 'Cluster', metadata: { name: 'storcon-db', namespace: 'peat' } } ],
      [ 'peat-compute', { kind: 'Cluster', metadata: { name: RESTORED_CLUSTER, namespace: 'peat-compute' } } ],
      [ 'peat', { kind: 'Pod', metadata: { name: 'drill-x', namespace: 'peat-compute' }, spec: { nodeSelector: { [LABEL.key]: LABEL.value } } } ],
      [ null, { kind: 'PersistentVolume', metadata: { name: 'x' }, spec: { hostPath: { path: DEFAULT_VOLUMES.controllerDatabase, type: 'Directory' } } } ],
      [ null, { kind: 'PersistentVolume', metadata: { name: RESTORED_CLUSTER }, spec: { hostPath: { path: DEFAULT_VOLUMES.pageserver, type: 'Directory' } } } ],
      [ null, { kind: 'PersistentVolume', metadata: { name: RESTORED_CLUSTER }, spec: { hostPath: { path: `${DEFAULT_VOLUMES.controllerDatabase}/../x`, type: 'Directory' } } } ],
      [ null, { kind: 'PersistentVolume', metadata: { name: RESTORED_CLUSTER }, spec: { hostPath: { path: DEFAULT_VOLUMES.controllerDatabase, type: 'DirectoryOrCreate' } } } ],
    ] as [string | null, any][]) await expect(g.apply(bad[0], bad[1])).rejects.toBeInstanceOf(Refusal)
  })
  test('the control layer: only the database routes, a delete, start or stop only of a drill* database, never a restore, a trim, an upgrade, a registration or a replica', async () => {
    const g = guard(world(), true)
    for (const [m, p] of [['GET', '/dbs'], ['GET', '/db/drill-par200'], ['POST', '/db'], ['POST', '/db/drill-par200/stop'], ['POST', '/branch'], ['DELETE', '/db/drill-sleep'], ['DELETE', '/db/drill-par200'], ['GET', '/sk-replace']] as const)
      await expect(g.control(m, p, p === '/db' || p === '/branch' ? { name: 'drill-x' } : undefined)).resolves.toBeDefined()
    for (const [m, p] of [['DELETE', '/db/par200'], ['DELETE', '/db/tenant-one'], ['POST', '/db/par200/stop'], ['POST', '/db/tenant-one/start'], ['DELETE', '/db/app-db'], ['DELETE', '/db/x'], ['DELETE', '/db/drill/../x'], ['POST', '/restore'], ['POST', '/wal-trim'], ['POST', '/sk-register?id=1'], ['POST', '/sk-upgrade?id=1'], ['POST', '/replica'], ['POST', '/sk-replace/parallel'], ['PUT', '/db'], ['GET', '/db/../healthz']] as const)
      await expect(g.control(m as any, p)).rejects.toBeInstanceOf(Refusal)
  })
  test('a recovery of a drill database and a decommission of a member are allowed, the recovery of any other database is not, and both run on the acting pod', async () => {
    const x = world(); const g = guard(x)
    for (const [m, p] of [['POST', '/db/drill-par200/recover'], ['POST', '/db/drill-par200/recover?dry=1'], ['POST', '/sk/7/decommission'], ['POST', '/sk/10/decommission?force=1']] as const) await expect(g.control(m, p)).resolves.toBeDefined()
    for (const [m, p] of [['POST', '/db/seed/recover'], ['POST', '/db/par200/recover?dry=1'], ['POST', '/db/drill-x/recover?force=1'], ['POST', '/db/drill-x/recover/x'], ['POST', '/sk/x/decommission'], ['POST', '/sk/7/decommission?dry=1'], ['POST', '/sk/7/move?to=machine-3'], ['POST', '/sk/7/decommission/x'], ['GET', '/sk/7/decommission']] as const)
      await expect(g.control(m as any, p)).rejects.toBeInstanceOf(Refusal)
  })
  test('a storage read is a GET of a Service in the platform\'s namespace, never another host and never a change', async () => {
    const g = guard(new PlanExec())
    await expect(g.storage('admin', 'http://storage-controller.peat.svc:1234/control/v1/node')).resolves.toBeDefined()
    for (const u of ['http://app-db-rw.app.svc:5432/x', 'http://evil.example.com/', 'http://storage-controller.peat.svc:1234/../../x', 'https://storage-controller.peat.svc:1234/', 'http://pageserver-w1-0.pageserver.peat-compute.svc:9898/x', 'http://storage-controller.peatXsvc:1234/']) await expect(g.storage('admin', u)).rejects.toBeInstanceOf(Refusal)
  })
  test('a tenant detached only on one of the platform\'s pageservers (by the id its pod carries) and only by a 32 hex id; a kubectl read is only get, logs, top, describe', async () => {
    const g = guard(new PlanExec(), true)
    await expect(g.detachTenant(1, 'a'.repeat(32))).resolves.toBeUndefined()
    for (const [n, t] of [[3, 'a'.repeat(32)], [0, 'a'.repeat(32)], [1, 'a'.repeat(31)], [1, '../x'], [1, 'A'.repeat(32)]] as const) await expect(g.detachTenant(n, t)).rejects.toBeInstanceOf(Refusal)
    for (const a of [['delete', 'pod', 'x'], ['-n', 'peat', 'delete', 'pod', 'x'], ['apply', '-f', '-'], ['exec', 'x'], ['scale', 'x'], ['patch', 'x'], ['get', '--raw', '/healthz']]) await expect(g.kread(a)).rejects.toBeInstanceOf(Refusal)
  })
})

describe('volumes', () => {
  test('the guard reads which machine holds which volume from the cluster: the pageservers\' where a pageserver runs, the safekeepers\' on every platform machine, an outside machine by its safekeeper', async () => {
    const shape = await guard(world()).platform()
    expect(shape.volumeMachines[DEFAULT_VOLUMES.pageserver]).toEqual(['machine-1', 'machine-2'])
    expect(shape.volumeMachines[DEFAULT_VOLUMES.safekeeper]).toEqual(MACHINES)
    expect(shape.volumeMachines[DEFAULT_VOLUMES.controllerDatabase]).toEqual(['machine-3'])
    expect(shape.outside).toEqual({ 'outside-1': [{ key: 'example.com/outside', operator: 'Equal', value: 'true', effect: 'NoSchedule' }] })
    expect(shape.pageservers).toEqual([{ id: 1, machine: 'machine-1' }, { id: 2, machine: 'machine-2' }])
    expect(shape.storageImage).toBe('registry.example/peat/storage:test')
  })
  test('accepted: a path under a platform volume on a machine that has it', () => {
    const t = (m: string, p: string) => volumeTarget(m, p, SHAPE, DEFAULT_VOLUMES)
    expect(t('machine-1', '/var/mnt/pageserver')).toEqual({ root: '/var/mnt/pageserver', sub: '' })
    expect(t('machine-2', '/var/mnt/pageserver/neon/tenants/abc*')).toEqual({ root: '/var/mnt/pageserver', sub: 'neon/tenants/abc*' })
    expect(t('machine-3', '/var/mnt/safekeeper/sk-3')).toEqual({ root: '/var/mnt/safekeeper', sub: 'sk-3' })
    expect(t('machine-3', '/var/mnt/storcon-db/restored')).toEqual({ root: '/var/mnt/storcon-db', sub: 'restored' })
    // an outside machine has only the safekeepers' volume, and may have its member's directory wiped (the recovery drill)
    expect(t('outside-1', '/var/mnt/safekeeper/sk-10')).toEqual({ root: '/var/mnt/safekeeper', sub: 'sk-10' })
  })
  test('the wipe Job on an outside machine is pinned to it by name and carries its safekeeper\'s own tolerations, and mounts only the safekeepers\' volume', async () => {
    const x = world(); const g = guard(x)
    await g.wipeVolume('outside-1', '/var/mnt/safekeeper/sk-10')
    const job = JSON.parse(x.stdins[x.argvs.findIndex((a) => a.includes('apply'))]!)
    expect(job.spec.template.spec.nodeSelector).toEqual({ 'kubernetes.io/hostname': 'outside-1' })
    expect(job.spec.template.spec.tolerations).toEqual([{ key: 'example.com/outside', operator: 'Equal', value: 'true', effect: 'NoSchedule' }])
    expect(job.spec.template.spec.volumes).toEqual([{ name: 'vol', hostPath: { path: '/var/mnt/safekeeper', type: 'Directory' } }])
    expect(job.spec.template.spec.containers[0].command[2]).toBe('rm -rf /vol/sk-10 ; echo wiped /vol/sk-10')
    expect(job.spec.template.spec.containers[0].securityContext).toEqual({ runAsUser: 0 })
  })
  const refused: [string, string][] = [
    ['machine-3', '/var/mnt/pageserver'], ['machine-1', '/var/mnt/storcon-db'], ['machine-2', '/var/mnt/storcon-db/x'],
    ['machine-1', '/'], ['machine-1', '/var'], ['machine-1', '/var/mnt'], ['machine-1', '/var/lib/containerd'], ['machine-1', '/etc/kubernetes'], ['machine-1', '/var/mnt/u-pageserver'], ['machine-1', '/var/mnt/pageserverx'], ['machine-1', '/var/mnt/pageserver/'],
    ['machine-1', '/var/mnt/pageserver/..'], ['machine-1', '/var/mnt/pageserver/../safekeeper'], ['machine-1', '/var/mnt/pageserver/a/../../b'], ['machine-1', '/var/mnt/pageserver//x'], ['machine-1', '/var/mnt/pageserver/a b'], ['machine-1', '/var/mnt/pageserver/$(id)'], ['machine-1', '/var/mnt/pageserver/;rm'],
    ['machine-1', '/var/mnt/pageserver/`x`'], ['machine-1', '/var/mnt/pageserver/a\nb'], ['machine-1', '/var/mnt/pageserver/.'], ['machine-1', '/var/mnt/pageserver/./x'], ['machine-1', 'var/mnt/pageserver'], ['machine-1', '../var/mnt/pageserver'], ['machine-1', '/var/mnt/pageserver/-rf'],
    ['control-plane', '/var/mnt/pageserver'], ['db-machine', '/var/mnt/safekeeper'], ['outside-1', '/var/mnt/pageserver'], ['outside-1', '/var/mnt/storcon-db/x'], ['outside-1', '/var/mnt'], ['outside-1', '/var/lib/containerd'], ['outside-1', '/var/mnt/safekeeper/../x'], ['machine-10', '/var/mnt/safekeeper'], ['machine-1-db', '/var/mnt/pageserver'],
  ]
  for (const [m, p] of refused) test(`${m} ${JSON.stringify(p)} is refused`, () => expect(() => volumeTarget(m, p, SHAPE, DEFAULT_VOLUMES)).toThrow(Refusal))
  test('a refused wipe issues no command; an accepted one is a root Job pinned to that machine with only that volume\'s root mounted', async () => {
    const x = world(); const g = guard(x)
    await expect(g.wipeVolume('machine-3', '/var/mnt/pageserver')).rejects.toBeInstanceOf(Refusal)
    await expect(g.wipeVolume('db-machine', '/var/mnt/safekeeper/sk-1')).rejects.toBeInstanceOf(Refusal)
    expect(x.argvs.filter((a) => !a.includes('get'))).toEqual([])
    await g.wipeVolume('machine-1', '/var/mnt/pageserver/neon/tenants/abc*')
    const apply = x.argvs.findIndex((a) => a.includes('apply'))
    expect(apply).toBeGreaterThanOrEqual(0)
    const job = JSON.parse(x.stdins[apply]!)
    expect(job.metadata.namespace).toBe('peat')
    expect(job.spec.template.spec.nodeSelector).toEqual({ [LABEL.key]: LABEL.value, 'kubernetes.io/hostname': 'machine-1' })
    expect(job.spec.template.spec.volumes).toEqual([{ name: 'vol', hostPath: { path: '/var/mnt/pageserver', type: 'Directory' } }])
    expect(job.spec.template.spec.containers[0].command[2]).toBe('rm -rf /vol/neon/tenants/abc* ; echo wiped /vol/neon/tenants/abc*')
    expect(job.spec.template.spec.containers[0].image).toBe('registry.example/peat/storage:test')
    expect(JSON.stringify(job)).not.toContain('/var/mnt/safekeeper')
  })
  test('a platform whose machines are tainted: the wipe Job tolerates the machines\' taint', async () => {
    const x = world()
    const s = settings()
    const g = new Guard(x, { clock: new FakeClock(), log: () => {}, stateDir: null, settings: { ...s, names: { ...s.names, machineTaint: { key: 'example.com/dedicated', value: 'db', effect: 'NoSchedule' } } } })
    await g.wipeVolume('machine-2', '/var/mnt/pageserver')
    const job = JSON.parse(x.stdins[x.argvs.findIndex((a) => a.includes('apply'))]!)
    expect(job.spec.template.spec.tolerations).toEqual([{ key: 'example.com/dedicated', operator: 'Equal', value: 'db', effect: 'NoSchedule' }])
  })
})

describe('the silence', () => {
  const base = { machines: MACHINES, namespaces: NAMESPACES }
  test('its matchers are fixed: the platform\'s alerts less the ones a drill tests, and a machine\'s node alerts; an excluded name must be one of the platform\'s alerts', () => {
    const workload = [
      { name: 'alertname', value: 'WorkloadNotAvailable|ScrapeTargetDown|StatefulSetNotReady|PodRestartingInALoop|PodCrashLoopBackOff', isRegex: true, isEqual: true },
      { name: 'namespace', value: 'peat|peat-compute|peat-gateway|peat-db', isRegex: true, isEqual: true },
    ]
    expect(silenceMatchers(base).length).toBe(2)
    expect(silenceMatchers(base)[0]).toEqual([{ name: 'alertname', value: 'Peat.*', isRegex: true, isEqual: true }])
    expect(silenceMatchers(base)[1]).toEqual(workload)
    const m = silenceMatchers({ ...base, machine: 'machine-2', exclude: ['PeatUploadsLagging'] })
    expect(m).toEqual([
      [{ name: 'alertname', value: 'Peat.*', isRegex: true, isEqual: true }, { name: 'alertname', value: 'PeatUploadsLagging', isRegex: false, isEqual: false }],
      workload,
      [{ name: 'node', value: 'machine-2', isRegex: false, isEqual: true }],
    ])
    // no matcher on `instance`: the targets' instance is an address, so such a matcher matched nothing
    expect(JSON.stringify(m)).not.toContain('instance')
    expect(silenceMatchers({ ...base, machine: 'machine-1', skipPeat: true })).toEqual([[{ name: 'node', value: 'machine-1', isRegex: false, isEqual: true }]])
    for (const bad of ['NodeNotReady', 'Peat.*', '.*', 'PeatNope', '']) expect(() => silenceMatchers({ ...base, exclude: [bad] })).toThrow(Refusal)
    for (const bad of ['machine-10', 'control-plane', '.*', 'machine-1|control-plane', ' machine-1']) expect(() => silenceMatchers({ ...base, machine: bad })).toThrow(Refusal)
  })
  test('no input produces a matcher outside the fixed shapes (any machine string that is not a platform machine is refused, so no regex of an argument reaches Alertmanager)', () => {
    for (const s of ['.*', 'machine-.*', 'a|b', 'machine-1)(.*', '"', '\\', '(?i)machine-1']) expect(() => silenceMatchers({ ...base, machine: s })).toThrow(Refusal)
  })
  test('a silence is made by amtool in the named Alertmanager\'s pod: 30 minutes, the fixed matchers, a plain comment, and its expiry is recorded as an undo', async () => {
    expect(SILENCE_MINUTES).toBe(30)
    const x = world({ also: (a) => (a.includes('add') ? { code: 0, out: '0f3c1a2b-4d5e-6789-abcd-ef0123456789\n', err: '' } : undefined) })
    const g = guard(x)
    await g.silence({ machine: 'machine-1', exclude: ['PeatUploadsLagging'], comment: 'worker: "machine-1"; $(x) cut' })
    const adds = x.argvs.filter((a) => a.includes('add'))
    expect(adds.length).toBe(3)
    expect(adds[0]!.slice(0, 9)).toEqual(['kubectl', '-n', 'monitoring', 'exec', 'deploy/alertmanager', '--', 'amtool', '--alertmanager.url=http://localhost:9093', 'silence'])
    expect(adds[0]!.slice(9, 13)).toEqual(['add', 'alertname=~"Peat.*"', 'alertname!="PeatUploadsLagging"', '--duration=30m'])
    expect(adds[0]!.slice(13, 14)).toEqual(['--author=peat-drills'])
    expect(adds[0]![14]).toMatch(/^--comment=peat drill: worker/)
    expect(adds[1]!.slice(9, 11)).toEqual(['add', 'alertname=~"WorkloadNotAvailable|ScrapeTargetDown|StatefulSetNotReady|PodRestartingInALoop|PodCrashLoopBackOff"'])
    expect(adds[1]![11]).toBe('namespace=~"peat|peat-compute|peat-gateway|peat-db"')
    expect(adds[2]!.slice(9, 11)).toEqual(['add', 'node="machine-1"'])
    expect(adds.every((a) => !JSON.stringify(a).includes('$(') && !JSON.stringify(a).includes('"machine-1";'))).toBe(true)
    expect(g.pending.length).toBe(3)
    await g.undoAll()
    expect(x.argvs.filter((a) => a.includes('expire')).length).toBe(3)
  })
  test('with no Alertmanager named, nothing is silenced and no command runs to make one', async () => {
    const x = world()
    expect(await guard(x, false, null, { alertmanager: null }).silence({ machine: 'machine-1', comment: 'x' })).toEqual([])
    expect(x.argvs.some((a) => a.includes('amtool'))).toBe(false)
  })
  test('as Alertmanager reads them (anchored regexes, every matcher of a silence ANDed), the silences hide the platform\'s own workload alerts and no alert of any other namespace', () => {
    const am = (matchers: ReturnType<typeof silenceMatchers>[number], labels: Record<string, string>) => matchers.every((m) => { const v = labels[m.name] ?? ''; const hit = m.isRegex ? new RegExp(`^(?:${m.value})$`).test(v) : v === m.value; return m.isEqual ? hit : !hit })
    const hidden = (labels: Record<string, string>, machine?: string) => silenceMatchers({ ...base, machine }).some((set) => am(set, labels))
    for (const ns of NAMESPACES) for (const a of ['WorkloadNotAvailable', 'ScrapeTargetDown', 'StatefulSetNotReady', 'PodRestartingInALoop', 'PodCrashLoopBackOff']) expect(hidden({ alertname: a, namespace: ns }), `${a} in ${ns}`).toBe(true)
    expect(hidden({ alertname: 'PeatUploadsLagging', node: 'machine-1' })).toBe(true)
    expect(hidden({ alertname: 'NodeNotReady', node: 'machine-1' }, 'machine-1')).toBe(true)
    const kept: Record<string, string>[] = [
      { alertname: 'WorkloadNotAvailable', namespace: 'app-db' }, { alertname: 'WorkloadNotAvailable', namespace: 'other-gateway' }, { alertname: 'ScrapeTargetDown', namespace: 'media' }, { alertname: 'StatefulSetNotReady', namespace: 'peat-x' },
      { alertname: 'PodCrashLoopBackOff', namespace: 'jobs' }, { alertname: 'WorkloadNotAvailable', namespace: 'ci-runners' }, { alertname: 'WorkloadNotAvailable', namespace: 'monitoring' }, { alertname: 'WorkloadNotAvailable' },
      { alertname: 'HostLowMemory', host: 'host-1' }, { alertname: 'HostDiskAlmostFull' }, { alertname: 'NodeNotReady', node: 'db-machine' }, { alertname: 'NodeNotReady', node: 'machine-1' }, { alertname: 'ScrapeTargetDown', instance: '192.0.2.133:10250' },
      { alertname: 'PostgresArchiveBehind', namespace: 'app-db' }, { alertname: 'CronJobSuspended', namespace: 'peat' }, { alertname: 'PodRestartingInALoop', namespace: 'peat-compute-2' },
    ]
    for (const l of kept) expect(hidden(l), JSON.stringify(l)).toBe(false) // (NodeNotReady of machine-1 is silenced only by a drill that cuts it: `machine`)
  })
  test('a drill database\'s tenant gets a short checkpoint (10 to 120 s) and only a drill database\'s', async () => {
    const x = new PlanExec(); const g = guard(x, true)
    await expect(g.setTenantCheckpoint('drill-par200', 30)).resolves.toBeUndefined()
    for (const [db, n] of [['tenant-one', 30], ['par200', 30], ['drill-x', 5], ['drill-x', 300], ['drill-x', 30.5]] as const) await expect(g.setTenantCheckpoint(db, n)).rejects.toBeInstanceOf(Refusal)
    expect(read('drills.ts')).toContain('await c.setCheckpoint(D, 30)')
    expect(read('drills.ts')).toContain('await c.setCheckpoint(DB, 30)')
  })
  test('the alerts the drills test are the platform\'s own, the six stuck states are covered once each, and only the critical one is let through', () => {
    // the two alerts of the safekeepers' replacement rest on their rule tests, not on a drill
    const all = peatAlertNames()
    expect(all.filter((a) => /^PeatSafekeeperReplacement(Blocked|Open)$/.test(a)).sort()).toEqual(['PeatSafekeeperReplacementBlocked', 'PeatSafekeeperReplacementOpen'])
    const known = all.filter((a) => !/^PeatSafekeeperReplacement/.test(a))
    expect(known.sort()).toEqual(['PeatAttachNotActive', 'PeatAttachedTenantMissing', 'PeatDeletionFailures', 'PeatNodeWarmingUpTooLong', 'PeatSafekeeperTrimBehind', 'PeatUploadsLagging'])
    const tested = DRILLS.flatMap((d) => d.alerts?.tested ?? [])
    expect(tested.sort()).toEqual(known)
    const notify = DRILLS.filter((d) => d.alerts?.notify)
    expect(notify.map((d) => d.name)).toEqual(['partial-loss'])
    expect(notify[0]!.alerts!.tested).toEqual(['PeatAttachedTenantMissing'])
  })
  test('a rule file given is where the platform\'s alert names are read from', () => {
    const d = mkdtempSync(join(tmpdir(), 'peat-rules-'))
    const f = join(d, 'rules.yml')
    writeFileSync(f, 'groups:\n- name: peat\n  rules:\n  - alert: PeatOne\n    expr: up\n  - alert: OtherThing\n    expr: up\n  - alert: PeatTwo\n    expr: up\n')
    expect(peatAlertNames(f)).toEqual(['PeatOne', 'PeatTwo'])
    rmSync(d, { recursive: true, force: true })
  })
})

describe('the quiet hours', () => {
  // the install's own scheduled work: 05:00 to 05:45 and 17:00 to 17:45 New York time every day, Monday 08:00 to 09:00, and Friday night 23:30 to 00:30
  const windows = [
    { start: '05:00', end: '05:45', timeZone: 'America/New_York' },
    { start: '17:00', end: '17:45', timeZone: 'America/New_York' },
    { start: '08:00', end: '09:00', timeZone: 'America/New_York', days: ['Mon' as const] },
    { start: '23:30', end: '00:30', timeZone: 'America/New_York', days: ['Fri' as const] },
  ]
  const at = (h: number, m: number, day = 20) => new Date(Date.UTC(2026, 10, day, h + 5, m)) // November 2026: New York is UTC-5; the 20th is a Friday, the 23rd a Monday
  test('a window is refused when it comes within 30 minutes of quiet hours, before or after, or meets them', () => {
    expect(CLEAR_MINUTES).toBe(30)
    expect(quietConflict(at(13, 0), 12, windows)).toBeNull()
    expect(quietConflict(at(10, 0), 120, windows)).toBeNull()
    expect(quietConflict(at(16, 40), 12, windows)).toMatch(/17:00 to 17:45 America\/New_York/)
    expect(quietConflict(at(16, 15), 12, windows)).toBeNull() // ends 16:27, 33 minutes ahead of 17:00
    expect(quietConflict(at(17, 30), 12, windows)).toMatch(/were on|are now/) // inside them
    expect(quietConflict(at(18, 0), 12, windows)).toMatch(/17:00/) // 15 minutes after they ended
    expect(quietConflict(at(18, 20), 12, windows)).toBeNull()
  })
  test('a window of some days is found only on its days, and one past midnight belongs to the day it starts', () => {
    expect(quietConflict(at(8, 30, 23), 12, windows)).toMatch(/on Mon/)
    expect(quietConflict(at(8, 30, 20), 12, windows)).toBeNull() // the same time on a Friday
    expect(quietConflict(at(0, 10, 21), 5, windows)).toMatch(/23:30 to 00:30 America\/New_York on Fri/) // Saturday 00:10: Friday's window
    expect(quietConflict(at(0, 10, 22), 5, windows)).toBeNull() // Sunday 00:10: Saturday has no such window
    expect(quietConflict(at(1, 10, 21), 5, windows)).toBeNull()
  })
  test('no window declared: no refusal on the clock', () => {
    expect(quietConflict(at(17, 10), 120, [])).toBeNull()
    expect(drillSettings({}).quietHours).toEqual([])
    expect(drillSettings({ PEAT_QUIET_HOURS: JSON.stringify(windows) }).quietHours.length).toBe(4)
  })
  test('the guard refuses a drill inside its install\'s quiet hours with the reason, and accepts one clear of them', () => {
    const g = guard(new PlanExec(), false, null, { quietHours: windows })
    expect(() => g.assertClearOfQuietHours(12, at(16, 40))).toThrow(/not now: the quiet hours 17:00/)
    expect(() => g.assertClearOfQuietHours(12, at(13, 0))).not.toThrow()
  })
})

describe('the undo log is on disk before the change, and a failed undo stays there', () => {
  const dirs: string[] = []
  const tmp = () => { const d = mkdtempSync(join(tmpdir(), 'peat-drills-')); dirs.push(d); return d }
  const cleanup = () => { for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true }) }
  test('a machine cut writes the way back to pending.json BEFORE the machine is stopped, and clears it when the machine is started again', async () => {
    const dir = tmp()
    let seenAtStop: string[] = []
    const x = world({ also: (a) => { if (a[0] === 'machinectl' && a.includes('stop')) seenAtStop = readPending(dir).map((e) => e.what); return undefined } })
    const g = guard(x, false, dir)
    await g.cutMachine('machine-2')
    expect(seenAtStop).toEqual(['start machine-2 again'])
    expect(readPending(dir)[0]!.cmds).toEqual([['machinectl', 'start', 'machine-2']])
    await g.undoAll()
    expect(existsSync(pendingPath(dir))).toBe(false)
    cleanup()
  })
  test('a SIGKILL leaves the file: the next run refuses, and recover replays it (starting the machine), clears it and applies nothing of the platform again', async () => {
    const dir = tmp()
    const a = world(); const g1 = guard(a, false, dir)
    await g1.cutMachine('machine-3') // the process is "killed" here: no undo runs
    expect(() => assertNoPending(dir)).toThrow(/not undone.*start machine-3 again.*recover/)
    const b = world(); const g2 = guard(b, false, dir)
    const r = await g2.recover()
    expect(b.argvs.some((c) => c.join(' ') === 'machinectl start machine-3')).toBe(true)
    expect(r.pendingLeft).toEqual([])
    expect(b.argvs.some((c) => c.includes('apply') && !c.includes('-f'))).toBe(false)
    expect(existsSync(pendingPath(dir))).toBe(false)
    cleanup()
  })
  test('an undo that cannot reach the machine is tried three times and STAYS pending', async () => {
    const dir = tmp()
    let fails = true
    const x = world({ also: (a) => (a[0] === 'machinectl' && a.includes('start') && fails ? { code: 1, out: '', err: 'dial tcp: i/o timeout' } : undefined) })
    const g = guard(x, false, dir)
    await g.cutMachine('machine-1')
    await g.undoAll()
    expect(x.argvs.filter((c) => c[0] === 'machinectl' && c.includes('start')).length).toBe(3)
    expect(g.pending).toEqual(['start machine-1 again'])
    expect(readPending(dir).map((e) => e.what)).toEqual(['start machine-1 again'])
    fails = false
    await g.undoAll()
    expect(existsSync(pendingPath(dir))).toBe(false)
    cleanup()
  })
  test('the synchronous undo of the exit handler keeps what it could not do', async () => {
    const dir = tmp()
    const x = world({ also: (a) => (a[0] === 'machinectl' ? { code: 1, out: '', err: 'no route' } : undefined) })
    const g = guard(x, false, dir)
    await g.platform()
    ;(g as any).pushUndo('start machine-1 again', [['machinectl', 'start', 'machine-1']])
    g.undoSync()
    expect(readPending(dir).map((e) => e.what)).toEqual(['start machine-1 again'])
    cleanup()
  })
  test('the store drill refuses to save a stripped policy as "the original", and saves its own undo before it strips', async () => {
    const dir = tmp()
    const stripped = new Recorder((argv) => ({ code: 0, out: argv.includes('networkpolicy') ? JSON.stringify({ spec: { egress: [{ to: [{ podSelector: {} }] }] } }) : '{}', err: '' }))
    await expect(guard(stripped, false, dir).setPolicyEgress('peat', 'pageserver', [])).rejects.toThrow(/not as declared/)
    expect(existsSync(pendingPath(dir))).toBe(false)
    const egress = [{ to: [{ podSelector: { matchLabels: { app: 'storage-broker' } } }] }, { to: [{ ipBlock: { cidr: '0.0.0.0/0' } }] }]
    let seen: string[] = []
    const good = new Recorder((argv) => {
      if (argv.includes('patch')) seen = readPending(dir).map((e) => e.what)
      return { code: 0, out: argv.includes('networkpolicy') && argv.includes('get') ? JSON.stringify({ spec: { egress } }) : '{}', err: '' }
    })
    const g = guard(good, false, dir)
    await g.setPolicyEgress('peat', 'pageserver', egress.slice(0, -1))
    expect(seen).toEqual(['restore egress of networkpolicy peat/pageserver'])
    await g.undoAll()
    expect(existsSync(pendingPath(dir))).toBe(false)
    cleanup()
  })
  test('egressReachesStore: an egress with its rule to the internet passes; one without it does not', () => {
    const e = [{ to: [{ podSelector: {} }] }, { to: [{ ipBlock: { cidr: '0.0.0.0/0' } }] }]
    expect(egressReachesStore(e)).toBe(true)
    expect(egressReachesStore(e.slice(0, 1))).toBe(false)
    expect(egressReachesStore([])).toBe(false)
    expect(egressReachesStore(undefined)).toBe(false)
  })
  test('the pending file is replayed only as undos the guard makes: a hand-edited command that does anything else is refused', () => {
    const fine = [
      ['machinectl', 'start', 'machine-1'],
      ['kubectl', '-n', 'peat', 'scale', 'statefulset/pageserver-w1', '--replicas=1'],
      ['kubectl', '-n', 'peat', 'patch', 'networkpolicy', 'pageserver', '--type=json', '-p', '[{"op":"replace","path":"/spec/egress","value":[]}]'],
      ['kubectl', '-n', 'peat-gateway', 'patch', 'deployment', 'peat-gateway', '--type=json', '-p', '[{"op":"replace","path":"/spec/template/spec/containers/0/env","value":[]}]'],
      ['kubectl', '-n', 'peat-compute', 'delete', 'pod', 'drill-writer', '--ignore-not-found', '--wait=false'],
      ['kubectl', '-n', 'peat', 'delete', 'cluster', RESTORED_CLUSTER, '--ignore-not-found', '--wait=false'],
      ['kubectl', 'delete', 'persistentvolume', RESTORED_CLUSTER, '--ignore-not-found', '--wait=false'],
      ['kubectl', '-n', 'monitoring', 'exec', 'deploy/alertmanager', '--', 'amtool', '--alertmanager.url=http://localhost:9093', 'silence', 'expire', '0f3c1a2b-4d5e-6789-abcd-ef0123456789'],
    ]
    for (const c of fine) expect(() => assertReplayable(c, replay), c.join(' ')).not.toThrow()
    const bad = [
      ['machinectl', 'stop', 'machine-1'], ['machinectl', 'start', 'control-plane'], ['machinectl', 'start', 'machine-1', '--now'], ['machinectl', 'start', 'outside-1'], ['sh', '-c', 'machinectl start machine-1'],
      ['kubectl', '-n', 'app-db', 'delete', 'pod', 'drill-x'], ['kubectl', '-n', 'peat', 'delete', 'pod', 'pageserver-w1-0'], ['kubectl', '-n', 'peat', 'delete', 'cluster', 'storcon-db'], ['kubectl', 'delete', 'persistentvolume', 'pvc-1234'], ['kubectl', 'delete', 'namespace', 'peat'],
      ['kubectl', '-n', 'peat', 'scale', 'statefulset/pageserver-w1', '--replicas=9'], ['kubectl', '-n', 'app-db', 'scale', 'statefulset/app-db', '--replicas=0'], ['kubectl', '-n', 'peat', 'scale', 'job/x', '--replicas=1'],
      ['kubectl', '-n', 'peat', 'patch', 'secret', 'x', '--type=json', '-p', '[]'], ['kubectl', '-n', 'peat', 'patch', 'deployment', 'x', '--type=json', '-p', '[{"op":"replace","path":"/spec/replicas","value":0}]'],
      ['kubectl', 'delete', '--raw', '/api/v1/namespaces/kube-system/pods/x'], ['kubectl', '-n', 'monitoring', 'exec', 'deploy/alertmanager', '--', 'amtool', '--alertmanager.url=http://localhost:9093', 'silence', 'expire', '--all'], ['kubectl', '-n', 'monitoring', 'exec', 'deploy/prometheus', '--', 'rm', '-rf', '/'], ['kubectl', '-n', 'monitoring', 'delete', 'pod', 'drill-x'], ['kubectl', '-n', 'peat', 'exec', 'x', '--', 'rm', '-rf', '/'], ['sh', '-c', 'echo hi'], ['kubectl', '-n', 'peat', 'delete', 'pod', 'drill-x', '--force'], [], ['kubectl'],
    ]
    for (const c of bad) expect(() => assertReplayable(c as string[], replay), JSON.stringify(c)).toThrow(Refusal)
    // with no machine command set, no machine start is an undo at all
    expect(() => assertReplayable(['machinectl', 'start', 'machine-1'], { ...replay, machineCommand: null })).toThrow(Refusal)
  })
  test('run refuses while the file holds anything', () => {
    const dir = tmp()
    writeFileSync(pendingPath(dir), JSON.stringify([{ what: 'x', cmds: [] }]))
    expect(() => assertNoPending(dir)).toThrow(Refusal)
    cleanup()
  })
  test('recover sweeps drill-* objects, the restored Cluster and PersistentVolume, and the drills\' databases, and starts a platform machine whose node is not Ready by the installer\'s command', async () => {
    const x = world({ also: (argv) => {
      const j = argv.join(' ')
      if (/get node machine-2 -o json/.test(j)) return { code: 0, out: JSON.stringify(nodeJson('machine-2', true, false)), err: '' }
      if (/-n peat-compute get pods/.test(j)) return { code: 0, out: JSON.stringify({ items: [{ metadata: { name: 'drill-writer' } }, { metadata: { name: 'compute-drill-par200-x' } }] }), err: '' }
      if (/get clusters/.test(j)) return { code: 0, out: JSON.stringify({ items: [{ metadata: { name: RESTORED_CLUSTER } }, { metadata: { name: 'storcon-db' } }] }), err: '' }
      if (/get persistentvolumes/.test(j)) return { code: 0, out: JSON.stringify({ items: [{ metadata: { name: RESTORED_CLUSTER } }, { metadata: { name: 'peat-storcon-db' } }] }), err: '' }
      if (argv.includes('exec') && argv.at(-1)!.includes('/dbs')) return { code: 0, out: JSON.stringify({ status: 200, json: [{ name: 'drill-par200' }, { name: 'drill-uplink' }] }), err: '' }
      if (/ get (configmaps|networkpolicies|jobs|pods)/.test(j) && !/-n peat get pods -o json/.test(j)) return { code: 0, out: '{"items":[]}', err: '' }
      return undefined
    } })
    const g = guard(x, false)
    const swept = await g.sweepDrillObjects()
    expect(swept).toContain('peat-compute/Pod/drill-writer')
    expect(swept).toContain(`peat/Cluster/${RESTORED_CLUSTER}`)
    expect(swept).toContain(`PersistentVolume/${RESTORED_CLUSTER}`)
    expect(swept).toContain('database drill-uplink')
    expect(swept).not.toContain('database drill-par200') // the standing database is kept
    expect(swept.some((s) => s.includes('compute-drill-par200') || s.includes('storcon-db ') || s.endsWith('/peat-storcon-db'))).toBe(false)
    expect(await g.startStoppedMachines()).toEqual(['machine-2'])
    expect(x.argvs.filter((a) => a[0] === 'machinectl')).toEqual([['machinectl', 'start', 'machine-2']])
  })
})

describe('the reserved prefix: only the drills make drill* databases, and the drills make only those', () => {
  test('the guard makes a database only named drill*, sends the drills\' header on every call, and never starts, stops or deletes another', async () => {
    const x = new PlanExec(); const g = guard(x)
    await expect(g.control('POST', '/db', { name: 'tenant-one' })).rejects.toThrow(/only databases named drill/)
    await expect(g.control('POST', '/db', {})).rejects.toBeInstanceOf(Refusal)
    await expect(g.control('POST', '/branch', { from: 'a', name: 'tenant-b' })).rejects.toBeInstanceOf(Refusal)
    await expect(g.control('POST', '/db', { name: 'drill' + 'x'.repeat(30) })).rejects.toBeInstanceOf(Refusal)
    await g.control('POST', '/db', { name: 'drill-par200' })
    await g.control('POST', '/db', { name: 'drillbr' })
    const posted = x.argvs.filter((a) => a.includes('exec')).map((a) => a.at(-1)!)
    expect(posted.length).toBe(2)
    for (const script of posted) expect(script).toContain("'x-peat-drill': '1'")
  })
  test('recover waits for a started machine\'s node to be Ready', async () => {
    const t = read('guard.ts')
    const i = t.indexOf('const started = await this.startStoppedMachines()')
    expect(t.slice(i, i + 200)).toContain('await this.waitNodeReady(m, 360)')
    const x = world()
    await expect(guard(x).waitNodeReady('machine-2', 30)).resolves.toBe(true)
    await expect(guard(x).waitNodeReady('app-db', 30)).rejects.toBeInstanceOf(Refusal)
  })
})

describe('the other rules', () => {
  test('the pod\'s password goes in on stdin and is in no argument', async () => {
    const x = new Recorder(); const g = guard(x)
    await g.clientPsql('drill-sql', { host: 'drill-par200.peat-db.svc', password: 'S3cretPassw0rd', seconds: 30 }, 'select 1')
    const a = x.argvs.at(-1)!
    expect(JSON.stringify(a)).not.toContain('S3cretPassw0rd')
    expect(x.stdins.at(-1)).toBe('S3cretPassw0rd\n')
    expect(a).toContain('-i')
    await expect(g.clientPsql('drill-sql', { host: 'drill-par200.app-db.svc', password: 'x', seconds: 30 }, 'select 1')).rejects.toBeInstanceOf(Refusal)
  })
  test('the drills\' commands use the kubeconfig given (--kubeconfig or KUBECONFIG), and run.ts refuses while anything is pending', () => {
    expect(read('guard.ts')).toContain('...(kubeconfig ? { KUBECONFIG: kubeconfig } : {})')
    expect(read('run.ts')).toContain("const k = flag('kubeconfig')")
    expect(read('run.ts')).toContain('assertNoPending(s.stateDir)')
  })
  test('the alert watch polls every 3 seconds, inside Alertmanager\'s 30 second group wait', () => {
    expect(read('run.ts')).toContain('await c.sleep(3) // under Alertmanager')
  })
  test('restore-store, disk-loss and sk-replace refuse, changing nothing, while the platform holds a database that is not a drill\'s', async () => {
    const x = world({ also: (argv) => (argv.includes('exec') && argv.at(-1)!.includes('/dbs') ? { code: 0, out: JSON.stringify({ status: 200, json: [{ name: 'drill-par200' }, { name: 'drill-sleep' }, { name: 'tenant-one' }] }), err: '' } : undefined) })
    for (const name of ['restore-store', 'disk-loss', 'sk-replace']) {
      const g = guard(x, true)
      const c = new FakeCtx(g, name, 1)
      await expect(DRILLS.find((d) => d.name === name)!.run(c, {})).rejects.toThrow(/not the drills' own \(tenant-one\)/)
      expect(g.actions.filter((a) => /^(WIPE|scale|cut)/i.test(a.what))).toEqual([])
    }
  })
  test('the uplink loads in chunks, the next only after the previous is flushed, and stops on the expression the install names', () => {
    const t = read('drills.ts')
    const u = t.slice(t.indexOf("name: 'uplink'"))
    expect(u).toContain('await c.g.prom(guard.behind)')
    expect(u).toContain('while (!(await c.flushed(D)))')
    expect(u.indexOf('while (!(await c.flushed(D)))')).toBeGreaterThan(u.indexOf('insert into uplink_load'))
    expect(u).toContain('chunkRows = 100_000')
    expect(drillSettings({ PEAT_DRILL_UPLINK_BEHIND: 'up == 0', PEAT_DRILL_UPLINK_ALERT: 'Behind' }).uplink).toEqual({ behind: 'up == 0', alert: 'Behind' })
  })
  test('the drill that lets an alert through reads Alertmanager\'s own notification counters, before and during', async () => {
    const x = new Recorder((a) => ({ code: 0, out: a.includes('--raw') ? '# HELP x\nalertmanager_notifications_total{integration="webhook"} 12\nalertmanager_notifications_total{integration="email"} 3\nalertmanager_notifications_failed_total{integration="webhook"} 1\n' : '{}', err: '' }))
    expect(await guard(x).notificationCounters()).toEqual({ asked: 15, failed: 1 })
    expect(read('run.ts')).toContain('sent.before = await c.g.notificationCounters()')
    expect(read('run.ts')).toContain("the first at ${b.sentAt ?? 'NEVER'}")
  })
  test('the controller database restore refuses, changing nothing, when no controller database is named', async () => {
    const g = new Guard(new PlanExec(), { dry: true, clock: new FakeClock(), log: () => {}, settings: fakeSettings({ controllerDatabase: null }) })
    await expect(DRILLS.find((d) => d.name === 'storcon-restore')!.run(new FakeCtx(g, 'storcon-restore', 1), {})).rejects.toThrow(/PEAT_DRILL_CONTROLLER_DB/)
    expect(g.actions).toEqual([])
  })
})

describe('every drill, run against a fake world with the real guard', () => {
  for (const d of DRILLS)
    test(`${d.name}: every step passes the guard, the machine cuts are the platform's, and the undo log ends empty`, async () => {
      const clock = new FakeClock(); const x = new PlanExec(); const lines: string[] = []
      const g = new Guard(x, { dry: true, clock, log: (l) => lines.push(l), settings: fakeSettings() })
      const c = new FakeCtx(g, d.name, 1)
      await g.silence({ exclude: d.alerts?.tested, comment: 'test' })
      await d.run(c, { want: undefined })
      await g.undoAll()
      expect(g.pending).toEqual([])
      const cuts = g.actions.filter((a) => a.what.startsWith('cut '))
      expect(cuts.length).toBe(d.machineCut ? 1 : 0)
      for (const a of g.actions) {
        const m = /^(?:cut|start) (\S+)$/.exec(a.what)
        if (m) expect(MACHINES).toContain(m[1]!)
        if (a.argv && a.argv[0] !== 'kubectl') expect(a.argv[0]).toBe('machinectl')
        const w = /^WIPE (\S+?):(\S+)$/.exec(a.what)
        if (w) expect(() => volumeTarget(w[1], w[2], SHAPE, DEFAULT_VOLUMES)).not.toThrow()
      }
      // every argv that reached the (fake) cluster is a kubectl read, or one the guard itself builds for a platform piece; in a dry run no machine command runs at all
      for (const a of x.argvs) {
        expect(a[0]).toBe('kubectl')
        const nsAt = a.indexOf('-n')
        if (nsAt >= 0 && a[nsAt + 1] !== 'monitoring') expect(NAMESPACES).toContain(a[nsAt + 1]!)
      }
    })
  test('worker-comp refuses before cutting when the compute\'s machine does not match what the run wants', async () => {
    const g = new Guard(new PlanExec(), { dry: true, clock: new FakeClock(), log: () => {}, settings: fakeSettings() })
    const c = new FakeCtx(g, 'worker-comp', 1, 'machine-3', ['machine-1', 'machine-2'])
    await expect(DRILLS.find((d) => d.name === 'worker-comp')!.run(c, { want: 'with-gateway' })).rejects.toThrow(/Nothing was cut/)
    expect(g.actions.filter((a) => a.what.startsWith('cut '))).toEqual([])
  })
  test('plan() prints the steps and returns', async () => {
    const orig = console.log; const out: string[] = []; console.log = (l: string) => { out.push(String(l)) }
    try { await plan(DRILLS.find((d) => d.name === 'ps-hold')!) } finally { console.log = orig }
    expect(out.some((l) => /KILL pod peat\/pageserver-w1-0/.test(l))).toBe(true)
  })
  test('the drills that cut a machine, the store or the uplink are the ones that must be clear of the quiet hours', () => {
    const clear = DRILLS.filter((d) => d.clear).map((d) => d.name).sort()
    expect(clear).toEqual(['alerts-store', 'store-down', 'worker', 'worker-comp', 'worker-ctl', 'worker-w3', 'uplink'].sort())
    for (const d of DRILLS.filter((d) => d.machineCut)) expect(d.clear).toBe(true)
  })
})

describe('no other file builds a command', () => {
  test('the drills\' other files contain no process spawn and no kubectl: the guard is the one door', () => {
    for (const f of readdirSync(drillsDir).filter((f) => f.endsWith('.ts') && !['guard.ts', 'fake.ts'].includes(f))) {
      const text = read(f)
      expect(text, f).not.toMatch(/Bun\.spawn|child_process|execSync|spawnSync|\bkubectl\b|Bun\.\$|\$`/)
    }
  })
  test('the guard is the only file that imports a way to run a command, and it takes it by injection', () => {
    for (const f of readdirSync(drillsDir).filter((f) => f.endsWith('.ts') && f !== 'guard.ts')) expect(read(f), f).not.toMatch(/from 'node:(child_process|os)'/)
    expect(read('guard.ts')).toContain('export class RealExec')
  })
})

describe('the clients and the analysis', () => {
  test('the reader scans the HEAP: its statement reads every row\'s payload, not a count, and its expected answer is the table\'s size', () => {
    expect(READER_SQL).toBe('select sum(length(payload)) from filler')
    expect(READER_SQL).not.toMatch(/count\(/)
    expect(READER_EXPECT).toBe(String(FILLER_ROWS * FILLER_BYTES))
    expect(read('lib.ts')).toContain('startReader(node: string, db = DB) { await this.startClientPod(\'drill-reader\', node, db, loop(\'reader\', READER_SQL, 60)) }')
    expect(DB).toBe('drill-par200')
  })
  test('the clients connect through the front door with verify-full against the platform CA\'s public half, in the computes\' namespace, on the platform machines', () => {
    const t = read('lib.ts')
    expect(t).toContain("PGSSLMODE: 'verify-full'")
    expect(t).toContain("PGSSLROOTCERT: '/ca/ca.crt'")
    expect(t).toContain('`${db}.${n.dbNamespace}.svc`')
    expect(t).toContain("'tls.crt'")
    expect(t).not.toContain("'tls.key'")
  })
  test('analyzeWriter: acknowledged rows missing, the longest gap and what it began and ended after the fault', () => {
    const log = ['ok 1000 1100 1|t', 'ok 2000 2100 2|t', 'ok 3000 3100 3|t', 'err 4000 14000 rc=124 timeout', 'ok 15000 15100 4|t', 'ok 16000 16100 5|t', 'ok 17000 17100 5|t'].join('\n')
    const s = analyzeWriter(log, '1\n2\n3\n5\n', 3500)
    expect(s.ackedTotal).toBe(6)
    expect(s.missingAcked).toBe(1)
    expect(s.missingFirst).toEqual([4])
    expect(s.duplicates).toEqual([5])
    expect(s.longestGapS).toBe(12)
    expect(s.gapBeganMsAfterFault).toBe(3100 - 3500)
    expect(s.firstOkAfterGapEndedMsAfterFault).toBe(15100 - 3500)
    expect(s.errsFromFault).toBe(1)
  })
  test('analyzeReader: the longest gap, errors and answers that are not the expected one', () => {
    const log = ['ok 1000 1500 200000000', 'ok 3000 3500 200000000', 'err 4000 9000 rc=1 boom', 'ok 20000 20500 1'].join('\n')
    const r = analyzeReader(log, 3000)
    expect(r.answers).toBe(3)
    expect(r.longestGapS).toBe(17)
    expect(r.wrongCounts).toBe(1)
    expect(r.errsFromFault).toBe(1)
  })
  test('the snapshot diff flags a changed pod of a watched namespace, ignores a runner of an unwatched one and the platform\'s own pods', () => {
    const snap = (pods: Snapshot['pods']): Snapshot => ({ at: '', pods, firing: [], availableGiB: null })
    const appDb = { ns: 'app-db', name: 'app-db-1', uid: 'aaaa1111', node: 'db', restarts: 0, job: false }
    const gw = { ns: 'other-gateway', name: 'other-gateway-x', uid: 'bbbb2222', node: 'main', restarts: 0, job: false }
    const runner = { ns: 'ci-runners', name: 'runner-1', uid: 'cccc3333', node: 'ci', restarts: 0, job: false }
    const o = { platform: NAMESPACES, watched: ['app-db', 'other-gateway'] }
    const a = snap([appDb, gw, runner])
    expect(diffSnapshots(a, snap([appDb, gw]), o).flagged).toEqual([])
    expect(diffSnapshots(a, snap([{ ...appDb, restarts: 1 }, gw, runner]), o).flagged.join()).toContain('app-db/app-db-1: restarts 0 to 1')
    expect(diffSnapshots(a, snap([appDb, { ...gw, uid: 'dddd4444' }, runner]), o).flagged.join()).toContain('other-gateway/other-gateway-x: a different pod')
    expect(diffSnapshots(a, snap([appDb, runner]), o).flagged.join()).toContain('other-gateway')
  })
  test('with no list of watched namespaces, every namespace but the platform\'s and kube-system is watched', () => {
    const snap = (pods: Snapshot['pods']): Snapshot => ({ at: '', pods, firing: [], availableGiB: null })
    const runner = { ns: 'ci-runners', name: 'runner-1', uid: 'cccc3333', node: 'ci', restarts: 0, job: false }
    const own = { ns: 'peat', name: 'pageserver-w1-0', uid: 'eeee5555', node: 'machine-1', restarts: 0, job: false }
    const dns = { ns: 'kube-system', name: 'coredns-1', uid: 'ffff6666', node: 'main', restarts: 0, job: false }
    const d = diffSnapshots(snap([runner, own, dns]), snap([{ ...runner, restarts: 2 }, { ...own, restarts: 3 }, { ...dns, restarts: 1 }]), { platform: NAMESPACES })
    expect(d.flagged).toEqual(['ci-runners/runner-1: restarts 0 to 2'])
  })
})
