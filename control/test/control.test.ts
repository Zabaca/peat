import { describe, expect, test } from 'bun:test'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DEFAULT_NAMES, GROUP, fromEnv, frontDoor, placement, toEnv } from '../src/names'
import {
  DEFAULT_PG_VERSION,
  SAFEKEEPER_RESOURCES,
  UNRECORDED_PG_VERSION,
  certInfo,
  certNeedsReload,
  closeDecision,
  computeImageFor,
  decommissionDecision,
  formatSeedGroup,
  isPullTimelineFailure,
  keeperNodeProblem,
  keeperReplacementVerdict,
  memoryGauges,
  membersOffBox,
  moveDecision,
  neverWritten,
  nodeDrainState,
  nodeFillState,
  otherReplacementOpen,
  parseSeedGroup,
  pgVersionOf,
  placementFor,
  placementVerdict,
  reopenDecision,
  replaceDecision,
  replacementMachines,
  replacementPool,
  requestedPgVersion,
  safekeeperScript,
  safekeeperStatefulSet,
  secondaryWarmProblem,
  seedCompletion,
  seedDecision,
  callerVerdict,
  seedGroup,
  lostEverywhere,
  shardLocations,
  seedIds,
  staleErrorProblem,
  startVerdict,
  storageStateProblems,
  storageStateReport,
  tcpUp,
  totalLossEvidence,
  within,
  type MemberAt,
  type ReplacementMark,
  type SafekeeperOptions,
  type ShardLocations,
} from '../src/storage-spec'

// The control layer's program (src/control.ts) and its pure decisions (src/storage-spec.ts). The decisions are judged with literal rows;
// what the program does with them runs only against a cluster, so what is pinned for it here is what its source shows: the order of its
// calls, the routes it serves and to whom, the settings it reads. The objects the operator renders around it (its Roles, policies, the
// storage pieces) are judged with the operator.

const src = (p: string) => readFileSync(join(import.meta.dir, '../src', p), 'utf8')
const control = src('control.ts')
const LABEL = DEFAULT_NAMES.machineLabel
const KEEPER = { key: `${GROUP}/keeper`, value: 'true' }

const baseOptions: SafekeeperOptions = {
  id: 3,
  node: 'machine-3',
  image: 'storage:test',
  namespace: DEFAULT_NAMES.namespace,
  controlUrl: 'http://peat-control.peat.svc:8080',
  remote: { endpoint: 'https://store.example', bucket: 'peat-safekeeper', region: 'auto', prefix: 'safekeeper' },
  secrets: { store: 'peat-store-safekeeper', auth: 'peat-auth-public', tokens: 'peat-safekeeper-tokens' },
  placement: placement(DEFAULT_NAMES),
  volumeRoot: '/var/lib/peat/safekeeper',
  walBackupParallelJobs: 2,
  resources: {},
}

/** The routes each caller class may call, read from the program's own table. */
const callerRoutes = (): Record<string, [string, RegExp][]> => {
  const start = control.indexOf('const CALLER_ROUTES')
  const block = control.slice(start, control.indexOf('\n}\n', start) + 2).replace(/^const CALLER_ROUTES: [^=]*= /, '')
  return new Function(`return ${block}`)() as Record<string, [string, RegExp][]>
}

describe('the names and settings the program reads', () => {
  test('the namespaces, the machine label and taint and the dead taint come from the environment through names.ts, each unset one a neutral default', () => {
    expect(control).toContain("import { GROUP, controlUrl, fromEnv, frontDoor, parseLabel, placement } from './names'")
    expect(control).toContain('const NAMES = fromEnv()')
    expect(control).toContain('const PLACEMENT = placement(NAMES)')
    expect(control).toContain('const DEAD_TAINT = NAMES.deadTaint.key')
    expect(fromEnv({})).toEqual(DEFAULT_NAMES)
    expect(DEFAULT_NAMES).toMatchObject({ namespace: 'peat', computeNamespace: 'peat-compute', gatewayNamespace: 'peat-gateway', dbNamespace: 'peat-db', machineLabel: { key: 'peat.zabaca.com/machine', value: 'true' }, machineTaint: null })
    expect(DEFAULT_NAMES.deadTaint).toEqual({ key: 'peat.zabaca.com/dead', value: 'true', effect: 'NoSchedule' })
    const own = { ...DEFAULT_NAMES, namespace: 'db-platform', machineLabel: { key: 'example.com/db', value: 'yes' }, machineTaint: { key: 'example.com/db', value: 'yes', effect: 'NoSchedule' as const } }
    expect(fromEnv(Object.fromEntries(toEnv(own).map((e) => [e.name, e.value])))).toEqual(own)
    expect(placement(own)).toEqual({ nodeSelector: { 'example.com/db': 'yes' }, tolerations: [{ key: 'example.com/db', operator: 'Equal', value: 'yes', effect: 'NoSchedule' }] })
    expect(placement(DEFAULT_NAMES).tolerations).toEqual([])
    expect(() => fromEnv({ MACHINE_LABEL: 'no-equals' })).toThrow(/key=value/)
    expect(() => fromEnv({ MACHINE_TAINT: 'a=b' })).toThrow(/key=value:Effect/)
  })

  test('the machine label is compared with its value, never with a literal true, wherever the program reads or selects machines', () => {
    expect(control).toContain('if (n.metadata?.labels?.[PEAT_LABEL] !== PEAT_LABEL_VALUE) throw new Error')
    expect(control).toContain('encodeURIComponent(`${PEAT_LABEL}=${PEAT_LABEL_VALUE}`)')
    expect(control).toContain('if (n.metadata.labels?.[PEAT_LABEL] !== PEAT_LABEL_VALUE) continue')
    expect(control).not.toContain("[PEAT_LABEL] !== 'true'")
  })

  test('the keeper machines, their label and tolerations, the safekeepers\' Secrets and their volume are settings with neutral defaults', () => {
    expect(control).toContain("parseLabel(process.env['KEEPER_LABEL'] || `${GROUP}/keeper=true`)")
    expect(control).toContain("JSON.parse(process.env['KEEPER_TOLERATIONS'] || '[]')")
    expect(control).toContain("(process.env['KEEPER_MACHINES'] ?? '').split(',').filter(Boolean)")
    expect(control).toContain("process.env['SAFEKEEPER_STORE_SECRET'] || 'peat-store-safekeeper'")
    expect(control).toContain("process.env['AUTH_PUBLIC_SECRET'] || 'peat-auth-public'")
    expect(control).toContain("process.env['SAFEKEEPER_TOKENS_SECRET'] || 'peat-safekeeper-tokens'")
    expect(control).toContain("process.env['SAFEKEEPER_VOLUME'] || '/var/lib/peat/safekeeper'")
    expect(control).toContain("process.env['ROUTE_ANNOTATION'] || `${GROUP}/route`")
  })

  test('the front door is the gateways\' one Service unless the install names another', () => {
    expect(control).toContain("process.env['FRONT_DOOR'] ?? frontDoor(NAMES)")
    expect(frontDoor(DEFAULT_NAMES)).toBe('pg-front.peat-gateway.svc.cluster.local')
  })

  test('the compute reconciler knows a database by its record too, and a status written again keeps its requests answered', () => {
    const reconciler = control.slice(control.indexOf('async function reconcileComputes'), control.indexOf('async function reconcileComputes') + 1500)
    expect(reconciler).toContain('const recs = await readRecords(RECORDS)')
    expect(reconciler).toContain('const d = knownDb(name, dbs, recs)')
    const rebuild = control.slice(control.indexOf("=== 'rebuild') {"), control.indexOf("=== 'rebuild') {") + 1200)
    expect(rebuild).toContain("Object.entries(o.spec?.requests ?? {})")
    expect(rebuild).toContain('{ answered }')
  })
  test('its program imports nothing but Node\'s own and the folder\'s shared modules', () => {
    for (const f of ['control.ts', 'storage-spec.ts']) {
      const specifiers = [...src(f).matchAll(/^import .* from '([^']+)'/gm)].map((m) => m[1]!)
      for (const s of specifiers) expect(s.startsWith('node:') || ['./storage-spec', './names', './operator/keys', './databases', './records', './quiet-hours', './move', './exec', './api/schema'].includes(s), `${f}: ${s}`).toBe(true)
    }
  })
})

describe('what the control layer makes, and what its account must be granted', () => {
  test('the objects it makes carry only its own labels: a label copied from elsewhere could make another tool prune them', () => {
    const keys = [...control.matchAll(/labels: \{([^}]*)\}/g)].flatMap((m) => [...m[1]!.matchAll(/(?:'([^']+)'|\b([a-z][\w-]*))\s*:/g)].map((k) => k[1] ?? k[2]!))
    expect(keys.length).toBeGreaterThan(8)
    for (const k of keys) expect(['app', 'compute', 'tenant', 'peat-port', 'peat-record', 'peat-compute-spec', 'peat-compute-deploy', 'peat-pg', 'peat-db-cert', 'peat-route', 'peat-sk', 'peat-sk-lost', 'peat-sk-open', 'peat-sk-keeper-tried', 'peat-recovered', 'peat-sk-replacement', 'peat-held', 'peat-superseded'], k).toContain(k)
  })

  // What the account needs, per namespace, read from the program's calls: the operator's Roles grant exactly this (judged with them).
  const NEEDS: Record<string, { group: string; resource: string; verbs: string[] }[]> = {
    NS: [
      { group: '', resource: 'pods', verbs: ['get', 'list', 'delete'] },
      { group: '', resource: 'secrets', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
      { group: '', resource: 'configmaps', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
      { group: 'apps', resource: 'statefulsets', verbs: ['get', 'list', 'create', 'update', 'delete'] },
      { group: 'batch', resource: 'jobs', verbs: ['get'] },
      { group: 'coordination.k8s.io', resource: 'leases', verbs: ['get', 'list', 'create', 'update', 'patch'] },
    ],
    CNS: [
      { group: '', resource: 'pods', verbs: ['get', 'list', 'delete'] },
      { group: '', resource: 'secrets', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
      { group: 'apps', resource: 'deployments', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
      { group: 'cert-manager.io', resource: 'certificates', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
    ],
    DB_NS: [{ group: '', resource: 'services', verbs: ['get', 'list', 'create', 'update', 'delete'] }],
    GW_NS: [{ group: '', resource: 'pods', verbs: ['get', 'list', 'delete'] }],
  }

  test('every call it makes with a literal path is within what it needs in that namespace, and in its own namespace it makes no Service, Deployment or pod', () => {
    const verbOf = (method: string, named: boolean) => ({ GET: named ? 'get' : 'list', POST: 'create', PUT: 'update', PATCH: 'patch', DELETE: 'delete' })[method]!
    const calls = [
      ...[...control.matchAll(/kube\('(GET|POST|PUT|PATCH|DELETE)', `(\/apis?\/[^`]*)`/g)].map((m) => [m[1]!, m[2]!]),
      ...[...control.matchAll(/\bapply\(`(\/apis?\/[^`]*)`/g)].flatMap((m) => [['POST', m[1]! + '/x'], ['PUT', m[1]! + '/x']]),
      ...[...control.matchAll(/\bdel\(`(\/apis?\/[^`]*)`/g)].map((m) => ['DELETE', m[1]!]),
    ]
    let checked = 0
    for (const [method, raw] of calls) {
      const m = /^\/apis?\/(?:(?:v1)|([a-z.0-9-]+)\/v1)\/namespaces\/\$\{(\w+)\}\/([a-z]+)(\/[^?]*)?/.exec(raw!)
      if (!m || !NEEDS[m[2]!]) continue
      const named = Boolean(m[4]) && m[4] !== '/'
      const granted = NEEDS[m[2]!]!.some((r) => r.group === (m[1] ?? '') && r.resource === m[3] && r.verbs.includes(verbOf(method!, named)))
      expect(granted, `${method} ${raw}`).toBe(true)
      checked++
    }
    expect(checked).toBeGreaterThan(30)
    expect(NEEDS['NS']!.map((r) => r.resource).sort()).toEqual(['configmaps', 'jobs', 'leases', 'pods', 'secrets', 'statefulsets'])
    // Nodes: read and the dead taint patched, never created or deleted (an admission rule holds the patch to the platform's machines)
    for (const m of control.matchAll(/kube\('(GET|POST|PUT|PATCH|DELETE)', `\/api\/v1\/nodes/g)) expect(['GET', 'PATCH']).toContain(m[1]!)
  })

  test('a database\'s certificate is created once and an existing one is left alone (a replace of a custom resource needs its resourceVersion: the first wake failed with a 422)', () => {
    expect(control).toContain("`create certificate ${tlsSecret(name)}`, [409])")
    expect(control).not.toMatch(/apply\(`\/apis\/cert-manager\.io/)
  })
})

describe('who may call which route', () => {
  test('a gateway reaches only what its program calls (never a delete, a restore, the trim, an upgrade or a registration), the safekeepers one question and the storage controller its hook', () => {
    const routes = callerRoutes()
    const allowed = (cls: string, method: string, path: string) => routes[cls]!.some(([m, re]) => m === method && re.test(path))
    const api = src('gateway/neon-api.ts') + src('gateway/main.ts')
    const gatewayCalls = [...api.matchAll(/call\('(GET|POST)', `([^`]*)`/g)].map((m) => [m[1]!, m[2]!.replace(/\$\{encodeURIComponent\([^}]*\)\}/g, 'a-db')]).concat([...api.matchAll(/call\('(GET|POST)', '([^']*)'/g)].map((m) => [m[1]!, m[2]!]))
    gatewayCalls.push(['GET', '/compute-addrs'], ['GET', '/route/a-db'], ['GET', '/gateway/peers'], ['POST', '/gateway/lease'])
    expect(gatewayCalls.length).toBeGreaterThan(8)
    for (const [m, p] of gatewayCalls) expect(allowed('gateway', m!, p!), `${m} ${p}`).toBe(true)
    for (const [m, p] of [['DELETE', '/db/a-db'], ['POST', '/restore'], ['POST', '/branch'], ['POST', '/db'], ['POST', '/replica'], ['POST', '/wal-trim'], ['POST', '/sk-upgrade'], ['POST', '/sk-register'], ['POST', '/sk-replace'], ['POST', '/sk-replace/parallel'], ['GET', '/sk-audit'], ['PUT', '/hook/notify-attach'], ['POST', '/db/a-db/stop/extra'], ['DELETE', '/cluster/a-db']])
      expect(allowed('gateway', m!, p!), `${m} ${p}`).toBe(false)
    expect(allowed('safekeeper', 'GET', '/sk/3/may-start-empty')).toBe(true)
    for (const [m, p] of [['GET', '/clusters'], ['POST', '/sk-register'], ['DELETE', '/db/a-db'], ['GET', '/sk/3/may-start-empty/x']]) expect(allowed('safekeeper', m!, p!)).toBe(false)
    expect(allowed('controller', 'PUT', '/hook/notify-attach')).toBe(true)
    for (const [m, p] of [['GET', '/hook/notify-attach'], ['POST', '/restore'], ['PUT', '/hook/a/b']]) expect(allowed('controller', m!, p!)).toBe(false)
    expect(control).toContain('server.requestIP(req)')
  })

  test('a caller in no class fails closed (only /healthz and /metrics), a compute is held to the one route it asks, and only the pod\'s own loopback is not narrowed', () => {
    const routes = callerRoutes()
    const allowed = (cls: string, method: string, path: string) => routes[cls]!.some(([m, re]) => m === method && re.test(path))
    const handler = control.slice(control.indexOf('async fetch(req, server)'))
    const served = [...handler.matchAll(/path === '(\/[^']+)'/g)].map((m) => m[1]!).concat([...handler.matchAll(/path\.startsWith\('(\/[^']+)'\)/g)].map((m) => m[1]! + 'x'))
    expect(served.length).toBeGreaterThan(20)
    for (const p of served.filter((p) => p !== '/compute/current')) for (const method of ['GET', 'POST', 'PUT', 'DELETE']) expect(allowed('compute', method, p), `a compute may not ${method} ${p}`).toBe(false)
    expect(allowed('compute', 'GET', '/compute/current')).toBe(true)
    expect(allowed('compute', 'POST', '/compute/current')).toBe(false)
    for (const p of served.filter((p) => p !== '/healthz' && p !== '/metrics')) for (const method of ['GET', 'POST', 'PUT', 'DELETE']) expect(allowed('unknown', method, p), `an unknown caller may not ${method} ${p}`).toBe(false)
    expect(allowed('unknown', 'GET', '/healthz')).toBe(true)
    expect(allowed('unknown', 'GET', '/metrics')).toBe(true)
    expect(allowed('unknown', 'POST', '/metrics')).toBe(false)
    const decide = control.slice(control.indexOf('async function callerClass'), control.indexOf('const hookLog'))
    expect(decide).toContain("if (address && /^(127\\.|::1$|::ffff:127\\.)/.test(address)) return null")
    expect(decide).toContain("if (!address) return 'unknown'")
    expect(decide).toContain("?? 'unknown'")
    expect(decide).toContain("catch { return 'unknown' }")
    expect(decide).not.toContain("return 'gateway'")
    expect(control).toContain("const verdict = cls ? callerVerdict(CALLER_ROUTES, cls, req.method, path) : 'pass'")
    // an address in no class calling what a piece may (a storage controller's first notification, before its pod's address is listed) is
    // asked to call again (503, retried by the controller), never admitted; what no piece may call is refused
    expect(callerVerdict(routes, 'unknown', 'PUT', '/hook/notify-attach')).toBe('again')
    expect(callerVerdict(routes, 'unknown', 'GET', '/compute/current')).toBe('again')
    expect(callerVerdict(routes, 'unknown', 'GET', '/healthz')).toBe('pass')
    expect(callerVerdict(routes, 'unknown', 'POST', '/db')).toBe('refuse')
    expect(callerVerdict(routes, 'compute', 'PUT', '/hook/notify-attach')).toBe('refuse')
    expect(callerVerdict(routes, 'controller', 'PUT', '/hook/notify-attach')).toBe('pass')
    expect(control).toContain("status: 503, headers: { 'content-type': 'application/json', 'retry-after': '1' }")
    // a move's exec is asked of the operator (status.move.pods) and waited for, bounded: this layer grants itself nothing
    const asking = control.slice(control.indexOf('async function grantExecs'), control.indexOf('async function readyPod'))
    expect(asking).toContain('await patchDbStatus(ns, name, { move: { pods } })')
    expect(asking).toContain('if (Date.now() - t0 > GRANT_WAIT_MS) throw new Error(')
    expect(asking).not.toMatch(/roles|rolebindings/)
    // a Database being deleted (or gone) asks for no exec in its cleanup: it would only wait out the grant in the loop that serves the others
    // the way back reads nothing: no exec, no comparison, no count of rows or writes; it keeps the version it leaves as a database of its own
    const back = control.slice(control.indexOf('async function runMoveBack'), control.indexOf('async function deleteOldCopies'))
    // it writes what it needs to finish (state going-back) before its first step, and a restart goes on with it from the moves' loop
    expect(back).toContain("state: 'going-back', oldTimeline: oldRec.timeline, newerDatabase: newer, newerTimeline: d.timeline, deleteNewerAfter: until")
    expect(control).toContain("if (mv.state === 'going-back') return finishMoveBack(ns, name)")
    expect(back).toContain('const steps = backSteps({')
    for (const reads of ['grantExecs', 'localPsql', 'fingerprintSql', 'compareSides', 'WRITES_SQL', 'ROLES_SQL', 'readyPod', 'startCompute']) expect(back, reads).not.toContain(reads)
    // the version it leaves under the first free name (a second way back's copy beside the first), each copy carrying its own delete time
    expect(back).toContain('if (!(await getDb(n)) && !records.has(n)) { newer = n; break }')
    expect(back).toContain('annotations: { [DELETE_AFTER]: mv.deleteNewerAfter }')
    expect(back).toContain("[COPYING]: 'true', ...(mv.deleteNewerAfter ? { [DELETE_AFTER]: mv.deleteNewerAfter } : {})")
    // a refusal is in the status's message, in the move's backRefused that the command prints, and in the log
    expect(back).toContain("move: { backRefused: `${request}: ${why}` }")
    expect(back).toContain('console.log(`move back ${ns}/${name}: refused: ${why}`)')
    const due = control.slice(control.indexOf('async function deleteOldCopies'), control.indexOf('// ---- the Database objects'))
    expect(due).toContain("if (!after || !o.spec?.parent || o.metadata.deletionTimestamp || !(Date.parse(after) <= Date.now())) continue")
    expect(back).toContain('await stopCompute(name, true)')
    expect(back).toContain('what was written since is in ${newer} until ${mv.deleteNewerAfter}')
    const cleanup = control.slice(control.indexOf('async function cleanupMove'), control.indexOf('async function runMove'))
    expect(cleanup).toContain("const going = obj.status === 404 || Boolean(obj.json?.metadata?.deletionTimestamp)")
    expect(cleanup).toContain('const src = going ? null : await livePod(name)')
    expect(control).toContain("labelSelector=app%3Dpeat-compute`), 'list compute pods'")
    // names that reach an API path are held to a pattern, and a name with a slash or `..` matches neither
    const DB = /^[a-z][a-z0-9-]{0,30}$/
    const POD = /^[a-z0-9]([a-z0-9-]{0,250}[a-z0-9])?$/
    expect(readFileSync(join(import.meta.dir, '../src/databases.ts'), 'utf8')).toContain('export const DB_NAME = /^[a-z][a-z0-9-]{0,30}$/')
    expect(control).toContain("import { COPYING, DB_NAME,")
    expect(control).toContain('const POD_NAME = /^[a-z0-9]([a-z0-9-]{0,250}[a-z0-9])?$/')
    for (const bad of ['..', '../x', 'a/b', '', 'A', '-a', 'a.b']) { expect(DB.test(bad), bad).toBe(false); expect(POD.test(bad), bad).toBe(false) }
    for (const p of ['/pod/..', '/pod/a/b', '/cluster/..', '/cluster/a.b', '/route/../x', '/cluster/' + 'a'.repeat(32)]) expect(allowed('gateway', 'GET', p), p).toBe(false)
    for (const p of ['/pod/compute-mydb-5f9c7d6b8-abcde', '/cluster/my-db', '/route/my-db']) expect(allowed('gateway', 'GET', p), p).toBe(true)
    for (const marker of ["path.startsWith('/cluster/')) { if (!DB_NAME", "path.startsWith('/route/')) { if (!DB_NAME", "path.startsWith('/pod/')) { if (!POD_NAME", 'if (!DB_NAME.test(name) || !POD_NAME.test(pod)) return json({ current: false })']) expect(control).toContain(marker)
  })

  test('/sk-register registers only a safekeeper whose pod answers, and refuses one the controller lists as anything but Active unless it is forced', () => {
    const i = control.indexOf("path === '/sk-register'")
    const block = control.slice(i, control.indexOf('registerSafekeeper(id)', i))
    expect(block).toContain('/v1/status')
    expect(block).toContain("scheduling_policy !== 'Active'")
    expect(block).toContain("searchParams.get('force') !== '1'")
    expect(block).toContain('the storage controller did not answer')
  })
})

describe('the program\'s settings for the pieces it makes', () => {
  test('a safekeeper asks 128 MiB and may use 512; a compute\'s scratch is capped; a safekeeper carries no API credential or service links', () => {
    expect(SAFEKEEPER_RESOURCES).toMatchObject({ requests: { memory: '128Mi' }, limits: { memory: '512Mi' } })
    expect(control).toContain('resources: SK_RESOURCES')
    expect(control).toContain("process.env['SAFEKEEPER_MEMORY_REQUEST'] || SAFEKEEPER_RESOURCES.requests.memory")
    expect(control).toContain('emptyDir: { sizeLimit: PGDATA_LIMIT }')
    expect(safekeeperStatefulSet(baseOptions).spec.template.spec).toMatchObject({ automountServiceAccountToken: false, enableServiceLinks: false })
    expect(control).not.toContain('COMPUTE_SERVICES')
  })

  test('the stall settings reach the computes: the program writes the pageserver disconnect timeout and the connection options it is given', () => {
    expect(control).toContain('neon.pageserver_response_disconnect_timeout')
    expect(control).toContain("process.env['PS_CONN_OPTS']")
  })

  test('the WAL trim reads each timeline hourly and lists the store only when its floor moved', () => {
    expect(control).toContain("process.env['WAL_TRIM_EVERY_S'] ?? '3600'")
    expect(control).toContain('trimmedAt')
    expect(control).not.toMatch(/neon-lab|\/var\/lib\/neon/)
    expect(control).not.toContain('node-fs')
  })

  test('a safekeeper\'s volume must exist by default; it is made only when the install asks (unchecked, so the runtime makes it on the machine)', () => {
    expect(safekeeperStatefulSet(baseOptions).spec.template.spec.volumes[0].hostPath).toEqual({ path: '/var/lib/peat/safekeeper', type: 'Directory' })
    expect(safekeeperStatefulSet({ ...baseOptions, volumeCreate: true }).spec.template.spec.volumes[0].hostPath.type).toBe('')
    expect(control).toContain("const SK_VOLUME_CREATE = process.env['SAFEKEEPER_VOLUME_TYPE'] === 'DirectoryOrCreate'")
    expect(control).toContain('volumeRoot: SK_VOLUME, volumeCreate: SK_VOLUME_CREATE,')
  })

  test('a safekeeper mounts its volume as itself, never through subPath (Talos\'s kubelet resolves one in its own view), and keeps its data in its own folder sk-<id>, as before', () => {
    const t = safekeeperStatefulSet(baseOptions).spec.template.spec
    for (const c of [...t.initContainers, ...t.containers]) for (const m of c.volumeMounts) expect(m.subPath).toBeUndefined()
    expect(t.containers[0].volumeMounts[0]).toEqual({ name: 'volume', mountPath: '/vol' })
    const script = t.containers[0].args[0] as string
    expect(script).toContain('d=/vol/sk-$id')
    expect(script).toContain('exec /usr/local/bin/safekeeper -D $d --id')
    expect(script).not.toContain('/data')
    expect(t.initContainers[0].command[2]).toContain(`mkdir -p /vol/sk-${baseOptions.id}; chown 1000:1000 /vol/sk-${baseOptions.id}`)
    // its folder wiped under it: the liveness probe fails; the restart makes the folder again and finds it empty, which for a registered member
    // the may-start-empty guard refuses (lost, replaced), never a quiet rejoin
    expect(t.initContainers[0].command[2]).toStartWith('set -eu; chown 1000:1000 /vol; ')
    // its identity, not only its folder: a wipe of the contents with the folder kept is seen too; the first look after the start's own wait
    expect(t.containers[0].livenessProbe).toEqual({ exec: { command: ['/bin/sh', '-c', `test -s /vol/sk-${baseOptions.id}/identity`] }, initialDelaySeconds: 90, periodSeconds: 10, timeoutSeconds: 5, failureThreshold: 3 })
    expect(script).toContain('d=/vol/sk-$id\nmkdir -p $d\nif [ ! -s $d/identity ]; then')
    expect(script.indexOf('mkdir -p $d')).toBeLessThan(script.indexOf('/sk/$id/may-start-empty'))
  })

  test('nothing the program writes turns on rel_size_v2 (under which a drop_dbdir change of the storage reaches Postgres 18)', () => {
    for (const f of ['control.ts', 'storage-spec.ts', 'storage/roll.ts', 'storage/state.ts', 'storage/api.ts']) expect(src(f), f).not.toMatch(/rel_size|default_tenant_conf/)
  })
})

describe('the safekeepers: closing, seeding, moving, deciding', () => {
  test('a timeline nobody answers about is answered for only when every member listed for it is lost for good', () => {
    // every member of its set lost (a lost-disk mark, or decommissioned already): answered for, the decommission goes through
    expect(lostEverywhere([7, 8, 9], new Set([7, 8, 9]))).toBe(true)
    // one lost, one merely unreachable (no mark: a reboot, a blip): unknown
    expect(lostEverywhere([7, 8], new Set([7]))).toBe(false)
    // one alive (no mark; were it holding the timeline it would have answered): unknown
    expect(lostEverywhere([7, 8, 9], new Set([7, 8]))).toBe(false)
    // a member it is moving to counts as listed; no member listed (the controller did not say) is unknown
    expect(lostEverywhere([7, 8, 9, 10], new Set([7, 8, 9]))).toBe(false)
    expect(lostEverywhere([], new Set([7]))).toBe(false)
    // in the closing count: only after nobody answered, from the controller's own set (and its move), with the lost marks and the decommissioned
    const reads = control.slice(control.indexOf('async function readTimelines'), control.indexOf('async function replacementOpen'))
    expect(reads).toContain('if (!read.answered && answerLostEverywhere) {')
    // only a person's decommission accepts a total loss: the replacement job's own close never does (every member looking lost may be a
    // data disk not mounted), so it keeps the lost members and says so
    expect(control).toContain('const findTodo = () => readTimelines(lost, lostHost, job.newId!)\n')
    expect(control).toContain('await readTimelines(id, sk.host, Math.max(...(list.json as any[]).map((x) => x.id as number)), true)')
    expect(reads).toContain('/debug/v1/tenant/${t}/timeline/${tl}/locate')
    expect(reads).toContain('if (lostEverywhere(members, lostIds)) read = { answered: true, onLost: false }')
    expect(reads).toContain("labelSelector=peat-sk-lost")
  })
  test('a lost safekeeper is closed only when every timeline was answered about and none lists it', () => {
    const on = { answered: true, onLost: true } as const, off = { answered: true, onLost: false } as const, unknown = { answered: false } as const
    expect(closeDecision([])).toEqual({ todo: 0, unknown: 0, close: true })
    expect(closeDecision([off, off])).toEqual({ todo: 0, unknown: 0, close: true })
    expect(closeDecision([off, on])).toEqual({ todo: 1, unknown: 0, close: false })
    expect(closeDecision([off, unknown])).toEqual({ todo: 0, unknown: 1, close: false }) // the other members blipped at the closing count
    expect(closeDecision([unknown, unknown, unknown])).toEqual({ todo: 0, unknown: 3, close: false }) // a total loss: nobody knows any timeline
    expect(closeDecision([on, unknown]).close).toBe(false)
    expect(control).toContain('closeDecision(reads)')
    expect(control).toContain('if (fin.close) {')
  })

  test('the locations of a shard with an error, as the controller and each pageserver give them; none when a pageserver did not answer', () => {
    const debug = [
      { tenant_shard_id: 't1', generation: 3, intent: { attached: 1, secondary: [2] }, observed: { locations: { 1: { conf: { mode: 'AttachedSingle', generation: 3 } }, 2: { conf: { mode: 'Secondary', generation: null } }, 3: null } } },
      { tenant_shard_id: 't2', generation: 1, intent: { attached: 2, secondary: [] }, observed: { locations: {} } },
    ]
    const held = { 1: new Map([['t1', { attached: true, generation: 3 }]]), 2: new Map([['t1', { attached: false, generation: null }]]) }
    expect(shardLocations(debug, held, new Set(['t1']))).toEqual(new Map([['t1', {
      generation: 3,
      intent: { attached: 1, secondary: [2] },
      observed: { 1: { mode: 'AttachedSingle', generation: 3 }, 2: { mode: 'Secondary', generation: null }, 3: undefined },
      held: { 1: { attached: true, generation: 3 }, 2: { attached: false, generation: null } },
    }]]))
    expect(shardLocations(debug, { ...held, 3: undefined }, new Set(['t1'])).size).toBe(0)
  })

  test('a seeding that stopped half way is finished, and a member replaced away is not made again', () => {
    const group = [{ id: 7, node: 'machine-1' }, { id: 8, node: 'machine-2' }, { id: 9, node: 'machine-3' }]
    expect(formatSeedGroup(group)).toBe('7=machine-1,8=machine-2,9=machine-3')
    expect(parseSeedGroup('7=machine-1,8=machine-2,9=machine-3')).toEqual(group)
    expect(parseSeedGroup(undefined)).toEqual([])
    expect(parseSeedGroup('x=machine-1,8=,9=machine-3')).toEqual([{ id: 9, node: 'machine-3' }])
    expect(seedCompletion(group, ['safekeeper-w7'], [])).toEqual([group[1]!, group[2]!])
    expect(seedCompletion(group, ['safekeeper-w7', 'safekeeper-w8', 'safekeeper-w9'], [])).toEqual([])
    expect(seedCompletion(group, ['safekeeper-w8', 'safekeeper-w9', 'safekeeper-w10'], [7, 8, 9, 10])).toEqual([]) // 7 was replaced by 10: listed, not made again
    expect(control).toContain('SEED_GROUP_ANNOTATION')
  })

  test('a member that is not decommissioned occupies its machine, whatever its state', () => {
    const pods = [{ nodeId: '1', node: 'machine-1' }, { nodeId: '2', node: 'machine-2' }, { nodeId: '3', node: 'machine-3' }]
    const machines = ['machine-1', 'machine-2', 'machine-3'].map((name) => ({ name, dead: false }))
    expect(replacementMachines(1, [{ id: 1, scheduling_policy: 'Active' }, { id: 2, scheduling_policy: 'Pause' }, { id: 3, scheduling_policy: 'Active' }], pods, machines)).toEqual(['machine-1'])
    expect(replacementMachines(1, [{ id: 1, scheduling_policy: 'Active' }, { id: 2, scheduling_policy: 'Decomissioned' }, { id: 3, scheduling_policy: 'Active' }], pods, machines).sort()).toEqual(['machine-1', 'machine-2'])
  })

  test('a total loss is shown by evidence, not by silence', () => {
    const lost = { lostMark: true, podRunningOnReadyNode: false, silentAfterWait: false }
    const silentAndUp = { lostMark: false, podRunningOnReadyNode: true, silentAfterWait: true }
    expect(totalLossEvidence([])).toBe(true)
    expect(totalLossEvidence([lost, lost])).toBe(true)
    expect(totalLossEvidence([silentAndUp, silentAndUp])).toBe(true)
    expect(totalLossEvidence([lost, silentAndUp])).toBe(true)
    expect(totalLossEvidence([silentAndUp, { ...silentAndUp, silentAfterWait: false }])).toBe(false) // one answered after the wait: a blip
    expect(totalLossEvidence([silentAndUp, { lostMark: false, podRunningOnReadyNode: false, silentAfterWait: true }])).toBe(false) // a pod not Running on a Ready node: a reboot
    expect(totalLossEvidence([{ lostMark: false, podRunningOnReadyNode: false, silentAfterWait: false }])).toBe(false)
  })

  test('an operator may decommission a silent member only when it is shown lost, its count closes, or forced', () => {
    const closed = { todo: 0, unknown: 0, close: true }, listing = { todo: 3, unknown: 0, close: false }, unknown = { todo: 0, unknown: 2, close: false }
    const base = { answers: false, othersAnswering: 2, lossEvidenced: false, count: null as any, force: false }
    expect(decommissionDecision({ ...base, answers: true, force: true })).toMatchObject({ allow: false })
    expect(decommissionDecision({ ...base, othersAnswering: 0, lossEvidenced: true })).toMatchObject({ allow: true })
    const silence = decommissionDecision({ ...base, othersAnswering: 0, lossEvidenced: false })
    expect(silence.allow).toBe(false)
    expect(silence.why).toContain('not shown lost')
    expect(decommissionDecision({ ...base, othersAnswering: 0, lossEvidenced: false, force: true }).allow).toBe(true)
    expect(decommissionDecision({ ...base, count: closed })).toMatchObject({ allow: true })
    const refused = decommissionDecision({ ...base, count: listing })
    expect(refused.allow).toBe(false)
    expect(refused.why).toContain('3 timelines still list it')
    expect(decommissionDecision({ ...base, count: unknown }).allow).toBe(false)
    expect(decommissionDecision({ ...base, count: null }).allow).toBe(false)
    const forced = decommissionDecision({ ...base, count: listing, force: true })
    expect(forced.allow).toBe(true)
    expect(forced.why).toContain('3 timelines still list it')
    const handler = control.slice(control.indexOf('const decom = path.match'), control.indexOf("path === '/hook-log'"))
    expect(handler).toContain('decommissionDecision(')
    expect(handler).toContain('totalLossEvidence(')
    expect(handler).toContain("searchParams.get('force') === '1'")
  })

  test('a member may be moved onto a keeper machine only when the set stays whole, and only one member is ever off the platform\'s machines', () => {
    const keeper = ['outside-1'], platform = ['machine-1', 'machine-2', 'machine-3']
    const at = (id: number, machine: string | null, over: Partial<MemberAt> = {}): MemberAt => ({ id, scheduling_policy: 'Active', machine, answers: true, ...over })
    const set = [at(7, 'machine-1'), at(8, 'machine-2'), at(9, 'machine-3')]
    const base = { id: 9, target: 'outside-1', members: set, keeperMachines: keeper, platformMachines: platform, replacementOpen: null as string | null }
    expect(moveDecision(base)).toMatchObject({ allow: true })
    expect(moveDecision({ ...base, members: [at(7, 'machine-1'), at(8, 'machine-2'), at(9, 'machine-3', { answers: false })] }).why).toContain('does not answer')
    expect(moveDecision({ ...base, members: [at(7, 'machine-1', { answers: false }), at(8, 'machine-2'), at(9, 'machine-3')] }).why).toContain('below three healthy')
    expect(moveDecision({ ...base, members: [at(8, 'machine-2'), at(9, 'machine-3')] }).why).toContain('fewer than two other members')
    expect(moveDecision({ ...base, members: [at(7, 'machine-1', { scheduling_policy: 'Pause' }), at(8, 'machine-2'), at(9, 'machine-3')] }).allow).toBe(true) // a paused other member still answers
    expect(moveDecision({ ...base, members: [at(7, 'machine-1'), at(8, 'machine-2'), at(9, 'machine-3', { scheduling_policy: 'Pause' })] }).why).toContain('not Active')
    expect(moveDecision({ ...base, replacementOpen: 'the replacement of safekeeper 5 is not closed' }).why).toContain('a replacement is open')
    expect(moveDecision({ ...base, target: 'other-node' }).why).toContain('neither a keeper machine nor a platform machine')
    expect(moveDecision({ ...base, target: 'machine-3' }).why).toContain('already on')
    expect(moveDecision({ ...base, target: 'machine-1' }).why).toContain('no two members on one machine')
    // two on the platform's machines, one off: with a member already on the keeper machine a second may not go there
    const withKeeper = [at(7, 'machine-1'), at(8, 'outside-1'), at(9, 'machine-3')]
    expect(moveDecision({ ...base, members: withKeeper, id: 9 }).why).toContain('no two members on one machine')
    expect(moveDecision({ ...base, members: withKeeper, id: 7, target: 'machine-2' })).toMatchObject({ allow: true })
    expect(membersOffBox(set, keeper)).toBe(0)
    expect(membersOffBox(withKeeper, keeper)).toBe(1)
    expect(membersOffBox([at(7, 'machine-1'), at(8, 'outside-1', { scheduling_policy: 'Decomissioned' })], keeper)).toBe(0)
    expect(membersOffBox([at(7, null)], keeper)).toBe(0)
  })

  test('a safekeeper on a keeper machine carries the keeper\'s placement (its label, that machine\'s taints tolerated), every other the platform\'s', () => {
    const platform = placement(DEFAULT_NAMES)
    const keeperP = { nodeSelector: { [KEEPER.key]: KEEPER.value }, tolerations: [{ key: 'example.com/workstation', operator: 'Equal', value: 'true', effect: 'NoSchedule' }] }
    expect(placementFor('outside-1', { keeperMachines: ['outside-1'], platform, keeper: keeperP })).toBe(keeperP)
    expect(placementFor('machine-1', { keeperMachines: ['outside-1'], platform, keeper: keeperP })).toBe(platform)
    expect(placementFor('outside-1', { keeperMachines: [], platform, keeper: keeperP })).toBe(platform) // no keeper machines configured: nothing goes there
    const sts = safekeeperStatefulSet({ ...baseOptions, id: 10, node: 'outside-1', placement: keeperP })
    const pod = sts.spec.template.spec
    expect(pod.nodeSelector).toEqual({ [KEEPER.key]: KEEPER.value, 'kubernetes.io/hostname': 'outside-1' })
    expect(pod.tolerations).toEqual([{ key: 'example.com/workstation', operator: 'Equal', value: 'true', effect: 'NoSchedule' }])
    expect(pod.volumes.find((v: any) => v.hostPath)?.hostPath).toEqual({ path: '/var/lib/peat/safekeeper', type: 'Directory' })
  })

  test('only one replacement or move runs in the whole cluster, and only on the acting pod', () => {
    const marks: ReplacementMark[] = [{ id: 9, at: 100, moveTo: 'outside-1' }]
    expect(otherReplacementOpen([], 7)).toBeNull()
    expect(otherReplacementOpen(marks, 7)).toBe(9) // another member's move is open: a fresh replacement of 7 waits (two jobs would share one new id)
    expect(otherReplacementOpen(marks, 9)).toBeNull() // its own mark is its own rerun, not a second job
    // two marks left open together: the OLDEST reruns, the other waits for it, so neither blocks the other for ever
    const two: ReplacementMark[] = [{ id: 9, at: 100, moveTo: '' }, { id: 7, at: 200, moveTo: '' }]
    expect(otherReplacementOpen(two, 9)).toBeNull()
    expect(otherReplacementOpen(two, 7)).toBe(9)
    expect(otherReplacementOpen([{ id: 9, at: 100, moveTo: '' }, { id: 7, at: 100, moveTo: '' }], 7)).toBeNull() // the same instant: the lower id goes
    expect(otherReplacementOpen([{ id: 9, at: 100, moveTo: '' }, { id: 7, at: 100, moveTo: '' }], 9)).toBe(7)
    const watch = control.slice(control.indexOf('async function skWatch'), control.indexOf('const gauges: Record'))
    expect(watch.match(/otherReplacementOpen\(replacementMarks,/g)?.length).toBe(3)
    expect(watch).toContain('replacementMarks.find((m) => m.id === sk.id)?.moveTo || undefined')
    expect(control).toContain('...(moveTo ? { moveTo } : {})')
    expect(control).toContain('moveTo ??= (memo.json.data.moveTo as string | undefined) || undefined')
    const handler = control.slice(control.indexOf('const mv = path.match'), control.indexOf('const decom = path.match'))
    expect(handler.indexOf('if (!isLeader)')).toBeGreaterThan(0)
    expect(handler.indexOf('if (!isLeader)')).toBeLessThan(handler.indexOf('moveDecision('))
    expect(handler).toContain('peat-control-leader')
    expect(control).toContain("if (!isLeader) throw new Error('this pod is no longer the acting control layer")
  })

  test('moving a member is an operator route: no caller class lists it, and it needs a Ready keeper node with the directory made and the image already on it', () => {
    const routes = control.slice(control.indexOf('const CALLER_ROUTES'), control.indexOf('let callerCache'))
    expect(routes).not.toContain('/move')
    const handler = control.slice(control.indexOf('const mv = path.match'), control.indexOf('const decom = path.match'))
    expect(handler).toContain('moveDecision(')
    expect(handler).toContain('await keeperProblem(to)')
    const check = control.slice(control.indexOf('async function keeperProblem'), control.indexOf('async function keepersReady'))
    expect(check).toContain('${KEEPER_LABEL}=${KEEPER.value}')
    expect(check).toContain('keeperNodeProblem(')
    expect(check).toContain('/jobs/keeper-prep')
    expect(check).toContain('status?.succeeded')
    expect(check).toContain('status?.images')
    expect(control).toContain('sk_members_off_box')
  })

  test('a replacement whose new member never started and moved nothing is undone, not left open', () => {
    const catchBlock = control.slice(control.indexOf("if (job.phase === 'creating' && job.newId !== undefined && !madeEarlier)"))
    expect(catchBlock.slice(0, 700)).toContain('statefulsets/safekeeper-w${job.newId}')
    expect(catchBlock.slice(0, 700)).toContain('configmaps/sk-replacement-${lost}')
    expect(control).toContain('madeEarlier = true') // one an earlier job made is not this job's to undo
  })

  test('a replacement whose member answers again is finished, and it is marked open from the job\'s first second', () => {
    const base = { answers: true, hasReplacementMark: true, jobRunning: false, sinceLastTryS: 400, everyS: 300 }
    expect(reopenDecision(base)).toBe('rerun')
    expect(reopenDecision({ ...base, answers: false })).toBe('nothing')
    expect(reopenDecision({ ...base, hasReplacementMark: false })).toBe('nothing')
    expect(reopenDecision({ ...base, jobRunning: true })).toBe('nothing')
    expect(reopenDecision({ ...base, sinceLastTryS: 100 })).toBe('nothing') // not every tick
    expect(control).toContain('mark the replacement open')
    expect(control.indexOf("'mark the replacement open'")).toBeLessThan(control.indexOf("'create safekeeper statefulset'"))
    expect(control).toContain('sk_replacement_open_seconds')
    expect(control).toContain('reopenDecision(')
  })

  test('a member is registered in the zone of its machine, and no new database is made while a replacement is open', () => {
    expect(control).toContain("availability_zone_id: SPREAD === 'none' ? `sk-${id}` : (zone ?? `sk-${id}`)")
    expect(control).not.toContain('availability_zone_id: `sk-${id}` }')
    expect(control).toContain('await ensureSkZones(')
    // every database is made by the acting layer's loop over the Database objects, which waits while a replacement is open (databaseAction decides)
    const loop = control.slice(control.indexOf('async function reconcileDatabases'), control.indexOf('async function reconcileDatabases') + 600)
    expect(loop).toContain('const busy = await replacementOpen()')
    expect(loop).toContain('const busy = await replacementOpen()')
    expect(control).toContain('databaseAction(o, all, { replacementOpen: busy, records })')
    expect(control.match(/'peat-sk-open': 'true'/g)?.length).toBe(2) // the lost mark and the replacement mark
  })

  test('decommissioning a safekeeper is an operator route: no caller class lists it, and a member that answers is refused', () => {
    const routes = control.slice(control.indexOf('const CALLER_ROUTES'), control.indexOf('let callerCache'))
    expect(routes).not.toContain('decommission')
    const handler = control.slice(control.indexOf('const decom = path.match'), control.indexOf("path === '/hook-log'"))
    expect(handler).toContain('decommissionDecision(')
    expect(handler).toContain("'Decomissioned'")
  })

  test('a safekeeper the control layer replaces is made from the shared definition, with the install\'s Secret names and volume', () => {
    expect(control).toContain("import { SAFEKEEPER_RESOURCES, SEED_GROUP_ANNOTATION, closeDecision, decommissionDecision, lostEverywhere, reopenDecision, totalLossEvidence, formatSeedGroup, parseSeedGroup, membersOffBox, moveDecision, otherReplacementOpen, placementFor, replaceDecision, replacementMachines, replacementPool, safekeeperStatefulSet, seedCompletion, seedDecision, seedGroup, seedIds, neverWritten, isPullTimelineFailure, keeperNodeProblem, keeperReplacementVerdict, certInfo, certNeedsReload, tcpUp, memoryGauges, placementVerdict, startVerdict, within, type MemberAt, type ReplacementMark, type SkRecord, type TimelineRead } from './storage-spec'")
    expect(control).toContain('secrets: SK_SECRETS,')
    expect(control).toContain('volumeRoot: SK_VOLUME')
  })
})

describe('the safekeepers belong to the control layer: when it replaces one, onto which machine, and when it makes the first three', () => {
  const live = [{ id: 3, scheduling_policy: 'Active' }, { id: 5, scheduling_policy: 'Active' }, { id: 6, scheduling_policy: 'Active' }]
  const pods = [{ nodeId: '3', node: 'machine-3' }, { nodeId: '5', node: 'machine-1' }, { nodeId: '6', node: 'machine-2' }]
  const up = [{ name: 'machine-1', dead: false }, { name: 'machine-2', dead: false }, { name: 'machine-3', dead: false }]
  test('a replacement goes only to a machine that holds no other Active safekeeper: with three machines and three members the only one is the lost member\'s own', () => {
    expect(replacementMachines(5, live, pods, up)).toEqual(['machine-1'])
    expect(replacementMachines(3, live, pods, up)).toEqual(['machine-3'])
    expect(replacementMachines(6, live, pods, up)).toEqual(['machine-2'])
  })
  test('with the lost member\'s machine gone there is nowhere to go (waiting, not a second member on a machine); a free machine is used when there is one; a decommissioned member does not hold its machine', () => {
    expect(replacementMachines(5, live, pods, [{ name: 'machine-1', dead: true }, { name: 'machine-2', dead: false }, { name: 'machine-3', dead: false }])).toEqual([])
    expect(replacementMachines(5, live, pods, [...up, { name: 'machine-4', dead: false }])).toEqual(['machine-1', 'machine-4'])
    expect(replacementMachines(5, [{ id: 3, scheduling_policy: 'Active' }, { id: 5, scheduling_policy: 'Active' }, { id: 6, scheduling_policy: 'Decomissioned' }], pods, up)).toEqual(['machine-1', 'machine-2'])
    expect(replacementMachines(5, live, [{ nodeId: '3', node: 'machine-3' }, { nodeId: '5', node: 'machine-1' }, { nodeId: '6', node: '' }], up)).toEqual(['machine-1', 'machine-2'])
  })
  test('a machine gone for three minutes gets its own safekeeper back: nothing is replaced inside the bound (30 minutes), one past it is replaced only with a machine to go to, else it waits', () => {
    expect(replaceDecision(175, 1800, ['machine-1'])).toBe('nothing')
    expect(replaceDecision(1800, 1800, ['machine-1'])).toBe('nothing')
    expect(replaceDecision(1801, 1800, ['machine-1'])).toBe('replace')
    expect(replaceDecision(1801, 1800, [])).toBe('wait')
    expect(control).toContain("process.env['SK_GONE_S'] ?? '1800'")
    expect(control).toContain('const what = replaceDecision(gone, SK_GONE_S, where)')
    expect(control).toContain('peat_control_')
    expect(control).toContain('sk_replacement_blocked')
  })
  test('a lost disk (the empty-disk guard\'s mark) is replaced at once, with no bound, and the replacement of an absent machine is not hurried by it', () => {
    expect(control).toContain('replaceSafekeeper(id, list.json as any[], true)')
    const i = control.indexOf('sk-replace: safekeeper ${id} refused to start on an empty disk')
    expect(control.slice(i - 200, i + 200)).not.toContain('SK_GONE_S')
  })
  test('the first three are made only when no safekeeper StatefulSet exists and the controller lists none; never recreated when one exists; refused (a person is told) when the controller lists some and none exists', () => {
    expect(seedDecision([], [])).toBe('seed')
    expect(seedDecision(['safekeeper-w3', 'safekeeper-w5'], [{ id: 3, scheduling_policy: 'Active' }])).toBe('exists')
    expect(seedDecision(['safekeeper-w5'], [])).toBe('exists')
    expect(seedDecision([], [{ id: 3, scheduling_policy: 'Active' }, { id: 5, scheduling_policy: 'Decomissioned' }])).toBe('refuse')
    expect(seedDecision([], [3, 5, 6].map((id) => ({ id, scheduling_policy: 'Decomissioned' })))).toBe('seed') // a total loss: every one replaced away
    expect(seedIds([3, 5, 6], [])).toEqual([3, 5, 6])
    expect(seedIds([3, 5, 6], [3, 5, 6, 9].map((id) => ({ id, scheduling_policy: 'Decomissioned' })))).toEqual([10, 11, 12])
    expect(seedDecision(['pageserver-w1', 'storage-broker'], [])).toBe('seed')
    expect(control).toContain("process.env['SAFEKEEPER_SEED']")
    expect(control).toContain("void loop('seed the safekeepers', 5000, seedSafekeepers)")
    expect(control).toContain('registerWhenReady')
  })
})

describe('the safekeeper\'s pod', () => {
  test('Neon\'s own partial-backup and eviction settings, offload with local deletion, a 20 minute request timeout, the empty-disk guard and a data directory of its own in the machine\'s volume', () => {
    const script = safekeeperScript(baseOptions)
    expect(script).toContain('--enable-offload --delete-offloaded-wal --wal-backup-parallel-jobs 2')
    expect(script).toContain("timeout='20m'")
    expect(script).not.toMatch(/partial-backup-timeout|eviction-min-resident/)
    expect(script).toContain('/sk/$id/may-start-empty')
    const pod = safekeeperStatefulSet(baseOptions).spec.template.spec
    expect(pod.initContainers[0].securityContext.runAsUser).toBe(0)
    expect(pod.containers[0].volumeMounts[0]).toEqual({ name: 'volume', mountPath: '/vol' })
    expect(script).toContain('d=/vol/sk-$id')
    expect(pod.nodeSelector).toEqual({ [LABEL.key]: LABEL.value, 'kubernetes.io/hostname': 'machine-3' })
  })
})

describe('the keeper\'s member after a loss (2026-11-20: a keeper machine\'s member came back on a platform machine and nothing was off the platform\'s machines)', () => {
  const live = [{ id: 7, scheduling_policy: 'Active' }, { id: 8, scheduling_policy: 'Active' }, { id: 10, scheduling_policy: 'Active' }]
  const pods = [{ nodeId: '7', node: 'machine-1' }, { nodeId: '8', node: 'machine-2' }, { nodeId: '10', node: 'outside-1' }]
  const platform = [{ name: 'machine-1', dead: false }, { name: 'machine-2', dead: false }, { name: 'machine-3', dead: false }]
  const keepers = ['outside-1']
  test('the keeper\'s own member goes back to the keeper machine when it is ready, before a free platform machine', () => {
    expect(replacementMachines(10, live, pods, replacementPool(platform, 'outside-1', keepers, ['outside-1']))).toEqual(['outside-1', 'machine-3'])
  })
  test('a keeper machine that is not ready is a dead machine: the replacement goes to a free platform machine, as before', () => {
    expect(replacementMachines(10, live, pods, replacementPool(platform, 'outside-1', keepers, []))).toEqual(['machine-3'])
  })
  test('a member of the platform\'s machines is never replaced onto a keeper machine, whatever is ready; and a keeper machine holding another member is busy', () => {
    expect(replacementPool(platform, 'machine-1', keepers, ['outside-1'])).toEqual(platform)
    expect(replacementMachines(7, live, pods, replacementPool(platform, 'machine-1', keepers, ['outside-1']))).toEqual(['machine-1', 'machine-3'])
    expect(replacementPool(platform, '', keepers, ['outside-1'])).toEqual(platform)
    expect(replacementPool(platform, 'outside-1', [], ['outside-1'])).toEqual(platform)
  })
  test('the keeper machine is no candidate while another member sits on it', () => {
    const moved = [{ nodeId: '7', node: 'outside-1' }, { nodeId: '8', node: 'machine-2' }, { nodeId: '10', node: 'outside-1' }]
    expect(replacementMachines(10, live, moved, replacementPool(platform, 'outside-1', keepers, ['outside-1']))).toEqual(['machine-1', 'machine-3'])
  })
})

describe('a seeding after a total loss puts one member on the keeper machine', () => {
  const configured = [{ id: 1, node: 'machine-1' }, { id: 2, node: 'machine-2' }, { id: 3, node: 'machine-3' }]
  test('with the keeper machine ready the last configured member (the controller\'s machine) goes to it: two on the platform\'s machines, one off', () => {
    expect(seedGroup(configured, ['outside-1'], ['outside-1'])).toEqual([{ id: 1, node: 'machine-1' }, { id: 2, node: 'machine-2' }, { id: 3, node: 'outside-1' }])
  })
  test('with none ready, none configured or the keeper already in the group, the group is the configured one', () => {
    expect(seedGroup(configured, ['outside-1'], [])).toEqual(configured)
    expect(seedGroup(configured, [], ['outside-1'])).toEqual(configured)
    expect(seedGroup([{ id: 1, node: 'machine-1' }, { id: 2, node: 'outside-1' }], ['outside-1'], ['outside-1'])).toEqual([{ id: 1, node: 'machine-1' }, { id: 2, node: 'outside-1' }])
    expect(seedGroup([{ id: 1, node: 'machine-1' }], ['outside-1'], ['outside-1'])).toEqual([{ id: 1, node: 'machine-1' }])
  })
  test('the ids of the seeded members are kept in order and the input is not changed', () => {
    const before = JSON.stringify(configured)
    expect(seedGroup(configured, ['outside-1'], ['outside-1']).map((x) => x.id)).toEqual([1, 2, 3])
    expect(JSON.stringify(configured)).toBe(before)
  })
})

describe('a timeline whose database has never started does not fail the move', () => {
  // the status of a written timeline and of a recovered one never started, as read 2026-11-20
  const written = { flush_lsn: '0/839F220', timeline_start_lsn: '0/839D618', commit_lsn: '0/839F220' }
  const fresh = { flush_lsn: '0/10609E18', timeline_start_lsn: '0/10609E18', commit_lsn: '0/10609E18' }
  test('never written is a flush position still at the start; a written timeline, an unread one and a status without the fields are not', () => {
    expect(neverWritten([fresh])).toBe(true)
    expect(neverWritten([written])).toBe(false)
    expect(neverWritten([null, undefined])).toBe(false); expect(neverWritten([])).toBe(false)
    expect(neverWritten([{ flush_lsn: '0/1' }])).toBe(false); expect(neverWritten([{}])).toBe(false)
  })
  test('the highest flush position over every member that answers decides: a member that has not caught up does not make a written timeline look unwritten', () => {
    const behind = { flush_lsn: '0/839D618', timeline_start_lsn: '0/839D618' } // the third member of a set made on a quorum of two
    expect(neverWritten([behind, written])).toBe(false)
    expect(neverWritten([written, behind])).toBe(false)
    expect(neverWritten([behind, behind])).toBe(true)
    expect(neverWritten([fresh, null, fresh])).toBe(true)
    expect(neverWritten([fresh, { flush_lsn: 'garbage', timeline_start_lsn: '0/1' }])).toBe(false) // a status that cannot be read is not evidence
  })
  test('the break a live run showed is recognised, other failures are not', () => {
    expect(isPullTimelineFailure('{"msg":"pull_timeline to 15 failed: safekeeper API: error decoding response body: error reading a body from connection: unexpected EOF during chunk size line"}')).toBe(true)
    expect(isPullTimelineFailure('{"msg":"timeout waiting for the storage controller"}')).toBe(false)
  })
  test('the migration loop starts the database once and tries again, only on that break and only on the first attempt, and the start is one at a time', () => {
    const loop = control.slice(control.indexOf('for (let attempt = 0; attempt < 3 && !okd; attempt++) {'), control.indexOf('job.inFlight--'))
    expect(loop).toContain('attempt === 0 && isPullTimelineFailure(JSON.stringify(r.json)) && (await startOnceIfNeverWritten(x.tenant, x.timeline, x.set))')
    const fn = control.slice(control.indexOf('async function startOnceIfNeverWritten'), control.indexOf('async function replaceSafekeeper'))
    expect(fn).toContain('neverWritten(statuses)')
    expect(fn).toContain('finally {')
    expect(fn).toContain('if (started) await stopCompute(d.name)')
    expect(fn).toContain('firstStartChain.then(')
    expect(fn).toContain('await stopCompute(d.name)')
    expect(fn).toContain('dep.json.spec.replicas === 1) return false')
  })
  test('the control layer asks the keeper machine\'s state once, for the move, the replacement and the seeding', () => {
    expect(control.match(/async function keeperProblem\(/g)?.length).toBe(1)
    expect(control).toContain('const why = await keeperProblem(to)')
    expect(control).toContain('replacementPool([...nodeStates.keys()]')
    expect(control).toContain('seedGroup(SAFEKEEPER_SEED, KEEPER_MACHINES, await keepersReady())')
  })
})

describe('a keeper machine that is Ready and cannot take a member is not chosen for ever', () => {
  const tolerations = [{ key: 'example.com/workstation', operator: 'Equal', value: 'true', effect: 'NoSchedule' }]
  const ready = [{ type: 'Ready', status: 'True' }, { type: 'DiskPressure', status: 'False' }, { type: 'MemoryPressure', status: 'False' }, { type: 'PIDPressure', status: 'False' }]
  const node = (o: { labels?: Record<string, string>; conditions?: { type: string; status: string }[]; unschedulable?: boolean; taints?: any[] } = {}) => ({
    metadata: { name: 'outside-1', labels: o.labels ?? { [KEEPER.key]: KEEPER.value } },
    spec: { unschedulable: o.unschedulable, taints: o.taints ?? [{ key: 'example.com/workstation', value: 'true', effect: 'NoSchedule' }] },
    status: { conditions: o.conditions ?? ready },
  })
  test('a keeper machine in good order passes: labelled, Ready, no pressure, and only the taint the keeper\'s pod tolerates', () => {
    expect(keeperNodeProblem(node(), KEEPER, tolerations)).toBeNull()
  })
  test('not labelled, not Ready or unreadable is refused with the move route\'s words', () => {
    expect(keeperNodeProblem(node({ labels: {} }), KEEPER, tolerations)).toBe('outside-1 is not a Ready node carrying peat.zabaca.com/keeper=true')
    expect(keeperNodeProblem(node({ conditions: [{ type: 'Ready', status: 'False' }] }), KEEPER, tolerations)).toContain('is not a Ready node')
    expect(keeperNodeProblem(node({ conditions: [] }), KEEPER, tolerations)).toContain('is not a Ready node')
    expect(keeperNodeProblem(null, KEEPER, tolerations)).toContain('is not a Ready node')
  })
  test('a cordoned node (a drain before an update) is refused although it is Ready', () => {
    expect(keeperNodeProblem(node({ unschedulable: true }), KEEPER, tolerations)).toContain('cordoned')
  })
  test('a NoSchedule or NoExecute taint the keeper\'s pod does not tolerate is refused, a tolerated one and a PreferNoSchedule one are not', () => {
    const t = (key: string, effect: string, value = '') => ({ key, value, effect })
    expect(keeperNodeProblem(node({ taints: [t('example.com/workstation', 'NoSchedule', 'true'), t('node.kubernetes.io/disk-pressure', 'NoSchedule')] }), KEEPER, tolerations)).toContain('node.kubernetes.io/disk-pressure')
    expect(keeperNodeProblem(node({ taints: [t('example.com/maintenance', 'NoExecute', 'x')] }), KEEPER, tolerations)).toContain('does not tolerate')
    expect(keeperNodeProblem(node({ taints: [t('example.com/workstation', 'NoExecute', 'true')] }), KEEPER, tolerations)).toContain('does not tolerate') // the toleration names the effect NoSchedule
    expect(keeperNodeProblem(node({ taints: [t('example.com/workstation', 'NoSchedule', 'other')] }), KEEPER, tolerations)).toContain('does not tolerate')
    expect(keeperNodeProblem(node({ taints: [t('example.com/soft', 'PreferNoSchedule')] }), KEEPER, tolerations)).toBeNull()
    expect(keeperNodeProblem(node({ taints: [t('anything', 'NoSchedule', 'v')] }), KEEPER, [{ key: 'anything', operator: 'Exists' }])).toBeNull()
  })
  test('Disk, Memory and PID pressure each refuse a node that is Ready', () => {
    for (const p of ['DiskPressure', 'MemoryPressure', 'PIDPressure'])
      expect(keeperNodeProblem(node({ conditions: [{ type: 'Ready', status: 'True' }, { type: p, status: 'True' }] }), KEEPER, tolerations)).toBe(`outside-1 reports ${p}`)
  })
  test('the keeper machine is tried once per loss: once marked, the pool is the platform\'s machines whatever is ready', () => {
    const platform = [{ name: 'machine-1', dead: false }, { name: 'machine-2', dead: false }, { name: 'machine-3', dead: false }]
    const live = [{ id: 7, scheduling_policy: 'Active' }, { id: 8, scheduling_policy: 'Active' }, { id: 10, scheduling_policy: 'Active' }]
    const pods = [{ nodeId: '7', node: 'machine-1' }, { nodeId: '8', node: 'machine-2' }, { nodeId: '10', node: 'outside-1' }]
    expect(replacementMachines(10, live, pods, replacementPool(platform, 'outside-1', ['outside-1'], ['outside-1'], false))).toEqual(['outside-1', 'machine-3'])
    expect(replacementMachines(10, live, pods, replacementPool(platform, 'outside-1', ['outside-1'], ['outside-1'], true))).toEqual(['machine-3'])
    expect(replacementPool(platform, 'outside-1', ['outside-1'], ['outside-1'], true)).toEqual(platform)
  })
  test('a replacement on the keeper machine whose migrations failed with nothing moved is abandoned; progress, an operator\'s move, a platform machine and a success are not', () => {
    const base = { onKeeper: true, operatorMove: false, failed: 14, referencingNew: 0, unknown: 0, done: 0 }
    expect(keeperReplacementVerdict(base)).toBe('abandon')
    expect(keeperReplacementVerdict({ ...base, referencingNew: 3 })).toBe('keep')
    expect(keeperReplacementVerdict({ ...base, operatorMove: true })).toBe('keep')
    expect(keeperReplacementVerdict({ ...base, onKeeper: false })).toBe('keep')
    expect(keeperReplacementVerdict({ ...base, failed: 0 })).toBe('keep')
  })
  test('the new member is never deleted while anything could be on it: a final read that came back unknown, or a migration of this job that succeeded, keeps it', () => {
    const base = { onKeeper: true, operatorMove: false, failed: 14, referencingNew: 0, unknown: 0, done: 0 }
    expect(keeperReplacementVerdict({ ...base, unknown: 1 })).toBe('keep')
    expect(keeperReplacementVerdict({ ...base, done: 1 })).toBe('keep')
    expect(keeperReplacementVerdict({ ...base, unknown: 2, done: 3, referencingNew: 1 })).toBe('keep')
    expect(keeperReplacementVerdict(base)).toBe('abandon')
  })
  test('a member that answers again loses its tried mark (the mark is for one loss)', () => {
    expect(control).toContain('labelSelector=peat-sk-keeper-tried')
    expect(control).toContain("if (r?.ok && triedMarks.some((m) => String(m.data?.id) === String(sk.id))) await kube('DELETE'")
  })
  test('the wiring: the mark is written on a creation failure and on an abandoned job, read by the replacement choice, removed with the member, and the attempt on the platform\'s machines is not made to wait', () => {
    expect(control).toContain('onKeeper ? await keeperTried(lost) : false')
    expect(control).toContain("if (KEEPER_MACHINES.includes(job.node ?? '') && !moveTo) { await markKeeperTried(lost,")
    expect(control).toContain("keeperReplacementVerdict({ onKeeper: KEEPER_MACHINES.includes(job.node ?? ''), operatorMove, failed: job.failed, referencingNew: fin.onNew, unknown: fin.unknown, done: job.done }) === 'abandon'")
    expect(control.match(/sk-keeper-tried-\$\{(lost|id)\}`\)\n/g)?.length).toBeGreaterThanOrEqual(2)
    expect(control).toContain('skSeen.set(lost, Date.now() - (SK_GONE_S + 1) * 1000)')
    const abandon = control.slice(control.indexOf("=== 'abandon') {"), control.indexOf('keeperAbandoned = true'))
    expect(abandon).toContain("'Decomissioned'")
    expect(abandon).toContain('statefulsets/safekeeper-w${job.newId}')
    expect(abandon).toContain('sk-replacement-${lost}')
    expect(abandon).not.toContain('sk-lost-')
  })
  test('the node reads of the keeper machine go through the one function the move, the replacement and the seeding share, with the keeper label the install set', () => {
    const check = control.slice(control.indexOf('async function keeperProblem'), control.indexOf('async function keepersReady'))
    expect(check).toContain('keeperNodeProblem(n.json, KEEPER, KEEPER_PLACEMENT.tolerations)')
  })
})

describe('a running compute reloads a renewed certificate (Postgres reads ssl_cert_file at start or on a reload), decided on the certificate and not on its Secret', () => {
  const T = Date.parse('2027-10-21T23:16:48Z') // a renewal: the new certificate's notBefore
  const started = Date.parse('2027-08-01T00:00:00Z') // the compute's container, up since August
  const settle = 300_000
  const base = { notBeforeMs: T, startedAtMs: started, nowMs: T + settle, serial: 'AB12', reloadedFor: undefined as string | undefined, settleMs: settle }
  test('a certificate issued after the container started, five minutes ago, not reloaded for: reload', () => {
    expect(certNeedsReload(base)).toBe(true)
    expect(certNeedsReload({ ...base, nowMs: T + 3 * 86_400_000 })).toBe(true)
  })
  test('not before the mounted file has had time to follow (a reload then would read the old one)', () => {
    expect(certNeedsReload({ ...base, nowMs: T + settle - 1 })).toBe(false)
    expect(certNeedsReload({ ...base, nowMs: T })).toBe(false)
  })
  test('a container that started after the certificate was issued read it at its start: nothing to do (every wake, every restart, a first pod)', () => {
    expect(certNeedsReload({ ...base, startedAtMs: T + 1 })).toBe(false)
    expect(certNeedsReload({ ...base, startedAtMs: T })).toBe(false)
    expect(certNeedsReload({ ...base, startedAtMs: T + 86_400_000, nowMs: T + 2 * 86_400_000 })).toBe(false)
  })
  test('once reloaded for this certificate (its serial) it is not done again, a label on its Secret changes nothing, and the next renewal is another serial', () => {
    expect(certNeedsReload({ ...base, reloadedFor: 'AB12' })).toBe(false)
    expect(certNeedsReload({ ...base, reloadedFor: 'AB12', nowMs: T + 30 * 86_400_000 })).toBe(false)
    const next = T + 335 * 86_400_000
    expect(certNeedsReload({ ...base, notBeforeMs: next, nowMs: next + settle, serial: 'CD34', reloadedFor: 'AB12' })).toBe(true)
  })
  test('a time or a serial that cannot be read is never a reason to reload', () => {
    expect(certNeedsReload({ ...base, notBeforeMs: 0 })).toBe(false)
    expect(certNeedsReload({ ...base, notBeforeMs: NaN })).toBe(false)
    expect(certNeedsReload({ ...base, startedAtMs: NaN })).toBe(false)
    expect(certNeedsReload({ ...base, startedAtMs: 0 })).toBe(false)
    expect(certNeedsReload({ ...base, serial: '' })).toBe(false)
  })
  test('the serial and the issue time are read from the certificate, and a PEM that is not one reads as nothing', () => {
    const dir = mkdtempSync(join(tmpdir(), 'cert-'))
    const made = Bun.spawnSync(['openssl', 'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes', '-keyout', join(dir, 'k.pem'), '-out', join(dir, 'c.pem'), '-days', '2', '-subj', '/CN=x'])
    expect(made.exitCode).toBe(0)
    const info = certInfo(readFileSync(join(dir, 'c.pem'), 'utf8'))
    rmSync(dir, { recursive: true, force: true })
    expect(info).not.toBeNull()
    expect(info!.serial).toMatch(/^[0-9a-fA-F]+$/)
    expect(Math.abs(info!.notBeforeMs - Date.now())).toBeLessThan(10 * 60_000)
    expect(certInfo('not a certificate')).toBeNull()
    expect(certInfo('')).toBeNull()
  })
  test('the control layer wiring: the acting pod looks every five minutes, decides on the certificate in the Secret against the compute container\'s start, posts the stored spec to compute_ctl\'s /configure, and never replaces or deletes a compute for a reload', () => {
    const fn = control.slice(control.indexOf('async function reloadRenewedCerts'), control.indexOf('// ---- how long each storage node has been warming up'))
    expect(fn).toContain('if (!isLeader) return')
    expect(fn).toContain('podReady(p)')
    expect(fn).toContain('secrets/${tlsSecret(name)}')
    expect(fn).toContain("certInfo(Buffer.from(String(sec.json?.data?.['tls.crt'] ?? ''), 'base64').toString())")
    expect(fn).toContain("c.name === 'compute'")
    expect(fn).toContain('state?.running?.startedAt')
    expect(fn).not.toContain('startTime')
    expect(fn).not.toContain('managedFields')
    expect(fn).toContain('settleMs: 300_000')
    expect(fn).toContain('certReloaded.set(p.metadata.uid, cert.serial)')
    expect(fn).toContain(':3080/configure')
    expect(fn).toContain('await refreshSpec(d)')
    expect(fn).not.toContain('replaceCompute')
    expect(fn).not.toContain("'DELETE'")
    expect(fn).not.toContain('stopCompute')
    expect(control).toContain("void loop('reload renewed certificates', 300_000, reloadRenewedCerts)")
    expect(control).toContain('certInfo, certNeedsReload, tcpUp, memoryGauges, placementVerdict, startVerdict, within, type MemberAt')
  })
})

describe('the control layer\'s memory (2026-11-21: every pod grew about 10 MiB an hour until the follower was OOM-killed)', () => {
  test('the probe answers true for a listener, false for a closed port and false when nothing answers in time, and leaves no socket of its own open on the listener', async () => {
    let open = 0, seen = 0
    const server = Bun.listen({ hostname: '127.0.0.1', port: 0, socket: { open() { open++; seen++ }, close() { open-- }, data() {}, error() {} } })
    expect(await tcpUp('127.0.0.1', server.port, 1500)).toBe(true)
    await Bun.sleep(100)
    expect(seen).toBe(1); expect(open).toBe(0)
    const closed = Bun.listen({ hostname: '127.0.0.1', port: 0, socket: { open() {}, data() {} } }); const port = closed.port; closed.stop(true)
    expect(await tcpUp('127.0.0.1', port, 1500)).toBe(false)
    expect(await tcpUp('127.0.0.1', 1, 1)).toBe(false) // a 1 ms limit: the answer is no, at once
    server.stop(true)
  })
  test('many probes in a row all answer and all close (the loop the control layer runs for the life of the pod)', async () => {
    let open = 0
    const server = Bun.listen({ hostname: '127.0.0.1', port: 0, socket: { open() { open++ }, close() { open-- }, data() {}, error() {} } })
    for (let i = 0; i < 300; i++) expect(await tcpUp('127.0.0.1', server.port, 1500)).toBe(true)
    await Bun.sleep(200)
    expect(open).toBe(0)
    server.stop(true)
  })
  test('the gauges name the five figures that say what grows, in bytes', () => {
    expect(memoryGauges({ rss: 1, heapUsed: 2, heapTotal: 3, external: 4, arrayBuffers: 5 })).toEqual({ process_rss_bytes: 1, process_heap_used_bytes: 2, process_heap_total_bytes: 3, process_external_bytes: 4, process_array_buffers_bytes: 5 })
  })
  test('the control layer probes with that function and does not import node:net; its metrics carry the gauges and an operator route reads the heap\'s object types and the sizes of its maps', () => {
    expect(control).not.toContain("from 'node:net'")
    expect(control).not.toMatch(/\bconnect\(\{ host/)
    expect(control).toContain('memoryGauges(process.memoryUsage())')
    const routes = control.slice(control.indexOf('const CALLER_ROUTES'), control.indexOf('let callerCache'))
    expect(routes).not.toContain('/memory')
    expect(control).toContain("path === '/memory'")
    expect(control).toContain("import('bun:jsc')")
  })
})

describe('the control layer collects its garbage itself (2026-11-21: JavaScriptCore collected lazily in a container whose limit it does not see)', () => {
  test('a full collection runs every 30 seconds by default, is counted, and can be turned off', () => {
    expect(control).toContain("const GC_EVERY_S = Number(process.env['GC_EVERY_S'] ?? '30')")
    expect(control).toContain("if (GC_EVERY_S > 0) setInterval(() => { Bun.gc(true); bump('gc_runs_total') }, GC_EVERY_S * 1000)")
  })
  test('a loop of the program\'s own shape holds a flat heap with that collection (a full collection every 30 ms stands for 30 s here)', async () => {
    const { heapStats } = await import('bun:jsc')
    const live = () => (heapStats() as { objectTypeCounts: Record<string, number> }).objectTypeCounts.Promise ?? 0
    let stop = false
    const worker = (async () => { while (!stop) { await new Promise((r) => setTimeout(r, 1)); await Promise.all([1, 2, 3].map(async () => JSON.parse('{"a":[1,2,3]}'))) } })()
    const timer = setInterval(() => Bun.gc(true), 30)
    await Bun.sleep(400)
    const a = live()
    await Bun.sleep(600)
    const b = live()
    stop = true; clearInterval(timer); await worker
    expect(b - a).toBeLessThan(200)
  })
})

describe('a replacement that throws does not leave its migration monitor running', () => {
  test('the monitor\'s interval is declared before the try, assigned inside it, cleared on the success path and again in a finally that the catch does not skip', () => {
    const fn = control.slice(control.indexOf('async function replaceSafekeeper'), control.indexOf('async function loop('))
    expect(fn).toContain('let monitor: ReturnType<typeof setInterval> | undefined')
    expect(fn.indexOf('let monitor')).toBeLessThan(fn.indexOf('  try {'))
    expect(fn).toContain('    monitor = setInterval(async () => {')
    expect(fn).not.toContain('const monitor')
    expect(fn.match(/clearInterval\(monitor\)/g)).toHaveLength(2)
    const tail = fn.slice(fn.lastIndexOf('  } catch (e) {'))
    expect(tail).toMatch(/\} finally \{\n    clearInterval\(monitor\)\n  \}\n\}\n?$/)
  })
})

describe('a start that cannot be scheduled answers with the reason (a client waited the gateway\'s 120 s without being told the machines were full)', () => {
  const unschedulable = { metadata: { name: 'compute-x-abc' }, spec: {}, status: { phase: 'Pending', conditions: [{ type: 'PodScheduled', status: 'False', reason: 'Unschedulable', message: '0/3 nodes are available: 1 node(s) had untolerated taint {peat.zabaca.com/dead: }, 2 Insufficient memory.\n preemption: 0/3 nodes are available.' }] } }
  test('no pod yet, a pod the scheduler has not looked at, and a pod on its way out are all still waiting', () => {
    expect(placementVerdict(null)).toEqual({ kind: 'waiting' })
    expect(placementVerdict(undefined)).toEqual({ kind: 'waiting' })
    expect(placementVerdict({ metadata: {}, spec: {}, status: { phase: 'Pending' } })).toEqual({ kind: 'waiting' })
    expect(placementVerdict({ metadata: {}, spec: {}, status: { phase: 'Pending', conditions: [{ type: 'PodScheduled', status: 'False', reason: 'SchedulingGated' }] } })).toEqual({ kind: 'waiting' })
    expect(placementVerdict({ metadata: { deletionTimestamp: 'x' }, spec: { nodeName: 'w1' }, status: {} })).toEqual({ kind: 'waiting' })
  })
  test('a pod with a machine is placed, Ready or not', () => {
    expect(placementVerdict({ metadata: {}, spec: { nodeName: 'w1' }, status: { phase: 'Pending' } })).toEqual({ kind: 'placed' })
    expect(placementVerdict({ metadata: {}, spec: { nodeName: 'w1' }, status: { conditions: [{ type: 'PodScheduled', status: 'True' }] } })).toEqual({ kind: 'placed' })
  })
  test('an unschedulable pod is refused with the scheduler\'s own words on one line', () => {
    const v = placementVerdict(unschedulable)
    expect(v.kind).toBe('refused')
    if (v.kind === 'refused') {
      expect(v.reason).toContain('Insufficient memory')
      expect(v.reason).not.toContain('\n')
    }
  })
  test('a long message is cut, and a condition with no message still gives a reason', () => {
    const long = placementVerdict({ metadata: {}, spec: {}, status: { conditions: [{ type: 'PodScheduled', status: 'False', reason: 'Unschedulable', message: 'x'.repeat(2000) }] } })
    expect(long.kind === 'refused' && long.reason.length).toBe(300)
    const none = placementVerdict({ metadata: {}, spec: {}, status: { conditions: [{ type: 'PodScheduled', status: 'False', reason: 'Unschedulable' }] } })
    expect(none.kind === 'refused' && none.reason).toContain('no machine')
  })
  test('the start route refuses with 503 and the reason, puts the compute back to sleep and counts it; the other starts are unchanged', () => {
    const fn = control.slice(control.indexOf('async function startPlaced'), control.indexOf('const rootOf'))
    expect(fn).toContain('startVerdict({')
    expect(fn).toContain('await bounded(stopCompute(d.name))')
    expect(fn).toContain("bump('start_refused_total')")
    expect(fn).toContain('START_LOOK_S')
    const route = control.slice(control.indexOf("m[2] === 'start'"), control.indexOf("m[2] === 'stop'"))
    expect(route).toContain('await startPlaced(d, reqAt)')
    expect(route).toContain('the machines are full')
    expect(route).toContain(', 503)')
    expect(control.match(/await startCompute\(/g)!.length).toBeGreaterThanOrEqual(4) // creating a database and the recovery paths still call startCompute directly
  })
})

describe('a start is refused only when it surely would not have been placed, and always answers inside the gateway\'s 10 s', () => {
  const stuck = (extra: object = {}) => ({ metadata: { labels: { compute: 'orders' }, creationTimestamp: '2026-11-21T10:00:00Z', ...extra }, spec: {}, status: { conditions: [{ type: 'PodScheduled', status: 'False', reason: 'Unschedulable', message: '0/3 nodes are available: 3 Insufficient memory.' }] } })
  const base = { refuseAfter: 3 }
  test('placed at once, however many looks came before', () => {
    const placed = { metadata: { labels: { compute: 'orders' } }, spec: { nodeName: 'w1' }, status: {} }
    expect(startVerdict({ ...base, mine: [placed], pods: [placed], refusedLooks: 2, lookEnded: true })).toEqual({ kind: 'placed', refusedLooks: 0, reason: '' })
  })
  test('Unschedulable is only counted before the look ends and until it has held three looks in a row', () => {
    const p = stuck()
    expect(startVerdict({ ...base, mine: [p], pods: [p], refusedLooks: 0, lookEnded: false }).refusedLooks).toBe(1)
    expect(startVerdict({ ...base, mine: [p], pods: [p], refusedLooks: 2, lookEnded: false }).kind).toBe('waiting') // the look is not over
    expect(startVerdict({ ...base, mine: [p], pods: [p], refusedLooks: 1, lookEnded: true }).kind).toBe('waiting') // 2 looks: not enough
    const r = startVerdict({ ...base, mine: [p], pods: [p], refusedLooks: 2, lookEnded: true })
    expect(r.kind).toBe('refused')
    expect(r.reason).toContain('Insufficient memory')
  })
  test('a look that is not Unschedulable resets the count', () => {
    expect(startVerdict({ ...base, mine: [], pods: [], refusedLooks: 2, lookEnded: true })).toEqual({ kind: 'waiting', refusedLooks: 0, reason: '' })
  })
  test('no refusal while any compute pod, of any database, is on its way out: its memory comes free within seconds', () => {
    const leaving = { metadata: { labels: { compute: 'other' }, deletionTimestamp: '2026-11-21T10:00:01Z' }, spec: { nodeName: 'w2' }, status: {} }
    const p = stuck()
    expect(startVerdict({ ...base, mine: [p], pods: [p, leaving], refusedLooks: 5, lookEnded: true }).kind).toBe('waiting')
    expect(startVerdict({ ...base, mine: [p, { ...leaving, metadata: { ...leaving.metadata, labels: { compute: 'orders' } } }], pods: [p, leaving], refusedLooks: 5, lookEnded: true }).kind).toBe('waiting')
  })
  test('the route measures its look from the start of the request and ends it at START_LOOK_S, well inside the gateway\'s 10 s', () => {
    expect(control).toContain('const reqAt = Date.now()')
    const fn = control.slice(control.indexOf('async function startPlaced'), control.indexOf('const rootOf'))
    expect(fn).toContain('const lookEnds = requestAt + START_LOOK_S * 1000')
    expect(fn).toContain('while (Date.now() < lookEnds)')
    expect(control).toContain("process.env['START_LOOK_S'] ?? '6'")
    expect(Number(/START_LOOK_S'\] \?\? '(\d+)'/.exec(control)![1])).toBeLessThanOrEqual(7)
    expect(src('gateway/neon-api.ts')).toContain('AbortSignal.timeout(10_000)')
  })
  test('before a refusal stops the compute, the pod is read once more, so a pod the other gateway saw placed is not stopped', () => {
    const fn = control.slice(control.indexOf('async function startPlaced'), control.indexOf('const rootOf'))
    const at = fn.indexOf("if (v.kind === 'refused')")
    expect(fn.indexOf('const again = startVerdict', at)).toBeGreaterThan(at)
    expect(fn.indexOf("if (again.kind !== 'refused') return { pod: name }")).toBeLessThan(fn.indexOf('await bounded(stopCompute(d.name))'))
  })
})

describe('the look never turns a wake the old route would have answered into a failure', () => {
  test('within answers with the value when the work settles first, and with done:false when time runs out first', async () => {
    expect(await within(500, Promise.resolve(7))).toEqual({ done: true, value: 7 })
    const t0 = Date.now()
    expect(await within(50, new Promise((res) => setTimeout(() => res(1), 2000)))).toEqual({ done: false })
    expect(Date.now() - t0).toBeLessThan(1000)
    expect(await within(0, new Promise((res) => setTimeout(() => res(1), 200)))).toEqual({ done: false }) // no time left: no wait
  })
  test('a rejection still rejects (the caller decides), and a timer is not left running', async () => {
    await expect(within(500, Promise.reject(new Error('503 from the API')))).rejects.toThrow('503')
    const t0 = Date.now()
    await within(5000, Promise.resolve(1))
    expect(Date.now() - t0).toBeLessThan(500)
  })
  test('every read and the stop inside the look go through bounded(), and the hard limit is well inside the gateway\'s 10 s', () => {
    const lookFn = control.slice(control.indexOf('async function look('), control.indexOf('const rootOf'))
    expect(lookFn.match(/kube\(/g)!.length).toBe(1)
    expect(lookFn).toContain("bounded(kube('GET'")
    expect(lookFn).toContain('await bounded(stopCompute(d.name))')
    expect(lookFn).not.toContain('await stopCompute(')
    expect(lookFn.match(/STOP_RESERVE_MS/g)!.length).toBe(2) // before the reread and before the stop
    expect(control).toContain("process.env['START_HARD_S'] ?? '8'")
    expect(control).toContain('const hardEnds = requestAt + START_HARD_S * 1000')
    expect(Number(/START_HARD_S'\] \?\? '(\d+)'/.exec(control)![1])).toBeLessThanOrEqual(9)
    expect(Number(/START_LOOK_S'\] \?\? '(\d+)'/.exec(control)![1])).toBeLessThan(Number(/START_HARD_S'\] \?\? '(\d+)'/.exec(control)![1]))
  })
  test('startPlaced starts first, then answers any error of the look as the old success, never as a 500', () => {
    const fn = control.slice(control.indexOf('async function startPlaced'), control.indexOf('async function look('))
    expect(fn.indexOf('await startCompute(d)')).toBeGreaterThan(-1)
    expect(fn.indexOf('await startCompute(d)')).toBeLessThan(fn.indexOf('try {'))
    expect(fn).toContain('return await look(d, name, requestAt)')
    expect(fn.slice(fn.indexOf('catch (e) {'))).toContain('return { pod: name }')
    expect(fn).toContain("bump('start_look_failed_total')")
  })
})

describe('the storage as an upgrade\'s check reads it', () => {
  test('a settled platform has no problem; each way of not being settled is named', () => {
    const node = (id: number, a = 'Active', s = 'Active') => ({ id, availability: a, scheduling: s })
    const shard = (o: Record<string, unknown> = {}) => ({ tenant_shard_id: 'abcdef0123456789', node_attached: 1, node_secondary: [2], last_error: '', is_reconciling: false, is_pending_compute_notification: false, is_splitting: false, is_importing: false, scheduling_policy: 'Active', ...o })
    const tenant = (o: Record<string, unknown> = {}) => ({ tenant_id: 'abcdef0123456789', policy: { Attached: 1 }, shards: [shard(o)] })
    const nodes = [node(1), node(2)]
    expect(storageStateProblems(nodes, [tenant(), tenant({ node_attached: 2, node_secondary: [1] })], [1, 2])).toEqual([])
    expect(storageStateProblems([node(1)], [], [1, 2])).toEqual(['node 2 is not registered'])
    expect(storageStateProblems([node(1), node(2, 'Offline')], [], [1, 2])[0]).toContain('node 2 availability')
    expect(storageStateProblems([node(1), node(2, 'Active', 'Pause')], [], [1, 2])[0]).toContain('node 2 scheduling')
    expect(storageStateProblems(nodes, [tenant({ node_attached: null })], [1, 2])[0]).toContain('attached nowhere')
    expect(storageStateProblems([node(1, 'Offline'), node(2)], [tenant({ node_attached: 1 })], [1, 2]).some((p) => p.includes('attached to node 1, which is not Active'))).toBe(true)
    expect(storageStateProblems(nodes, [tenant({ last_error: 'boom' })], [1, 2])[0]).toContain('has an error')
    expect(storageStateProblems(nodes, [tenant({ is_reconciling: true })], [1, 2])[0]).toContain('reconciling')
    expect(storageStateProblems(nodes, [tenant({ is_pending_compute_notification: true })], [1, 2])[0]).toContain('notification')
    expect(storageStateProblems(nodes, [tenant({ node_secondary: [] })], [1, 2])[0]).toBe('shard abcdef01 has 0 of 1 secondaries')
    expect(storageStateProblems(nodes, [{ tenant_id: 'x', policy: { Attached: 0 }, shards: [shard({ node_secondary: [] })] }], [1, 2])).toEqual([])
  })
  test('the drain and fill judgements: a drain is done only at PauseForRestart with nothing attached; a fill only at Active and available again', () => {
    const node = (id: number, scheduling: string, availability = 'Active') => ({ id, availability, scheduling })
    const on = (n: number) => [{ shards: [{ node_attached: n }] }, { shards: [{ node_attached: 2 }] }]
    expect(nodeDrainState([node(1, 'Active')], on(1), 1)).toEqual({ done: false, why: 'node 1 scheduling is "Active", 1 shards still attached' })
    expect(nodeDrainState([node(1, 'Draining')], on(1), 1).done).toBe(false)
    expect(nodeDrainState([node(1, 'PauseForRestart')], on(1), 1)).toEqual({ done: false, why: '1 shards are still attached to node 1' })
    expect(nodeDrainState([node(1, 'PauseForRestart')], on(2), 1)).toEqual({ done: true, why: '' })
    expect(nodeDrainState([node(1, 'pause_for_restart')], on(2), 1).done).toBe(true) // either spelling
    expect(nodeDrainState([node(2, 'PauseForRestart')], on(2), 1).done).toBe(false) // not registered
    expect(nodeFillState([node(1, 'Filling')], 1).done).toBe(false)
    expect(nodeFillState([node(1, 'PauseForRestart')], 1).done).toBe(false)
    expect(nodeFillState([node(1, 'Active', 'Offline')], 1).done).toBe(false)
    expect(nodeFillState([node(1, 'Active')], 1)).toEqual({ done: true, why: '' })
  })
  test('a secondary is warm only when a heatmap was seen and every layer and byte in it is on disk', () => {
    const ok = { heatmap_mtime: '2026-11-22T03:40:21.000Z', layers_downloaded: 11, layers_total: 11, bytes_downloaded: 100, bytes_total: 100 }
    expect(secondaryWarmProblem(ok)).toBeNull()
    expect(secondaryWarmProblem({ ...ok, layers_downloaded: 0, layers_total: 0, bytes_downloaded: 0, bytes_total: 0 })).toBeNull() // an empty tenant
    expect(secondaryWarmProblem({ ...ok, layers_downloaded: 9 })).toBe('9 of 11 layers downloaded (100 of 100 bytes)')
    expect(secondaryWarmProblem({ ...ok, bytes_downloaded: 40 })).toContain('40 of 100 bytes')
    expect(secondaryWarmProblem({ ...ok, heatmap_mtime: null })).toContain('no heatmap')
    expect(secondaryWarmProblem(undefined)).toContain('unreadable')
  })
  test('the state judgement counts a cold or unread secondary as a problem when statuses are given, and not when they are not', () => {
    const nodes = [{ id: 1, availability: 'Active', scheduling: 'Active' }, { id: 2, availability: 'Active', scheduling: 'Active' }]
    const tenants = [{ tenant_id: 'abcdef0123456789', policy: { Attached: 1 }, shards: [{ tenant_shard_id: 'abcdef0123456789', node_attached: 1, node_secondary: [2], last_error: '', is_reconciling: false, is_pending_compute_notification: false, is_splitting: false, is_importing: false, scheduling_policy: 'Active' }] }]
    const warm = { heatmap_mtime: 'x', layers_downloaded: 3, layers_total: 3, bytes_downloaded: 9, bytes_total: 9 }
    expect(storageStateProblems(nodes, tenants, [1, 2])).toEqual([])
    expect(storageStateProblems(nodes, tenants, [1, 2], new Map([['abcdef0123456789/2', warm]]))).toEqual([])
    expect(storageStateProblems(nodes, tenants, [1, 2], new Map([['abcdef0123456789/2', { ...warm, layers_downloaded: 1 }]]))).toEqual(["shard abcdef01's secondary on node 2 is not warm: 1 of 3 layers downloaded (9 of 9 bytes)"])
    expect(storageStateProblems(nodes, tenants, [1, 2], new Map())[0]).toContain('unreadable')
  })
  test('the roll script drains, puts the node back to Active, fills and waits 10 minutes for each, and never falls back to a delete', () => {
    const roll = src('storage/roll.ts')
    for (const need of ['`/control/v1/node/${id}/drain`', '`/control/v1/node/${id}/config`', '`/control/v1/node/${id}/fill`', "scheduling: 'Active'", "'ROLL_WAIT_S'] ?? '600'", 'nodeDrainState', 'nodeFillState', 'STOP:']) expect(roll).toContain(need)
    expect(roll).not.toContain('delete pod')
    expect(roll).not.toContain("'DELETE', `/api/v1")
  })

  describe('a shard\'s old error is a warning only when everything agrees (2026-11-21: the controller never clears an error a retried compute notification left)', () => {
    const loc = (o: Partial<ShardLocations> = {}): ShardLocations => ({
      generation: 4,
      intent: { attached: 2, secondary: [1] },
      observed: { '1': { mode: 'Secondary', generation: null }, '2': { mode: 'AttachedSingle', generation: 4 } },
      held: { '1': undefined, '2': { attached: true, generation: 4 } },
      ...o,
    })
    const quiet = { is_reconciling: false, is_pending_compute_notification: false }
    test('the leftover: nothing pending, the intended node holds it attached at the controller\'s generation by both views, no other node holds it, the secondary is observed', () => {
      expect(staleErrorProblem(quiet, loc())).toBeNull()
      expect(staleErrorProblem(quiet, loc({ intent: { attached: 2, secondary: [] } }))).toBeNull()
    })
    test('every way it is not a leftover keeps the error a problem', () => {
      expect(staleErrorProblem({ is_reconciling: true }, loc())).toContain('reconciling')
      expect(staleErrorProblem({ is_pending_compute_notification: true }, loc())).toContain('notification is pending')
      expect(staleErrorProblem(quiet, undefined)).toContain('could not be compared')
      expect(staleErrorProblem(quiet, loc({ generation: null }))).toContain('no generation')
      expect(staleErrorProblem(quiet, loc({ intent: { attached: null, secondary: [] } }))).toContain('no attached intent')
      expect(staleErrorProblem(quiet, loc({ observed: { '1': { mode: 'Secondary', generation: null }, '2': { mode: 'AttachedSingle', generation: 3 } } }))).toContain('not attached at generation 4')
      expect(staleErrorProblem(quiet, loc({ observed: { '1': { mode: 'Secondary', generation: null }, '2': { mode: 'Secondary', generation: null } } }))).toContain('not attached at generation 4')
      expect(staleErrorProblem(quiet, loc({ held: { '1': undefined, '2': undefined } }))).toContain('not held')
      expect(staleErrorProblem(quiet, loc({ held: { '1': undefined, '2': { attached: true, generation: 3 } } }))).toContain('not attached at generation 4')
      expect(staleErrorProblem(quiet, loc({ held: { '1': { attached: true, generation: 4 }, '2': { attached: true, generation: 4 } } }))).toContain('node 1 also lists it attached')
      expect(staleErrorProblem(quiet, loc({ observed: { '1': undefined, '2': { mode: 'AttachedSingle', generation: 4 } } }))).toContain('intended secondary on node 1')
    })
    const nodes = [{ id: 1, availability: 'Active', scheduling: 'Active' }, { id: 2, availability: 'Active', scheduling: 'Active' }]
    const shard = (o: Record<string, unknown> = {}) => ({ tenant_shard_id: 'abcdef0123456789', node_attached: 2, node_secondary: [1], last_error: 'Non-retryable error 403 Forbidden', is_reconciling: false, is_pending_compute_notification: false, is_splitting: false, is_importing: false, scheduling_policy: 'Active', ...o })
    const tenants = (o: Record<string, unknown> = {}) => [{ tenant_id: 'abcdef0123456789', policy: { Attached: 1 }, shards: [shard(o)] }]
    test('the report: an agreeing old error is a warning and does not fail the gate; a disagreeing one, a missing comparison, or a pending one fails it', () => {
      const ok = storageStateReport(nodes, tenants(), [1, 2], undefined, new Map([['abcdef0123456789', loc()]]))
      expect(ok.problems).toEqual([])
      expect(ok.warnings).toHaveLength(1)
      expect(ok.warnings[0]).toContain('a leftover')
      expect(ok.warnings[0]).toContain('403 Forbidden')
      const off = storageStateReport(nodes, tenants(), [1, 2], undefined, new Map([['abcdef0123456789', loc({ generation: 5 })]]))
      expect(off.warnings).toEqual([])
      expect(off.problems[0]).toContain('not a leftover')
      expect(storageStateReport(nodes, tenants(), [1, 2]).problems[0]).toContain('could not be compared')
      expect(storageStateReport(nodes, tenants({ is_pending_compute_notification: true }), [1, 2], undefined, new Map([['abcdef0123456789', loc()]])).problems.some((p) => p.includes('notification pending'))).toBe(true)
      expect(storageStateReport(nodes, tenants({ last_error: '' }), [1, 2]).warnings).toEqual([])
      expect(storageStateProblems(nodes, tenants({ last_error: '' }), [1, 2])).toEqual([]) // the old entry point still returns the problems only
    })
    test('the state script reads the controller\'s debug listing and each pageserver\'s own list, only for shards with an error, and prints the warnings', () => {
      const state = src('storage/state.ts')
      for (const need of ['debugTenants', 'heldByPageserver', 'withError', 'storageStateReport', 'warning: ']) expect(state).toContain(need)
      const api = src('storage/api.ts')
      expect(api).toContain("'/debug/v1/tenant'")
      expect(api).toContain("'/v1/tenant'")
    })
  })
})

describe('a database\'s Postgres version: 19 by default since 2026-11-26, 18 for a database made before or asked for', () => {
  test('a record without `pg` is 18, a record or a request names 18 or 19 and nothing else, and an absent request is 19 while an unrecorded record stays 18', () => {
    expect(UNRECORDED_PG_VERSION).toBe(18); expect(DEFAULT_PG_VERSION).toBe(19)
    expect(pgVersionOf(undefined)).toBe(18); expect(pgVersionOf('')).toBe(18)
    expect(pgVersionOf('18')).toBe(18); expect(pgVersionOf('19')).toBe(19)
    expect(() => pgVersionOf('17')).toThrow(/does not run/); expect(() => pgVersionOf('x')).toThrow()
    expect(requestedPgVersion(undefined)).toBe(19); expect(requestedPgVersion(null)).toBe(19); expect(requestedPgVersion('')).toBe(19)
    expect(requestedPgVersion(19)).toBe(19); expect(requestedPgVersion('19')).toBe(19); expect(requestedPgVersion(18)).toBe(18)
    for (const bad of [17, 20, 'nineteen', 19.5, {}, true]) expect(() => requestedPgVersion(bad as any), String(bad)).toThrow(/pg_version/)
  })
  test('a version with no image is refused, and each version gets its own', () => {
    expect(computeImageFor(18, { 18: 'a', 19: 'b' })).toBe('a'); expect(computeImageFor(19, { 18: 'a', 19: 'b' })).toBe('b')
    expect(() => computeImageFor(19, { 18: 'a', 19: undefined })).toThrow(/no compute image is set for Postgres 19/)
  })
  test('the program reads the 19 image from its environment (optional: unset, a request for 19 is refused and 18 is served as before), records the version, and gives a timeline, a branch, a replica and a recovery their database\'s', () => {
    expect(control).toContain("19: process.env['COMPUTE_IMAGE_PG19'] || undefined")
    expect(control).toContain("const DEFAULT_PG = Number(process.env['DEFAULT_PG_VERSION'] || '19')")
    expect(control).toContain('image: computeImageFor(d.pg, COMPUTE_IMAGES)')
    const records = readFileSync(join(import.meta.dir, '../src/records.ts'), 'utf8')
    expect(records).toContain("pg: pgVersionOf(s.pgVersion === undefined ? undefined : String(s.pgVersion))") // a record read from its Database's status
    expect(records).toContain('tenant: r.tenant, timeline: r.timeline, pgVersion: r.pg, createdAt: r.created') // and written there by save
    expect(control).toContain('const pg = requestedPgVersion(asked, DEFAULT_PG)')
    expect(control).toContain('{ new_timeline_id: timeline, pg_version: pg }')
    expect(control).toContain("pg_version: parent.pg }), 'create branch timeline', [409])") // a branch and a restore are of the parent's version
    expect(control).toContain('created: new Date().toISOString(), pg: parent.pg, closed: false }, lsn, kind }')
    expect(control).toContain("pg_version: d.pg }), 'create the recovered timeline'")
    expect(control).toContain("{ pgVersion: Number(b.pg_version) }") // the API's version is the Database's
    expect(control).toContain('await createDb(name, act.pg, ns)')
  })
  test('the version is checked, and a compute image looked up, before the tenant is made: a request no compute could serve leaves nothing behind', () => {
    const fn = control.slice(control.indexOf('async function createDb('), control.indexOf('async function attachedPageserverApi'))
    expect(fn.indexOf('requestedPgVersion(asked, DEFAULT_PG)')).toBeLessThan(fn.indexOf("storcon('POST', '/v1/tenant'"))
    expect(fn.indexOf('computeImageFor(pg, COMPUTE_IMAGES)')).toBeLessThan(fn.indexOf("storcon('POST', '/v1/tenant'"))
  })
  test('a Deployment carries its version as a label, so a later move of every compute to a new image can leave the other version\'s alone', () => {
    expect(control).toContain("'peat-pg': String(d.pg)")
  })
})
