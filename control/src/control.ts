// The platform's control layer: the small program that turns "a database" into Neon's terms (a tenant, a timeline, a compute), and keeps
// the storage it runs on whole. Two pods of it run, one acting at a time (a Lease, below); both serve every call.
//   - The namespaces are the install's (src/names.ts): the control layer, the storage and the records in the platform namespace, the
//     computes in the compute namespace, the gateways in theirs and each database's name in the database namespace.
//   - Every pod it makes selects the platform's machines and tolerates their taint (the computes run tenants' SQL), has no service
//     account token, and carries a TLS certificate of its own: a cert-manager Certificate per database for `<db>.<database namespace>.svc`
//     (the platform's CA signs it), and an ExternalName Service `<db>` in the database namespace that leads to the gateways' one front door.
//   - It sees only the platform's machines: nodes are listed by the machine label, so its probes, its "half the machines at once" rule and
//     its dead taint cover them and no other (an admission rule also refuses its account any change to any other node).
//   - The safekeeper's pod is `storage-spec.ts`'s, the same one the platform starts with.
//   - The WAL trim reads each timeline at most hourly and lists the store only when its floor has moved (a 30 s cycle over 150 timelines
//     would be about 13 million listings a month).
// Its records are the Database objects (the status of each holds its record); its other marks are ConfigMaps and Secrets in the platform
// namespace. Run by Bun from the control image, under the account `peat-control`.
import { X509Certificate, createHmac, createHash, createPrivateKey, createPublicKey, generateKeyPairSync, pbkdf2Sync, randomBytes, sign as edSign, verify as edVerify } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { SAFEKEEPER_RESOURCES, SEED_GROUP_ANNOTATION, closeDecision, decommissionDecision, reopenDecision, totalLossEvidence, formatSeedGroup, parseSeedGroup, membersOffBox, moveDecision, otherReplacementOpen, placementFor, replaceDecision, replacementMachines, replacementPool, safekeeperStatefulSet, seedCompletion, seedDecision, seedGroup, seedIds, neverWritten, isPullTimelineFailure, keeperNodeProblem, keeperReplacementVerdict, certInfo, certNeedsReload, tcpUp, memoryGauges, placementVerdict, startVerdict, within, type MemberAt, type ReplacementMark, type SkRecord, type TimelineRead } from './storage-spec'
import { computeImageFor, pgVersionOf, requestedPgVersion } from './storage-spec' // a database's Postgres version and the compute image that serves it
import { ancestorFor, olderTimelineFor, ownMemberProbe, recoverDecision, recoverStep, recoveryPoint, recoveryScope, timelineOwnMembers, unfinishedRepoint, type MemberProbe, type RecoveryMark } from './storage-spec' // the recovery of a database under its own name (recoverDatabase)
import { GROUP, controlUrl, fromEnv, frontDoor, parseLabel, placement } from './names'
import { newLeaf } from './operator/keys'
import { COPYING, DB_NAME, DRILL, FINALIZER, databaseAction, deleteAfter } from './databases'
import { DB_API, HELD_SELECTOR, beginMaking, knownDb, claimPort, copyRecords, dbObjectPath, endMaking, ensureLogin, fromStatus, hold, orphanedTenant, purgeHeld, readRecords, save as saveRecord, unfinishedMake, writeRecord, type Db, type Deps } from './records'

const NAMES = fromEnv()
const NS = NAMES.namespace // the control layer, the storage, the records, the leases
const CNS = NAMES.computeNamespace // the computes, their specs and certificates
const GW_NS = NAMES.gatewayNamespace
const DB_NS = NAMES.dbNamespace // each database's name: an ExternalName Service leading to the gateways' front door
const FRONT_DOOR = process.env['FRONT_DOOR'] ?? frontDoor(NAMES)
const STORCON = `http://storage-controller.${NS}.svc:1234`
const CONTROL_URL = controlUrl(NAMES)
const SK_MAX_ID = Number(process.env['SK_MAX_ID'] ?? '30')
const PS_SUFFIX = `.pageserver.${NS}.svc.cluster.local`
const SK_SUFFIX = `.safekeeper.${NS}.svc.cluster.local`
// the platform's machines, which every pod it makes selects and tolerates, and the label that names them
const PEAT_LABEL = NAMES.machineLabel.key
const PEAT_LABEL_VALUE = NAMES.machineLabel.value
const PLACEMENT = placement(NAMES)
// A keeper machine: a machine outside the platform's that may hold one safekeeper, so that a timeline's set does not live on the platform's
// machines alone (when those share one host, a member elsewhere survives that host). It is chosen by a label of its own (KEEPER_LABEL,
// `key=value`) and its pod tolerates what that machine carries (KEEPER_TOLERATIONS, a JSON list). KEEPER_MACHINES names the machines.
const KEEPER = parseLabel(process.env['KEEPER_LABEL'] || `${GROUP}/keeper=true`)
const KEEPER_LABEL = KEEPER.key
const KEEPER_PLACEMENT = { nodeSelector: { [KEEPER.key]: KEEPER.value }, tolerations: JSON.parse(process.env['KEEPER_TOLERATIONS'] || '[]') as Record<string, unknown>[] }
const KEEPER_MACHINES = (process.env['KEEPER_MACHINES'] ?? '').split(',').filter(Boolean)
// The Secrets the safekeepers read: the store's key, Neon's public key and the safekeepers' own token
const SK_SECRETS = { store: process.env['SAFEKEEPER_STORE_SECRET'] || 'peat-store-safekeeper', auth: process.env['AUTH_PUBLIC_SECRET'] || 'peat-auth-public', tokens: process.env['SAFEKEEPER_TOKENS_SECRET'] || 'peat-safekeeper-tokens' }
// The directory on each machine that holds its safekeepers' data (one subdirectory per member), and whether it must exist already
// (`Directory`: a volume that did not provision fails the pod instead of filling the system disk) or is made (`DirectoryOrCreate`)
const SK_VOLUME = process.env['SAFEKEEPER_VOLUME'] || '/var/lib/peat/safekeeper'
const SK_VOLUME_TYPE = process.env['SAFEKEEPER_VOLUME_TYPE'] === 'DirectoryOrCreate' ? 'DirectoryOrCreate' : 'Directory'
// one StatefulSet of one pod per machine, node id = the machine's number: pageserver-w<id>-0, safekeeper-w<id>-0
const COMPUTE_IMAGE = process.env['COMPUTE_IMAGE'] ?? ''
if (!COMPUTE_IMAGE) throw new Error('COMPUTE_IMAGE is not set')
// A database is of one Postgres version, 19 unless its creation asked for 18 (`pg_version` of `POST /db`; its record's `pg`, and a record
// without one is 18); its timeline is made as that version and its compute pod runs that version's image. COMPUTE_IMAGE serves 18 and
// COMPUTE_IMAGE_PG19 serves 19; with no image for 19, a request that names no version (a request for 19) is refused, and a database already
// made is served by its own version's image as before.
const COMPUTE_IMAGES: Record<number, string | undefined> = { 18: COMPUTE_IMAGE, 19: process.env['COMPUTE_IMAGE_PG19'] || undefined }
const ROUTE_ANNOTATION = process.env['ROUTE_ANNOTATION'] || `${GROUP}/route`
const SLEEP_ENABLED = 'xata.io/scale-to-zero-enabled'
const SLEEP_IDLE_MINUTES = 'xata.io/scale-to-zero-inactivity-minutes'
const HIBERNATION = 'cnpg.io/hibernation'
const IDLE_MINUTES = process.env['IDLE_MINUTES'] ?? '30' // the full-sleep time: a database with no session for this long has its compute removed
const PORT_BASE = 15010
const COMPUTE_MEMORY_REQUEST = process.env['COMPUTE_MEMORY_REQUEST'] ?? '512Mi'
const COMPUTE_MEMORY_LIMIT = process.env['COMPUTE_MEMORY_LIMIT'] ?? '1Gi'
const PGDATA_LIMIT = process.env['PGDATA_LIMIT'] ?? '2Gi'
const DEFAULT_PG = Number(process.env['DEFAULT_PG_VERSION'] || '19')
// How many warm copies of a new database's pages the controller keeps on other pageservers: one (a pageserver's loss fails over in
// seconds), or none when the platform has one pageserver (the controller cannot place a copy, and refuses the tenant)
const SECONDARIES = Number(process.env['PAGESERVER_SECONDARIES'] ?? '1')
// SPREAD `none` is a one-machine trial: every safekeeper is a zone of its own (the controller picks a timeline's members from distinct zones)
// and a replacement may go to a machine that holds another member. `machines` (the default) keeps a machine's loss from taking two members.
const SPREAD = process.env['SPREAD'] === 'none' ? 'none' : 'machines'
// The node label whose value is a safekeeper's zone: its machine's name by default
const SK_ZONE_LABEL = process.env['SAFEKEEPER_ZONE_LABEL'] || 'kubernetes.io/hostname'
const SK_RESOURCES = { requests: { cpu: '100m', memory: process.env['SAFEKEEPER_MEMORY_REQUEST'] || SAFEKEEPER_RESOURCES.requests.memory }, limits: { memory: process.env['SAFEKEEPER_MEMORY_LIMIT'] || SAFEKEEPER_RESOURCES.limits.memory } }
// The folder of the store's bucket the safekeepers write under (and the WAL trim lists)
const S3_PREFIX = (process.env['S3_PREFIX'] || 'safekeeper').replace(/\/+$/, '')
// A database's certificate: a cert-manager issuer (`Issuer/<name>` or `ClusterIssuer/<name>`) signs it, or, with none, this layer signs it
// with the platform's own authority (its certificate and key mounted at CA_DIR). DB_DOMAIN, when set, is the domain a client outside the
// cluster reaches the databases under: `<db>.<DB_DOMAIN>` is in each certificate too.
const CERT_ISSUER = process.env['CERT_ISSUER'] ?? ''
const CA_DIR = process.env['CA_DIR'] ?? '/ca'
const DB_DOMAIN = process.env['DB_DOMAIN'] ?? ''

// ---- Kubernetes, as the pod sees it
const API = `https://${process.env['KUBERNETES_SERVICE_HOST'] ?? 'kubernetes.default.svc'}:${process.env['KUBERNETES_SERVICE_PORT'] ?? '443'}`
const SA = '/var/run/secrets/kubernetes.io/serviceaccount'
const kube = async (method: string, path: string, body?: unknown, contentType = 'application/json'): Promise<{ status: number; json: any }> => {
  const token = readFileSync(`${SA}/token`, 'utf8').trim()
  const ca = readFileSync(`${SA}/ca.crt`, 'utf8')
  const r = await fetch(`${API}${path}`, {
    method,
    headers: { authorization: `Bearer ${token}`, ...(body !== undefined ? { 'content-type': contentType } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
    tls: { ca },
  } as RequestInit)
  const text = await r.text()
  // an API group the cluster does not have (cert-manager's, when none is installed) answers 404 in plain text
  return { status: r.status, json: text ? safeJson(text) : {} }
}
const ok = (r: { status: number; json: any }, what: string, also: number[] = []) => {
  if (r.status >= 300 && !also.includes(r.status)) throw new Error(`${what}: ${r.status} ${JSON.stringify(r.json).slice(0, 300)}`)
  return r
}
const apply = async (collection: string, name: string, obj: any) => {
  // create, or replace when it exists
  const made = await kube('POST', collection, obj)
  if (made.status === 409) return ok(await kube('PUT', `${collection}/${name}`, obj), `replace ${name}`)
  return ok(made, `create ${name}`)
}
const del = (path: string) => kube('DELETE', path)

// ---- Neon's own keys
// The control layer is the control plane, so it holds the private key (a Secret mounted at /keys, which the operator makes when the install gives none) and signs what it needs: a token of scope `admin` or `pageserverapi` for the storage
// controller, `pageserverapi` for a pageserver's HTTP API, `safekeeperdata` for a safekeeper's, and for each compute a token of scope `tenant` for its own tenant (the spec's `storage_auth_token`: the compute
// presents it to the pageserver and the safekeepers, which accept it only for that tenant). Neon's tokens carry no expiry. It also checks the token the storage controller sends its compute hook (`generations_api`).
const KEY_DIR = process.env['KEY_DIR'] ?? '/keys'
let signingKey: ReturnType<typeof createPrivateKey> | undefined
let verifyKey: ReturnType<typeof createPublicKey> | undefined
try {
  signingKey = createPrivateKey(readFileSync(`${KEY_DIR}/auth_private_key.pem`, 'utf8'))
  verifyKey = createPublicKey(signingKey)
} catch { throw new Error(`no signing key at ${KEY_DIR}/auth_private_key.pem: the platform's storage pieces all check tokens, so a control layer without it can do nothing`) }
const b64url = (b: Buffer | string) => Buffer.from(b).toString('base64url')
const mintToken = (claims: Record<string, unknown>): string | null => {
  if (!signingKey) return null
  const head = b64url(JSON.stringify({ alg: 'EdDSA', typ: 'JWT' }))
  const body = b64url(JSON.stringify(claims))
  return `${head}.${body}.${b64url(edSign(null, Buffer.from(`${head}.${body}`), signingKey))}`
}
const scoped = (scope: string): Record<string, string> => { const t = mintToken({ scope }); return t ? { authorization: `Bearer ${t}` } : {} }
const tenantToken = (tenant: string): string | null => mintToken({ scope: 'tenant', tenant_id: tenant })
const hookTokenOk = (header: string | null): boolean => {
  if (!verifyKey) return true
  const t = (header ?? '').replace(/^Bearer /, '').split('.')
  if (t.length !== 3) return false
  try {
    if (!edVerify(null, Buffer.from(`${t[0]}.${t[1]}`), verifyKey, Buffer.from(t[2]!, 'base64url'))) return false
    return JSON.parse(Buffer.from(t[1]!, 'base64url').toString()).scope === 'generations_api'
  } catch { return false }
}

// ---- Neon's storage controller and pageserver
const storcon = async (method: string, path: string, body?: unknown, timeoutMs = 60_000) => {
  const r = await fetch(`${STORCON}${path}`, { method, headers: { ...scoped(path.startsWith('/control/') ? 'admin' : 'pageserverapi'), ...(body === undefined ? {} : { 'content-type': 'application/json' }) }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs) })
  const text = await r.text()
  return { status: r.status, json: text ? safeJson(text) : {} }
}
const safeJson = (t: string) => { try { return JSON.parse(t) } catch { return { raw: t } } }
const hex32 = () => randomBytes(16).toString('hex')
const pageserverHost = (nodeId: number) => `pageserver-w${nodeId}-0${PS_SUFFIX}`

async function attachedNode(tenant: string): Promise<number> {
  const r = ok(await storcon('GET', `/control/v1/tenant/${tenant}`), 'describe tenant')
  return r.json.shards[0].node_attached as number
}

/** The safekeepers' own answer about a timeline: its members and membership generation (the controller creates the timeline on them; the compute must carry the same two facts). */
async function safekeepersOf(tenant: string, timeline: string): Promise<{ members: string[]; generation: number }> {
  let last = ''
  // any safekeeper that knows the timeline answers with its members; a replaced safekeeper has a new id, so every id up to SK_MAX_ID is tried, not just 1 to 3
  for (let i = 1; i <= SK_MAX_ID; i++) {
    try {
      const r = await fetch(`http://safekeeper-w${i}-0${SK_SUFFIX}:7676/v1/tenant/${tenant}/timeline/${timeline}`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(3000) })
      if (!r.ok) { last = `${r.status}`; continue }
      const m = ((await r.json()) as any).mconf
      return { members: m.members.map((x: any) => `${x.host}:${x.pg_port}`), generation: m.generation }
    } catch (e) { last = String(e) }
  }
  throw new Error(`no safekeeper knows timeline ${timeline}: ${last}`)
}

// ---- records
// A database's record is its Database object (peat.zabaca.com/v1alpha1), in whatever namespace it was made: the command and the API write
// the object, this layer makes the storage behind it (reconcileDatabases, below) and keeps its record in the object's status and, as the
// durable copy, in a ConfigMap `db-<name>` of the platform namespace (records.ts says why and what else is kept beside it). The status is
// written by this layer alone (the status subresource); a name is unique across namespaces.
const RECORDS: Deps = {
  kube, storcon: (m, p, b) => storcon(m, p, b), ns: NS, dbNs: DB_NS, domain: DB_DOMAIN, log: (l) => console.log(l),
  ancestorOf: async (tenant, timeline) => {
    const r = await fetch(`${await attachedPageserverApi(tenant)}/v1/tenant/${tenant}/timeline/${timeline}`, { headers: scoped('pageserverapi'), signal: AbortSignal.timeout(10_000) }).catch(() => null)
    return r?.ok ? (((await r.json()) as any).ancestor_timeline_id ?? null) : null
  },
}
const recordOf = fromStatus
// Every Database of the cluster, read once a second at most: the gateways' rows, the hook and the loops all read the records
let dbCache: { at: number; items: Promise<any[]> } | undefined
const dbObjects = (): Promise<any[]> => {
  if (dbCache && Date.now() - dbCache.at < 1000) return dbCache.items
  const items = (async () => ok(await kube('GET', `${DB_API}/databases`), 'list databases').json.items as any[])()
  dbCache = { at: Date.now(), items }
  items.catch(() => { if (dbCache?.items === items) dbCache = undefined })
  return items
}
const forgetDbs = () => { dbCache = undefined; routesCache = undefined; clustersCache = undefined }
const getDb = async (name: string): Promise<Db | null> => (await dbObjects()).map(recordOf).find((d) => d?.name === name) ?? null
const listDbs = async (): Promise<Db[]> => (await dbObjects()).map(recordOf).filter((d): d is Db => d !== null)
/** The database as its object says now, past every cache (a start reads it so that a database closed a moment ago is not woken). */
const freshDb = async (d: Db): Promise<Db | null> => recordOf(ok(await kube('GET', dbObjectPath(d.namespace, d.name)), `read ${d.name}`, [404]).json)
/** Writes a database's record (its ConfigMap, then its object's status). */
const saveDb = async (d: Db, more: Record<string, unknown> = {}) => { await saveRecord(RECORDS, d, more); forgetDbs() }
const patchDbStatus = async (ns: string, name: string, status: Record<string, unknown>) => {
  await kube('PATCH', `${dbObjectPath(ns, name)}/status`, { status }, 'application/merge-patch+json')
  forgetDbs()
}
/** Whether a compute runs, in the status: written as it starts or stops, never in the way of either. */
const markAwake = (d: { namespace: string; name: string }, awake: boolean) => { void patchDbStatus(d.namespace, d.name, { awake }).catch(() => {}) }
// The gateways read each database's port from `GET /services` (built from the records below): there is no Service per database in the gateways' namespace, only the one front door `pg-front`.

// ---- the compute's spec, as neon_local writes one for an endpoint (control_plane/src/endpoint.rs), reduced to what a pod needs
function scram(password: string, iterations = 4096): string {
  const salt = randomBytes(16)
  const salted = pbkdf2Sync(password, salt, iterations, 32, 'sha256')
  const clientKey = createHmac('sha256', salted).update('Client Key').digest()
  const storedKey = createHash('sha256').update(clientKey).digest()
  const serverKey = createHmac('sha256', salted).update('Server Key').digest()
  return `SCRAM-SHA-256$${iterations}:${salt.toString('base64')}$${storedKey.toString('base64')}:${serverKey.toString('base64')}`
}
// TLS for logins: the certificate and key are the database's own, from the Secret `tls-<db>` that cert-manager makes for its Certificate (the platform's CA signs it) and the pod mounts at /tls; hba_file is the
// platform's ConfigMap `peat-pg-hba`, mounted at /hba (a remote login must be TLS and scram-sha-256)
const TLS_CONF = ["ssl=on", "ssl_cert_file='/tls/tls.crt'", "ssl_key_file='/tls/tls.key'", "ssl_min_protocol_version='TLSv1.2'", "hba_file='/hba/pg_hba.conf'"]
const BASE_CONF = [...TLS_CONF, 'shared_buffers=128MB', 'effective_io_concurrency=2', 'fsync=off', 'max_connections=100', 'wal_level=logical', 'wal_sender_timeout=5s', 'wal_keep_size=0', 'restart_after_crash=off', "shared_preload_libraries='neon'", "listen_addresses='0.0.0.0'"]
const PRIMARY_CONF = ['max_replication_write_lag=15MB', 'max_replication_flush_lag=10GB', "synchronous_standby_names='walproposer'"]

// The compute's wait on a pageserver that has stopped answering: a backend with a request in flight on a connection that the pageserver's death did not close waits for the whole of
// `neon.pageserver_response_disconnect_timeout` (150 s by default, read from a running compute) before it drops the connection and reconnects to the pageserver the hook's /configure has since written.
// The stalls work (2026-11-19) found that this is what stalls reads for 135 to 153 s after a pageserver's loss, repoint or no repoint. PS_RESPONSE_DISCONNECT_MS (unset: Neon's default) sets it in the spec.
const PS_RESPONSE_DISCONNECT_MS = process.env['PS_RESPONSE_DISCONNECT_MS']
// A backend that reconnects after that wait dials the address the spec held when it started; to a power-cut host a connect() hangs for the kernel's SYN retries (about 127 s) and no connect timeout applies
// (libpagestore uses PQconnectStartParams and its own wait loop). libpq's `tcp_user_timeout` (ms) is a socket option set before connect(), so it bounds that dial. PS_CONN_OPTS (unset: none) is appended
// to the pageserver connection string, e.g. "?tcp_user_timeout=5000".
const PS_CONN_OPTS = process.env['PS_CONN_OPTS'] ?? ''
const psConnstr = (host: string) => `postgresql://no_user@${host}:6400${PS_CONN_OPTS}`

function computeSpec(d: Db, password: string, pageserver: string, sk: { members: string[]; generation: number }, jwk: any) {
  const conf = [...BASE_CONF]
  if (PS_RESPONSE_DISCONNECT_MS) conf.push(`neon.pageserver_response_disconnect_timeout=${PS_RESPONSE_DISCONNECT_MS}`)
  if (d.mode === 'primary') conf.push(...PRIMARY_CONF)
  else {
    // a replica streams from the safekeepers (neon_local's Replica configuration)
    const hosts = sk.members.map((m) => m.split(':')[0]).join(',')
    const ports = sk.members.map((m) => m.split(':')[1]).join(',')
    conf.push(`primary_conninfo='host=${hosts} port=${ports} options=''-c timeline_id=${d.timeline} tenant_id=${d.tenant}'' application_name=replica replication=true'`, `primary_slot_name='repl_${d.timeline}_'`, 'hot_standby=on', 'recovery_prefetch=off')
  }
  return {
    spec: {
      format_version: 1.0, suspend_timeout_seconds: -1, skip_pg_catalog_updates: false,
      cluster: { roles: [{ name: 'app', encrypted_password: scram(password), options: null }], databases: [{ name: 'app', owner: 'app', options: null, restrict_conn: false, invalid: false }], settings: null, postgresql_conf: conf.join('\n') + '\n' },
      tenant_id: d.tenant, timeline_id: d.timeline, endpoint_id: `peat-${d.name}`.slice(0, 40),
      mode: d.mode === 'primary' ? 'Primary' : 'Replica',
      pageserver_connstring: psConnstr(pageserver),
      safekeeper_connstrings: d.mode === 'primary' ? sk.members : [],
      safekeepers_generation: d.mode === 'primary' ? sk.generation : null,
      storage_auth_token: tenantToken(d.tenant), shard_stripe_size: 2048,
    },
    compute_ctl_config: { jwks: { keys: [jwk] } },
  }
}

// ---- the key compute_ctl trusts for its /configure (an Ed25519 JWK in the spec's compute_ctl_config; a token with scope compute_ctl:admin and audience compute). One key per platform, kept in a Secret.
let jwtKey: { jwk: any; privatePem: string } | undefined
async function jwtKeyPair() {
  if (jwtKey) return jwtKey
  const got = await kube('GET', `/api/v1/namespaces/${NS}/secrets/peat-compute-jwt`)
  if (got.status === 200) {
    jwtKey = { jwk: JSON.parse(Buffer.from(got.json.data.jwk, 'base64').toString()), privatePem: Buffer.from(got.json.data.private, 'base64').toString() }
    return jwtKey
  }
  const { publicKey, privateKey } = generateKeyPairSync('ed25519')
  const jwk = { ...(publicKey.export({ format: 'jwk' }) as any), kid: 'peat', use: 'sig', alg: 'EdDSA' }
  const privatePem = privateKey.export({ format: 'pem', type: 'pkcs8' }) as string
  const made = await kube('POST', `/api/v1/namespaces/${NS}/secrets`, { apiVersion: 'v1', kind: 'Secret', metadata: { name: 'peat-compute-jwt', namespace: NS }, stringData: { jwk: JSON.stringify(jwk), private: privatePem } })
  if (made.status === 409) return jwtKeyPair() // another call made it first
  ok(made, 'make jwt key')
  jwtKey = { jwk, privatePem }
  return jwtKey
}
const b64u = (b: Buffer | string) => Buffer.from(b).toString('base64url')
async function adminToken(): Promise<string> {
  const k = await jwtKeyPair()
  const head = b64u(JSON.stringify({ alg: 'EdDSA', typ: 'JWT', kid: 'peat' }))
  const body = b64u(JSON.stringify({ scope: 'compute_ctl:admin', aud: ['compute'], exp: Math.floor(Date.now() / 1000) + 300 }))
  const sig = edSign(null, Buffer.from(`${head}.${body}`), createPrivateKey(k.privatePem))
  return `${head}.${body}.${b64u(sig)}`
}

// ---- a compute is a Deployment of one pod
// A bare pod is not enough: when its machine was power cut the pod stayed `Unknown` and nothing replaced it (13 to 27 minutes in the first drills, until a hand repair). So the owner is a Deployment
// `compute-<name>` (replicas 1 awake, 0 asleep), so Kubernetes makes the pod again, and the reconciler below treats a compute that is unknown, stuck or on a dead machine as gone and replaces it faster than
// Kubernetes does. The spec each pod reads is the Secret `compute-<name>`, kept current by `refreshSpec` before every replacement, so a pod made after a pageserver moved reads the new address.
const computeSelector = (name: string) => `app=peat-compute,compute=${name}`
const depPath = (name: string) => `/apis/apps/v1/namespaces/${CNS}/deployments/compute-${name}`
const STUCK_S = Number(process.env['STUCK_S'] ?? '120') // a compute pod not Ready this long after it was created is stuck
const START_GRACE_S = Number(process.env['START_GRACE_S'] ?? '20') // a young pod is not probed
const PROBE_EVERY_MS = Number(process.env['PROBE_EVERY_MS'] ?? '2000')
const DEAD_AFTER = Number(process.env['DEAD_AFTER'] ?? '3') // consecutive failed probes before a machine or a pod counts as down
const LIVE_NODE_UNREACHABLE_AFTER = Number(process.env['LIVE_NODE_UNREACHABLE_AFTER'] ?? '8') // a pod that answers no probe on a machine that does answer: replaced after this many probes
const DEAD_TAINT = NAMES.deadTaint.key
/** The newest compute pod of a database that is not on its way out, or null. */
async function livePod(name: string): Promise<any | null> {
  const r = ok(await kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=${encodeURIComponent(computeSelector(name))}`), 'list pods')
  const pods = (r.json.items as any[]).filter((p) => !p.metadata.deletionTimestamp).sort((a, b) => (a.metadata.creationTimestamp < b.metadata.creationTimestamp ? 1 : -1))
  return pods[0] ?? null
}
const podReady = (p: any): boolean => !!p?.status?.conditions?.some((c: any) => c.type === 'Ready' && c.status === 'True')

/** The compute's spec as it should be now, written into its Secret (create or replace). */
async function writeSpec(d: Db): Promise<void> {
  const pw = Buffer.from(ok(await kube('GET', `/api/v1/namespaces/${NS}/secrets/db-${d.root ? d.name : rootOf(d)}-login`), 'read login').json.data.password, 'base64').toString()
  const node = await attachedNode(d.tenant)
  const sk = await safekeepersOf(d.tenant, d.timeline)
  const cfg = computeSpec(d, pw, pageserverHost(node), sk, (await jwtKeyPair()).jwk)
  await apply(`/api/v1/namespaces/${CNS}/secrets`, `compute-${d.name}`, { apiVersion: 'v1', kind: 'Secret', metadata: { name: `compute-${d.name}`, namespace: CNS, labels: { 'peat-compute-spec': 'true' } }, stringData: { 'config.json': JSON.stringify(cfg) } })
}
/** Keep the stored spec's pageserver and safekeepers current without touching its login hash: for a compute about to be made again, or one that is not running while the storage moved. */
async function refreshSpec(d: Db): Promise<void> {
  const got = await kube('GET', `/api/v1/namespaces/${CNS}/secrets/compute-${d.name}`)
  if (got.status !== 200) return writeSpec(d)
  const cfg = JSON.parse(Buffer.from(got.json.data['config.json'], 'base64').toString())
  cfg.spec.pageserver_connstring = psConnstr(pageserverHost(await attachedNode(d.tenant)))
  cfg.spec.storage_auth_token = tenantToken(d.tenant)
  if (d.mode === 'primary') {
    const sk = await safekeepersOf(d.tenant, d.timeline)
    cfg.spec.safekeeper_connstrings = sk.members
    cfg.spec.safekeepers_generation = sk.generation
  }
  await kube('PATCH', `/api/v1/namespaces/${CNS}/secrets/compute-${d.name}`, { stringData: { 'config.json': JSON.stringify(cfg) } }, 'application/merge-patch+json')
}

const computePodTemplate = (d: Db) => {
  const labels = { app: 'peat-compute', compute: d.name, tenant: d.tenant }
  return {
    metadata: { labels },
    spec: {
      restartPolicy: 'Always', terminationGracePeriodSeconds: 10, enableServiceLinks: false,
      // a compute runs a tenant's SQL: it gets no service account token, and only the platform's machines (the nodeSelector and the toleration of the platform's taint; admission rule 10 refuses a compute without the
      // selector)
      automountServiceAccountToken: false,
      securityContext: { fsGroup: 1000 }, // the postgres user's group: the key file is root-owned with group read, as Postgres allows
      nodeSelector: PLACEMENT.nodeSelector,
      // Kubernetes adds a 300 s toleration of an unreachable or not-ready machine to every pod, which would keep a compute on a dead machine for five minutes before it is replaced
      tolerations: [
        ...PLACEMENT.tolerations,
        { key: 'node.kubernetes.io/unreachable', operator: 'Exists', effect: 'NoExecute', tolerationSeconds: 5 },
        { key: 'node.kubernetes.io/not-ready', operator: 'Exists', effect: 'NoExecute', tolerationSeconds: 5 },
      ],
      containers: [{
        name: 'compute', image: computeImageFor(d.pg, COMPUTE_IMAGES),
        // compute_ctl is run by a small shell. When compute_ctl exits on its own (a crash, or a fence by a newer compute) the shell asks the control layer whether this pod is still the current compute of its database
        // (`GET /compute/current`, 3 s at most). Yes: it exits, and the pod restarts the container in place (a crash costs a restart, not a replacement). No, or no answer: it stays up and not Ready and never starts
        // compute_ctl again, because a restarted compute re-elects itself at a still higher term and fences the compute that replaced it (seen 2026-11-19, the fence drill: three rounds in 75 s). A pod cut off from the control
        // layer cannot get a yes, so it stays down: the safe side. A pod told to stop (SIGTERM) passes it on and exits.
        command: ['/bin/sh', '-c', `term=; trap 'term=1; kill -TERM $p 2>/dev/null' TERM INT; /usr/local/bin/compute_ctl "$@" & p=$!; wait $p; wait $p; rc=$?; [ -n "$term" ] && exit $rc; if timeout 3 bash -c 'exec 3<>/dev/tcp/peat-control.${NS}.svc/8080 && printf "GET /compute/current?name=%s&pod=%s HTTP/1.0\\r\\nHost: peat-control\\r\\n\\r\\n" "$0" "$1" >&3 && grep -q "\\"current\\":true" <&3' "$COMPUTE_NAME" "$POD_NAME"; then echo "compute_ctl exited ($rc): this pod is still the current compute: restarting in place"; exit $rc; fi; echo "compute_ctl exited ($rc): not the current compute, or the control layer did not answer: not started again here; the control layer replaces this pod"; while :; do sleep 3600 & wait $!; done`, 'compute'],
        args: ['--pgdata=/var/db/postgres/compute', '--connstr=postgresql://cloud_admin@localhost:5432/postgres', '--config=/config/config.json', '--pgbin=/usr/local/bin/postgres', `--compute-id=${d.name}`, '--dev'],
        env: [{ name: 'OTEL_SDK_DISABLED', value: 'true' }, { name: 'COMPUTE_NAME', value: d.name }, { name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } }],
        ports: [{ containerPort: 5432 }, { containerPort: 3080 }],
        resources: { requests: { cpu: '250m', memory: COMPUTE_MEMORY_REQUEST }, limits: { memory: COMPUTE_MEMORY_LIMIT } },
        // Ready means the login works: compute_ctl starts Postgres first and creates the spec's role and database a little later (longer under load), and a pod that is only listening answers a client with
        // "password authentication failed" (found 2026-11-17, making databases in batches of ten); so the probe asks Postgres whether the role and database exist (an exec probe judges only the exit code, so the answer is piped to grep: the first version of it passed on an empty answer). A tcp probe on 5432 was the first version.
        readinessProbe: { exec: { command: ['sh', '-c', `psql -h 127.0.0.1 -U cloud_admin -d postgres -Atc "select 1 from pg_roles r, pg_database d where r.rolname = 'app' and r.rolpassword is not null and d.datname = 'app'" | grep -q 1`] }, periodSeconds: 1, timeoutSeconds: 3 },
        volumeMounts: [{ name: 'config', mountPath: '/config' }, { name: 'pgdata', mountPath: '/var/db/postgres' }, { name: 'tls', mountPath: '/tls', readOnly: true }, { name: 'hba', mountPath: '/hba', readOnly: true }],
      }],
      volumes: [
        { name: 'config', secret: { secretName: `compute-${d.name}` } },
        { name: 'pgdata', emptyDir: { sizeLimit: PGDATA_LIMIT } }, // a tenant's temporary files must not fill the machine's 30 GiB system disk (the pod is evicted at the limit)
        { name: 'tls', secret: { secretName: tlsSecret(d.name), defaultMode: 0o440 } },
        { name: 'hba', configMap: { name: 'peat-pg-hba' } },
      ],
    },
  }
}

// ---- a database's name and certificate
// A client dials `<db>.<database namespace>.svc` (or `<db>.<DB_DOMAIN>` from outside): an ExternalName Service that leads to the gateways'
// one front door (`pg-front`), which learns the database from the TLS server name. kube-proxy programs nothing for an ExternalName Service
// (it is a DNS answer), so a thousand databases do not add two thousand Services to every machine's rules. The compute's certificate names
// the database's names and is signed by the platform's authority: by a cert-manager issuer when one is named (a Certificate, which fills the
// Secret `tls-<db>`), or by this layer itself (it writes the Secret, and renews it 30 days before it expires). The compute mounts the Secret;
// both go with the database.
const tlsSecret = (name: string) => `tls-${name}`
const dnsNamesOf = (name: string) => [`${name}.${DB_NS}.svc`, `${name}.${DB_NS}.svc.cluster.local`, ...(DB_DOMAIN ? [`${name}.${DB_DOMAIN}`] : [])]
const certificate = (name: string) => {
  const [kind, issuer] = CERT_ISSUER.includes('/') ? CERT_ISSUER.split('/', 2) : ['Issuer', CERT_ISSUER]
  return {
    apiVersion: 'cert-manager.io/v1', kind: 'Certificate',
    metadata: { name: tlsSecret(name), namespace: CNS, labels: { 'peat-db-cert': 'true' } },
    spec: {
      secretName: tlsSecret(name), issuerRef: { name: issuer, kind },
      dnsNames: dnsNamesOf(name),
      duration: '8760h', renewBefore: '720h', privateKey: { algorithm: 'ECDSA', size: 256 }, usages: ['digital signature', 'key encipherment', 'server auth'],
    },
  }
}
const RENEW_BEFORE_MS = 30 * 86_400_000
let caPair: { certPem: string; keyPem: string } | undefined
const ca = () => (caPair ??= { certPem: readFileSync(`${CA_DIR}/tls.crt`, 'utf8'), keyPem: readFileSync(`${CA_DIR}/tls.key`, 'utf8') })
/** The database's certificate exists and is not about to expire: made once by the issuer, or signed here and renewed here. */
async function ensureCertificate(name: string): Promise<void> {
  if (CERT_ISSUER) {
    // created once and left to cert-manager to renew: a replace of a custom resource needs its resourceVersion (found 2026-11-19, the first wake: 422), so an existing one is not touched
    ok(await kube('POST', `/apis/cert-manager.io/v1/namespaces/${CNS}/certificates`, certificate(name)), `create certificate ${tlsSecret(name)}`, [409])
    return
  }
  const got = await kube('GET', `/api/v1/namespaces/${CNS}/secrets/${tlsSecret(name)}`)
  if (got.status === 200) {
    const notAfter = (() => { try { return Date.parse(new X509Certificate(Buffer.from(String(got.json?.data?.['tls.crt'] ?? ''), 'base64').toString()).validTo) } catch { return 0 } })()
    if (notAfter - Date.now() > RENEW_BEFORE_MS) return
  }
  const leaf = newLeaf(ca(), dnsNamesOf(name))
  await apply(`/api/v1/namespaces/${CNS}/secrets`, tlsSecret(name), { apiVersion: 'v1', kind: 'Secret', type: 'kubernetes.io/tls', metadata: { name: tlsSecret(name), namespace: CNS, labels: { 'peat-db-cert': 'true' } }, stringData: { 'tls.crt': leaf.certPem, 'tls.key': leaf.keyPem, 'ca.crt': ca().certPem } })
  if (got.status === 200) console.log(`cert: ${name}'s certificate renewed`)
}
/** Every database's own certificate renewed in time (this layer signs them when no issuer does): the acting layer, once an hour. */
async function renewOwnCerts() {
  if (!isLeader || CERT_ISSUER) return
  for (const d of await listDbs()) await ensureCertificate(d.name).catch((e) => console.log(`cert ${d.name}: ${e}`)) // a replica's compute has a certificate of its own too
}
const nameService = (name: string) => ({
  apiVersion: 'v1', kind: 'Service',
  metadata: { name, namespace: DB_NS, labels: { 'peat-route': 'true' } },
  spec: { type: 'ExternalName', externalName: FRONT_DOOR },
})

/** A start refused because the database is closed (its object, read past every cache, says so). */
class ClosedDb extends Error {}
/**
 * Make the database's compute exist: the spec Secret current, the Service, and the Deployment at one replica (made if it is not there).
 * Returns the pod's name when a pod already exists, else the Deployment's. A closed database is refused, except for the platform's own
 * one-off start (`evenClosed`: a safekeeper replacement's first start of a timeline never written, which no client reaches).
 */
async function startCompute(d: Db, opts: { evenClosed?: boolean } = {}): Promise<string> {
  const existing = await livePod(d.name)
  const dep = await kube('GET', depPath(d.name))
  if (existing && dep.status === 200 && dep.json.spec.replicas === 1) return existing.metadata.name
  if (!opts.evenClosed && (await freshDb(d))?.closed) throw new ClosedDb(`${d.name} is closed: it is opened before anything connects`)
  // created once and left to cert-manager to renew: `apply` replaces on a conflict, and a replace of a custom resource needs its resourceVersion (found 2026-11-19, the first wake: 422), so an existing one is not touched
  await ensureCertificate(d.name)
  await writeSpec(d)
  if (dep.status === 200) ok(await kube('PATCH', depPath(d.name), { spec: { replicas: 1 } }, 'application/merge-patch+json'), `scale compute-${d.name} up`)
  else {
    ok(await kube('POST', `/apis/apps/v1/namespaces/${CNS}/deployments`, {
      apiVersion: 'apps/v1', kind: 'Deployment',
      // `peat-pg` is the database's Postgres version: a move of every Deployment to a new compute image selects by it (`-l peat-compute-deploy,peat-pg!=19`), so that one version's image is never set on the other's databases
      metadata: { name: `compute-${d.name}`, namespace: CNS, labels: { 'peat-compute-deploy': 'true', 'peat-pg': String(d.pg), compute: d.name } },
      spec: { replicas: 1, revisionHistoryLimit: 0, selector: { matchLabels: { app: 'peat-compute', compute: d.name } }, strategy: { type: 'RollingUpdate', rollingUpdate: { maxSurge: 1, maxUnavailable: 1 } }, template: computePodTemplate(d) },
    }), `create deployment compute-${d.name}`)
  }
  markAwake(d, true)
  return `compute-${d.name}`
}
/**
 * A start that says whether the machines had room (the wake path's route, `POST /db/<name>/start`). The look is bounded by the request's own clock: the gateway calls this with a 10 s limit, so the look ends at
 * `START_LOOK_S` after the request began (default 6) and never later, and a `startCompute` that was itself slow leaves no look at all: the start then answers as it always did. Refused only when the pod was
 * Unschedulable at every look for at least `REFUSE_AFTER_LOOKS` in a row, up to the end of the look, and no compute pod is on its way out anywhere (a stopping compute frees its memory within seconds, and the
 * wake the old code held 120 s for would have been placed): otherwise the start goes on and the gateway holds its callers as before. A refusal puts the compute back to sleep (a Pending pod left alone would be replaced as
 * "stuck" every `STUCK_S`, and keeps asking for room) after reading the pod once more, so that two gateways waking one database cannot stop a pod the other saw placed. The next client asks again. The decision is `startVerdict`.
 */
const START_LOOK_S = Number(process.env['START_LOOK_S'] ?? '6')
const START_HARD_S = Number(process.env['START_HARD_S'] ?? '8') // nothing inside the look runs past this, measured from the request: the gateway gives up at 10 s
const STOP_RESERVE_MS = 1500 // a refusal is begun only with this much of the hard limit left
const REFUSE_AFTER_LOOKS = Number(process.env['REFUSE_AFTER_LOOKS'] ?? '3')
const LOOK_EVERY_MS = 500
class LookOver extends Error {}
/** The compute is already started when the look begins, so the look may only ever improve the answer: any error in it (an API 5xx or 429 through `ok()`, a read that outlives the hard limit) is answered as the old success. */
async function startPlaced(d: Db, requestAt: number): Promise<{ pod: string } | { refused: string }> {
  const name = await startCompute(d)
  try {
    return await look(d, name, requestAt)
  } catch (e) {
    if (!(e instanceof LookOver)) { bump('start_look_failed_total'); console.log(`start ${d.name}: the look failed (${e}); answered as started`) }
    return { pod: name }
  }
}
async function look(d: Db, name: string, requestAt: number): Promise<{ pod: string } | { refused: string }> {
  const lookEnds = requestAt + START_LOOK_S * 1000
  const hardEnds = requestAt + START_HARD_S * 1000
  const bounded = async <T>(work: Promise<T>): Promise<T> => {
    const r = await within(hardEnds - Date.now(), work)
    if (!r.done) throw new LookOver()
    return r.value
  }
  const allComputes = async (): Promise<any[]> => (await bounded(kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=app%3Dpeat-compute`)).then((r) => ok(r, 'list pods'))).json.items as any[]
  let refusedLooks = 0
  while (Date.now() < lookEnds) {
    const pods = await allComputes()
    const mine = pods.filter((p) => p.metadata?.labels?.compute === d.name)
    const v = startVerdict({ mine, pods, refusedLooks, lookEnded: Date.now() + LOOK_EVERY_MS >= lookEnds, refuseAfter: REFUSE_AFTER_LOOKS })
    refusedLooks = v.refusedLooks
    if (v.kind === 'placed') return { pod: name }
    if (v.kind === 'refused') {
      if (hardEnds - Date.now() < STOP_RESERVE_MS) return { pod: name }
      const latest = await allComputes()
      const again = startVerdict({ mine: latest.filter((p) => p.metadata?.labels?.compute === d.name), pods: latest, refusedLooks: REFUSE_AFTER_LOOKS, lookEnded: true, refuseAfter: REFUSE_AFTER_LOOKS })
      if (again.kind !== 'refused') return { pod: name }
      if (hardEnds - Date.now() < STOP_RESERVE_MS) return { pod: name }
      await bounded(stopCompute(d.name))
      bump('start_refused_total')
      console.log(`start ${d.name}: refused, no machine has room (${v.reason}); put back to sleep`)
      return { refused: v.reason }
    }
    await new Promise((res) => setTimeout(res, LOOK_EVERY_MS))
  }
  return { pod: name } // not decided: the start goes on and the gateway holds its callers as before
}
const rootOf = (d: Db): string => d.parent.split('/')[0] === '' ? d.name : d.parent // a branch's login is its root's; parent holds the root's name for a branch of a branch too (see makeBranch)

async function stopCompute(name: string, waitGone = false): Promise<void> {
  // scale to zero first (so nothing makes the pod again), then delete what is left (a pod on a machine that is gone would otherwise sit `Terminating` for as long as the machine is off)
  const sc = await kube('PATCH', depPath(name), { spec: { replicas: 0 } }, 'application/merge-patch+json')
  if (sc.status >= 300 && sc.status !== 404) ok(sc, `scale compute-${name} down`)
  const r = ok(await kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=${encodeURIComponent(computeSelector(name))}`), 'list pods')
  for (const p of r.json.items as any[]) await kube('DELETE', `/api/v1/namespaces/${CNS}/pods/${p.metadata.name}`, { gracePeriodSeconds: 5 })
  void getDb(name).then((d) => { if (d) markAwake(d, false) }, () => {})
  // a sleeping database has no compute Service: every Service is a rule on every machine's kube-proxy, and 2,000 of them (a route and a compute Service for each of 1,001 databases) made a Service's endpoint change
  // take 45 to 60 s to reach a client (seen 2026-11-19, the compute-worker cut: the pod and the gateway's own address answered at +13 to +27 s, the Service at +44 to +62 s). It is made again by the next start.
  // waitGone: the caller is about to delete the compute's timeline. A compute that is still shutting down keeps its walproposer reconnecting to the safekeepers, which answer "timeline not found" (3 ERROR lines on the
  // safekeepers per deleted compute, found 2026-11-18: an earlier version deleted the timeline right after asking the pod to go). So wait until the pod is gone (at most 20 s) before the timeline is deleted.
  if (!waitGone) return
  for (let i = 0; i < 40; i++) {
    const left = ok(await kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=${encodeURIComponent(computeSelector(name))}`), 'list pods').json.items as any[]
    if (left.length === 0) return
    await new Promise((res) => setTimeout(res, 500))
  }
}

// ---- the reconciler: a compute that is unknown, stuck or on a dead machine is gone, and is replaced
// Two signals, both ours. (1) Each worker's kubelet port is dialed every PROBE_EVERY_MS; DEAD_AFTER failures in a row mean the machine is dead, and it gets the dead taint (DEAD_TAINT, NoSchedule)
// so that nothing is made on it (removed after DEAD_AFTER answers). Kubernetes itself takes about 50 s to mark such a machine NotReady. (2) Each compute pod's Postgres port is dialed; a pod that answers no
// probe on a dead machine, or fails its status, or was never Ready, or has no owner or no database, is deleted with no grace period: the Deployment makes the next one, from a spec refreshed first.
// A control layer on the dead machine itself can do nothing: Kubernetes' own eviction (5 s toleration, after its 50 s) is the fallback.
interface NodeState { name: string; ip: string; fails: number; oks: number; dead: boolean; tainted?: boolean }
const nodeStates = new Map<string, NodeState>()
let nodesRead = 0
const counters: Record<string, number> = {}
const bump = (k: string, by = 1) => { counters[k] = (counters[k] ?? 0) + by }
const taintNode = async (name: string, on: boolean) => {
  const n = ok(await kube('GET', `/api/v1/nodes/${name}`), 'read node').json
  if (n.metadata?.labels?.[PEAT_LABEL] !== PEAT_LABEL_VALUE) throw new Error(`${name} is not a platform machine: not tainted`)
  const taints = ((n.spec?.taints ?? []) as any[]).filter((t) => t.key !== DEAD_TAINT)
  if (on) taints.push({ key: DEAD_TAINT, value: NAMES.deadTaint.value, effect: NAMES.deadTaint.effect })
  await kube('PATCH', `/api/v1/nodes/${name}`, { spec: { taints } }, 'application/merge-patch+json')
}
async function probeNodes() {
  if (Date.now() - nodesRead > 10_000) {
    // ONLY the platform's machines: nodes are listed by their label, so the probes, the "half the machines at once" rule and the dead taint below cover them and no other node (the main node, the database machine, the CI and sandbox machines
    // are never listed, never probed and never tainted; the admission rule peat-control-keeps-to-peat-nodes refuses a change to any other node whatever this code does)
    const r = await kube('GET', `/api/v1/nodes?labelSelector=${encodeURIComponent(`${PEAT_LABEL}=${PEAT_LABEL_VALUE}`)}`)
    if (r.status === 200) {
      nodesRead = Date.now()
      for (const n of r.json.items as any[]) {
        if (n.metadata.labels?.[PEAT_LABEL] !== PEAT_LABEL_VALUE) continue
        const ip = (n.status.addresses as any[]).find((a) => a.type === 'InternalIP')?.address
        if (!ip) continue
        const s = nodeStates.get(n.metadata.name)
        if (s) s.ip = ip
        else nodeStates.set(n.metadata.name, { name: n.metadata.name, ip, fails: 0, oks: 0, dead: false })
      }
      const listed = new Set((r.json.items as any[]).map((n) => n.metadata.name as string))
      for (const k of [...nodeStates.keys()]) if (!listed.has(k)) nodeStates.delete(k)
    }
  }
  const all = [...nodeStates.values()]
  const answers = await Promise.all(all.map((s) => tcpUp(s.ip, 10250, 1500)))
  const failing = answers.filter((a) => !a).length
  for (let i = 0; i < all.length; i++) {
    const s = all[i]!
    if (answers[i]) { s.fails = 0; s.oks++ } else { s.oks = 0; s.fails++ }
    // "everything is down" is more likely this program's own network than every machine: nothing is declared dead when half or more of the machines fail at once
    if (!s.dead && s.fails >= DEAD_AFTER && failing * 2 < all.length) {
      s.dead = true
      console.log(`reconcile: machine ${s.name} (${s.ip}) is dead: ${s.fails} probes of its kubelet failed`)
      bump('machines_declared_dead_total')
    } else if (s.dead && s.oks >= DEAD_AFTER) {
      s.dead = false
      console.log(`reconcile: machine ${s.name} answers again`)
    }
    // only the acting control layer changes a machine's taints (it also repairs one a former leader left behind)
    if (isLeader && s.dead !== s.tainted) { s.tainted = s.dead; await taintNode(s.name, s.dead).catch((e) => { s.tainted = !s.dead; console.log(`taint ${s.name}: ${e}`) }) }
  }
}

const podFails = new Map<string, number>() // pod uid -> consecutive failed probes of its Postgres port
async function replaceCompute(p: any, d: Db | null, reason: string) {
  const name = p.metadata.labels?.compute as string
  console.log(`reconcile: replacing compute pod ${p.metadata.name} of ${name} on ${p.spec?.nodeName ?? '?'}: ${reason}`)
  bump('compute_replacements_total')
  if (d) await refreshSpec(d).catch((e) => console.log(`refresh spec ${name}: ${e}`))
  await kube('DELETE', `/api/v1/namespaces/${CNS}/pods/${p.metadata.name}`, { gracePeriodSeconds: 0 })
  podFails.delete(p.metadata.uid)
}
async function reconcileComputes() {
  if (!isLeader) return
  await reapHaPods()
  const pods = ok(await kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=app%3Dpeat-compute`), 'list compute pods').json.items as any[]
  // a database is known by its object's status or by the platform's record of it: a status wiped, or a switch before the copy has made
  // the Database objects, leaves the record, and a serving compute is not killed for that
  const dbs = new Map((await listDbs()).map((d) => [d.name, d]))
  const recs = await readRecords(RECORDS)
  const live = new Set<string>()
  await Promise.all(pods.map(async (p) => {
    if (p.metadata.deletionTimestamp && !(p.spec?.nodeName && nodeStates.get(p.spec.nodeName)?.dead)) return
    live.add(p.metadata.uid)
    const name = p.metadata.labels?.compute as string
    const d = knownDb(name, dbs, recs)
    const age = (Date.now() - Date.parse(p.metadata.creationTimestamp)) / 1000
    try {
      if (!d) return await replaceCompute(p, null, 'unknown: no database record')
      if (!p.metadata.ownerReferences?.length) return await replaceCompute(p, d, 'unknown: no owner (a bare pod nothing would replace)')
      const phase = p.status?.phase
      if (phase === 'Failed' || phase === 'Succeeded' || phase === 'Unknown' || p.status?.reason === 'Evicted' || p.status?.reason === 'NodeLost') return await replaceCompute(p, d, `unknown: phase ${phase} ${p.status?.reason ?? ''}`)
      const node = nodeStates.get(p.spec?.nodeName)
      if (!podReady(p) && age > STUCK_S) return await replaceCompute(p, d, `stuck: not Ready ${Math.round(age)} s after it was made`)
      if (age <= START_GRACE_S || !p.status?.podIP) return
      const up = await tcpUp(p.status.podIP, 5432, 1500)
      const fails = up ? 0 : (podFails.get(p.metadata.uid) ?? 0) + 1
      podFails.set(p.metadata.uid, fails)
      if (fails >= 1 && node?.dead) return await replaceCompute(p, d, `dead machine ${node.name}: its Postgres port does not answer`)
      if (fails >= LIVE_NODE_UNREACHABLE_AFTER) return await replaceCompute(p, d, `unreachable: ${fails} failed probes of its Postgres port on a machine that answers`)
    } catch (e) { console.log(`reconcile ${p.metadata.name}: ${e}`) }
  }))
  for (const uid of podFails.keys()) if (!live.has(uid)) podFails.delete(uid)
}
// ---- a running compute's certificate: Postgres reads ssl_cert_file only at start or on a reload, and nothing else asks it to (storage-spec.ts, `certNeedsReload`). Every five minutes the acting layer looks at each awake
// compute's `tls-<name>` Secret; one that changed after its pod started (cert-manager renewed it) and has had five minutes to reach the pod's mounted file gets its stored spec posted to compute_ctl's /configure, which
// reloads Postgres (no restart, no dropped connection: the same call the compute hook makes for a moved pageserver, here with nothing changed but the file). A failure is logged and tried again at the next look; it
// NEVER replaces the compute (unlike the hook, a reload that does not work must not restart a database that is serving).
const certReloaded = new Map<string, string>() // pod uid -> the serial of the certificate its last reload was made for
async function reloadRenewedCerts() {
  if (!isLeader) return
  const pods = ok(await kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=app%3Dpeat-compute`), 'list compute pods').json.items as any[]
  const live = new Set<string>()
  for (const p of pods) {
    if (p.metadata.deletionTimestamp || !podReady(p) || !p.status?.podIP) continue
    live.add(p.metadata.uid)
    const name = p.metadata.labels?.compute as string
    const sec = await kube('GET', `/api/v1/namespaces/${CNS}/secrets/${tlsSecret(name)}`)
    if (sec.status !== 200) continue
    // the certificate itself (its serial and its issue time), against when the compute's CONTAINER started (after its volume was mounted): a Secret written for another reason, or after the pod was admitted, is no new certificate
    const cert = certInfo(Buffer.from(String(sec.json?.data?.['tls.crt'] ?? ''), 'base64').toString())
    const startedAt = Date.parse(String((p.status?.containerStatuses ?? []).find((c: any) => c.name === 'compute')?.state?.running?.startedAt ?? ''))
    if (!cert || !certNeedsReload({ notBeforeMs: cert.notBeforeMs, startedAtMs: startedAt, nowMs: Date.now(), serial: cert.serial, reloadedFor: certReloaded.get(p.metadata.uid), settleMs: 300_000 })) continue
    try {
      const d = await getDb(name)
      if (!d) continue
      await refreshSpec(d)
      const cfg = JSON.parse(Buffer.from(ok(await kube('GET', `/api/v1/namespaces/${CNS}/secrets/compute-${name}`), 'read spec').json.data['config.json'], 'base64').toString())
      const r = await fetch(`http://${p.status.podIP}:3080/configure`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${await adminToken()}` }, body: JSON.stringify(cfg), signal: AbortSignal.timeout(20_000) })
      if (!r.ok) { console.log(`cert-reload ${name}: configure answered ${r.status} ${(await r.text()).slice(0, 160)}: tried again at the next look`); bump('cert_reload_failures_total'); continue }
      certReloaded.set(p.metadata.uid, cert.serial)
      bump('cert_reloads_total')
      console.log(`cert-reload: ${name} (${p.metadata.name}) asked to reload: its certificate (serial ${cert.serial}) was issued at ${new Date(cert.notBeforeMs).toISOString()}, after its container started`)
    } catch (e) { console.log(`cert-reload ${name}: ${e}: tried again at the next look`); bump('cert_reload_failures_total') }
  }
  for (const uid of certReloaded.keys()) if (!live.has(uid)) certReloaded.delete(uid)
}
// ---- how long each storage node has been warming up: the storage controller does not fail a warming-up node's tenants over until its grace ends, and exposes no gauge for it, so the acting control layer
// reads /control/v1/node and exports one (`peat_control_node_warming_up_seconds{node}`, 0 when the node is not warming up) for the alert rule.
const warmingSince = new Map<string, number>()
const nodeWarming: Record<string, number> = {}
async function pollStorageNodes() {
  if (!isLeader) { for (const k of Object.keys(nodeWarming)) delete nodeWarming[k]; return }
  const r = await storcon('GET', '/control/v1/node')
  if (r.status !== 200 || !Array.isArray(r.json)) return
  for (const n of r.json as any[]) {
    const id = String(n.id)
    const a = typeof n.availability === 'string' ? n.availability : JSON.stringify(n.availability)
    if (/warming/i.test(a)) { if (!warmingSince.has(id)) warmingSince.set(id, Date.now()); nodeWarming[id] = Math.round((Date.now() - warmingSince.get(id)!) / 1000) }
    else { warmingSince.delete(id); nodeWarming[id] = 0 }
  }
  // tenants the controller believes are attached to a node and that the node does not hold (found 2026-11-19, the disk-loss drill: one tenant directory wiped on a pageserver that kept its others; the controller's disk-loss
  // handling acts only when NO tenant is found locally, so that tenant stayed attached on paper and absent on the pageserver, with no metric of anyone's moving)
  const tenants = await storcon('GET', '/control/v1/tenant')
  if (tenants.status !== 200 || !Array.isArray(tenants.json)) return
  const wanted = new Map<number, string[]>()
  for (const t of tenants.json as any[]) { const n = t.shards?.[0]?.node_attached; if (n) wanted.set(n, [...(wanted.get(n) ?? []), t.tenant_id as string]) }
  for (const [node, ids] of wanted) {
    try {
      const r = await fetch(`http://${pageserverHost(node)}:9898/v1/tenant`, { headers: scoped('pageserverapi'), signal: AbortSignal.timeout(4000) })
      if (!r.ok) continue
      const held = new Set(((await r.json()) as any[]).map((x) => x.id as string))
      const missing = ids.filter((id) => !held.has(id))
      missingAttached[String(node)] = missing.length
      for (const id of missing) { if (!missingSince.has(id)) missingSince.set(id, Date.now()) }
      for (const id of ids) if (held.has(id)) missingSince.delete(id)
    } catch { /* an unreachable pageserver is the controller's heartbeat to judge */ }
  }
  // a tenant missing from the pageserver it is attached to for RECOVER_AFTER_S is attached again: the controller is told to detach it and attach it as before (its placement policy is read first and
  // put back), which makes the pageserver fetch it from the object store again; the compute hook follows as for any attach. A cool-down keeps a tenant that stays missing from being flipped in a loop.
  // Only a PARTIAL loss: when more than a few tenants are missing from one node it is the node's event (a restart that has not finished loading its tenants, or a whole disk lost: the controller's own handling),
  // and flipping hundreds of tenants at once overloaded the controller in the first version of this (2026-11-19: 16 flipped during a pageserver's restart, heartbeats timed out). At most one recovery per 2 minutes.
  const worst = Math.max(0, ...Object.values(missingAttached))
  if (worst > MAX_PARTIAL_MISSING) return
  for (const [id, since] of [...missingSince]) {
    if (Date.now() - lastRecovery < 120_000) break
    if (Date.now() - since < RECOVER_AFTER_S * 1000 || Date.now() - (recoveredAt.get(id) ?? 0) < 600_000) continue
    lastRecovery = Date.now()
    recoveredAt.set(id, Date.now()); missingSince.delete(id)
    try {
      const d = await storcon('GET', `/control/v1/tenant/${id}`)
      const policy = d.json?.policy
      if (d.status !== 200 || !policy || policy === 'Detached') continue
      console.log(`recover: tenant ${id} is attached on the controller and absent on its pageserver for ${Math.round((Date.now() - since) / 1000)} s: attaching it again (policy ${JSON.stringify(policy)})`)
      bump('partial_loss_recoveries_total')
      ok(await storcon('PUT', `/control/v1/tenant/${id}/policy`, { placement: 'Detached' }), 'detach')
      await new Promise((r) => setTimeout(r, 5000))
      ok(await storcon('PUT', `/control/v1/tenant/${id}/policy`, { placement: policy }), 'attach again')
    } catch (e) { console.log(`recover ${id}: ${e}`) }
  }
}
const missingSince = new Map<string, number>()
const recoveredAt = new Map<string, number>()
const RECOVER_AFTER_S = Number(process.env['RECOVER_AFTER_S'] ?? '60')
const MAX_PARTIAL_MISSING = Number(process.env['MAX_PARTIAL_MISSING'] ?? '3')
let lastRecovery = 0
const missingAttached: Record<string, number> = {}

// ---- WAL in the object store is trimmed by position
// A safekeeper uploads each completed 16 MiB WAL segment (and partial ones) and, in Neon's open code, nothing ever deletes them but the timeline's deletion. What may still be read from them was read in the source
// (safekeeper/src/wal_storage.rs): a reader whose position is BELOW the safekeeper's local start reads the segment from the object store (a pageserver or a lagging peer asking for old WAL); a timeline evicted from a
// safekeeper is brought back from its last partial segment; `pull_timeline` and `copy_timeline` copy segments at the timeline's tail. The safekeeper's own local removal horizon is the least of what the pageserver has
// persisted (`remote_consistent_lsn`), what the peers have (`peer_horizon_lsn`) and what was backed up (`backup_lsn`): nothing below that is wanted by the pageserver or a peer. So this job deletes, per timeline, the
// full-segment objects wholly below the LEAST of every safekeeper's remote_consistent_lsn and peer_horizon_lsn and the attached pageserver's own remote_consistent_lsn, keeping the whole segment that holds that
// position (the margin) and never the partial segments; and only when all three safekeepers and the attached pageserver answered, and the same floor (or a lower one) was read at the previous look (two looks, a
// cycle apart: a number that is not stable is not acted on). Acting control layer only.
// Cadence (a 30 s cycle over 150 timelines would be about 13 million listings a month, which a store charges for): once an HOUR, every timeline, and a timeline's folder is LISTED only when its floor has moved since it was
// last trimmed. Reading the safekeepers' and the pageserver's positions is HTTP to our own pods and costs nothing; the listing is the store's request. 0 (unset): the job does not run by itself.
const WAL_TRIM_EVERY_S = Number(process.env['WAL_TRIM_EVERY_S'] ?? '3600')
const WAL_TRIM_BATCH = Number(process.env['WAL_TRIM_BATCH'] ?? '100000')
const WAL_SEG = 16 * 1024 * 1024
let s3client: any
const s3 = () => {
  if (s3client) return s3client
  const key = process.env['AWS_ACCESS_KEY_ID'], secret = process.env['AWS_SECRET_ACCESS_KEY']
  if (!key || !secret || !process.env['S3_BUCKET'] || !process.env['S3_ENDPOINT']) throw new Error('no object store credentials, bucket or endpoint in this pod')
  s3client = new (Bun as any).S3Client({ accessKeyId: key, secretAccessKey: secret, bucket: process.env['S3_BUCKET'] ?? '', endpoint: process.env['S3_ENDPOINT'] ?? '', region: process.env['S3_REGION'] ?? 'auto' })
  return s3client
}
const lsnOf = (t: string): bigint => { const [a, b] = t.split('/'); return (BigInt('0x' + a!) << 32n) | BigInt('0x' + b!) }
const segnoOfName = (n: string): bigint => (BigInt('0x' + n.slice(8, 16)) * 256n) + BigInt('0x' + n.slice(16, 24))
const walFloors = new Map<string, bigint>() // "tenant/timeline" -> the floor read at the previous look
const trimmedAt = new Map<string, bigint>() // "tenant/timeline" -> the floor at which its folder was last listed and trimmed
interface TrimReport { timeline: string; floor?: string; floorSegment?: string; objects: number; bytes: number; deleted: number; deletedBytes: number; skipped?: string }
async function trimTimeline(tenant: string, timeline: string, dry: boolean): Promise<TrimReport> {
  const key = `${tenant}/${timeline}`
  const rep: TrimReport = { timeline: key, objects: 0, bytes: 0, deleted: 0, deletedBytes: 0 }
  const lsns: bigint[] = []
  let members: string[]
  try { members = (await safekeepersOf(tenant, timeline)).members.map((m) => m.split(':')[0]!) } catch (e) { rep.skipped = `no safekeeper answered for the members: ${e}`; return rep }
  for (const host of members) {
    const r = await fetch(`http://${host}:7676/v1/tenant/${tenant}/timeline/${timeline}`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(4000) }).catch(() => null)
    if (!r || !r.ok) { rep.skipped = `${host} did not answer`; return rep }
    const j = (await r.json()) as any
    if (!j.remote_consistent_lsn || !j.peer_horizon_lsn) { rep.skipped = `${host} reports no positions`; return rep }
    lsns.push(lsnOf(j.remote_consistent_lsn), lsnOf(j.peer_horizon_lsn))
  }
  let node: number
  try { node = await attachedNode(tenant) } catch (e) { rep.skipped = `the controller does not know the attachment: ${e}`; return rep }
  const pr = await fetch(`http://${pageserverHost(node)}:9898/v1/tenant/${tenant}/timeline/${timeline}`, { headers: scoped('pageserverapi'), signal: AbortSignal.timeout(4000) }).catch(() => null)
  if (!pr || !pr.ok) { rep.skipped = `the attached pageserver (node ${node}) did not answer`; return rep }
  const pj = (await pr.json()) as any
  // `remote_consistent_lsn` reads 0/0 for a timeline that has not uploaded since the pageserver started (an idle one); `remote_consistent_lsn_visible` is the same fact without that gap
  const psRemote = pj.remote_consistent_lsn_visible ?? pj.remote_consistent_lsn
  if (!psRemote || psRemote === '0/0') { rep.skipped = 'the pageserver reports no remote_consistent_lsn'; return rep }
  lsns.push(lsnOf(psRemote))
  let floor = lsns.reduce((a, b) => (a < b ? a : b))
  const before = walFloors.get(key)
  walFloors.set(key, floor)
  if (before === undefined && !dry) { rep.skipped = 'first look: acted on at the next'; return rep }
  if (before !== undefined && before < floor) floor = before
  const floorSeg = floor / BigInt(WAL_SEG)
  // listing is the store's request and costs: only when the floor has moved since this timeline was last trimmed (a dry look always reads)
  if (!dry && trimmedAt.get(key) === floor) { rep.skipped = 'the floor has not moved since the last trim: nothing listed'; return rep }
  rep.floor = `${(floor >> 32n).toString(16).toUpperCase()}/${(floor & 0xffffffffn).toString(16).toUpperCase()}`
  rep.floorSegment = floorSeg.toString()
  const prefix = `${S3_PREFIX}/${key}/`
  let token: string | undefined
  const doomed: string[] = []
  do {
    const page = await s3().list({ prefix, maxKeys: 1000, ...(token ? { continuationToken: token } : {}) })
    for (const o of (page.contents ?? []) as { key: string; size: number }[]) {
      rep.objects++; rep.bytes += o.size
      const name = o.key.slice(prefix.length)
      if (/^[0-9A-F]{24}$/.test(name) && segnoOfName(name) < floorSeg) { doomed.push(o.key); rep.deleted++; rep.deletedBytes += o.size }
    }
    token = page.isTruncated ? page.nextContinuationToken : undefined
  } while (token)
  if (!dry) for (const k of doomed) await s3().delete(k)
  if (!dry) trimmedAt.set(key, floor)
  if (dry) { rep.deleted = 0 }
  bump('wal_trim_objects_deleted_total', dry ? 0 : rep.deleted); bump('wal_trim_bytes_deleted_total', dry ? 0 : rep.deletedBytes)
  return rep
}
let walCursor = 0
async function walTrimCycle() {
  if (!isLeader || WAL_TRIM_EVERY_S <= 0) return
  const seen = new Set<string>(), pairs: [string, string][] = []
  for (const d of await listDbs()) { const k = `${d.tenant}/${d.timeline}`; if (!seen.has(k) && d.mode === 'primary') { seen.add(k); pairs.push([d.tenant, d.timeline]) } }
  for (let n = 0; n < Math.min(WAL_TRIM_BATCH, pairs.length); n++) {
    const [t, tl] = pairs[(walCursor + n) % pairs.length]!
    const r = await trimTimeline(t, tl, false).catch((e) => ({ skipped: String(e) } as any))
    if (r.skipped) bump('wal_trim_skipped_total')
  }
  walCursor = (walCursor + WAL_TRIM_BATCH) % Math.max(1, pairs.length)
}

// ---- the safekeepers: this layer owns every one of them, and replaces one only when it should (as the first machine cuts of a live platform taught)
// OWNERSHIP. Every safekeeper StatefulSet is this layer's (`peat-storage` declares none: a declaration would drift from what is live the first time one is replaced, and applying it would recreate a replaced member on an
// empty disk). The first three are SEEDED by the acting layer when the platform has no safekeeper StatefulSet and the storage controller lists none (SAFEKEEPER_SEED: `<id>=<machine>,...`), and registered when their pods answer;
// when a StatefulSet exists nothing is seeded, and when the controller lists safekeepers but no StatefulSet exists it is NOT seeded (the members would be empty disks) and a person is told by a metric and a log line.
// WHEN TO REPLACE. A machine that is gone for minutes and comes back with its disk intact must get its safekeeper back, not a replacement that copies every timeline: the first machine cuts of a live platform replaced a safekeeper
// each, at 180 s, though every machine returned in about 175 s. So: a safekeeper whose disk is known LOST (it refused to start empty: the guard's mark) is replaced at once; one that merely does not answer is replaced only
// after SK_GONE_S (default 1800 s, 30 minutes: a machine is back inside a few minutes, an hour is a decision), and only onto a machine that holds no other Active safekeeper (no timeline's set may have two members on one
// machine: its loss would take two of three). With none, it WAITS (`peat_control_sk_replacement_blocked{id}` 1, a log line every 5 minutes, the StatefulSet-not-ready alert keeps the absence seen). A replacement is a new id
// on such a machine, registered with the storage controller; then every timeline that had the lost member is moved to a set with the new one, SK_MIGRATE_PARALLEL at a time (the controller's `safekeeper_migrate`), each retried;
// then the lost one is decommissioned (so nothing is scheduled onto it) and its StatefulSet removed. Progress: GET /sk-replace, and peat_control_sk_* metrics.
const SK_GONE_S = Number(process.env['SK_GONE_S'] ?? '1800')
const SK_MIGRATE_PARALLEL = Number(process.env['SK_MIGRATE_PARALLEL'] ?? '16')
const SK_REPLACE = process.env['SK_REPLACE'] !== '0'
const STORAGE_IMAGE = process.env['STORAGE_IMAGE'] ?? ''
const skSeen = new Map<number, number>()
const blockedLogged = new Map<number, number>()
const markTried = new Map<number, number>()
const reopenTried = new Map<number, number>()
let openReported = new Set<string>()
interface SkJob { lost: number; newId?: number; node?: string; phase: string; startedAt: number; endedAt?: number; total: number; done: number; failed: number; inFlight: number; maxInFlight: number; parallel: number; note?: string; durationsMs: number[]; created?: boolean; nodeBlips: number; levels: Record<string, { n: number; sumMs: number; since: number }> }
let skJob: SkJob | null = null
let skJobHistory: SkJob[] = []
// The safekeeper's pod is `storage-spec.ts`'s (one definition for the three the platform starts with and for a replacement): built here from the environment the Deployment gives this pod.
// stamped with the release of the storage image it runs (RELEASE, from the operator), which the Platform's status reports
const RELEASE = process.env['RELEASE'] ?? ''
const skStatefulSet = (id: number, node: string) => {
  const o = safekeeperStatefulSet({
    id, node, namespace: NS, image: STORAGE_IMAGE, controlUrl: CONTROL_URL,
    remote: { endpoint: process.env['S3_ENDPOINT'] ?? '', bucket: process.env['S3_BUCKET'] ?? '', region: process.env['S3_REGION'] ?? 'auto', prefix: S3_PREFIX },
    secrets: SK_SECRETS,
    placement: placementFor(node, { keeperMachines: KEEPER_MACHINES, platform: PLACEMENT, keeper: KEEPER_PLACEMENT }), volumeRoot: SK_VOLUME, volumeType: SK_VOLUME_TYPE, walBackupParallelJobs: Number(process.env['SK_WAL_BACKUP_JOBS'] ?? '2'),
    resources: SK_RESOURCES,
    labels: { 'peat-sk': 'true' },
  })
  if (RELEASE) o.metadata.annotations = { ...(o.metadata.annotations ?? {}), [`${GROUP}/release`]: RELEASE }
  return o
}
/** The machine a safekeeper's pod is on (or pinned to while it waits), or null when it has no pod. */
async function skMachine(id: number): Promise<string | null> {
  const p = await kube('GET', `/api/v1/namespaces/${NS}/pods/safekeeper-w${id}-0`)
  if (p.status !== 200) return null
  return (p.json?.spec?.nodeName ?? p.json?.spec?.nodeSelector?.['kubernetes.io/hostname'] ?? null) as string | null
}
/**
 * A member's registration. Its availability zone is its MACHINE: the storage controller picks a new timeline's members from distinct zones (`safekeepers_for_new_timeline`, read 2026-11-20), so with a zone per machine a set never
 * holds two members of one machine, and a lost member and its replacement (made on one machine) are never both in a set. (It was `sk-<id>`, a zone per member, which allowed both.)
 */
const skRegistration = (id: number, machine: string | null, zone: string | null = machine) => ({ id, created_at: '2023-10-25T09:11:25Z', updated_at: '2024-08-28T11:32:43Z', region_id: 'peat', host: `safekeeper-w${id}-0${SK_SUFFIX}`, port: 5454, http_port: 7676, https_port: null, version: 5957, availability_zone_id: SPREAD === 'none' ? `sk-${id}` : (zone ?? `sk-${id}`) })
/** A member's zone: its machine's name, or the value of SAFEKEEPER_ZONE_LABEL on its machine; a zone of its own with spread none. */
async function zoneOf(id: number, machine: string | null): Promise<string | null> {
  if (SPREAD === 'none') return `sk-${id}`
  if (!machine || SK_ZONE_LABEL === 'kubernetes.io/hostname') return machine
  const n = await kube('GET', `/api/v1/nodes/${machine}`)
  return n.status === 200 ? String(n.json?.metadata?.labels?.[SK_ZONE_LABEL] ?? machine) : machine
}
/** Registers a running safekeeper with the storage controller and makes it Active, as neon_local does (control_plane/src/storage_controller.rs, register_safekeepers). Safe to repeat. Used for a replacement and, by `POST /sk-register`, for the platform's first three (nothing else registers them). */
async function registerSafekeeper(id: number): Promise<void> {
  const machine = await skMachine(id)
  ok(await storcon('POST', `/control/v1/safekeeper/${id}`, skRegistration(id, machine, await zoneOf(id, machine))), 'register')
  ok(await storcon('POST', `/control/v1/safekeeper/${id}/scheduling_policy`, { scheduling_policy: 'Active' }), 'activate')
}
/** Puts every listed member that is not decommissioned in the zone of its machine (an upsert only: its scheduling policy is left alone). Members registered before zones were machines are moved here once. */
async function ensureSkZones(list: any[]): Promise<Map<number, string | null>> {
  const machines = new Map<number, string | null>()
  for (const sk of list) {
    if (/^decom/i.test(String(sk.scheduling_policy))) continue
    const machine = await skMachine(sk.id)
    machines.set(sk.id, machine)
    const zone = await zoneOf(sk.id, machine)
    if (!machine || !zone || sk.availability_zone_id === zone) continue
    const r = await storcon('POST', `/control/v1/safekeeper/${sk.id}`, skRegistration(sk.id, machine, zone))
    console.log(`sk-zone: safekeeper ${sk.id} (was ${sk.availability_zone_id}) put in the zone ${zone}: ${r.status}`)
  }
  return machines
}
// ---- seeding the first safekeepers (this layer owns them; see above)
const SAFEKEEPER_SEED = (process.env['SAFEKEEPER_SEED'] ?? '').split(',').filter(Boolean).map((x) => { const [id, node] = x.split('='); return { id: Number(id), node: node ?? '' } })
let seedRefusedLogged = 0
async function registerWhenReady(id: number) {
  for (let i = 0; i < 150; i++) {
    const p = await kube('GET', `/api/v1/namespaces/${NS}/pods/safekeeper-w${id}-0`)
    if (p.status === 200 && podReady(p.json)) { await registerSafekeeper(id).then(() => console.log(`sk-seed: safekeeper ${id} registered`)).catch((e) => console.log(`sk-seed: register ${id}: ${e}`)); return }
    await new Promise((r) => setTimeout(r, 2000))
  }
  console.log(`sk-seed: safekeeper ${id} did not become ready in 5 minutes: not registered (POST /sk-register?id=${id} when it is)`)
}
async function seedSafekeepers() {
  if (!isLeader || SAFEKEEPER_SEED.length === 0) return
  const sts = ok(await kube('GET', `/apis/apps/v1/namespaces/${NS}/statefulsets`), 'list statefulsets').json.items as any[]
  const names = sts.map((x) => String(x.metadata.name))
  const makeOne = async (id: number, node: string, group: string) => {
    const body = skStatefulSet(id, node)
    body.metadata.annotations = { ...(body.metadata.annotations ?? {}), [SEED_GROUP_ANNOTATION]: group }
    ok(await kube('POST', `/apis/apps/v1/namespaces/${NS}/statefulsets`, body), `seed safekeeper ${id}`, [409])
    void registerWhenReady(id)
  }
  if (seedDecision(names, []) === 'exists') {
    // the usual tick. A seeding that stopped half way (the group is on its StatefulSets) is finished: the members never made and never listed by the controller are made now
    const group = parseSeedGroup(sts.map((x) => x.metadata.annotations?.[SEED_GROUP_ANNOTATION] as string | undefined).find(Boolean))
    if (group.length === 0) return
    const list = await storcon('GET', '/control/v1/safekeeper')
    if (list.status !== 200 || !Array.isArray(list.json)) return
    const missing = seedCompletion(group, names, (list.json as any[]).map((x) => x.id as number))
    if (missing.length === 0) return
    console.log(`sk-seed: finishing a seeding that stopped half way: making ${missing.map((m) => `${m.id} on ${m.node}`).join(', ')}`)
    for (const m of missing) await makeOne(m.id, m.node, formatSeedGroup(group))
    return
  }
  const list = await storcon('GET', '/control/v1/safekeeper')
  if (list.status !== 200 || !Array.isArray(list.json)) return // asked again at the next tick
  const listed = list.json as any[]
  const what = seedDecision(names, listed)
  if (what === 'exists') return
  if (what === 'refuse') {
    bump_gauge('sk_seed_refused', 1)
    if (Date.now() - seedRefusedLogged > 300_000) { seedRefusedLogged = Date.now(); console.log('sk-seed: the storage controller lists safekeepers that are not decommissioned but no safekeeper StatefulSet exists: NOT seeding (every new member would be an empty disk under an old name); this needs a person') }
    return
  }
  bump_gauge('sk_seed_refused', 0)
  // one member on the labelled keeper machine when it is ready (`seedGroup`): the set after a total loss is two on the platform's machines and one on the keeper machine, as the platform runs
  const seed = seedGroup(SAFEKEEPER_SEED, KEEPER_MACHINES, await keepersReady())
  const ids = seedIds(seed.map((x) => x.id), listed)
  const group = seed.map((x, i) => ({ id: ids[i]!, node: x.node }))
  console.log(`sk-seed: no safekeeper exists${listed.length ? ` (every listed one is decommissioned: a total loss)` : ''}: making ${group.map((x) => `${x.id} on ${x.node}`).join(', ')}`)
  for (const m of group) await makeOne(m.id, m.node, formatSeedGroup(group))
}
async function skWatch() {
  if (!isLeader || !SK_REPLACE) return
  const list = await storcon('GET', '/control/v1/safekeeper')
  if (list.status !== 200 || !Array.isArray(list.json)) return
  const machines = await ensureSkZones(list.json as any[]).catch((e) => { console.log(`sk-zone: ${e}`); return new Map<number, string | null>() })
  // how many members sit on a keeper machine: 1 is the design (two on the platform's machines, one on the keeper machine); 0 after the keeper's member was replaced onto the platform's machines (a person moves one back)
  if (KEEPER_MACHINES.length > 0 && machines.size > 0) bump_gauge('sk_members_off_box', membersOffBox((list.json as any[]).map((x) => ({ id: x.id, scheduling_policy: x.scheduling_policy, machine: machines.get(x.id) ?? null, answers: true })), KEEPER_MACHINES))
  // the open replacements (a member marked lost, a replacement not yet closed): how long each has been open, and the members whose replacement ended with failures or unknowns and that answer again
  const openMarks = ((await kube('GET', `/api/v1/namespaces/${NS}/configmaps?labelSelector=peat-sk-open`)).json?.items ?? []) as any[]
  const nowOpen = new Set<string>()
  for (const m of openMarks) {
    const id = String(m.data?.id ?? ''); if (!id) continue
    nowOpen.add(id)
    bump_gauge(`sk_replacement_open_seconds{id="${id}"}`, Math.max(0, Math.round((Date.now() - Date.parse(m.metadata.creationTimestamp)) / 1000)))
  }
  for (const id of openReported) if (!nowOpen.has(id)) bump_gauge(`sk_replacement_open_seconds{id="${id}"}`, 0)
  openReported = nowOpen
  // the replacement marks of the whole cluster (not this pod's skJob): a job starts only when no other member's replacement is open, and a rerun keeps the move's target the mark recorded
  const replacementMarks: ReplacementMark[] = openMarks.filter((m) => String(m.metadata.name).startsWith('sk-replacement-')).map((m) => ({ id: Number(m.data?.id), at: Date.parse(m.metadata.creationTimestamp) || 0, moveTo: String(m.data?.moveTo ?? '') }))
  const replacementIds = new Set(replacementMarks.map((m) => m.id))
  // a member that answers again has not been lost for good: the keeper machine is not given up on for it (the mark is for one loss)
  const triedMarks = ((await kube('GET', `/api/v1/namespaces/${NS}/configmaps?labelSelector=peat-sk-keeper-tried`)).json?.items ?? []) as any[]
  for (const sk of list.json as any[]) {
    if (sk.scheduling_policy !== 'Active') continue
    const r = await fetch(`http://${sk.host}:${sk.http_port}/v1/status`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(2000) }).catch(() => null)
    if (r?.ok || !skSeen.has(sk.id)) skSeen.set(sk.id, Date.now())
    const gone = (Date.now() - skSeen.get(sk.id)!) / 1000
    bump_gauge(`sk_gone_seconds{id="${sk.id}"}`, r?.ok ? 0 : Math.round(gone))
    if (r?.ok) bump_gauge(`sk_replacement_blocked{id="${sk.id}"}`, 0)
    if (r?.ok && triedMarks.some((m) => String(m.data?.id) === String(sk.id))) await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-keeper-tried-${sk.id}`)
    if (reopenDecision({ answers: Boolean(r?.ok), hasReplacementMark: replacementIds.has(sk.id), jobRunning: skJob !== null || otherReplacementOpen(replacementMarks, sk.id) !== null, sinceLastTryS: (Date.now() - (reopenTried.get(sk.id) ?? 0)) / 1000, everyS: 300 }) === 'rerun') {
      reopenTried.set(sk.id, Date.now())
      console.log(`sk-replace: safekeeper ${sk.id} answers again with its replacement still open: finishing the replacement`)
      void replaceSafekeeper(sk.id, list.json as any[], false, replacementMarks.find((m) => m.id === sk.id)?.moveTo || undefined)
    }
    if (gone > SK_GONE_S && !skJob && otherReplacementOpen(replacementMarks, sk.id) === null) {
      const where = await replacementMachinesNow(sk.id, list.json as any[]).catch(() => [] as string[])
      const what = replaceDecision(gone, SK_GONE_S, where)
      bump_gauge(`sk_replacement_blocked{id="${sk.id}"}`, what === 'wait' ? 1 : 0)
      if (what === 'replace') void replaceSafekeeper(sk.id, list.json as any[])
      else if (Date.now() - (blockedLogged.get(sk.id) ?? 0) > 300_000) {
        blockedLogged.set(sk.id, Date.now())
        console.log(`sk-replace: safekeeper ${sk.id} has not answered for ${Math.round(gone)} s and no machine without another member exists: waiting, not replacing`)
      }
    }
  }
  // a safekeeper that refused to start empty (the guard above) left a mark: it is lost now, whatever its pod does
  if (!skJob) {
    const marks = await kube('GET', `/api/v1/namespaces/${NS}/configmaps?labelSelector=peat-sk-lost`)
    for (const m of (marks.json?.items ?? []) as any[]) {
      const id = Number(m.data?.id)
      if (!id || !(list.json as any[]).some((x) => x.id === id && x.scheduling_policy === 'Active')) { await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/${m.metadata.name}`); continue }
      if (otherReplacementOpen(replacementMarks, id) !== null) continue // another member's replacement is open: this one waits for it
      if (Date.now() - (markTried.get(id) ?? 0) < 60_000) continue // a replacement that could not be made (no machine free of another member) is tried again each minute, not each tick
      markTried.set(id, Date.now())
      console.log(`sk-replace: safekeeper ${id} refused to start on an empty disk: replacing it now`)
      void replaceSafekeeper(id, list.json as any[], true)
      break
    }
  }
}
const gauges: Record<string, number> = {}
const bump_gauge = (k: string, v: number) => { gauges[k] = v }
/** The machines a replacement may go to (`storage-spec.ts`, `replacementMachines`, which a test judges): from the live pods and the machines this layer knows. */
/** Why a keeper machine cannot hold a member now, or null when it can: it must carry the keeper label (a hand step: `kubectl label node <machine> <KEEPER_LABEL>`) and be Ready, its directory must exist (the Job keeper-prep completed) and the storage image must already be on the node (a first pull over a slow link inside the three minutes a new member is given would fail). */
async function keeperProblem(machine: string): Promise<string | null> {
  const n = await kube('GET', `/api/v1/nodes/${machine}`)
  if (n.status !== 200) return `${machine} is not a Ready node carrying ${KEEPER_LABEL}=${KEEPER.value}`
  // Ready is not enough: a cordoned node, a taint the keeper's pod does not tolerate and Disk, Memory or PID pressure each leave a new member Pending or evict it (`keeperNodeProblem`)
  const node = keeperNodeProblem(n.json, KEEPER, KEEPER_PLACEMENT.tolerations)
  if (node) return node
  const prep = await kube('GET', `/apis/batch/v1/namespaces/${NS}/jobs/keeper-prep`)
  if (prep.status !== 200 || (prep.json?.status?.succeeded ?? 0) < 1) return 'the Job keeper-prep has not completed: the keeper\'s directory does not exist (apply peat-keeper)'
  const digest = STORAGE_IMAGE.split('@')[1] ?? STORAGE_IMAGE
  const present = ((n.json?.status?.images ?? []) as any[]).some((i) => ((i.names ?? []) as string[]).some((nm) => nm.includes(digest)))
  if (!present) return `the storage image is not on ${machine} yet (the keeper-prep Job pulls it)`
  return null
}
/** The keeper machines that can hold a member now (`keeperProblem` says none): what a replacement of the keeper's own member and a seeding after a total loss use. */
async function keepersReady(): Promise<string[]> {
  const out: string[] = []
  for (const m of KEEPER_MACHINES) if ((await keeperProblem(m).catch(() => 'unreadable')) === null) out.push(m)
  return out
}
/** The keeper machine is tried once per loss: a replacement that failed on it leaves this mark (not an open mark: it blocks nothing), and the next decision for that member uses the platform's machines. Removed when the member is closed or decommissioned. */
async function markKeeperTried(lost: number, why: string) {
  const made = await kube('POST', `/api/v1/namespaces/${NS}/configmaps`, { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: `sk-keeper-tried-${lost}`, namespace: NS, labels: { 'peat-sk-keeper-tried': 'true' } }, data: { id: String(lost), at: new Date().toISOString(), why } })
  if (made.status >= 300 && made.status !== 409) console.log(`sk-replace: could not mark that the keeper machine was tried for safekeeper ${lost}: ${made.status}`)
  else console.log(`sk-replace: the keeper machine was tried for safekeeper ${lost} and did not work (${why}): the next attempt goes to the platform's machines`)
}
const keeperTried = async (lost: number): Promise<boolean> => (await kube('GET', `/api/v1/namespaces/${NS}/configmaps/sk-keeper-tried-${lost}`)).status === 200
async function replacementMachinesNow(lost: number, all: any[]): Promise<string[]> {
  const pods = ok(await kube('GET', `/api/v1/namespaces/${NS}/pods?labelSelector=app%3Dsafekeeper`), 'list safekeepers').json.items as any[]
  const podRecords = pods.map((p) => ({ nodeId: String(p.metadata.labels?.['node-id']), node: (p.spec?.nodeName ?? p.spec?.nodeSelector?.['kubernetes.io/hostname'] ?? '') as string }))
  // the keeper's own member comes back to its machine when that is ready (`replacementPool`); a member of the platform's machines never goes to a keeper machine
  const lostNode = podRecords.find((p) => p.nodeId === String(lost))?.node ?? ''
  const onKeeper = KEEPER_MACHINES.includes(lostNode)
  const keeperReady = onKeeper ? await keepersReady() : []
  // a one-machine trial: any machine that is alive, the lost member's first (the platform survives no machine's loss anyway)
  if (SPREAD === 'none') return [...nodeStates.values()].filter((m) => !m.dead).map((m) => m.name).sort((a, b) => (a === lostNode ? -1 : 0) - (b === lostNode ? -1 : 0))
  return replacementMachines(
    lost, all as SkRecord[], podRecords,
    replacementPool([...nodeStates.keys()].map((n) => ({ name: n, dead: Boolean(nodeStates.get(n)?.dead) })), lostNode, KEEPER_MACHINES, keeperReady, onKeeper ? await keeperTried(lost) : false),
  )
}
async function pickNode(lost: number, all: any[]): Promise<string> {
  const where = await replacementMachinesNow(lost, all)
  if (!where.length) throw new Error('no machine without another member of the set is alive: waiting')
  return where[0]!
}
/**
 * Asks the members other than `lost` (ids 1 to `ceiling`, in id order, the first answer decides) about every primary timeline of the control layer's records: which still list `lost` (or a pending change of members), and which
 * nobody answered about (unknown). The rule that turns this into a decision is `closeDecision` (storage-spec.ts).
 */
async function readTimelines(lost: number, lostHost: string, ceiling: number) {
  const todo: { tenant: string; timeline: string; set: number[] }[] = []
  const reads: TimelineRead[] = []
  let onNew = 0 // timelines that refer to the new member (`ceiling` is its id) in their set or a pending change of it: where a replacement made progress
  const pairs: [string, string][] = []
  const seen = new Set<string>()
  for (const d of await listDbs()) { const k = `${d.tenant}/${d.timeline}`; if (d.mode === 'primary' && !seen.has(k)) { seen.add(k); pairs.push([d.tenant, d.timeline]) } }
  let pi = 0
  await Promise.all(Array.from({ length: 16 }, async () => {
    for (;;) {
      const i = pi++; if (i >= pairs.length) return
      const [t, tl] = pairs[i]!
      let read: TimelineRead = { answered: false } // nobody answered: unknown, never "not on the lost one"
      for (let id = 1; id <= ceiling; id++) {
        if (id === lost) continue
        const r = await fetch(`http://safekeeper-w${id}-0${SK_SUFFIX}:7676/v1/tenant/${t}/timeline/${tl}`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(3000) }).catch(() => null)
        if (!r?.ok) continue
        const m = ((await r.json()) as any).mconf
        const ids = (m.members as any[]).map((x) => x.id as number)
        const onLost = m.members.some((x: any) => x.host === lostHost) || Boolean(m.new_members ?? null)
        if (ids.includes(ceiling) || JSON.stringify(m).includes(`safekeeper-w${ceiling}-0`)) onNew++
        if (onLost) todo.push({ tenant: t, timeline: tl, set: ids })
        read = { answered: true, onLost }
        break
      }
      reads.push(read)
    }
  }))
  const { unknown, close } = closeDecision(reads)
  return { todo, pairs: pairs.length, unknown, close, onNew }
}
/** Whether a safekeeper replacement is open: a job running, a member marked lost, or a replacement made whose lost member is not yet closed. */
async function replacementOpen(): Promise<string | null> {
  if (skJob) return `safekeeper ${skJob.lost} is being replaced`
  const marks = await kube('GET', `/api/v1/namespaces/${NS}/configmaps?labelSelector=peat-sk-open`)
  const open = ((marks.json?.items ?? []) as any[]).map((m) => String(m.data?.id ?? m.metadata.name))
  return open.length ? `the replacement of safekeeper ${open.join(', ')} is not closed` : null
}
/** The timelines already given their first start by this pod, and the one start that runs at a time (a platform machine holds about five computes). */
const firstStarted = new Set<string>()
let firstStartChain: Promise<unknown> = Promise.resolve()
/**
 * A timeline that was never written (`neverWritten`: a safekeeper's flush position is still its start) has no WAL file, and a migration of it fails in `pull_timeline`. If its database is asleep it is started once, waited for
 * until Ready (its first record is then on the safekeepers) and stopped again; true when that was done now or earlier, so that the migration is tried again. One at a time. False when the timeline has been written, has no
 * database of its own to start or the start failed: the migration's failure then stands.
 */
async function startOnceIfNeverWritten(tenant: string, timeline: string, set: number[]): Promise<boolean> {
  const key = `${tenant}/${timeline}`
  if (firstStarted.has(key)) return true
  // the status of EVERY member of the set that answers: one that has not caught up can show the start for a timeline that was written (`neverWritten` takes the highest flush position)
  const statuses: any[] = []
  for (const id of set) {
    const r = await fetch(`http://safekeeper-w${id}-0${SK_SUFFIX}:7676/v1/tenant/${tenant}/timeline/${timeline}`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(3000) }).catch(() => null)
    if (r?.ok) statuses.push(await r.json())
  }
  if (!neverWritten(statuses)) return false
  const d = (await listDbs()).find((x) => x.tenant === tenant && x.timeline === timeline && x.mode === 'primary')
  if (!d) return false
  const run = firstStartChain.then(async () => {
    if (firstStarted.has(key)) return true
    const dep = await kube('GET', depPath(d.name))
    if (dep.status === 200 && dep.json.spec.replicas === 1) return false // awake: it is not the first start that is missing
    console.log(`sk-replace: ${key} (${d.name}) was never written: starting its database once so that it has a WAL file`)
    let started = false, ready = false
    try {
      started = true // from here a throw may have left it scaled up
      await startCompute(d, { evenClosed: true })
      for (let i = 0; i < 90 && !ready; i++) { ready = podReady(await livePod(d.name)); if (!ready) await new Promise((res) => setTimeout(res, 1000)) }
    } finally {
      if (started) await stopCompute(d.name).catch((e) => console.log(`sk-replace: could not stop ${d.name} after its first start: ${e}`))
    }
    if (ready) firstStarted.add(key)
    return ready
  }).catch((e) => { console.log(`sk-replace: the first start of ${d.name} failed: ${e}`); return false })
  firstStartChain = run
  return run
}
async function replaceSafekeeper(lost: number, all: any[], immediate = false, moveTo?: string) {
  const job: SkJob = { lost, phase: 'starting', startedAt: Date.now(), total: 0, done: 0, failed: 0, inFlight: 0, maxInFlight: 0, parallel: SK_MIGRATE_PARALLEL, durationsMs: [], nodeBlips: 0, levels: {} }
  skJob = job
  let keeperAbandoned = false, tryBoxAtOnce = false
  let madeEarlier = false // the replacement was made by an earlier job (its mark says so): not this job's to undo
  let monitor: ReturnType<typeof setInterval> | undefined // the migrations' monitor: cleared in the `finally` too, so a throw cannot leave it running (and holding the job) for the life of the pod
  const finish = (note?: string) => { job.endedAt = Date.now(); job.note = note ?? job.note; skJobHistory.push(job); skJob = null; skSeen.set(lost, Date.now()) }
  try {
    console.log(moveTo ? `sk-move: safekeeper ${lost} moves to ${moveTo}, ${SK_MIGRATE_PARALLEL} migrations at a time` : `sk-replace: safekeeper ${lost} ${immediate ? 'is lost (empty disk)' : `has not answered for ${SK_GONE_S} s`}: replacing it, ${SK_MIGRATE_PARALLEL} migrations at a time`)
    // an earlier job for this safekeeper may have made its replacement already (it left a ConfigMap): that one is used, so that a job that ended with failures does not leave a new safekeeper behind each time it runs
    const memo = await kube('GET', `/api/v1/namespaces/${NS}/configmaps/sk-replacement-${lost}`)
    if (memo.status === 200) {
      job.newId = Number(memo.json.data.newId)
      moveTo ??= (memo.json.data.moveTo as string | undefined) || undefined // a rerun keeps the target the first job was given
      if (moveTo) job.node = moveTo
      if (memo.json.data.created !== 'false') { madeEarlier = true; job.node = moveTo ?? memo.json.data.node; job.note = 'reused the replacement an earlier job made'; job.created = true } // marks made before `created` existed were made after the replacement
    } else {
      if (moveTo) job.node = moveTo
      job.newId = Math.max(...all.map((x) => x.id as number)) + 1
      // the mark that says a replacement is open is made now, so that both control layer pods refuse new databases from the job's first second (the replacement itself follows)
      ok(await kube('POST', `/api/v1/namespaces/${NS}/configmaps`, { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: `sk-replacement-${lost}`, namespace: NS, labels: { 'peat-sk-open': 'true' } }, data: { id: String(lost), newId: String(job.newId), created: 'false', ...(moveTo ? { moveTo } : {}) } }), 'mark the replacement open', [409])
    }
    const operatorMove = Boolean(moveTo) // an operator chose the machine (a move, or a rerun of one): the keeper machine is not given up on
    const lostHost = (all.find((x) => x.id === lost)?.host ?? '') as string
    const findTodo = () => readTimelines(lost, lostHost, job.newId!)
    // the number in flight adapts: the controller's heartbeats to the pageservers time out when too many migrations run at once (64 in flight took both pageservers Offline for about 5 minutes, 2026-11-19), so
    // a check every 5 s halves it when a storage node is not Active and raises it by 2 after 12 good checks, up to the ceiling (SK_MIGRATE_PARALLEL)
    const ceiling = job.parallel
    let good = 0
    monitor = setInterval(async () => {
      const r = await storcon('GET', '/control/v1/node').catch(() => null)
      const bad = !r || r.status !== 200 || !Array.isArray(r.json) || (r.json as any[]).some((n) => (typeof n.availability === 'string' ? n.availability : JSON.stringify(n.availability)) !== 'Active')
      if (bad) { job.nodeBlips++; good = 0; if (job.parallel > 2) job.parallel = Math.max(2, Math.floor(job.parallel / 2)) } else if (++good >= 12 && job.parallel < ceiling) { job.parallel = Math.min(ceiling, job.parallel + 2); good = 0 }
    }, 5000)
    // The replacement is made first, whether or not any timeline is found to need it: a set must not shrink because nothing was found to move (when every member lost its disk no safekeeper knows a timeline, and a replacement that waited for
    // one to move would be skipped while the lost members were decommissioned, leaving none).
    if (!job.created) {
      job.created = true
      if (!job.node) job.node = await pickNode(lost, all).catch(async (e) => { await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-replacement-${lost}`); throw e }) // nothing was made: nothing is open
      job.phase = 'creating'
      ok(await kube('POST', `/apis/apps/v1/namespaces/${NS}/statefulsets`, skStatefulSet(job.newId, job.node)), 'create safekeeper statefulset', [409])
      for (let i = 0; i < 90; i++) {
        const p = await kube('GET', `/api/v1/namespaces/${NS}/pods/safekeeper-w${job.newId}-0`)
        if (p.status === 200 && podReady(p.json)) break
        if (i === 89) throw new Error('the new safekeeper did not become ready in 3 minutes')
        await new Promise((r) => setTimeout(r, 2000))
      }
      job.phase = 'registering'
      await registerSafekeeper(job.newId)
      // The lost one is NOT paused here: the controller panics (safekeeper_reconciler.rs:69, an unwrap on a missing reconciler) when it schedules the exclusion of a paused safekeeper from a timeline that is being
      // moved off it (seen 2026-11-19, the first fleet run: the controller exited and restarted at the first migrations). It is decommissioned after the last timeline has left it.
      ok(await kube('PATCH', `/api/v1/namespaces/${NS}/configmaps/sk-replacement-${lost}`, { data: { created: 'true', node: job.node } }, 'application/merge-patch+json'), 'record the replacement')
    }
    for (let pass = 1; pass <= 4; pass++) {
      if (!isLeader) throw new Error('this pod is no longer the acting control layer: the acting one finishes the replacement from its mark')
      job.phase = pass === 1 ? 'finding the timelines' : `finding what is left (pass ${pass})`
      const { todo, pairs } = await findTodo()
      if (todo.length === 0) break
      job.total += todo.length; if (pass === 1) job.failed = 0
      job.phase = pass === 1 ? 'migrating' : `migrating (pass ${pass})`
      console.log(`sk-replace: pass ${pass}: ${todo.length} of ${pairs} timelines still have safekeeper ${lost}; the new one is ${job.newId} on ${job.node}`)
      let ti = 0
      await Promise.all(Array.from({ length: 128 }, async (_, w) => {
        for (;;) {
          if (ti >= todo.length) return
          if (w >= job.parallel) { await new Promise((res) => setTimeout(res, 1000)); continue }
          const i = ti++; if (i >= todo.length) return
          const x = todo[i]!
          job.inFlight++; job.maxInFlight = Math.max(job.maxInFlight, job.inFlight)
          const level = String(job.parallel)
          job.levels[level] ??= { n: 0, sumMs: 0, since: Date.now() }
          const t0 = Date.now()
          let okd = false
          for (let attempt = 0; attempt < 3 && !okd; attempt++) {
            try {
              const r = await storcon('POST', `/v1/tenant/${x.tenant}/timeline/${x.timeline}/safekeeper_migrate`, { new_sk_set: [...x.set.filter((y) => y !== lost), job.newId!].filter((y, k, a) => a.indexOf(y) === k).slice(0, 3) }, 300_000)
              okd = r.status < 300
              // a recovered database never started has no WAL file and the migration's pull_timeline breaks on it: its database is started once and stopped, and the migration is tried again at once
              if (!okd && attempt === 0 && isPullTimelineFailure(JSON.stringify(r.json)) && (await startOnceIfNeverWritten(x.tenant, x.timeline, x.set))) continue
              if (!okd) { if (attempt === 2) console.log(`sk-replace: ${x.tenant}/${x.timeline}: ${r.status} ${JSON.stringify(r.json).slice(0, 160)}`); await new Promise((res) => setTimeout(res, 3000 * (attempt + 1))) }
            } catch (e) { if (attempt === 2) console.log(`sk-replace: ${x.tenant}/${x.timeline}: ${e}`); await new Promise((res) => setTimeout(res, 3000 * (attempt + 1))) }
          }
          job.inFlight--
          if (okd) { job.done++; job.durationsMs.push(Date.now() - t0); job.levels[level]!.n++; job.levels[level]!.sumMs += Date.now() - t0 } else job.failed++
        }
      }))
      if (pass > 1) await new Promise((res) => setTimeout(res, 5000))
    }
    clearInterval(monitor)
    // what is left after the passes is counted from the safekeepers themselves: a move that was reported failed may have gone through. The lost member is closed only when every timeline was answered about and none lists it
    const fin = await findTodo()
    job.failed = fin.todo.length
    job.phase = 'closing'
    if (fin.close) {
      await storcon('POST', `/control/v1/safekeeper/${lost}/scheduling_policy`, { scheduling_policy: 'Decomissioned' }).catch(() => {})
      await kube('DELETE', `/apis/apps/v1/namespaces/${NS}/statefulsets/safekeeper-w${lost}`)
      await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-replacement-${lost}`)
      await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-lost-${lost}`)
      await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-keeper-tried-${lost}`)
    } else if (keeperReplacementVerdict({ onKeeper: KEEPER_MACHINES.includes(job.node ?? ''), operatorMove, failed: job.failed, referencingNew: fin.onNew, unknown: fin.unknown, done: job.done }) === 'abandon') {
      // the keeper machine took the member and not a single timeline could be moved onto it (the machine is cordoned, under pressure, or cannot be reached from the platform's machines): the member is retired (it holds nothing), the machine is
      // marked tried, and the next attempt, at once, goes to the platform's machines
      await markKeeperTried(lost, `${job.failed} migrations to safekeeper ${job.newId} on ${job.node} failed and none moved`)
      await storcon('POST', `/control/v1/safekeeper/${job.newId}/scheduling_policy`, { scheduling_policy: 'Decomissioned' }).catch(() => {})
      await kube('DELETE', `/apis/apps/v1/namespaces/${NS}/statefulsets/safekeeper-w${job.newId}`)
      await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-replacement-${lost}`)
      job.note = `the keeper machine ${job.node} did not work: safekeeper ${job.newId} on it was retired (nothing had moved to it), the next attempt goes to the platform's machines`
      keeperAbandoned = true
    } else if (fin.unknown > 0) job.note = `${fin.unknown} timelines were not answered about: safekeeper ${lost} is kept (decommission it by hand, POST /sk/${lost}/decommission, when they are known)`
    job.phase = keeperAbandoned ? 'done with failures' : fin.close ? 'done' : fin.unknown > 0 && job.failed === 0 ? 'done with unknowns' : 'done with failures'
    console.log(`sk-replace: ${job.phase} in ${Math.round((Date.now() - job.startedAt) / 1000)} s: ${job.done} moved, ${job.failed} failed, ${fin.unknown} unknown`)
    finish()
    if (keeperAbandoned) skSeen.set(lost, Date.now() - (SK_GONE_S + 1) * 1000) // the attempt on the platform's machines is not made to wait the half hour a failed attempt otherwise does
  } catch (e) {
    // A replacement whose new member never became Ready, with nothing registered or migrated, is undone: its StatefulSet and its mark are deleted, so that nothing stays open (every new database is refused while one is)
    if (job.phase === 'creating' && job.newId !== undefined && !madeEarlier) {
      await kube('DELETE', `/apis/apps/v1/namespaces/${NS}/statefulsets/safekeeper-w${job.newId}`).catch(() => {})
      await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-replacement-${lost}`).catch(() => {})
      // on the keeper machine this is the machine's doing (a Ready node that cannot run the pod): it is not chosen again for this loss, and the attempt on the platform's machines is not made to wait
      if (KEEPER_MACHINES.includes(job.node ?? '') && !moveTo) { await markKeeperTried(lost, `the new member ${job.newId} did not become Ready on ${job.node}`).catch(() => {}); tryBoxAtOnce = true }
      console.log(`sk-replace: the new member ${job.newId} did not start and nothing was moved: its StatefulSet and the replacement mark of safekeeper ${lost} are removed (${e})`)
    }
    job.phase = 'failed'; console.log(`sk-replace: ${e}`); finish(String(e))
    if (tryBoxAtOnce) skSeen.set(lost, Date.now() - (SK_GONE_S + 1) * 1000)
  } finally {
    clearInterval(monitor)
  }
}
async function loop(name: string, everyMs: number, fn: () => Promise<void>) {
  for (;;) {
    try { await fn() } catch (e) { console.log(`${name}: ${e}`); bump(`loop_errors_total`) }
    await new Promise((res) => setTimeout(res, everyMs))
  }
}

// ---- two control layer pods, one acting at a time
// A Lease `peat-control-leader` in this namespace, taken and renewed by compare-and-swap on its resourceVersion (the Kubernetes API refuses a write made on a stale version, so two pods cannot both hold it). A pod that
// does not see the Lease change for LEASE_S seconds takes it over (it watches for change on its own clock, as client-go's leader election does, so no clock of another machine is compared). Only the pod that holds it
// taints machines and reconciles computes; both pods serve every call (the state is Kubernetes objects) and both probe the machines (so a new leader already knows which are dead).
// The same arbitration serves the gateways' sleep decision (`POST /gateway/lease`): a second Lease, held by the gateway pod that last asked and kept for 15 s after its last ask.
const ME = process.env['POD_NAME'] ?? `control-${randomBytes(3).toString('hex')}`
const LEASE_S = Number(process.env['LEASE_S'] ?? '10')
let isLeader = false
const micro = () => new Date().toISOString().replace('Z', '000Z')
const leaseSeen = new Map<string, { rv: string; at: number }>()
async function acquireLease(ns: string, name: string, holder: string, durS: number): Promise<boolean> {
  const path = `/apis/coordination.k8s.io/v1/namespaces/${ns}/leases`
  const got = await kube('GET', `${path}/${name}`)
  if (got.status === 404) {
    const made = await kube('POST', path, { apiVersion: 'coordination.k8s.io/v1', kind: 'Lease', metadata: { name, namespace: ns }, spec: { holderIdentity: holder, leaseDurationSeconds: durS, acquireTime: micro(), renewTime: micro(), leaseTransitions: 0 } })
    return made.status < 300
  }
  ok(got, `read lease ${name}`)
  const l = got.json
  const key = `${ns}/${name}`
  const rv = l.metadata.resourceVersion as string
  const seen = leaseSeen.get(key)
  if (!seen || seen.rv !== rv) leaseSeen.set(key, { rv, at: Date.now() })
  const mine = l.spec.holderIdentity === holder
  if (!mine && Date.now() - leaseSeen.get(key)!.at <= durS * 1000) return false
  const next = { ...l, spec: { ...l.spec, holderIdentity: holder, leaseDurationSeconds: durS, renewTime: micro(), ...(mine ? {} : { acquireTime: micro(), leaseTransitions: (l.spec.leaseTransitions ?? 0) + 1 }) } }
  const r = await kube('PUT', `${path}/${name}`, next) // carries the resourceVersion read above: refused if anyone wrote since
  if (r.status >= 300) return false
  leaseSeen.set(key, { rv: r.json.metadata.resourceVersion, at: Date.now() })
  return true
}
async function leaderTick() {
  const was = isLeader
  isLeader = await acquireLease(NS, 'peat-control-leader', ME, LEASE_S).catch(() => false)
  if (isLeader !== was) console.log(`leader: ${ME} ${isLeader ? 'is now the acting control layer' : 'is no longer the acting control layer'}`)
}
/** Pods of ours that Deployments own on a machine this layer has declared dead (a control layer or gateway pod): deleted at once so that their Service endpoints go and the Deployment makes the next one elsewhere. */
async function reapHaPods() {
  for (const ns of [NS, GW_NS]) {
    const pods = ok(await kube('GET', `/api/v1/namespaces/${ns}/pods?labelSelector=peat-ha%3Dtrue`), 'list ha pods').json.items as any[]
    for (const p of pods) {
      // a pod that ended (a rollout's old pod reads `Error`) is removed; a pod on a dead machine is replaced
      if (p.status?.phase !== 'Failed' && !nodeStates.get(p.spec?.nodeName)?.dead) continue
      console.log(`reconcile: replacing ${ns}/${p.metadata.name}: ${p.status?.phase === 'Failed' ? 'it has ended' : `on dead machine ${p.spec.nodeName}`}`)
      bump('ha_pods_replaced_total')
      await kube('DELETE', `/api/v1/namespaces/${ns}/pods/${p.metadata.name}`, { gracePeriodSeconds: 0 })
    }
  }
}
async function gatewayPeers(): Promise<{ name: string; ip: string }[]> {
  const pods = ok(await kube('GET', `/api/v1/namespaces/${GW_NS}/pods?labelSelector=app%3Dpeat-gateway`), 'list gateways').json.items as any[]
  return pods.filter((p) => !p.metadata.deletionTimestamp && p.status?.podIP && podReady(p) && !nodeStates.get(p.spec?.nodeName)?.dead).map((p) => ({ name: p.metadata.name as string, ip: p.status.podIP as string }))
}

// ---- calls
// A make claims its name and its ids first (`making-<name>`, records.ts), so a pass that dies half way is finished by the next with the
// same tenant and timeline, not begun again beside them; each step accepts what an earlier pass already made. The record (its ConfigMap,
// then the status) is written last, by the loop, once everything it names exists.
async function createDb(name: string, asked: unknown, namespace: string): Promise<Db> {
  if (!DB_NAME.test(name)) throw new Error(`bad name ${name}`)
  const pg = requestedPgVersion(asked, DEFAULT_PG)
  computeImageFor(pg, COMPUTE_IMAGES) // refused before anything is made when no compute could serve this version
  const ids = await beginMaking(RECORDS, name, namespace, { tenant: hex32(), timeline: hex32() }, { root: true, mode: 'primary', parent: '', from: '' })
  if ('taken' in ids) throw new Error(`the name ${name} is being made in ${ids.taken}`)
  const { tenant, timeline } = ids
  const earlier = await orphanedTenant(RECORDS, name, tenant)
  if (earlier) throw new Refused(`an earlier database named ${name} lost its record and its data is still in the store (tenant ${earlier}): its login db-${name}-login names it. Write its record again (a copy of db-${name}) and run /records/copy, or delete that login to make a new, empty database under the name`)
  ok(await storcon('POST', '/v1/tenant', { new_tenant_id: tenant, placement_policy: { Attached: SECONDARIES } }), 'create tenant', [409])
  ok(await storcon('POST', `/v1/tenant/${tenant}/timeline`, { new_timeline_id: timeline, pg_version: pg }), 'create timeline', [409])
  await ensureLogin(RECORDS, name, tenant, () => randomBytes(12).toString('hex'))
  const port = await claimPort(RECORDS, name, timeline, PORT_BASE, await usedPorts())
  await apply(`/api/v1/namespaces/${DB_NS}/services`, name, nameService(name))
  return { name, namespace, tenant, timeline, parent: '', from: '', root: true, mode: 'primary', port, created: new Date().toISOString(), pg, closed: false }
}
/** A make that cannot go on until a person acts: the loop writes it as Failed, not tried again in silence. */
class Refused extends Error {}
/** The ports the records and the statuses hold (a claim is the other half: claimPort). */
const usedPorts = async () => new Set([...(await readRecords(RECORDS)).values()].map((r) => r.db.port).concat((await listDbs()).map((d) => d.port)))

async function attachedPageserverApi(tenant: string): Promise<string> {
  return `http://${pageserverHost(await attachedNode(tenant))}:9898`
}

async function makeBranch(from: string, name: string, at: string | undefined, namespace: string): Promise<{ db: Db; lsn: string | null; kind?: string }> {
  const parent = await getDb(from)
  if (!parent) throw new Error(`no database ${from}`)
  const root = parent.root ? parent.name : parent.parent
  const ids = await beginMaking(RECORDS, name, namespace, { tenant: parent.tenant, timeline: hex32() }, { root: false, mode: 'primary', parent: root, from })
  if ('taken' in ids) throw new Error(`the name ${name} is being made in ${ids.taken}`)
  let lsn: string | null = null
  let kind: string | undefined
  // a database recovered under its own name (recoverDatabase) starts its timeline at the recovery point: a moment before it is on the old timeline the recovery kept (the pageserver refuses a branch below a timeline's branch point)
  let ancestor = parent.timeline
  const recoveries = at ? await recoveryMarks(parent.tenant) : []
  if (at) {
    if (/^[0-9A-F]+\/[0-9A-F]+$/i.test(at)) { lsn = at; ancestor = ancestorFor(recoveries, parent.timeline, at) }
    else {
      // Neon's own answer to "what was the log position at this moment": the pageserver's get_lsn_by_timestamp
      for (;;) {
        const r = await fetch(`${await attachedPageserverApi(parent.tenant)}/v1/tenant/${parent.tenant}/timeline/${ancestor}/get_lsn_by_timestamp?timestamp=${encodeURIComponent(at)}`, { headers: scoped('pageserverapi'), signal: AbortSignal.timeout(30_000) })
        if (!r.ok) throw new Error(`get_lsn_by_timestamp: ${r.status} ${await r.text()}`)
        const j = (await r.json()) as any
        kind = j.kind
        const older = olderTimelineFor(recoveries, ancestor, j.kind)
        if (older) { ancestor = older; continue }
        if (j.kind !== 'present') throw new Error(`no commit at ${at}: ${JSON.stringify(j)}`)
        lsn = j.lsn
        break
      }
    }
  }
  const timeline = ids.timeline
  ok(await storcon('POST', `/v1/tenant/${parent.tenant}/timeline`, { new_timeline_id: timeline, ancestor_timeline_id: ancestor, ...(lsn ? { ancestor_start_lsn: lsn } : {}), pg_version: parent.pg }), 'create branch timeline', [409])
  const port = await claimPort(RECORDS, name, timeline, PORT_BASE, await usedPorts())
  await apply(`/api/v1/namespaces/${DB_NS}/services`, name, nameService(name))
  return { db: { name, namespace, tenant: parent.tenant, timeline, parent: root, from, root: false, mode: 'primary', port, created: new Date().toISOString(), pg: parent.pg, closed: false }, lsn, kind }
}

async function makeReplica(from: string, name: string, namespace: string): Promise<Db> {
  const parent = await getDb(from)
  if (!parent) throw new Error(`no database ${from}`)
  const root = parent.root ? parent.name : parent.parent
  const ids = await beginMaking(RECORDS, name, namespace, { tenant: parent.tenant, timeline: parent.timeline }, { root: false, mode: 'replica', parent: root, from })
  if ('taken' in ids) throw new Error(`the name ${name} is being made in ${ids.taken}`)
  // a replica is a second compute on the parent's own timeline: same tenant and timeline, read only; no route (it is reached through its own Service)
  return { name, namespace, tenant: parent.tenant, timeline: parent.timeline, parent: root, from, root: false, mode: 'replica', port: 0, created: new Date().toISOString(), pg: parent.pg, closed: false }
}

// A deleted database's compute and names go at once; its data in the store stays for the Platform's hold (DELETE_HOLD: `7d` by default,
// `never` keeps it), recorded in a ConfigMap `held-<name>-<timeline>` in the platform namespace, and is removed by purgeHeld (records.ts)
// when the hold is over. A drill's database is removed at once, once its record is gone. The port claim and the login go with the data.
const DELETE_HOLD = process.env['DELETE_HOLD'] || '7d'
async function deleteDb(d: Db, holdData: boolean): Promise<void> {
  const name = d.name
  await stopCompute(name, true)
  await del(depPath(name))
  await del(`/api/v1/namespaces/${DB_NS}/services/${name}`)
  await del(`/apis/cert-manager.io/v1/namespaces/${CNS}/certificates/${tlsSecret(name)}`)
  await del(`/api/v1/namespaces/${CNS}/secrets/${tlsSecret(name)}`)
  await del(`/api/v1/namespaces/${CNS}/secrets/compute-${name}`)
  const after = holdData ? deleteAfter(DELETE_HOLD) : new Date(0)
  await hold(RECORDS, d, after)
  forgetDbs()
  console.log(`delete: ${name} (${d.namespace}): compute removed; its data ${after === null ? 'is kept (the hold is never)' : after.getTime() === 0 ? 'goes now (a drill\'s)' : `is kept until ${after.toISOString()}`}`)
  if (!holdData) await purgeHeld(RECORDS, await notDeleting())
}
/** Every database whose object is not being deleted: what the purge must never take data from. */
const notDeleting = async (): Promise<Db[]> => (await dbObjects()).filter((o) => !o.metadata.deletionTimestamp).map(recordOf).filter((d): d is Db => d !== null)

// ---- the Database objects: made, closed, opened, stopped and deleted as they ask (databaseAction, databases.ts, decides)
const finalize = (o: any) => kube('PATCH', dbObjectPath(o.metadata.namespace, o.metadata.name), { metadata: { finalizers: [...(o.metadata.finalizers ?? []), FINALIZER] } }, 'application/merge-patch+json')
const unfinalize = (o: any) => kube('PATCH', dbObjectPath(o.metadata.namespace, o.metadata.name), { metadata: { finalizers: (o.metadata.finalizers ?? []).filter((f: string) => f !== FINALIZER) } }, 'application/merge-patch+json')
const working = new Set<string>()
/** How long a close waits between refusing new starts and stopping the compute: longer than any control layer pod's cache of the records. */
const CLOSE_SETTLE_MS = 1500
async function reconcileDatabases() {
  if (!isLeader) return
  forgetDbs()
  const all = await dbObjects()
  const recs = await readRecords(RECORDS)
  const records = new Map([...recs].map(([n, r]) => [n, { namespace: r.db.namespace }]))
  const busy = await replacementOpen()
  for (const o of all) {
    const key = `${o.metadata.namespace}/${o.metadata.name}`
    if (working.has(key)) continue
    const ns = o.metadata.namespace, name = o.metadata.name
    // a record the status holds and the platform's ConfigMaps lost is written again from the status
    const s = recordOf(o)
    if (s && !o.metadata.deletionTimestamp && !o.metadata.annotations?.[COPYING] && !recs.has(name)) await writeRecord(RECORDS, s).then(() => console.log(`database: ${key}: its record written again from its status`), (e) => console.log(`database ${key}: write its record: ${e}`))
    const act = databaseAction(o, all, { replacementOpen: busy, records })
    if (act.do === 'none') continue
    working.add(key)
    try {
      if (act.do === 'finalize') ok(await finalize(o), `finalize ${key}`)
      else if (act.do === 'refuse') { if (o.status?.message !== act.why) await patchDbStatus(ns, name, s ? { message: act.why } : { phase: 'Failed', message: act.why }) }
      else if (act.do === 'wait') { if (o.status?.message !== act.why) await patchDbStatus(ns, name, { ...(s ? {} : { phase: 'Creating' }), message: act.why }) }
      else if (act.do === 'rebuild') {
        // the requests its spec holds now count as answered: the status that said so is gone, and a stop answered before the wipe is not
        // run again on a database a client may be using (a request not yet answered when the status went is dropped: ask again)
        const answered = Object.fromEntries(Object.entries(o.spec?.requests ?? {}).filter(([, v]) => typeof v === 'string' && v))
        await saveDb(recs.get(name)!.db, { ...(o.spec?.closed ? { phase: 'Closed', message: "closed: its status was written again from the platform's record of it" } : { phase: 'Ready', message: "ready: its status was written again from the platform's record of it" }), ...(Object.keys(answered).length ? { answered } : {}) })
        await endMaking(RECORDS, name).catch((e) => console.log(`database ${key}: release its claim: ${e}`))
        console.log(`database: ${key}: its status written again from its record`)
      } else if (act.do === 'make') {
        if (o.status?.phase !== 'Failed') await patchDbStatus(ns, name, { phase: 'Creating', message: `making a ${act.kind === 'root' ? 'new database' : act.kind === 'branch' ? (act.at ? `restore of ${act.from} at ${act.at}` : `branch of ${act.from}`) : `replica of ${act.from}`}` })
        const d = act.kind === 'root' ? await createDb(name, act.pg, ns) : act.kind === 'branch' ? (await makeBranch(act.from!, name, act.at, ns)).db : await makeReplica(act.from!, name, ns)
        await saveDb(d, { phase: 'Ready', message: 'ready: it sleeps until a client connects', awake: false })
        await endMaking(RECORDS, name).catch((e) => console.log(`database ${key}: release its claim: ${e}`)) // the database works: a claim left behind goes with its delete
        console.log(`database: ${key} made (${act.kind}${act.from ? ` of ${act.from}` : ''})`)
      } else if (act.do === 'delete') {
        const rec = recs.get(name)?.db
        const d = s ?? (rec && rec.namespace === ns ? rec : null)
        await patchDbStatus(ns, name, { phase: 'Deleting', message: 'deleting' }).catch(() => {})
        if (d) await deleteDb(d, act.holdData)
        else {
          // a make that never finished: what it made of the storage goes at once (no client had it)
          const u = await unfinishedMake(RECORDS, name, ns)
          if (u) { await hold(RECORDS, u, new Date(0)); await purgeHeld(RECORDS, await notDeleting()) }
        }
        ok(await unfinalize(o), `let ${key} go`, [404])
      } else if (act.do === 'close') {
        // closed first, so that a start from here on refuses it (a start reads the object past every cache), then the compute stops: a
        // pooled client that reconnects the moment its session ends finds the database closed, not woken
        await patchDbStatus(ns, name, { phase: 'Closed', message: 'closed: every connection is refused until it is opened' })
        await new Promise((res) => setTimeout(res, CLOSE_SETTLE_MS))
        await stopCompute(name)
        await patchDbStatus(ns, name, { awake: false })
      } else if (act.do === 'open') {
        await patchDbStatus(ns, name, { phase: 'Ready', message: 'ready: it sleeps until a client connects' })
      } else if (act.do === 'stop') {
        await stopCompute(name)
        await patchDbStatus(ns, name, { answered: { ...(o.status?.answered ?? {}), stop: act.request }, awake: false, message: 'asleep: the next connection wakes it' })
      }
    } catch (e) {
      console.log(`database ${key}: ${act.do}: ${e}`)
      if (act.do === 'make' && e instanceof Refused) { if (o.status?.message !== e.message) await patchDbStatus(ns, name, { phase: 'Failed', message: e.message }).catch(() => {}) }
      else if (act.do === 'make') await patchDbStatus(ns, name, { phase: 'Creating', message: `making it failed, tried again: ${String(e).slice(0, 300)}` }).catch(() => {})
    } finally { working.delete(key) }
  }
}

// ---- a database whose safekeepers no longer know its timeline, recovered under its own name (`POST /db/<name>/recover`, operator only: no caller class lists it, so only the pod's own loopback reaches it)
// After a total loss of the safekeepers the pageserver's copy is intact and no safekeeper holds the timeline. The database's record is repointed at a NEW timeline that is a branch of the old one at the pageserver's last uploaded
// record: its name, route, port, certificate and login are the same, so a client sees an outage and reconnects. The old timeline stays as the new one's ancestor and nothing in the object store is deleted (not by this code:
// the branch is made by the storage controller's ordinary create, and the old timeline is never deleted). Decisions: `recoverDecision`, `recoveryPoint`, `recoveryScope`, `recoverStep` (storage-spec.ts).
const recovering = new Set<string>()
/** The recoveries' marks of one tenant (`recovered-<old timeline>`, label `peat-recovered`): what a restore from before a recovery and a recovery left half way read. */
async function recoveryMarks(tenant: string): Promise<RecoveryMark[]> {
  const r = ok(await kube('GET', `/api/v1/namespaces/${NS}/configmaps?labelSelector=peat-recovered`), 'list recoveries')
  return (r.json.items as any[]).filter((i) => i.data?.tenant === tenant).map((i) => ({ name: i.metadata.name as string, tenant: i.data.tenant, from: i.data.from, to: i.data.to, lsn: i.data.lsn, state: i.data.state ?? '' }))
}
async function recoverDatabase(name: string, dry: boolean): Promise<{ status: number; body: any }> {
  const refuse = (status: number, error: string, more: object = {}) => ({ status, body: { error, ...more } })
  const d = await getDb(name)
  if (!d) return refuse(404, `no database ${name}`)
  if (recovering.has(name)) return refuse(409, `a recovery of ${name} is already running on this pod`)
  recovering.add(name)
  try {
    // a call that stopped half way through the records (the primary is repointed first) is finished before anything is decided: its new timeline is known to the safekeepers, so the decision below would refuse it
    const half = unfinishedRepoint(await recoveryMarks(d.tenant), (await listDbs()).map((r) => ({ name: r.name, tenant: r.tenant, timeline: r.timeline, mode: r.mode })), { name: d.name, tenant: d.tenant, timeline: d.timeline, mode: d.mode })
    if (half) {
      if (dry) return { status: 200, body: { dry: true, finishing: half.mark.name, to: half.mark.to, repoint: half.names } }
      const all = await listDbs()
      for (const n of half.names) { await stopCompute(n, true); const r = all.find((x) => x.name === n); if (r) await saveDb({ ...r, timeline: half.mark.to }) }
      await kube('PATCH', `/api/v1/namespaces/${NS}/configmaps/${half.mark.name}`, { data: { state: 'done' } }, 'application/merge-patch+json')
      console.log(`recover: finished ${half.mark.name} for ${d.name}: ${half.names.join(', ')} repointed to ${half.mark.to}`)
      return { status: 200, body: { finished: half.mark.name, to: half.mark.to, repointed: half.names, compute: 'left asleep: the first client wakes it' } }
    }
    const list = await storcon('GET', '/control/v1/safekeeper')
    if (list.status !== 200 || !Array.isArray(list.json)) return refuse(503, 'the storage controller did not answer')
    const members = (list.json as any[]).filter((x) => !/^decom/i.test(String(x.scheduling_policy)))
    const probes: MemberProbe[] = await Promise.all(members.map(async (x) => {
      const r = await fetch(`http://${x.host}:${x.http_port}/v1/tenant/${d.tenant}/timeline/${d.timeline}`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(3000) }).catch(() => null)
      return { id: x.id as number, active: x.scheduling_policy === 'Active', probe: r?.ok ? 'knows' as const : r?.status === 404 ? 'unknown' as const : 'silent' as const }
    }))
    // the timeline's own members too (the hosts its compute spec names), whatever their policy: one retired while it still held the timeline is asked if it can be (a pod that is gone cannot be)
    const spec = await kube('GET', `/api/v1/namespaces/${CNS}/secrets/compute-${d.name}`)
    const named: string[] = spec.status === 200 ? (JSON.parse(Buffer.from(spec.json.data['config.json'], 'base64').toString()).spec?.safekeeper_connstrings ?? []) : []
    for (const m of timelineOwnMembers(named, members.map((x) => String(x.host)))) {
      const r = await fetch(`http://${m.host}:7676/v1/tenant/${d.tenant}/timeline/${d.timeline}`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(3000) }).catch(() => null)
      const podExists = r ? true : (await kube('GET', `/api/v1/namespaces/${NS}/pods/safekeeper-w${m.id}-0`)).status === 200
      probes.push({ id: m.id, active: false, probe: ownMemberProbe({ status: r ? r.status : null, podExists }) })
    }
    const verdict = recoverDecision({ mode: d.mode, probes, replacementOpen: await replacementOpen() })
    if (!verdict.allow) return refuse(409, `${name} is not recovered: ${verdict.why}`, { probes })
    const scope = recoveryScope((await listDbs()).map((r) => ({ name: r.name, tenant: r.tenant, timeline: r.timeline, mode: r.mode })), { name: d.name, tenant: d.tenant, timeline: d.timeline, mode: d.mode })
    // an earlier call that stopped half way recorded the new timeline and the point it branches at: both are reused (the new timeline already holds that point)
    const markName = `recovered-${d.timeline}`
    const mark = await kube('GET', `/api/v1/namespaces/${NS}/configmaps/${markName}`)
    const api = await attachedPageserverApi(d.tenant)
    const psGet = (tl: string) => fetch(`${api}/v1/tenant/${d.tenant}/timeline/${tl}`, { headers: scoped('pageserverapi'), signal: AbortSignal.timeout(10_000) })
    const oldInfo = await psGet(d.timeline)
    if (!oldInfo.ok) return refuse(502, `the attached pageserver does not give the timeline: ${oldInfo.status} ${(await oldInfo.text()).slice(0, 200)}`)
    const info = (await oldInfo.json()) as any
    const point = recoveryPoint(info)
    if (!point.ok) return refuse(409, `${name} is not recovered: ${point.why}`)
    let lsn = mark.status === 200 ? String(mark.json.data.lsn) : point.lsn
    let newTimeline = mark.status === 200 ? String(mark.json.data.to) : hex32()
    const preview = { database: name, from: d.timeline, to: newTimeline, lsn, unuploadedBytes: point.unuploadedBytes, repoint: scope.repoint, othersOfTheTenant: scope.others, resumed: mark.status === 200 }
    if (dry) return { status: 200, body: { dry: true, ...preview, note: 'unuploadedBytes is what the pageserver holds past the uploaded record: it is lost unless the pageserver uploads it first (its own flush, within the checkpoint timeout) and this is asked again' } }
    const dep = await kube('GET', depPath(name))
    const awake = dep.status === 200 && dep.json.spec.replicas === 1
    for (const n of scope.repoint) await stopCompute(n, true) // a compute on the old timeline cannot be used again, and its spec must be written afresh
    if (mark.status !== 200) {
      const made = await kube('POST', `/api/v1/namespaces/${NS}/configmaps`, { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: markName, namespace: NS, labels: { 'peat-recovered': 'true' } }, data: { database: name, tenant: d.tenant, from: d.timeline, to: newTimeline, lsn, at: new Date().toISOString(), state: 'started' } })
      // a mark already there is another call's (a second pod's, a rerun's): its timeline and position decide, so that no two timelines are made for one recovery
      if (made.status === 409) { const other = ok(await kube('GET', `/api/v1/namespaces/${NS}/configmaps/${markName}`), 'read the recovery mark').json.data; newTimeline = String(other.to); lsn = String(other.lsn) } else ok(made, 'mark the recovery')
    }
    const there = await psGet(newTimeline)
    const state: 'absent' | 'ours' | 'foreign' = there.status === 404 ? 'absent' : there.ok && ((await there.json()) as any).ancestor_timeline_id === d.timeline ? 'ours' : 'foreign'
    if (there.status !== 404 && !there.ok) return refuse(502, `the attached pageserver does not give the new timeline: ${there.status}`)
    const step = recoverStep(state)
    if (step.do === 'refuse') return refuse(409, `${name} is not recovered: ${step.why}`)
    if (step.do === 'create') ok(await storcon('POST', `/v1/tenant/${d.tenant}/timeline`, { new_timeline_id: newTimeline, ancestor_timeline_id: d.timeline, ancestor_start_lsn: lsn, pg_version: d.pg }), 'create the recovered timeline')
    // the records move only now that the new timeline exists: until then a failure leaves the name as it was
    const all = await listDbs()
    for (const n of scope.repoint) { const r = all.find((x) => x.name === n); if (r) await saveDb({ ...r, timeline: newTimeline }) }
    await kube('PATCH', `/api/v1/namespaces/${NS}/configmaps/${markName}`, { data: { state: 'done' } }, 'application/merge-patch+json')
    bump('db_recoveries_total')
    console.log(`recover: ${name} (tenant ${d.tenant}) timeline ${d.timeline} -> ${newTimeline} at ${lsn} (${point.unuploadedBytes} bytes past it were not uploaded); ${scope.repoint.join(', ')} repointed`)
    let ready = false
    if (awake) {
      await startCompute({ ...d, timeline: newTimeline })
      for (let i = 0; i < 60 && !ready; i++) { ready = podReady(await livePod(name)); if (!ready) await new Promise((res) => setTimeout(res, 1000)) }
    }
    return { status: 200, body: { ...preview, resumed: mark.status === 200, compute: awake ? (ready ? 'started and Ready' : 'started, not Ready after 60 s') : 'left asleep: the first client wakes it' } }
  } finally { recovering.delete(name) }
}

// ---- what the gateway copy reads: a database as a CloudNativePG "Cluster" (annotations and status), so that the gateway program runs on it as it is
// A database as a CloudNativePG "Cluster" (annotations and status), built from its record, its Deployment's replicas and its compute pods.
function clusterRow(d: Db, pods: any[], replicas: number | undefined) {
  const mine = pods.filter((p) => p.metadata.labels?.compute === d.name && !p.metadata.deletionTimestamp).sort((a, b) => (a.metadata.creationTimestamp < b.metadata.creationTimestamp ? 1 : -1))
  // a pod on a machine that stopped answering still reads Ready until Kubernetes notices (about 50 s): a machine this layer has declared dead does not count
  const pod = mine.find((p) => !nodeStates.get(p.spec?.nodeName)?.dead) ?? mine[0]
  const ready = podReady(pod) && !nodeStates.get(pod?.spec?.nodeName)?.dead ? 1 : 0
  // asleep means the Deployment is at zero (a compute being replaced is awake and not ready: the gateway holds its callers, as it holds them for a database that is starting)
  const awake = (replicas ?? 0) > 0
  return {
    metadata: {
      name: d.name,
      // changes whenever a compute is made or removed, so that a sleep decided on an older reading is refused
      resourceVersion: `${pod ? pod.metadata.uid : 'none'}:${ready}:${awake ? 1 : 0}`,
      annotations: { [SLEEP_ENABLED]: 'true', [SLEEP_IDLE_MINUTES]: IDLE_MINUTES, [HIBERNATION]: awake ? 'off' : 'on' },
    },
    status: { readyInstances: ready, ...(pod ? { currentPrimary: pod.metadata.name } : {}) },
  }
}
// The whole list is O(databases) and each gateway asks for it every 5 s; kept for 3 s so that callers' reads do not each rebuild it.
let clustersCache: { at: number; rows: Promise<any[]> } | undefined
async function clusters(): Promise<any[]> {
  if (clustersCache && Date.now() - clustersCache.at < 3000) return clustersCache.rows
  const rows = (async () => {
    const dbs = (await listDbs()).filter((d) => d.mode === 'primary')
    const pods = ok(await kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=app%3Dpeat-compute`), 'list pods').json.items as any[]
    const deps = new Map((ok(await kube('GET', `/apis/apps/v1/namespaces/${CNS}/deployments?labelSelector=peat-compute-deploy`), 'list deployments').json.items as any[]).map((x) => [x.metadata.labels.compute as string, x.spec.replicas as number]))
    return dbs.map((d) => clusterRow(d, pods, deps.get(d.name)))
  })()
  clustersCache = { at: Date.now(), rows }
  rows.catch(() => { if (clustersCache?.rows === rows) clustersCache = undefined })
  return rows
}
/** One database's row, without reading the others: what a gateway asks for each time a caller needs a wake or a look (with 1,001 databases the list took long enough under load that a replaced compute's callers waited 50 s). */
async function clusterOne(name: string): Promise<any | null> {
  const d = await getDb(name)
  if (!d || d.mode !== 'primary') return null
  const [pods, dep] = await Promise.all([
    kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=${encodeURIComponent(computeSelector(name))}`),
    kube('GET', depPath(name)),
  ])
  ok(pods, 'list pods')
  return clusterRow(d, pods.json.items as any[], dep.status === 200 ? dep.json.spec.replicas : 0)
}

// The routes the gateways listen on, as the Service rows the gateway program reads (it reads Services; here they are built from the database records, so no Service exists per database).
let routesCache: { at: number; rows: Promise<any[]> } | undefined
async function routeRows(): Promise<any[]> {
  if (routesCache && Date.now() - routesCache.at < 3000) return routesCache.rows
  const rows = (async () => (await listDbs()).filter((d) => d.mode === 'primary' && d.port && !d.closed).map((d) => ({ metadata: { name: d.name, annotations: { [ROUTE_ANNOTATION]: `${NS}/${d.name}` } }, spec: { ports: [{ port: 5432, targetPort: d.port }] } })))()
  routesCache = { at: Date.now(), rows }
  rows.catch(() => { if (routesCache?.rows === rows) routesCache = undefined })
  return rows
}
// Where each running compute is: database name -> pod address, for Ready pods on machines not declared dead. The gateway dials these directly (no compute Service); kept 1 s.
let addrsCache: { at: number; rows: Promise<Record<string, string>> } | undefined
async function computeAddrs(): Promise<Record<string, string>> {
  if (addrsCache && Date.now() - addrsCache.at < 1000) return addrsCache.rows
  const rows = (async () => {
    const pods = ok(await kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=app%3Dpeat-compute`), 'list pods').json.items as any[]
    const out: Record<string, string> = {}
    for (const p of pods) if (!p.metadata.deletionTimestamp && p.status?.podIP && podReady(p) && !nodeStates.get(p.spec?.nodeName)?.dead && p.metadata.labels?.compute) out[p.metadata.labels.compute] = p.status.podIP
    return out
  })()
  addrsCache = { at: Date.now(), rows }
  rows.catch(() => { if (addrsCache?.rows === rows) addrsCache = undefined })
  return rows
}

const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } })
const read = async (req: Request): Promise<any> => { const t = await req.text(); return t ? JSON.parse(t) : {} }

// ---- the compute hook: the tenant's pageserver moved. Repoint each running compute of the tenant: ask compute_ctl to reconfigure (its /configure takes a whole spec), else restart the compute.
// Every compute of the tenant is handled at once (a compute on a machine that is gone must not hold the others' repoint for its 20 s). A sleeping database's spec is NOT refreshed here (a wake writes a fresh one):
// an earlier version listed every database record on every notification, and with 1,001 databases and about 680 tenants moved at once that made a pageserver's loss take 76 s to reach a compute instead of 31 s (2026-11-19).
async function repoint(tenant: string, nodeId: number): Promise<{ repointed: string[]; restarted: string[]; skipped: string[] }> {
  const out = { repointed: [] as string[], restarted: [] as string[], skipped: [] as string[] }
  const pods = ok(await kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=app%3Dpeat-compute%2Ctenant%3D${tenant}`), 'list pods').json.items as any[]
  const running = new Set<string>()
  await Promise.all(pods.map(async (p) => {
    if (p.metadata.deletionTimestamp) return
    const name = p.metadata.labels.compute as string
    // a pod with no address yet (being made after a replacement) reads its spec when it starts: bring the stored one up to date
    if (!p.status?.podIP) { const dd = await getDb(name); if (dd) await refreshSpec(dd).catch(() => {}); return }
    running.add(name)
    const d = await getDb(name)
    if (!d) return
    const specSecret = ok(await kube('GET', `/api/v1/namespaces/${CNS}/secrets/compute-${name}`), 'read spec').json.data['config.json']
    const cfg = JSON.parse(Buffer.from(specSecret, 'base64').toString())
    cfg.spec.pageserver_connstring = psConnstr(pageserverHost(nodeId))
    await kube('PATCH', `/api/v1/namespaces/${CNS}/secrets/compute-${name}`, { stringData: { 'config.json': JSON.stringify(cfg) } }, 'application/merge-patch+json')
    if (nodeStates.get(p.spec?.nodeName)?.dead) { out.skipped.push(name); return } // the reconciler replaces it, from the spec just written
    let done = false
    try {
      const r = await fetch(`http://${p.status.podIP}:3080/configure`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${await adminToken()}` }, body: JSON.stringify(cfg), signal: AbortSignal.timeout(20_000) })
      done = r.ok
      if (!r.ok) console.log(`configure ${name}: ${r.status} ${(await r.text()).slice(0, 200)}`)
    } catch (e) { console.log(`configure ${name} failed: ${e}`) }
    if (done) out.repointed.push(name)
    else {
      // fall back: a new compute from the updated spec (open sessions are lost): delete the pod, the Deployment makes the next
      await replaceCompute(p, d, 'the hook could not reconfigure it')
      out.restarted.push(name)
    }
  }))
  return out
}


// ---- the compute hook, second kind: a timeline's safekeepers changed. The controller sends the new generation and the members' host names (the port is the safekeepers' libpq port, 5454).
async function resafekeepers(tenant: string, timeline: string, generation: number, hosts: string[]): Promise<{ configured: string[]; replaced: string[]; stored: string[] }> {
  const out = { configured: [] as string[], replaced: [] as string[], stored: [] as string[] }
  if (!hosts.length) return out
  const dbs = (await listDbs()).filter((d) => d.tenant === tenant && d.timeline === timeline && d.mode === 'primary')
  await Promise.all(dbs.map(async (d) => {
    const got = await kube('GET', `/api/v1/namespaces/${CNS}/secrets/compute-${d.name}`)
    if (got.status !== 200) return
    const cfg = JSON.parse(Buffer.from(got.json.data['config.json'], 'base64').toString())
    cfg.spec.safekeeper_connstrings = hosts.map((h) => `${h}:5454`)
    cfg.spec.safekeepers_generation = generation
    await kube('PATCH', `/api/v1/namespaces/${CNS}/secrets/compute-${d.name}`, { stringData: { 'config.json': JSON.stringify(cfg) } }, 'application/merge-patch+json')
    out.stored.push(d.name)
    const pod = await livePod(d.name)
    if (!pod?.status?.podIP) return
    let done = false
    try {
      const r = await fetch(`http://${pod.status.podIP}:3080/configure`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${await adminToken()}` }, body: JSON.stringify(cfg), signal: AbortSignal.timeout(20_000) })
      done = r.ok
      if (!r.ok) console.log(`configure ${d.name}: ${r.status} ${(await r.text()).slice(0, 200)}`)
    } catch (e) { console.log(`configure ${d.name} failed: ${e}`) }
    if (done) out.configured.push(d.name)
    else { await replaceCompute(pod, d, 'the hook could not reconfigure it for the new safekeepers'); out.replaced.push(d.name) }
  }))
  return out
}

// ---- who may call what
// The API has no login (the network policy names its callers, and a `kubectl exec` or port-forward is the admin's). So a caller is held to the routes it uses, by the class its pod address says it is: the gateways (which accept
// connections from ANY pod of the cluster, so are the piece an attacker would reach first), the safekeepers (one question), the storage controller (its hook) and the computes (one question: a compute runs tenants' SQL). **It fails
// closed:** an address that is in no class gets `/healthz` and `/metrics` and nothing else (the kubelet's probes and Prometheus are such callers); only a call from the pod's own loopback (`kubectl exec`, a port-forward) is not
// narrowed. A pod that is new (a replacement gateway, a restarted compute) is not in a map made seconds ago, so an address not found is looked up once more, fresh (at most twice a second), before it is called unknown.
// DB_NAME (databases.ts), NAMESPACE_NAME and POD_NAME are what a name in a path, a query or a body may be: the control layer builds a Kubernetes API path from it with its own token, so it never carries a `/` or a `..`.
const NAMESPACE_NAME = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/
const POD_NAME = /^[a-z0-9]([a-z0-9-]{0,250}[a-z0-9])?$/
const CALLER_ROUTES: Record<string, [string, RegExp][]> = {
  gateway: [
    ['GET', /^\/(healthz|metrics|clusters|services|compute-addrs|gateway\/peers)$/],
    ['GET', /^\/(cluster|route)\/[a-z][a-z0-9-]{0,30}$/],
    ['GET', /^\/pod\/[a-z0-9]([a-z0-9-]{0,250}[a-z0-9])?$/],
    ['POST', /^\/gateway\/lease$/],
    ['POST', /^\/db\/[a-z][a-z0-9-]{0,30}\/(start|stop)$/],
  ],
  safekeeper: [['GET', /^\/sk\/\d+\/may-start-empty$/]],
  controller: [['PUT', /^\/hook\/[^/]+$/]],
  compute: [['GET', /^\/compute\/current$/]],
  unknown: [['GET', /^\/(healthz|metrics)$/]],
}
let callerCache: { at: number; ips: Promise<Map<string, string>> } | undefined
async function callerIps(fresh = false): Promise<Map<string, string>> {
  if (callerCache && Date.now() - callerCache.at < (fresh ? 500 : 3000)) return callerCache.ips
  const ips = (async () => {
    const m = new Map<string, string>()
    for (const p of ok(await kube('GET', `/api/v1/namespaces/${GW_NS}/pods`), 'list gateway pods').json.items as any[]) if (p.status?.podIP) m.set(p.status.podIP, 'gateway')
    const sel = encodeURIComponent('app in (safekeeper,storage-controller)')
    for (const p of ok(await kube('GET', `/api/v1/namespaces/${NS}/pods?labelSelector=${sel}`), 'list storage pods').json.items as any[]) if (p.status?.podIP) m.set(p.status.podIP, p.metadata.labels.app === 'safekeeper' ? 'safekeeper' : 'controller')
    for (const p of ok(await kube('GET', `/api/v1/namespaces/${CNS}/pods?labelSelector=app%3Dpeat-compute`), 'list compute pods').json.items as any[]) if (p.status?.podIP) m.set(p.status.podIP, 'compute')
    return m
  })()
  callerCache = { at: Date.now(), ips }
  ips.catch(() => { if (callerCache?.ips === ips) callerCache = undefined })
  return ips
}
/** The class a caller is held to: null for the pod's own loopback (not narrowed), `unknown` for an address in no class or when the API cannot be read. */
async function callerClass(address: string | undefined): Promise<string | null> {
  if (address && /^(127\.|::1$|::ffff:127\.)/.test(address)) return null
  if (!address) return 'unknown'
  const ip = address.replace(/^::ffff:/, '')
  try { return (await callerIps()).get(ip) ?? (await callerIps(true)).get(ip) ?? 'unknown' } catch { return 'unknown' }
}

// Names that start with `drill` are the platform drills': the control layer refuses to give one to anyone else, so a tenant's database can never be mistaken for a drill's and wiped or deleted by one.
// The drills say so with a header on their own calls (`x-peat-drill: 1`); this is a guard against a mistake, not against a caller who means it (every caller of the creating routes has an administrator's reach).
const reservedName = (name: unknown, req: Request) => typeof name === 'string' && name.startsWith('drill') && req.headers.get('x-peat-drill') !== '1'
const RESERVED_MESSAGE = { error: 'database names that start with drill are reserved for the platform drills' }

const hookLog: any[] = []
Bun.serve({
  port: 8080,
  idleTimeout: 120,
  async fetch(req, server) {
    const reqAt = Date.now()
    const url = new URL(req.url)
    const path = url.pathname
    try {
      const cls = await callerClass(server.requestIP(req)?.address)
      if (cls && !CALLER_ROUTES[cls]!.some(([m, re]) => m === req.method && re.test(path))) { bump('caller_refused_total'); console.log(`refused: a ${cls} may not ${req.method} ${path}`); return json({ error: `a ${cls} may not call this` }, 403) }
      if (req.method === 'GET' && path === '/healthz') return json({ ok: true })
      // the storage controller's compute hook
      if (req.method === 'PUT' && path.startsWith('/hook/')) {
        if (!hookTokenOk(req.headers.get('authorization'))) { console.log(`hook ${path}: refused, no valid token`); bump('hook_refused_total'); return json({ error: 'unauthorized' }, 401) }
        const kind = path.slice('/hook/'.length)
        const body = await read(req)
        hookLog.push({ at: new Date().toISOString(), kind, body })
        if (hookLog.length > 200) hookLog.shift()
        console.log(JSON.stringify({ kind, body }))
        if (kind === 'notify-attach') {
          const node = body.shards?.[0]?.node_id as number | undefined
          if (node) {
            // answer 200 only after the computes were repointed, as the controller expects (it retries until it succeeds)
            const r = await repoint(body.tenant_id, node)
            console.log(`repoint ${body.tenant_id} -> node ${node}: ${JSON.stringify(r)}`)
          }
        }
        if (kind === 'notify-safekeepers') {
          // a timeline's safekeepers changed (one was replaced): the computes of that timeline carry the new set and generation, by /configure, else by a new pod
          const r = await resafekeepers(body.tenant_id, body.timeline_id, body.generation, (body.safekeepers ?? []).map((x: any) => x.hostname))
          console.log(`safekeepers of ${body.tenant_id}/${body.timeline_id} -> generation ${body.generation} ${JSON.stringify((body.safekeepers ?? []).map((x: any) => x.id))}: ${JSON.stringify(r)}`)
        }
        return json({})
      }
      // An operator's way to bring a database back under its own name after every safekeeper lost its data (`?dry=1` says what it would do and changes nothing). Held to the pod's own loopback like the routes below: no caller class lists it.
      const rec = path.match(/^\/db\/([a-z][a-z0-9-]{0,30})\/recover$/)
      if (req.method === 'POST' && rec) {
        // a real call runs on the acting control layer only (as a move does): two pods would each make a timeline, since the guard against a second call is in one pod's memory
        if (!isLeader && url.searchParams.get('dry') !== '1') {
          const lease = await kube('GET', `/apis/coordination.k8s.io/v1/namespaces/${NS}/leases/peat-control-leader`)
          return json({ error: `this pod is not the acting control layer: call the pod that holds the lease, ${lease.json?.spec?.holderIdentity ?? 'unknown'}` }, 409)
        }
        const r = await recoverDatabase(rec[1]!, url.searchParams.get('dry') === '1')
        return json(r.body, r.status)
      }
      // An operator's way to give up a safekeeper that cannot come back (a total loss: every member lost its disk, so none is replaced on its own). Held to the pod's own loopback by CALLER_ROUTES (no caller class lists it, so a
      // `kubectl port-forward` is the only way in). It refuses a member that answers; otherwise the member is decommissioned in the storage controller and its StatefulSet removed, and the seeding makes three again once none is left.
      // A database's history window and checkpoint (operator only, like decommission and move: no caller class lists it): `POST /db/<name>/history?days=30&checkpoint=60` sets the tenant's `pitr_interval` (how far back a
      // restore to a moment reaches) and `checkpoint_timeout` (how long a write may sit only in the pageserver's memory and the safekeepers before it is uploaded) through the storage controller. (A database that must be restorable for a month: 30 days and 60 seconds.)
      const hist = path.match(/^\/db\/([a-z][a-z0-9-]{0,30})\/history$/)
      if (req.method === 'POST' && hist) {
        const days = Number(url.searchParams.get('days') ?? ''), checkpoint = Number(url.searchParams.get('checkpoint') ?? '')
        if (!Number.isInteger(days) || days < 7 || days > 365) return json({ error: 'days is a whole number from 7 to 365' }, 400)
        if (!Number.isInteger(checkpoint) || checkpoint < 10 || checkpoint > 600) return json({ error: 'checkpoint is a whole number of seconds from 10 to 600' }, 400)
        const d = await getDb(hist[1]!)
        if (!d) return json({ error: `no database ${hist[1]}` }, 404)
        // PATCH, not PUT: a PUT replaces the tenant's whole config (read in the storage controller's `tenant_config_set`), a PATCH changes only the fields named (checked on a drill tenant: the other field kept its value)
        const r = await storcon('PATCH', '/v1/tenant/config', { tenant_id: d.tenant, pitr_interval: `${days}days`, checkpoint_timeout: `${checkpoint}s` })
        console.log(`history: ${d.name} (${d.tenant}): ${days} days, checkpoint ${checkpoint} s: ${r.status}`)
        return json({ database: d.name, tenant: d.tenant, days, checkpoint, status: r.status }, r.status < 300 ? 200 : 502)
      }
      // MOVE a live member to another machine (operator only, like decommission: no caller class lists it): a member is put on a keeper machine this way, by migrating it, never by wiping. `moveDecision` (storage-spec.ts) says when.
      const mv = path.match(/^\/sk\/(\d+)\/move$/)
      if (req.method === 'POST' && mv) {
        const id = Number(mv[1]), to = url.searchParams.get('to') ?? ''
        // the job runs on THIS pod and its guards (the one-job rule, the reruns) are the acting pod's: only the acting control layer may start a move
        if (!isLeader) {
          const lease = await kube('GET', `/apis/coordination.k8s.io/v1/namespaces/${NS}/leases/peat-control-leader`)
          return json({ error: `this pod is not the acting control layer: call the pod that holds the lease, ${lease.json?.spec?.holderIdentity ?? 'unknown'}` }, 409)
        }
        const list = await storcon('GET', '/control/v1/safekeeper')
        if (list.status !== 200 || !Array.isArray(list.json)) return json({ error: 'the storage controller did not answer' }, 503)
        const members: MemberAt[] = await Promise.all((list.json as any[]).filter((x) => !/^decom/i.test(String(x.scheduling_policy))).map(async (x) => ({
          id: x.id as number, scheduling_policy: String(x.scheduling_policy), machine: await skMachine(x.id),
          answers: await fetch(`http://${x.host}:${x.http_port}/v1/status`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(3000) }).then((r) => r.ok).catch(() => false),
        })))
        const platformMachines = [...nodeStates.keys()]
        const verdict = moveDecision({ id, target: to, members, keeperMachines: KEEPER_MACHINES, platformMachines, replacementOpen: await replacementOpen() })
        if (!verdict.allow) return json({ error: `safekeeper ${id} is not moved: ${verdict.why}` }, 409)
        if (KEEPER_MACHINES.includes(to)) {
          const why = await keeperProblem(to)
          if (why) return json({ error: why }, 409)
        }
        console.log(`sk-move: ${verdict.why}`)
        void replaceSafekeeper(id, list.json as any[], false, to)
        return json({ moving: id, to })
      }
      const decom = path.match(/^\/sk\/(\d+)\/decommission$/)
      if (req.method === 'POST' && decom) {
        const id = Number(decom[1])
        const list = await storcon('GET', '/control/v1/safekeeper')
        const sk = Array.isArray(list.json) ? (list.json as any[]).find((x) => x.id === id) : undefined
        if (!sk) return json({ error: `no safekeeper ${id}` }, 404)
        const answers = (h: any) => fetch(`http://${h.host}:${h.http_port}/v1/status`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(3000) }).then((r) => r.ok).catch(() => false)
        const live = (list.json as any[]).filter((x) => x.id !== id && !/^decom/i.test(String(x.scheduling_policy)))
        let othersAnswering = (await Promise.all(live.map(answers))).filter(Boolean).length
        const answersItself = await answers(sk)
        // Nobody answering is not evidence of a total loss (a box reboot or a blip on this pod silences every member): each other member must carry a lost-disk mark, or have its pod Running on a Ready node and still be silent after a minute
        let lossEvidenced = false
        if (!answersItself && othersAnswering === 0) {
          const info = await Promise.all(live.map(async (x) => ({
            lostMark: (await kube('GET', `/api/v1/namespaces/${NS}/configmaps/sk-lost-${x.id}`)).status === 200,
            podRunningOnReadyNode: await (async () => {
              const pod = await kube('GET', `/api/v1/namespaces/${NS}/pods/safekeeper-w${x.id}-0`)
              const node = pod.status === 200 ? pod.json?.spec?.nodeName : null
              if (!node || pod.json?.status?.phase !== 'Running') return false
              const n = await kube('GET', `/api/v1/nodes/${node}`)
              return n.status === 200 && (n.json?.status?.conditions ?? []).some((c: any) => c.type === 'Ready' && c.status === 'True')
            })(),
          })))
          if (info.every((i) => i.lostMark || i.podRunningOnReadyNode)) {
            const needWait = info.some((i) => !i.lostMark)
            let again: boolean[] = live.map(() => false)
            if (needWait) { await new Promise((r) => setTimeout(r, 60_000)); again = await Promise.all(live.map(answers)); othersAnswering = again.filter(Boolean).length }
            lossEvidenced = totalLossEvidence(info.map((i, k) => ({ ...i, silentAfterWait: !again[k] })))
          }
        }
        // the closing count is read only when it can decide (it is slow at many databases): not for a member that answers, nor when nobody does
        const count = answersItself || othersAnswering === 0 ? null : await readTimelines(id, sk.host, Math.max(...(list.json as any[]).map((x) => x.id as number))).then((c) => ({ todo: c.todo.length, unknown: c.unknown, close: c.close })).catch(() => null)
        const verdict = decommissionDecision({ answers: answersItself, othersAnswering, lossEvidenced, count, force: url.searchParams.get('force') === '1' })
        if (!verdict.allow) return json({ error: `safekeeper ${id} is not decommissioned: ${verdict.why}`, ...(count ?? {}) }, 409)
        console.log(`sk-decommission: safekeeper ${id}: ${verdict.why}`)
        ok(await storcon('POST', `/control/v1/safekeeper/${id}/scheduling_policy`, { scheduling_policy: 'Decomissioned' }), 'decommission')
        await kube('DELETE', `/apis/apps/v1/namespaces/${NS}/statefulsets/safekeeper-w${id}`)
        await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-replacement-${id}`)
        await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-lost-${id}`)
        await kube('DELETE', `/api/v1/namespaces/${NS}/configmaps/sk-keeper-tried-${id}`)
        console.log(`sk-decommission: safekeeper ${id} decommissioned by an operator`)
        return json({ decommissioned: id })
      }
      // An operator's way to bring the records (ConfigMaps `db-<name>` labelled `peat-record`: what this layer kept before Database
      // objects, and the durable copy it keeps of each since) into Database objects, with the control layer's loops stopped from acting on
      // them until each is written (the COPYING mark), and to compare the two (records.ts, copyRecords). `?dry=1` compares and writes
      // nothing; `?held=<name>` (repeated) brings a deleted database back from its held data first. The ConfigMaps stay, marked
      // `peat-superseded`. Loopback only, like the routes below.
      if (req.method === 'POST' && path === '/records/copy') return json(await copyRecords(RECORDS, { dry: url.searchParams.get('dry') === '1', held: url.searchParams.getAll('held').filter((n) => HELD_SELECTOR.test(n)) }))
      if (req.method === 'GET' && path === '/hook-log') return json(hookLog)
      if (req.method === 'GET' && path === '/compute/current') {
        // is this pod still the current compute of its database? (asked by the compute pod's own shell when compute_ctl exits)
        const name = url.searchParams.get('name') ?? '', pod = url.searchParams.get('pod') ?? ''
        if (!DB_NAME.test(name) || !POD_NAME.test(pod)) return json({ current: false })
        const dep = await kube('GET', depPath(name))
        const cur = await livePod(name)
        // newline-terminated: the pod's shell reads the answer with grep, which does not act on an unterminated last line until the connection closes
        return new Response(JSON.stringify({ current: dep.status === 200 && dep.json.spec.replicas === 1 && cur?.metadata?.name === pod }) + '\n', { headers: { 'content-type': 'application/json' } })
      }
      if (req.method === 'POST' && path === '/wal-trim') {
        // one database now: ?db=<name>&dry=1 reads and reports without deleting; without dry it deletes (needs a look before: the first call with dry=0 only records the floor)
        const d = await getDb(url.searchParams.get('db') ?? '')
        if (!d) return json({ error: 'no such database' }, 404)
        return json(await trimTimeline(d.tenant, d.timeline, url.searchParams.get('dry') === '1'))
      }
      if (req.method === 'POST' && path === '/sk-replace/parallel') { if (!skJob) return json({ error: 'no replacement is running' }, 409); skJob.parallel = Math.max(1, Math.min(128, Number(url.searchParams.get('n') ?? '16'))); return json({ parallel: skJob.parallel }) }
      { const m = path.match(/^\/sk\/(\d+)\/may-start-empty$/)
        if (req.method === 'GET' && m) {
          const id = Number(m[1])
          const list = await storcon('GET', '/control/v1/safekeeper')
          if (list.status !== 200 || !Array.isArray(list.json)) return json({ allowed: false, reason: 'the storage controller did not answer' })
          if (!(list.json as any[]).some((x) => x.id === id)) return json({ allowed: true, reason: 'a new id' })
          const made = await kube('POST', `/api/v1/namespaces/${NS}/configmaps`, { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: `sk-lost-${id}`, namespace: NS, labels: { 'peat-sk-lost': 'true', 'peat-sk-open': 'true' } }, data: { id: String(id), at: new Date().toISOString() } })
          if (made.status < 300) console.log(`sk-replace: safekeeper ${id} asked to start on an empty disk: refused, marked lost`)
          return json({ allowed: false, reason: 'a registered member with an empty disk is lost' })
        }
        const up = path.match(/^\/sk-upgrade$/)
        if (req.method === 'POST' && up) {
          // regenerate one safekeeper's StatefulSet from the current template (same machine, same directory): a rolling restart of that one pod
          const id = Number(url.searchParams.get('id'))
          const cur = await kube('GET', `/apis/apps/v1/namespaces/${NS}/statefulsets/safekeeper-w${id}`)
          if (cur.status !== 200) return json({ error: 'no such statefulset' }, 404)
          const node = cur.json.spec.template.spec.nodeSelector['kubernetes.io/hostname'] as string
          const next = skStatefulSet(id, node) as any
          next.metadata.resourceVersion = cur.json.metadata.resourceVersion
          const r = await kube('PUT', `/apis/apps/v1/namespaces/${NS}/statefulsets/safekeeper-w${id}`, next)
          return json({ status: r.status, node })
        }
      }
      if (req.method === 'POST' && path === '/sk-register') {
        // Registers a safekeeper that exists: its pod must answer, and one the controller already lists as anything but Active (decommissioned, paused) is refused unless `force=1` (an Active-listed one that answers is an upsert of the same values)
        const id = Number(url.searchParams.get('id'))
        if (!Number.isInteger(id) || id < 1 || id > SK_MAX_ID) return json({ error: 'bad id' }, 400)
        const up = await fetch(`http://safekeeper-w${id}-0${SK_SUFFIX}:7676/v1/status`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(3000) }).catch(() => null)
        if (!up?.ok) return json({ error: `safekeeper ${id} does not answer on /v1/status: nothing registered` }, 409)
        const list = await storcon('GET', '/control/v1/safekeeper')
        if (list.status !== 200 || !Array.isArray(list.json)) return json({ error: 'the storage controller did not answer: nothing registered' }, 409)
        const listed = (list.json as any[]).find((x) => x.id === id)
        if (listed && listed.scheduling_policy !== 'Active' && url.searchParams.get('force') !== '1') return json({ error: `safekeeper ${id} is listed as ${listed.scheduling_policy}: add force=1 to register it Active again` }, 409)
        await registerSafekeeper(id)
        return json({ registered: id })
      }
      if (req.method === 'GET' && path === '/sk-audit') {
        // the members of every timeline, read from the safekeepers: how many timelines have each set, and how many have no answering member
        const sets = new Map<string, number>(); let unread = 0, joint = 0
        const pairs: [string, string][] = []; const seen = new Set<string>()
        for (const d of await listDbs()) { const k = `${d.tenant}/${d.timeline}`; if (d.mode === 'primary' && !seen.has(k)) { seen.add(k); pairs.push([d.tenant, d.timeline]) } }
        let pi = 0
        await Promise.all(Array.from({ length: 16 }, async () => { for (;;) { const i = pi++; if (i >= pairs.length) return; const [t, tl] = pairs[i]!
          let got = false
          for (let id = 1; id <= SK_MAX_ID + 4 && !got; id++) {
            const r = await fetch(`http://safekeeper-w${id}-0${SK_SUFFIX}:7676/v1/tenant/${t}/timeline/${tl}`, { headers: scoped('safekeeperdata'), signal: AbortSignal.timeout(2500) }).catch(() => null)
            if (!r?.ok) continue
            const m = ((await r.json()) as any).mconf; got = true
            if (m.new_members) joint++
            const k = (m.members as any[]).map((x) => x.id).sort((a, b) => a - b).join(','); sets.set(k, (sets.get(k) ?? 0) + 1)
          }
          if (!got) unread++ } }))
        return json({ timelines: pairs.length, unread, joint, sets: Object.fromEntries(sets) })
      }
      if (req.method === 'GET' && path === '/sk-replace') return json({ current: skJob ? { ...skJob, durationsMs: undefined } : null, history: skJobHistory.map((j) => ({ ...j, durationsMs: undefined, median_s: j.durationsMs.length ? [...j.durationsMs].sort((a, b) => a - b)[Math.floor(j.durationsMs.length / 2)]! / 1000 : null, wall_s: Math.round(((j.endedAt ?? Date.now()) - j.startedAt) / 1000) })) })
      if (req.method === 'GET' && path === '/gateway/peers') return json(await gatewayPeers())
      if (req.method === 'POST' && path === '/gateway/lease') {
        const b = await read(req)
        return json({ leader: await acquireLease(NS, 'peat-gateway-sleep', String(b.holder), 15) })
      }
      // an operator route (no caller class lists it): the process's memory and the JS heap's most numerous object types, to read what grows without a heap snapshot (2026-11-21)
      if (req.method === 'GET' && path === '/memory') {
        const { heapStats } = await import('bun:jsc')
        const h = heapStats() as { objectTypeCounts?: Record<string, number>; heapSize?: number; heapCapacity?: number; extraMemorySize?: number }
        const top = Object.entries(h.objectTypeCounts ?? {}).sort((a, b) => b[1] - a[1]).slice(0, 25)
        return json({ me: ME, leader: isLeader, uptimeS: Math.round(process.uptime()), memory: process.memoryUsage(), heap: { size: h.heapSize, capacity: h.heapCapacity, extra: h.extraMemorySize }, objectTypes: Object.fromEntries(top), maps: { nodeStates: nodeStates.size, podFails: podFails.size, certReloaded: certReloaded.size, missingSince: missingSince.size, recoveredAt: recoveredAt.size, walFloors: walFloors.size, trimmedAt: trimmedAt.size, skSeen: skSeen.size, skJobHistory: skJobHistory.length, hookLog: hookLog.length, gauges: Object.keys(gauges).length, counters: Object.keys(counters).length, firstStarted: firstStarted.size, leaseSeen: leaseSeen.size } })
      }
      if (req.method === 'GET' && path === '/reconcile') return json({ me: ME, leader: isLeader, counters, machines: [...nodeStates.values()] })
      if (req.method === 'GET' && path === '/metrics') {
        const lines = Object.entries(counters).map(([k, v]) => `peat_control_${k} ${v}`)
        lines.push(`peat_control_is_leader ${isLeader ? 1 : 0}`)
        for (const [k, v] of Object.entries(memoryGauges(process.memoryUsage()))) lines.push(`peat_control_${k} ${v}`)
        for (const [k, v] of Object.entries(gauges)) lines.push(`peat_control_${k} ${v}`)
        if (skJob) lines.push(`peat_control_sk_migrations_done ${skJob.done}`, `peat_control_sk_migrations_total ${skJob.total}`, `peat_control_sk_migrations_failed ${skJob.failed}`)
        for (const [id, n] of Object.entries(missingAttached)) lines.push(`peat_control_attached_tenants_missing{node="${id}"} ${n}`)
        for (const [id, sec] of Object.entries(nodeWarming)) lines.push(`peat_control_node_warming_up_seconds{node="${id}"} ${sec}`)
        for (const s of nodeStates.values()) lines.push(`peat_control_machine_dead{machine="${s.name}"} ${s.dead ? 1 : 0}`)
        return new Response(lines.join('\n') + '\n', { headers: { 'content-type': 'text/plain' } })
      }
      if (req.method === 'GET' && path === '/clusters') return json(await clusters())
      if (req.method === 'GET' && path.startsWith('/cluster/')) { if (!DB_NAME.test(path.slice('/cluster/'.length))) return json({ error: 'bad name' }, 400); const row = await clusterOne(path.slice('/cluster/'.length)); return row ? json(row) : json({ error: 'no such database' }, 404) }
      if (req.method === 'GET' && path === '/services') return json(await routeRows())
      if (req.method === 'GET' && path === '/compute-addrs') return json(await computeAddrs())
      if (req.method === 'GET' && path.startsWith('/route/')) { if (!DB_NAME.test(path.slice('/route/'.length))) return json({ error: 'bad name' }, 400); const d = await getDb(path.slice('/route/'.length)); return d && d.mode === 'primary' && !d.closed ? json({ port: d.port }) : d?.closed ? json({ error: `${d.name} is closed` }, 409) : json({ error: 'no such database' }, 404) }
      if (req.method === 'GET' && path.startsWith('/pod/')) { if (!POD_NAME.test(path.slice(5))) return json({ error: 'bad name' }, 400); const r = await kube('GET', `/api/v1/namespaces/${CNS}/pods/${path.slice(5)}`); return json(r.json, r.status) }
      // Making a database through the API writes its Database object (in DB_NS, or `namespace`; a branch, a restore and a replica in their
      // parent's) and waits for the acting layer to make it, as the command does: one record, whoever asks.
      if (req.method === 'POST' && (path === '/db' || path === '/branch' || path === '/restore' || path === '/replica')) {
        const b = await read(req)
        if (reservedName(b.name, req)) return json(RESERVED_MESSAGE, 403)
        if (typeof b.name !== 'string' || !DB_NAME.test(b.name)) return json({ error: `bad name ${JSON.stringify(b.name)}` }, 400)
        if (path === '/restore' && !b.at) return json({ error: 'restore needs "at"' }, 400)
        if (b.namespace !== undefined && (typeof b.namespace !== 'string' || !NAMESPACE_NAME.test(b.namespace))) return json({ error: `bad namespace ${JSON.stringify(b.namespace)}` }, 400)
        const parent = path === '/db' ? null : await getDb(String(b.from ?? ''))
        if (path !== '/db' && !parent) return json({ error: `no database ${b.from}` }, 404)
        const open = await replacementOpen()
        if (open) return json({ error: `new databases wait while a safekeeper replacement is open: ${open}` }, 503)
        const ns = parent?.namespace ?? (b.namespace || DB_NS)
        const spec = path === '/db' ? (b.pg_version !== undefined ? { pgVersion: Number(b.pg_version) } : {}) : path === '/replica' ? { replicaOf: b.from } : { parent: b.from, ...(b.at ? { at: String(b.at) } : {}) }
        const t0 = Date.now()
        const made = await kube('POST', `${DB_API}/namespaces/${ns}/databases`, { apiVersion: `${GROUP}/v1alpha1`, kind: 'Database', metadata: { name: b.name, namespace: ns, ...(req.headers.get('x-peat-drill') === '1' ? { annotations: { [DRILL]: 'true' } } : {}) }, spec })
        if (made.status === 409) return json({ error: `${b.name} exists` }, 409)
        ok(made, `make the Database ${ns}/${b.name}`)
        let d: Db | null = null, last: any = null
        for (let i = 0; i < 240 && !d; i++) {
          await new Promise((res) => setTimeout(res, 500))
          forgetDbs()
          last = ok(await kube('GET', dbObjectPath(ns, b.name)), 'read the Database').json
          d = recordOf(last)
          if (!d && last?.status?.phase === 'Failed') break
        }
        if (!d) {
          // a call that did not get its database takes its object back, so the name is not held by an object nobody waits for
          await kube('DELETE', dbObjectPath(ns, b.name), { preconditions: { uid: made.json.metadata?.uid } })
          forgetDbs()
          if (last?.status?.phase === 'Failed') return json({ error: last.status.message }, 409)
          return json({ error: `${b.name} was not made in 120 s: ${last?.status?.message ?? 'no word from the acting control layer'}` }, 504)
        }
        const madeMs = Date.now() - t0
        if (b.start !== false) await startCompute(d)
        return json({ ...d, ms: { made: madeMs, total: Date.now() - t0 } })
      }
      const m = path.match(/^\/db\/([a-z][a-z0-9-]*)(?:\/(start|stop))?$/)
      if (m) {
        const name = m[1]!
        const d = await getDb(name)
        if (!d) return json({ error: `no database ${name}` }, 404)
        // a delete is the object's: its compute goes at once and its data after the hold (reconcileDatabases)
        if (req.method === 'DELETE') { ok(await kube('DELETE', dbObjectPath(d.namespace, name)), `delete ${name}`, [404]); forgetDbs(); return json({ deleted: name }) }
        if (req.method === 'POST' && m[2] === 'start') {
          if (d.closed) return json({ error: `${name} is closed: it is opened before anything connects` }, 409)
          const r = await startPlaced(d, reqAt).catch((e) => { if (e instanceof ClosedDb) return { closed: e.message }; throw e })
          if ('closed' in r) return json({ error: r.closed }, 409)
          if ('refused' in r) return json({ error: `cannot start ${name}: the machines are full: ${r.refused}`, refused: r.refused }, 503)
          return json(r)
        }
        if (req.method === 'POST' && m[2] === 'stop') { await stopCompute(name); return json({ stopped: name }) }
        if (req.method === 'GET') return json({ ...d, pod: (await livePod(name))?.metadata?.name ?? null })
      }
      if (req.method === 'GET' && path === '/dbs') return json(await listDbs())
      return json({ error: 'not found' }, 404)
    } catch (e) {
      console.log(`error ${req.method} ${path}: ${e}`)
      return json({ error: String(e) }, 500)
    }
  },
})
console.log('peat control layer listening on 8080')
void loop('leader election', 2000, leaderTick)
void loop('safekeepers', 5000, skWatch)
void loop('seed the safekeepers', 5000, seedSafekeepers)
if (WAL_TRIM_EVERY_S > 0) void loop('wal trim', WAL_TRIM_EVERY_S * 1000, walTrimCycle)
void loop('storage nodes', 5000, pollStorageNodes)
void loop('probe machines', PROBE_EVERY_MS, probeNodes)
void loop('reconcile computes', PROBE_EVERY_MS + 1000, reconcileComputes)
void loop('reload renewed certificates', 300_000, reloadRenewedCerts)
void loop('renew the certificates this layer signs', 3_600_000, renewOwnCerts)
void loop('databases', 2000, reconcileDatabases)
void loop('remove held data whose hold is over', 3_600_000, async () => { if (isLeader) await purgeHeld(RECORDS, await notDeleting()) })
// JavaScriptCore collects lazily and does not know the container's memory limit: run in the pod, the garbage of the loops and of every call piled up until the kernel killed the process at 256 MiB (read 2026-11-21: with the
// program's own loops and calls, a local run of the same Bun held 1.6 MiB of heap with a full collection every 30 s and grew its live promises and closures without one). A full collection of a heap this small takes
// milliseconds. `GC_EVERY_S` 0 turns it off.
const GC_EVERY_S = Number(process.env['GC_EVERY_S'] ?? '30')
if (GC_EVERY_S > 0) setInterval(() => { Bun.gc(true); bump('gc_runs_total') }, GC_EVERY_S * 1000)
