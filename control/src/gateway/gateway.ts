// The Postgres gateway: the one program in front of every database that sleeps. It was written for databases that are CloudNativePG Clusters
// (`kubeApi`, below, is that seam), and Peat's gateway pods run it unchanged over the platform's control layer (neon-api.ts, main.ts), whose
// rows are written in the same shape. Tested by test/gateway.test.ts.
//
// This file imports nothing but Node's and Bun's own modules: the names it shares with whoever writes its routes and Clusters (`Names`) and
// its limits (`Options`) come from its caller.
//
// What it does. Every few seconds it lists the Services of its own namespace; each one that carries the route annotation leads to one database ("<namespace>/<cluster>"), and the
// gateway listens on the port that Service maps 5432 to, a port that is that database's alone. A connection on that port is the database named by the Service, so the gateway reads
// no byte of what the client sends: it dials the database's read-write Service and passes the bytes both ways, untouched, encryption included. The first dial of a caller is short
// (300 ms; a live Service answers in single-digit milliseconds). A Service with no ready server does not refuse on the cluster it was measured on, the dial hangs (measured 2026-11-15; why was not
// established), so a first dial that does not answer in that time counts as refused, and it reads the Cluster:
//   - asleep (the hibernation mark is "on", or the hibernation condition is true; never the phase, which still reads healthy while a Cluster sleeps) and declared to sleep: it sets the
//     mark to "off" (a merge patch of that one annotation) and holds the caller until the server answers or the wake limit (120 s) passes: passed to the server's own address once it takes
//     logins (the direct dial, below), or through the Service, dialed every quarter second (each dial abandoned after 300 ms, since it hangs until the server is ready), whichever answers first;
//   - asleep and NOT declared to sleep (someone stopped it by hand): it closes the caller, counts it refused, and wakes nothing;
//   - not asleep: it holds the caller for a short limit of its own (30 s), dialing every half second with a dial limit of 3 s, then closes it. That is also where a database that is awake
//     but slow to accept lands, so a caller the 300 ms dial missed is served as it was before. It does not try to tell a server that is starting from one that is crashing:
//     CloudNativePG 1.30.1 lists a Pending pod (the first seconds of every start) as failed and a pod in CrashLoopBackOff as replicating, and it reads a pod only for a wake it began and for a caller held for an awake database whose last wake passed callers to a pod that is not yet Ready (below);
//   - the Cluster cannot be read: nothing is woken, and the caller is held as for a database that is not asleep, so an outage of the API does not close a caller that the 3 s first dial used to serve.
// One wake per database at a time: every caller waiting on a database shares the one wake. The short hold is the caller's own and is not a wake.
//
// The direct dial. A wake does not wait for Kubernetes to route to the woken server: the program reads the Cluster's current primary and that pod,
// and as soon as the pod has an address it asks the server at that address whether it takes logins (below), then passes each waiting caller to that address. Postgres takes logins about 3 s
// after the first byte of a wake, the pod and the read-write Service's endpoint are Ready 1.4 s later and the Service leads to it about 1 s after that (measured 2026-11-15), so a caller is
// through at about 3 s instead of 5.4 s. The route through the Service stays: it is the way in for an awake database, and the fallback of a wake whose pod cannot be read (the API is down, the
// permission is missing, the Cluster names no primary, the pod is not this wake's) or whose address does not answer. The address is used only for the callers of the wake that read it and
// for callers that arrive before that same pod (by its UID) is Ready; it is read again for every wake and never kept between them (a deleted pod's address can be given to another database's pod).
//
// Asking whether a server takes logins (`ask`). Postgres listens a few tens of ms before it takes logins, and then answers a startup message with the error "the database system is
// starting up" (SQLSTATE 57P03; also "shutting down"). Through the Service that is hidden, because a pod is not an endpoint until it is Ready; at the pod's own address it is not, and with
// encryption (psql's default, sslmode=prefer) the answer would be inside a handshake this program cannot read. So before passing a caller it sends one plain startup message for a role that
// can never be a login (`PROBE_ROLE`: a name that starts with pg_ cannot be created) and reads only the first answer, as libpq's PQping and pg_isready do: an authentication request, or any
// error but 57P03, means it takes logins; 57P03 or no answer means ask again. It then closes the socket without authenticating. **It opens no session and is no session:** the backend is
// in authentication and never gets past it (it is not in pg_stat_activity as active, idle or idle in transaction, which is what a count of open sessions reads), and nothing here counts a
// session but `splice`, which an ask never reaches. Postgres 17's source (auth.c `recv_password_packet`, auth-sasl.c, `ClientAuthentication`) makes no log entry for a client that disconnects
// where it was asked for a password: STATUS_EOF ends the backend with exit 0. That a ping is a legal use of the protocol is the comment in backend_startup.c ("This also allows a pg_ping
// utility to be written").
//
// What it never does: open a session on a database by itself. Every dial and every ask it makes is on behalf of a caller that is waiting; when the last caller leaves, the dialing and the asking stop.
//
// It also keeps the backup schedules of a sleeping permanent database: a second step in `tick`, beside `refresh`. A permanent database is one whose route port is in the
// permanent range. While its Cluster is asleep (the mark or the condition) it suspends each of its ScheduledBackups that is not suspended, and marks each one it suspended; once the
// Cluster is awake and has a ready server it resumes only the ones it marked, so a schedule somebody suspended by hand stays suspended. A resumed schedule whose slot passed takes that
// backup at once (the operator works the next run out from the schedule's last check, which a suspended schedule does not move). A branch has no schedules.
//
// Three things for real data. A sleep the API server refuses because the Cluster changed since it was read is counted
// (`pg_gateway_sleeps_refused_total`) beside its log line and decided again at the next look. An awake database is put to sleep only at a look at which its Cluster reports a ready instance.
// And when a wake begins, the caller that made it is named: its first message is read for the login and the database if it is a plain startup message (`readStartup`; a caller that asks for
// encryption first reads as `encrypted`, since what follows is inside the handshake), and the login is the label of `pg_gateway_wake_callers_total` and a word of the wake's log line. Only
// a well-formed role name is a label, at most `MAX_LOGINS` per database, then `other`; nothing else of the message is logged or kept.
import { readFileSync } from 'node:fs'
import { type Server, type Socket, connect, createServer } from 'node:net'

/** "<namespace>/<cluster>": the database a route leads to, and the `database` label of every metric. */
export type Database = string

/** What the gateway reads of a Service. */
export interface ServiceRow {
  metadata: { name: string; annotations?: Record<string, string> }
  spec?: { ports?: { port?: number; targetPort?: number | string }[] }
}

/** What the gateway reads of a CloudNativePG Cluster. */
export interface ClusterRow {
  metadata?: { name?: string; resourceVersion?: string; annotations?: Record<string, string> }
  /** `currentPrimary` is the name of the pod that is the Cluster's primary (the one pod of a database here). */
  status?: { readyInstances?: number; currentPrimary?: string; conditions?: { type?: string; status?: string }[] }
}

/** What the gateway reads of a pod of a database: whose it is, which one it is, where it is and whether it is on its way out or Ready. */
export interface PodRow {
  metadata?: { name?: string; uid?: string; creationTimestamp?: string; deletionTimestamp?: string; labels?: Record<string, string> }
  status?: { phase?: string; podIP?: string; conditions?: { type?: string; status?: string }[] }
}

/** What the gateway reads of a CloudNativePG ScheduledBackup. */
export interface ScheduleRow {
  metadata: { name: string; annotations?: Record<string, string> }
  spec?: { suspend?: boolean; cluster?: { name?: string } }
}

/** What the gateway reads of a CloudNativePG Backup: whose it is and whether it has finished (`completed` or `failed`). */
export interface BackupRow {
  metadata?: { name?: string }
  spec?: { cluster?: { name?: string } }
  status?: { phase?: string }
}

/** The two stages of sleep: light is CloudNativePG's fence (Postgres stopped, the pod kept), full is its hibernation (the pod deleted, the volume kept). */
export type Stage = 'light' | 'full'
const STAGES: readonly Stage[] = ['light', 'full']

/** The few calls it makes of the cluster's API, so a test can stand in for the cluster. */
export interface Api {
  /** The Services of the gateway's own namespace. */
  listServices(): Promise<ServiceRow[]>
  /** The Cluster, or null when there is none. */
  getCluster(namespace: string, name: string): Promise<ClusterRow | null>
  /** One pod by name, or null when there is none (the server's own address is read from it). */
  getPod(namespace: string, name: string): Promise<PodRow | null>
  /** The Clusters of one namespace, in one call (the gateway's look at every database it serves). */
  listClusters(namespace: string): Promise<ClusterRow[]>
  /** The Backups of one namespace, in one call (a sleep is not made while one of a Cluster is unfinished). */
  listBackups(namespace: string): Promise<BackupRow[]>
  /**
   * Wakes a database in one merge patch. Light: removes the fence and the gateway's mark. Full: turns the hibernation mark off and removes any fence and mark left. Nothing else of the Cluster is
   * touched.
   */
  wake(namespace: string, name: string, stage: Stage): Promise<void>
  /**
   * Light sleep: one merge patch that sets the fence on every instance and the gateway's mark (the moment the last session ended, RFC 3339). Like `hibernate` and `unfence` it carries the
   * `resourceVersion` of the Cluster row the decision was made on, so the API server refuses it (409) when the Cluster has changed since: a decision is never applied to a Cluster that is no
   * longer the one it was made about.
   */
  fence(namespace: string, name: string, idleSince: string, resourceVersion: string): Promise<void>
  /** Full sleep: sets the hibernation mark on (a fence and mark already there are kept, and removed by `unfence` once the pod is gone). */
  hibernate(namespace: string, name: string, resourceVersion: string): Promise<void>
  /** Removes the fence and the gateway's mark: one merge patch. */
  unfence(namespace: string, name: string, resourceVersion: string): Promise<void>
  /** The ScheduledBackups of one Cluster. */
  listSchedules(namespace: string, cluster: string): Promise<ScheduleRow[]>
  /** Suspends a ScheduledBackup and marks it as suspended here: a merge patch of `spec.suspend` and the one annotation. */
  suspendSchedule(namespace: string, name: string): Promise<void>
  /** Resumes a ScheduledBackup this program suspended and takes its mark off: the same two fields back. */
  resumeSchedule(namespace: string, name: string): Promise<void>
}

/** The names it shares with whoever writes its routes and Clusters: the annotations it reads and writes, its namespace and its ports. */
export interface Names {
  /** The gateway's own namespace: where its routes are. */
  namespace: string
  routeAnnotation: string
  /** The first of the two sleep annotations (under the prefix of the plugin that first defined them): "declared to sleep" means it is "true". */
  sleepEnabledAnnotation: string
  /** The light time, in whole minutes, of a database declared to sleep (the idle minutes annotation, kept as it was). */
  sleepIdleMinutesAnnotation: string
  /** The full time in whole minutes or `sleepNever`; absent means the light time (full sleep only, as before the two stages). */
  sleepFullMinutesAnnotation: string
  sleepNever: string
  hibernationAnnotation: string
  /** CloudNativePG's fence, its value for every instance, and the gateway's own mark on a Cluster it fenced. */
  fencedInstancesAnnotation: string
  fenceAll: string
  idleSinceAnnotation: string
  /** On a ScheduledBackup it suspended, so that it resumes only its own. */
  suspendedByAnnotation: string
  /** The ports it may listen on: a Service that names any other port is not a route. */
  portRanges: { from: number; to: number }[]
  /** The range of the permanent databases' ports (inside `portRanges`): a route in it is a permanent database, whose schedules it keeps. */
  permanentPorts: { from: number; to: number }
}

export interface Options {
  api: Api
  names: Names
  /** Where a database is dialed. Default: the Cluster's read-write Service, `<cluster>-rw.<namespace>.svc:5432`. */
  target?: (database: Database) => { host: string; port: number }
  /** Where a woken server is dialed by its own address (default: that address, port 5432). For a test, whose stand-in database listens on a port of its own. */
  podTarget?: (ip: string) => { host: string; port: number }
  /** How often a wake asks the server whether it takes logins, once it has an address (default 25 ms: the server took logins 60 to 100 ms after the wake patch in the hand trial of 2026-11-15, so three or four asks). */
  askRetryMs?: number
  /** How often a wake reads the Cluster's primary and its pod again (default a quarter second, the wake's interval): until the pod has an address, and to know the pod is still the one that was read. */
  locateRetryMs?: number
  /** How soon after the gateway's own fence a light wake counts as "just fenced" (default 5 s), and how long such a wake waits to see the server stopped before it takes a yes anyway (default 3 s). */
  fenceRecentMs?: number
  stoppedWaitMs?: number
  /** How long a wake waits in the background for the first message of the caller that began it to be whole before it counts the caller as `other` (default 2 s). The wake itself never waits. */
  firstMessageMs?: number
  /** How long a light wake waits for the server's own address before its callers may dial the Service (default 2 s; see `hold`). */
  lightDirectGraceMs?: number
  /** How long a caller is held for a database it woke to answer (default 120 s, ten times the 11.1 s measured by hand). */
  holdLimitMs?: number
  /** How long a caller is held when the database is not asleep and its dial is refused (default 30 s). */
  awakeHoldMs?: number
  /** How often a caller held for a database that is not asleep dials again (default half a second). */
  retryMs?: number
  /** How long one dial of a caller held for a database that is not asleep may take before it counts as refused (default 3 s). */
  dialTimeoutMs?: number
  /**
   * How long a caller's first dial may take before it counts as refused and the Cluster is read (default 0.3 s). A dial to a Service with no ready server hangs on the cluster it was measured on instead of
   * being refused, so the limit is what the first look at the Cluster waits for. A database that is awake and slow to accept falls to the hold above, with `dialTimeoutMs`.
   */
  firstDialMs?: number
  /** How long one dial of a caller waiting on a wake may take (default 0.3 s): a dial to a Service whose server is not ready yet hangs, and the next one is what finds it ready. */
  wakeDialMs?: number
  /** How often a caller waiting on a wake dials again (default a quarter second), so it is through within about a third of a second of the server being ready. */
  wakeRetryMs?: number
  /** How often the Services are read (default 5 s). */
  refreshMs?: number
  /** How long the second side of a pair is given to finish once the first has gone, before it is cut (default 5 s). */
  closeGraceMs?: number
  /**
   * How long bytes waiting to be written **to the client** may go with none accepted, before the pair is ended (default 300 s). A client that drops off the network in the middle of a large
   * read is not found by keepalive (Linux sends no probe while data is queued) but only by TCP's retransmission limit, about 15 minutes, and the database is held awake that long. A
   * session with nothing waiting is never cut, and a reader that is slow but moving is not: each accepted byte starts the time again. **Bytes waiting toward the database are never
   * limited**: a `COPY FROM STDIN` or a large pipelined batch whose statement waits on a lock waits as it would on a direct connection.
   */
  stallMs?: number
  /** How a dial is made (default `net.connect`): for a test that needs a dial that takes a while. */
  connect?: (target: { host: string; port: number }) => Socket
  /** Idle time before a connection's peer is probed (default 30 s; Bun sets the probe interval to 1 s and the count to 10, read from the socket on macOS, so a peer that is gone is found in about 40 s). */
  keepAliveMs?: number
  listenHost?: string
  now?: () => number
  log?: (line: string) => void
}

/** The condition CloudNativePG sets on a Cluster it has hibernated (read on 2026-11-14: reason `Hibernated`, while the phase stayed "Cluster in healthy state"). */
const HIBERNATION_CONDITION = 'cnpg.io/hibernation'
const RESULTS = ['woken', 'timed_out', 'refused'] as const
type Result = (typeof RESULTS)[number]
const HOLD_RESULTS = ['reached', 'gave_up'] as const
type HoldResult = (typeof HOLD_RESULTS)[number]
/** Seconds: a light wake is aimed at about a third of a second, a full one at about 3 s (5.4 s before the direct dial, 11.1 by hand); the limit is 120. */
const BUCKETS = [0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 10, 12, 15, 20, 30, 45, 60, 90, 120]
const NAME = /^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/
/** What it keeps of a client's bytes while no database has the connection: past this the client is paused, not closed (a Postgres client sends one small packet and waits; a fast one is held back). */
const MAX_PENDING_BYTES = 1024 * 1024
/** How long a side that is closing is given to finish what it was sent. */
const CLOSE_GRACE_MS = 5_000
/**
 * How long a connection is idle before the kernel starts asking its peer whether it is there (TCP keepalive). A peer that vanishes without a word (a deleted pod sends no FIN and no
 * reset, and a connection the node forgot is answered by nothing) leaves a socket nobody reads or writes, which never learns of it; the probes do, and Bun then reports an error and
 * the pair is ended. A peer that answers is untouched, so this is no idle timeout: a Postgres session may sit idle for as long as it likes.
 */
const KEEPALIVE_MS = 30_000
/** How long bytes waiting to be written to the client may go with none accepted, before the pair is ended: a lazily reading client is far likelier than a vanished one, and 5 minutes is a third of TCP's own limit. */
const STALL_MS = 300_000

/**
 * What a Cluster is, read from its annotations and condition and never its phase:
 * - `awake`: neither mark;
 * - `light-sleep`: the fence on every instance **and** the gateway's mark (a time), and declared to sleep (Postgres stopped, the pod kept);
 * - `full-sleep`: the hibernation mark on or its condition true, and declared to sleep (the pod gone), whatever fence and mark of the gateway's are left from the way there;
 * - `by-hand`: someone stopped it: any fence without the gateway's mark (or one that is not the whole-Cluster fence), or either mark on a database not declared to sleep. Refused, never woken.
 * An empty fence (`[]`, or none) is awake.
 */
export type State = 'awake' | 'light-sleep' | 'full-sleep' | 'by-hand'

/** The fence a Cluster carries: none, the gateway's (the whole-Cluster fence with its mark, a time), or someone else's. */
export function fenceOf(cluster: ClusterRow, names: Pick<Names, 'fencedInstancesAnnotation' | 'fenceAll' | 'idleSinceAnnotation'>): 'none' | 'ours' | 'foreign' {
  const annotations = cluster.metadata?.annotations ?? {}
  const raw = annotations[names.fencedInstancesAnnotation]
  if (raw === undefined || raw === '') return 'none'
  let list: unknown
  try {
    list = JSON.parse(raw)
  } catch {
    return 'foreign'
  }
  if (Array.isArray(list) && list.length === 0) return 'none'
  let whole: unknown
  try {
    whole = JSON.parse(names.fenceAll)
  } catch {
    whole = undefined
  }
  const mark = annotations[names.idleSinceAnnotation]
  const ours = JSON.stringify(list) === JSON.stringify(whole) && mark !== undefined && !Number.isNaN(Date.parse(mark))
  return ours ? 'ours' : 'foreign'
}

/**
 * Reads a Cluster. Asleep is a mark or a condition, never the phase (which still reads healthy in both stages). Anything else is awake: **nothing about the servers is read to tell a start from
 * a crash** (not the phase, not `instancesStatus`; the pods are read only for a wake's own address, never to judge a server), because what CloudNativePG 1.30.1 puts in those lists does not tell
 * a server that is starting from one that is crashing (read at the tag, `api/v1/base_funcs.go` `ListStatusPods` and `pkg/utils/pod_conditions.go` `IsPodActive`: a Pending pod is listed
 * under `failed`, a pod in CrashLoopBackOff, whose phase is Running, as replicating).
 */
export function classify(cluster: ClusterRow, names: Pick<Names, 'sleepEnabledAnnotation' | 'hibernationAnnotation' | 'fencedInstancesAnnotation' | 'fenceAll' | 'idleSinceAnnotation'>): State {
  const annotations = cluster.metadata?.annotations ?? {}
  const declared = annotations[names.sleepEnabledAnnotation] === 'true'
  const fence = fenceOf(cluster, names)
  if (fence === 'foreign') return 'by-hand'
  const condition = (cluster.status?.conditions ?? []).find((c) => c.type === HIBERNATION_CONDITION)
  if (annotations[names.hibernationAnnotation] === 'on' || condition?.status === 'True') return declared ? 'full-sleep' : 'by-hand'
  if (fence === 'ours') return declared ? 'light-sleep' : 'by-hand'
  return 'awake'
}

/** The stage a stopped Cluster is in, for the count of a refusal: a fence is light sleep's, a hibernation full sleep's. */
export const stageOf = (state: State, cluster: ClusterRow, names: Pick<Names, 'fencedInstancesAnnotation'>): Stage =>
  state === 'light-sleep' || (state === 'by-hand' && (cluster.metadata?.annotations?.[names.fencedInstancesAnnotation] ?? '') !== '') ? 'light' : 'full'

interface Series {
  connections: number
  sessions: number
  wakes: Record<Stage, Record<Result, number>>
  sleeps: Record<Stage, number>
  /** Sleeps the API server refused because the Cluster had changed since it was read, by the stage the sleep was for (decided again at the next look). */
  refused: Record<Stage, number>
  /** Who began each wake, by stage and login label (`WHO_ENCRYPTED`, `WHO_OTHER` or a role name); see `readStartup`. */
  callers: Record<Stage, Map<string, number>>
  /** The role names given a label of their own so far: at most `MAX_LOGINS`, so no caller can grow the series without bound. */
  logins: Set<string>
  holds: Record<HoldResult, number>
  buckets: Record<Stage, number[]>
  sum: Record<Stage, number>
  count: Record<Stage, number>
}

/**
 * The role the question to a server is asked as. A name that starts with `pg_` cannot be made as a role (CREATE ROLE refuses it), so it is never a login on any database here and the
 * server cannot take the question for a login attempt that could succeed.
 */
const PROBE_ROLE = 'pg_gateway_probe'
/** The startup message of the question: length, protocol 3.0, `user` and the role, the closing zero. No database, no password, no SSLRequest: it is read as a plain connection. */
const PROBE_PACKET = (() => {
  const body = Buffer.concat([Buffer.from([0, 3, 0, 0]), Buffer.from(`user\0${PROBE_ROLE}\0\0`)])
  const length = Buffer.alloc(4)
  length.writeInt32BE(body.length + 4)
  return Buffer.concat([length, body])
})()
/** SQLSTATE 57P03, "cannot connect now": what Postgres answers a startup message with while it starts or stops (and a standby that is not yet consistent). */
const CANNOT_CONNECT_NOW = '57P03'

/** What a server says to the question: it takes logins, it is up but cannot take one now, or it did not answer (refused, closed, silent, or not a Postgres). */
export type Answer = 'open' | 'starting' | 'unreachable'

/**
 * The first answer to the question, as far as the bytes read tell it, or undefined while more are needed. `R` (an authentication request of any kind, "ok" included) means it takes logins; `E`
 * (an error) means it does unless its SQLSTATE is 57P03; anything else is not a Postgres that is answering a startup message.
 */
export function readAnswer(bytes: Buffer): Answer | undefined {
  if (bytes.length < 5) return undefined
  const type = String.fromCharCode(bytes[0]!)
  if (type === 'R') return 'open'
  if (type !== 'E') return 'unreachable'
  const length = bytes.readInt32BE(1)
  if (length < 5 || length > 8192) return 'unreachable'
  if (bytes.length < 1 + length) return undefined
  // the error's fields: a type byte and a zero-ended string each, ended by a zero byte; `C` is the SQLSTATE
  let at = 5
  while (at < 1 + length && bytes[at] !== 0) {
    const field = String.fromCharCode(bytes[at]!)
    const end = bytes.indexOf(0, at + 1)
    if (end < 0) break
    if (field === 'C') return bytes.toString('utf8', at + 1, end) === CANNOT_CONNECT_NOW ? 'starting' : 'open'
    at = end + 1
  }
  // an error with no SQLSTATE: not one Postgres sends, and not an answer that says it takes logins
  return 'unreachable'
}

// ── who woke it ──────────────────────────────────────────────────────────────

/** The label of a wake begun by a caller that asked for encryption first (`psql`'s default): its startup message is inside the handshake, and the gateway reads no byte of that. */
export const WHO_ENCRYPTED = 'encrypted'
/** The label of every caller whose login is not one: a name that is not a well-formed role name, a first message that is not a startup message, none sent in time, and the logins past `MAX_LOGINS` of one database. */
export const WHO_OTHER = 'other'
/** At most this many role names are a label of their own per database; the next is `other`. */
export const MAX_LOGINS = 16
/** A well-formed role name, as a label and as a word in a log line: no quote, backslash, space, newline or control character can be in it, so a client cannot write into the log or the exposition. */
const ROLE_NAME = /^[A-Za-z_][A-Za-z0-9_$-]{0,62}$/
/** How long a wake waits for a caller's first message to be whole before it counts the caller as `other` (`Options.firstMessageMs`): a Postgres client sends it the moment it connects. */
const FIRST_MESSAGE_MS = 2_000
/** Postgres refuses a startup message longer than 10000 bytes; the gateway reads no more than that of what a caller sent. */
const MAX_STARTUP_BYTES = 10_000
/** The request codes of the protocol: SSLRequest and GSSENCRequest (an encrypted connection is asked for) and the version of a plain startup message, 3.0. */
const SSL_REQUEST = 80877103
const GSS_ENC_REQUEST = 80877104
const PROTOCOL_3 = 196608

/** What the first bytes of a connection are, as far as they tell it: not enough yet, a request for encryption, a plain startup message (its login and database, either of which may be missing) or anything else. */
export type Startup = { kind: 'more' } | { kind: 'encrypted' } | { kind: 'startup'; user?: string; database?: string } | { kind: 'other' }

/**
 * Reads a caller's first message (`bytes` is what it has sent so far) for the login and the database. Only the two names of a plain startup message are read, and nothing else of the message is
 * kept: the other parameters (`application_name`, `options`, `client_encoding`, whatever a client adds) are skipped. There is no secret in it (the password comes after the server asks, and a caller
 * that asks for encryption first is not read at all).
 */
export function readStartup(bytes: Buffer): Startup {
  if (bytes.length < 8) return { kind: 'more' }
  const code = bytes.readInt32BE(4)
  if (code === SSL_REQUEST || code === GSS_ENC_REQUEST) return { kind: 'encrypted' }
  if (code !== PROTOCOL_3) return { kind: 'other' }
  const length = bytes.readInt32BE(0)
  if (length < 9 || length > MAX_STARTUP_BYTES) return { kind: 'other' }
  if (bytes.length < length) return { kind: 'more' }
  // the parameters: a name and a value, each ended by a zero, and a zero after the last
  const body = bytes.subarray(8, length)
  if (body[body.length - 1] !== 0) return { kind: 'other' }
  const words = body.toString('utf8', 0, body.length - 1).split('\0')
  const found: { user?: string; database?: string } = {}
  for (let i = 0; i + 1 < words.length; i += 2) {
    if (words[i] === 'user') found.user = words[i + 1]
    else if (words[i] === 'database') found.database = words[i + 1]
  }
  return { kind: 'startup', ...found }
}

/** The first `limit` bytes of what a caller has sent so far, as one buffer. */
function headOf(pending: Buffer[], limit: number): Buffer {
  const parts: Buffer[] = []
  let have = 0
  for (const chunk of pending) {
    parts.push(chunk)
    have += chunk.length
    if (have >= limit) break
  }
  return Buffer.concat(parts).subarray(0, limit)
}

/** A pod of a database as the gateway uses it: which one, where, and whether it is on its way out or Ready. */
interface Located {
  name: string
  uid: string
  ip: string
  ready: boolean
  /** Milliseconds since the epoch, or NaN when the pod says none. */
  created: number
}

/** The pod `row` as a server of the Cluster `cluster`, or null when it is not one that can be dialed: not found, not Running, no address, on its way out, or labelled as another Cluster's. */
function locatedOf(row: PodRow | null, cluster: string): Located | null {
  const name = row?.metadata?.name
  const uid = row?.metadata?.uid
  const ip = row?.status?.podIP
  if (!row || !name || !uid || !ip) return null
  if (row.metadata?.deletionTimestamp) return null
  // a pod that is Failed (evicted, a node shut down) or Succeeded can keep its address in its status after the network has freed it
  if (row.status?.phase !== 'Running') return null
  // a pod of this database and no other: branches of one parent share its login, so an address of another database's pod would be served as this one
  if (row.metadata?.labels?.['cnpg.io/cluster'] !== cluster) return null
  const ready = (row.status?.conditions ?? []).some((c) => c.type === 'Ready' && c.status === 'True')
  return { name, uid, ip, ready, created: Date.parse(row.metadata?.creationTimestamp ?? '') }
}

/** One look at a database whose dial was refused, and the wake it may lead to: shared by every caller that arrives while it is made. */
interface Wake {
  startedAt: number
  deadline: number
  /** Light (the fence lifted: the server is the one that was there) or full (the pod is made again). Known once the Cluster is read. */
  stage: Stage
  /**
   * A light wake begun within `fenceRecentMs` of the gateway's own fence: a yes to a question is taken only once an ask has seen the server stopped (refused, or "cannot connect now"), or
   * at this time (a fence lifted before the instance manager acted never stops Postgres): the Postgres that has not begun to stop yet would answer yes, and be stopped under the caller.
   */
  mustSeeStoppedUntil?: number
  sawStopped: boolean
  /** Some ask of this wake was answered "takes logins": the server came, so a wake that reaches its deadline with late joiners waiting is not a failed one. */
  answered: boolean
  /** The wake was counted `timed_out` at its deadline while late joiners waited (once); a caller served after that is not counted `woken`. */
  counted: boolean
  /**
   * How many times the wake has been left with no caller waiting while it was unsettled (it is kept until its deadline and until its last late joiner leaves, so a later caller joins it). An address is found for the callers
   * that wait, and by a reading made while one did: when the last caller leaves, the address is dropped and a finding in progress ends, so a caller that joins after the gap reads the pod and
   * asks for itself. A wake that has had a gap is also no measure of a wake time (its callers arrived late) and gives its late callers the awake hold's limit.
   */
  gap: number
  /**
   * The server's own address, once it has been asked and takes logins there: the callers of this wake dial it. Read again for every wake, and dropped with the last waiting caller, so a pod
   * that has gone and whose address another pod has taken is never dialed from an earlier reading.
   */
  direct?: Located
  /** The program that reads the pod and asks is running (one per wake, started by a caller that waits). */
  finding: boolean
  /** Aborted, and replaced, whenever `direct` is found: a caller whose dial of the Service hangs, or who is between two, goes on at once. */
  changed: AbortController
  /** The name of the pod the Cluster gave as its current primary when the wake read it: the one pod the server's own address is read from (none: the wake goes through the Service). */
  primary?: string
  /** The asks of this wake that were not yet answered "takes logins", for the log line of its end. */
  asked: { starting: number; unreachable: number }
  /** `wake`: the mark was turned off here, wait for the server; `wait`: not asleep, or the Cluster cannot be read, each caller holds for itself; `refused`: asleep by hand; `close`: not waited for. */
  verdict: Promise<'wake' | 'wait' | 'refused' | 'close'>
  settled: boolean
  /** How it ended, once it has: a server that answered, or a deadline that passed with none. A caller is never served after `timed_out`, so the count and what callers saw agree. */
  outcome?: 'woken' | 'timed_out'
  waiters: number
  /** Who began the wake, for the line that says how it ended (`login app_reader on database app`): set when the first message of the caller that made it has been read. */
  who?: string
  /** Dials in flight for this wake. A deadline that comes while one is in flight waits for it: the answer to that dial decides the count. */
  dialing: number
  timer?: ReturnType<typeof setTimeout>
}

/** The caller that makes a wake: its socket, and what it has sent so far (kept for the database, and read here only for its login and database names). */
interface Caller {
  client: Socket
  pending: Buffer[]
}

/** What the gateway keeps about one database's use and state (see `watches`). */
interface Watch {
  /** The state its Cluster read at the last look, or the gateway's own change since; undefined until the first look. */
  state?: State
  cluster?: ClusterRow
  /** Milliseconds since the epoch, on the gateway's clock. */
  lastActive: number
  waiting: number
  /**
   * The gateway's own change (a fence, a hibernation, an unfence) that is in flight: the database is going to sleep, so a caller is held for it and a wake reads the Cluster only after it has
   * settled, and no row is taken from a look while it is. One change at a time per database.
   */
  changing?: Promise<void>
  /** When the gateway last changed (or began to change) this database's Cluster: a row listed before it is older than what the gateway has done, and is not taken in. */
  changedAt: number
  /** When the gateway's own fence landed: a light wake begun soon after it waits to see the server stopped before it takes a yes. */
  fencedAt?: number
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))
const noop = () => {}
/** A whole number of minutes above zero, or undefined. */
const minutesOf = (value: string | undefined): number | undefined => (value !== undefined && /^[0-9]+$/.test(value) && Number(value) > 0 ? Number(value) : undefined)

export function createGateway(opts: Options) {
  const { api, names } = opts
  const holdLimitMs = opts.holdLimitMs ?? 120_000
  const awakeHoldMs = opts.awakeHoldMs ?? 30_000
  const retryMs = opts.retryMs ?? 500
  const dialTimeoutMs = opts.dialTimeoutMs ?? 3_000
  const firstDialMs = opts.firstDialMs ?? 300
  const wakeDialMs = opts.wakeDialMs ?? 300
  const wakeRetryMs = opts.wakeRetryMs ?? 250
  const askRetryMs = opts.askRetryMs ?? 25
  const locateRetryMs = opts.locateRetryMs ?? 250
  const firstMessageMs = opts.firstMessageMs ?? FIRST_MESSAGE_MS
  const lightDirectGraceMs = opts.lightDirectGraceMs ?? 2_000
  const fenceRecentMs = opts.fenceRecentMs ?? 5_000
  const stoppedWaitMs = opts.stoppedWaitMs ?? 3_000
  const refreshMs = opts.refreshMs ?? 5_000
  const closeGraceMs = opts.closeGraceMs ?? CLOSE_GRACE_MS
  const keepAliveMs = opts.keepAliveMs ?? KEEPALIVE_MS
  const stallMs = opts.stallMs ?? STALL_MS
  const dial = opts.connect ?? ((t: { host: string; port: number }) => connect({ host: t.host, port: t.port }))
  const keepAlive = (socket: Socket) => {
    // A socket that is already gone is of no use to probe; whatever it says is said by its own error.
    if (!socket.destroyed) socket.setKeepAlive(true, keepAliveMs)
  }
  const podTarget = opts.podTarget ?? ((ip: string) => ({ host: ip, port: 5432 }))
  const listenHost = opts.listenHost ?? '0.0.0.0'
  const now = opts.now ?? Date.now
  const log = opts.log ?? ((line: string) => console.log(line))
  const target =
    opts.target ??
    ((database: Database) => {
      const [namespace, cluster] = database.split('/')
      return { host: `${cluster}-rw.${namespace}.svc`, port: 5432 }
    })

  /** `held`: the callers of the route that no database has the connection of yet, closed when the route goes. */
  const routes = new Map<number, { database: Database; server: Server; held: Set<Socket> }>()
  const series = new Map<Database, Series>()
  const wakes = new Map<Database, Wake>()
  /** The pod the last wake of a database passed callers to, by name and UID: a caller that arrives before that same pod is Ready is passed to its address too. Never an address. */
  const warm = new Map<Database, { name: string; uid: string }>()
  /**
   * What the gateway keeps per database it serves, apart from the metrics: the last reading of its Cluster (from the one list a namespace's look makes, and updated
   * at once by the gateway's own changes), the moment it last saw use (`lastActive`: a session ended, a server just started, a caller waiting, or the gateway itself starting) and the callers
   * waiting on it. The idle time runs from `lastActive`; while a session is open, a caller waits or a wake is under way it does not run.
   */
  const watches = new Map<Database, Watch>()
  const watchOf = (database: Database): Watch => {
    let w = watches.get(database)
    if (!w) {
      w = { lastActive: now(), waiting: 0, changedAt: 0 }
      watches.set(database, w)
    }
    return w
  }
  const sockets = new Set<Socket>()
  let apiErrors = 0
  let timer: ReturnType<typeof setInterval> | undefined
  let refreshing: Promise<void> | undefined
  let stopped = false
  const said = new Set<string>()
  /** Says a thing once (a Service the gateway cannot use is read every few seconds, and the log should not say so every time). */
  const sayOnce = (line: string) => {
    if (said.has(line)) return
    said.add(line)
    log(line)
  }
  /** What a fault last said, by what it is about: a fault that goes on is said once, and again only when it changes or has gone and come back. */
  const faults = new Map<string, string>()
  const fault = (about: string, message: string) => {
    if (faults.get(about) === message) return
    faults.set(about, message)
    log(`pg-gateway: ${about}: ${message}`)
  }

  const seriesOf = (database: Database): Series => {
    let s = series.get(database)
    if (!s) {
      const perStage = <T>(make: () => T): Record<Stage, T> => ({ light: make(), full: make() })
      s = {
        connections: 0,
        sessions: 0,
        wakes: perStage(() => ({ woken: 0, timed_out: 0, refused: 0 })),
        sleeps: { light: 0, full: 0 },
        refused: { light: 0, full: 0 },
        // the two labels that are not role names are there at zero from the start, so `increase` sees the first one
        callers: perStage(() => new Map([[WHO_ENCRYPTED, 0], [WHO_OTHER, 0]])),
        logins: new Set(),
        holds: { reached: 0, gave_up: 0 },
        buckets: perStage(() => BUCKETS.map(() => 0)),
        sum: { light: 0, full: 0 },
        count: { light: 0, full: 0 },
      }
      series.set(database, s)
    }
    return s
  }

  const apiError = (what: string, error: unknown) => {
    apiErrors += 1
    log(`pg-gateway: ${what} failed: ${(error as Error).message}`)
  }
  const listenKey = (port: number, database: Database) => `cannot listen on ${port} for ${database}`

  // ── the routes ───────────────────────────────────────────────────────────────

  /** The database and port a Service leads to, or null when it is not a route (or not a usable one). */
  function routeOf(service: ServiceRow): { database: Database; port: number } | null {
    const value = service.metadata.annotations?.[names.routeAnnotation]
    if (value === undefined) return null
    const parts = value.split('/')
    if (parts.length !== 2 || !NAME.test(parts[0]!) || !NAME.test(parts[1]!)) {
      sayOnce(`pg-gateway: service ${service.metadata.name}: the route "${value}" is not "<namespace>/<cluster>"`)
      return null
    }
    const port = (service.spec?.ports ?? []).map((p) => p.targetPort).find((p): p is number => typeof p === 'number')
    if (port === undefined || !names.portRanges.some((r) => port >= r.from && port <= r.to)) {
      sayOnce(`pg-gateway: service ${service.metadata.name}: its target port ${port} is not in a range the gateway listens on`)
      return null
    }
    return { database: value, port }
  }

  function listen(port: number, database: Database): Promise<Server | null> {
    return new Promise((resolve) => {
      const server = createServer((client) => void onConnection(port, client))
      server.once('error', (error) => {
        // Tried again at every look at the Services, and said once.
        fault(listenKey(port, database), (error as Error).message)
        resolve(null)
      })
      server.listen(port, listenHost, () => {
        faults.delete(listenKey(port, database))
        // From here an error of the listener is said, not thrown.
        server.removeAllListeners('error')
        server.on('error', (error) => log(`pg-gateway: the listener on ${port} for ${database}: ${(error as Error).message}`))
        resolve(server)
      })
    })
  }

  /** One look at the Services: listens on a new route's port and stops listening on a removed one's. A failed read changes nothing. */
  async function refresh(): Promise<void> {
    if (refreshing) return refreshing
    refreshing = (async () => {
      let rows: ServiceRow[]
      try {
        rows = await api.listServices()
      } catch (error) {
        // Counted every time, said once (it is read every few seconds and an API that is down stays down for a while).
        apiErrors += 1
        fault('listing the Services failed', (error as Error).message)
        return
      }
      if (faults.delete('listing the Services failed')) log('pg-gateway: listing the Services works again')
      const wanted = new Map<number, Database>()
      for (const service of [...rows].sort((a, b) => a.metadata.name.localeCompare(b.metadata.name))) {
        const route = routeOf(service)
        if (!route) continue
        const held = wanted.get(route.port)
        if (held !== undefined) {
          sayOnce(`pg-gateway: service ${service.metadata.name}: port ${route.port} already leads to ${held}`)
          continue
        }
        wanted.set(route.port, route.database)
      }
      const attempts = new Set([...wanted].map(([port, database]) => listenKey(port, database)))
      for (const key of [...faults.keys()]) if (key.startsWith('cannot listen on ') && !attempts.has(key)) faults.delete(key)
      for (const [port, route] of [...routes]) {
        if (wanted.has(port)) continue
        // Stops accepting and closes the callers still waiting for a database; the sessions already open on it run on (the database's own going ends them).
        route.server.close()
        for (const client of route.held) client.destroy()
        routes.delete(port)
        warm.delete(route.database)
        watches.delete(route.database)
        log(`pg-gateway: route ${port} to ${route.database} removed`)
      }
      for (const [port, database] of wanted) {
        const existing = routes.get(port)
        if (existing) {
          existing.database = database
          continue
        }
        const server = await listen(port, database)
        if (!server || stopped) {
          server?.close()
          continue
        }
        routes.set(port, { database, server, held: new Set() })
        seriesOf(database)
        // from here the idle time of a database the gateway has just met runs from now (it did not see the last session of one that was already awake)
        watchOf(database)
        log(`pg-gateway: route ${port} to ${database}`)
      }
    })().finally(() => {
      refreshing = undefined
    })
    return refreshing
  }

  // ── the dial ─────────────────────────────────────────────────────────────────

  /** One dial of `to`, on behalf of a caller that is waiting: the socket, or null when it is refused, does not answer within `limitMs`, or `signal` is aborted first. */
  function dialTo(to: { host: string; port: number }, limitMs: number, signal?: AbortSignal): Promise<Socket | null> {
    return new Promise((resolve) => {
      let done = false
      const socket = dial(to)
      socket.setNoDelay(true)
      const finish = (result: Socket | null) => {
        if (done) return
        done = true
        clearTimeout(timeout)
        signal?.removeEventListener('abort', cancel)
        if (!result) socket.destroy()
        resolve(result)
      }
      const cancel = () => finish(null)
      const timeout = setTimeout(() => finish(null), limitMs)
      socket.on('error', () => finish(null))
      socket.once('connect', () => {
        keepAlive(socket)
        finish(socket)
      })
      if (signal?.aborted) cancel()
      else signal?.addEventListener('abort', cancel, { once: true })
    })
  }

  /** One dial of the database's read-write Service (or whatever `target` says). */
  const dialOnce = (database: Database, limitMs: number, signal?: AbortSignal) => dialTo(target(database), limitMs, signal)

  /** Waits `ms`, or until `signal` is aborted. */
  const sleepUnless = (ms: number, signal: AbortSignal) =>
    new Promise<void>((resolve) => {
      if (signal.aborted) return resolve()
      const done = () => {
        clearTimeout(timer)
        signal.removeEventListener('abort', done)
        resolve()
      }
      const timer = setTimeout(done, ms)
      signal.addEventListener('abort', done, { once: true })
    })

  /**
   * The question to a server (see the header): one connection to `to`, one startup message of the probe role, the first answer read, the socket closed. `open`: it takes logins. `starting`:
   * it answered "cannot connect now". `unreachable`: refused, closed, silent for `limitMs`, or not a Postgres. Nothing is ever sent after the first message, so no login happens and no
   * session is opened.
   */
  function ask(to: { host: string; port: number }, limitMs: number): Promise<Answer> {
    return new Promise((resolve) => {
      let done = false
      const socket = dial(to)
      socket.setNoDelay(true)
      let bytes = Buffer.alloc(0)
      const finish = (answer: Answer) => {
        if (done) return
        done = true
        clearTimeout(timeout)
        socket.destroy()
        resolve(answer)
      }
      const timeout = setTimeout(() => finish('unreachable'), limitMs)
      socket.on('error', () => finish('unreachable'))
      socket.on('close', () => finish('unreachable'))
      socket.once('connect', () => socket.write(PROBE_PACKET))
      socket.on('data', (chunk: Buffer) => {
        bytes = Buffer.concat([bytes, chunk])
        const answer = readAnswer(bytes)
        if (answer) finish(answer)
      })
    })
  }

  // ── the server's own address ────────────────────────────────────────────────

  /**
   * One read of the pod named `pod` as a server of `database`'s Cluster that can be dialed, or null. A failed read is counted, said once, and is no address: the wake (or the hold) goes on
   * through the Service.
   */
  async function readPod(database: Database, pod: string): Promise<Located | null> {
    const [namespace, name] = database.split('/') as [string, string]
    const about = `reading the server of ${database}`
    try {
      const found = locatedOf(await api.getPod(namespace, pod), name)
      if (faults.delete(about)) log(`pg-gateway: ${about} works again`)
      return found
    } catch (error) {
      apiErrors += 1
      fault(about, (error as Error).message)
      return null
    }
  }

  /**
   * The pod `pod` (the name the Cluster gave as its current primary when the wake read it) as a server made for the wake: not created before the second the wake began in (a pod's creation
   * time has a one-second grain, so with the clocks of this pod and the API server within a second a pod the wake made always passes; a pod older than the wake is not this wake's server and
   * the Service is the way in).
   */
  async function locate(database: Database, pod: string, since: number | undefined): Promise<Located | null> {
    const found = await readPod(database, pod)
    return found && (since === undefined || found.created >= Math.floor(since / 1000) * 1000) ? found : null
  }

  /** Called when `wake.direct` is set: the callers waiting on a dial that hangs go on at once. */
  const announce = (wake: Wake) => {
    const old = wake.changed
    wake.changed = new AbortController()
    old.abort()
  }

  /**
   * Finds the server's own address for a wake and asks it whether it takes logins, until it does (then `direct` is set), the wake is over, or no caller is waiting. Reads the pod the
   * Cluster named as its primary (`wake.primary`, from the one read of the Cluster the wake makes: a Cluster that names none is a wake through the Service) every `locateRetryMs` and asks every
   * `askRetryMs` once the pod has an address; a pod that is replaced or goes while it is asked starts the finding again from the read. It dials for no one: it runs only while a caller
   * waits on the wake, and a caller that joins later starts it again (`ensureFinder`).
   */
  async function find(database: Database, wake: Wake): Promise<void> {
    const primary = wake.primary
    if (!primary) return
    // A finding belongs to the stretch of the wake in which a caller waits: once the last caller leaves (`gap` moves) it ends, and nothing it read or asked is used by a caller that joins later.
    const gap = wake.gap
    // a full wake's pod is one made for it; a light wake's is the one that was there (the fence stopped Postgres and kept the pod), so it has no creation to check
    const since = wake.stage === 'full' ? wake.startedAt : undefined
    let located: Located | null = null
    let lastLocate = -Infinity
    while (!stopped && !wake.settled && wake.outcome === undefined && wake.waiters > 0 && wake.gap === gap) {
      if (now() - lastLocate >= locateRetryMs) {
        lastLocate = now()
        located = await locate(database, primary, since)
        continue
      }
      if (located) {
        const answer = await ask(podTarget(located.ip), wakeDialMs)
        if (answer !== 'open') wake.sawStopped = true
        // just after the gateway's own fence, a yes may be the Postgres that has not begun to stop: it is taken only once the server has been seen stopped, or the wait is over
        const unsure = answer === 'open' && wake.mustSeeStoppedUntil !== undefined && !wake.sawStopped && now() < wake.mustSeeStoppedUntil
        if (answer === 'open' && !unsure) {
          // The pod is read once more, so the address a caller is passed to is of a pod read a moment before, never of an earlier reading that the pod may have outlived.
          wake.answered = true
          const again = await locate(database, primary, since)
          if (wake.gap !== gap || wake.waiters === 0) return
          if (again && again.uid === located.uid) {
            wake.direct = again
            announce(wake)
            return
          }
          located = again
          lastLocate = now()
          continue
        }
        if (answer !== 'open') wake.asked[answer === 'starting' ? 'starting' : 'unreachable'] += 1
      }
      await sleep(askRetryMs)
    }
  }

  /** Starts the finding for a wake that has none under way and no address: called by each caller that waits on it, so a caller that joins after the others left starts it again. */
  function ensureFinder(database: Database, wake: Wake) {
    if (wake.finding || wake.direct || wake.settled || stopped) return
    wake.finding = true
    void find(database, wake).finally(() => (wake.finding = false))
  }

  /**
   * A caller that arrives while the pod the last wake passed callers to is not yet Ready (its endpoint is not in the Service for another second or so): that pod is read again by name, and
   * if it is the same one (UID), not on its way out and still not Ready, it is asked and dialed at its own address. Anything else, or any failure, is no address: the Service is the way in.
   */
  async function dialWarm(database: Database): Promise<Socket | null> {
    const remembered = warm.get(database)
    if (!remembered) return null
    const pod = await readPod(database, remembered.name)
    if (!pod || pod.uid !== remembered.uid || pod.ready) {
      warm.delete(database)
      return null
    }
    if ((await ask(podTarget(pod.ip), wakeDialMs)) !== 'open') return null
    return await dialTo(podTarget(pod.ip), wakeDialMs)
  }

  // ── the wake ─────────────────────────────────────────────────────────────────

  const dropWake = (database: Database, wake: Wake) => {
    clearTimeout(wake.timer)
    if (wakes.get(database) === wake) wakes.delete(database)
  }

  /**
   * A wake at its deadline: counted as given up on, and dropped, by whichever comes first, the timer or the last caller to see the time is up. A wake nobody is waiting on at its deadline
   * is dropped without a count: only a caller's dial can say whether the server answered, and the gateway dials for no one else. **A dial still in flight decides it**: the deadline waits for
   * it (a dial ends within its own limit), so a caller that is passed through is never counted as a wake that timed out; whichever dial ends last asks again.
   *
   * A wake that was left with no caller (`gap`) has only late joiners, each with a limit of its own (`hold`), and stays the database's one wake until the last of them leaves, so that a new
   * caller joins it and no second look is made. At its deadline it is counted `timed_out` once, when a late joiner waits and no ask was ever answered "takes logins" (the server never came in the
   * wake limit, whoever waited: `PostgresWakeFailed` must see it; a server that answered is never counted as failed), and the joiners go on to their own limits.
   */
  function expire(database: Database, wake: Wake) {
    if (wake.settled) return
    if (wake.gap > 0) {
      if (wake.waiters === 0) return dropWake(database, wake)
      if (!wake.answered && !wake.counted) {
        wake.counted = true
        seriesOf(database).wakes[wake.stage].timed_out += 1
        log(`pg-gateway: ${database} did not answer in ${holdLimitMs} ms: ${wake.waiters} caller(s) that joined its wake late are held to their own limit`)
      }
      return
    }
    if (wake.dialing > 0) return
    wake.settled = true
    wake.outcome = 'timed_out'
    if (wake.waiters > 0) {
      seriesOf(database).wakes[wake.stage].timed_out += 1
      log(`pg-gateway: ${database} did not answer in ${holdLimitMs} ms: ${wake.waiters} caller(s) closed`)
    }
    dropWake(database, wake)
  }

  /** The label a caller's first message gives, and what is counted: the login if it is a well-formed role name that has a label (or can have one), `encrypted`, or `other`. */
  const labelOf = (database: Database, read: Startup): string => {
    if (read.kind === 'encrypted') return WHO_ENCRYPTED
    if (read.kind !== 'startup' || read.user === undefined || !ROLE_NAME.test(read.user) || read.user === WHO_ENCRYPTED || read.user === WHO_OTHER) return WHO_OTHER
    const s = seriesOf(database)
    if (!s.logins.has(read.user)) {
      if (s.logins.size >= MAX_LOGINS) return WHO_OTHER
      s.logins.add(read.user)
    }
    return read.user
  }

  /** What a log line says of a caller: the label, and the database it named when that is a well-formed name too (nothing else of its message is ever logged). */
  const calledBy = (label: string, read: Startup): string =>
    read.kind === 'startup' && label !== WHO_OTHER && read.database !== undefined && ROLE_NAME.test(read.database) ? `login ${label} on database ${read.database}` : `login ${label}`

  /**
   * Who began a wake (the caller that made it, whose first message is read; the others only join): counted in `pg_gateway_wake_callers_total` and returned for the wake's log line. A caller whose
   * first message is not whole yet is waited for in the background, never holding the wake up, for `FIRST_MESSAGE_MS`, then read as `other`; its line is then a line of its own.
   */
  function noteCaller(database: Database, wake: Wake, caller: Caller): string | undefined {
    const count = (label: string) => {
      const perLogin = seriesOf(database).callers[wake.stage]
      perLogin.set(label, (perLogin.get(label) ?? 0) + 1)
    }
    const first = readStartup(headOf(caller.pending, MAX_STARTUP_BYTES))
    if (first.kind !== 'more') {
      const label = labelOf(database, first)
      count(label)
      wake.who = calledBy(label, first)
      return wake.who
    }
    void (async () => {
      const until = now() + firstMessageMs
      let read: Startup = first
      while (read.kind === 'more' && now() < until && !caller.client.destroyed && !stopped) {
        await sleep(10)
        read = readStartup(headOf(caller.pending, MAX_STARTUP_BYTES))
      }
      if (stopped) return
      if (read.kind === 'more') read = { kind: 'other' }
      const label = labelOf(database, read)
      count(label)
      wake.who = calledBy(label, read)
      log(`pg-gateway: ${database}: the ${wake.stage} wake was begun by ${wake.who}`)
    })()
    return undefined
  }

  /**
   * The look at `database` that is under way, or a new one, begun here: the Cluster is read, and the mark turned off when it is asleep and declared to sleep. Only that last is a wake
   * (counted, timed, held for the wake limit); a Cluster that is not asleep, or cannot be read, gives `wait`, and each caller holds for itself. `caller` is the one that makes the wake, if
   * this call does: it is read for who woke the database, and no one else is.
   */
  function wakeOf(database: Database, caller: Caller): Wake {
    const existing = wakes.get(database)
    if (existing) return existing
    const [namespace, name] = database.split('/') as [string, string]
    const wake: Wake = {
      startedAt: now(),
      deadline: now() + holdLimitMs,
      stage: 'full',
      answered: false,
      sawStopped: false,
      counted: false,
      settled: false,
      waiters: 0,
      dialing: 0,
      finding: false,
      gap: 0,
      changed: new AbortController(),
      asked: { starting: 0, unreachable: 0 },
      verdict: undefined as never,
    }
    wakes.set(database, wake)
    wake.verdict = (async () => {
      const decide = async (): Promise<'wake' | 'wait' | 'refused' | 'close'> => {
        // One change at a time per database: a sleep patch the gateway has in flight settles first, so that the Cluster read here is the one the wake acts on (a wake read before a hibernation that is
        // in flight would lift a fence that is about to be a hibernation, and wait for a pod that is going).
        const mine = watchOf(database)
        if (mine.changing) await mine.changing
        let cluster: ClusterRow | null
        try {
          cluster = await api.getCluster(namespace, name)
        } catch (error) {
          // The Cluster cannot be read: the caller is held as for a database that is not asleep (dials of `dialTimeoutMs` for `awakeHoldMs`), so that an API outage does not close a caller of a
          // database that is awake and was only slow to answer the short first dial (it was served by the 3 s first dial before the short one, with no API call). Nothing is woken.
          apiError(`reading ${database}`, error)
          return 'wait'
        }
        if (!cluster) {
          log(`pg-gateway: ${database} has no Cluster; the caller is closed`)
          return 'close'
        }
        const state = classify(cluster, names)
        wake.stage = stageOf(state, cluster, names)
        watchOf(database).cluster = cluster
        watchOf(database).state = state
        if (state === 'by-hand') {
          log(`pg-gateway: ${database} is stopped and not declared to sleep, or stopped by hand (a fence of someone else's, or a mark on a database that does not sleep): the caller is refused, nothing is woken`)
          return 'refused'
        }
        if (state === 'awake') return 'wait'
        try {
          await api.wake(namespace, name, wake.stage)
        } catch (error) {
          // A wake the gateway gave up on, by the same count as one that timed out: the alert reads it, and no caller is left waiting.
          apiError(`waking ${database}`, error)
          seriesOf(database).wakes[wake.stage].timed_out += 1
          return 'close'
        }
        wake.primary = cluster.status?.currentPrimary
        // what an earlier wake passed callers to is of no use to this one: its address is read again (a look at an awake database is no wake, and keeps what the last one left)
        warm.delete(database)
        // a server about to start counts as in use: the idle time runs from here, so a woken server stays up its whole light time, and the gateway's own change is the state callers meet at once
        const w = watchOf(database)
        w.lastActive = now()
        w.changedAt = now()
        w.state = 'awake'
        if (wake.stage === 'light' && w.fencedAt !== undefined && now() - w.fencedAt < fenceRecentMs) wake.mustSeeStoppedUntil = now() + stoppedWaitMs
        // Naming the caller is bookkeeping: whatever goes wrong in it, the wake goes on.
        let who: string | undefined
        try {
          who = noteCaller(database, wake, caller)
        } catch (error) {
          fault(`naming who woke ${database}`, (error as Error).message)
        }
        log(`pg-gateway: waking ${database} (${wake.stage} sleep)${who ? `, begun by ${who}` : ''}`)
        return 'wake'
      }
      const verdict = await decide()
      if (verdict !== 'wake') {
        dropWake(database, wake)
        return verdict
      }
      // An unsettled wake stays until its deadline, even when every caller has left (the callers' count is not what keeps it): a caller that arrives later, a client that retried after a
      // connect timeout shorter than the wake, joins it and is passed to the server as soon as it is up, instead of reading the Cluster again and being held as for an awake database.
      // The deadline drops it uncounted when nobody waits (`expire`).
      if (wakes.get(database) === wake) wake.timer = setTimeout(() => expire(database, wake), Math.max(0, wake.deadline - now()))
      return verdict
    })()
    return wake
  }

  /** The caller of a database that is not asleep whose dial was refused (a server starting, or one that will not): waits `awakeHoldMs` for it to answer, and is counted for it. */
  async function holdAwake(database: Database, client: Socket): Promise<Socket | null> {
    const deadline = now() + awakeHoldMs
    for (;;) {
      if (client.destroyed || stopped) return null
      const upstream = (await dialWarm(database)) ?? (await dialOnce(database, dialTimeoutMs))
      if (upstream) {
        seriesOf(database).holds.reached += 1
        return upstream
      }
      const left = deadline - now()
      if (left <= 0) {
        seriesOf(database).holds.gave_up += 1
        log(`pg-gateway: ${database} is not asleep and did not answer in ${awakeHoldMs} ms: a caller is closed`)
        return null
      }
      await sleep(Math.min(retryMs, left))
    }
  }

  /** The caller whose dial was refused: waits for the database to answer, and returns the connection it gets, or null when it is closed. */
  async function hold(database: Database, client: Socket, pending: Buffer[]): Promise<Socket | null> {
    const wake = wakeOf(database, { client, pending })
    // Counted before the verdict is awaited: a caller that leaves while the Cluster is read and patched must not drop the wake under the callers still on it.
    wake.waiters += 1
    try {
      const verdict = await wake.verdict
      if (verdict === 'refused') {
        seriesOf(database).wakes[wake.stage].refused += 1
        return null
      }
      if (verdict === 'close') return null
      if (verdict === 'wait') return await holdAwake(database, client)
      // A caller that joins a wake that was left with no caller is a late one: its limit is at least the awake hold's (where main would have held it), not what is left of a wake that began
      // long before it arrived, and the wake gives no time sample (`settle`).
      const late = wake.gap > 0
      const limit = late ? Math.max(wake.deadline, now() + awakeHoldMs) : wake.deadline
      for (;;) {
        // The deadline has passed and was counted: nobody is served after it, whatever a later dial would find.
        if (client.destroyed || stopped || wake.outcome === 'timed_out') return null
        ensureFinder(database, wake)
        // Read together, with nothing between: a server found after this is an abort of this signal, so a dial that hangs or a wait is never missed.
        const signal = wake.changed.signal
        const via = wake.direct
        // A light wake does not dial the Service while the server's own address is being found: the fenced pod stays in the Service's endpoints for up to 18 s (it turns not Ready at the next probe),
        // so a dial of the Service connects the moment Postgres listens, before it takes logins, and the caller would meet "starting up" inside its encryption. The Service is the way in only when
        // the address cannot be found (no primary named, the pod not readable): after `lightDirectGraceMs`.
        if (!via && wake.stage === 'light' && wake.primary && now() - wake.startedAt < lightDirectGraceMs) {
          const left = limit - now()
          if (left <= 0) {
            if (late) {
              seriesOf(database).holds.gave_up += 1
              log(`pg-gateway: ${database} did not answer a caller that joined its wake late, in ${awakeHoldMs} ms or the wake's limit: the caller is closed`)
            } else expire(database, wake)
            return null
          }
          await sleepUnless(Math.min(wakeRetryMs, left, 50), signal)
          continue
        }
        wake.dialing += 1
        // The server's own address once it has answered that it takes logins; until then the Service (a dial of it hangs while no endpoint is ready, and is abandoned at `wakeDialMs` or when the address is found).
        const upstream = await (via ? dialTo(podTarget(via.ip), wakeDialMs) : dialOnce(database, wakeDialMs, signal)).finally(() => (wake.dialing -= 1))
        if (upstream) {
          if (!wake.settled) settle(database, wake, via)
          return upstream
        }
        // It took logins a moment ago and does not answer now (the pod went): the address is read again.
        if (via && wake.direct === via) wake.direct = undefined
        const left = limit - now()
        if (left <= 0) {
          if (late) {
            seriesOf(database).holds.gave_up += 1
            log(`pg-gateway: ${database} did not answer a caller that joined its wake late, in ${awakeHoldMs} ms or the wake's limit: the caller is closed`)
          } else expire(database, wake)
          return null
        }
        await sleepUnless(Math.min(wakeRetryMs, left), signal)
      }
    } finally {
      wake.waiters -= 1
      // The last caller gone before the server answered: the wake stays (a later caller joins it), but what was found for the callers that waited is not for the one that comes: the address is
      // dropped and a finding in progress ends, so that caller reads the pod and asks for itself.
      if (wake.waiters === 0 && !wake.settled) {
        wake.gap += 1
        wake.direct = undefined
        // a wake kept for late joiners past its deadline goes with the last of them
        if (now() >= wake.deadline) dropWake(database, wake)
      }
    }
  }

  /** The first answer of a wake: counted, timed, and logged with the way it came (the server's own address, with the asks that came before, or the Service). */
  function settle(database: Database, wake: Wake, via: Located | undefined) {
    wake.settled = true
    wake.outcome = 'woken'
    dropWake(database, wake)
    if (via) warm.set(database, { name: via.name, uid: via.uid })
    const w = watchOf(database)
    w.lastActive = now()
    w.state = 'awake'
    const seconds = (now() - wake.startedAt) / 1000
    const s = seriesOf(database)
    // a wake counted failed at its deadline (while late joiners waited) is not counted woken as well
    if (!wake.counted) s.wakes[wake.stage].woken += 1
    // a wake that was left with no caller and joined later was answered for a caller that came after, so its seconds are not a wake time: counted woken, not timed
    if (wake.gap === 0) {
      const stage = wake.stage
      s.sum[stage] += seconds
      s.count[stage] += 1
      BUCKETS.forEach((le, i) => {
        if (seconds <= le) s.buckets[stage][i]! += 1
      })
    }
    const how = via
      ? `by the server's own address (${via.name} at ${via.ip}; asked before it took logins: ${wake.asked.starting} "starting up", ${wake.asked.unreachable} unanswered)`
      : 'through the Service'
    log(`pg-gateway: ${database} answered ${seconds.toFixed(1)} s after the ${wake.stage} wake began, ${how}${wake.who ? `; begun by ${wake.who}` : ''}`)
  }

  // ── the two stages of sleep ──────────────────────────────────────────────────

  /**
   * The gateway decides when a database sleeps, from the sessions it carries. Use is a session through it: the idle time runs from the moment the last one ended
   * (or from the moment the gateway first met the database, or a server just started, so a server that has just started stays up its whole light time), and does not run while a session is
   * open, a caller waits or a wake is under way. **Light sleep** (after the light time, `xata.io/scale-to-zero-inactivity-minutes`) is CloudNativePG's fence on every instance, set in one merge
   * patch with the gateway's mark, the moment the last session ended: the instance manager stops Postgres and keeps the pod, and the next caller is passed to the same pod's address within a
   * third of a second. **Full sleep** (after the full time, the full-sleep annotation's minutes, counted from the same moment, which the mark carries so that a restart of the gateway does not
   * reset it) is the hibernation, as before: from light sleep the mark is set with the fence and mark kept, and once the hibernation condition is true (the pod is gone) the fence and mark are
   * removed (a fence lifted while the pod still exists would start Postgres for a moment); from awake, when the light time is not shorter than the full time, the hibernation alone. A full time
   * of `never` keeps a database in light sleep for good; none (the annotation absent) is the light time: full sleep only, as before the two stages.
   *
   * It acts only on a Cluster it has a route for and that is declared to sleep, and never while a Backup of the Cluster is unfinished (a fence in the middle of a backup fails it, and a branch's
   * origin snapshot is a Backup of its parent). A permanent database is the one with Backups (its route is in the permanent range); a branch has none. It never touches a database stopped by hand.
   */
  const busy = (database: Database) => seriesOf(database).sessions > 0 || watchOf(database).waiting > 0 || wakes.has(database)

  const isPermanent = (database: Database) => [...routes].some(([port, route]) => route.database === database && port >= names.permanentPorts.from && port <= names.permanentPorts.to)

  /** Whether a Backup of the database's Cluster is not yet finished. A database with none (a branch) has none; a list that cannot be read is "unfinished": nothing sleeps on a guess. */
  async function backupUnfinished(database: Database): Promise<boolean> {
    if (!isPermanent(database)) return false
    const [namespace, name] = database.split('/') as [string, string]
    const about = `listing the Backups of ${namespace}`
    try {
      const rows = await api.listBackups(namespace)
      if (faults.delete(about)) log(`pg-gateway: ${about} works again`)
      return rows.some((b) => b.spec?.cluster?.name === name && b.status?.phase !== 'completed' && b.status?.phase !== 'failed')
    } catch (error) {
      apiErrors += 1
      fault(about, (error as Error).message)
      return true
    }
  }

  /** The cached Cluster with the gateway's own change applied (a null removes the annotation), so that the next caller and the schedules' keeping see it at once. */
  const applied = (w: Watch, changes: Record<string, string | null>) => {
    if (!w.cluster) return
    const annotations = { ...(w.cluster.metadata?.annotations ?? {}) }
    for (const [key, value] of Object.entries(changes)) {
      if (value === null) delete annotations[key]
      else annotations[key] = value
    }
    w.cluster = { ...w.cluster, metadata: { ...w.cluster.metadata, annotations } }
  }

  /** The light time of a database declared to sleep (whole minutes), or undefined. */
  const lightMinutes = (cluster: ClusterRow) => minutesOf(cluster.metadata?.annotations?.[names.sleepIdleMinutesAnnotation])
  /** The full time: the light time when none is declared, `Infinity` for `never`, or undefined when what is declared is no number. */
  const fullMinutes = (cluster: ClusterRow, light: number): number | undefined => {
    const declared = cluster.metadata?.annotations?.[names.sleepFullMinutesAnnotation]
    if (declared === undefined) return light
    return declared === names.sleepNever ? Infinity : minutesOf(declared)
  }

  /**
   * Puts the gateway's own change to one database's Cluster in flight: **one change at a time**. The state callers meet is set before the patch is sent (in the same step as the last check that
   * nobody is using the database), so a caller that arrives while it is in flight is held for the sleep and not dialed (a session the fence would then end), and a wake reads the Cluster only
   * after it has settled (`wakeOf`). A patch that fails, or is refused because the Cluster changed since the row it was decided on (409), leaves the database as it was and is decided again at
   * the next look. Returns whether it was made. `sleepFor` is the stage of a sleep the change is for (none for the removal of a fence and mark, which is the end of a full sleep and no sleep of its
   * own): a sleep the API server refused (409) is counted by it, `pg_gateway_sleeps_refused_total`, beside the log line.
   */
  async function change(database: Database, w: Watch, to: State, patch: () => Promise<void>, after: () => void, sleepFor?: Stage): Promise<boolean> {
    const previous = w.state
    w.state = to
    w.changedAt = now()
    const done = patch().then(
      () => true,
      (error: Error) => {
        if (/^409\b/.test(error.message)) {
          if (sleepFor) seriesOf(database).refused[sleepFor] += 1
          log(`pg-gateway: ${database} changed since it was read: not put to sleep now, decided again at the next look`)
        } else apiError(`putting ${database} to sleep`, error)
        return false
      },
    )
    const settled = done.then(() => undefined)
    w.changing = settled
    const ok = await done
    w.changedAt = now()
    if (w.changing === settled) w.changing = undefined
    if (ok) after()
    else if (w.state === to) w.state = previous
    return ok
  }

  /** Suspends the ScheduledBackups of a permanent database that are not suspended, and marks each (what `keepSchedules` does for a sleeping one): before a sleep, so no Backup is made between the look at the Backups and the fence. */
  async function suspendSchedules(database: Database): Promise<void> {
    const [namespace, cluster] = database.split('/') as [string, string]
    for (const schedule of await api.listSchedules(namespace, cluster)) {
      if (schedule.spec?.suspend === true) continue
      await api.suspendSchedule(namespace, schedule.metadata.name)
      log(`pg-gateway: ${database} is about to sleep: schedule ${schedule.metadata.name} suspended`)
    }
  }

  /**
   * What the gateway does about one database at a look: takes in its Cluster, starts or stops the idle clock, and puts it into the next stage when its time has come. `listedAt` is when the
   * list the row came from was asked for: a row listed before the gateway's own last change to the database, or read while one is in flight, is older than what the gateway has done, and
   * nothing is taken from it (not its state, not its mark, not a decision). Every patch carries the row's `resourceVersion`.
   */
  async function observe(database: Database, row: ClusterRow, listedAt: number): Promise<void> {
    const w = watchOf(database)
    if (w.changing || listedAt < w.changedAt) return
    const state = classify(row, names)
    const marked = row.metadata?.annotations?.[names.idleSinceAnnotation]
    // A database that is awake and was not (somebody woke it, or the gateway did) starts its idle time now. A database in light sleep carries the moment in its mark, which is what the full time
    // counts from: the Cluster is the memory, so a restart of the gateway does not reset it.
    if (state === 'awake' && w.state !== 'awake' && w.state !== undefined) w.lastActive = now()
    if (fenceOf(row, names) === 'ours' && marked !== undefined) w.lastActive = Date.parse(marked)
    if (busy(database)) w.lastActive = now()
    w.state = state
    w.cluster = row
    if (row.metadata?.annotations?.[names.sleepEnabledAnnotation] !== 'true') return
    const version = row.metadata?.resourceVersion
    // never an unconditional patch: without the row's version a decision could be applied to a Cluster that is no longer the one it was made about
    if (!version) return
    const [namespace, name] = database.split('/') as [string, string]
    const idle = now() - w.lastActive
    try {
      if (state === 'full-sleep') {
        // The way down from light sleep is finished: the pod is gone (the condition), so the fence and mark that were kept for it are removed, together.
        const gone = (row.status?.conditions ?? []).some((c) => c.type === HIBERNATION_CONDITION && c.status === 'True')
        if (gone && fenceOf(row, names) === 'ours') {
          await change(database, w, 'full-sleep', () => api.unfence(namespace, name, version), () => {
            applied(w, { [names.fencedInstancesAnnotation]: null, [names.idleSinceAnnotation]: null })
            log(`pg-gateway: ${database} is in full sleep (the pod is gone): its fence and mark are removed`)
          })
        }
        return
      }
      const light = lightMinutes(row)
      if (light === undefined) return
      const full = fullMinutes(row, light)
      if (full === undefined) return
      if (state === 'awake') {
        // A server that is not ready is not put to sleep: a fence on one that is still starting, or a hibernation of one that is recovering, would stop what the operator is making. The Cluster says at
        // this look whether it has a ready instance; if not, nothing is decided and the next look asks again, so it is put to sleep at the first look after it reports one (if all the rest allows).
        const ready = (row.status?.readyInstances ?? 0) >= 1
        const waiting = `not putting ${database} to sleep`
        if (ready && faults.delete(waiting)) log(`pg-gateway: ${database} reports a ready instance again`)
        if (idle < light * 60_000 || busy(database)) return
        if (!ready) {
          fault(waiting, 'its Cluster reports no ready instance')
          return
        }
        if (isPermanent(database)) {
          // The order: look at the Backups, suspend the schedules, look at the Backups again, then sleep, so that no Backup can be made between the look and the fence. (A Backup that is running
          // is seen first, so the schedules are not suspended and resumed at every look for as long as it runs.)
          if (await backupUnfinished(database)) return
          await suspendSchedules(database)
          if (await backupUnfinished(database)) return
        }
        if (busy(database)) return
        const mark = new Date(w.lastActive).toISOString()
        if (full <= light) {
          // the light time is not shorter than the full time (or none is declared): straight to full sleep
          await change(database, w, 'full-sleep', () => api.hibernate(namespace, name, version), () => {
            applied(w, { [names.hibernationAnnotation]: 'on' })
            seriesOf(database).sleeps.full += 1
            log(`pg-gateway: ${database} has had no session for ${Math.round(idle / 1000)} s: full sleep (hibernated)`)
          }, 'full')
        } else {
          await change(database, w, 'light-sleep', () => api.fence(namespace, name, mark, version), () => {
            applied(w, { [names.fencedInstancesAnnotation]: names.fenceAll, [names.idleSinceAnnotation]: mark })
            w.fencedAt = now()
            seriesOf(database).sleeps.light += 1
            log(`pg-gateway: ${database} has had no session for ${Math.round(idle / 1000)} s: light sleep (Postgres stopped, the pod kept)`)
          }, 'light')
        }
      } else if (state === 'light-sleep') {
        if (full === Infinity || idle < full * 60_000 || busy(database) || (await backupUnfinished(database)) || busy(database)) return
        // the fence and its mark stay until the pod is gone
        await change(database, w, 'full-sleep', () => api.hibernate(namespace, name, version), () => {
          applied(w, { [names.hibernationAnnotation]: 'on' })
          seriesOf(database).sleeps.full += 1
          log(`pg-gateway: ${database} has been in light sleep for ${Math.round(idle / 1000)} s: full sleep (hibernated)`)
        }, 'full')
      }
    } catch (error) {
      apiError(`putting ${database} to sleep`, error)
    }
  }

  let looking: Promise<void> | undefined

  /**
   * One look at every database the gateway serves: the Clusters of each namespace it has a route in are listed, one call a namespace and not one a database, and each route's Cluster
   * is taken in and acted on (`observe`). A failed list changes nothing and is said once; the previous reading stands until the next look. It reads and patches Clusters and Backups and never
   * dials a database, so it opens no session.
   */
  async function look(): Promise<void> {
    if (looking) return looking
    looking = (async () => {
      const byNamespace = new Map<string, Set<Database>>()
      for (const route of routes.values()) {
        const [namespace] = route.database.split('/') as [string]
        byNamespace.set(namespace, (byNamespace.get(namespace) ?? new Set()).add(route.database))
      }
      for (const [namespace, databases] of byNamespace) {
        if (stopped) return
        const about = `listing the Clusters of ${namespace}`
        let rows: ClusterRow[]
        const listedAt = now()
        try {
          rows = await api.listClusters(namespace)
        } catch (error) {
          apiErrors += 1
          fault(about, (error as Error).message)
          continue
        }
        if (faults.delete(about)) log(`pg-gateway: ${about} works again`)
        const byName = new Map(rows.map((r) => [r.metadata?.name, r]))
        for (const database of databases) {
          if (stopped) return
          const row = byName.get(database.split('/')[1])
          if (!row) {
            // a Cluster that is not there (deleted, and made again by `apply` under the same route): when it is back it has its whole idle time
            const w = watchOf(database)
            w.cluster = undefined
            w.state = undefined
            w.lastActive = now()
            continue
          }
          await observe(database, row, listedAt)
        }
      }
    })().finally(() => {
      looking = undefined
    })
    return looking
  }

  // ── a connection ─────────────────────────────────────────────────────────────

  /**
   * Passes the bytes both ways, untouched, from here on. Written by hand rather than `pipe`: under Bun a server-side socket reads from the moment it is accepted, and a chunk that
   * arrives while nothing listens is lost, so the client's data is taken from the first moment (`onConnection`) and handed over here. Each side is paused while the other's write
   * buffer is full, so a large read does not pile up in memory.
   */
  function splice(database: Database, client: Socket, upstream: Socket, pending: Buffer[]) {
    const s = seriesOf(database)
    s.sessions += 1
    // a session is use: the idle time starts again when the last one ends (below), and does not run while one is open
    watchOf(database).lastActive = now()
    sockets.add(upstream)
    upstream.on('error', noop)
    let full = false
    for (const chunk of pending) if (!upstream.write(chunk)) full = true
    // A client held back while it waited goes on, unless the database's side is full: its drain goes on for it.
    if (full) client.pause()
    else client.resume()
    upstream.on('data', (chunk) => {
      if (!client.write(chunk)) upstream.pause()
    })
    client.on('drain', () => upstream.resume())
    upstream.on('drain', () => client.resume())
    // A side that goes lets the other finish what it was sent, then ends it; whatever has not closed after a few seconds is cut, both sides, however each is paused.
    // "Goes" is an end, an error or a close, not the close alone: under Bun a socket whose buffered writes cannot be sent (its peer has gone, or reset it, while the
    // other side's read filled that buffer) reports `end` and never `close`, so a pair that waited for `close` stayed open for good, and the database kept its backend and its
    // session count.
    const sides = [client, upstream]
    let cut: ReturnType<typeof setTimeout> | undefined
    let released = false
    let watch: ReturnType<typeof setInterval> | undefined
    const release = () => {
      if (released) return
      released = true
      clearTimeout(cut)
      clearInterval(watch)
      s.sessions -= 1
      watchOf(database).lastActive = now()
      sockets.delete(upstream)
      sockets.delete(client)
    }
    const over = () => {
      if (cut) return
      for (const side of sides) if (!side.destroyed) side.end()
      cut = setTimeout(() => {
        for (const side of sides) side.destroy()
        release()
      }, closeGraceMs)
      cut.unref()
    }
    for (const side of sides) {
      side.once('end', over)
      side.once('error', over)
      side.once('close', () => {
        over()
        if (sides.every((other) => other.destroyed)) release()
      })
    }
    // A write to the client that makes no progress. A client that drops off the network in the middle of a read leaves bytes waiting that nothing will ever accept (keepalive does not probe a
    // socket with data queued, so TCP's own retransmission limit, about 15 minutes, is what finds it, and the database is held awake that long). "Waiting" is bytes handed to the client's socket
    // and not yet accepted by the kernel (`writableLength`), "accepted" is the count of bytes that have left that buffer, and the time starts again whenever the count moves or nothing waits:
    // a session with nothing waiting is never cut however idle, and a reader that is slow but moving is not. Past the limit the pair is ended by the way above. **Only the client's side is
    // watched**: bytes waiting toward the database are the database's to take (a statement that waits on a lock while a client sends a COPY or a pipelined batch must wait as it would on a direct
    // connection), and it is the database's own session that goes with the pair.
    const progress = [client].map((side) => ({ side, accepted: side.bytesWritten - side.writableLength, since: now() }))
    watch = setInterval(() => {
      if (released || cut) return
      const t = now()
      for (const p of progress) {
        const accepted = p.side.bytesWritten - p.side.writableLength
        if (p.side.writableLength === 0 || accepted > p.accepted) p.since = t
        p.accepted = accepted
        if (t - p.since >= stallMs) {
          log(`pg-gateway: ${database}: a write made no progress for ${stallMs} ms: the pair is ended`)
          over()
          return
        }
      }
    }, Math.min(1000, Math.max(10, Math.floor(stallMs / 4))))
    watch.unref()
  }

  async function onConnection(port: number, client: Socket): Promise<void> {
    sockets.add(client)
    client.once('close', () => sockets.delete(client))
    keepAlive(client)
    // What the client sends while no database has the connection is kept here, from the first byte (a Postgres client sends one small packet and waits).
    const pending: Buffer[] = []
    let kept = 0
    let upstream: Socket | null = null
    // A caller that goes before a database has the connection (it half-closes, or resets and Bun says only that it ended) is closed at once: nothing it could be sent is left to
    // send, and a held caller that is not seen to be gone is dialed for until its limit, and handed a database's connection that nobody reads. Once a database has it, `splice` ends the pair.
    const gone = () => {
      if (!upstream) client.destroy()
    }
    client.on('error', gone)
    client.once('end', gone)
    client.on('data', (chunk: Buffer) => {
      if (upstream) {
        if (!upstream.write(chunk)) client.pause()
        return
      }
      pending.push(chunk)
      kept += chunk.length
      if (kept > MAX_PENDING_BYTES) client.pause()
    })
    const route = routes.get(port)
    if (!route) {
      client.destroy()
      return
    }
    const database = route.database
    seriesOf(database).connections += 1
    // Until a database has the connection the caller is on the route's list, so that a route that goes closes it.
    route.held.add(client)
    // A caller waiting is use: the idle time does not run, so a database is not put to sleep under a caller that is on its way in.
    const w = watchOf(database)
    w.waiting += 1
    let reached: Socket | null
    try {
      // A database the gateway last read as asleep (either stage), or one it is waking, is not dialed first: a dial of a Service whose server is not ready waits its 0.3 s, and a database in light
      // sleep is woken at once. A reading that is out of date is no harm: the Cluster is read afresh by the wake, and an awake database is then held as for any that refused a dial.
      const asleep = wakes.has(database) || (w.state !== undefined && w.state !== 'awake')
      reached = (asleep ? null : await dialOnce(database, firstDialMs)) ?? (await hold(database, client, pending))
    } finally {
      route.held.delete(client)
      w.waiting -= 1
    }
    if (!reached) {
      client.destroy()
      return
    }
    if (client.destroyed) {
      reached.destroy()
      return
    }
    // Handed over in one synchronous step, so no chunk falls between what was kept and what is forwarded.
    splice(database, client, reached, pending)
    upstream = reached
  }

  // ── metrics ──────────────────────────────────────────────────────────────────

  /**
   * Prometheus text exposition. Every series of a route is there from the moment the route is, at zero, so that a selector matches before the fault does. A database whose
   * route is gone and whose sessions have ended is left out.
   */
  function metrics(): string {
    const live = new Set([...routes.values()].map((r) => r.database))
    for (const [database, s] of [...series]) if (!live.has(database) && s.sessions <= 0) series.delete(database)
    const rows = [...series].sort(([a], [b]) => a.localeCompare(b))
    const out: string[] = []
    const family = (name: string, help: string, type: string, lines: (database: Database, s: Series) => string[]) => {
      out.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`)
      for (const [database, s] of rows) out.push(...lines(database, s))
    }
    family('pg_gateway_connections_total', 'connections accepted, by database', 'counter', (d, s) => [`pg_gateway_connections_total{database="${d}"} ${s.connections}`])
    family('pg_gateway_sessions_open', 'connections being passed through now, by database', 'gauge', (d, s) => [`pg_gateway_sessions_open{database="${d}"} ${s.sessions}`])
    const perStage = (lines: (stage: Stage, d: Database, s: Series) => string[]) => (d: Database, s: Series) => STAGES.flatMap((stage) => lines(stage, d, s))
    family(
      'pg_gateway_wakes_total',
      'wakes by database, stage (light: the fence lifted, full: the pod made again) and result: woken, timed_out (given up on) or refused (stopped by hand, or asleep and not declared to sleep); only a wake the gateway began by turning a mark off is counted',
      'counter',
      perStage((stage, d, s) => RESULTS.map((r) => `pg_gateway_wakes_total{database="${d}",stage="${stage}",result="${r}"} ${s.wakes[stage][r]}`)),
    )
    family(
      'pg_gateway_sleeps_total',
      'times the gateway put the database into each stage of sleep: light (Postgres stopped by the fence) or full (hibernated)',
      'counter',
      (d, s) => STAGES.map((stage) => `pg_gateway_sleeps_total{database="${d}",stage="${stage}"} ${s.sleeps[stage]}`),
    )
    family(
      'pg_gateway_sleeps_refused_total',
      'sleeps the API server refused because the Cluster had changed since the gateway read it, by database and the stage the sleep was for (light or full); each is decided again at the next look',
      'counter',
      (d, s) => STAGES.map((stage) => `pg_gateway_sleeps_refused_total{database="${d}",stage="${stage}"} ${s.refused[stage]}`),
    )
    family(
      'pg_gateway_wake_callers_total',
      'wakes by database, stage and the login of the caller that began each, read from its first message when that is a plain startup message: a well-formed role name (at most 16 per database), encrypted (it asked for encryption first) or other',
      'counter',
      perStage((stage, d, s) => [...s.callers[stage]].map(([login, n]) => `pg_gateway_wake_callers_total{database="${d}",stage="${stage}",login="${login}"} ${n}`)),
    )
    family('pg_gateway_holds_total', 'callers held for a database that is not asleep and refused a dial, by result: reached (it answered within the hold) or gave_up (closed at the limit); a wake is not counted here', 'counter', (d, s) =>
      HOLD_RESULTS.map((r) => `pg_gateway_holds_total{database="${d}",result="${r}"} ${s.holds[r]}`),
    )
    family(
      'pg_gateway_wake_seconds',
      'seconds from the wake beginning to the database answering, by database and stage; a wake that was left with no caller and joined later gives no sample',
      'histogram',
      perStage((stage, d, s) => [
        ...BUCKETS.map((le, i) => `pg_gateway_wake_seconds_bucket{database="${d}",stage="${stage}",le="${le}"} ${s.buckets[stage][i]}`),
        `pg_gateway_wake_seconds_bucket{database="${d}",stage="${stage}",le="+Inf"} ${s.count[stage]}`,
        `pg_gateway_wake_seconds_sum{database="${d}",stage="${stage}"} ${s.sum[stage]}`,
        `pg_gateway_wake_seconds_count{database="${d}",stage="${stage}"} ${s.count[stage]}`,
      ]),
    )
    out.push('# HELP pg_gateway_api_errors_total calls of the cluster API that failed', '# TYPE pg_gateway_api_errors_total counter', `pg_gateway_api_errors_total ${apiErrors}`, '')
    return out.join('\n')
  }

  /** The metrics port's handler: `/metrics` for Prometheus, `/health` for the pod's probe. */
  const handle = async (req: Request): Promise<Response> => {
    const url = new URL(req.url)
    if (req.method === 'GET' && url.pathname === '/health') return Response.json({ ok: true })
    if (req.method === 'GET' && url.pathname === '/metrics') return new Response(metrics(), { headers: { 'Content-Type': 'text/plain; version=0.0.4' } })
    return new Response('not found', { status: 404 })
  }

  // ── the schedules of a sleeping permanent database ───────────────────────────

  let keeping: Promise<void> | undefined

  /**
   * One look at each permanent database's schedules, in either stage of sleep. A database is permanent when its route's port is in the permanent range. Asleep (a
   * mark or the condition, in either stage, declared to sleep or not: a backup of a hibernated Cluster fails either way, and one of a fenced Cluster is left pending until its pod is Ready):
   * every ScheduledBackup of it that is not suspended is suspended and marked. Awake with a ready server: every one it marked is resumed, and one nobody marked (suspended by hand) is left as it
   * is. A Cluster that is awake and not ready yet keeps its schedules suspended until it answers. It reads the Cluster as the look just took it in (no call of its own) and the schedules, and
   * never dials the database, so it opens no session. A failed call is counted, said once, and tried again at the next look.
   */
  async function keepSchedules(): Promise<void> {
    if (keeping) return keeping
    keeping = (async () => {
      const permanent = new Set<Database>()
      for (const [port, route] of routes) {
        if (port >= names.permanentPorts.from && port <= names.permanentPorts.to) permanent.add(route.database)
      }
      for (const database of permanent) {
        if (stopped) return
        const [namespace, cluster] = database.split('/') as [string, string]
        const about = `keeping the schedules of ${database}`
        try {
          const row = watches.get(database)?.cluster
          if (!row) continue
          const asleepNow = classify(row, names) !== 'awake'
          const ready = (row.status?.readyInstances ?? 0) >= 1
          if (!asleepNow && !ready) continue
          const schedules = await api.listSchedules(namespace, cluster)
          for (const schedule of schedules) {
            const name = schedule.metadata.name
            const marked = schedule.metadata.annotations?.[names.suspendedByAnnotation] !== undefined
            if (asleepNow && schedule.spec?.suspend !== true) {
              await api.suspendSchedule(namespace, name)
              log(`pg-gateway: ${database} is asleep: schedule ${name} suspended`)
            } else if (!asleepNow && marked) {
              await api.resumeSchedule(namespace, name)
              log(`pg-gateway: ${database} answers: schedule ${name} resumed`)
            }
          }
          if (faults.delete(about)) log(`pg-gateway: ${about} works again`)
        } catch (error) {
          apiErrors += 1
          fault(about, (error as Error).message)
        }
      }
    })().finally(() => {
      keeping = undefined
    })
    return keeping
  }

  /** What happens every `refreshMs`: the routes are read, then every database's Cluster (and the sleep it may be due), then the schedules of the permanent databases are kept. */
  const tick = async () => {
    await refresh()
    await look()
    await keepSchedules()
  }

  return {
    refresh,
    /** One round of what `start` does every few seconds: the routes, then the schedules. For a test. */
    tick,
    handle,
    metrics,
    /** The ports it listens on, sorted. */
    ports: () => [...routes.keys()].sort((a, b) => a - b),
    start() {
      void tick()
      timer = setInterval(() => void tick(), refreshMs)
    },
    /** Stops listening, ends every connection and drops every wake: for a test, and for the process's end. */
    async stop() {
      stopped = true
      clearInterval(timer)
      await refreshing
      await looking
      await keeping
      for (const route of routes.values()) route.server.close()
      routes.clear()
      for (const wake of wakes.values()) clearTimeout(wake.timer)
      wakes.clear()
      for (const socket of sockets) socket.destroy()
      sockets.clear()
    },
  }
}

/** The cluster's API as the pod sees it: its own service account's token and CA, and the Roles' calls. */
export function kubeApi(o: {
  namespace: string
  hibernationAnnotation: string
  fencedInstancesAnnotation: string
  fenceAll: string
  idleSinceAnnotation: string
  suspendedByAnnotation: string
  host: string
  port: string
  tokenFile?: string
  caFile?: string
  fetchFn?: typeof fetch
}): Api {
  const dir = '/var/run/secrets/kubernetes.io/serviceaccount'
  const base = `https://${o.host}:${o.port}`
  const fetchFn = o.fetchFn ?? fetch
  // Read at each call: a service account's token is rotated while the pod runs.
  const call = async (path: string, init: { method: string; body?: string; contentType?: string }) => {
    const token = readFileSync(o.tokenFile ?? `${dir}/token`, 'utf8').trim()
    const ca = o.caFile === '' ? undefined : readFileSync(o.caFile ?? `${dir}/ca.crt`, 'utf8')
    return fetchFn(`${base}${path}`, {
      method: init.method,
      body: init.body,
      headers: { authorization: `Bearer ${token}`, ...(init.contentType ? { 'content-type': init.contentType } : {}) },
      signal: AbortSignal.timeout(10_000),
      ...(ca ? { tls: { ca } } : {}),
    } as RequestInit)
  }
  const cluster = (namespace: string, name: string) => `/apis/postgresql.cnpg.io/v1/namespaces/${encodeURIComponent(namespace)}/clusters/${encodeURIComponent(name)}`
  const schedules = (namespace: string) => `/apis/postgresql.cnpg.io/v1/namespaces/${encodeURIComponent(namespace)}/scheduledbackups`
  /** A merge patch of a Cluster's annotations, and nothing else (a null removes one): the only change the gateway ever makes to a Cluster is to these three (the hibernation mark, the fence, its own mark). */
  const patchCluster = async (namespace: string, name: string, annotations: Record<string, string | null>, resourceVersion?: string) => {
    const response = await call(cluster(namespace, name), {
      method: 'PATCH',
      contentType: 'application/merge-patch+json',
      // a merge patch that names a `resourceVersion` is applied only to that version (the API server answers 409 when the Cluster has changed since)
      body: JSON.stringify({ metadata: { ...(resourceVersion ? { resourceVersion } : {}), annotations } }),
    })
    if (!response.ok) throw new Error(`${response.status} ${await response.text()}`)
  }
  /** A merge patch of one ScheduledBackup's `spec.suspend` and the one annotation (a null removes it): the only two fields it ever changes. */
  const patchSchedule = async (namespace: string, name: string, suspend: boolean) => {
    const response = await call(`${schedules(namespace)}/${encodeURIComponent(name)}`, {
      method: 'PATCH',
      contentType: 'application/merge-patch+json',
      body: JSON.stringify({ metadata: { annotations: { [o.suspendedByAnnotation]: suspend ? 'pg-gateway' : null } }, spec: { suspend } }),
    })
    if (!response.ok) throw new Error(`${response.status} ${await response.text()}`)
  }
  return {
    async listServices() {
      const response = await call(`/api/v1/namespaces/${encodeURIComponent(o.namespace)}/services`, { method: 'GET' })
      if (!response.ok) throw new Error(`${response.status} ${await response.text()}`)
      return ((await response.json()) as { items: ServiceRow[] }).items
    },
    async getCluster(namespace, name) {
      const response = await call(cluster(namespace, name), { method: 'GET' })
      if (response.status === 404) return null
      if (!response.ok) throw new Error(`${response.status} ${await response.text()}`)
      return (await response.json()) as ClusterRow
    },
    async getPod(namespace, name) {
      const response = await call(`/api/v1/namespaces/${encodeURIComponent(namespace)}/pods/${encodeURIComponent(name)}`, { method: 'GET' })
      if (response.status === 404) return null
      if (!response.ok) throw new Error(`${response.status} ${await response.text()}`)
      return (await response.json()) as PodRow
    },
    async listSchedules(namespace, name) {
      const response = await call(schedules(namespace), { method: 'GET' })
      if (!response.ok) throw new Error(`${response.status} ${await response.text()}`)
      return ((await response.json()) as { items: ScheduleRow[] }).items.filter((s) => s.spec?.cluster?.name === name)
    },
    suspendSchedule: (namespace, name) => patchSchedule(namespace, name, true),
    resumeSchedule: (namespace, name) => patchSchedule(namespace, name, false),
    async listClusters(namespace) {
      const response = await call(`/apis/postgresql.cnpg.io/v1/namespaces/${encodeURIComponent(namespace)}/clusters`, { method: 'GET' })
      if (!response.ok) throw new Error(`${response.status} ${await response.text()}`)
      return ((await response.json()) as { items: ClusterRow[] }).items
    },
    async listBackups(namespace) {
      const response = await call(`/apis/postgresql.cnpg.io/v1/namespaces/${encodeURIComponent(namespace)}/backups`, { method: 'GET' })
      if (!response.ok) throw new Error(`${response.status} ${await response.text()}`)
      return ((await response.json()) as { items: BackupRow[] }).items
    },
    // light: the fence and the gateway's mark off; full: the hibernation mark off and any fence and mark left from the way down, in the one patch
    wake: (namespace, name, stage) =>
      patchCluster(namespace, name, {
        ...(stage === 'full' ? { [o.hibernationAnnotation]: 'off' } : {}),
        [o.fencedInstancesAnnotation]: null,
        [o.idleSinceAnnotation]: null,
      }),
    fence: (namespace, name, idleSince, version) => patchCluster(namespace, name, { [o.fencedInstancesAnnotation]: o.fenceAll, [o.idleSinceAnnotation]: idleSince }, version),
    hibernate: (namespace, name, version) => patchCluster(namespace, name, { [o.hibernationAnnotation]: 'on' }, version),
    unfence: (namespace, name, version) => patchCluster(namespace, name, { [o.fencedInstancesAnnotation]: null, [o.idleSinceAnnotation]: null }, version),
  }
}

