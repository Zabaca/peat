import { X509Certificate } from 'node:crypto'
// The safekeeper's pod, in one place. Two readers use it: the operator, which makes the safekeepers a platform starts with, and the
// control layer (control.ts), which makes a new safekeeper when one is lost and so needs the same pod. One definition, two users: a change
// to the safekeeper's command line is made here and reaches both. The rest of this file is the control layer's decisions, kept pure so a
// test can judge them.
//
// What it makes: a StatefulSet of one pod `safekeeper-w<id>`, PINNED to its machine (the id and the machine's data are tied together: a pod
// that finds another id's marker in its directory refuses to start, and one that finds NO marker for an id the storage controller already
// knows refuses too, asking the control layer, `GET /sk/<id>/may-start-empty`: the empty-disk guard). Its data is a directory `sk-<id>` of
// the machine's safekeeper volume: the hostPath is the volume's root and its type is `Directory` by default, never made, so that a volume
// that did not provision fails the pod instead of filling the system disk (an install that keeps its data on the system disk asks for
// `DirectoryOrCreate`); an init container, as root, makes and owns `sk-<id>` and the main container mounts only that directory. Several
// safekeepers (a replacement beside one that is there) therefore share a machine's one volume, each in a directory of its own.
//
// Settings: the safekeepers' partial-backup timeout and eviction residence are Neon's defaults (15 minutes each); offload with local
// deletion is on; upload concurrency is set low (`--wal-backup-parallel-jobs`) so that the databases keep their share of a slow link to the
// store; the object store's request timeout is 20 minutes (a 16 MiB segment at 1 to 3 MB/s is a minute, a stalled request must not hold a
// worker for Neon's default of two).

/** What a safekeeper pod asks for and may use: the platform's three at the start and every replacement the control layer makes. */
export const SAFEKEEPER_RESOURCES = { requests: { cpu: '100m', memory: '128Mi' }, limits: { memory: '512Mi' } }

export interface SafekeeperOptions {
  id: number
  /** The machine's name: the StatefulSet is pinned to it by `kubernetes.io/hostname`. */
  node: string
  image: string
  namespace: string
  /** The control layer's URL, for the empty-disk guard. */
  controlUrl: string
  /** The object store: its endpoint, bucket, region and the folder of the bucket the safekeepers write under. */
  remote: { endpoint: string; bucket: string; region: string; prefix: string }
  /** The Secret that holds the store's key for the safekeepers (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` entries), the Secret of Neon's public key and the one of the safekeepers' own token. */
  secrets: { store: string; auth: string; tokens: string }
  /** The nodeSelector and tolerations every platform pod carries (`placement` in names.ts). The pin to the machine is added to the selector here. */
  placement: { nodeSelector: Record<string, string>; tolerations: object[] }
  /** The hostPath that is the machine's safekeeper volume (its root). */
  volumeRoot: string
  /** Whether that root must exist (`Directory`, the default) or is made (`DirectoryOrCreate`). */
  /** Make the volume's root when it is missing (`hostDir`). */
  volumeCreate?: boolean
  walBackupParallelJobs: number
  resources: Record<string, unknown>
  /** Labels the object carries beyond its own (the control layer marks the ones it makes). */
  labels?: Record<string, string>
}

/**
 * A piece's host directory as a volume. It is mounted as itself, never through `subPath`, and each piece addresses its own folder inside
 * it: where the kubelet runs in a container of its own (Talos), it resolves a subPath in its own view of the machine, which is not the
 * one the container runtime mounts. A directory made when missing is mounted unchecked (type ''), so the runtime makes it on the machine
 * itself: DirectoryOrCreate makes it in the kubelet's view, which on Talos is read-only under /var/mnt and another folder under /var/lib.
 * One that must exist is checked (Directory), as before.
 */
export const hostDir = (path: string, create = false) => ({ path, type: create ? '' : 'Directory' })

/** The command the container runs: the guard, then the safekeeper. */
export function safekeeperScript(o: Pick<SafekeeperOptions, 'id' | 'namespace' | 'controlUrl' | 'remote' | 'walBackupParallelJobs'>): string {
  return `set -eu
id=${o.id}
d=/vol/sk-$id
mkdir -p $d
if [ ! -s $d/identity ]; then
  i=0
  while :; do
    a=$(curl -sS --max-time 3 "${o.controlUrl}/sk/$id/may-start-empty" 2>/dev/null || true)
    case "$a" in
      *'"allowed":true'*) break ;;
      *'"allowed":false'*) echo "refusing to start: the data directory is empty and safekeeper $id is a registered member; it is treated as lost and replaced by a new one" >&2; sleep 20; exit 1 ;;
    esac
    i=$((i + 1)); [ "$i" -le 12 ] || { echo "refusing to start: the data directory is empty and the control layer did not answer" >&2; sleep 20; exit 1; }
    sleep 5
  done
fi
if [ -f $d/identity ] && [ "$(cat $d/identity)" != "$id" ]; then
  echo "refusing to start: this directory belongs to safekeeper $(cat $d/identity), not $id" >&2
  exit 1
fi
echo "$id" > $d/identity
exec /usr/local/bin/safekeeper -D $d --id "$id" \\
  --listen-pg 0.0.0.0:5454 --listen-http 0.0.0.0:7676 \\
  --advertise-pg "safekeeper-w\${id}-0.safekeeper.${o.namespace}.svc.cluster.local:5454" \\
  --availability-zone "sk-\${id}" \\
  --broker-endpoint http://storage-broker:50051 \\
  --pg-auth-public-key-path /auth/auth_public_key.pem --pg-tenant-only-auth-public-key-path /auth/auth_public_key.pem --http-auth-public-key-path /auth/auth_public_key.pem \\
  --auth-token-path /tok/safekeeper_peer.jwt \\
  --enable-offload --delete-offloaded-wal --wal-backup-parallel-jobs ${o.walBackupParallelJobs} \\
  --remote-storage "{endpoint='${o.remote.endpoint}', bucket_name='${o.remote.bucket}', bucket_region='${o.remote.region}', prefix_in_bucket='${o.remote.prefix}', timeout='20m'}"
`
}

export function safekeeperStatefulSet(o: SafekeeperOptions): Record<string, any> {
  const labels = { app: 'safekeeper', 'node-id': String(o.id) }
  const dir = `sk-${o.id}`
  const volume = { name: 'volume', hostPath: hostDir(o.volumeRoot, o.volumeCreate) }
  return {
    apiVersion: 'apps/v1',
    kind: 'StatefulSet',
    metadata: { name: `safekeeper-w${o.id}`, namespace: o.namespace, ...(o.labels ? { labels: o.labels } : {}) },
    spec: {
      serviceName: 'safekeeper',
      replicas: 1,
      selector: { matchLabels: labels },
      template: {
        metadata: { labels },
        spec: {
          // no API credential and no service links: the pod calls nothing of Kubernetes (the namespace's default account has no rights, but a token is one more thing to read)
          automountServiceAccountToken: false,
          enableServiceLinks: false,
          nodeSelector: { ...o.placement.nodeSelector, 'kubernetes.io/hostname': o.node },
          tolerations: o.placement.tolerations,
          initContainers: [
            {
              name: 'prep',
              image: o.image,
              securityContext: { runAsUser: 0 },
              // the directory of this safekeeper in the machine's volume: made and owned for uid 1000 (the safekeeper's), and only its own (never the volume's other directories)
              command: ['/bin/sh', '-c', `set -eu; chown 1000:1000 /vol; mkdir -p /vol/${dir}; chown 1000:1000 /vol/${dir}`],
              volumeMounts: [{ name: 'volume', mountPath: '/vol' }],
            },
          ],
          containers: [
            {
              name: 'safekeeper',
              image: o.image,
              env: [
                { name: 'AWS_ACCESS_KEY_ID', valueFrom: { secretKeyRef: { name: o.secrets.store, key: 'AWS_ACCESS_KEY_ID' } } },
                { name: 'AWS_SECRET_ACCESS_KEY', valueFrom: { secretKeyRef: { name: o.secrets.store, key: 'AWS_SECRET_ACCESS_KEY' } } },
              ],
              command: ['/bin/sh', '-c'],
              args: [safekeeperScript(o)],
              ports: [{ containerPort: 5454 }, { containerPort: 7676 }],
              resources: o.resources,
              readinessProbe: { httpGet: { path: '/v1/status', port: 7676 }, periodSeconds: 5 },
              // its folder gone under it (a volume wiped): the kubelet restarts it, and its start finds the folder empty, which for a registered
              // member is refused (the control layer's may-start-empty): it is treated as lost and replaced, never back as a healthy member
              livenessProbe: { exec: { command: ['/bin/sh', '-c', `test -d /vol/${dir}`] }, periodSeconds: 10, failureThreshold: 2 },
              volumeMounts: [
                { name: 'volume', mountPath: '/vol' },
                { name: 'auth', mountPath: '/auth' },
                { name: 'tok', mountPath: '/tok' },
              ],
            },
          ],
          volumes: [volume, { name: 'auth', secret: { secretName: o.secrets.auth } }, { name: 'tok', secret: { secretName: o.secrets.tokens } }],
        },
      },
    },
  }
}

// ---- the control layer's decisions about the safekeepers, pure so that a test can judge them (the control layer runs from a ConfigMap and cannot be imported)

export interface SkRecord { id: number; scheduling_policy: string }
export interface SkPodRecord { nodeId: string; node: string }
export interface MachineRecord { name: string; dead: boolean }
/**
 * The machines a replacement of `lost` may go to: alive platform machines that hold no OTHER member that is not decommissioned (no timeline's set may have two members on one machine: its loss would take two of three). `pods` are the
 * safekeepers' pods with the machine each is on (or pinned to while it waits). The lost one's own machine comes first when it is alive and qualifies (a lost disk: the machine is fine).
 */
export function replacementMachines(lost: number, safekeepers: SkRecord[], pods: SkPodRecord[], machines: MachineRecord[]): string[] {
  // every listed member that is not decommissioned is still in its timelines' sets (a Paused one too), so it occupies its machine
  const others = new Set(safekeepers.filter((x) => !/^decom/i.test(String(x.scheduling_policy)) && x.id !== lost).map((x) => String(x.id)))
  const busy = new Set(pods.filter((p) => p.node && others.has(p.nodeId)).map((p) => p.node))
  const lostNode = pods.find((p) => p.nodeId === String(lost))?.node ?? ''
  return machines.filter((m) => !m.dead && !busy.has(m.name)).map((m) => m.name).sort((a, b) => (a === lostNode ? -1 : 0) - (b === lostNode ? -1 : 0))
}
/**
 * The machines a replacement chooses from: the platform's, plus the lost member's OWN machine when that is a keeper machine and it is ready. A replacement never goes TO a keeper machine from another, but
 * the keeper's own member comes back to it: otherwise the first replacement of the keeper's member puts it on the platform's machines and nothing is on a keeper machine any more (2026-11-20: member 10 became member 11 on a platform machine). `keeperReady` are the
 * keeper machines that carry the label, are Ready and have their directory and image (`keeperProblem` in control.ts); the lost member's machine is then the first candidate (`replacementMachines` sorts it first), and still
 * only when no other member is on it. A keeper machine that is not ready is a dead machine here: the replacement goes to the platform's machines, as it always did.
 */
export function replacementPool(platform: MachineRecord[], lostNode: string, keeperMachines: readonly string[], keeperReady: readonly string[], keeperTried = false): MachineRecord[] {
  // the keeper machine is tried ONCE per loss (`keeperTried`: an earlier replacement of this member failed on it, `markKeeperTried` in control.ts): a machine that Kubernetes calls Ready and cannot run or reach a member would
  // otherwise be chosen every cycle for ever, leaving the set at two members and refusing new databases for the minutes each attempt takes
  if (keeperTried || !lostNode || !keeperMachines.includes(lostNode) || platform.some((m) => m.name === lostNode)) return platform
  return [...platform, { name: lostNode, dead: !keeperReady.includes(lostNode) }]
}
/**
 * The group a seeding makes (after a total loss, or at the platform's first start): the configured members, one per platform machine, except that when a keeper machine is ready its member replaces the LAST configured one
 * (the configured order puts the controller's machine last: two members on the platform's machines where the pageservers are, one on the keeper machine). With no keeper machine ready, or none configured, the group is the configured one.
 */
export function seedGroup(configured: { id: number; node: string }[], keeperMachines: readonly string[], keeperReady: readonly string[]): { id: number; node: string }[] {
  const keeper = keeperMachines.find((m) => keeperReady.includes(m))
  if (!keeper || configured.length < 2 || configured.some((x) => x.node === keeper)) return configured
  return configured.map((x, i) => (i === configured.length - 1 ? { ...x, node: keeper } : x))
}
/**
 * Whether a timeline has never been written: a safekeeper's status for it where the flushed position is still the timeline's start. It has no WAL file on any member (a recovery makes the timeline at a position inside a segment and
 * the compute's first start is what writes the first record), and the storage controller's `pull_timeline`, which a migration to a new member runs, streams the segment files and breaks off in the middle of the body ("unexpected EOF
 * during chunk size line", seen 2026-11-20 on 10 of 40 timelines of the recovered databases). Such a timeline is made writable by starting its database once; the migration is then tried again.
 */
export function neverWritten(statuses: ({ flush_lsn?: string; timeline_start_lsn?: string } | null | undefined)[]): boolean {
  // the HIGHEST flush position over every member that answered: a member that has not caught up (a keeper machine's after an outage, the third of a set made on a quorum of two) can still show the start for a timeline that was written
  const read = statuses.filter((x): x is { flush_lsn?: string; timeline_start_lsn?: string } => Boolean(x))
  if (read.length === 0) return false
  const starts = read.map((x) => lsnValue(x.timeline_start_lsn)), flushes = read.map((x) => lsnValue(x.flush_lsn))
  if (starts.some((v) => v === null) || flushes.some((v) => v === null)) return false
  const start = starts.reduce((a, b) => (a! > b! ? a : b))!
  const highest = flushes.reduce((a, b) => (a! > b! ? a : b))!
  return highest <= start
}
/** Whether a failed migration's answer is the `pull_timeline` break (the storage controller says which call failed on the new member). */
export function isPullTimelineFailure(message: string): boolean {
  return /pull_timeline/.test(message)
}

/** A toleration as the keeper's pod carries it (`KEEPER_PLACEMENT` in control.ts). */
export interface PodToleration { key?: string; operator?: string; value?: string; effect?: string }
/**
 * Why a keeper machine's node cannot take a new member now, or null: it must carry the keeper label and be Ready, and it must not be cordoned (`spec.unschedulable`), carry a NoSchedule or NoExecute taint the keeper's
 * pod does not tolerate (the kubelet adds `node.kubernetes.io/disk-pressure` and `memory-pressure` on its own, and a drain cordons), or report Disk, Memory or PID pressure (a keeper machine's data may be on its system disk, which
 * the kubelet and containerd share). A Ready node can be all of these: the pod would stay Pending, or be evicted.
 */
export function keeperNodeProblem(node: any, label: { key: string; value: string }, tolerations: readonly PodToleration[]): string | null {
  const name = String(node?.metadata?.name ?? 'the keeper machine')
  const conds = (node?.status?.conditions ?? []) as { type: string; status: string }[]
  if (node?.metadata?.labels?.[label.key] !== label.value || !conds.some((c) => c.type === 'Ready' && c.status === 'True')) return `${name} is not a Ready node carrying ${label.key}=${label.value}`
  if (node?.spec?.unschedulable) return `${name} is cordoned (unschedulable): a new member would stay Pending`
  for (const t of (node?.spec?.taints ?? []) as { key: string; value?: string; effect: string }[]) {
    if (t.effect === 'PreferNoSchedule') continue
    const tolerated = tolerations.some((p) => p.key === t.key && (p.operator === 'Exists' || (p.value ?? '') === (t.value ?? '')) && (!p.effect || p.effect === t.effect))
    if (!tolerated) return `${name} carries the taint ${t.key}=${t.value ?? ''}:${t.effect}, which the keeper's pod does not tolerate`
  }
  for (const p of ['DiskPressure', 'MemoryPressure', 'PIDPressure']) if (conds.some((c) => c.type === p && c.status === 'True')) return `${name} reports ${p}`
  return null
}
/**
 * What a replacement that ended with timelines still on the lost member does about the keeper machine it chose: 'abandon' when it was chosen for a loss (not an operator's move), the job's migrations failed and no timeline refers
 * to the new member and none was moved or left unread (so the keeper machine took the member but could not be reached from the platform's machines, or could not run it), which sends the next attempt to the platform's machines; 'keep' otherwise (progress on the new member shows the machine works, an
 * operator chose it, or it is a platform machine).
 */
export function keeperReplacementVerdict(o: { onKeeper: boolean; operatorMove: boolean; failed: number; referencingNew: number; unknown: number; done: number }): 'abandon' | 'keep' {
  // the new member is deleted only when nothing can be on it: no migration of this job succeeded (`done`), no timeline refers to it, and every timeline was answered about (`unknown`: a final read that came back unknown may hide one)
  return o.onKeeper && !o.operatorMove && o.failed > 0 && o.referencingNew === 0 && o.unknown === 0 && o.done === 0 ? 'abandon' : 'keep'
}

/** What to do about a safekeeper that does not answer: nothing yet (gone for less than the bound: a machine that comes back gets its own safekeeper back), replace (past it, with a machine to go to) or wait (past it, none). */
export function replaceDecision(goneSeconds: number, boundSeconds: number, candidates: string[]): 'nothing' | 'replace' | 'wait' {
  if (goneSeconds <= boundSeconds) return 'nothing'
  return candidates.length ? 'replace' : 'wait'
}
/** A safekeeper as the storage controller lists it. */
export interface ListedSk { id: number; scheduling_policy: string }
const decommissioned = (x: ListedSk) => /^decom/i.test(String(x.scheduling_policy))
/** Whether safekeepers are made from nothing: when a safekeeper StatefulSet exists nothing is (it is not the control layer's to recreate what a person removed); when the storage controller still lists a safekeeper that is not decommissioned and no StatefulSet exists it is refused
 * (a person removed them: new members would be empty disks under the old members' names). When every listed one is decommissioned (a total loss, every one replaced away) or none is listed, the set is made again, under ids no one has used. */
export function seedDecision(statefulsetNames: string[], listed: ListedSk[]): 'seed' | 'exists' | 'refuse' {
  if (statefulsetNames.some((n) => n.startsWith('safekeeper-w'))) return 'exists'
  return listed.some((x) => !decommissioned(x)) ? 'refuse' : 'seed'
}
/** The ids a seeding uses: the configured ones when the controller lists nothing, else the next ones after every id it has ever listed (a decommissioned id is never reused). */
export function seedIds(configured: number[], listed: ListedSk[]): number[] {
  if (listed.length === 0) return configured
  const top = Math.max(...listed.map((x) => x.id))
  return configured.map((_, i) => top + 1 + i)
}

/** What one timeline's members said when asked about it: nobody answered (unknown), or some member answered and the timeline does or does not still list the lost member (or a pending change of members). */
export type TimelineRead = { answered: false } | { answered: true; onLost: boolean }
/**
 * Whether a lost safekeeper may be decommissioned (and its StatefulSet and marks removed): only when EVERY primary timeline was answered about and none still lists it. A timeline nobody answered about is unknown, never
 * "not on the lost one": when the other members blip at the closing count, or every member lost its disk, nothing is known, and a decommission then leaves sets without their replacement (found in review, 2026-11-20).
 */
export function closeDecision(reads: TimelineRead[]): { todo: number; unknown: number; close: boolean } {
  const todo = reads.filter((r) => r.answered && r.onLost).length
  const unknown = reads.filter((r) => !r.answered).length
  return { todo, unknown, close: todo === 0 && unknown === 0 }
}
/** A seeded set is recorded on each of its StatefulSets as `7=machine-1,8=machine-2,9=machine-3` (the annotation below), so that a seeding that stopped half way is finished rather than mistaken for a platform that has a safekeeper. */
export const SEED_GROUP_ANNOTATION = 'peat.zabaca.com/seed-group'
export interface SeedMember { id: number; node: string }
export const formatSeedGroup = (g: SeedMember[]): string => g.map((x) => `${x.id}=${x.node}`).join(',')
export function parseSeedGroup(v: string | undefined): SeedMember[] {
  return (v ?? '').split(',').filter(Boolean).map((x) => { const [id, node] = x.split('='); return { id: Number(id), node: node ?? '' } }).filter((x) => Number.isInteger(x.id) && x.id > 0 && x.node !== '')
}
/** The members of a seeded group that were never made: no StatefulSet and never listed by the storage controller (a member that was replaced away is listed, decommissioned, and is not made again). */
export function seedCompletion(group: SeedMember[], statefulsetNames: string[], listedIds: number[]): SeedMember[] {
  return group.filter((m) => !statefulsetNames.includes(`safekeeper-w${m.id}`) && !listedIds.includes(m.id))
}

/** What an operator's decommission asks about each OTHER member when nothing answers: a lost-disk mark (its pod refused to start empty), whether its pod is Running on a Ready node, and whether it was still silent after a minute. */
export interface MemberEvidence { lostMark: boolean; podRunningOnReadyNode: boolean; silentAfterWait: boolean }
/** Silence is not evidence: a box reboot, or a blip on the control pod, silences every member. A total loss is shown only when each other member carries a lost-disk mark, or its pod is Running on a Ready node and is still silent after a minute. */
export function totalLossEvidence(others: MemberEvidence[]): boolean {
  return others.every((m) => m.lostMark || (m.podRunningOnReadyNode && m.silentAfterWait))
}
/**
 * Whether an operator may decommission a silent safekeeper (`POST /sk/<id>/decommission`): refused when it answers (even forced); allowed in a total loss that is EVIDENCED (`totalLossEvidence`: no other member answers and each is shown
 * lost), or when the closing count for it says close (every timeline answered about, none lists it); otherwise refused with the counts, unless forced (which reports how many timelines still list it). A member whose machine is
 * rebooting looks lost for a minute; retiring it then would leave every timeline on two members of three with nothing to replace it.
 */
export function decommissionDecision(o: { answers: boolean; othersAnswering: number; lossEvidenced: boolean; count: { todo: number; unknown: number; close: boolean } | null; force: boolean }): { allow: boolean; why: string } {
  if (o.answers) return { allow: false, why: 'it answers' }
  if (o.othersAnswering === 0 && o.lossEvidenced) return { allow: true, why: 'a total loss: no other member answers and each is shown lost' }
  if (o.count?.close) return { allow: true, why: 'every timeline was answered about and none lists it' }
  const c = o.othersAnswering === 0 ? 'nobody answers, which a reboot or a blip also causes, and the other members are not shown lost' : o.count ? `${o.count.todo} timelines still list it and ${o.count.unknown} were not answered about` : 'the timelines could not be read'
  if (o.force) return { allow: true, why: `forced; ${c}` }
  return { allow: false, why: `${c} (force=1 retires it anyway)` }
}
/** Whether a member that answers again should have its replacement finished: it has a replacement mark (a replacement that ended with failures or unknowns), no job runs, and it was last tried over the bound ago. */
export function reopenDecision(o: { answers: boolean; hasReplacementMark: boolean; jobRunning: boolean; sinceLastTryS: number; everyS: number }): 'rerun' | 'nothing' {
  return o.answers && o.hasReplacementMark && !o.jobRunning && o.sinceLastTryS >= o.everyS ? 'rerun' : 'nothing'
}

// ---- the keeper: one safekeeper on a machine outside the platform's (KEEPER_MACHINES in control.ts)

export interface Placement { nodeSelector: Record<string, string>; tolerations: object[] }
/** The placement a safekeeper on `node` carries: the keeper's (its label, that machine's taints tolerated) on a keeper machine, the platform's everywhere else. */
export function placementFor(node: string, o: { keeperMachines: readonly string[]; platform: Placement; keeper: Placement }): Placement {
  return o.keeperMachines.includes(node) ? o.keeper : o.platform
}
/** A listed safekeeper with the machine its pod is on (null when it has no pod). */
export interface MemberAt { id: number; scheduling_policy: string; machine: string | null; answers: boolean }
/**
 * Whether an operator may MOVE a live member to another machine (`POST /sk/<id>/move?to=<machine>`): the one way a member is put on a keeper machine, by migrating it, never by wiping. Refused unless: the member is
 * Active and answers; every other member that is not decommissioned answers too and there are at least two (the set is never below three healthy while it moves: the migration is a joint configuration, and a
 * set that is already short stays short); the target is a keeper machine or a platform machine, is not where the member is, and holds no other member; no replacement is open; and, when the target is a keeper
 * machine, no other member is already on a keeper machine (two on the platform's machines, one off them).
 */
export function moveDecision(o: { id: number; target: string; members: MemberAt[]; keeperMachines: readonly string[]; platformMachines: readonly string[]; replacementOpen: string | null }): { allow: boolean; why: string } {
  const live = o.members.filter((m) => !/^decom/i.test(String(m.scheduling_policy)))
  const me = live.find((m) => m.id === o.id)
  if (!me) return { allow: false, why: `no safekeeper ${o.id} that is not decommissioned` }
  if (me.scheduling_policy !== 'Active') return { allow: false, why: `safekeeper ${o.id} is ${me.scheduling_policy}, not Active` }
  if (!me.answers) return { allow: false, why: `safekeeper ${o.id} does not answer: a member that is gone is replaced, not moved` }
  const others = live.filter((m) => m.id !== o.id)
  if (others.length < 2) return { allow: false, why: 'fewer than two other members: the set is already short' }
  const silent = others.filter((m) => !m.answers).map((m) => m.id)
  if (silent.length) return { allow: false, why: `safekeeper ${silent.join(', ')} does not answer: the set would be below three healthy while a member moves` }
  if (o.replacementOpen) return { allow: false, why: `a replacement is open: ${o.replacementOpen}` }
  const isKeeper = o.keeperMachines.includes(o.target)
  if (!isKeeper && !o.platformMachines.includes(o.target)) return { allow: false, why: `${o.target} is neither a keeper machine nor a platform machine` }
  if (me.machine === o.target) return { allow: false, why: `safekeeper ${o.id} is already on ${o.target}` }
  const holder = others.find((m) => m.machine === o.target)
  if (holder) return { allow: false, why: `${o.target} already holds safekeeper ${holder.id}: no two members on one machine` }
  if (isKeeper && others.some((m) => m.machine !== null && o.keeperMachines.includes(m.machine))) return { allow: false, why: 'a member is already on a keeper machine: two on the platform\'s machines, one off them' }
  return { allow: true, why: `safekeeper ${o.id} moves to ${o.target}` }
}
/** How many members sit on keeper machines (off the platform's machines): the gauge `peat_control_sk_members_off_box`. */
export function membersOffBox(members: MemberAt[], keeperMachines: readonly string[]): number {
  return members.filter((m) => !/^decom/i.test(String(m.scheduling_policy)) && m.machine !== null && keeperMachines.includes(m.machine)).length
}

/** An open replacement mark (`sk-replacement-<id>`, label `peat-sk-open`): the member it is for, when it was made (ms), and the machine a move was asked to put its replacement on (empty for a replacement of a lost member). */
export interface ReplacementMark { id: number; at: number; moveTo: string }
/**
 * The member whose replacement is open and must finish before `forId` may start or rerun one, or null. A job (a replacement or a move) starts only when no OTHER member's replacement is open anywhere in the cluster, not only
 * on this pod: `skJob` is in memory, one per control layer pod, and two jobs that each computed the next id from a list read before the other registered would share one new member and could close one timeline on two
 * members (found in review). A member that already has its own mark is a RERUN: only an OLDER other mark blocks it (the oldest proceeds; the lowest id on a tie), so two leftover marks cannot block each other for ever.
 */
export function otherReplacementOpen(marks: ReplacementMark[], forId: number): number | null {
  const mine = marks.find((m) => m.id === forId)
  const blockers = marks.filter((m) => m.id !== forId && (!mine || m.at < mine.at || (m.at === mine.at && m.id < forId)))
  return blockers.length ? blockers[0]!.id : null
}

// ---- a database whose safekeepers no longer know its timeline: recovered under its own name (`POST /db/<name>/recover`, an operator route)
// Two routes were compared: (a) a new timeline made as a branch of the old one at the
// pageserver's last UPLOADED record, the database's record repointed at it (same name, port, certificate, login), the old timeline kept as its ancestor; (b) the same timeline made again on the new safekeepers. (a) is built:
// it uses only the calls a branch already makes. (b) would hand-write the storage controller's own rows and the safekeepers' membership, and a safekeeper restarted at the pageserver's in-memory position rather than its
// uploaded one would leave the two with different histories after a pageserver restart. These functions are the decisions; the calls are `recoverDatabase` in control.ts.

/** What one member said when asked about the timeline: it knows it (200), it answers and does not (404), or it did not answer (anything else). */
export type TimelineProbe = 'knows' | 'unknown' | 'silent'
export interface MemberProbe { id: number; active: boolean; probe: TimelineProbe }
/** The storage controller picks a new timeline's members from the Active ones, three of distinct machines: fewer than this many that answer and are Active and a branch cannot be made. */
export const RECOVER_MIN_MEMBERS = 3
/**
 * Whether a database may be recovered: only when it is a primary, every member that is not decommissioned answered and none knows its timeline (a member that knows it means the database can start as it is, and a member that
 * is silent may hold it), at least three Active members answered, and no replacement is open (the storage controller could choose a dead Active member for the new timeline). It refuses when it is not needed.
 */
export function recoverDecision(o: { mode: string; probes: MemberProbe[]; replacementOpen: string | null }): { allow: boolean; why: string } {
  if (o.mode !== 'primary') return { allow: false, why: 'it is a replica: recover its primary, and the replica follows' }
  const knows = o.probes.filter((p) => p.probe === 'knows').map((p) => p.id)
  if (knows.length) return { allow: false, why: `safekeeper ${knows.join(', ')} still knows its timeline: nothing to recover (a start of the database retries on its own)` }
  const silent = o.probes.filter((p) => p.probe === 'silent').map((p) => p.id)
  if (silent.length) return { allow: false, why: `safekeeper ${silent.join(', ')} does not answer: it may hold the timeline, so it is not known to be lost` }
  const active = o.probes.filter((p) => p.active).length
  if (active < RECOVER_MIN_MEMBERS) return { allow: false, why: `${active} Active safekeepers answer and a new timeline needs ${RECOVER_MIN_MEMBERS} (a total loss is followed by the control layer's seeding: wait for it)` }
  if (o.replacementOpen) return { allow: false, why: `a safekeeper replacement is open: ${o.replacementOpen}` }
  return { allow: true, why: 'no safekeeper knows its timeline and three Active ones answer' }
}

const LSN_FORMAT = /^[0-9A-Fa-f]+\/[0-9A-Fa-f]+$/
/** A log position as the pageserver prints it (`16/B374D848`) as a number, or null when it is not one. */
export function lsnValue(t: string | null | undefined): bigint | null {
  if (typeof t !== 'string' || !LSN_FORMAT.test(t)) return null
  const [hi, lo] = t.split('/')
  return (BigInt('0x' + hi!) << 32n) | BigInt('0x' + lo!)
}
export interface PageserverTimeline { remote_consistent_lsn?: string; remote_consistent_lsn_visible?: string; last_record_lsn?: string; ancestor_lsn?: string | null }
/**
 * Where the new timeline branches off the old: the pageserver's last UPLOADED record (`remote_consistent_lsn_visible`, which has no gap on an idle timeline; else `remote_consistent_lsn`), never `last_record_lsn`: what the
 * pageserver holds only in memory is lost if it restarts, and the safekeepers that would replay it are gone. `unuploadedBytes` is how far the pageserver's last record is ahead of it (what a wait for the pageserver's own
 * flush, up to the checkpoint timeout, would still save); the position must not be behind the timeline's own branch point (the pageserver refuses a branch there).
 */
export function recoveryPoint(info: PageserverTimeline): { ok: true; lsn: string; unuploadedBytes: number } | { ok: false; why: string } {
  const usable = [info.remote_consistent_lsn_visible, info.remote_consistent_lsn].find((x) => { const v = lsnValue(x); return v !== null && v > 0n })
  if (!usable) return { ok: false, why: 'the pageserver reports no uploaded position for the timeline (remote_consistent_lsn is 0/0 or missing)' }
  const at = lsnValue(usable)!
  const branch = lsnValue(info.ancestor_lsn)
  if (branch !== null && at < branch) return { ok: false, why: `the uploaded position ${usable} is behind the timeline's branch point ${info.ancestor_lsn}` }
  const last = lsnValue(info.last_record_lsn)
  return { ok: true, lsn: usable, unuploadedBytes: last !== null && last > at ? Number(last - at) : 0 }
}

/** A database's record as far as the recovery reads it. */
export interface RecoveryRecord { name: string; tenant: string; timeline: string; mode: string }
/**
 * Which records the recovery repoints and which it leaves: the database and its replicas share one timeline and move to the new one (the primary first); every other database of the tenant (its child branches, and a
 * branch's siblings) has a timeline of its own, which keeps its data and its ancestry (the old timeline stays as an ancestor, never deleted) and needs its own call when its safekeeper timeline is gone too.
 */
export function recoveryScope(records: RecoveryRecord[], d: RecoveryRecord): { repoint: string[]; others: string[] } {
  const same = records.filter((r) => r.tenant === d.tenant && r.timeline === d.timeline)
  return {
    repoint: [...same.filter((r) => r.mode === 'primary'), ...same.filter((r) => r.mode !== 'primary')].map((r) => r.name),
    others: records.filter((r) => r.tenant === d.tenant && r.timeline !== d.timeline && r.mode === 'primary').map((r) => r.name).sort(),
  }
}

/**
 * What a call does about the new timeline, which a rerun after a stop half way must not make twice: `absent` makes it (at the point the mark recorded, or the one just read); `ours` (it exists on the pageserver as a branch of
 * the old timeline) is adopted and the records repointed; `foreign` (it exists as something else) refuses.
 */
export function recoverStep(newTimeline: 'absent' | 'ours' | 'foreign'): { do: 'create' | 'adopt' | 'refuse'; why?: string } {
  if (newTimeline === 'absent') return { do: 'create' }
  if (newTimeline === 'ours') return { do: 'adopt' }
  return { do: 'refuse', why: 'the timeline the earlier call recorded exists and is not a branch of the old one' }
}

// ---- the recovery's second round (review of the first): the timeline's own members, a stop half way, and a restore from before the recovery point

/** A recovery's mark (`recovered-<old timeline>`, label `peat-recovered`): the ConfigMap's name, the tenant, the old timeline, the new one, the position it branches at, and how far the call got. */
export interface RecoveryMark { name: string; tenant: string; from: string; to: string; lsn: string; state: string }
/**
 * The members a timeline names itself (the hosts in its compute spec's `safekeeper_connstrings`) that the storage controller's list of members that are not decommissioned does not contain: a member decommissioned while it
 * still held the timeline is among them, and must be asked too. `listedHosts` are the hosts already probed (with or without a port).
 */
export function timelineOwnMembers(connstrings: string[], listedHosts: string[]): { id: number; host: string }[] {
  const listed = new Set(listedHosts.map((h) => h.split(':')[0]))
  const out = new Map<number, string>()
  for (const c of connstrings) {
    const host = c.split(':')[0]!
    const id = /^safekeeper-w(\d+)-0\./.exec(host)?.[1]
    if (id && !listed.has(host)) out.set(Number(id), host)
  }
  return [...out].map(([id, host]) => ({ id, host })).sort((a, b) => a.id - b.id)
}
/**
 * What one of those members said. A member that answers decides (200 knows it, 404 does not); one that does not answer but whose pod exists is silent (it may hold the timeline); one whose pod is gone (the decommission
 * removes the StatefulSet) cannot be asked and is counted as not holding it: that is every old member after a total loss, and a person who retired a member whose disk still held the timeline has taken it out of reach of this question.
 */
export function ownMemberProbe(o: { status: number | null; podExists: boolean }): TimelineProbe {
  if (o.status !== null && o.status >= 200 && o.status < 300) return 'knows'
  if (o.status === 404) return 'unknown'
  return o.podExists ? 'silent' : 'unknown'
}
/**
 * A recovery a stop left half way: the database's record already holds a recovery's new timeline (`to`), and records of the same tenant (its replicas: the primary is repointed first) still hold the old one (`from`). It is finished
 * before anything else is decided, because the new timeline is known to the safekeepers and the decision would refuse it as "nothing to recover". Null when there is nothing left to move.
 */
export function unfinishedRepoint(marks: RecoveryMark[], records: RecoveryRecord[], d: RecoveryRecord): { mark: RecoveryMark; names: string[] } | null {
  for (const mark of marks.filter((m) => m.tenant === d.tenant && m.to === d.timeline)) {
    const names = records.filter((r) => r.tenant === mark.tenant && r.timeline === mark.from).map((r) => r.name)
    if (names.length) return { mark, names }
  }
  return null
}
/**
 * The timeline a branch or restore of `timeline` at `lsn` must be taken from: the timeline itself, or, when the position is before the point a recovery made it branch at (the pageserver refuses a branch below its
 * branch point), the old timeline that recovery kept, and so on back along the recoveries.
 */
export function ancestorFor(marks: RecoveryMark[], timeline: string, lsn: string): string {
  const at = lsnValue(lsn)
  let tl = timeline
  for (let hops = 0; at !== null && hops < 32; hops++) {
    const m = marks.find((x) => x.to === tl)
    const point = m ? lsnValue(m.lsn) : null
    if (!m || point === null || at >= point) break
    tl = m.from
  }
  return tl
}
/** When the pageserver says a moment is before a timeline's records (`past`, or `nodata` on one that has no commit yet), the timeline the recovery that made it kept is where to look; null when there is none. */
export function olderTimelineFor(marks: RecoveryMark[], timeline: string, kind: string): string | null {
  if (kind !== 'past' && kind !== 'nodata') return null
  return marks.find((m) => m.to === timeline)?.from ?? null
}

// ---- a running compute's certificate (2026-11-21: nothing reloaded it)
// cert-manager renews a database's certificate 30 days before it expires (`renewBefore` 720 h of `duration` 8760 h, control.ts `certificate`) and updates the Secret `tls-<name>`; the kubelet updates the file the compute mounts,
// and Postgres reads `ssl_cert_file` only at start or on a reload (SIGHUP). A compute that stays up past the renewal serves the old certificate until it expires: the adapter (which verifies, against the platform CA and the
// name `<db>.peat-db.svc`) and every client that verifies then refuse it. Every database sleeps after 30 idle minutes (a start reads the new file), so only a compute that never goes 30 minutes quiet is exposed.

/**
 * The serial (hex) and the issue time (`notBefore`, in ms) of the first certificate of a PEM, or null when it cannot be read. **The reload is decided on the certificate itself, never on when its Secret was written**: a
 * database's first pod is admitted before cert-manager writes the Secret, and a label or an annotation on the Secret moves its managed fields' times, and neither is a new certificate (the review of 2026-11-21).
 */
export function certInfo(pem: string): { serial: string; notBeforeMs: number } | null {
  try {
    const c = new X509Certificate(pem)
    const notBeforeMs = Date.parse(c.validFrom)
    return Number.isFinite(notBeforeMs) ? { serial: c.serialNumber, notBeforeMs } : null
  } catch {
    return null
  }
}
/**
 * Whether a compute's Postgres is to be asked to reload now: the certificate in its Secret was issued AFTER the compute's container started (`startedAtMs` is the container's `state.running.startedAt`, which comes after
 * the volume is mounted, so a container that started later read this certificate or a newer one), the mounted file has had `settleMs` since the issue to follow (the kubelet updates a mounted Secret within a minute or two,
 * and a reload before that would read the old file), and this certificate, by its serial, was not already reloaded for.
 */
export function certNeedsReload(o: { notBeforeMs: number; startedAtMs: number; nowMs: number; serial: string; reloadedFor: string | undefined; settleMs: number }): boolean {
  if (!(o.notBeforeMs > 0) || !(o.startedAtMs > 0) || !o.serial) return false
  if (o.notBeforeMs <= o.startedAtMs) return false
  if (o.nowMs - o.notBeforeMs < o.settleMs) return false
  return o.reloadedFor !== o.serial
}

// ---- the control layer's memory (2026-11-21: every control pod grew about 10 MiB an hour until the follower was OOM-killed at 256 MiB)
/**
 * Whether `host:port` accepts a TCP connection within `ms`: the probe of each machine's kubelet port (every `PROBE_EVERY_MS`, a few a second for the life of the pod) and of each compute's Postgres port.
 * **It uses Bun's own sockets, not `node:net`:** a loop of `net.connect` and `destroy` (the first form of this probe) took a Bun 1.3.14 process from 115 to 175 MiB in 200,000 connects, in steps and never back,
 * while `Bun.connect` plus `end` stayed at 77 MiB from 40,000 connects on (read 2026-11-21 against a local listener, the same Bun as the pod's image); the control layer's growth is the first, at its probe rate.
 * A connection that opens after the time ran out is ended at once, so a slow host is not left with sockets.
 */
export function tcpUp(host: string, port: number, ms: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    let done = false
    const finish = (up: boolean) => { if (done) return; done = true; clearTimeout(timer); resolve(up) }
    const timer = setTimeout(() => finish(false), ms)
    Bun.connect({
      hostname: host,
      port,
      socket: {
        open(socket) { finish(true); try { socket.end() } catch { /* already closed */ } },
        data() {},
        close() {},
        error() { finish(false) },
        connectError() { finish(false) },
      },
    }).catch(() => finish(false))
  })
}
/** The process's memory as gauges (bytes): resident set, JS heap in use and reserved, native memory outside the heap, and ArrayBuffers: the figures that say whether a growth is garbage, the heap or the native side. */
export function memoryGauges(m: { rss: number; heapUsed: number; heapTotal: number; external: number; arrayBuffers: number }): Record<string, number> {
  return { process_rss_bytes: m.rss, process_heap_used_bytes: m.heapUsed, process_heap_total_bytes: m.heapTotal, process_external_bytes: m.external, process_array_buffers_bytes: m.arrayBuffers }
}


/**
 * Whether a compute's pod has a machine yet (2026-11-21, follow-ups "Tenants wait without saying why when the machines are full"): about five computes fit at once, and a sixth sits Pending with
 * `PodScheduled=False, reason Unschedulable, message "0/3 nodes are available: 3 Insufficient memory..."` while its client's connection runs out its 120 s. `placed` once the pod has a machine, `refused` with
 * the scheduler's own words once it says it cannot be placed, `waiting` while there is no pod yet or the scheduler has not looked (a few hundred milliseconds). A pod that is on its way out is `waiting`, not placed.
 */
export type PlacementVerdict = { kind: 'placed' } | { kind: 'refused'; reason: string } | { kind: 'waiting' }
export function placementVerdict(pod: any | null | undefined): PlacementVerdict {
  if (!pod || pod.metadata?.deletionTimestamp) return { kind: 'waiting' }
  if (pod.spec?.nodeName) return { kind: 'placed' }
  const c = (pod.status?.conditions ?? []).find((x: any) => x?.type === 'PodScheduled' && x?.status === 'False' && x?.reason === 'Unschedulable')
  if (!c) return { kind: 'waiting' }
  return { kind: 'refused', reason: String(c.message ?? 'the scheduler found no machine for it').replace(/\s+/g, ' ').trim().slice(0, 300) }
}

/**
 * One look of a start at its compute (control.ts `startPlaced`). `placed` once the pod has a machine; `refused` only when this pod was Unschedulable at `refuseAfter` looks in a row (this one included) AND the look has
 * ended AND no compute pod anywhere is on its way out (a stopping compute frees its memory in seconds, and a wake the gateway would have held for its 120 s would have been placed); otherwise `waiting`, and the caller
 * goes on as before. `refusedLooks` is the count to carry to the next look: it resets whenever the pod is not Unschedulable.
 */
export function startVerdict(a: { mine: any[]; pods: any[]; refusedLooks: number; lookEnded: boolean; refuseAfter: number }): { kind: 'placed' | 'waiting' | 'refused'; refusedLooks: number; reason: string } {
  const live = a.mine.filter((p) => !p?.metadata?.deletionTimestamp).sort((x, y) => (x.metadata?.creationTimestamp < y.metadata?.creationTimestamp ? 1 : -1))
  const v = placementVerdict(live[0])
  if (v.kind === 'placed') return { kind: 'placed', refusedLooks: 0, reason: '' }
  if (v.kind === 'waiting') return { kind: 'waiting', refusedLooks: 0, reason: '' }
  const looks = a.refusedLooks + 1
  const leaving = a.pods.some((p) => !!p?.metadata?.deletionTimestamp)
  if (a.lookEnded && looks >= a.refuseAfter && !leaving) return { kind: 'refused', refusedLooks: looks, reason: v.reason }
  return { kind: 'waiting', refusedLooks: looks, reason: v.reason }
}

/** Waits for a promise for at most `ms`: `{ done: true, value }` when it settles first (a rejection still rejects), `{ done: false }` when the time runs out first (the work goes on, its answer is ignored). */
export async function within<T>(ms: number, work: Promise<T>): Promise<{ done: true; value: T } | { done: false }> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const late = new Promise<{ done: false }>((res) => { timer = setTimeout(() => res({ done: false }), Math.max(0, ms)) })
  try { return await Promise.race([work.then((value) => ({ done: true as const, value })), late]) } finally { clearTimeout(timer) }
}

const policyKey = (v: unknown): string => String(v).toLowerCase().replace(/_/g, '')
/** A secondary location's download progress (the pageserver's `secondary/status`) as a problem, or null when it is warm: a heatmap has been seen and every layer in it is on disk. */
export function secondaryWarmProblem(status: any): string | null {
  if (!status || typeof status !== 'object') return 'its download status is unreadable'
  if (!status.heatmap_mtime) return 'no heatmap has been downloaded yet'
  const have = Number(status.layers_downloaded), total = Number(status.layers_total)
  if (!(have >= total) || !(Number(status.bytes_downloaded) >= Number(status.bytes_total))) return `${have} of ${total} layers downloaded (${Number(status.bytes_downloaded)} of ${Number(status.bytes_total)} bytes)`
  return null
}
/** What the controller holds for a shard and what the pageservers hold, for `staleErrorProblem`: the controller's generation and intent, the locations it has observed, and what each pageserver says it holds attached. */
export interface ShardLocations {
  generation: number | null
  intent: { attached: number | null; secondary: number[] }
  observed: Record<string, { mode: string; generation: number | null } | undefined>
  /** per pageserver node id: the tenant as that pageserver lists it (`GET /v1/tenant`), absent when it does not list it */
  held: Record<string, { attached: boolean; generation: number | null } | undefined>
}
/**
 * The locations `staleErrorProblem` compares, for the shards that carry an error: the controller's (`GET /debug/v1/tenant`) and what each
 * pageserver lists (`GET /v1/tenant`, by tenant id). None when a pageserver did not answer (`undefined` in `held`): the shard then stays a
 * problem.
 */
export function shardLocations(debug: any[], held: Record<string, Map<string, { attached: boolean; generation: number | null }> | undefined>, withError: Set<string>): Map<string, ShardLocations> {
  const out = new Map<string, ShardLocations>()
  if (Object.values(held).some((h) => h === undefined)) return out
  for (const d of debug) {
    const id = String(d.tenant_shard_id)
    if (!withError.has(id)) continue
    out.set(id, {
      generation: typeof d.generation === 'number' ? d.generation : null,
      intent: { attached: d.intent?.attached ?? null, secondary: (d.intent?.secondary ?? []) as number[] },
      observed: Object.fromEntries(Object.entries(d.observed?.locations ?? {}).map(([n, v]: [string, any]) => [n, v?.conf ? { mode: String(v.conf.mode), generation: v.conf.generation ?? null } : undefined])),
      held: Object.fromEntries(Object.entries(held).map(([n, h]) => [n, h!.get(id)])),
    })
  }
  return out
}
/** The shards that carry an error (`last_error`), by id. */
export const shardsWithError = (tenants: any[]): Set<string> => new Set<string>(tenants.flatMap((t) => (t.shards ?? []) as any[]).filter((s) => s.last_error).map((s) => String(s.tenant_shard_id)))
/**
 * Whether a shard's `last_error` is a leftover: this version of the controller never clears one (a reconcile that succeeds leaves it; only a later error replaces it, and a restart empties it), so after a failed
 * compute notification that was retried and settled the string stays. It is a leftover, and so only a warning, when the shard is not reconciling, has no compute notification pending, **and the controller's intent, what it
 * has observed and what the pageservers themselves hold all agree**: the intended attached node holds it attached at the controller's generation (by the controller's observation and by that pageserver's own list), no other
 * pageserver lists it attached, and every intended secondary is observed as a secondary. In every other case (no locations to compare included) it is a problem. Returns null for a leftover, else why not.
 */
export function staleErrorProblem(shard: { is_reconciling?: boolean; is_pending_compute_notification?: boolean }, loc: ShardLocations | undefined): string | null {
  if (shard.is_reconciling) return 'it is reconciling'
  if (shard.is_pending_compute_notification) return 'a compute notification is pending'
  if (!loc) return 'its locations could not be compared'
  const A = loc.intent.attached
  if (A === null || A === undefined || loc.generation === null || loc.generation === undefined) return 'it has no attached intent or no generation'
  const obs = loc.observed[String(A)]
  if (!obs || !/^Attached/.test(obs.mode) || obs.generation !== loc.generation) return `the controller observes node ${A} as ${obs ? `${obs.mode} at generation ${obs.generation}` : 'nothing'}, not attached at generation ${loc.generation}`
  const held = loc.held[String(A)]
  if (!held || !held.attached || held.generation !== loc.generation) return `node ${A} itself lists it as ${held ? `${held.attached ? 'attached' : 'not attached'} at generation ${held.generation}` : 'not held'}, not attached at generation ${loc.generation}`
  for (const [node, h] of Object.entries(loc.held)) if (node !== String(A) && h?.attached) return `node ${node} also lists it attached`
  for (const sec of loc.intent.secondary) { const o = loc.observed[String(sec)]; if (!o || o.mode !== 'Secondary') return `the intended secondary on node ${sec} is observed as ${o ? o.mode : 'nothing'}` }
  return null
}
/**
 * What the platform's storage looks like to the move of an image (the upgrade's check between steps): the storage controller's node list and tenant list, judged. Nothing is a problem that a settled platform
 * does not show: every node `Active` for availability and scheduling, every shard attached to an Active node, no reconciliation or compute notification pending, no splitting or importing, no error left on a shard that
 * is not a leftover (`staleErrorProblem`: a leftover is a warning), as many secondaries as the tenant's policy asks for, **and, when `statuses` is given (key `<shard>/<node>`, the pageserver's `secondary/status` of that
 * secondary), each of them warm**: a second pageserver restarted before the first one's copies have finished downloading would fail over onto cold ones. `locations` (by shard) is what `staleErrorProblem` compares.
 */
export function storageStateReport(nodes: any[], tenants: any[], expectedNodes: number[], statuses?: Map<string, any>, locations?: Map<string, ShardLocations>): { problems: string[]; warnings: string[] } {
  const problems: string[] = []
  const warnings: string[] = []
  const byId = new Map<number, any>(nodes.map((n) => [Number(n.id), n]))
  for (const id of expectedNodes) {
    const n = byId.get(id)
    if (!n) { problems.push(`node ${id} is not registered`); continue }
    if (n.availability !== 'Active') problems.push(`node ${id} availability is ${JSON.stringify(n.availability)}`)
    if (n.scheduling !== 'Active') problems.push(`node ${id} scheduling is ${JSON.stringify(n.scheduling)}`)
  }
  for (const t of tenants) {
    const secondariesWanted = Number(t?.policy?.Attached ?? 0)
    for (const s of (t.shards ?? []) as any[]) {
      const id = String(s.tenant_shard_id ?? t.tenant_id).slice(0, 8)
      const at = s.node_attached
      if (at === null || at === undefined) problems.push(`shard ${id} is attached nowhere`)
      else if (byId.get(Number(at))?.availability !== 'Active') problems.push(`shard ${id} is attached to node ${at}, which is not Active`)
      if (s.last_error) {
        const why = staleErrorProblem(s, locations?.get(String(s.tenant_shard_id)))
        if (why === null) warnings.push(`shard ${id} still shows an old error, a leftover (nothing pending; the controller and both pageservers agree on where it is attached): ${String(s.last_error).slice(0, 120)}`)
        else problems.push(`shard ${id} has an error that is not a leftover (${why}): ${String(s.last_error).slice(0, 120)}`)
      }
      if (s.is_reconciling) problems.push(`shard ${id} is reconciling`)
      if (s.is_pending_compute_notification) problems.push(`shard ${id} has a compute notification pending`)
      if (s.is_splitting) problems.push(`shard ${id} is splitting`)
      if (s.is_importing) problems.push(`shard ${id} is importing`)
      if (s.scheduling_policy && s.scheduling_policy !== 'Active') problems.push(`shard ${id} scheduling policy is ${s.scheduling_policy}`)
      const secondaries = (s.node_secondary ?? []) as number[]
      if (secondaries.length < secondariesWanted) problems.push(`shard ${id} has ${secondaries.length} of ${secondariesWanted} secondaries`)
      if (statuses) for (const sec of secondaries) {
        const why = secondaryWarmProblem(statuses.get(`${s.tenant_shard_id}/${sec}`))
        if (why) problems.push(`shard ${id}'s secondary on node ${sec} is not warm: ${why}`)
      }
    }
  }
  return { problems, warnings }
}
export const storageStateProblems = (nodes: any[], tenants: any[], expectedNodes: number[], statuses?: Map<string, any>, locations?: Map<string, ShardLocations>): string[] => storageStateReport(nodes, tenants, expectedNodes, statuses, locations).problems
/** Whether a drain of node `id` has finished: the controller has put it to `PauseForRestart` (its own last step) and no shard is attached to it. `why` says what is left. */
export function nodeDrainState(nodes: any[], tenants: any[], id: number): { done: boolean; why: string } {
  const n = nodes.find((x) => Number(x.id) === id)
  if (!n) return { done: false, why: `node ${id} is not registered` }
  const attached = tenants.flatMap((t) => (t.shards ?? []) as any[]).filter((s) => Number(s.node_attached) === id).length
  const pol = policyKey(n.scheduling)
  if (pol !== 'pauseforrestart') return { done: false, why: `node ${id} scheduling is ${JSON.stringify(n.scheduling)}, ${attached} shards still attached` }
  if (attached > 0) return { done: false, why: `${attached} shards are still attached to node ${id}` }
  return { done: true, why: '' }
}
/** Whether a fill of node `id` has finished: the node is available and its scheduling is back to `Active` (the controller sets `Filling` while it moves shards onto it). */
export function nodeFillState(nodes: any[], id: number): { done: boolean; why: string } {
  const n = nodes.find((x) => Number(x.id) === id)
  if (!n) return { done: false, why: `node ${id} is not registered` }
  if (n.availability !== 'Active') return { done: false, why: `node ${id} availability is ${JSON.stringify(n.availability)}` }
  if (policyKey(n.scheduling) !== 'active') return { done: false, why: `node ${id} scheduling is ${JSON.stringify(n.scheduling)}` }
  return { done: true, why: '' }
}

// ---- a database's Postgres major version
// A database's timeline carries one version fixed at creation (the storage controller's `pg_version`), and its compute must be that version's image: a branch, a restore and a recovered timeline are of the parent's version
// and the compute that serves any of them is that version's. The version is part of the database's record (`pg`); **a record without it is 18**, every database made before 2026-11-26 (`UNRECORDED_PG_VERSION`: this never moves, it
// is what those records are). **19 is the default of a request that names none** (`DEFAULT_PG_VERSION`, since 2026-11-26); `pg_version: 18` still asks for the old one.
export const PG_VERSIONS = [18, 19] as const
export const UNRECORDED_PG_VERSION = 18
export const DEFAULT_PG_VERSION = 19

/** The version a database's record holds: `pg` absent (made before the field existed) is 18; anything but a version the platform runs is an error, never a guess. */
export function pgVersionOf(recorded: string | undefined): number {
  if (recorded === undefined || recorded === '') return UNRECORDED_PG_VERSION
  const v = Number(recorded)
  if (!(PG_VERSIONS as readonly number[]).includes(v)) throw new Error(`a database record names Postgres ${JSON.stringify(recorded)}, which the platform does not run`)
  return v
}

/** The version a create call asks for (`pg_version`, a number or a numeric string): absent is `DEFAULT_PG_VERSION` (19), and a version the platform does not run is refused. */
export function requestedPgVersion(asked: unknown, byDefault: number = DEFAULT_PG_VERSION): number {
  if (asked === undefined || asked === null || asked === '') return byDefault
  const v = Number(asked)
  if (!(PG_VERSIONS as readonly number[]).includes(v)) throw new Error(`pg_version must be one of ${PG_VERSIONS.join(', ')}, not ${JSON.stringify(asked)}`)
  return v
}

/** The compute image of a version, from the control layer's environment (`COMPUTE_IMAGE` for 18, `COMPUTE_IMAGE_PG19` for 19). A version with no image set is refused: nothing is made that no compute could serve. */
export function computeImageFor(pg: number, images: Record<number, string | undefined>): string {
  const image = images[pg]
  if (!image) throw new Error(`no compute image is set for Postgres ${pg}`)
  return image
}

/**
 * What the control layer answers a caller held to `cls` (its pod address's class) calling `method path`: `pass`, `refuse` (403), or
 * `again` (503, call again) for an address in no class calling what some class may. That is most likely a pod so new that the API server
 * does not list its address yet: a storage controller sends its first notification within a second of starting, and a 403 is an answer it
 * never retries and keeps as the shard's last error. `again` admits nothing.
 */
export function callerVerdict(routes: Record<string, [string, RegExp][]>, cls: string, method: string, path: string): 'pass' | 'again' | 'refuse' {
  const may = (c: string) => routes[c]!.some(([m, re]) => m === method && re.test(path))
  if (may(cls)) return 'pass'
  return cls === 'unknown' && Object.keys(routes).some((c) => c !== 'unknown' && may(c)) ? 'again' : 'refuse'
}
