// Peat's two object kinds, `Platform` and `Database`, in the API group `peat.zabaca.com`, version `v1alpha1`: their fields, what each
// means and its default. This file is the one definition: the install file's CustomResourceDefinitions are rendered from it (crds.ts),
// `kubectl explain platform.spec` prints its descriptions, and the operator, the control layer and the command read objects through its
// types. Field names follow Kubernetes's conventions (camelCase; a Secret named by `...Secret`; durations as strings).
import { GROUP, VERSION } from '../names'
import type { Window } from '../quiet-hours'
import { bool, duration, int, list, map, obj, rules, str, type Schema } from './schema'

export { GROUP, VERSION }
export const API_VERSION = `${GROUP}/${VERSION}`
export const PG_VERSIONS = [18, 19] as const
export type PgVersion = (typeof PG_VERSIONS)[number]

// ---- shared shapes

const memory = (what: string, request: string, limit: string): Schema =>
  obj(`The memory ${what} asks for and may use.`, {
    request: str('What the pod asks for (a Kubernetes quantity).', { default: request }),
    limit: str('What it may use before it is killed.', { default: limit }),
  }, { default: { request, limit } })
const label = (description: string, def?: { key: string; value: string }): Schema =>
  obj(description, { key: str('The label key.'), value: str('The label value.') }, { required: ['key', 'value'], ...(def ? { default: def } : {}) })
const taint = (description: string): Schema =>
  obj(description, { key: str('The taint key.'), value: str('The taint value.'), effect: str('The taint effect.', { enum: ['NoSchedule', 'PreferNoSchedule', 'NoExecute'] }) }, { required: ['key', 'effect'] })
const tolerations: Schema = list('Tolerations, as a pod spec carries them.', { type: 'object', 'x-kubernetes-preserve-unknown-fields': true })
const storage = (what: string, defPath: string): Schema =>
  obj(`Where ${what} keep their data on their machine: a host directory (the default) or a volume of a storage class.`, {
    hostPath: obj('A directory on the machine.', {
      path: str('The directory.', { default: defPath }),
      create: bool('Make the directory when it is missing. False (the default) fails the pod instead, so a volume that did not provision never fills the system disk.', { default: false }),
    }),
    storageClass: obj('A PersistentVolumeClaim per piece, of this storage class.', { name: str('The storage class.'), size: str('The size of each volume (a Kubernetes quantity).') }, { required: ['name', 'size'] }),
  }, { default: { hostPath: { path: defPath, create: false } } })
const store = (what: string): Schema =>
  obj(`The bucket ${what} use, when it is not the platform's one bucket.`, {
    bucket: str('The bucket.'),
    credentialsSecret: str('A Secret in the platform namespace holding AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for this bucket.'),
    prefix: str("The folder of the bucket this piece writes under; default the piece's own name."),
  }, { required: ['bucket', 'credentialsSecret'] })
const windows = (description: string): Schema =>
  list(description, obj('A window: a start and an end on the clock of a time zone, on the days listed (every day when none are). An end at or before the start runs past midnight.', {
    start: str('HH:MM, 24-hour.', { pattern: '^([01][0-9]|2[0-3]):[0-5][0-9]$' }),
    end: str('HH:MM, 24-hour.', { pattern: '^([01][0-9]|2[0-3]):[0-5][0-9]$' }),
    timeZone: str('An IANA time zone; UTC when absent.'),
    days: list('The days the window starts on.', str('A day.', { enum: ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] })),
  }, { required: ['start', 'end'] }), { default: [] })
const condition: Schema = obj('A condition, as Kubernetes objects report them.', {
  type: str('What the condition is about.'),
  status: str('True, False or Unknown.', { enum: ['True', 'False', 'Unknown'] }),
  reason: str('A one-word reason.'),
  message: str('What a person reads.'),
  lastTransitionTime: str('When the status last changed.', { format: 'date-time' }),
  observedGeneration: int('The generation the condition was set from.'),
}, { required: ['type', 'status'] })

// ---- Platform

const imagesSchema = (description: string): Schema => obj(description, {
  release: str('The release these images are.'),
  storage: str('The storage image.'), control: str('The control image.'),
  compute: obj('The compute image of each Postgres version.', { '18': str('Postgres 18.'), '19': str('Postgres 19.') }),
})

export const DATABASE_NAME = '^[a-z][a-z0-9-]{0,30}$'

export const platformSpec: Schema = obj('One Peat platform: its storage, its control layer, its gateways and how its databases run. One per cluster.', {
  release: str('The Peat release every piece runs: a release tag (release-...) or a commit of Peat main. Changing it upgrades the platform in the safe order; setting the earlier one back takes back what had moved.'),
  images: obj('The images, when they are not the release\'s own from the registry (an install that pins images by digest names them here).', {
    registry: str('Where the release\'s images are.', { default: 'ghcr.io/zabaca/peat' }),
    storage: str('The storage image (pageserver, safekeeper, storage controller, broker).'),
    control: str('The control image (the operator, the control layer and the gateways).'),
    compute: obj('The compute image of each Postgres version.', { '18': str('Postgres 18.'), '19': str('Postgres 19.') }),
  }, { default: { registry: 'ghcr.io/zabaca/peat' } }),
  namespaces: obj('The namespaces the platform uses (the status records them). They cannot change once the platform is made (its pieces and its data live in them), and none may be a namespace of the cluster\'s own.', {
    platform: str('The storage, the control layer and its records.', { default: 'peat' }),
    computes: str("The databases' computes.", { default: 'peat-compute' }),
    gateways: str('The gateways and the front door.', { default: 'peat-gateway' }),
    databases: str("Each database's name inside the cluster (an ExternalName Service per database), and the ConfigMap peat-ca: the authority's public certificate (ca.crt), which every namespace that holds a Database also gets.", { default: 'peat-db' }),
  }, { default: { platform: 'peat', computes: 'peat-compute', gateways: 'peat-gateway', databases: 'peat-db' }, ...rules(
    ['self == oldSelf', 'the namespaces cannot change: the platform\'s pieces and its data live in them'],
    ["[self.platform, self.computes, self.gateways, self.databases].all(n, n != 'default' && n != 'peat-system' && !n.startsWith('kube-'))", "a namespace of the platform may not be default, the operator's peat-system or one of the kube- namespaces"],
  ) }),
  machines: obj('The machines the platform runs on.', {
    label: label('The label that marks them: the operator places every piece on these and the control layer probes and taints these and no other.', { key: `${GROUP}/machine`, value: 'true' }),
    taint: taint('A taint the machines carry, which every piece tolerates (none: the machines are shared with other work).'),
    deadTaint: taint('The taint the control layer puts on a machine it has declared dead.'),
  }, { default: { label: { key: `${GROUP}/machine`, value: 'true' } } }),
  spread: str('machines (the default): keepers on distinct zones, the controller never beside a pageserver, and a placement that breaks these is refused. none: a one-machine trial in which every piece may share a machine; the status says the platform survives no machine\'s loss.', { enum: ['machines', 'none'], default: 'machines' }),
  objectStore: obj('The S3-compatible store that holds the data. One bucket and one Secret serve every piece (each under a folder of its own), unless a piece is given its own.', {
    endpoint: str('The store\'s URL.'),
    region: str('The region of its buckets.', { default: 'us-east-1' }),
    bucket: str('The one bucket.'),
    credentialsSecret: str('A Secret in the platform namespace holding AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.'),
    pageservers: store('the pageservers'),
    safekeepers: store('the safekeepers'),
    records: store("the copy of the storage controller's records"),
    walTrim: obj("The key the control layer's WAL trim lists and deletes the safekeepers' old WAL with, when it is not the safekeepers' own.", { credentialsSecret: str('A Secret holding AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.') }, { required: ['credentialsSecret'] }),
  }, { required: ['endpoint'], ...rules(['(has(self.bucket) && has(self.credentialsSecret)) || (has(self.pageservers) && has(self.safekeepers) && has(self.records))', 'the store needs the one bucket and its credentialsSecret, or a bucket of their own for the pageservers, the safekeepers and the records']) }),
  pageservers: obj('The pageservers: where the databases\' pages are kept and served.', {
    count: int('How many; default 2, and 1 with spread none (which allows no more: two would share one machine\'s directory).', { minimum: 1, maximum: 64 }),
    machines: list('The machine of each, by node name, in order; default the operator chooses from the labelled machines.', str('A node name.')),
    memory: memory('a pageserver', '768Mi', '1536Mi'),
    storage: storage('the pageservers', '/var/lib/peat/pageserver'),
  }, { default: {} }),
  safekeepers: obj('The safekeepers: the write-ahead log, kept on a majority before a commit is acknowledged.', {
    count: int('Three or five.', { enum: [3, 5], default: 3 }),
    zoneLabel: str('The node label whose value is a machine\'s zone: no two members of a timeline\'s set are in one zone.', { default: 'kubernetes.io/hostname' }),
    machines: list('The machine of each of the first members, by node name, in order; default the operator chooses.', str('A node name.')),
    outside: obj('Machines outside the platform\'s that may hold one member, so a timeline\'s set does not live on the platform\'s machines alone. A member is moved there by an operator\'s call; a replacement of that member comes back to it.', {
      machines: list('The machines, by node name.', str('A node name.')),
      label: label('The label those machines carry.', { key: `${GROUP}/keeper`, value: 'true' }),
      tolerations,
    }),
    memory: memory('a safekeeper', '128Mi', '512Mi'),
    storage: storage('the safekeepers', '/var/lib/peat/safekeeper'),
  }, { default: {} }),
  controller: obj("The storage controller and the broker: where each database's data is placed, and the storage pieces' meeting point.", {
    records: str("Where the controller keeps its records: kubernetes (the default: objects in the platform namespace, copied to the store after every change) or postgres (a database you run, named below).", { enum: ['kubernetes', 'postgres'], default: 'kubernetes' }),
    postgres: obj('The Postgres the records are kept in, for records postgres.', { urlSecret: str('A Secret in the platform namespace holding the database URL.'), key: str('The key of that Secret.', { default: 'uri' }) }, { required: ['urlSecret'] }),
    machine: str('The machine of the controller and the broker, by node name; never a pageserver\'s (spread machines); default the operator chooses.'),
    memory: memory('the controller', '64Mi', '256Mi'),
  }, { default: {}, ...rules(["!has(self.records) || self.records != 'postgres' || has(self.postgres)", 'records postgres needs controller.postgres: the Secret holding the database\'s URL']) }),
  gateways: obj('The gateways: the one front door every database is reached through, which wakes a sleeping database when a client connects.', {
    count: int('How many, each on its own machine.', { minimum: 1, maximum: 16, default: 2 }),
    domain: str('The domain the databases are reached under from outside the cluster: a client connects to <database>.<domain> and the front door reads the name from TLS. Point its wildcard at the front door.'),
    service: obj('How the front door is exposed.', {
      type: str('The Service type.', { enum: ['ClusterIP', 'LoadBalancer', 'NodePort'], default: 'ClusterIP' }),
      annotations: map('Annotations of the Service (a load balancer\'s settings).'),
    }, { default: { type: 'ClusterIP' } }),
    memory: memory('a gateway', '48Mi', '192Mi'),
  }, { default: {} }),
  certificates: obj("The databases' certificates.", {
    issuer: obj('A cert-manager issuer to sign them; none (the default): the platform\'s own certificate authority signs them. With an issuer, the ConfigMap peat-ca clients verify with carries the authority keys.caSecret names, or is not published (the platform\'s own authority would sign nothing).', {
      name: str('The issuer.'), kind: str('Issuer (in the compute namespace) or ClusterIssuer.', { enum: ['Issuer', 'ClusterIssuer'], default: 'Issuer' }),
    }, { required: ['name'] }),
  }, { default: {} }),
  computes: obj('How the databases\' computes run.', {
    defaultPgVersion: int('The Postgres version of a database that names none.', { enum: [...PG_VERSIONS], default: 19 }),
    idleAfter: duration('A database with no session for this long is put to sleep (its compute removed); the next connection wakes it. A duration (30m).', { default: '30m' }),
    memory: memory('a compute', '512Mi', '1Gi'),
    scratch: str('The size of a compute\'s scratch directory (temporary files, sorts).', { default: '2Gi' }),
  }, { default: {} }),
  databases: obj('How databases are kept.', {
    deleteHold: str("How long a deleted database's data stays in the store before it is removed: a duration (7d, 36h), or never.", { pattern: '^(never|[0-9]+(s|m|h|d))$', default: '7d' }),
  }, { default: {} }),
  upgrades: obj('How an upgrade moves.', {
    quietHours: windows('Windows in which no upgrade step and no drill starts. An upgrade under way pauses at the next step and goes on when they end.'),
    restartHours: windows('Windows in which an awake database is restarted onto a new compute image; outside them a compute takes it at its next sleep. None: only at sleep.'),
    checkTimeout: duration('How long each check between steps waits for the platform to settle before the upgrade halts there. A duration (10m).', { default: '10m' }),
  }, { default: {} }),
  keys: obj('The keys the platform signs with. None given: the operator makes them once and keeps them.', {
    authSecret: str("A Secret in the platform namespace holding the Ed25519 pair the storage's tokens are signed with (auth_private_key.pem, auth_public_key.pem)."),
    caSecret: str("A Secret in the platform namespace holding the certificate authority that signs the databases' certificates (tls.crt, tls.key): the platform namespace holds every key; clients get only its certificate (the ConfigMap peat-ca). With certificates.issuer, a Secret holding the issuer's certificate alone (ca.crt, the authority above an intermediate issuer, or tls.crt), so peat-ca carries the certificate that signs the databases': its key is never needed or kept, and a Secret that holds it (tls.key) is refused. The status warns 30 days before the certificate expires."),
  }, { default: {} }),
  control: obj('The control layer.', {
    replicas: int('How many pods; one acts at a time.', { minimum: 1, maximum: 5, default: 2 }),
    memory: memory('a control layer pod', '64Mi', '256Mi'),
  }, { default: {} }),
  adopt: list('Objects the operator may take over by name from an install made another way. It compares each with what it would make: one it would not change is taken over as it is; one it would change is taken over and left as it is, the difference listed in status.pendingChanges, until requests.applyPending asks for it to be applied (taking an object off the list applies nothing). It is also how the operator takes back objects its record of what it made has lost (a cluster restored with new UIDs).', obj('An object.', {
    kind: str('Its kind.'), namespace: str('Its namespace (none: cluster-scoped).'), name: str('Its name.'),
  }, { required: ['kind', 'name'] })),
  suspend: bool('True: the operator changes nothing (it reports, and lets go of nothing it adopted). The way to hand the platform back to another way of running it.', { default: false }),
  requests: obj('One-off requests: set a new value to ask again.', {
    retry: str('Go on with a halted upgrade from the step that halted.'),
    applyPending: str('Apply the changes listed in status.pendingChanges to the adopted objects (each may restart its pods).'),
  }),
}, { required: ['release', 'objectStore'], ...rules(
  ["!has(self.spread) || self.spread != 'none' || !has(self.pageservers) || !has(self.pageservers.count) || self.pageservers.count == 1", 'spread none runs one pageserver: two would share one machine and its directory'],
) })

export const platformStatus: Schema = obj('What the operator found and did.', {
  observedGeneration: int('The generation of the spec this status is about.'),
  phase: str('Installing, Ready, Upgrading, Paused (quiet hours), Halted (a check failed: waiting for a person), Suspended or Failed.', { enum: ['Installing', 'Ready', 'Upgrading', 'Paused', 'Halted', 'Suspended', 'Failed'] }),
  release: str('The release every piece runs, once they all run one.'),
  images: imagesSchema('The images of that release: what the next upgrade starts from.'),
  message: str('What a person reads first.'),
  survives: str("What the platform survives: one machine's loss, or none (spread none)."),
  namespaces: obj('The namespaces the platform runs in, recorded: a Platform that names others is refused.', { platform: str('The platform namespace.'), computes: str('The computes\' namespace.'), gateways: str('The gateways\' namespace.'), databases: str('The database namespace.') }),
  pieces: list('Every piece and the release it runs.', obj('A piece.', {
    kind: str('Its kind.'), namespace: str('Its namespace.'), name: str('Its name.'),
    release: str('The release it runs.'), ready: bool('Whether it is ready.'), adopted: bool('Whether the operator took it over.'), message: str('Why not, when not.'),
  })),
  upgrade: obj('The upgrade under way, or the last one.', {
    from: str('The release it started from.'), to: str('The release it goes to.'),
    direction: str('forward, or back (the earlier release set again: only what had moved goes back, in reverse order).', { enum: ['forward', 'back'] }),
    step: str('The step it is on.'), steps: list('Every step, in order.', str('A step.')), moved: list('The pieces on the release it goes to, in the order they moved.', str('A step.')),
    phase: str('Where the step is: precheck, draining, applying, restarting, waiting, filling, postcheck.', { enum: ['precheck', 'draining', 'applying', 'restarting', 'waiting', 'filling', 'postcheck'] }),
    since: str('When the step\'s phase began: its wait is bounded by upgrades.checkTimeout.', { format: 'date-time' }),
    fromImages: imagesSchema('The images it started from.'), toImages: imagesSchema('The images it goes to.'),
    complete: bool('Whether it is done.'),
    startedAt: str('When it started.', { format: 'date-time' }),
    lastCheck: obj('The last check between steps.', { at: str('When.', { format: 'date-time' }), passed: bool('Whether it passed.'), message: str('What it found.') }),
    halted: obj('Where and why it halted.', { at: str('When.', { format: 'date-time' }), step: str('The step.'), reason: str('Why.') }),
    retried: str('The last retry request answered.'),
    refill: int('A pageserver whose drain the way back dropped: filled again before the next step.'),
    reason: str('Why the way back was taken without the earlier release being set.'),
  }),
  answered: obj('The requests answered, by the value that was asked (the retry an upgrade answered is upgrade.retried).', { applyPending: str('The applyPending answered.') }),
  pendingChanges: list('Differences between an adopted object and what the operator would make, not applied (requests.applyPending applies them).', obj('A difference.', { kind: str('Kind.'), namespace: str('Namespace.'), name: str('Name.'), change: str('What would change.') })),
  conditions: list('Conditions.', condition),
})

// ---- Database

export const databaseSpec: Schema = obj('A database on the platform. Its name is the object\'s name (1 to 31 lowercase letters, digits or hyphens, starting with a letter), unique across the platform; it is reached at <name>.<the platform\'s domain> through the front door, and inside the cluster at <name>.<database namespace>.svc.', {
  pgVersion: int("Its Postgres version; default the platform's. Raising it moves the database to the newer version under its own name, address and login (the door is shut for the copy, every table is compared, the old copy is kept stopped); lowering it is refused, and so is any version below the one it runs (status.pgVersion), as for a database made without one and later given an older one.", { enum: [...PG_VERSIONS] }),
  parent: str('A database (in the same namespace) this one is made from: a copy-on-write branch of it, or, with at, a restore of it to a moment. The parent is untouched.', { pattern: DATABASE_NAME }),
  at: str('With parent: the moment (an RFC 3339 time) or the log position (X/Y) the copy is made at; none: now.'),
  replicaOf: str("A database this one is a read replica of: a second compute on its parent's data, read only.", { pattern: DATABASE_NAME }),
  idleAfter: duration("How long with no session before it sleeps; default the platform's. A duration (30m)."),
  memory: obj('The memory of its compute; default the platform\'s.', { request: str('What it asks for.'), limit: str('What it may use.') }),
  closed: bool('True: every connection is refused and it stays asleep until this is false again.', { default: false }),
  keepOldFor: duration('After a move to a newer Postgres version, how long the old copy is kept, stopped, as the way back. A duration (30d).', { default: '30d' }),
  requests: obj('One-off requests: set a new value to ask again; the status says which value was answered.', {
    stop: str('Put it to sleep now (the next connection wakes it).'),
    rehearseMove: str('Copy and compare to the version named in pgVersion without shutting the door or switching: how long the move would take.'),
  }),
}, rules(
  ['!has(self.at) || has(self.parent)', 'at is the moment of a parent: name the parent'],
  ['!(has(self.parent) && has(self.replicaOf))', 'a database is made from a parent or is a replica of one, not both'],
  ['(has(self.parent) == has(oldSelf.parent)) && (!has(self.parent) || self.parent == oldSelf.parent)', 'parent cannot change: it is what the database was made from'],
  ['(has(self.at) == has(oldSelf.at)) && (!has(self.at) || self.at == oldSelf.at)', 'at cannot change: it is the moment the database was made from'],
  ['(has(self.replicaOf) == has(oldSelf.replicaOf)) && (!has(self.replicaOf) || self.replicaOf == oldSelf.replicaOf)', 'replicaOf cannot change'],
  ['!has(oldSelf.pgVersion) || (has(self.pgVersion) && self.pgVersion >= oldSelf.pgVersion)', 'pgVersion is never lowered: a database moves to a newer Postgres, never back'],
))

export const databaseStatus: Schema = obj('The database as it is.', {
  observedGeneration: int('The generation of the spec this status is about.'),
  phase: str('Creating, Ready (awake or asleep), Closed, Moving, Deleting or Failed.', { enum: ['Creating', 'Ready', 'Closed', 'Moving', 'Deleting', 'Failed'] }),
  message: str('What a person reads first.'),
  host: str('The name a client connects to from outside the cluster (<name>.<domain>), when the platform has a domain.'),
  serviceHost: str('The name inside the cluster.'),
  port: int('The port (the front door\'s, 5432).'),
  loginSecret: str('The Secret in this namespace holding its login: username, password, and a connection URL.'),
  caConfigMap: str("The ConfigMap in this namespace holding the certificate authority's public certificate (ca.crt), for sslmode=verify-full."),
  pgVersion: int('The Postgres version it runs.'),
  awake: bool('Whether a compute runs now.'),
  tenant: str("The storage's tenant id."),
  timeline: str("The storage's timeline id."),
  createdAt: str('When it was made.', { format: 'date-time' }),
  answered: obj('The requests answered, by the value that was asked.', { stop: str('The stop request answered.'), rehearseMove: str('The rehearsal answered.') }),
  move: obj('The move to a newer Postgres version under way, or the last one (or the last rehearsal).', {
    from: int('The version it moved from.'), to: int('The version it moved to.'),
    rehearsal: bool('Whether this was a rehearsal.'), state: str('The step it is on, or done, or failed.'),
    seconds: int('How long the copy took, or the door was shut.'), equal: bool('Whether every table, role, privilege and extension compared equal.'),
    oldDatabase: str('The old copy, kept stopped.'), message: str('What it found.'),
  }),
  record: obj("The control layer's own record of the database, written by it alone; it keeps a copy in the platform namespace and writes this again from it if it is lost.", {
    mode: str('primary, or replica (a read replica on its parent\'s timeline).', { enum: ['primary', 'replica'] }),
    root: bool('Whether it is the first database of its tenant (a branch and a replica share their root\'s tenant and login).'),
    parent: str("The root database whose login a branch or a replica uses; empty for a root."),
    from: str('The database it was made from (a branch\'s or a replica\'s parent); empty for a root.'),
    port: int("The gateways' own port for it (inside the gateway pods; clients use the front door)."),
  }),
  deleteAfter: str("When a deleted database's data is removed from the store (never: kept).", { }),
  conditions: list('Conditions.', condition),
})

// ---- the types the program reads objects through

export interface Memory { request: string; limit: string }
export interface Storage { hostPath?: { path: string; create?: boolean }; storageClass?: { name: string; size: string } }
export interface Store { bucket: string; credentialsSecret: string; prefix?: string }
export interface PlatformSpec {
  release: string
  images: { registry: string; storage?: string; control?: string; compute?: { '18'?: string; '19'?: string } }
  namespaces: { platform: string; computes: string; gateways: string; databases: string }
  machines: { label: { key: string; value: string }; taint?: { key: string; value?: string; effect: string }; deadTaint?: { key: string; value?: string; effect: string } }
  spread: 'machines' | 'none'
  objectStore: { endpoint: string; region: string; bucket?: string; credentialsSecret?: string; pageservers?: Store; safekeepers?: Store; records?: Store; walTrim?: { credentialsSecret: string } }
  pageservers: { count?: number; machines?: string[]; memory: Memory; storage: Storage }
  safekeepers: { count: 3 | 5; zoneLabel: string; machines?: string[]; outside?: { machines?: string[]; label: { key: string; value: string }; tolerations?: Record<string, unknown>[] }; memory: Memory; storage: Storage }
  controller: { records: 'kubernetes' | 'postgres'; postgres?: { urlSecret: string; key: string }; machine?: string; memory: Memory }
  gateways: { count: number; domain?: string; service: { type: 'ClusterIP' | 'LoadBalancer' | 'NodePort'; annotations?: Record<string, string> }; memory: Memory }
  certificates: { issuer?: { name: string; kind: 'Issuer' | 'ClusterIssuer' } }
  computes: { defaultPgVersion: PgVersion; idleAfter: string; memory: Memory; scratch: string }
  databases: { deleteHold: string }
  upgrades: { quietHours: Window[]; restartHours: Window[]; checkTimeout: string }
  keys: { authSecret?: string; caSecret?: string }
  control: { replicas: number; memory: Memory }
  adopt?: { kind: string; namespace?: string; name: string }[]
  suspend: boolean
  requests?: { retry?: string; applyPending?: string }
}
export interface DatabaseSpec {
  pgVersion?: PgVersion
  parent?: string
  at?: string
  replicaOf?: string
  idleAfter?: string
  memory?: Partial<Memory>
  closed: boolean
  keepOldFor: string
  requests?: { stop?: string; rehearseMove?: string }
}
export interface ObjectMeta { name: string; namespace?: string; uid?: string; generation?: number; resourceVersion?: string; labels?: Record<string, string>; annotations?: Record<string, string>; finalizers?: string[]; deletionTimestamp?: string; creationTimestamp?: string }
export interface Platform { apiVersion: typeof API_VERSION; kind: 'Platform'; metadata: ObjectMeta; spec: PlatformSpec; status?: Record<string, unknown> }
export interface Database { apiVersion: typeof API_VERSION; kind: 'Database'; metadata: ObjectMeta; spec: DatabaseSpec; status?: Record<string, unknown> }

/** The kinds, as the CustomResourceDefinitions name them. */
export const KINDS = {
  Platform: { plural: 'platforms', singular: 'platform', scope: 'Cluster' as const, shortNames: ['peat'], spec: platformSpec, status: platformStatus },
  Database: { plural: 'databases', singular: 'database', scope: 'Namespaced' as const, shortNames: ['peatdb'], spec: databaseSpec, status: databaseStatus },
}
