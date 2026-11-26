// Every object a platform runs on, rendered from its resolved Platform: what the operator applies and keeps. Pure; a test reads it.
//
// The pieces, and where each runs (the rules of spread `machines`, which platform.ts checks):
//   - the base: the four namespaces (the platform's is privileged: its pieces keep data in host directories, and their init containers run
//     as root; the computes' baseline, as they run tenants' SQL; the gateways' and the databases' restricted), a default-deny policy in each
//     namespace that holds pods, and the login rules every compute mounts;
//   - the keys: the Secrets each piece reads, from the operator's record of the keys (keys.ts);
//   - the storage: the broker and the storage controller on the controller's machine, a pageserver on each of its machines (pinned: its
//     data is there); the safekeepers are the control layer's, which makes the first ones from SAFEKEEPER_SEED and replaces a lost one;
//   - the control layer: two pods on different machines, one acting at a time, its account and what it may do;
//   - the gateways and the one front door;
//   - the guards: admission rules that hold the control layer's account to the platform's machines, to the safekeeper's shape and to
//     Services that stay inside the cluster, whatever its program does.
// Nothing rendered copies a label of the Platform object (a tool that applies the Platform by label and prunes what carries its label
// would otherwise take the pieces with it). Each object carries the operator's own two labels and nothing of the Platform's.
import { GROUP, controlUser, frontDoor, placement, toEnv, type Names } from '../names'
import { SAFEKEEPER_RESOURCES } from '../storage-spec'
import type { Resolved } from './platform'
import { TOKENS, type Holder } from './keys'

export type Manifest = Record<string, any> & { apiVersion: string; kind: string; metadata: { name: string; namespace?: string; labels?: Record<string, string>; annotations?: Record<string, string> } }

export const MANAGED_BY = 'app.kubernetes.io/managed-by'
export const OPERATOR = 'peat-operator'
export const PLATFORM_LABEL = `${GROUP}/platform`
/** The operator's record of the keys, in the platform namespace: never rendered, never pruned. */
export const KEYS_SECRET = 'peat-keys'
/** The Secrets the pieces read, made from the record. */
export const SECRETS = {
  authPublic: 'peat-auth-public',
  controlKey: 'peat-control-key',
  controllerTokens: 'peat-storcon-tokens',
  pageserverTokens: 'peat-pageserver-tokens',
  safekeeperTokens: 'peat-safekeeper-tokens',
  ca: 'peat-ca',
  caKey: 'peat-ca-key',
} as const
export const CA_ISSUER = 'peat-ca'
export const HBA_CONFIG_MAP = 'peat-pg-hba'
export const PORTS = { brokerGrpc: 50051, controller: 1234, pageserverPg: 6400, pageserverHttp: 9898, safekeeperPg: 5454, safekeeperHttp: 7676, control: 8080, gatewayPg: 5432, gatewayMetrics: 9090 } as const
/** The label of a namespace whose pods may read every piece's metrics (a Prometheus's). */
export const SCRAPE_LABEL = `${GROUP}/scrape`

export const HBA = `# the compute's own connections (compute_ctl, the readiness probe) are local and trusted, as Neon's default; every other login must use TLS and a scram-sha-256 password
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
hostssl all             all             all                     scram-sha-256
`

// ---- small builders

const tcp = (...ports: number[]) => ports.map((port) => ({ protocol: 'TCP', port }))
const inNamespace = (name: string) => ({ namespaceSelector: { matchLabels: { 'kubernetes.io/metadata.name': name } } })
const piece = (namespace: string, app: string) => ({ ...inNamespace(namespace), podSelector: { matchLabels: { app } } })
const DNS = { to: [{ ...inNamespace('kube-system'), podSelector: { matchLabels: { 'k8s-app': 'kube-dns' } } }], ports: [{ protocol: 'UDP', port: 53 }, { protocol: 'TCP', port: 53 }] }
const SCRAPER = { namespaceSelector: { matchLabels: { [SCRAPE_LABEL]: 'true' } } }
/** The API server, at whatever address the cluster gives it (a policy sees the endpoint's address and port, not the Service's). */
const API_SERVER = { to: [{ ipBlock: { cidr: '0.0.0.0/0' } }], ports: tcp(443, 6443) }
const storePort = (endpoint: string): number => { const u = new URL(endpoint); return Number(u.port || (u.protocol === 'https:' ? 443 : 80)) }
/** The object store, at its port and only its port (it may be outside the cluster or a pod inside it). */
const STORE = (r: Resolved) => ({ to: [{ ipBlock: { cidr: '0.0.0.0/0' } }], ports: tcp(storePort(r.store.endpoint)) })

function meta(r: Resolved, name: string, namespace?: string, more: Record<string, string> = {}) {
  return { name, ...(namespace ? { namespace } : {}), labels: { [MANAGED_BY]: OPERATOR, [PLATFORM_LABEL]: r.name, ...more } }
}
function policy(r: Resolved, namespace: string, app: string, ingress: { from: unknown[]; ports: unknown[] }[], egress: unknown[]): Manifest {
  return { apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy', metadata: meta(r, app, namespace), spec: { podSelector: { matchLabels: { app } }, policyTypes: ['Ingress', 'Egress'], ingress, egress } }
}
const secretEnv = (name: string, secret: string, key: string) => ({ name, valueFrom: { secretKeyRef: { name: secret, key } } })
const storeEnv = (secret: string) => [secretEnv('AWS_ACCESS_KEY_ID', secret, 'AWS_ACCESS_KEY_ID'), secretEnv('AWS_SECRET_ACCESS_KEY', secret, 'AWS_SECRET_ACCESS_KEY')]
const remoteStorage = (r: Resolved, s: { bucket: string; prefix: string }) => `{ endpoint='${r.store.endpoint}', bucket_name='${s.bucket}', bucket_region='${r.store.region}', prefix_in_bucket='${s.prefix}', timeout='20m' }`
/** A pod on the platform's machines: the label selected, the taint tolerated, no service account token unless it calls the API, no service links. */
function platformPod(names: Names, extra: Record<string, any> = {}, apiAccess = false) {
  const p = placement(names)
  return { automountServiceAccountToken: apiAccess, enableServiceLinks: false, ...extra, nodeSelector: { ...p.nodeSelector, ...(extra.nodeSelector ?? {}) }, tolerations: [...p.tolerations, ...(extra.tolerations ?? [])] }
}
const pinTo = (node: string) => ({ 'kubernetes.io/hostname': node })
/** Kubernetes adds a 300 s toleration of an unreachable or not-ready machine to every pod: a pod of ours on a dead machine leaves it after 5. */
const LEAVE_A_DEAD_MACHINE = [
  { key: 'node.kubernetes.io/unreachable', operator: 'Exists', effect: 'NoExecute', tolerationSeconds: 5 },
  { key: 'node.kubernetes.io/not-ready', operator: 'Exists', effect: 'NoExecute', tolerationSeconds: 5 },
]
const RESTRICTED_CONTAINER = { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ['ALL'] } }
const RESTRICTED_POD = { runAsNonRoot: true, runAsUser: 1000, runAsGroup: 1000, seccompProfile: { type: 'RuntimeDefault' } }
const twoMachines = (spread: string, labels: Record<string, string>) => spread === 'none' ? {} : { affinity: { podAntiAffinity: { requiredDuringSchedulingIgnoredDuringExecution: [{ labelSelector: { matchLabels: labels }, topologyKey: 'kubernetes.io/hostname' }] } } }

// ---- the base

export function base(r: Resolved): Manifest[] {
  const n = r.names
  const pss: [string, string][] = [[n.namespace, 'privileged'], [n.computeNamespace, 'baseline'], [n.gatewayNamespace, 'restricted'], [n.dbNamespace, 'restricted']]
  const deny = (ns: string): Manifest => ({ apiVersion: 'networking.k8s.io/v1', kind: 'NetworkPolicy', metadata: meta(r, 'default-deny', ns), spec: { podSelector: {}, policyTypes: ['Ingress', 'Egress'] } })
  return [
    ...pss.map(([name, level]) => ({ apiVersion: 'v1', kind: 'Namespace', metadata: meta(r, name, undefined, { 'pod-security.kubernetes.io/enforce': level }) })),
    { apiVersion: 'v1', kind: 'ConfigMap', metadata: meta(r, HBA_CONFIG_MAP, n.computeNamespace), data: { 'pg_hba.conf': HBA } },
    deny(n.namespace), deny(n.computeNamespace), deny(n.gatewayNamespace),
  ]
}

// ---- the keys: the record's entries, each to the Secret of the piece that reads it

export function keySecrets(r: Resolved, kept: Record<string, string>): Manifest[] {
  const n = r.names
  const secret = (name: string, namespace: string, data: Record<string, string>, type?: string): Manifest => ({ apiVersion: 'v1', kind: 'Secret', metadata: meta(r, name, namespace), ...(type ? { type } : {}), stringData: data })
  const tokens = (holder: Holder) => Object.fromEntries(TOKENS.filter((t) => t.holder === holder).map((t) => [t.key, kept[t.key] ?? '']))
  return [
    secret(SECRETS.authPublic, n.namespace, { 'auth_public_key.pem': kept['auth_public_key.pem'] ?? '' }),
    secret(SECRETS.controlKey, n.namespace, { 'auth_private_key.pem': kept['auth_private_key.pem'] ?? '' }),
    secret(SECRETS.controllerTokens, n.namespace, tokens('controller')),
    secret(SECRETS.pageserverTokens, n.namespace, tokens('pageserver')),
    secret(SECRETS.safekeeperTokens, n.namespace, tokens('safekeeper')),
    // the authority, in the computes' namespace: where a cert-manager CA Issuer would read it (tls.crt, tls.key), and the control layer signs with it
    secret(SECRETS.ca, n.computeNamespace, { 'tls.crt': kept['ca.crt'] ?? '', 'tls.key': kept['ca.key'] ?? '', 'ca.crt': kept['ca.crt'] ?? '' }, 'kubernetes.io/tls'),
    // and in the platform namespace, where the control layer reads it to sign each database's certificate (when no issuer is named)
    ...(r.spec.certificates.issuer ? [] : [secret(SECRETS.caKey, n.namespace, { 'tls.crt': kept['ca.crt'] ?? '', 'tls.key': kept['ca.key'] ?? '' }, 'kubernetes.io/tls')]),
  ]
}

// ---- the storage

function service(r: Resolved, name: string, app: string, ports: { name: string; port: number }[], headless = false): Manifest {
  return { apiVersion: 'v1', kind: 'Service', metadata: meta(r, name, r.names.namespace), spec: { ...(headless ? { clusterIP: 'None', publishNotReadyAddresses: true } : {}), selector: { app }, ports } }
}

export function broker(r: Resolved, image = r.images.storage): Manifest[] {
  const labels = { app: 'storage-broker' }
  return [
    service(r, 'storage-broker', 'storage-broker', [{ name: 'grpc', port: PORTS.brokerGrpc }]),
    {
      apiVersion: 'apps/v1', kind: 'Deployment', metadata: meta(r, 'storage-broker', r.names.namespace),
      spec: { replicas: 1, strategy: { type: 'Recreate' }, selector: { matchLabels: labels }, template: { metadata: { labels }, spec: platformPod(r.names, {
        nodeSelector: pinTo(r.placements.controller),
        containers: [{ name: 'storage-broker', image, command: ['/usr/local/bin/storage_broker', '--listen-addr', `0.0.0.0:${PORTS.brokerGrpc}`], ports: [{ containerPort: PORTS.brokerGrpc }], resources: { requests: { cpu: '50m', memory: '32Mi' }, limits: { memory: '128Mi' } }, readinessProbe: { tcpSocket: { port: PORTS.brokerGrpc }, periodSeconds: 5 } }],
      }) } },
    },
  ]
}

/** The controller's account: it reads and writes its records (records kubernetes) and nothing else of the cluster. */
export const CONTROLLER_ACCOUNT = 'storage-controller'
export const RECORD_KINDS = ['tenants', 'metadatahealths', 'timelines', 'pageservers', 'safekeepers', 'leaders'] as const
export const RECORDS_GROUP = `storagecontroller.${GROUP}`

/**
 * The storage controller, with Neon's own keys: the key that checks incoming tokens and the tokens it carries to the pageservers, the
 * safekeepers, the control layer's hook and a peer controller. Its records are Kubernetes objects in the platform namespace with a copy in
 * the store (records kubernetes), or a Postgres the install names. `--control-plane-url` is mandatory in practice (with none, the first
 * notification panics), `--timelines-onto-safekeepers` makes the controller create a timeline on the safekeepers (without it a compute
 * started on a new timeline reads empty pages), and `--timeline-safekeeper-count` is the number the platform runs.
 */
export function controller(r: Resolved, image = r.images.storage): Manifest[] {
  const labels = { app: 'storage-controller' }
  const n = r.names
  const t = SECRETS.controllerTokens
  const kubernetes = r.spec.controller.records === 'kubernetes'
  const records = kubernetes
    // its address in the Leader record, by which a second controller asks it to step down and the records' copy tool finds it to see that it is stopped
    ? ['--records', 'kubernetes', '--records-namespace', n.namespace, '--records-copy-storage', remoteStorage(r, r.store.records), '--address-for-peers', `http://storage-controller.${n.namespace}.svc:${PORTS.controller}`]
    : ['--database-url', '$(DATABASE_URL)']
  const recordsEnv = kubernetes ? storeEnv(r.store.records.credentialsSecret) : [secretEnv('DATABASE_URL', r.spec.controller.postgres!.urlSecret, r.spec.controller.postgres!.key)]
  return [
    service(r, 'storage-controller', 'storage-controller', [{ name: 'http', port: PORTS.controller }]),
    { apiVersion: 'v1', kind: 'ServiceAccount', metadata: meta(r, CONTROLLER_ACCOUNT, n.namespace) },
    ...(kubernetes ? [
      { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'Role', metadata: meta(r, CONTROLLER_ACCOUNT, n.namespace), rules: [{ apiGroups: [RECORDS_GROUP], resources: [...RECORD_KINDS], verbs: ['get', 'list', 'watch', 'create', 'update', 'delete'] }] },
      { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'RoleBinding', metadata: meta(r, CONTROLLER_ACCOUNT, n.namespace), roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'Role', name: CONTROLLER_ACCOUNT }, subjects: [{ kind: 'ServiceAccount', name: CONTROLLER_ACCOUNT, namespace: n.namespace }] },
    ] : []),
    {
      apiVersion: 'apps/v1', kind: 'Deployment', metadata: meta(r, 'storage-controller', n.namespace),
      spec: { replicas: 1, strategy: { type: 'Recreate' }, selector: { matchLabels: labels }, template: { metadata: { labels }, spec: platformPod(n, {
        serviceAccountName: CONTROLLER_ACCOUNT,
        nodeSelector: pinTo(r.placements.controller),
        containers: [{
          name: 'storage-controller', image,
          command: ['/usr/local/bin/storage_controller', '--listen', `0.0.0.0:${PORTS.controller}`, ...records, '--control-plane-url', `http://peat-control.${n.namespace}.svc:${PORTS.control}/hook`, '--timelines-onto-safekeepers', `--timeline-safekeeper-count=${r.spec.safekeepers.count}`, '--handle-ps-local-disk-loss', '--max-warming-up-interval=30s'],
          env: [...recordsEnv, secretEnv('PUBLIC_KEY', SECRETS.authPublic, 'auth_public_key.pem'), secretEnv('PAGESERVER_JWT_TOKEN', t, 'storcon_to_pageservers.jwt'), secretEnv('SAFEKEEPER_JWT_TOKEN', t, 'storcon_to_safekeepers.jwt'), secretEnv('CONTROL_PLANE_JWT_TOKEN', t, 'storcon_to_control_plane.jwt'), secretEnv('PEER_JWT_TOKEN', t, 'storcon_peer.jwt')],
          ports: [{ containerPort: PORTS.controller }],
          resources: { requests: { cpu: '50m', memory: r.spec.controller.memory.request }, limits: { memory: r.spec.controller.memory.limit } },
          readinessProbe: { httpGet: { path: '/status', port: PORTS.controller }, periodSeconds: 5 },
        }],
      }, kubernetes) } },
    },
  ]
}

/**
 * A pageserver, pinned to its machine (its data is there), its node id its place in the list. Two init containers: one, as root, makes the
 * directory the pageserver's (uid 1000); the other writes `identity.toml`, `pageserver.toml` and `metadata.json` (what the pageserver
 * registers itself with the controller from) and REFUSES when the directory already holds another node's identity. It restarts only when
 * its pod is deleted (OnDelete): an image change never restarts two at once, and an upgrade restarts each in turn, drained first.
 */
export function pageserver(r: Resolved, at: { id: number; node: string }, image = r.images.storage): Manifest {
  const n = r.names
  const id = at.id
  const labels = { app: 'pageserver', 'node-id': String(id) }
  const host = `pageserver-w${id}-0.pageserver.${n.namespace}.svc.cluster.local`
  const vol = r.spec.pageservers.storage.hostPath!
  const script = `set -eu
id=${id}
if [ -f /data/.neon/identity.toml ] && [ "$(cat /data/.neon/identity.toml)" != "id=$id" ]; then
  echo "refusing to start: this directory belongs to $(cat /data/.neon/identity.toml), not id=$id" >&2
  exit 1
fi
host="${host}"
echo "id=$id" > /data/.neon/identity.toml
cat > /data/.neon/pageserver.toml <<TOML
broker_endpoint='http://storage-broker:${PORTS.brokerGrpc}'
pg_distrib_dir='/usr/local/'
listen_pg_addr='0.0.0.0:${PORTS.pageserverPg}'
listen_http_addr='0.0.0.0:${PORTS.pageserverHttp}'
availability_zone='\${NODE_NAME}'
control_plane_api='http://storage-controller:${PORTS.controller}/upcall/v1/'
pg_auth_type='NeonJWT'
http_auth_type='NeonJWT'
auth_validation_public_key_path='/auth/auth_public_key.pem'
control_plane_api_token='$(cat /tok/pageserver_to_controller.jwt)'
remote_storage=${remoteStorage(r, r.store.pageservers)}
TOML
printf '{"host":"%s","http_host":"%s","http_port":${PORTS.pageserverHttp},"port":${PORTS.pageserverPg},"availability_zone_id":"%s"}' "$host" "$host" "$NODE_NAME" > /data/.neon/metadata.json
`
  return {
    apiVersion: 'apps/v1', kind: 'StatefulSet', metadata: meta(r, `pageserver-w${id}`, n.namespace),
    spec: {
      serviceName: 'pageserver', replicas: 1, updateStrategy: { type: 'OnDelete' }, selector: { matchLabels: labels },
      template: { metadata: { labels }, spec: platformPod(n, {
        nodeSelector: pinTo(at.node),
        initContainers: [
          { name: 'prep', image, securityContext: { runAsUser: 0 }, command: ['/bin/sh', '-c', 'set -eu; mkdir -p /vol/neon; chown 1000:1000 /vol/neon'], volumeMounts: [{ name: 'volume', mountPath: '/vol' }] },
          { name: 'config', image, env: [{ name: 'NODE_NAME', valueFrom: { fieldRef: { fieldPath: 'spec.nodeName' } } }], command: ['/bin/sh', '-c'], args: [script], volumeMounts: [{ name: 'volume', mountPath: '/data/.neon', subPath: 'neon' }, { name: 'tok', mountPath: '/tok' }] },
        ],
        containers: [{
          name: 'pageserver', image,
          env: [...storeEnv(r.store.pageservers.credentialsSecret), secretEnv('NEON_AUTH_TOKEN', SECRETS.pageserverTokens, 'pageserver_to_safekeepers.jwt')],
          command: ['/usr/local/bin/pageserver', '-D', '/data/.neon'],
          ports: [{ containerPort: PORTS.pageserverPg }, { containerPort: PORTS.pageserverHttp }],
          resources: { requests: { cpu: '500m', memory: r.spec.pageservers.memory.request }, limits: { memory: r.spec.pageservers.memory.limit } },
          readinessProbe: { httpGet: { path: '/v1/status', port: PORTS.pageserverHttp }, periodSeconds: 5 },
          volumeMounts: [{ name: 'volume', mountPath: '/data/.neon', subPath: 'neon' }, { name: 'auth', mountPath: '/auth' }],
        }],
        volumes: [
          { name: 'volume', hostPath: { path: vol.path, type: vol.create ? 'DirectoryOrCreate' : 'Directory' } },
          { name: 'auth', secret: { secretName: SECRETS.authPublic } },
          { name: 'tok', secret: { secretName: SECRETS.pageserverTokens } },
        ],
      }) },
    },
  }
}

/** What each storage piece may be called by and may call (the namespace's default-deny holds everything else). */
export function storagePolicies(r: Resolved): Manifest[] {
  const n = r.names, ns = n.namespace
  const inPeat = (app: string) => piece(ns, app)
  const computes = piece(n.computeNamespace, 'peat-compute')
  const P = PORTS
  const records = r.spec.controller.records === 'kubernetes' ? [API_SERVER, STORE(r)] : [{ to: [{ ipBlock: { cidr: '0.0.0.0/0' } }], ports: tcp(5432) }]
  return [
    policy(r, ns, 'storage-broker', [{ from: [inPeat('pageserver'), inPeat('safekeeper'), SCRAPER], ports: tcp(P.brokerGrpc) }], [DNS]),
    policy(r, ns, 'storage-controller', [{ from: [inPeat('pageserver'), inPeat('peat-control'), inPeat('peat-operator'), SCRAPER], ports: tcp(P.controller) }], [DNS, ...records, { to: [inPeat('pageserver')], ports: tcp(P.pageserverHttp) }, { to: [inPeat('safekeeper')], ports: tcp(P.safekeeperHttp) }, { to: [inPeat('peat-control')], ports: tcp(P.control) }]),
    policy(r, ns, 'pageserver', [{ from: [inPeat('storage-controller'), inPeat('peat-control'), SCRAPER], ports: tcp(P.pageserverHttp) }, { from: [computes], ports: tcp(P.pageserverPg) }], [DNS, { to: [inPeat('storage-broker')], ports: tcp(P.brokerGrpc) }, { to: [inPeat('safekeeper')], ports: tcp(P.safekeeperPg) }, { to: [inPeat('storage-controller')], ports: tcp(P.controller) }, STORE(r)]),
    policy(r, ns, 'safekeeper', [{ from: [computes, inPeat('pageserver')], ports: tcp(P.safekeeperPg) }, { from: [inPeat('storage-controller'), inPeat('peat-control'), SCRAPER], ports: tcp(P.safekeeperHttp) }, { from: [inPeat('safekeeper')], ports: tcp(P.safekeeperPg, P.safekeeperHttp) }], [DNS, { to: [inPeat('storage-broker')], ports: tcp(P.brokerGrpc) }, { to: [inPeat('peat-control')], ports: tcp(P.control) }, { to: [inPeat('safekeeper')], ports: tcp(P.safekeeperPg, P.safekeeperHttp) }, STORE(r)]),
  ]
}

export function storage(r: Resolved): Manifest[] {
  return [
    service(r, 'pageserver', 'pageserver', [{ name: 'pg', port: PORTS.pageserverPg }, { name: 'http', port: PORTS.pageserverHttp }], true),
    service(r, 'safekeeper', 'safekeeper', [{ name: 'pg', port: PORTS.safekeeperPg }, { name: 'http', port: PORTS.safekeeperHttp }], true),
    ...broker(r),
    ...controller(r),
    ...r.placements.pageservers.map((p) => pageserver(r, p)),
    ...storagePolicies(r),
  ]
}

// ---- the control layer

export const CONTROL_ACCOUNT = 'peat-control'

/** The control layer's settings, as its environment: the shared names, the images, the platform's sizes and its safekeepers. */
export function controlEnv(r: Resolved, images = r.images): { name: string; value?: string; valueFrom?: unknown }[] {
  const s = r.spec
  const n = r.names
  const outside = s.safekeepers.outside
  const issuer = s.certificates.issuer
  const sk = r.store.safekeepers
  return [
    ...toEnv(n),
    { name: 'FRONT_DOOR', value: frontDoor(n) },
    { name: 'COMPUTE_IMAGE', value: images.compute[18] },
    { name: 'COMPUTE_IMAGE_PG19', value: images.compute[19] },
    { name: 'STORAGE_IMAGE', value: images.storage },
    { name: 'DEFAULT_PG_VERSION', value: String(s.computes.defaultPgVersion) },
    { name: 'PAGESERVER_SECONDARIES', value: s.pageservers.count > 1 ? '1' : '0' },
    { name: 'DELETE_HOLD', value: s.databases.deleteHold },
    { name: 'IDLE_MINUTES', value: String(s.computes.idleMinutes) },
    { name: 'COMPUTE_MEMORY_REQUEST', value: s.computes.memory.request },
    { name: 'COMPUTE_MEMORY_LIMIT', value: s.computes.memory.limit },
    { name: 'PGDATA_LIMIT', value: s.computes.scratch },
    // a compute waits 10 s, not Neon's 150 s, on a pageserver that stopped answering, and a dial to a machine that lost power gives up after 5 s
    { name: 'PS_RESPONSE_DISCONNECT_MS', value: '10000' },
    { name: 'PS_CONN_OPTS', value: '?tcp_user_timeout=5000' },
    { name: 'HOME', value: '/tmp' },
    { name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } },
    { name: 'SPREAD', value: s.spread },
    { name: 'CERT_ISSUER', value: issuer ? `${issuer.kind}/${issuer.name}` : '' },
    { name: 'DB_DOMAIN', value: s.gateways.domain ?? '' },
    // the WAL trim's key (it may list and delete in the safekeepers' bucket), once an hour, every timeline
    ...storeEnv(r.store.walTrim),
    { name: 'S3_ENDPOINT', value: r.store.endpoint },
    { name: 'S3_BUCKET', value: sk.bucket },
    { name: 'S3_PREFIX', value: sk.prefix },
    { name: 'S3_REGION', value: r.store.region },
    { name: 'WAL_TRIM_EVERY_S', value: '3600' },
    // the safekeepers: the control layer makes the first ones when none exists and replaces a lost one after 30 minutes away
    { name: 'SAFEKEEPER_SEED', value: r.placements.safekeepers.map((m) => `${m.id}=${m.node}`).join(',') },
    { name: 'SK_GONE_S', value: '1800' },
    { name: 'SK_MIGRATE_PARALLEL', value: '16' },
    { name: 'SK_WAL_BACKUP_JOBS', value: '2' },
    { name: 'SAFEKEEPER_STORE_SECRET', value: sk.credentialsSecret },
    { name: 'SAFEKEEPER_VOLUME', value: s.safekeepers.storage.hostPath!.path },
    { name: 'SAFEKEEPER_VOLUME_TYPE', value: s.safekeepers.storage.hostPath!.create ? 'DirectoryOrCreate' : 'Directory' },
    { name: 'SAFEKEEPER_ZONE_LABEL', value: s.safekeepers.zoneLabel },
    { name: 'SAFEKEEPER_MEMORY_REQUEST', value: s.safekeepers.memory.request },
    { name: 'SAFEKEEPER_MEMORY_LIMIT', value: s.safekeepers.memory.limit },
    { name: 'KEEPER_MACHINES', value: (outside?.machines ?? []).join(',') },
    { name: 'KEEPER_LABEL', value: outside ? `${outside.label.key}=${outside.label.value}` : `${GROUP}/keeper=true` },
    { name: 'KEEPER_TOLERATIONS', value: JSON.stringify(outside?.tolerations ?? []) },
  ]
}

const role = (r: Resolved, namespace: string, rules: unknown[]): Manifest[] => [
  { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'Role', metadata: meta(r, CONTROL_ACCOUNT, namespace), rules },
  { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'RoleBinding', metadata: meta(r, CONTROL_ACCOUNT, namespace), roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'Role', name: CONTROL_ACCOUNT }, subjects: [{ kind: 'ServiceAccount', name: CONTROL_ACCOUNT, namespace: r.names.namespace }] },
]
const rule = (apiGroups: string[], resources: string[], verbs: string[]) => ({ apiGroups, resources, verbs })
const CRUD = ['get', 'list', 'create', 'update', 'patch', 'delete']

/**
 * What the control layer's account may do, and no more: in the platform namespace, read and delete pods, keep its records in ConfigMaps and
 * Secrets, make and remove safekeeper StatefulSets, read the keeper machines' prep Job and hold its Leases; in the compute namespace, a
 * database's compute (its Deployment, its spec and login Secrets, its Certificate); in the database namespace, the Services that give each
 * database its name; in the gateways' namespace, read and delete the gateways' pods (one on a dead machine is replaced at once); cluster-wide,
 * read Nodes and patch them (the dead taint, the one change the node guard allows, and only on a platform machine).
 */
export function controlAccount(r: Resolved): Manifest[] {
  const n = r.names
  return [
    { apiVersion: 'v1', kind: 'ServiceAccount', metadata: meta(r, CONTROL_ACCOUNT, n.namespace) },
    ...role(r, n.namespace, [rule([''], ['pods'], ['get', 'list', 'delete']), rule([''], ['secrets', 'configmaps'], CRUD), rule(['apps'], ['statefulsets'], ['get', 'list', 'create', 'update', 'delete']), rule(['batch'], ['jobs'], ['get']), rule(['coordination.k8s.io'], ['leases'], ['get', 'list', 'create', 'update', 'patch'])]),
    ...role(r, n.computeNamespace, [rule([''], ['pods'], ['get', 'list', 'delete']), rule([''], ['secrets'], CRUD), rule(['apps'], ['deployments'], CRUD), rule(['cert-manager.io'], ['certificates'], CRUD)]),
    ...role(r, n.dbNamespace, [rule([''], ['services'], ['get', 'list', 'create', 'update', 'delete'])]),
    ...role(r, n.gatewayNamespace, [rule([''], ['pods'], ['get', 'list', 'delete'])]),
    { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRole', metadata: meta(r, `${CONTROL_ACCOUNT}-nodes`), rules: [rule([''], ['nodes'], ['get', 'list', 'patch'])] },
    // the databases' records: the Database objects of every namespace, their status and their finalizer (the API makes one in the database namespace, or the namespace it is asked for)
    { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRole', metadata: meta(r, `${CONTROL_ACCOUNT}-databases`), rules: [rule([GROUP], ['databases'], ['get', 'list', 'watch', 'create', 'patch', 'update', 'delete']), rule([GROUP], ['databases/status'], ['get', 'patch', 'update'])] },
    { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRoleBinding', metadata: meta(r, `${CONTROL_ACCOUNT}-databases`), roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'ClusterRole', name: `${CONTROL_ACCOUNT}-databases` }, subjects: [{ kind: 'ServiceAccount', name: CONTROL_ACCOUNT, namespace: n.namespace }] },
    { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRoleBinding', metadata: meta(r, `${CONTROL_ACCOUNT}-nodes`), roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'ClusterRole', name: `${CONTROL_ACCOUNT}-nodes` }, subjects: [{ kind: 'ServiceAccount', name: CONTROL_ACCOUNT, namespace: n.namespace }] },
  ]
}

export function control(r: Resolved, images = r.images): Manifest[] {
  const n = r.names
  const labels = { app: 'peat-control' }
  const probe = { httpGet: { path: '/healthz', port: PORTS.control }, periodSeconds: 3 }
  const issuer = r.spec.certificates.issuer
  return [
    ...controlAccount(r),
    { apiVersion: 'v1', kind: 'Service', metadata: meta(r, 'peat-control', n.namespace), spec: { selector: labels, ports: [{ name: 'http', port: PORTS.control }] } },
    {
      apiVersion: 'apps/v1', kind: 'Deployment', metadata: meta(r, 'peat-control', n.namespace),
      spec: {
        replicas: r.spec.control.replicas, strategy: { type: 'RollingUpdate', rollingUpdate: { maxSurge: 0, maxUnavailable: 1 } }, selector: { matchLabels: labels },
        // `peat-ha` marks the pods the control layer replaces at once when their machine is declared dead
        template: { metadata: { labels: { ...labels, 'peat-ha': 'true' } }, spec: platformPod(n, {
          serviceAccountName: CONTROL_ACCOUNT,
          ...twoMachines(r.spec.spread, labels),
          tolerations: LEAVE_A_DEAD_MACHINE,
          securityContext: RESTRICTED_POD,
          containers: [{
            name: 'control', image: images.control, args: ['control'], env: controlEnv(r, images), ports: [{ containerPort: PORTS.control }],
            resources: { requests: { cpu: '20m', memory: r.spec.control.memory.request }, limits: { memory: r.spec.control.memory.limit } },
            securityContext: RESTRICTED_CONTAINER, readinessProbe: probe, livenessProbe: { ...probe, failureThreshold: 10 },
            volumeMounts: [{ name: 'key', mountPath: '/keys', readOnly: true }, ...(issuer ? [] : [{ name: 'ca', mountPath: '/ca', readOnly: true }]), { name: 'tmp', mountPath: '/tmp' }],
          }],
          volumes: [{ name: 'key', secret: { secretName: SECRETS.controlKey } }, ...(issuer ? [] : [{ name: 'ca', secret: { secretName: SECRETS.caKey } }]), { name: 'tmp', emptyDir: { sizeLimit: '64Mi' } }],
        }, true) },
      },
    },
    ...controlPolicies(r),
  ]
}

/** The control layer's own policy and the computes'. */
export function controlPolicies(r: Resolved): Manifest[] {
  const n = r.names, ns = n.namespace
  const inPeat = (app: string) => piece(ns, app)
  const computes = piece(n.computeNamespace, 'peat-compute')
  const gateways = piece(n.gatewayNamespace, 'peat-gateway')
  return [
    policy(r, ns, 'peat-control', [{ from: [inPeat('storage-controller'), inPeat('safekeeper'), gateways, computes, inPeat('peat-operator'), SCRAPER], ports: tcp(PORTS.control) }], [
      DNS, API_SERVER,
      { to: [inPeat('storage-controller')], ports: tcp(PORTS.controller) },
      { to: [inPeat('pageserver')], ports: tcp(PORTS.pageserverHttp) },
      { to: [inPeat('safekeeper')], ports: tcp(PORTS.safekeeperHttp) },
      { to: [computes], ports: tcp(3080, 5432) },
      // the kubelet port of every platform machine, the probe that tells a dead machine; and the store, for the WAL trim
      { to: [{ ipBlock: { cidr: '0.0.0.0/0' } }], ports: tcp(10250) },
      STORE(r),
    ]),
    policy(r, n.computeNamespace, 'peat-compute', [{ from: [gateways], ports: tcp(5432) }, { from: [inPeat('peat-control')], ports: tcp(3080, 5432) }],
      [DNS, { to: [inPeat('pageserver')], ports: tcp(PORTS.pageserverPg) }, { to: [inPeat('safekeeper')], ports: tcp(PORTS.safekeeperPg) }, { to: [inPeat('peat-control')], ports: tcp(PORTS.control) }]),
  ]
}

// ---- the gateways

export function gateways(r: Resolved, images = r.images): Manifest[] {
  const n = r.names
  const labels = { app: 'peat-gateway' }
  const probe = { httpGet: { path: '/health', port: PORTS.gatewayMetrics }, periodSeconds: 3 }
  const svc = r.spec.gateways.service
  return [
    { apiVersion: 'v1', kind: 'Service', metadata: { ...meta(r, 'pg-front', n.gatewayNamespace), ...(svc.annotations ? { annotations: svc.annotations } : {}) }, spec: { type: svc.type, selector: labels, ports: [{ name: 'pg', port: PORTS.gatewayPg, targetPort: PORTS.gatewayPg }] } },
    {
      apiVersion: 'apps/v1', kind: 'Deployment', metadata: meta(r, 'peat-gateway', n.gatewayNamespace),
      spec: {
        replicas: r.spec.gateways.count, strategy: { type: 'RollingUpdate', rollingUpdate: { maxSurge: 0, maxUnavailable: 1 } }, selector: { matchLabels: labels },
        template: { metadata: { labels: { ...labels, 'peat-ha': 'true' } }, spec: platformPod(n, {
          ...twoMachines(r.spec.spread, labels),
          tolerations: LEAVE_A_DEAD_MACHINE,
          terminationGracePeriodSeconds: 10,
          securityContext: RESTRICTED_POD,
          containers: [{
            name: 'gateway', image: images.control, args: ['gateway'],
            env: [...toEnv(n), { name: 'HOME', value: '/tmp' }, { name: 'IDLE_MINUTES', value: String(r.spec.computes.idleMinutes) }, { name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } }],
            ports: [{ name: 'pg', containerPort: PORTS.gatewayPg }, { name: 'metrics', containerPort: PORTS.gatewayMetrics }],
            resources: { requests: { cpu: '20m', memory: r.spec.gateways.memory.request }, limits: { memory: r.spec.gateways.memory.limit } },
            securityContext: RESTRICTED_CONTAINER, readinessProbe: probe, livenessProbe: { ...probe, failureThreshold: 10 },
            volumeMounts: [{ name: 'tmp', mountPath: '/tmp' }],
          }],
          volumes: [{ name: 'tmp', emptyDir: { sizeLimit: '16Mi' } }],
        }) },
      },
    },
    // In: the front door's 5432 from any pod (a client is a pod of the cluster or comes through the Service; the databases' own logins keep the wrong caller out: TLS and a password),
    // the other gateway's and a scraper's 9090. Out: DNS, the control layer, the computes' 5432 and the other gateway's 9090.
    policy(r, n.gatewayNamespace, 'peat-gateway', [{ from: [{ namespaceSelector: {} }, { ipBlock: { cidr: '0.0.0.0/0' } }], ports: tcp(PORTS.gatewayPg) }, { from: [SCRAPER, piece(n.gatewayNamespace, 'peat-gateway')], ports: tcp(PORTS.gatewayMetrics) }],
      [DNS, { to: [piece(n.namespace, 'peat-control')], ports: tcp(PORTS.control) }, { to: [piece(n.computeNamespace, 'peat-compute')], ports: tcp(5432) }, { to: [piece(n.gatewayNamespace, 'peat-gateway')], ports: tcp(PORTS.gatewayMetrics) }]),
  ]
}

// ---- a keeper machine's directory: the control layer makes a member's StatefulSet there, and cannot make the directory its hostPath names

export const KEEPER_PREP_JOB = 'keeper-prep'
export function keeperPrep(r: Resolved, image = r.images.storage): Manifest[] {
  const outside = r.spec.safekeepers.outside
  if (!outside?.machines?.length) return []
  const root = r.spec.safekeepers.storage.hostPath!.path
  const parent = root.replace(/\/[^/]+\/?$/, '') || '/'
  const dir = root.slice(parent.length).replace(/^\//, '')
  return [{
    apiVersion: 'batch/v1', kind: 'Job', metadata: meta(r, KEEPER_PREP_JOB, r.names.namespace),
    spec: { backoffLimit: 3, template: { metadata: { labels: { app: KEEPER_PREP_JOB } }, spec: {
      restartPolicy: 'OnFailure', automountServiceAccountToken: false, enableServiceLinks: false,
      nodeSelector: { [outside.label.key]: outside.label.value }, tolerations: outside.tolerations ?? [],
      containers: [{ name: 'mkdir', image, securityContext: { runAsUser: 0 }, command: ['/bin/sh', '-c', `set -eu; mkdir -p /vol/${dir}; chown 1000:1000 /vol/${dir}`], resources: { requests: { cpu: '10m', memory: '16Mi' }, limits: { memory: '64Mi' } }, volumeMounts: [{ name: 'parent', mountPath: '/vol' }] }],
      volumes: [{ name: 'parent', hostPath: { path: parent, type: 'Directory' } }],
    } } },
  }]
}

// ---- the guards

/**
 * Admission rules for the control layer's account. It may patch Nodes (to taint a dead machine) and make StatefulSets in a privileged
 * namespace and Services: verbs that, left alone, let a misread probe or a bug taint a machine that is not the platform's, make a pod that
 * mounts any host path, or capture traffic with a Service's external address. RBAC cannot say "only these nodes" or "only this shape"; an
 * admission rule can. Each judges that account's requests and no one else's, and fails closed for them.
 *   - Nodes: no create or delete; an update only of a node that carries the machine label, changing nothing but the one dead taint.
 *   - StatefulSets: only in the platform namespace, every container of the storage images admitted (`storageImages`: the release's, and
 *     the one before while an upgrade is under way), no host namespaces, nothing privileged or with added capabilities, no host port, only
 *     the default account, no unconfined seccomp profile, and a hostPath only on the safekeepers' volume: the safekeeper's shape.
 *   - Services: ClusterIP or ExternalName only, no external IPs, and an ExternalName only in the database namespace and only to the front door.
 */
export const NODE_POLICY = 'peat-control-keeps-to-peat-nodes'
export const STATEFULSET_POLICY = 'peat-control-statefulsets-keep-to-the-safekeeper'
export const SERVICE_POLICY = 'peat-control-services-stay-inside'
export const NODE_SPEC_FIELDS = ['configSource', 'externalID', 'podCIDR', 'podCIDRs', 'providerID', 'unschedulable'] as const

export function guards(r: Resolved, storageImages: string[] = [r.images.storage]): Manifest[] {
  const n = r.names
  const user = controlUser(n)
  const byControl = [{ name: 'by-the-control-layer', expression: `request.userInfo.username == '${user}'` }]
  const binding = (name: string): Manifest => ({ apiVersion: 'admissionregistration.k8s.io/v1', kind: 'ValidatingAdmissionPolicyBinding', metadata: meta(r, name), spec: { policyName: name, validationActions: ['Deny'] } })
  const unchanged = (path: string) => `(has(object.${path}) == has(oldObject.${path})) && (!has(object.${path}) || object.${path} == oldObject.${path})`
  const taints = (o: 'object' | 'oldObject') => `(variables.updating && has(${o}.spec.taints) ? dyn(${o}.spec.taints) : dyn([]))`
  const whenUpdating = (e: string) => `!variables.updating || (${e})`
  const dead = n.deadTaint
  const label = n.machineLabel
  const pod = 'object.spec.template.spec'
  const all = (list: string, e: string) => `(!has(${pod}.${list}) || ${pod}.${list}.all(c, ${e}))`
  const containers = (e: string) => `${all('containers', e)} && ${all('initContainers', e)}`
  const falseOrUnset = (path: string) => `(!has(${path}) || ${path} == false)`
  const skVolume = r.spec.safekeepers.storage.hostPath!.path
  const skVolumeTypes = r.spec.safekeepers.storage.hostPath!.create ? `['Directory', 'DirectoryOrCreate']` : `['Directory']`
  const front = frontDoor(n)
  return [
    {
      apiVersion: 'admissionregistration.k8s.io/v1', kind: 'ValidatingAdmissionPolicy', metadata: meta(r, NODE_POLICY),
      spec: {
        failurePolicy: 'Fail',
        matchConstraints: { resourceRules: [{ apiGroups: [''], apiVersions: ['v1'], operations: ['CREATE', 'UPDATE', 'DELETE'], resources: ['nodes'] }] },
        matchConditions: byControl,
        variables: [
          { name: 'updating', expression: "request.operation == 'UPDATE'" },
          { name: 'deadKey', expression: `'${dead.key}'` },
          { name: 'oldTaints', expression: taints('oldObject') },
          { name: 'newTaints', expression: taints('object') },
          { name: 'otherOldTaints', expression: 'variables.oldTaints.filter(t, t.key != variables.deadKey)' },
          { name: 'otherNewTaints', expression: 'variables.newTaints.filter(t, t.key != variables.deadKey)' },
          { name: 'deadTaints', expression: 'variables.newTaints.filter(t, t.key == variables.deadKey)' },
        ],
        validations: [
          { expression: 'variables.updating', message: "the control layer's account may only update a node's taints: it may not create or delete a node" },
          { expression: whenUpdating(`has(oldObject.metadata.labels) && '${label.key}' in oldObject.metadata.labels && oldObject.metadata.labels['${label.key}'] == '${label.value}'`), message: `the control layer's account may change only a node that carries the label ${label.key}=${label.value} (a platform machine)` },
          { expression: whenUpdating(NODE_SPEC_FIELDS.map((f) => `(${unchanged(`spec.${f}`)})`).join(' && ')), message: "the control layer's account may change a node's taints only, not any other field of its spec (it may not cordon a node)" },
          { expression: whenUpdating('variables.otherOldTaints == variables.otherNewTaints'), message: `the control layer's account may change only the taint ${dead.key}, not another taint of the node` },
          { expression: whenUpdating(`size(variables.deadTaints) <= 1 && variables.deadTaints.all(t, has(t.value) && t.value == '${dead.value}' && t.effect == '${dead.effect}')`), message: `the control layer's account may put the taint ${dead.key} on a node once, with the value ${dead.value} and the effect ${dead.effect}` },
          { expression: whenUpdating(unchanged('metadata.labels')), message: "the control layer's account may change a node's taints only, not its labels" },
          { expression: whenUpdating(unchanged('metadata.annotations')), message: "the control layer's account may change a node's taints only, not its annotations" },
          { expression: whenUpdating(unchanged('metadata.finalizers')), message: "the control layer's account may change a node's taints only, not its finalizers" },
          { expression: whenUpdating(unchanged('metadata.ownerReferences')), message: "the control layer's account may change a node's taints only, not its owner references" },
        ],
      },
    },
    binding(NODE_POLICY),
    {
      apiVersion: 'admissionregistration.k8s.io/v1', kind: 'ValidatingAdmissionPolicy', metadata: meta(r, STATEFULSET_POLICY),
      spec: {
        failurePolicy: 'Fail',
        matchConstraints: { resourceRules: [{ apiGroups: ['apps'], apiVersions: ['v1'], operations: ['CREATE', 'UPDATE'], resources: ['statefulsets'] }] },
        matchConditions: byControl,
        validations: [
          { expression: `object.metadata.namespace == '${n.namespace}'`, message: `the control layer's account may make a StatefulSet only in ${n.namespace} (a safekeeper)` },
          { expression: containers(`c.image in [${storageImages.map((i) => `'${i}'`).join(', ')}]`), message: "the control layer's account may make a StatefulSet only of the platform's storage image (the release's, or the one before it while an upgrade is under way)" },
          { expression: `${falseOrUnset(`${pod}.hostNetwork`)} && ${falseOrUnset(`${pod}.hostPID`)} && ${falseOrUnset(`${pod}.hostIPC`)}`, message: "the control layer's account may not make a StatefulSet that uses the host's network, PID or IPC namespace" },
          { expression: containers(`(!has(c.securityContext) || ${falseOrUnset('c.securityContext.privileged')})`), message: "the control layer's account may not make a StatefulSet with a privileged container" },
          { expression: containers('(!has(c.securityContext) || !has(c.securityContext.capabilities) || !has(c.securityContext.capabilities.add) || size(c.securityContext.capabilities.add) == 0)'), message: "the control layer's account may not make a StatefulSet that adds Linux capabilities" },
          { expression: `(!has(${pod}.volumes) || ${pod}.volumes.all(v, has(v.hostPath) || has(v.secret) || has(v.emptyDir) || has(v.configMap) || has(v.projected)))`, message: "the control layer's account may mount a hostPath, secret, emptyDir, configMap or projected volume only (no network or inline CSI volume)" },
          { expression: containers('(!has(c.ports) || c.ports.all(p, !has(p.hostPort) || p.hostPort == 0))'), message: "the control layer's account may not make a StatefulSet that opens a host port" },
          { expression: `(!has(${pod}.serviceAccountName) || ${pod}.serviceAccountName == 'default')`, message: "the control layer's account may make a StatefulSet that runs as the default account only" },
          { expression: `(!has(${pod}.securityContext) || !has(${pod}.securityContext.seccompProfile) || ${pod}.securityContext.seccompProfile.type != 'Unconfined') && ${containers("(!has(c.securityContext) || !has(c.securityContext.seccompProfile) || c.securityContext.seccompProfile.type != 'Unconfined')")}`, message: "the control layer's account may not make a StatefulSet with an unconfined seccomp profile" },
          { expression: `(!has(${pod}.volumes) || ${pod}.volumes.all(v, !has(v.hostPath) || (v.hostPath.path == '${skVolume}' && has(v.hostPath.type) && v.hostPath.type in ${skVolumeTypes})))`, message: `the control layer's account may mount a host path only on ${skVolume}` },
        ],
      },
    },
    binding(STATEFULSET_POLICY),
    {
      apiVersion: 'admissionregistration.k8s.io/v1', kind: 'ValidatingAdmissionPolicy', metadata: meta(r, SERVICE_POLICY),
      spec: {
        failurePolicy: 'Fail',
        matchConstraints: { resourceRules: [{ apiGroups: [''], apiVersions: ['v1'], operations: ['CREATE', 'UPDATE'], resources: ['services'] }] },
        matchConditions: byControl,
        variables: [{ name: 'type', expression: "has(object.spec.type) ? object.spec.type : 'ClusterIP'" }],
        validations: [
          { expression: "variables.type in ['ClusterIP', 'ExternalName']", message: "the control layer's account may make a Service of type ClusterIP or ExternalName only (not NodePort or LoadBalancer)" },
          { expression: '!has(object.spec.externalIPs) || size(object.spec.externalIPs) == 0', message: "the control layer's account may not give a Service external IPs" },
          { expression: `variables.type != 'ExternalName' || (object.metadata.namespace == '${n.dbNamespace}' && has(object.spec.externalName) && object.spec.externalName == '${front}')`, message: `the control layer's account may make an ExternalName Service only in ${n.dbNamespace}, and only to ${front}` },
          { expression: `object.metadata.namespace != '${n.dbNamespace}' || variables.type == 'ExternalName'`, message: `in ${n.dbNamespace} the control layer's account may make ExternalName Services only` },
        ],
      },
    },
    binding(SERVICE_POLICY),
  ]
}

/** Every object of the platform, in the order it is applied: the base first, the guards before the account they hold. */
export function render(r: Resolved, kept: Record<string, string>, storageImages?: string[]): Manifest[] {
  return [...base(r), ...keySecrets(r, kept), ...guards(r, storageImages), ...storage(r), ...keeperPrep(r), ...control(r), ...gateways(r)]
}

export { SAFEKEEPER_RESOURCES }
