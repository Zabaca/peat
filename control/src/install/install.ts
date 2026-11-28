// The install file: what a user applies once (`kubectl apply --server-side -f peat.yaml`) before declaring a Platform. It holds Peat's two
// object kinds, the storage controller's record kinds (when records are kept in Kubernetes), the operator's namespace, its account and
// what that account may do, and the operator's Deployment, which runs the control image of the release.
//
//   bun src/install/install.ts --image ghcr.io/zabaca/peat/control:<tag> [--out peat.yaml]
//
// The operator makes every other piece (namespaces, accounts, Roles, admission rules, the storage, the control layer, the gateways), so its
// account may make those kinds. It holds every rule it grants the pieces' accounts, so it needs neither `bind` nor `escalate`. Narrowed
// where a default install allows: it deletes no namespace; on Secrets it has get, create and patch cluster-wide (each database's login,
// in the database's own namespace) and every other right only in the platform's four namespaces (bound there by this file, `--namespaces`
// when the Platform names its own); exec only in the compute namespace, to grant it; and on admission policies only those it makes, by
// name. The guards it installs hold the control layer, the platform's piece with node rights.
import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { crds } from '../api/crds'
import { DEFAULT_NAMES, GROUP } from '../names'
import { COMPUTE_POLICY, EXEC_SHAPE_POLICY, NODE_POLICY, OPERATOR_NAMESPACE, SERVICE_POLICY, STATEFULSET_POLICY } from '../operator/render'

export const OPERATOR_ACCOUNT = 'peat-operator'
export const OPERATOR_EXEC_POLICY = 'peat-operator-runs-no-exec'
const RECORDS_CRDS = join(import.meta.dir, '../../../storage_controller/kubernetes/crds.yaml')
const labels = { 'app.kubernetes.io/name': 'peat', 'app.kubernetes.io/component': 'operator' }
const ALL = ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete']

/** The storage controller's record kinds, from the controller's own contract (storage_controller/kubernetes/crds.yaml), when it is in the tree. */
export function recordsCrds(path = RECORDS_CRDS): Record<string, unknown>[] {
  if (!existsSync(path)) return []
  const docs = readFileSync(path, 'utf8').split(/^---\s*$/m).map((d) => Bun.YAML.parse(d) as any).filter((d) => d && d.kind === 'CustomResourceDefinition')
  return docs
}

/** The platform's four namespaces, as the Platform will name them (the defaults unless the install says otherwise). */
export interface InstallNamespaces { platform: string; computes: string; gateways: string; databases: string }
export const DEFAULT_INSTALL_NAMESPACES: InstallNamespaces = { platform: DEFAULT_NAMES.namespace, computes: DEFAULT_NAMES.computeNamespace, gateways: DEFAULT_NAMES.gatewayNamespace, databases: DEFAULT_NAMES.dbNamespace }
/** The admission policies the operator makes (the guards it renders), and those an earlier release made that it removes: the only ones it may change. */
export const OPERATOR_POLICIES = [NODE_POLICY, STATEFULSET_POLICY, SERVICE_POLICY, COMPUTE_POLICY, EXEC_SHAPE_POLICY, 'peat-control-exec-only-for-a-move']
export const SECRETS_ROLE = 'peat-operator-secrets'
export const EXEC_ROLE = 'peat-operator-exec'

export function installManifests(image: string, ns: InstallNamespaces = DEFAULT_INSTALL_NAMESPACES): Record<string, unknown>[] {
  const meta = (name: string, namespace?: string) => ({ name, ...(namespace ? { namespace } : {}), labels })
  const subject = { kind: 'ServiceAccount', name: OPERATOR_ACCOUNT, namespace: OPERATOR_NAMESPACE }
  const platformNamespaces = [ns.platform, ns.computes, ns.gateways, ns.databases]
  return [
    ...crds(),
    ...recordsCrds(),
    { apiVersion: 'v1', kind: 'Namespace', metadata: { ...meta(OPERATOR_NAMESPACE), labels: { ...labels, 'pod-security.kubernetes.io/enforce': 'restricted' } } },
    { apiVersion: 'v1', kind: 'ServiceAccount', metadata: meta(OPERATOR_ACCOUNT, OPERATOR_NAMESPACE) },
    {
      apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRole', metadata: meta(OPERATOR_ACCOUNT),
      rules: [
        { apiGroups: [GROUP], resources: ['platforms', 'databases'], verbs: ALL },
        { apiGroups: [GROUP], resources: ['platforms/status', 'databases/status'], verbs: ['get', 'update', 'patch'] },
        { apiGroups: [`storagecontroller.${GROUP}`], resources: ['*'], verbs: ALL },
        // namespaces: made, never deleted (what is in one may not be the platform's)
        { apiGroups: [''], resources: ['namespaces'], verbs: ['get', 'list', 'watch', 'create', 'update', 'patch'] },
        { apiGroups: [''], resources: ['configmaps', 'services', 'serviceaccounts'], verbs: ALL },
        // Secrets anywhere: read, made and patched only (each database's login, published in its own namespace and removed with it by its
        // owner reference); every other right on Secrets is a Role's in the platform's own namespaces (below)
        { apiGroups: [''], resources: ['secrets'], verbs: ['get', 'create', 'patch'] },
        // pods: read, and deleted (an upgrade restarts a pageserver by deleting its pod); the operator makes none. patch on nodes: the control layer's grant, held so it can be given
        { apiGroups: [''], resources: ['pods'], verbs: ['get', 'list', 'watch', 'delete'] },
        { apiGroups: [''], resources: ['nodes'], verbs: ['get', 'list', 'watch', 'patch'] },
        { apiGroups: ['apps'], resources: ['deployments', 'statefulsets'], verbs: ALL },
        { apiGroups: ['batch'], resources: ['jobs'], verbs: ALL },
        { apiGroups: ['networking.k8s.io'], resources: ['networkpolicies'], verbs: ALL },
        { apiGroups: ['coordination.k8s.io'], resources: ['leases'], verbs: ALL },
        { apiGroups: ['cert-manager.io'], resources: ['certificates'], verbs: ALL },
        { apiGroups: ['rbac.authorization.k8s.io'], resources: ['roles', 'rolebindings', 'clusterroles', 'clusterrolebindings'], verbs: ALL },
        // admission policies: made (a create cannot name its object), then read, changed and removed only by the names of its own
        { apiGroups: ['admissionregistration.k8s.io'], resources: ['validatingadmissionpolicies', 'validatingadmissionpolicybindings'], verbs: ['create'] },
        { apiGroups: ['admissionregistration.k8s.io'], resources: ['validatingadmissionpolicies', 'validatingadmissionpolicybindings'], resourceNames: OPERATOR_POLICIES, verbs: ['get', 'update', 'patch', 'delete'] },
      ],
    },
    // the platform's namespaces, made here so that the operator's rights in them can be bound before its first pass (it then labels them)
    ...platformNamespaces.map((n) => ({ apiVersion: 'v1', kind: 'Namespace', metadata: meta(n) })),
    // every right on Secrets, in the platform's own namespaces only: its keys, the pieces' credentials, and what it grants the control layer
    { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRole', metadata: meta(SECRETS_ROLE), rules: [{ apiGroups: [''], resources: ['secrets'], verbs: ALL }] },
    ...platformNamespaces.map((n) => ({ apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'RoleBinding', metadata: meta(SECRETS_ROLE, n), roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'ClusterRole', name: SECRETS_ROLE }, subjects: [subject] })),
    // exec, in the compute namespace only: held so that a move's grant to the control layer can be given; the operator runs none (below)
    { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRole', metadata: meta(EXEC_ROLE), rules: [{ apiGroups: [''], resources: ['pods/exec'], verbs: ['create', 'get'] }] },
    { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'RoleBinding', metadata: meta(EXEC_ROLE, ns.computes), roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'ClusterRole', name: EXEC_ROLE }, subjects: [subject] },
    { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRoleBinding', metadata: meta(OPERATOR_ACCOUNT), roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'ClusterRole', name: OPERATOR_ACCOUNT }, subjects: [{ kind: 'ServiceAccount', name: OPERATOR_ACCOUNT, namespace: OPERATOR_NAMESPACE }] },
    // The operator holds exec only to grant it to the control layer (the API server refuses a grant of a right the granter does not hold,
    // and does not look at admission rules), and runs none: this rule refuses every exec and attach by its account, in any namespace.
    {
      apiVersion: 'admissionregistration.k8s.io/v1', kind: 'ValidatingAdmissionPolicy', metadata: meta(OPERATOR_EXEC_POLICY),
      spec: {
        failurePolicy: 'Fail',
        matchConstraints: { resourceRules: [{ apiGroups: [''], apiVersions: ['v1'], operations: ['CONNECT'], resources: ['pods/exec', 'pods/attach'] }] },
        matchConditions: [{ name: 'by-the-operator', expression: `request.userInfo.username == 'system:serviceaccount:${OPERATOR_NAMESPACE}:${OPERATOR_ACCOUNT}'` }],
        validations: [{ expression: 'false', message: "the operator's account runs no exec and attaches to no pod: it holds pods/exec only to grant it to the control layer" }],
      },
    },
    { apiVersion: 'admissionregistration.k8s.io/v1', kind: 'ValidatingAdmissionPolicyBinding', metadata: meta(OPERATOR_EXEC_POLICY), spec: { policyName: OPERATOR_EXEC_POLICY, validationActions: ['Deny'] } },
    {
      apiVersion: 'apps/v1', kind: 'Deployment', metadata: meta(OPERATOR_ACCOUNT, OPERATOR_NAMESPACE),
      spec: {
        replicas: 1, strategy: { type: 'Recreate' }, selector: { matchLabels: { app: 'peat-operator' } },
        template: { metadata: { labels: { app: 'peat-operator' } }, spec: {
          serviceAccountName: OPERATOR_ACCOUNT, enableServiceLinks: false,
          securityContext: { runAsNonRoot: true, runAsUser: 1000, runAsGroup: 1000, seccompProfile: { type: 'RuntimeDefault' } },
          containers: [{
            name: 'operator', image, args: ['operator'],
            env: [{ name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } }, { name: 'HOME', value: '/tmp' }],
            ports: [{ name: 'metrics', containerPort: 8081 }],
            resources: { requests: { cpu: '20m', memory: '64Mi' }, limits: { memory: '256Mi' } },
            securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ['ALL'] } },
            readinessProbe: { httpGet: { path: '/healthz', port: 8081 }, periodSeconds: 5 },
            volumeMounts: [{ name: 'tmp', mountPath: '/tmp' }],
          }],
          volumes: [{ name: 'tmp', emptyDir: { sizeLimit: '16Mi' } }],
        } },
      },
    },
  ]
}

export const toYaml = (docs: Record<string, unknown>[]): string => docs.map((d) => `---\n${Bun.YAML.stringify(d, null, 2)}`).join('\n') + '\n'

if (import.meta.main) {
  const arg = (name: string) => { const i = process.argv.indexOf(name); return i > 0 ? process.argv[i + 1] : undefined }
  const image = arg('--image')
  if (!image) { console.error('usage: install.ts --image <control image> [--namespaces <platform>,<computes>,<gateways>,<databases>] [--out <file>]'); process.exit(2) }
  // the Platform's namespaces, when it names its own (its spec.namespaces): the operator's rights on Secrets and exec are bound in them
  const given = arg('--namespaces')?.split(',')
  if (given && given.length !== 4) { console.error('--namespaces is four names: <platform>,<computes>,<gateways>,<databases>'); process.exit(2) }
  const ns = given ? { platform: given[0]!, computes: given[1]!, gateways: given[2]!, databases: given[3]! } : DEFAULT_INSTALL_NAMESPACES
  const text = `# Peat's install file. Apply it once, then declare a Platform (see control/examples/).\n${toYaml(installManifests(image, ns))}`
  const out = arg('--out')
  if (out) writeFileSync(out, text)
  else process.stdout.write(text)
}
