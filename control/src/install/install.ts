// The install file: what a user applies once (`kubectl apply --server-side -f peat.yaml`) before declaring a Platform. It holds Peat's two
// object kinds, the storage controller's record kinds (when records are kept in Kubernetes), the operator's namespace, its account and
// what that account may do, and the operator's Deployment, which runs the control image of the release.
//
//   bun src/install/install.ts --image ghcr.io/zabaca/peat/control:<tag> [--out peat.yaml]
//
// The operator makes every other piece (namespaces, accounts, Roles, admission rules, the storage, the control layer, the gateways), so its
// account may make those kinds, and may grant (`bind`, `escalate`) the Roles it gives the pieces' accounts. That is the account of a
// cluster administrator in all but name; the guards it installs hold the control layer, the platform's least trusted piece with node rights.
import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { crds } from '../api/crds'
import { GROUP } from '../names'
import { OPERATOR_NAMESPACE } from '../operator/render'

export { OPERATOR_NAMESPACE }

export const OPERATOR_ACCOUNT = 'peat-operator'
const RECORDS_CRDS = join(import.meta.dir, '../../../storage_controller/kubernetes/crds.yaml')
const labels = { 'app.kubernetes.io/name': 'peat', 'app.kubernetes.io/component': 'operator' }
const ALL = ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete']

/** The storage controller's record kinds, from the controller's own contract (storage_controller/kubernetes/crds.yaml), when it is in the tree. */
export function recordsCrds(path = RECORDS_CRDS): Record<string, unknown>[] {
  if (!existsSync(path)) return []
  const docs = readFileSync(path, 'utf8').split(/^---\s*$/m).map((d) => Bun.YAML.parse(d) as any).filter((d) => d && d.kind === 'CustomResourceDefinition')
  return docs
}

export function installManifests(image: string): Record<string, unknown>[] {
  const meta = (name: string, namespace?: string) => ({ name, ...(namespace ? { namespace } : {}), labels })
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
        { apiGroups: [''], resources: ['namespaces', 'configmaps', 'secrets', 'services', 'serviceaccounts', 'pods', 'persistentvolumeclaims'], verbs: ALL },
        { apiGroups: [''], resources: ['nodes'], verbs: ['get', 'list', 'watch', 'patch'] },
        { apiGroups: [''], resources: ['events'], verbs: ['create', 'patch'] },
        { apiGroups: ['apps'], resources: ['deployments', 'statefulsets'], verbs: ALL },
        { apiGroups: ['batch'], resources: ['jobs'], verbs: ALL },
        { apiGroups: ['networking.k8s.io'], resources: ['networkpolicies'], verbs: ALL },
        { apiGroups: ['coordination.k8s.io'], resources: ['leases'], verbs: ALL },
        { apiGroups: ['cert-manager.io'], resources: ['certificates'], verbs: ALL },
        { apiGroups: ['rbac.authorization.k8s.io'], resources: ['roles', 'rolebindings', 'clusterroles', 'clusterrolebindings'], verbs: [...ALL, 'bind', 'escalate'] },
        { apiGroups: ['admissionregistration.k8s.io'], resources: ['validatingadmissionpolicies', 'validatingadmissionpolicybindings'], verbs: ALL },
      ],
    },
    { apiVersion: 'rbac.authorization.k8s.io/v1', kind: 'ClusterRoleBinding', metadata: meta(OPERATOR_ACCOUNT), roleRef: { apiGroup: 'rbac.authorization.k8s.io', kind: 'ClusterRole', name: OPERATOR_ACCOUNT }, subjects: [{ kind: 'ServiceAccount', name: OPERATOR_ACCOUNT, namespace: OPERATOR_NAMESPACE }] },
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
  if (!image) { console.error('usage: install.ts --image <control image> [--out <file>]'); process.exit(2) }
  const text = `# Peat's install file. Apply it once, then declare a Platform (see control/examples/).\n${toYaml(installManifests(image))}`
  const out = arg('--out')
  if (out) writeFileSync(out, text)
  else process.stdout.write(text)
}
