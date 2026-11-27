import { describe, expect, test } from 'bun:test'
import type { Platform } from '../src/api/v1alpha1'
import { controlUser, frontDoor } from '../src/names'
import { AUTH_PRIVATE, makeMissing } from '../src/operator/keys'
import { storageOf } from '../src/operator/storage'
import { choosePlacements, resolve, specOf, type Resolved } from '../src/operator/platform'
import { COMPUTE_POLICY, CONTROL_ACCOUNT, EXEC_SHAPE_POLICY, NODE_POLICY, SECRETS, SERVICE_POLICY, STATEFULSET_POLICY, moveExecGrant, render, type Manifest } from '../src/operator/render'

// What the operator renders for a platform: where each pod runs, what it mounts and reads, what each account may do, what may reach each
// pod, and the admission rules that hold the control layer's account. Read from the objects, as the API server would receive them.
// The guards' expressions are judged here as declared only; the install test applies them to a real API server.

const MACHINES = ['machine-1', 'machine-2', 'machine-3']
const KEPT = makeMissing({})

function resolved(spec: Record<string, unknown> = {}, machines = MACHINES): Resolved {
  const p = {
    apiVersion: 'peat.zabaca.com/v1alpha1', kind: 'Platform', metadata: { name: 'peat' },
    spec: { release: 'release-2026-12-01', objectStore: { endpoint: 'https://s3.example.com', bucket: 'peat-data', credentialsSecret: 'peat-store' }, ...spec },
  } as unknown as Platform
  const checked = specOf(p)
  if ('errors' in checked) throw new Error(checked.errors.join('; '))
  const chosen = choosePlacements(checked.spec, machines)
  if ('errors' in chosen) throw new Error(chosen.errors.join('; '))
  return resolve(p, checked.spec, chosen.placements)
}

const r = resolved()
const all = render(r, KEPT)
const trial = resolved({ spread: 'none', pageservers: { count: 1 }, gateways: { count: 1 }, control: { replicas: 1 } }, ['machine-1'])
const trialAll = render(trial, KEPT)
const tainted = resolved({ machines: { label: { key: 'peat.zabaca.com/machine', value: 'true' }, taint: { key: 'example.com/dedicated', value: 'peat', effect: 'NoSchedule' } } })
const taintedAll = render(tainted, KEPT)

const workloads = (ms: Manifest[]) => ms.filter((m) => ['Deployment', 'StatefulSet', 'Job'].includes(m.kind))
const podOf = (m: Manifest) => m.spec.template.spec
const named = (ms: Manifest[], kind: string, name: string, namespace?: string) => ms.find((m) => m.kind === kind && m.metadata.name === name && (!namespace || m.metadata.namespace === namespace))!
const containersOf = (m: Manifest): any[] => [...(podOf(m).initContainers ?? []), ...podOf(m).containers]
const nodeOf = (m: Manifest) => podOf(m).nodeSelector?.['kubernetes.io/hostname']

describe('where the pods run', () => {
  test('every pod but the keeper machines\' prep selects the platform\'s machine label', () => {
    const ws = workloads(all)
    expect(ws.length).toBeGreaterThan(5)
    for (const m of ws) expect([m.metadata.name, podOf(m).nodeSelector?.['peat.zabaca.com/machine']]).toEqual([m.metadata.name, 'true'])
  })
  test('the machines\' taint, when one is set, is tolerated by every pod; none is tolerated when none is set', () => {
    for (const m of workloads(taintedAll)) expect([m.metadata.name, (podOf(m).tolerations as any[]).some((t) => t.key === 'example.com/dedicated' && t.value === 'peat' && t.effect === 'NoSchedule')]).toEqual([m.metadata.name, true])
    for (const m of workloads(all)) expect((podOf(m).tolerations ?? []).some((t: any) => t.key === 'example.com/dedicated')).toBe(false)
  })
  test('spread machines: two pageservers on two machines, the controller and the broker on the third, never beside a pageserver', () => {
    const ps = all.filter((m) => m.kind === 'StatefulSet' && m.metadata.name.startsWith('pageserver-w'))
    expect(ps.map((m) => m.metadata.name).sort()).toEqual(['pageserver-w1', 'pageserver-w2'])
    const psNodes = ps.map(nodeOf)
    expect(new Set(psNodes).size).toBe(2)
    const ctl = nodeOf(named(all, 'Deployment', 'storage-controller'))
    expect(psNodes).not.toContain(ctl)
    expect(nodeOf(named(all, 'Deployment', 'storage-broker'))).toBe(ctl)
    expect(MACHINES).toContain(ctl)
  })
  test('the control layer and the gateways: replicas as declared, on different machines (required anti-affinity), and they leave a dead machine after 5 s', () => {
    for (const [name, ns] of [['peat-control', 'peat'], ['peat-gateway', 'peat-gateway']] as const) {
      const d = named(all, 'Deployment', name, ns)
      expect(d.spec.replicas).toBe(2)
      expect(podOf(d).affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution[0].topologyKey).toBe('kubernetes.io/hostname')
      expect((podOf(d).tolerations as any[]).filter((t) => t.tolerationSeconds === 5).map((t) => t.key).sort()).toEqual(['node.kubernetes.io/not-ready', 'node.kubernetes.io/unreachable'])
    }
  })
  test('spread none: every pinned piece on the one machine, one replica each, no anti-affinity', () => {
    for (const m of workloads(trialAll)) if (nodeOf(m)) expect(nodeOf(m)).toBe('machine-1')
    for (const name of ['peat-control', 'peat-gateway']) {
      const d = trialAll.find((m) => m.kind === 'Deployment' && m.metadata.name === name)!
      expect(d.spec.replicas).toBe(1)
      expect(podOf(d).affinity).toBeUndefined()
    }
  })
  test('every object is in one of the platform\'s four namespaces, or cluster-scoped', () => {
    const nss = new Set(['peat', 'peat-compute', 'peat-gateway', 'peat-db'])
    for (const m of all) if (m.metadata.namespace) expect([m.kind, m.metadata.name, nss.has(m.metadata.namespace)]).toEqual([m.kind, m.metadata.name, true])
    expect(all.filter((m) => !m.metadata.namespace).map((m) => m.kind).sort()).toEqual([
      'ClusterRole', 'ClusterRole', 'ClusterRoleBinding', 'ClusterRoleBinding', 'Namespace', 'Namespace', 'Namespace', 'Namespace',
      ...Array(5).fill('ValidatingAdmissionPolicy'), ...Array(5).fill('ValidatingAdmissionPolicyBinding'),
    ])
  })
  test('the namespaces\' pod security: the platform\'s privileged (host directories), the computes\' baseline, the gateways\' and the databases\' restricted', () => {
    const level = (n: string) => named(all, 'Namespace', n).metadata.labels!['pod-security.kubernetes.io/enforce']
    expect([level('peat'), level('peat-compute'), level('peat-gateway'), level('peat-db')]).toEqual(['privileged', 'baseline', 'restricted', 'restricted'])
  })
})

describe('volumes', () => {
  test('every hostPath is of type Directory unless the Platform asks for create (then unchecked, made by the runtime on the machine): a volume that did not provision fails the pod', () => {
    const paths = (ms: Manifest[]) => workloads(ms).flatMap((m) => (podOf(m).volumes ?? []).filter((v: any) => v.hostPath).map((v: any) => v.hostPath))
    expect(paths(all).length).toBeGreaterThan(0)
    for (const h of paths(all)) expect(h.type).toBe('Directory')
    const made = paths(render(resolved({ pageservers: { storage: { hostPath: { path: '/var/lib/peat/pageserver', create: true } } } }), KEPT)).filter((h) => h.path === '/var/lib/peat/pageserver')
    expect(made.map((h) => h.type)).toEqual(['', ''])
  })
  test('the pageserver\'s data is a directory of its own in the machine\'s volume, made by a root init container, refused when it holds another node\'s identity', () => {
    const ps = named(all, 'StatefulSet', 'pageserver-w1')
    expect(podOf(ps).initContainers[0]).toMatchObject({ name: 'prep', securityContext: { runAsUser: 0 } })
    expect(podOf(ps).initContainers[1].args[0]).toContain('refusing to start: this directory belongs to')
    // the machine's volume mounted as itself and its folder `neon` addressed inside: never a subPath, which Talos's kubelet resolves in its own view
    for (const c of [...podOf(ps).initContainers, ...podOf(ps).containers]) for (const m of c.volumeMounts) expect(m.subPath).toBeUndefined()
    expect(podOf(ps).containers[0].volumeMounts).toContainEqual({ name: 'volume', mountPath: '/vol' })
    // a wiped folder under a running pageserver: the liveness probe fails, and the restart runs the same config script (the identity guard
    // with it) before the pageserver, which makes the folder again (the volume's root is the pageserver's)
    const main = podOf(ps).containers[0]
    expect(main.command[0]).toBe('/bin/sh')
    expect(main.command[2]).toBe(`${podOf(ps).initContainers[1].args[0]}exec /usr/local/bin/pageserver -D /vol/neon\n`)
    expect(main.command[2]).toStartWith('set -eu\nid=1\nmkdir -p /vol/neon\n')
    expect(main.livenessProbe).toEqual({ exec: { command: ['/bin/sh', '-c', 'test -f /vol/neon/identity.toml'] }, periodSeconds: 10, failureThreshold: 2 })
    expect(main.volumeMounts).toContainEqual({ name: 'tok', mountPath: '/tok' })
    expect(main.env).toContainEqual({ name: 'NODE_NAME', valueFrom: { fieldRef: { fieldPath: 'spec.nodeName' } } })
    expect(podOf(ps).initContainers[0].command[2]).toBe('set -eu; chown 1000:1000 /vol; mkdir -p /vol/neon; chown 1000:1000 /vol/neon')
    expect(podOf(ps).initContainers[1].args[0]).toContain('echo "id=$id" > /vol/neon/identity.toml')
    expect(podOf(ps).initContainers[1].args[0]).not.toContain('/data')
  })
  test('no workload of the render mounts a host directory through subPath', () => {
    for (const m of workloads(all)) {
      const pod = podOf(m)
      const host = new Set((pod.volumes ?? []).filter((v: any) => v.hostPath).map((v: any) => v.name))
      for (const c of [...(pod.initContainers ?? []), ...(pod.containers ?? [])]) for (const v of c.volumeMounts ?? []) if (host.has(v.name)) expect(v.subPath, `${m.metadata.name}/${c.name}`).toBeUndefined()
    }
  })
  test('a compute\'s scratch is capped: the setting reaches the control layer', () => {
    const env = Object.fromEntries((podOf(named(all, 'Deployment', 'peat-control')).containers[0].env as any[]).filter((e) => 'value' in e).map((e) => [e.name, e.value]))
    expect(env['PGDATA_LIMIT']).toBe('2Gi')
  })
})

describe('secrets', () => {
  const made = new Set(all.filter((m) => m.kind === 'Secret').map((m) => `${m.metadata.namespace}/${m.metadata.name}`))
  test('no rendered Secret is made twice', () => {
    const list = all.filter((m) => m.kind === 'Secret').map((m) => `${m.metadata.namespace}/${m.metadata.name}`)
    expect(list.length).toBe(new Set(list).size)
  })
  test('every Secret a pod reads (env, volume) is one the renderer makes, or the store\'s or the records\' the Platform names', () => {
    const withPostgres = resolved({ controller: { records: 'postgres', postgres: { urlSecret: 'records-db' } } })
    for (const [res, ms] of [[r, all], [withPostgres, render(withPostgres, KEPT)]] as const) {
      const ok = new Set([...ms.filter((m) => m.kind === 'Secret').map((m) => `${m.metadata.namespace}/${m.metadata.name}`), `peat/${res.store.pageservers.credentialsSecret}`, ...(res.spec.controller.postgres ? [`peat/${res.spec.controller.postgres.urlSecret}`] : [])])
      for (const m of workloads(ms)) {
        const ns = m.metadata.namespace
        const reads = [
          ...containersOf(m).flatMap((c) => (c.env ?? []).map((e: any) => e.valueFrom?.secretKeyRef?.name).filter(Boolean)),
          ...(podOf(m).volumes ?? []).map((v: any) => v.secret?.secretName).filter(Boolean),
        ]
        for (const s of reads) expect([m.metadata.name, `${ns}/${s}`, ok.has(`${ns}/${s}`)]).toEqual([m.metadata.name, `${ns}/${s}`, true])
      }
    }
  })
  test('the signing key (the private half) reaches the control layer alone; the authority\'s key reaches it in its own namespace', () => {
    const holders = workloads(all).filter((m) => (podOf(m).volumes ?? []).some((v: any) => v.secret?.secretName === SECRETS.controlKey) || containersOf(m).some((c) => (c.env ?? []).some((e: any) => e.valueFrom?.secretKeyRef?.name === SECRETS.controlKey)))
    expect(holders.map((m) => m.metadata.name)).toEqual(['peat-control'])
    expect(made.has(`peat/${SECRETS.caKey}`)).toBe(true)
    const caReaders = workloads(all).filter((m) => (podOf(m).volumes ?? []).some((v: any) => v.secret?.secretName === SECRETS.caKey))
    expect(caReaders.map((m) => m.metadata.name)).toEqual(['peat-control'])
  })
  test('with an issuer named, the authority\'s key is not copied to the platform namespace', () => {
    const withIssuer = render(resolved({ certificates: { issuer: { name: 'my-issuer' } } }), KEPT)
    expect(withIssuer.some((m) => m.kind === 'Secret' && m.metadata.name === SECRETS.caKey)).toBe(false)
  })
})

describe('the control layer\'s account', () => {
  // what control.ts calls, per namespace (test/control.test.ts reads the program for it): its Roles grant exactly this
  const NEEDS: Record<string, { group: string; resource: string; verbs: string[] }[]> = {
    peat: [
      { group: '', resource: 'pods', verbs: ['get', 'list', 'delete'] },
      { group: '', resource: 'secrets', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
      { group: '', resource: 'configmaps', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
      { group: 'apps', resource: 'statefulsets', verbs: ['get', 'list', 'create', 'update', 'delete'] },
      { group: 'batch', resource: 'jobs', verbs: ['get'] },
      { group: 'coordination.k8s.io', resource: 'leases', verbs: ['get', 'list', 'create', 'update', 'patch'] },
    ],
    'peat-compute': [
      { group: '', resource: 'pods', verbs: ['get', 'list', 'delete'] },
      { group: '', resource: 'secrets', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
      { group: 'apps', resource: 'deployments', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
      { group: 'cert-manager.io', resource: 'certificates', verbs: ['get', 'list', 'create', 'update', 'patch', 'delete'] },
    ],
    'peat-db': [{ group: '', resource: 'services', verbs: ['get', 'list', 'create', 'update', 'delete'] }],
    'peat-gateway': [{ group: '', resource: 'pods', verbs: ['get', 'list', 'delete'] }],
  }
  const flat = (rules: any[]) => rules.flatMap((x) => x.apiGroups.flatMap((g: string) => x.resources.map((res: string) => ({ group: g, resource: res, verbs: [...x.verbs].sort() })))).sort((a, b) => `${a.group}/${a.resource}`.localeCompare(`${b.group}/${b.resource}`))
  test('a Role in each of the four namespaces granting what the program calls and nothing more, bound to its account alone', () => {
    const roles = all.filter((m) => m.kind === 'Role' && m.metadata.name === CONTROL_ACCOUNT)
    expect(roles.map((x) => x.metadata.namespace).sort()).toEqual(Object.keys(NEEDS).sort())
    for (const role of roles) {
      const want = NEEDS[role.metadata.namespace!]!.map((x) => ({ ...x, verbs: [...x.verbs].sort() })).sort((a, b) => `${a.group}/${a.resource}`.localeCompare(`${b.group}/${b.resource}`))
      expect([role.metadata.namespace, flat(role.rules)]).toEqual([role.metadata.namespace, want])
      const binding = named(all, 'RoleBinding', CONTROL_ACCOUNT, role.metadata.namespace)
      expect(binding.subjects).toEqual([{ kind: 'ServiceAccount', name: CONTROL_ACCOUNT, namespace: 'peat' }])
    }
  })
  test('no wildcard in any Role or ClusterRole the operator renders', () => {
    for (const role of all.filter((m) => m.kind === 'Role' || m.kind === 'ClusterRole')) for (const x of role.rules) for (const v of [...x.apiGroups, ...x.resources, ...x.verbs]) expect([role.metadata.name, v]).not.toEqual([role.metadata.name, '*'])
  })
  test('the two cluster-wide grants: Nodes, get, list and patch, never create or delete (the node guard holds the patch to the platform\'s machines); and the Database objects, the databases\' records, with their status', () => {
    const crs = all.filter((m) => m.kind === 'ClusterRole')
    expect(crs.map((m) => m.metadata.name)).toEqual([`${CONTROL_ACCOUNT}-nodes`, `${CONTROL_ACCOUNT}-databases`])
    expect(crs[0]!.rules).toEqual([{ apiGroups: [''], resources: ['nodes'], verbs: ['get', 'list', 'patch'] }])
    expect(crs[1]!.rules).toEqual([
      { apiGroups: ['peat.zabaca.com'], resources: ['databases'], verbs: ['get', 'list', 'watch', 'create', 'patch', 'update', 'delete'] },
      { apiGroups: ['peat.zabaca.com'], resources: ['databases/status'], verbs: ['get', 'patch', 'update'] },
    ])
    for (const cr of crs) expect(all.find((m) => m.kind === 'ClusterRoleBinding' && m.metadata.name === cr.metadata.name)!.subjects).toEqual([{ kind: 'ServiceAccount', name: CONTROL_ACCOUNT, namespace: 'peat' }])
  })
  test('only the control layer and the storage controller (for its records) mount an API token; the gateways hold no credential at all', () => {
    const withToken = workloads(all).filter((m) => podOf(m).automountServiceAccountToken !== false).map((m) => m.metadata.name).sort()
    expect(withToken).toEqual(['peat-control', 'storage-controller'])
    const gw = named(all, 'Deployment', 'peat-gateway')
    expect(podOf(gw).automountServiceAccountToken).toBe(false)
    expect(podOf(gw).serviceAccountName).toBeUndefined()
    expect(containersOf(gw).some((c) => (c.env ?? []).some((e: any) => e.valueFrom?.secretKeyRef))).toBe(false)
    expect((podOf(gw).volumes ?? []).some((v: any) => v.secret)).toBe(false)
  })
  test('with records postgres, the storage controller mounts no API token and has no Role', () => {
    const pg = render(resolved({ controller: { records: 'postgres', postgres: { urlSecret: 'records-db' } } }), KEPT)
    expect(podOf(named(pg, 'Deployment', 'storage-controller')).automountServiceAccountToken).toBe(false)
    expect(pg.some((m) => m.kind === 'Role' && m.metadata.name === 'storage-controller')).toBe(false)
  })
})

describe('policies', () => {
  const selects = (p: Manifest, labels: Record<string, string>) => Object.entries(p.spec.podSelector.matchLabels ?? {}).every(([k, v]) => labels[k] === v)
  test('every pod has a policy of its own that selects it, in its namespace', () => {
    const pols = all.filter((m) => m.kind === 'NetworkPolicy' && m.metadata.name !== 'default-deny')
    for (const m of workloads(all)) {
      const labels = m.spec.template.metadata.labels
      expect([m.metadata.name, pols.some((p) => p.metadata.namespace === m.metadata.namespace && selects(p, labels))]).toEqual([m.metadata.name, true])
    }
  })
  test('the computes, which the control layer makes, have their policy too', () => {
    expect(named(all, 'NetworkPolicy', 'peat-compute', 'peat-compute').spec.podSelector).toEqual({ matchLabels: { app: 'peat-compute' } })
  })
  test('a default-deny in every namespace that holds pods, both ways', () => {
    const deny = all.filter((m) => m.kind === 'NetworkPolicy' && m.metadata.name === 'default-deny')
    expect(deny.map((m) => m.metadata.namespace).sort()).toEqual(['peat', 'peat-compute', 'peat-gateway'])
    for (const d of deny) expect(d.spec).toEqual({ podSelector: {}, policyTypes: ['Ingress', 'Egress'] })
  })
  test('the computes reach the control layer on 8080, and the control layer admits them there', () => {
    const compute = named(all, 'NetworkPolicy', 'peat-compute', 'peat-compute')
    expect(JSON.stringify(compute.spec.egress)).toContain('"app":"peat-control"')
    const ctl = named(all, 'NetworkPolicy', 'peat-control', 'peat')
    expect(JSON.stringify(ctl.spec.ingress)).toContain('"app":"peat-compute"')
    expect(ctl.spec.ingress[0].ports).toEqual([{ protocol: 'TCP', port: 8080 }])
  })
  test('the pieces that use the store reach it at its port only', () => {
    for (const name of ['pageserver', 'safekeeper', 'peat-control']) {
      const p = named(all, 'NetworkPolicy', name, 'peat')
      expect(p.spec.egress).toContainEqual({ to: [{ ipBlock: { cidr: '0.0.0.0/0' } }], ports: [{ protocol: 'TCP', port: 443 }] })
    }
    const minio = render(resolved({ objectStore: { endpoint: 'http://store.example.test:9000', bucket: 'b', credentialsSecret: 's' } }), KEPT)
    expect(named(minio, 'NetworkPolicy', 'pageserver', 'peat').spec.egress).toContainEqual({ to: [{ ipBlock: { cidr: '0.0.0.0/0' } }], ports: [{ protocol: 'TCP', port: 9000 }] })
  })
})

describe('sizes and settings', () => {
  const mem = (m: Manifest, c = 0) => podOf(m).containers[c].resources
  test('the defaults: a pageserver 768 MiB / 1.5 GiB, the controller 64 / 256, the broker 32 / 128, the control layer 64 / 256, a gateway 48 / 192', () => {
    expect(mem(named(all, 'StatefulSet', 'pageserver-w1'))).toMatchObject({ requests: { memory: '768Mi' }, limits: { memory: '1536Mi' } })
    expect(mem(named(all, 'Deployment', 'storage-controller'))).toMatchObject({ requests: { memory: '64Mi' }, limits: { memory: '256Mi' } })
    expect(mem(named(all, 'Deployment', 'storage-broker'))).toMatchObject({ requests: { memory: '32Mi' }, limits: { memory: '128Mi' } })
    expect(mem(named(all, 'Deployment', 'peat-control'))).toMatchObject({ requests: { memory: '64Mi' }, limits: { memory: '256Mi' } })
    expect(mem(named(all, 'Deployment', 'peat-gateway'))).toMatchObject({ requests: { memory: '48Mi' }, limits: { memory: '192Mi' } })
  })
  test('every container of the platform\'s pieces has a memory limit', () => {
    for (const m of workloads(all)) for (const c of podOf(m).containers) expect([m.metadata.name, c.resources?.limits?.memory !== undefined]).toEqual([m.metadata.name, true])
  })
  test('the stall settings reach the control layer: a compute waits 10 s on a pageserver that stopped answering and gives up a dial after 5 s', () => {
    const env = Object.fromEntries((podOf(named(all, 'Deployment', 'peat-control')).containers[0].env as any[]).filter((e) => 'value' in e).map((e) => [e.name, e.value]))
    expect(env['PS_RESPONSE_DISCONNECT_MS']).toBe('10000')
    expect(env['PS_CONN_OPTS']).toBe('?tcp_user_timeout=5000')
    expect(env['WAL_TRIM_EVERY_S']).toBe('3600')
    expect(env['SK_GONE_S']).toBe('1800')
    expect(env['PAGESERVER_SECONDARIES']).toBe('1')
  })
  test('a one-pageserver platform keeps no secondary copy (the controller could not place one)', () => {
    const env = Object.fromEntries((podOf(named(trialAll, 'Deployment', 'peat-control')).containers[0].env as any[]).filter((e) => 'value' in e).map((e) => [e.name, e.value]))
    expect(env['PAGESERVER_SECONDARIES']).toBe('0')
  })
})

describe('the storage pieces', () => {
  test('a pageserver restarts only when its pod is deleted (OnDelete): an image change never restarts two at once', () => {
    for (const m of all.filter((x) => x.kind === 'StatefulSet' && x.metadata.name.startsWith('pageserver-w'))) expect(m.spec.updateStrategy).toEqual({ type: 'OnDelete' })
  })
  test('the controller and the broker are replaced, never two at once (Recreate)', () => {
    for (const name of ['storage-controller', 'storage-broker']) expect(named(all, 'Deployment', name).spec.strategy).toEqual({ type: 'Recreate' })
  })
  test('no safekeeper StatefulSet is rendered: the control layer owns every one', () => {
    expect(all.some((m) => m.kind === 'StatefulSet' && m.metadata.name.startsWith('safekeeper'))).toBe(false)
  })
  test('the controller: its records in Kubernetes by default, with a copy in the store; or a Postgres named', () => {
    const cmd = podOf(named(all, 'Deployment', 'storage-controller')).containers[0].command as string[]
    expect(cmd.slice(cmd.indexOf('--records'), cmd.indexOf('--records') + 4)).toEqual(['--records', 'kubernetes', '--records-namespace', 'peat'])
    // a TOML document, as the controller reads it (a bare inline table it refuses: "invalid key")
    const copy = cmd[cmd.indexOf('--records-copy-storage') + 1]!
    expect(copy).toStartWith('remote_storage={ ')
    expect(copy).toContain("bucket_name='peat-data'")
    expect(Bun.TOML.parse(copy)).toMatchObject({ remote_storage: { bucket_name: 'peat-data', prefix_in_bucket: 'storage-controller' } })
    // the pod's own address in the Leader record, never the Service's (which every controller pod would share)
    expect(cmd[cmd.indexOf('--address-for-peers') + 1]).toBe('http://$(POD_IP):1234')
    expect(podOf(named(all, 'Deployment', 'storage-controller')).containers[0].env).toContainEqual({ name: 'POD_IP', valueFrom: { fieldRef: { fieldPath: 'status.podIP' } } })
    expect(cmd).toContain('--timelines-onto-safekeepers')
    expect(cmd).toContain('--timeline-safekeeper-count=3')
    const pg = podOf(named(render(resolved({ controller: { records: 'postgres', postgres: { urlSecret: 'records-db' } } }), KEPT), 'Deployment', 'storage-controller')).containers[0]
    expect(pg.command).toContain('--database-url')
    expect(pg.command).not.toContain('--records')
    expect(pg.env).toContainEqual({ name: 'DATABASE_URL', valueFrom: { secretKeyRef: { name: 'records-db', key: 'uri' } } })
  })
  test('the front door is the one Service in the gateways\' namespace, of the type the Platform asks for', () => {
    expect(named(all, 'Service', 'pg-front', 'peat-gateway').spec).toMatchObject({ type: 'ClusterIP', selector: { app: 'peat-gateway' }, ports: [{ port: 5432 }] })
    expect(named(render(resolved({ gateways: { service: { type: 'LoadBalancer' } } }), KEPT), 'Service', 'pg-front').spec.type).toBe('LoadBalancer')
  })
})

describe('the guards', () => {
  const policies = all.filter((m) => m.kind === 'ValidatingAdmissionPolicy')
  const bindings = all.filter((m) => m.kind === 'ValidatingAdmissionPolicyBinding')
  test('five policies, each bound and denying, none with a parameter; which pods the control layer may exec into is RBAC\'s, per move', () => {
    expect(policies.map((p) => p.metadata.name).sort()).toEqual([NODE_POLICY, SERVICE_POLICY, STATEFULSET_POLICY, COMPUTE_POLICY, EXEC_SHAPE_POLICY].sort())
    for (const p of policies) expect(bindings.find((b) => b.metadata.name === p.metadata.name)!.spec).toEqual({ policyName: p.metadata.name, validationActions: ['Deny'] })
    // a parameter object's copy in the API server can go stale for good (kubernetes#141015): no policy reads one
    for (const p of policies) expect(p.spec.paramKind).toBeUndefined()
    const shape = JSON.stringify(policies.find((p) => p.metadata.name === EXEC_SHAPE_POLICY))
    expect(shape).not.toContain('params')
    for (const k of ["object.command[0] == 'psql'", "object.command[0] == 'bash' && object.command[1] == '/move/copy.sh'", "object.container == 'compute'", "request.namespace == 'peat-compute'"]) expect(shape).toContain(k)
    expect(all.some((m) => m.kind === 'ConfigMap' && /^peat-move-(pods|execs)$/.test(m.metadata.name))).toBe(false)
    // the control layer's own Roles hold no exec anywhere
    for (const role of all.filter((m) => m.kind === 'Role' && m.metadata.name === CONTROL_ACCOUNT)) expect(JSON.stringify(role.rules)).not.toContain('pods/exec')
  })
  test('a move\'s grant: a Role of exactly the pods named, get and create on pods/exec, bound to the control layer\'s account, in the compute namespace', () => {
    expect(moveExecGrant(r, 'orders', [])).toEqual([])
    const [role, binding] = moveExecGrant(r, 'orders', ['compute-orders-a', 'compute-mv-0123456789ab-b']) as any[]
    expect(role).toMatchObject({ kind: 'Role', metadata: { name: 'peat-move-orders', namespace: 'peat-compute' }, rules: [{ apiGroups: [''], resources: ['pods/exec'], resourceNames: ['compute-orders-a', 'compute-mv-0123456789ab-b'], verbs: ['create', 'get'] }] })
    expect(binding).toMatchObject({ kind: 'RoleBinding', metadata: { name: 'peat-move-orders', namespace: 'peat-compute' }, roleRef: { kind: 'Role', name: 'peat-move-orders' }, subjects: [{ kind: 'ServiceAccount', name: CONTROL_ACCOUNT, namespace: 'peat' }] })
  })
  test('the compute guard names the platform\'s compute images, and judges a change that keeps a compute\'s images apart', () => {
    const compute = JSON.stringify(policies.find((p) => p.metadata.name === COMPUTE_POLICY))
    expect(compute).toContain('compute-pg18:release-2026-12-01')
    expect(compute).toContain('compute-pg19:release-2026-12-01')
    // a compute made on the release before an upgrade is stopped on its own image: a change that keeps every image is not judged on them
    expect(compute).toContain("request.operation == 'UPDATE' && (has(object.spec.template.spec.containers) ? dyn(object.spec.template.spec.containers.map(c, c.image)) : dyn([])) == (has(oldObject.spec.template.spec.containers) ? dyn(oldObject.spec.template.spec.containers.map(c, c.image)) : dyn([]))")
    expect(compute).toContain('variables.keepsImages || (')
  })
  test('each judges the control layer\'s account and no one else\'s, and fails closed', () => {
    for (const p of policies) {
      expect(p.spec.failurePolicy).toBe('Fail')
      expect(p.spec.matchConditions).toEqual([{ name: 'by-the-control-layer', expression: `request.userInfo.username == '${controlUser(r.names)}'` }])
    }
    expect(controlUser(r.names)).toBe('system:serviceaccount:peat:peat-control')
  })
  test('the node guard: creates, updates and deletes of Nodes; names the platform\'s machine label and the dead taint', () => {
    const p = named(all, 'ValidatingAdmissionPolicy', NODE_POLICY)
    expect(p.spec.matchConstraints.resourceRules).toEqual([{ apiGroups: [''], apiVersions: ['v1'], operations: ['CREATE', 'UPDATE', 'DELETE'], resources: ['nodes'] }])
    const text = JSON.stringify(p)
    expect(text).toContain("'peat.zabaca.com/machine'")
    expect(p.spec.variables).toContainEqual({ name: 'deadKey', expression: "'peat.zabaca.com/dead'" })
    for (const f of ['configSource', 'externalID', 'podCIDR', 'podCIDRs', 'providerID', 'unschedulable']) expect(text).toContain(`object.spec.${f}`)
  })
  test('the node guard follows an install\'s own label and dead taint', () => {
    const own = render(resolved({ machines: { label: { key: 'example.com/db-machine', value: 'yes' }, deadTaint: { key: 'example.com/gone', value: 'true', effect: 'NoSchedule' } } }), KEPT)
    const text = JSON.stringify(named(own, 'ValidatingAdmissionPolicy', NODE_POLICY))
    expect(text).toContain("'example.com/db-machine'")
    expect(text).toContain("'example.com/gone'")
    expect(text).not.toContain('peat.zabaca.com/machine')
  })
  test('the statefulset guard admits the storage images given, and only those; a host path only on the safekeepers\' volume', () => {
    const one = JSON.stringify(named(all, 'ValidatingAdmissionPolicy', STATEFULSET_POLICY))
    expect(one).toContain("c.image in ['ghcr.io/zabaca/peat/storage:release-2026-12-01']")
    const two = JSON.stringify(named(render(r, KEPT, undefined, ['ghcr.io/zabaca/peat/storage:a', 'ghcr.io/zabaca/peat/storage:b']), 'ValidatingAdmissionPolicy', STATEFULSET_POLICY))
    expect(two).toContain("c.image in ['ghcr.io/zabaca/peat/storage:a', 'ghcr.io/zabaca/peat/storage:b']")
    expect(two).not.toContain('release-2026-12-01')
    expect(one).toContain("v.hostPath.path == '/var/lib/peat/safekeeper'")
    expect(one).toContain("object.metadata.namespace == 'peat'")
  })
  test('the service guard: an ExternalName only in the database namespace and only to the front door', () => {
    const text = JSON.stringify(named(all, 'ValidatingAdmissionPolicy', SERVICE_POLICY))
    expect(text).toContain("object.metadata.namespace == 'peat-db'")
    expect(text).toContain(`object.spec.externalName == '${frontDoor(r.names)}'`)
    expect(frontDoor(r.names)).toBe('pg-front.peat-gateway.svc.cluster.local')
  })
})

describe('style', () => {
  test('no em dash or en dash in anything rendered', () => {
    for (const ms of [all, trialAll, taintedAll]) expect([0x2013, 0x2014].some((c) => JSON.stringify(ms).includes(String.fromCharCode(c)))).toBe(false)
  })
})

describe('the upgrade\'s storage check', () => {
  // the storage controller and one pageserver, as they answer: a shard that kept the error of a notification refused once (this controller never clears one)
  const answers = (held: { slug: string; generation: number }) => async (url: string | URL | Request) => {
    const u = String(url)
    const body = u.endsWith('/control/v1/node') ? [{ id: 1, availability: 'Active', scheduling: 'Active', listen_http_addr: 'ps1', listen_http_port: 9898 }]
      : u.endsWith('/control/v1/tenant') ? [{ tenant_id: 't1', policy: { Attached: 0 }, shards: [{ tenant_shard_id: 't1', node_attached: 1, node_secondary: [], last_error: 'Non-retryable error 403 Forbidden', is_reconciling: false, is_pending_compute_notification: false, scheduling_policy: 'Active' }] }]
      : u.endsWith('/debug/v1/tenant') ? [{ tenant_shard_id: 't1', generation: 3, intent: { attached: 1, secondary: [] }, observed: { locations: { 1: { conf: { mode: 'AttachedSingle', generation: 3 } } } } }]
      : u === 'http://ps1:9898/v1/tenant' ? [{ id: 't1', attachment_status: { slug: held.slug }, generation: held.generation }]
      : u.endsWith('/control/v1/safekeeper') ? [] : null
    return new Response(JSON.stringify(body), { status: body === null ? 404 : 200 })
  }
  test('an old error on a shard the controller and its pageserver agree is attached is a leftover, not a halt; where they disagree it is a problem', async () => {
    expect(await storageOf(r, KEPT[AUTH_PRIVATE]!, answers({ slug: 'attached', generation: 3 }) as typeof fetch).problems([1])).toEqual([])
    const stale = await storageOf(r, KEPT[AUTH_PRIVATE]!, answers({ slug: 'attached', generation: 2 }) as typeof fetch).problems([1])
    expect(stale).toHaveLength(1)
    expect(stale[0]).toContain('has an error that is not a leftover (node 1 itself lists it as attached at generation 2, not attached at generation 3)')
  })
})
